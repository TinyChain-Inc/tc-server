use std::io;

use bytes::Bytes;
use futures::{TryStreamExt, stream};
use hyper::Body;
use tc_ir::Scalar;

use crate::http_body::BoundedBody;
use crate::{Deadline, HostLimits, HostResources};

fn resources(application_bytes: usize) -> HostResources {
    let mut limits = HostLimits::default();
    limits.ingress.application_in_flight_bytes = application_bytes;
    HostResources::new(limits).expect("host resources")
}

#[tokio::test]
async fn admitted_stream_decodes_fragmented_json_and_merges_capacity() {
    let encoded = br#"{"/service/example/catalog/1.0.0":"catalog"}"#;
    let resources = resources(encoded.len());
    let input = stream::iter([
        Ok::<_, io::Error>(Bytes::new()),
        Ok(Bytes::copy_from_slice(&encoded[..17])),
        Ok(Bytes::copy_from_slice(&encoded[17..])),
    ]);
    let mut body = BoundedBody::new(
        Body::wrap_stream(input),
        encoded.len(),
        Some(resources.application_admission(Deadline::after(std::time::Duration::from_secs(1)))),
    );

    let crate::literal::Definition(identity, definition) = destream_json::try_decode((), &mut body)
        .await
        .expect("streamed definition");
    assert_eq!(identity.to_string(), "/service/example/catalog/1.0.0");
    assert_eq!(definition, Scalar::from(tc_value::Value::from("catalog")));
    assert_eq!(
        body.permit.take().expect("admission").num_permits(),
        encoded.len()
    );
}

#[tokio::test]
async fn admitted_stream_preserves_payload_limit_errors() {
    let resources = resources(8);
    let input = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"abcde"))]);
    let mut body = BoundedBody::new(
        Body::wrap_stream(input),
        4,
        Some(resources.application_admission(Deadline::after(std::time::Duration::from_secs(1)))),
    );

    (&mut body)
        .try_collect::<Vec<_>>()
        .await
        .expect_err("oversized body");

    let error = body.take_failure().expect("structured body failure");
    assert_eq!(error.code(), tc_error::ErrorKind::PayloadTooLarge);
    assert!(body.permit.take().is_none());
}

#[tokio::test]
async fn admitted_stream_preserves_admission_saturation() {
    let resources = resources(1);
    let deadline = Deadline::after(std::time::Duration::from_millis(20));
    let _occupied = resources
        .admit_application_bytes(1, Deadline::after(std::time::Duration::from_secs(1)))
        .await
        .expect("occupy application admission");
    let input = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"a"))]);
    let mut body = BoundedBody::new(
        Body::wrap_stream(input),
        1,
        Some(resources.application_admission(deadline)),
    );

    (&mut body)
        .try_collect::<Vec<_>>()
        .await
        .expect_err("admission deadline");

    let error = body.take_failure().expect("structured body failure");
    assert_eq!(error.code(), tc_error::ErrorKind::Unavailable);
}

#[tokio::test]
async fn malformed_json_releases_its_admission_with_the_stream() {
    let resources = resources(1);
    {
        let input = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"{"))]);
        let mut body = BoundedBody::new(
            Body::wrap_stream(input),
            1,
            Some(
                resources.application_admission(Deadline::after(std::time::Duration::from_secs(1))),
            ),
        );
        destream_json::try_decode::<_, _, crate::literal::Definition>((), &mut body)
            .await
            .expect_err("malformed definition");
        assert_eq!(body.permit.as_ref().expect("admission").num_permits(), 1);
    }

    let _released = resources
        .admit_application_bytes(1, Deadline::after(std::time::Duration::from_secs(1)))
        .await
        .expect("released admission");
}

#[tokio::test]
async fn shared_decoder_preserves_application_and_native_representations() {
    let kernel = crate::txn::test_kernel("shared-http-decoder").await;
    for root in ["/lib", "/class", "/service"] {
        let guard = kernel
            .begin_request(crate::Method::Put, root, false, None)
            .await
            .unwrap();

        let encoded = format!(r#"{{"{root}/example/item/1.0.0":"definition"}}"#);
        let request = hyper::Request::builder()
            .body(Body::from(encoded.clone()))
            .unwrap();

        let (state, permit) = crate::http::parse::decode_body(request, &guard)
            .await
            .unwrap();

        assert!(
            matches!(&state.unwrap(), crate::State::Tuple(values) if values.len() == 2),
            "{root}"
        );

        assert_eq!(permit.unwrap().num_permits(), encoded.len(), "{root}");
    }

    let guard = kernel
        .begin_request(crate::Method::Put, "/lib", false, None)
        .await
        .unwrap();

    let bytes = b"\0asm\x01\0\0\0";
    let request = hyper::Request::builder()
        .header("content-type", "application/wasm")
        .body(Body::from(bytes.as_slice()))
        .unwrap();

    let (state, permit) = crate::http::parse::decode_body(request, &guard)
        .await
        .unwrap();

    assert!(matches!(&state.unwrap(), crate::State::Tuple(values) if values.len() == 2));
    assert_eq!(permit.unwrap().num_permits(), bytes.len());
    drop(guard);

    for (body, present) in [(Body::empty(), false), (Body::from("null"), true)] {
        let guard = kernel
            .begin_request(crate::Method::Get, "/state", !present, None)
            .await
            .unwrap();

        let request = hyper::Request::new(body);
        let (state, permit) = crate::http::parse::decode_body(request, &guard)
            .await
            .unwrap();

        assert_eq!(state.is_some(), present);
        assert!(permit.is_none());
    }

    kernel.shutdown().await;
}

#[tokio::test]
async fn expired_response_rejects_ready_chunks_and_releases_request() {
    use hyper::body::HttpBody;

    use tc_value::class::NativeClass;

    let kernel = crate::txn::test_kernel("ready-response-deadline").await;
    let target = tc_value::ValueType::None.path().to_string();
    let request = kernel
        .begin_request(crate::Method::Get, &target, true, None)
        .await
        .unwrap();

    let deadline = request.deadline();
    let state = crate::State::Tuple(vec![
        crate::State::from(tc_value::Value::from(1_u64)),
        crate::State::from(tc_value::Value::from(2_u64)),
    ]);

    let mut response = super::native_state_response(state, *request).await.unwrap();
    response.body_mut().data().await.unwrap().unwrap();
    // The source can immediately produce more output after the original deadline.
    tokio::time::sleep_until(deadline.instant()).await;
    let error = response.body_mut().data().await.unwrap().unwrap_err();
    let error = std::error::Error::source(&error)
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();

    let error = error
        .get_ref()
        .unwrap()
        .downcast_ref::<tc_error::TCError>()
        .unwrap();

    assert_eq!(error.code(), tc_error::ErrorKind::Unavailable);
    assert!(response.body_mut().data().await.is_none());
    tokio::time::timeout(std::time::Duration::from_secs(2), kernel.shutdown())
        .await
        .unwrap();
}
