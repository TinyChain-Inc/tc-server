use std::io;

use bytes::Bytes;
use futures::{TryStreamExt, stream};
use tc_ir::Scalar;

use crate::http_body::BoundedBody;
use crate::{Deadline, HostLimits, HostResources};
use hyper::Body;

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

    let error = (&mut body)
        .try_collect::<Vec<_>>()
        .await
        .expect_err("oversized body");
    let error = body.decode_error(error);
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

    let error = (&mut body)
        .try_collect::<Vec<_>>()
        .await
        .expect_err("admission deadline");
    let error = body.decode_error(error);
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
