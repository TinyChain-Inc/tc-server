use std::time::Duration;

use hyper::rt::Executor;
use hyper::{Body, Request};
use tower::Service;

use crate::{Kernel, Method};
use super::{KernelService, Transport};

#[cfg(feature = "http-client")]
#[tokio::test]
async fn enabled_http_protocols_stream_native_results_and_shutdown() {
    const ADD: pathlink::PathLabel =
        pathlink::path_label(&["state", "scalar", "value", "number", "add"]);

    let kernel = crate::txn::test_kernel("http-protocols").await;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopping) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(
        super::HttpServer::new(kernel.clone())
            .serve_listener_with_shutdown(listener, async { stopping.await.unwrap() }),
    );

    for version in [
        hyper::Version::HTTP_11,
        #[cfg(feature = "http2")]
        hyper::Version::HTTP_2,
    ] {
        let builder = hyper::Client::builder();
        #[cfg(feature = "http2")]
        let builder = {
            let mut builder = builder;
            builder.http2_only(version == hyper::Version::HTTP_2);
            builder
        };

        let client = builder.build_http();
        let path = pathlink::PathBuf::from(ADD);
        let request = Request::builder()
            .uri(format!("http://{address}{path}"))
            .header("content-type", "application/json")
            .body(Body::from("[19,23]"))
            .unwrap();

        let response = tokio::time::timeout(Duration::from_secs(5), client.request(request))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(response.version(), version);
        assert_eq!(response.status(), hyper::StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "application/json");
        let value: tc_value::Value = tokio::time::timeout(
            Duration::from_secs(5),
            destream_json::try_decode((), response.into_body()),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(value, tc_value::Value::from(42_u64));
    }

    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    kernel.shutdown().await;
}

#[tokio::test]
async fn expired_upload_releases_request_during_shutdown() {
    let kernel = crate::txn::test_kernel("stalled-upload-drain").await;
    let mut service = KernelService::new(kernel.clone());
    let (polled, entered) = tokio::sync::oneshot::channel();
    let mut polled = Some(polled);
    let body = Body::wrap_stream(futures::stream::poll_fn(move |_| {
        if let Some(polled) = polled.take() {
            polled.send(()).unwrap();
        }

        std::task::Poll::<Option<Result<bytes::Bytes, std::io::Error>>>::Pending
    }));

    let response = tokio::spawn(
        service.call(
            Request::builder()
                .method("PUT")
                .uri("/service")
                .header("content-type", "application/json")
                .body(body)
                .unwrap(),
        ),
    );

    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .unwrap()
        .unwrap();

    kernel.begin_shutdown();
    let (response, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(response, kernel.shutdown())
    })
    .await
    .unwrap();

    assert_eq!(
        response.unwrap().unwrap().status(),
        hyper::StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn cancelling_transport_drops_unfinished_response_ownership() {
    let kernel: Kernel = crate::txn::test_kernel("stalled-response-drain").await;
    let request = kernel
        .begin_request(Method::Get, "/state", true, None)
        .await
        .unwrap();

    let transport = Transport::new();
    let (send, started) = tokio::sync::oneshot::channel();
    transport.execute(async move {
        let _request = request;
        send.send(()).unwrap();
        std::future::pending::<()>().await;
    });

    started.await.unwrap();
    kernel.begin_shutdown();
    {
        let drain = kernel.shutdown();
        tokio::pin!(drain);
        assert!(futures::poll!(&mut drain).is_pending());
    }

    transport.stop.cancel();
    transport.tasks.close();
    tokio::time::timeout(Duration::from_secs(5), async {
        transport.tasks.wait().await;
        kernel.shutdown().await;
    })
    .await
    .unwrap();

    assert!(transport.tasks.is_empty());
}

#[tokio::test]
async fn http_shutdown_waits_for_an_admitted_upload_deadline() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let kernel = crate::txn::test_kernel("http-upload-drain").await;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopping) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(
        super::HttpServer::new(kernel.clone())
            .serve_listener_with_shutdown(listener, async { stopping.await.unwrap() }),
    );

    let earliest_deadline = kernel.deadline();
    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    client.write_all(b"PUT /service HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 1\r\nExpect: 100-continue\r\n\r\n").await.unwrap();
    // Hyper sends Continue when the admitted handler begins polling the body.
    // This coordinates shutdown with admission without sleeping or racing a send.
    let mut response = [0; b"HTTP/1.1 100 Continue\r\n\r\n".len()];
    tokio::time::timeout(Duration::from_secs(5), client.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(&response, b"HTTP/1.1 100 Continue\r\n\r\n");
    stop.send(()).unwrap();
    kernel.shutdown_started().await;
    assert!(
        !task.is_finished(),
        "accepted upload must drain before transport cancellation"
    );

    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert!(
        earliest_deadline.is_expired(),
        "stalled upload retains its original deadline"
    );

    kernel.shutdown().await;
}

#[tokio::test]
async fn shared_request_errors_release_admission_before_shutdown() {
    let kernel = crate::txn::test_kernel("shared-http-errors").await;
    let mut service = KernelService::new(kernel.clone());
    for (path, content_type, body) in [
        ("/lib", "application/json", "{"),
        ("/class", "application/json", "{}"),
        ("/service", "application/json", "{}"),
        ("/lib", "application/wasm", ""),
        ("/service", "text/plain", "definition"),
        ("/state", "application/json", "{"),
    ] {
        let response = service
            .call(
                Request::builder()
                    .method(if path == "/state" { "GET" } else { "PUT" })
                    .uri(path)
                    .header("content-type", content_type)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            hyper::StatusCode::BAD_REQUEST,
            "{path}: {content_type}"
        );

        assert_eq!(response.headers()["content-type"], "application/json");
        hyper::body::to_bytes(response.into_body()).await.unwrap();
    }

    tokio::time::timeout(Duration::from_secs(5), kernel.shutdown())
        .await
        .unwrap();
}
