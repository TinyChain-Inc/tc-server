use super::*;

#[tokio::test]
async fn preserves_remote_pressure_metadata() {
    let error = TCError::resource_unavailable(
        "busy",
        tc_error::Pressure::new("/host/resource/rpc", tc_error::PressureReason::Saturated)
            .with_retry_after_ms(250),
    );
    let encoded = destream_json::encode(error.clone()).unwrap();
    let response = hyper::Response::builder()
        .status(hyper::StatusCode::SERVICE_UNAVAILABLE)
        .body(hyper::Body::wrap_stream(encoded))
        .unwrap();
    let decoded = match success_body(response, crate::Deadline::after(DEFAULT_TIMEOUT), 1024).await
    {
        Ok(_) => panic!("expected remote error"),
        Err(error) => error,
    };

    assert_eq!(decoded.code(), tc_error::ErrorKind::Unavailable);
    let pressure = decoded.pressure().unwrap();
    assert_eq!(pressure.resource(), "/host/resource/rpc");
    assert_eq!(pressure.retry_after_ms(), Some(250));
}

#[tokio::test]
async fn rejects_noncanonical_remote_errors() {
    let response = hyper::Response::builder()
        .status(hyper::StatusCode::BAD_REQUEST)
        .body(hyper::Body::from("not JSON"))
        .unwrap();
    let error = match success_body(response, crate::Deadline::after(DEFAULT_TIMEOUT), 1024).await {
        Ok(_) => panic!("expected malformed remote error"),
        Err(error) => error,
    };
    assert_eq!(error.code(), tc_error::ErrorKind::BadGateway);
}
