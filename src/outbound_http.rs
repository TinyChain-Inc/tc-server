use std::time::Duration;

use bytes::Bytes;
use tc_error::{TCError, TCResult};

pub(crate) const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) async fn send(
    client: &hyper::Client<hyper::client::HttpConnector, hyper::Body>,
    request: hyper::Request<hyper::Body>,
    timeout: Duration,
) -> TCResult<(hyper::StatusCode, Bytes)> {
    let response = tokio::time::timeout(timeout, client.request(request))
        .await
        .map_err(|_| {
            TCError::bad_gateway(format!("request timed out after {}s", timeout.as_secs()))
        })?
        .map_err(|err| TCError::bad_gateway(err.to_string()))?;

    let status = response.status();
    let body = hyper::body::to_bytes(response.into_body())
        .await
        .map_err(|err| TCError::bad_gateway(err.to_string()))?;

    Ok((status, body))
}

pub(crate) fn error_from_status(status: hyper::StatusCode, body: Bytes) -> TCError {
    let message = String::from_utf8_lossy(&body).to_string();
    match status {
        hyper::StatusCode::BAD_REQUEST => TCError::bad_request(message),
        hyper::StatusCode::UNAUTHORIZED => TCError::unauthorized(message),
        hyper::StatusCode::NOT_FOUND => TCError::not_found(message),
        hyper::StatusCode::CONFLICT => TCError::conflict(message),
        hyper::StatusCode::METHOD_NOT_ALLOWED => TCError::method_not_allowed("request", message),
        hyper::StatusCode::BAD_GATEWAY => TCError::bad_gateway(message),
        _ => TCError::internal(message),
    }
}

pub(crate) fn ensure_success(status: hyper::StatusCode, body: Bytes) -> TCResult<Bytes> {
    if status.is_success() {
        Ok(body)
    } else {
        Err(error_from_status(status, body))
    }
}
