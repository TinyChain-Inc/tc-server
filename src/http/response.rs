use hyper::StatusCode;
use tc_error::{ErrorKind, TCError};

use super::{Body, Response};

pub(crate) fn method_not_allowed() -> Response {
    hyper::Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .body(Body::empty())
        .expect("method not allowed response")
}

pub(crate) fn not_found() -> Response {
    hyper::Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::empty())
        .expect("not found response")
}

pub(crate) fn handle_finalize_result(result: tc_error::TCResult<()>) -> Response {
    match result {
        Ok(()) => no_content(),
        Err(err) => tc_error_response(err),
    }
}

pub(crate) fn bad_request_response(msg: &str) -> Response {
    hyper::Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(Body::from(msg.to_string()))
        .expect("bad request response")
}

pub(crate) fn internal_error_response(msg: &str) -> Response {
    hyper::Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from(msg.to_string()))
        .expect("internal error response")
}

pub(crate) fn payload_too_large_response(msg: &str) -> Response {
    hyper::Response::builder()
        .status(StatusCode::PAYLOAD_TOO_LARGE)
        .body(Body::from(msg.to_string()))
        .expect("payload too large response")
}

pub(crate) fn no_content() -> Response {
    hyper::Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .expect("no content response")
}

pub(crate) fn tc_error_response(err: TCError) -> Response {
    let status = match err.code() {
        ErrorKind::BadGateway | ErrorKind::BadRequest => StatusCode::BAD_REQUEST,
        ErrorKind::Conflict => StatusCode::CONFLICT,
        ErrorKind::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
        ErrorKind::NotFound => StatusCode::NOT_FOUND,
        ErrorKind::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        ErrorKind::TooManyRequests => StatusCode::TOO_MANY_REQUESTS,
        ErrorKind::Unauthorized => StatusCode::UNAUTHORIZED,
        ErrorKind::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        ErrorKind::Timeout => StatusCode::REQUEST_TIMEOUT,
        ErrorKind::Forbidden => StatusCode::FORBIDDEN,
        ErrorKind::Internal | ErrorKind::NotImplemented => StatusCode::INTERNAL_SERVER_ERROR,
    };

    let mut response = hyper::Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "application/json");
    if let Some(retry_after_ms) = err
        .pressure()
        .and_then(|pressure| pressure.retry_after_ms())
    {
        let seconds = retry_after_ms.div_ceil(1000).max(1);
        response = response.header(hyper::header::RETRY_AFTER, seconds.to_string());
    }
    let pressure = err.pressure().map(|pressure| {
        serde_json::json!({
            "reason": pressure.reason().to_string(),
            "resource": pressure.resource(),
            "retry_after_ms": pressure.retry_after_ms(),
            "reliability": pressure.reliability().to_string(),
        })
    });
    let body = serde_json::json!({
        err.code().to_string(): {
            "message": err.message(),
            "stack": [],
            "pressure": pressure,
        }
    });
    response
        .body(Body::from(body.to_string()))
        .expect("tc error response")
}
