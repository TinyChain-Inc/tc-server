use hyper::StatusCode;
use tc_error::{ErrorKind, TCError};

use super::{Body, Response};

pub(crate) fn method_not_allowed() -> Response {
    tc_error_response(TCError::method_not_allowed("HTTP method", "host route"))
}

pub(crate) fn not_found() -> Response {
    tc_error_response(TCError::not_found("host route"))
}

pub(crate) fn bad_request_response(msg: &str) -> Response {
    tc_error_response(TCError::bad_request(msg))
}

pub(crate) fn no_content() -> Response {
    hyper::Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .expect("no content response")
}

pub(crate) fn tc_error_response(err: TCError) -> Response {
    let status = match err.code() {
        ErrorKind::BadGateway => StatusCode::BAD_GATEWAY,
        ErrorKind::BadRequest => StatusCode::BAD_REQUEST,
        ErrorKind::Conflict => StatusCode::CONFLICT,
        ErrorKind::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
        ErrorKind::NotFound => StatusCode::NOT_FOUND,
        ErrorKind::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        ErrorKind::TooManyRequests => StatusCode::TOO_MANY_REQUESTS,
        ErrorKind::Unauthorized => StatusCode::UNAUTHORIZED,
        ErrorKind::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        ErrorKind::Timeout => StatusCode::REQUEST_TIMEOUT,
        ErrorKind::Forbidden => StatusCode::FORBIDDEN,
        ErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        ErrorKind::NotImplemented => StatusCode::NOT_IMPLEMENTED,
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
    response
        .body(crate::http_body::json_body(err))
        .expect("tc error response")
}
