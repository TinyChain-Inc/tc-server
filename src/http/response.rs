use std::io;

use futures::TryStreamExt;
use hyper::StatusCode;
use tc_error::{ErrorKind, TCError};
use tc_ir::LibrarySchema;

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

pub(crate) fn handle_finalize_result(result: Result<(), crate::txn::TxnError>) -> Response {
    match result {
        Ok(()) => no_content(),
        Err(crate::txn::TxnError::NotFound) => bad_request_response("unknown transaction id"),
        Err(crate::txn::TxnError::Unauthorized) => hyper::Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body(Body::empty())
            .expect("unauthorized response"),
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
        ErrorKind::Unauthorized => StatusCode::UNAUTHORIZED,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };

    hyper::Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "text/plain")
        .body(Body::from(err.message().to_string()))
        .expect("tc error response")
}

pub(crate) fn schema_response(schema: LibrarySchema) -> Response {
    match destream_json::encode(schema) {
        Ok(stream) => {
            let body = Body::wrap_stream(stream.map_err(|err| io::Error::other(err.to_string())));
            hyper::Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(body)
                .expect("native schema response")
        }
        Err(err) => internal_error_response(&err.to_string()),
    }
}
