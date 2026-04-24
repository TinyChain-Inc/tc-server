use aes_gcm_siv::{Aes256GcmSiv, Key};
use hyper::body::to_bytes;
use hyper::{Body, Request, StatusCode};

use super::ReplicationIssuer;
use super::crypto::decode_encrypted_payload;
use crate::Response;

pub(crate) fn empty_response(status: StatusCode) -> Response {
    hyper::Response::builder()
        .status(status)
        .body(Body::empty())
        .expect("empty response")
}

pub(crate) fn method_not_allowed(allow: &'static str) -> Response {
    hyper::Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .header(hyper::header::ALLOW, allow)
        .body(Body::empty())
        .expect("method not allowed")
}

pub(crate) fn text_response(status: StatusCode, message: impl ToString) -> Response {
    hyper::Response::builder()
        .status(status)
        .body(Body::from(message.to_string()))
        .expect("text response")
}

pub(crate) fn bad_request(message: impl ToString) -> Response {
    text_response(StatusCode::BAD_REQUEST, message)
}

pub(crate) fn json_ok(body: Vec<u8>) -> Response {
    hyper::Response::builder()
        .status(StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .expect("json response")
}

pub(crate) async fn decode_encrypted_request(
    req: Request<Body>,
    issuer: &ReplicationIssuer,
) -> Result<(String, Key<Aes256GcmSiv>), Response> {
    let body = to_bytes(req.into_body())
        .await
        .map_err(|err| bad_request(err.to_string()))?;

    let (nonce, ciphertext) =
        decode_encrypted_payload(body).map_err(|err| bad_request(err.to_string()))?;

    issuer
        .decrypt_path_with_key(&nonce, &ciphertext)
        .map_err(|err| bad_request(err.to_string()))
}
