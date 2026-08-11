use bytes::Bytes;
use hyper::body::HttpBody;
use hyper::header;
use hyper::header::AUTHORIZATION;
use tc_error::{TCError, TCResult};

use super::response::{internal_error_response, payload_too_large_response};
use super::{Request, Response};

pub(crate) fn parse_txn_id(req: &Request) -> Result<Option<tc_ir::TxnId>, TxnParseError> {
    crate::txn::wire::parse_txn_id_query(req.uri().query()).map_err(|_| TxnParseError::Invalid)
}

pub(crate) enum TxnParseError {
    Invalid,
}

pub(crate) fn parse_bearer_token(req: &Request) -> Option<String> {
    let header = req.headers().get(AUTHORIZATION)?;
    let value = header.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }

    let token = token.trim();
    if token.is_empty() {
        return None;
    }

    Some(token.to_string())
}

#[allow(clippy::collapsible_if)]
pub(crate) async fn parse_body(
    req: Request,
    max_request_bytes: usize,
) -> Result<(Request, bool), Response> {
    let (parts, mut body) = req.into_parts();
    if let Some(len) = parts.headers.get(header::CONTENT_LENGTH) {
        let len = len
            .to_str()
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .ok_or_else(|| payload_too_large_response("invalid content-length header"))?;
        if len > max_request_bytes {
            return Err(payload_too_large_response("request payload too large"));
        }
    }

    let mut body_bytes = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(|_| internal_error_response("failed to read request body"))?;
        let next_len = body_bytes.len().saturating_add(chunk.len());
        if next_len > max_request_bytes {
            return Err(payload_too_large_response("request payload too large"));
        }
        body_bytes.extend_from_slice(&chunk);
    }

    let body_is_none = body_bytes.iter().all(|b| b.is_ascii_whitespace());
    let body_bytes = Bytes::from(body_bytes);
    Ok((
        Request::from_parts(parts, hyper::Body::from(body_bytes)),
        body_is_none,
    ))
}

pub(crate) async fn decode_native_body(
    req: Request,
    txn: crate::txn::TxnHandle,
    max_request_bytes: usize,
) -> TCResult<Option<tc_state::State<crate::txn::TxnHandle>>> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    if req.body().size_hint().exact() == Some(0) {
        return Ok(None);
    }
    if req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|length| length.to_str().ok())
        .and_then(|length| length.parse::<usize>().ok())
        .is_some_and(|length| length > max_request_bytes)
    {
        return Err(payload_limit_error(max_request_bytes));
    }

    let exceeded = Arc::new(AtomicBool::new(false));
    let exceeded_stream = Arc::clone(&exceeded);
    let body = req.into_body();
    let stream = Box::pin(futures::stream::try_unfold(
        (body, 0usize, exceeded_stream),
        move |(mut body, read, exceeded)| async move {
            match body.data().await {
                Some(Ok(chunk)) => {
                    let read = read.saturating_add(chunk.len());
                    if read > max_request_bytes {
                        exceeded.store(true, Ordering::Relaxed);
                        return Err(std::io::Error::other("request payload limit exceeded"));
                    }
                    Ok(Some((chunk, (body, read, exceeded))))
                }
                Some(Err(err)) => Err(std::io::Error::other(err.to_string())),
                None => Ok(None),
            }
        },
    ));

    match destream_json::try_decode(txn, stream).await {
        Ok(state) => Ok(Some(state)),
        Err(_) if exceeded.load(Ordering::Relaxed) => Err(payload_limit_error(max_request_bytes)),
        Err(err) => Err(TCError::bad_request(err.to_string())),
    }
}

fn payload_limit_error(limit: usize) -> TCError {
    TCError::payload_too_large(
        format!("request payload exceeds the {limit}-byte limit"),
        tc_error::Pressure::new(
            "/host/resource/http/request-body",
            tc_error::PressureReason::QuotaExceeded,
        ),
    )
}
