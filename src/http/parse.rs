use hyper::body::HttpBody;
use hyper::header::{self, AUTHORIZATION};
use tc_error::{TCError, TCResult};

use super::Request;

pub(crate) fn parse_bearer_token(req: &Request) -> Option<String> {
    req.headers()
        .get(AUTHORIZATION)?
        .to_str()
        .ok()
        .and_then(crate::auth::bearer_token)
        .map(str::to_owned)
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
