use std::sync::Arc;

use bytes::Bytes;
use futures::stream;
use hyper::header::AUTHORIZATION;
use hyper::{body::to_bytes, header};
use tc_error::{TCError, TCResult};
use tc_ir::TxnId;
use tc_value::Value;
use url::form_urlencoded;

use crate::txn::TxnHandle;

use super::response::{internal_error_response, payload_too_large_response};
use super::{Request, Response};

pub(crate) fn parse_txn_id(req: &Request) -> Result<Option<TxnId>, TxnParseError> {
    use std::str::FromStr;

    let query = req.uri().query().unwrap_or("");
    let txn_id_param = form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .find(|(k, _)| k.eq_ignore_ascii_case("txn_id"))
        .map(|(_, v)| v);

    match txn_id_param {
        Some(raw) => TxnId::from_str(&raw)
            .map(Some)
            .map_err(|_| TxnParseError::Invalid),
        None => Ok(None),
    }
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

pub(crate) async fn parse_body(
    req: Request,
    max_request_bytes_unauth: usize,
) -> Result<(Request, bool), Response> {
    let (parts, body) = req.into_parts();
    let has_bearer = parts.headers.get(AUTHORIZATION).is_some();
    let max_bytes = if has_bearer {
        None
    } else {
        Some(max_request_bytes_unauth)
    };

    if let (Some(limit), Some(len)) = (max_bytes, parts.headers.get(header::CONTENT_LENGTH)) {
        let len = len
            .to_str()
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .ok_or_else(|| payload_too_large_response("invalid content-length header"))?;
        if len > limit {
            return Err(payload_too_large_response("request payload too large"));
        }
    }

    let body_bytes = to_bytes(body)
        .await
        .map_err(|_| internal_error_response("failed to read request body"))?;
    if let Some(limit) = max_bytes && body_bytes.len() > limit {
        return Err(payload_too_large_response("request payload too large"));
    }

    let body_is_none = body_bytes.iter().all(|b| b.is_ascii_whitespace());
    let mut req = Request::from_parts(parts, hyper::Body::from(body_bytes.clone()));
    if !body_is_none {
        req.extensions_mut().insert(RequestBody::new(body_bytes));
    }

    Ok((req, body_is_none))
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone)]
pub(crate) struct RequestBody {
    bytes: Bytes,
}

#[cfg_attr(not(test), allow(dead_code))]
impl RequestBody {
    pub(crate) fn new(bytes: Bytes) -> Self {
        Self { bytes }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bytes.is_empty() || self.bytes.iter().all(|b| b.is_ascii_whitespace())
    }

    pub(crate) fn clone_bytes(&self) -> Bytes {
        self.bytes.clone()
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn decode_request_body_with_txn<T>(req: &Request) -> TCResult<Option<T>>
where
    T: destream::de::FromStream<Context = Arc<dyn tc_ir::Transaction>>,
{
    let body = match req.extensions().get::<RequestBody>() {
        Some(body) if !body.is_empty() => body.clone_bytes(),
        _ => return Ok(None),
    };

    let txn = req
        .extensions()
        .get::<TxnHandle>()
        .cloned()
        .ok_or_else(|| TCError::internal("missing transaction handle for request body"))?;

    let stream = stream::iter(vec![Ok::<Bytes, std::io::Error>(body)]);

    let context: Arc<dyn tc_ir::Transaction> = Arc::new(txn);
    destream_json::try_decode(context, stream)
        .await
        .map(Some)
        .map_err(|err| TCError::bad_request(err.to_string()))
}

pub(crate) async fn decode_value_body(req: &Request) -> TCResult<Option<Value>> {
    match req.extensions().get::<RequestBody>() {
        Some(body) if !body.is_empty() => {
            let stream = stream::iter(vec![Ok::<Bytes, std::io::Error>(body.clone_bytes())]);
            return destream_json::try_decode((), stream)
                .await
                .map(Some)
                .map_err(|err| TCError::bad_request(err.to_string()));
        }
        _ => {}
    }

    let query = req.uri().query().unwrap_or("");
    let key = form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .find(|(k, _)| k.eq_ignore_ascii_case("key"))
        .map(|(_, v)| v);

    let Some(raw) = key else {
        return Ok(None);
    };

    if raw.trim().is_empty() {
        return Ok(Some(Value::None));
    }

    let stream = stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from(
        raw.into_bytes(),
    ))]);

    destream_json::try_decode((), stream)
        .await
        .map(Some)
        .map_err(|err| TCError::bad_request(err.to_string()))
}
