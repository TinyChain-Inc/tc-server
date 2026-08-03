use std::{path::Path, sync::Arc};

use bytes::Bytes;
use futures::stream;
use hyper::header::AUTHORIZATION;
use hyper::{body::to_bytes, header};
use tc_collection::btree::PersistentFile;
use tc_error::{TCError, TCResult};
use tc_state::State;
use tc_value::Value;
use url::form_urlencoded;

use crate::txn::TxnHandle;

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
    if let Some(limit) = max_bytes {
        if body_bytes.len() > limit {
            return Err(payload_too_large_response("request payload too large"));
        }
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

/// An optional in-process representation of an HTTP request body.
#[cfg_attr(not(feature = "pyo3"), allow(dead_code))]
#[derive(Clone)]
pub(crate) struct NativeStateBody(State);

#[cfg_attr(not(feature = "pyo3"), allow(dead_code))]
impl NativeStateBody {
    pub(crate) fn new(state: State) -> Self {
        Self(state)
    }

    pub(crate) fn clone_state(&self) -> State {
        self.0.clone()
    }

    pub(crate) fn is_none(&self) -> bool {
        self.0.is_none()
    }
}

#[derive(Clone)]
pub(crate) struct BTreeDecodeRoots {
    persistent_dir: freqfs::DirLock<PersistentFile>,
    txn_root: freqfs::DirLock<PersistentFile>,
}

impl BTreeDecodeRoots {
    #[allow(dead_code)]
    pub(crate) fn new(
        persistent_dir: freqfs::DirLock<PersistentFile>,
        txn_root: freqfs::DirLock<PersistentFile>,
    ) -> Self {
        Self {
            persistent_dir,
            txn_root,
        }
    }

    pub(crate) fn persistent_dir(&self) -> freqfs::DirLock<PersistentFile> {
        self.persistent_dir.clone()
    }

    pub(crate) fn txn_root(&self) -> freqfs::DirLock<PersistentFile> {
        self.txn_root.clone()
    }
}

pub(crate) fn load_btree_decode_roots(data_dir: &Path) -> TCResult<BTreeDecodeRoots> {
    let root = data_dir.join("state").join("collection").join("btree_decode");
    std::fs::create_dir_all(root.join("persistent"))
        .map_err(|err| TCError::internal(err.to_string()))?;
    std::fs::create_dir_all(root.join("txn"))
        .map_err(|err| TCError::internal(err.to_string()))?;

    let cache = freqfs::Cache::<PersistentFile>::new(16 * 1024 * 1024, None);
    let persistent = Arc::clone(&cache)
        .load(root.join("persistent"))
        .map_err(|err| TCError::internal(err.to_string()))?;
    let txn = Arc::clone(&cache)
        .load(root.join("txn"))
        .map_err(|err| TCError::internal(err.to_string()))?;

    Ok(BTreeDecodeRoots::new(persistent, txn))
}

pub(crate) fn state_context_for_request(
    req: &Request,
    transaction: Arc<dyn tc_ir::Transaction>,
) -> tc_state::StateContext {
    let mut context = tc_state::state_context(transaction);
    if let Some(roots) = req.extensions().get::<BTreeDecodeRoots>() {
        context = context.with_btree_roots(roots.persistent_dir(), roots.txn_root());
    }

    context
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
    T: destream::de::FromStream + TryFrom<State>,
    T::Context: From<tc_state::StateContext>,
    <T as TryFrom<State>>::Error: std::fmt::Display,
{
    if let Some(body) = req.extensions().get::<NativeStateBody>() {
        if body.is_none() {
            return Ok(None);
        }

        return T::try_from(body.clone_state())
            .map(Some)
            .map_err(|err| TCError::bad_request(err.to_string()));
    }

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
    let state_context = state_context_for_request(req, context);
    destream_json::try_decode(state_context.into(), stream)
        .await
        .map(Some)
        .map_err(|err| TCError::bad_request(err.to_string()))
}

pub(crate) async fn decode_value_body(req: &Request) -> TCResult<Option<Value>> {
    decode_value_body_for_key(req, None).await
}

pub(crate) async fn decode_value_body_for_key(
    req: &Request,
    key_name: Option<&str>,
) -> TCResult<Option<Value>> {
    match req.extensions().get::<RequestBody>() {
        Some(body) if !body.is_empty() => {
            return decode_value_bytes_for_key(body.clone_bytes(), key_name).await;
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

    decode_value_bytes_for_key(Bytes::from(raw.into_bytes()), key_name).await
}

async fn decode_value_bytes_for_key(
    bytes: Bytes,
    key_name: Option<&str>,
) -> TCResult<Option<Value>> {
    let bytes = if let Some(key_name) = key_name {
        match serde_json::from_slice::<serde_json::Value>(&bytes) {
            Ok(serde_json::Value::Object(mut object)) => {
                if let Some(value) = object.remove(key_name) {
                    Bytes::from(serde_json::to_vec(&value).map_err(|err| {
                        TCError::bad_request(format!("invalid key value: {err}"))
                    })?)
                } else {
                    bytes
                }
            }
            _ => bytes,
        }
    } else {
        bytes
    };

    let stream = stream::iter(vec![Ok::<Bytes, std::io::Error>(bytes)]);
    destream_json::try_decode((), stream)
        .await
        .map(Some)
        .map_err(|err| TCError::bad_request(err.to_string()))
}
