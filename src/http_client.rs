use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};
use pathlink::Link;
use std::sync::Arc;
use tc_error::{TCError, TCResult};
use tc_ir::{Map, TxnId};
use tc_state::State;
use tc_value::Value;
use url::form_urlencoded;

use crate::{Method, gateway::RpcGateway};

#[derive(Clone)]
pub struct HttpRpcGateway {
    client: hyper::Client<hyper::client::HttpConnector, hyper::Body>,
}

impl HttpRpcGateway {
    pub fn new() -> Self {
        Self {
            client: hyper::Client::new(),
        }
    }
}

impl Default for HttpRpcGateway {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcGateway for HttpRpcGateway {
    fn get(
        &self,
        target: Link,
        txn: crate::txn::TxnHandle,
        key: Value,
    ) -> BoxFuture<'static, TCResult<State>> {
        let uri = match append_kernel_txn_query(&target.to_string(), txn.id(), Some(&key)) {
            Ok(uri) => uri,
            Err(err) => return futures::future::ready(Err(err)).boxed(),
        };

        let body = match encode_value_body(&key) {
            Ok(bytes) => bytes,
            Err(err) => return futures::future::ready(Err(err)).boxed(),
        };

        let request = match build_request(Method::Get, uri, txn.authorization_header(), body) {
            Ok(req) => req,
            Err(err) => return futures::future::ready(Err(err)).boxed(),
        };

        let client = self.client.clone();
        async move {
            let (status, body_bytes) =
                crate::outbound_http::send(&client, request, crate::outbound_http::DEFAULT_TIMEOUT)
                    .await?;
            let body_bytes = crate::outbound_http::ensure_success(status, body_bytes)?;

            decode_state_body(body_bytes, &txn).await
        }
        .boxed()
    }

    fn put(
        &self,
        target: Link,
        txn: crate::txn::TxnHandle,
        key: Value,
        value: State,
    ) -> BoxFuture<'static, TCResult<()>> {
        let uri = match append_kernel_txn_query(&target.to_string(), txn.id(), Some(&key)) {
            Ok(uri) => uri,
            Err(err) => return futures::future::ready(Err(err)).boxed(),
        };

        let body = match encode_state_body(value) {
            Ok(bytes) => bytes,
            Err(err) => return futures::future::ready(Err(err)).boxed(),
        };

        let request = match build_request(Method::Put, uri, txn.authorization_header(), body) {
            Ok(req) => req,
            Err(err) => return futures::future::ready(Err(err)).boxed(),
        };

        let client = self.client.clone();
        async move {
            let (status, body_bytes) =
                crate::outbound_http::send(&client, request, crate::outbound_http::DEFAULT_TIMEOUT)
                    .await?;
            let _ = crate::outbound_http::ensure_success(status, body_bytes)?;

            Ok(())
        }
        .boxed()
    }

    fn post(
        &self,
        target: Link,
        txn: crate::txn::TxnHandle,
        params: Map<State>,
    ) -> BoxFuture<'static, TCResult<State>> {
        let uri = match append_kernel_txn_query(&target.to_string(), txn.id(), None) {
            Ok(uri) => uri,
            Err(err) => return futures::future::ready(Err(err)).boxed(),
        };

        let body = if params.is_empty() {
            Vec::new()
        } else {
            match encode_params_body(params) {
                Ok(bytes) => bytes,
                Err(err) => return futures::future::ready(Err(err)).boxed(),
            }
        };

        let request = match build_request(Method::Post, uri, txn.authorization_header(), body) {
            Ok(req) => req,
            Err(err) => return futures::future::ready(Err(err)).boxed(),
        };

        let client = self.client.clone();
        async move {
            let (status, body_bytes) =
                crate::outbound_http::send(&client, request, crate::outbound_http::DEFAULT_TIMEOUT)
                    .await?;
            let body_bytes = crate::outbound_http::ensure_success(status, body_bytes)?;

            decode_state_body(body_bytes, &txn).await
        }
        .boxed()
    }

    fn delete(
        &self,
        target: Link,
        txn: crate::txn::TxnHandle,
        key: Value,
    ) -> BoxFuture<'static, TCResult<()>> {
        let uri = match append_kernel_txn_query(&target.to_string(), txn.id(), Some(&key)) {
            Ok(uri) => uri,
            Err(err) => return futures::future::ready(Err(err)).boxed(),
        };

        let request =
            match build_request(Method::Delete, uri, txn.authorization_header(), Vec::new()) {
                Ok(req) => req,
                Err(err) => return futures::future::ready(Err(err)).boxed(),
            };

        let client = self.client.clone();
        async move {
            let (status, body_bytes) =
                crate::outbound_http::send(&client, request, crate::outbound_http::DEFAULT_TIMEOUT)
                    .await?;
            let _ = crate::outbound_http::ensure_success(status, body_bytes)?;

            Ok(())
        }
        .boxed()
    }
}

fn build_request(
    method: Method,
    uri: String,
    authorization: Option<String>,
    body: Vec<u8>,
) -> TCResult<http::Request<hyper::Body>> {
    use http::header::{AUTHORIZATION, HeaderValue};

    let method = match method {
        Method::Get => hyper::Method::GET,
        Method::Put => hyper::Method::PUT,
        Method::Post => hyper::Method::POST,
        Method::Delete => hyper::Method::DELETE,
    };

    let mut builder = http::Request::builder().method(method).uri(uri);

    if let Some(token) = authorization {
        let value = HeaderValue::from_str(&token)
            .map_err(|err| TCError::bad_request(format!("invalid bearer token: {err}")))?;
        builder = builder.header(AUTHORIZATION, value);
    }

    builder
        .body(hyper::Body::from(body))
        .map_err(|err| TCError::bad_request(err.to_string()))
}

// Sync HTTP client boundary: encode via async streams by blocking explicitly here.
fn encode_state_body(state: State) -> TCResult<Vec<u8>> {
    use futures::TryStreamExt;

    if state.is_none() {
        return Ok(Vec::new());
    }

    let stream =
        destream_json::encode(state).map_err(|err| TCError::bad_request(err.to_string()))?;
    futures::executor::block_on(async move {
        stream
            .map_err(|err| std::io::Error::other(err.to_string()))
            .try_fold(Vec::new(), |mut acc, chunk| async move {
                acc.extend_from_slice(&chunk);
                Ok(acc)
            })
            .await
            .map_err(|err| TCError::bad_request(err.to_string()))
    })
}

fn encode_params_body(params: Map<State>) -> TCResult<Vec<u8>> {
    use futures::TryStreamExt;

    let stream = destream_json::encode(State::Map(params))
        .map_err(|err| TCError::bad_request(err.to_string()))?;
    futures::executor::block_on(async move {
        stream
            .map_err(|err| std::io::Error::other(err.to_string()))
            .try_fold(Vec::new(), |mut acc, chunk| async move {
                acc.extend_from_slice(&chunk);
                Ok(acc)
            })
            .await
            .map_err(|err| TCError::bad_request(err.to_string()))
    })
}

fn encode_value_body(value: &Value) -> TCResult<Vec<u8>> {
    use futures::TryStreamExt;

    if matches!(value, Value::None) {
        return Ok(Vec::new());
    }

    let stream = destream_json::encode(value.clone())
        .map_err(|err| TCError::bad_request(err.to_string()))?;
    futures::executor::block_on(async move {
        stream
            .map_err(|err| std::io::Error::other(err.to_string()))
            .try_fold(Vec::new(), |mut acc, chunk| async move {
                acc.extend_from_slice(&chunk);
                Ok(acc)
            })
            .await
            .map_err(|err| TCError::bad_request(err.to_string()))
    })
}

fn encode_value_json(value: &Value) -> TCResult<String> {
    use futures::TryStreamExt;

    let stream = destream_json::encode(value.clone())
        .map_err(|err| TCError::bad_request(err.to_string()))?;
    let bytes = futures::executor::block_on(async move {
        stream
            .map_err(|err| std::io::Error::other(err.to_string()))
            .try_fold(Vec::new(), |mut acc, chunk| async move {
                acc.extend_from_slice(&chunk);
                Ok(acc)
            })
            .await
            .map_err(|err| TCError::bad_request(err.to_string()))
    })?;

    String::from_utf8(bytes).map_err(|err| TCError::bad_request(err.to_string()))
}

async fn decode_state_body(body: Bytes, txn: &crate::txn::TxnHandle) -> TCResult<State> {
    use futures::stream;

    if body.is_empty() || body.iter().all(|b| b.is_ascii_whitespace()) {
        return Ok(State::None);
    }

    let stream = stream::iter(vec![Ok::<Bytes, std::io::Error>(body)]);
    let context: Arc<dyn tc_ir::Transaction> = Arc::new(txn.clone());
    destream_json::try_decode(context, stream)
        .await
        .map_err(|err| TCError::bad_request(err.to_string()))
}

/// Append the kernel-owned transaction query parameters for internal host-to-host RPC.
///
/// Public clients must not construct these URLs. This helper rejects targets which already
/// contain `txn_id` so callers cannot override the active transaction context.
pub(crate) fn append_kernel_txn_query(
    uri: &str,
    txn_id: TxnId,
    key: Option<&Value>,
) -> TCResult<String> {
    let parsed: http::Uri = uri
        .parse()
        .map_err(|err| TCError::bad_request(format!("invalid URI: {err}")))?;

    let path = parsed.path().to_string();
    let query = parsed.query().unwrap_or("").to_string();

    let pairs = form_urlencoded::parse(query.as_bytes()).into_owned();
    if pairs
        .into_iter()
        .any(|(key, _)| key.eq_ignore_ascii_case("txn_id"))
    {
        return Err(TCError::bad_request(
            "outbound targets must not include txn_id; it is supplied by the kernel".to_string(),
        ));
    }

    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (key, value) in form_urlencoded::parse(query.as_bytes()).into_owned() {
        serializer.append_pair(&key, &value);
    }
    serializer.append_pair("txn_id", &txn_id.to_string());
    if let Some(key) = key.filter(|key| !matches!(key, Value::None)) {
        let key_json = encode_value_json(key)?;
        serializer.append_pair("key", &key_json);
    }
    let query = serializer.finish();

    let mut parts = parsed.into_parts();
    let path_and_query = if query.is_empty() {
        path
    } else {
        format!("{path}?{query}")
    };

    parts.path_and_query = Some(
        http::uri::PathAndQuery::from_maybe_shared(path_and_query)
            .map_err(|err| TCError::bad_request(err.to_string()))?,
    );

    let rebuilt =
        http::Uri::from_parts(parts).map_err(|err| TCError::bad_request(err.to_string()))?;

    Ok(rebuilt.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tc_ir::{NetworkTime, TxnId};

    #[test]
    fn appends_txn_id_query_param() {
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(1), 1).with_trace([0_u8; 32]);

        let uri = "http://localhost:8702/lib?foo=bar";
        let updated = append_kernel_txn_query(uri, txn_id, None).expect("append txn_id");
        assert!(updated.contains("foo=bar"));
        assert!(updated.contains("txn_id="));
    }

    #[test]
    fn overwrites_existing_txn_id_query_param() {
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(2), 2).with_trace([0_u8; 32]);

        let uri = "http://localhost:8702/lib?txn_id=old&foo=bar";
        let err =
            append_kernel_txn_query(uri, txn_id, None).expect_err("should reject existing txn_id");
        assert!(err.message().contains("must not include txn_id"));
    }

    #[test]
    fn attaches_bearer_token_header() {
        let request = build_request(
            Method::Get,
            "http://localhost:8702/lib?txn_id=1".to_string(),
            Some("Bearer abc.def".to_string()),
            Vec::new(),
        )
        .expect("request");

        let auth = request.headers().get("authorization").expect("auth header");
        assert_eq!(auth.to_str().expect("auth header str"), "Bearer abc.def");
    }
}
