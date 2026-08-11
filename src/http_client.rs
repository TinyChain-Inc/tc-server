use crate::State;
use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};
use pathlink::Link;
use tc_error::{TCError, TCResult};
use tc_ir::{IntoView, Map, Scalar, TxnId};
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
        key: Scalar,
    ) -> BoxFuture<'static, TCResult<State>> {
        let client = self.client.clone();
        async move {
            let uri = append_kernel_txn_query(&target.to_string(), txn.id(), Some(&key)).await?;
            let body = encode_state_body(State::from_scalar(key.clone()), txn.clone()).await?;
            let request = build_request(Method::Get, uri, txn.authorization_header(), body)?;
            let (status, body_bytes) =
                crate::outbound_http::send(&client, request, txn.deadline()).await?;
            let body_bytes = crate::outbound_http::ensure_success(status, body_bytes)?;

            decode_state_body(body_bytes, &txn).await
        }
        .boxed()
    }

    fn put(
        &self,
        target: Link,
        txn: crate::txn::TxnHandle,
        key: Scalar,
        value: State,
    ) -> BoxFuture<'static, TCResult<()>> {
        let client = self.client.clone();
        async move {
            let uri = append_kernel_txn_query(&target.to_string(), txn.id(), Some(&key)).await?;
            let body = encode_state_body(value, txn.clone()).await?;
            let request = build_request(Method::Put, uri, txn.authorization_header(), body)?;
            let (status, body_bytes) =
                crate::outbound_http::send(&client, request, txn.deadline()).await?;
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
        let client = self.client.clone();
        async move {
            let uri = append_kernel_txn_query(&target.to_string(), txn.id(), None).await?;
            let body = if params.is_empty() {
                Vec::new()
            } else {
                encode_state_body(State::Map(params), txn.clone()).await?
            };
            let request = build_request(Method::Post, uri, txn.authorization_header(), body)?;
            let (status, body_bytes) =
                crate::outbound_http::send(&client, request, txn.deadline()).await?;
            let body_bytes = crate::outbound_http::ensure_success(status, body_bytes)?;

            decode_state_body(body_bytes, &txn).await
        }
        .boxed()
    }

    fn delete(
        &self,
        target: Link,
        txn: crate::txn::TxnHandle,
        key: Scalar,
    ) -> BoxFuture<'static, TCResult<()>> {
        let client = self.client.clone();
        async move {
            let uri = append_kernel_txn_query(&target.to_string(), txn.id(), Some(&key)).await?;
            let request =
                build_request(Method::Delete, uri, txn.authorization_header(), Vec::new())?;
            let (status, body_bytes) =
                crate::outbound_http::send(&client, request, txn.deadline()).await?;
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

async fn encode_state_body(state: State, txn: crate::TxnHandle) -> TCResult<Vec<u8>> {
    use futures::TryStreamExt;

    if state.is_none() {
        return Ok(Vec::new());
    }

    let view = state.into_view(txn).await?;
    let stream =
        destream_json::encode(view).map_err(|err| TCError::bad_request(err.to_string()))?;
    stream
        .map_err(|err| std::io::Error::other(err.to_string()))
        .try_fold(Vec::new(), |mut acc, chunk| async move {
            acc.extend_from_slice(&chunk);
            Ok(acc)
        })
        .await
        .map_err(|err| TCError::bad_request(err.to_string()))
}

async fn encode_scalar_json(value: &Scalar) -> TCResult<String> {
    use futures::TryStreamExt;

    let stream = destream_json::encode(value.clone())
        .map_err(|err| TCError::bad_request(err.to_string()))?;
    let bytes = stream
        .map_err(|err| std::io::Error::other(err.to_string()))
        .try_fold(Vec::new(), |mut acc, chunk| async move {
            acc.extend_from_slice(&chunk);
            Ok(acc)
        })
        .await
        .map_err(|err| TCError::bad_request(err.to_string()))?;

    String::from_utf8(bytes).map_err(|err| TCError::bad_request(err.to_string()))
}

async fn decode_state_body(body: Bytes, _txn: &crate::txn::TxnHandle) -> TCResult<State> {
    use futures::stream;

    if body.is_empty() || body.iter().all(|b| b.is_ascii_whitespace()) {
        return Ok(State::None);
    }

    let stream = stream::iter(vec![Ok::<Bytes, std::io::Error>(body)]);
    destream_json::try_decode(_txn.clone(), stream)
        .await
        .map_err(|err| TCError::bad_request(err.to_string()))
}

/// Append the kernel-owned transaction query parameters for internal host-to-host RPC.
///
/// Public clients must not construct these URLs. This helper rejects targets which already
/// contain `txn_id` so callers cannot override the active transaction context.
pub(crate) async fn append_kernel_txn_query(
    uri: &str,
    txn_id: TxnId,
    key: Option<&Scalar>,
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

    let key_json = match key.filter(|key| !matches!(key, Scalar::Value(tc_value::Value::None))) {
        Some(key) => Some(encode_scalar_json(key).await?),
        None => None,
    };

    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (key, value) in form_urlencoded::parse(query.as_bytes()).into_owned() {
        serializer.append_pair(&key, &value);
    }
    serializer.append_pair("txn_id", &txn_id.to_string());
    if let Some(key_json) = key_json {
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

    #[tokio::test]
    async fn appends_txn_id_query_param() {
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(1), 1).with_trace([0_u8; 32]);

        let uri = "http://localhost:8702/lib?foo=bar";
        let updated = append_kernel_txn_query(uri, txn_id, None)
            .await
            .expect("append txn_id");
        assert!(updated.contains("foo=bar"));
        assert!(updated.contains("txn_id="));
    }

    #[tokio::test]
    async fn overwrites_existing_txn_id_query_param() {
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(2), 2).with_trace([0_u8; 32]);

        let uri = "http://localhost:8702/lib?txn_id=old&foo=bar";
        let err = append_kernel_txn_query(uri, txn_id, None)
            .await
            .expect_err("should reject existing txn_id");
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
