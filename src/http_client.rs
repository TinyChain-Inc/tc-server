use crate::State;
use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};
use safecast::TryCastFrom;
use tc_error::{TCError, TCResult};
use tc_ir::{IntoView, Map, Scalar, TxnId};

use crate::{Method, gateway::RpcGateway};

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

#[derive(Clone)]
pub struct HttpGateway {
    client: hyper::Client<hyper::client::HttpConnector, hyper::Body>,
}

impl HttpGateway {
    pub fn new() -> Self {
        let mut connector = hyper::client::HttpConnector::new();
        connector.set_connect_timeout(Some(CONNECT_TIMEOUT));
        Self {
            client: hyper::Client::builder().build(connector),
        }
    }
}

impl Default for HttpGateway {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcGateway for HttpGateway {
    fn get(
        &self,
        target: pathlink::Link,
        txn: crate::txn::TxnHandle,
        key: Scalar,
    ) -> BoxFuture<'static, TCResult<State>> {
        let client = self.client.clone();
        async move {
            let body = encode_state_body(State::from_scalar(key), txn.clone()).await?;
            let body_bytes =
                send_request(&client, Method::Get, target, &txn, "application/json", body).await?;
            decode_state_body(body_bytes, &txn).await
        }
        .boxed()
    }

    fn put(
        &self,
        target: pathlink::Link,
        txn: crate::txn::TxnHandle,
        key: Scalar,
        value: State,
    ) -> BoxFuture<'static, TCResult<()>> {
        let client = self.client.clone();
        async move {
            let (content_type, body) = if target.path().len() == 1 {
                encode_application_put(key, value).await?
            } else {
                (
                    "application/json",
                    encode_state_body(
                        State::Tuple(vec![State::from_scalar(key), value]),
                        txn.clone(),
                    )
                    .await?,
                )
            };
            send_request(&client, Method::Put, target, &txn, content_type, body)
                .await
                .map(|_| ())
        }
        .boxed()
    }

    fn post(
        &self,
        target: pathlink::Link,
        txn: crate::txn::TxnHandle,
        params: Map<State>,
    ) -> BoxFuture<'static, TCResult<State>> {
        let client = self.client.clone();
        async move {
            let body = encode_state_body(State::Map(params), txn.clone()).await?;
            let body_bytes = send_request(
                &client,
                Method::Post,
                target,
                &txn,
                "application/json",
                body,
            )
            .await?;
            decode_state_body(body_bytes, &txn).await
        }
        .boxed()
    }

    fn delete(
        &self,
        target: pathlink::Link,
        txn: crate::txn::TxnHandle,
        key: Scalar,
    ) -> BoxFuture<'static, TCResult<()>> {
        let client = self.client.clone();
        async move {
            let body = encode_state_body(State::from_scalar(key), txn.clone()).await?;
            send_request(
                &client,
                Method::Delete,
                target,
                &txn,
                "application/json",
                body,
            )
            .await
            .map(|_| ())
        }
        .boxed()
    }
}

pub(crate) async fn encode_application_put(
    key: Scalar,
    value: State,
) -> TCResult<(&'static str, Vec<u8>)> {
    match value {
        State::Scalar(Scalar::Value(tc_value::Value::Bytes(module)))
            if matches!(key, Scalar::Value(tc_value::Value::None)) =>
        {
            Ok(("application/wasm", module.to_vec()))
        }
        value => {
            let Scalar::Value(tc_value::Value::Link(identity)) = key else {
                return Err(TCError::bad_request(
                    "application PUT requires an identity Link key",
                ));
            };
            let definition = Scalar::try_cast_from(value, |_| {
                TCError::bad_request("application PUT requires a scalar definition")
            })?;
            let body = crate::literal::encode_json(
                crate::literal::Definition(identity, definition),
                crate::library::MAX_LIBRARY_BYTES,
            )
            .await?;
            Ok(("application/json", body))
        }
    }
}

#[async_trait::async_trait]
impl crate::replication::ClusterGateway for HttpGateway {
    async fn put(
        &self,
        peer: &str,
        token: &str,
        txn_id: TxnId,
        target: &pathlink::Link,
        key: Scalar,
        value: State,
        deadline: crate::Deadline,
    ) -> TCResult<()> {
        let (content_type, body) = if target.path().len() == 1 {
            encode_application_put(key, value).await?
        } else {
            let value = Scalar::try_cast_from(value, |_| {
                TCError::bad_request("replicated PUT requires a scalar value")
            })?;
            let body = crate::literal::encode_json(
                Scalar::Tuple(vec![key, value]),
                crate::literal::MAX_DEFINITION_BYTES,
            )
            .await?;
            ("application/json", body)
        };
        send_peer_request(
            &self.client,
            peer,
            token,
            txn_id,
            &target.to_string(),
            hyper::Method::PUT,
            Some(content_type),
            body,
            deadline,
        )
        .await
    }

    async fn delete(
        &self,
        peer: &str,
        token: &str,
        txn_id: TxnId,
        target: &pathlink::Link,
        key: Scalar,
        deadline: crate::Deadline,
    ) -> TCResult<()> {
        let body = crate::literal::encode_json(key, crate::literal::MAX_DEFINITION_BYTES).await?;
        send_peer_request(
            &self.client,
            peer,
            token,
            txn_id,
            &target.to_string(),
            hyper::Method::DELETE,
            Some("application/json"),
            body,
            deadline,
        )
        .await
    }

    async fn decide_resource(
        &self,
        peer: &str,
        token: &str,
        txn_id: TxnId,
        resource: &pathlink::PathBuf,
        commit: bool,
        deadline: crate::Deadline,
    ) -> TCResult<()> {
        send_peer_request(
            &self.client,
            peer,
            token,
            txn_id,
            &resource.to_string(),
            if commit {
                hyper::Method::PUT
            } else {
                hyper::Method::DELETE
            },
            None,
            hyper::Body::empty(),
            deadline,
        )
        .await
    }
}

async fn send_peer_request(
    client: &hyper::Client<hyper::client::HttpConnector, hyper::Body>,
    peer: &str,
    token: &str,
    txn_id: TxnId,
    path: &str,
    method: hyper::Method,
    content_type: Option<&str>,
    body: impl Into<hyper::Body>,
    deadline: crate::Deadline,
) -> TCResult<()> {
    let uri = peer_txn_url(peer, path, txn_id)?;
    let (status, body) = send_http(
        client,
        method,
        uri,
        Some(format!("Bearer {token}")),
        content_type,
        body,
        deadline,
    )
    .await?;
    crate::outbound_http::ensure_success(status, body).map(|_| ())
}

pub(crate) fn peer_txn_url(peer: &str, path: &str, txn_id: TxnId) -> TCResult<String> {
    let peer = crate::replication::normalize_peer(peer)?;
    let mut url = url::Url::parse(&peer)
        .map_err(|error| TCError::bad_request(format!("invalid peer URL: {error}")))?;
    url.set_path(path);
    crate::uri::append_kernel_txn_id(&mut url, txn_id)
}

async fn send_request(
    client: &hyper::Client<hyper::client::HttpConnector, hyper::Body>,
    method: Method,
    target: pathlink::Link,
    txn: &crate::TxnHandle,
    content_type: &'static str,
    body: Vec<u8>,
) -> TCResult<Bytes> {
    let mut url = url::Url::parse(&target.to_string())
        .map_err(|error| TCError::bad_request(format!("invalid RPC target: {error}")))?;
    let uri = crate::uri::append_kernel_txn_id(&mut url, txn.id())?;
    let (status, body) = send_http(
        client,
        http_method(method),
        uri,
        txn.authorization_header(),
        Some(content_type),
        body,
        txn.deadline(),
    )
    .await?;
    crate::outbound_http::ensure_success(status, body)
}

pub(crate) async fn send_http(
    client: &hyper::Client<hyper::client::HttpConnector, hyper::Body>,
    method: hyper::Method,
    uri: String,
    authorization: Option<String>,
    content_type: Option<&str>,
    body: impl Into<hyper::Body>,
    deadline: crate::Deadline,
) -> TCResult<(http::StatusCode, Bytes)> {
    let request = build_http_request(method, uri, authorization, content_type, body)?;
    crate::outbound_http::send(client, request, deadline).await
}

fn build_http_request(
    method: hyper::Method,
    uri: String,
    authorization: Option<String>,
    content_type: Option<&str>,
    body: impl Into<hyper::Body>,
) -> TCResult<http::Request<hyper::Body>> {
    use http::header::{AUTHORIZATION, HeaderValue};

    let mut builder = http::Request::builder().method(method).uri(uri);
    if let Some(token) = authorization {
        let value = HeaderValue::from_str(&token)
            .map_err(|err| TCError::bad_request(format!("invalid bearer token: {err}")))?;
        builder = builder.header(AUTHORIZATION, value);
    }
    if let Some(content_type) = content_type {
        builder = builder.header(http::header::CONTENT_TYPE, content_type);
    }

    builder
        .body(body.into())
        .map_err(|err| TCError::bad_request(err.to_string()))
}

fn http_method(method: Method) -> hyper::Method {
    match method {
        Method::Get => hyper::Method::GET,
        Method::Put => hyper::Method::PUT,
        Method::Post => hyper::Method::POST,
        Method::Delete => hyper::Method::DELETE,
    }
}

async fn encode_state_body(state: State, txn: crate::TxnHandle) -> TCResult<Vec<u8>> {
    use futures::TryStreamExt;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn explicit_null_is_an_ordinary_request_body() {
        let txn = crate::txn::test_txn("http-rpc-null").await;
        let body = encode_state_body(State::None, txn)
            .await
            .expect("encode explicit null");
        assert_eq!(body, b"null");
    }

    #[test]
    fn attaches_application_authority_headers() {
        let request = build_http_request(
            hyper::Method::GET,
            "http://localhost:8702/lib?txn_id=1".to_string(),
            Some("Bearer abc.def".to_string()),
            Some("application/json"),
            Vec::new(),
        )
        .expect("request");

        let auth = request.headers().get("authorization").expect("auth header");
        assert_eq!(auth.to_str().expect("auth header str"), "Bearer abc.def");
    }
}
