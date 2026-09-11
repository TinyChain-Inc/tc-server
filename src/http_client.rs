use crate::State;
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
            let response =
                send_request(&client, Method::Get, target, &txn, "application/json", body).await?;
            decode_state_response(response, &txn).await
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
            let (content_type, body) = if crate::uri::is_application_root(&target) {
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
            let response =
                send_request(&client, Method::Put, target, &txn, content_type, body).await?;
            crate::outbound_http::consume(response, txn.deadline(), txn.request_body_limit(), false)
                .await
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
            let response = send_request(
                &client,
                Method::Post,
                target,
                &txn,
                "application/json",
                body,
            )
            .await?;
            crate::outbound_http::decode(
                response,
                txn.clone(),
                txn.deadline(),
                txn.request_body_limit(),
            )
            .await
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
            let response = send_request(
                &client,
                Method::Delete,
                target,
                &txn,
                "application/json",
                body,
            )
            .await?;
            crate::outbound_http::consume(response, txn.deadline(), txn.request_body_limit(), false)
                .await
        }
        .boxed()
    }
}

pub(crate) async fn encode_application_put(
    key: Scalar,
    value: State,
) -> TCResult<(&'static str, hyper::Body)> {
    match value {
        State::Scalar(Scalar::Value(tc_value::Value::Bytes(module)))
            if matches!(key, Scalar::Value(tc_value::Value::None)) =>
        {
            Ok((
                "application/wasm",
                hyper::Body::from(bytes::Bytes::from_owner(module)),
            ))
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
            let body =
                crate::http_body::json_body(crate::literal::Definition(identity, definition));
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
        let (content_type, body) = if crate::uri::is_application_root(target) {
            encode_application_put(key, value).await?
        } else {
            let value = Scalar::try_cast_from(value, |_| {
                TCError::bad_request("replicated PUT requires a scalar value")
            })?;
            let body = crate::http_body::json_body(Scalar::Tuple(vec![key, value]));
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
            false,
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
        let body = crate::http_body::json_body(key);
        send_peer_request(
            &self.client,
            peer,
            token,
            txn_id,
            &target.to_string(),
            hyper::Method::DELETE,
            Some("application/json"),
            body,
            false,
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
            true,
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
    require_empty: bool,
    deadline: crate::Deadline,
) -> TCResult<()> {
    let uri = peer_txn_url(peer, path, txn_id)?;
    let response = send_http(
        client,
        method,
        uri,
        Some(format!("Bearer {token}")),
        content_type,
        body,
        deadline,
    )
    .await?;
    crate::outbound_http::consume(
        response,
        deadline,
        crate::literal::MAX_DEFINITION_BYTES,
        require_empty,
    )
    .await
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
    body: hyper::Body,
) -> TCResult<hyper::Response<hyper::Body>> {
    let mut url = url::Url::parse(&target.to_string())
        .map_err(|error| TCError::bad_request(format!("invalid RPC target: {error}")))?;
    let uri = crate::uri::append_kernel_txn_id(&mut url, txn.id())?;
    send_http(
        client,
        http_method(method),
        uri,
        txn.authorization_header(),
        Some(content_type),
        body,
        txn.deadline(),
    )
    .await
}

pub(crate) async fn send_http(
    client: &hyper::Client<hyper::client::HttpConnector, hyper::Body>,
    method: hyper::Method,
    uri: String,
    authorization: Option<String>,
    content_type: Option<&str>,
    body: impl Into<hyper::Body>,
    deadline: crate::Deadline,
) -> TCResult<hyper::Response<hyper::Body>> {
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
    use hyper::body::HttpBody;

    let mut builder = http::Request::builder().method(method).uri(uri);
    if let Some(token) = authorization {
        let value = HeaderValue::from_str(&token)
            .map_err(|err| TCError::bad_request(format!("invalid bearer token: {err}")))?;
        builder = builder.header(AUTHORIZATION, value);
    }
    if let Some(content_type) = content_type {
        builder = builder.header(http::header::CONTENT_TYPE, content_type);
    }

    let body = body.into();
    if builder
        .headers_ref()
        .is_some_and(|headers| !headers.contains_key(http::header::CONTENT_LENGTH))
        && body.size_hint().exact().is_none()
        && builder
            .method_ref()
            .is_some_and(|method| method == hyper::Method::GET)
    {
        // Hyper intentionally assumes an unknown-length GET body is empty unless
        // its streaming transfer is explicit.
        builder = builder.header(http::header::TRANSFER_ENCODING, "chunked");
    }

    builder
        .body(body)
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

async fn encode_state_body(state: State, txn: crate::TxnHandle) -> TCResult<hyper::Body> {
    let view = state.into_view(txn).await?;
    Ok(crate::http_body::json_body(view))
}

async fn decode_state_response(
    response: hyper::Response<hyper::Body>,
    txn: &crate::TxnHandle,
) -> TCResult<State> {
    let content_type = response
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());
    if content_type == Some("application/wasm") {
        let bytes = crate::outbound_http::collect_bytes(
            response,
            txn.deadline(),
            txn.application_body_limit(),
        )
        .await?;
        return Ok(State::from(tc_value::Value::Bytes(bytes)));
    }

    crate::outbound_http::decode(
        response,
        txn.clone(),
        txn.deadline(),
        txn.request_body_limit(),
    )
    .await
}

#[cfg(test)]
#[path = "../tests/support/http_client.rs"]
mod tests;
