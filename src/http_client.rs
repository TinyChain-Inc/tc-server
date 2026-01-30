use futures::{FutureExt, TryFutureExt, future::BoxFuture};
use tc_ir::TxnId;
use url::form_urlencoded;

use crate::{
    Method,
    gateway::{RpcError, RpcGateway, RpcResponse},
    uri::{component_root, normalize_path},
};

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
    fn request(
        &self,
        method: Method,
        uri: String,
        txn_id: TxnId,
        bearer_token: Option<String>,
        body: Vec<u8>,
    ) -> BoxFuture<'static, Result<RpcResponse, RpcError>> {
        let uri = match append_txn_id(&uri, txn_id) {
            Ok(uri) => uri,
            Err(err) => return futures::future::ready(Err(err)).boxed(),
        };

        let request = match build_request(method, uri, bearer_token.as_deref(), body) {
            Ok(req) => req,
            Err(err) => return futures::future::ready(Err(err)).boxed(),
        };

        let client = self.client.clone();
        async move {
            let response = client
                .request(request)
                .map_err(|err| RpcError::Transport(err.to_string()))
                .await?;

            let status = response.status().as_u16();
            let headers = response
                .headers()
                .iter()
                .filter_map(|(name, value)| {
                    let value = value.to_str().ok()?;
                    Some((name.to_string(), value.to_string()))
                })
                .collect();

            let body_bytes = hyper::body::to_bytes(response.into_body())
                .map_err(|err| RpcError::Transport(err.to_string()))
                .await?;

            Ok(RpcResponse {
                status,
                headers,
                body: body_bytes.to_vec(),
            })
        }
        .boxed()
    }
}

#[allow(dead_code)]
fn validate_finalize_target(uri: &str) -> Result<String, RpcError> {
    let parsed: http::Uri = uri
        .parse()
        .map_err(|err| RpcError::InvalidTarget(format!("invalid URI: {err}")))?;

    let path = normalize_path(parsed.path());
    let root = component_root(path).ok_or_else(|| {
        RpcError::InvalidTarget("commit/rollback target is not a TinyChain component".to_string())
    })?;

    if root != path {
        return Err(RpcError::InvalidTarget(format!(
            "commit/rollback target must be component root (got {path})"
        )));
    }

    Ok(uri.to_string())
}

fn build_request(
    method: Method,
    uri: String,
    bearer_token: Option<&str>,
    body: Vec<u8>,
) -> Result<http::Request<hyper::Body>, RpcError> {
    use http::header::{AUTHORIZATION, HeaderValue};

    let method = match method {
        Method::Get => hyper::Method::GET,
        Method::Put => hyper::Method::PUT,
        Method::Post => hyper::Method::POST,
        Method::Delete => hyper::Method::DELETE,
    };

    let mut builder = http::Request::builder().method(method).uri(uri);

    if let Some(token) = bearer_token {
        let value = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|err| RpcError::InvalidTarget(format!("invalid bearer token: {err}")))?;
        builder = builder.header(AUTHORIZATION, value);
    }

    builder
        .body(hyper::Body::from(body))
        .map_err(|err| RpcError::InvalidTarget(err.to_string()))
}

fn append_txn_id(uri: &str, txn_id: TxnId) -> Result<String, RpcError> {
    let parsed: http::Uri = uri
        .parse()
        .map_err(|err| RpcError::InvalidTarget(format!("invalid URI: {err}")))?;

    let path = parsed.path().to_string();
    let query = parsed.query().unwrap_or("").to_string();

    let pairs = form_urlencoded::parse(query.as_bytes()).into_owned();
    if pairs
        .into_iter()
        .any(|(key, _)| key.eq_ignore_ascii_case("txn_id"))
    {
        return Err(RpcError::InvalidTarget(
            "outbound targets must not include txn_id; it is supplied by the kernel".to_string(),
        ));
    }

    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (key, value) in form_urlencoded::parse(query.as_bytes()).into_owned() {
        serializer.append_pair(&key, &value);
    }
    serializer.append_pair("txn_id", &txn_id.to_string());
    let query = serializer.finish();

    let mut parts = parsed.into_parts();
    let path_and_query = if query.is_empty() {
        path
    } else {
        format!("{path}?{query}")
    };

    parts.path_and_query = Some(
        http::uri::PathAndQuery::from_maybe_shared(path_and_query)
            .map_err(|err| RpcError::InvalidTarget(err.to_string()))?,
    );

    let rebuilt =
        http::Uri::from_parts(parts).map_err(|err| RpcError::InvalidTarget(err.to_string()))?;

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
        let updated = append_txn_id(uri, txn_id).expect("append txn_id");
        assert!(updated.contains("foo=bar"));
        assert!(updated.contains("txn_id="));
    }

    #[test]
    fn overwrites_existing_txn_id_query_param() {
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(2), 2).with_trace([0_u8; 32]);

        let uri = "http://localhost:8702/lib?txn_id=old&foo=bar";
        let err = append_txn_id(uri, txn_id).expect_err("should reject existing txn_id");
        match err {
            RpcError::InvalidTarget(msg) => assert!(msg.contains("must not include txn_id")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn attaches_bearer_token_header() {
        let request = build_request(
            Method::Get,
            "http://localhost:8702/lib?txn_id=1".to_string(),
            Some("abc.def"),
            Vec::new(),
        )
        .expect("request");

        let auth = request.headers().get("authorization").expect("auth header");
        assert_eq!(auth.to_str().expect("auth header str"), "Bearer abc.def");
    }

    #[test]
    fn rejects_finalize_to_non_root() {
        let err = validate_finalize_target("http://localhost:8702/lib/acme/foo/1.0.0/echo")
            .expect_err("should reject");

        match err {
            RpcError::InvalidTarget(msg) => assert!(msg.contains("component root")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn accepts_finalize_to_root() {
        validate_finalize_target("http://localhost:8702/lib/acme/foo/1.0.0")
            .expect("should accept");
    }
}
