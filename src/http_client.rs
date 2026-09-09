use crate::State;
use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};
use tc_error::{TCError, TCResult};
use tc_ir::{IntoView, Map, Scalar, TxnId};
use url::form_urlencoded;

use crate::{
    Method,
    gateway::{RpcGateway, RpcTarget},
};

pub type HttpRpcGateway = hyper::Client<hyper::client::HttpConnector, hyper::Body>;

impl RpcGateway for HttpRpcGateway {
    fn get(
        &self,
        target: RpcTarget,
        txn: crate::txn::TxnHandle,
        key: Scalar,
    ) -> BoxFuture<'static, TCResult<State>> {
        let client = self.clone();
        async move {
            let body = encode_state_body(State::from_scalar(key), txn.clone()).await?;
            let body_bytes = send_request(&client, Method::Get, target, &txn, body).await?;
            decode_state_body(body_bytes, &txn).await
        }
        .boxed()
    }

    fn put(
        &self,
        target: RpcTarget,
        txn: crate::txn::TxnHandle,
        key: Scalar,
        value: State,
    ) -> BoxFuture<'static, TCResult<()>> {
        let client = self.clone();
        async move {
            let body = encode_state_body(
                State::Tuple(vec![State::from_scalar(key), value]),
                txn.clone(),
            )
            .await?;
            send_request(&client, Method::Put, target, &txn, body)
                .await
                .map(drop)
        }
        .boxed()
    }

    fn post(
        &self,
        target: RpcTarget,
        txn: crate::txn::TxnHandle,
        params: Map<State>,
    ) -> BoxFuture<'static, TCResult<State>> {
        let client = self.clone();
        async move {
            let body = encode_state_body(State::Map(params), txn.clone()).await?;
            let body_bytes = send_request(&client, Method::Post, target, &txn, body).await?;
            decode_state_body(body_bytes, &txn).await
        }
        .boxed()
    }

    fn delete(
        &self,
        target: RpcTarget,
        txn: crate::txn::TxnHandle,
        key: Scalar,
    ) -> BoxFuture<'static, TCResult<()>> {
        let client = self.clone();
        async move {
            let body = encode_state_body(State::from_scalar(key), txn.clone()).await?;
            send_request(&client, Method::Delete, target, &txn, body)
                .await
                .map(drop)
        }
        .boxed()
    }
}

async fn send_request(
    client: &hyper::Client<hyper::client::HttpConnector, hyper::Body>,
    method: Method,
    target: RpcTarget,
    txn: &crate::TxnHandle,
    body: Vec<u8>,
) -> TCResult<Bytes> {
    let (target, expected_digest) = target.into_parts();
    let uri = append_kernel_txn_query(&target.to_string(), txn.id())?;
    let request = build_request(
        method,
        uri,
        txn.authorization_header(),
        expected_digest.as_ref(),
        body,
    )?;
    let (status, body) = crate::outbound_http::send(client, request, txn.deadline()).await?;
    crate::outbound_http::ensure_success(status, body)
}

fn build_request(
    method: Method,
    uri: String,
    authorization: Option<String>,
    expected_digest: Option<&crate::application::Digest>,
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
    if let Some(expected_digest) = expected_digest {
        builder = builder.header(
            crate::gateway::EXPECTED_DIGEST_HEADER,
            hex::encode(expected_digest),
        );
    }

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

/// Append the kernel-owned transaction identity for internal host-to-host RPC.
///
/// Public clients must not construct these URLs. This helper rejects targets which already
/// contain `txn_id` so callers cannot override the active transaction context.
pub(crate) fn append_kernel_txn_query(uri: &str, txn_id: TxnId) -> TCResult<String> {
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
        let updated = append_kernel_txn_query(uri, txn_id).expect("append txn_id");
        assert!(updated.contains("foo=bar"));
        assert!(updated.contains("txn_id="));
    }

    #[test]
    fn rejects_existing_txn_id_query_param() {
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(2), 2).with_trace([0_u8; 32]);

        let uri = "http://localhost:8702/lib?txn_id=old&foo=bar";
        let err = append_kernel_txn_query(uri, txn_id).expect_err("should reject existing txn_id");
        assert!(err.message().contains("must not include txn_id"));
    }

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
        let digest: crate::application::Digest =
            <sha2::Sha256 as sha2::Digest>::digest(b"definition").into();
        let request = build_request(
            Method::Get,
            "http://localhost:8702/lib?txn_id=1".to_string(),
            Some("Bearer abc.def".to_string()),
            Some(&digest),
            Vec::new(),
        )
        .expect("request");

        let auth = request.headers().get("authorization").expect("auth header");
        assert_eq!(auth.to_str().expect("auth header str"), "Bearer abc.def");
        assert_eq!(
            request.headers()[crate::gateway::EXPECTED_DIGEST_HEADER],
            hex::encode(digest)
        );
    }
}
