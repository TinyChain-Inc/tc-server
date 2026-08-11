use bytes::Bytes;
use futures::FutureExt;
use futures::stream;
use tc_value::Value;
use url::form_urlencoded;

use super::response::{bad_request_response, internal_error_response, not_found};
use super::{Body, Request, StatusCode};

pub fn host_handler_with_public_keys(
    keys: crate::auth::PublicKeyStore,
) -> impl crate::http::HttpHandler {
    move |req: Request| {
        let keys = keys.clone();

        async move {
            match req.uri().path() {
                crate::uri::HOST_METRICS => {
                    let snapshots = req
                        .extensions()
                        .get::<crate::HostResources>()
                        .map(crate::HostResources::snapshots)
                        .unwrap_or_default();
                    let body = serde_json::to_string(&snapshots)
                        .expect("capacity snapshots must encode as JSON");
                    hyper::Response::builder()
                        .status(StatusCode::OK)
                        .header(hyper::header::CONTENT_TYPE, "application/json")
                        .body(Body::from(body))
                        .expect("metrics response")
                }
                crate::uri::HOST_AUTH_CONTEXT => {
                    let Some(txn) = req.extensions().get::<crate::txn::TxnHandle>() else {
                        return hyper::Response::builder()
                            .status(StatusCode::UNAUTHORIZED)
                            .body(Body::empty())
                            .expect("unauthorized auth context response");
                    };
                    match crate::host::auth_context(txn) {
                        Ok(state) => crate::http::state_response(state),
                        Err(_) => hyper::Response::builder()
                            .status(StatusCode::UNAUTHORIZED)
                            .body(Body::empty())
                            .expect("unauthorized auth context response"),
                    }
                }
                crate::uri::HOST_PUBLIC_KEY => {
                    use base64::Engine as _;

                    let query = req.uri().query().unwrap_or("");
                    let key = form_urlencoded::parse(query.as_bytes())
                        .into_owned()
                        .find(|(k, _)| k.eq_ignore_ascii_case("key"))
                        .map(|(_, v)| v);

                    let Some(actor_id) = key else {
                        return bad_request_response("missing key query parameter");
                    };

                    let actor_id = match destream_json::try_decode(
                        (),
                        stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from(
                            actor_id.into_bytes(),
                        ))]),
                    )
                    .await
                    {
                        Ok(Value::String(value)) => value,
                        _ => return bad_request_response("invalid key query parameter"),
                    };

                    let Some(public_key) = keys.public_key(&actor_id) else {
                        return not_found();
                    };

                    let encoded =
                        base64::engine::general_purpose::STANDARD.encode(public_key.to_bytes());
                    let body = match serde_json::to_vec(&encoded) {
                        Ok(body) => body,
                        Err(_) => return internal_error_response("failed to encode public key"),
                    };

                    hyper::Response::builder()
                        .status(StatusCode::OK)
                        .header(hyper::header::CONTENT_TYPE, "application/json")
                        .body(Body::from(body))
                        .expect("public key response")
                }
                _ => not_found(),
            }
        }
        .boxed()
    }
}
