use std::str::FromStr;

use bytes::Bytes;
use futures::FutureExt;
use futures::stream;
use pathlink::PathBuf as LinkPathBuf;
use tc_value::Value;
use url::form_urlencoded;

use super::response::{bad_request_response, internal_error_response, not_found};
use super::{Body, Request, StatusCode};

pub fn host_handler_with_public_keys(
    keys: crate::auth::PublicKeyStore,
) -> impl crate::KernelHandler {
    let reflect = crate::reflect::reflect_handler();
    let state = crate::state::state_handler();

    move |req: Request| {
        let keys = keys.clone();
        let reflect = reflect.clone();
        let state = state.clone();

        async move {
            if req.uri().path().starts_with("/state/") {
                return state.call(req).await;
            }

            match req.uri().path() {
                _ if req.method() == hyper::Method::POST && {
                    let Ok(parsed) = LinkPathBuf::from_str(req.uri().path()) else {
                        return not_found();
                    };

                    parsed == LinkPathBuf::from(tc_ir::SCALAR_REFLECT_CLASS)
                        || parsed == LinkPathBuf::from(tc_ir::SCALAR_REFLECT_REF_PARTS)
                        || parsed == LinkPathBuf::from(tc_ir::OPDEF_REFLECT_FORM)
                        || parsed == LinkPathBuf::from(tc_ir::OPDEF_REFLECT_LAST_ID)
                        || parsed == LinkPathBuf::from(tc_ir::OPDEF_REFLECT_SCALARS)
                } =>
                {
                    reflect.call(req).await
                }
                crate::uri::HOST_METRICS => hyper::Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::empty())
                    .expect("metrics response"),
                crate::uri::HOST_AUTH_CONTEXT => {
                    let Some(txn) = req.extensions().get::<crate::txn::TxnHandle>() else {
                        return hyper::Response::builder()
                            .status(StatusCode::UNAUTHORIZED)
                            .body(Body::empty())
                            .expect("unauthorized auth context response");
                    };
                    let Some(auth) = txn.auth_context() else {
                        return hyper::Response::builder()
                            .status(StatusCode::UNAUTHORIZED)
                            .body(Body::empty())
                            .expect("unauthorized auth context response");
                    };

                    let claims = auth
                        .claims
                        .iter()
                        .map(|entry| {
                            serde_json::json!({
                                "host": entry.host,
                                "actor_id": entry.actor_id,
                                "link": entry.claim.link.to_string(),
                                "mode": u32::from(entry.claim.mask),
                            })
                        })
                        .collect::<Vec<_>>();
                    let payload = serde_json::json!({
                        "principal": auth.principal,
                        "txn_id": txn.id().to_string(),
                        "txn_timestamp_nanos": txn.id().timestamp().as_nanos(),
                        "token_verified_at_nanos": auth.verified_at_nanos,
                        "token_hosts": auth.token_hosts(),
                        "claims": claims,
                    });

                    let body = match serde_json::to_vec(&payload) {
                        Ok(body) => body,
                        Err(_) => return internal_error_response("failed to encode auth context"),
                    };

                    hyper::Response::builder()
                        .status(StatusCode::OK)
                        .header(hyper::header::CONTENT_TYPE, "application/json")
                        .body(Body::from(body))
                        .expect("auth context response")
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
