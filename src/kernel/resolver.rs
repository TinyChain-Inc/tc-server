use std::{str::FromStr, sync::Arc};

use futures::{TryStreamExt, future::BoxFuture};

use crate::egress::EgressPolicy;
use crate::library::LibraryRegistry;
use crate::txn_server::TxnServer;
use crate::{Body, Request, Response};

use super::{KernelHandler, Method};

#[derive(Clone)]
pub(super) struct KernelTxnResolver {
    pub(super) gateway: Option<Arc<dyn crate::gateway::RpcGateway>>,
    pub(super) library_registry: Option<Arc<LibraryRegistry>>,
    pub(super) egress: EgressPolicy,
    pub(super) token_verifier: Arc<dyn crate::auth::TokenVerifier>,
    pub(super) txn_manager: crate::txn::TxnManager,
    pub(super) txn_server: TxnServer,
    pub(super) host_handler: Arc<dyn KernelHandler>,
}

impl KernelTxnResolver {
    pub(super) async fn prepare_outbound(
        &self,
        method: Method,
        target: &pathlink::Link,
        txn: &crate::txn::TxnHandle,
    ) -> tc_error::TCResult<(pathlink::Link, crate::txn::TxnHandle)> {
        let target_str = target.to_string();
        let registry = self.library_registry.clone().ok_or_else(|| {
            tc_error::TCError::unauthorized("no library manifest loaded (egress is default-deny)")
        })?;

        let bearer_token = match txn.raw_token() {
            Some(token) => {
                let ctx = self
                    .token_verifier
                    .verify(token.to_string())
                    .await
                    .map_err(|_| tc_error::TCError::unauthorized("invalid bearer token"))?;

                let owner_id = if ctx.claims.is_empty() {
                    ctx.owner_id.clone()
                } else {
                    crate::txn::owner_id_from_token(txn.id(), &ctx)
                        .map_err(|_| tc_error::TCError::unauthorized("invalid bearer token"))?
                };

                if self.txn_manager.get(&txn.id()).is_some() {
                    match self.txn_manager.interpret_request(
                        Some(txn.id()),
                        Some(&owner_id),
                        Some(&ctx.bearer_token),
                    ) {
                        Ok(crate::txn::TxnFlow::Begin(handle))
                        | Ok(crate::txn::TxnFlow::Use(handle)) => {
                            self.txn_server.touch(handle.id())
                        }
                        Err(crate::txn::TxnError::NotFound) => {
                            return Err(tc_error::TCError::bad_request("unknown transaction id"));
                        }
                        Err(crate::txn::TxnError::Unauthorized) => {
                            return Err(tc_error::TCError::unauthorized(
                                "unauthorized transaction owner",
                            ));
                        }
                    };
                }

                let claim = token_claim_for_target(method, &target_str);
                let ctx = match claim {
                    Some(claim) => {
                        if self.txn_manager.get(&txn.id()).is_some() {
                            let _ = self.txn_manager.record_claim(&txn.id(), claim.clone());
                        }
                        self.token_verifier
                            .grant(ctx, claim)
                            .await
                            .map_err(|_| tc_error::TCError::unauthorized("invalid bearer token"))?
                    }
                    None => ctx,
                };

                Some(ctx.bearer_token)
            }
            None => None,
        };

        let schema = registry.schema_for_txn(txn)?;
        let resolved = self.egress.resolve_target(&schema, &target_str)?;
        let resolved = pathlink::Link::from_str(&resolved)
            .map_err(|err| tc_error::TCError::bad_request(err.to_string()))?;
        let outbound_txn = match bearer_token {
            Some(token) => txn.with_bearer_token(token),
            None => txn.clone(),
        };
        Ok((resolved, outbound_txn))
    }
}

impl crate::gateway::RpcGateway for KernelTxnResolver {
    fn get(
        &self,
        target: pathlink::Link,
        txn: crate::txn::TxnHandle,
        key: tc_value::Value,
    ) -> BoxFuture<'static, tc_error::TCResult<tc_state::State>> {
        let resolver = self.clone();
        Box::pin(async move {
            let gateway = resolver
                .gateway
                .clone()
                .ok_or_else(|| tc_error::TCError::bad_gateway("no RPC gateway configured"))?;
            let (resolved, outbound_txn) = resolver
                .prepare_outbound(Method::Get, &target, &txn)
                .await?;
            gateway.get(resolved, outbound_txn, key).await
        })
    }

    fn put(
        &self,
        target: pathlink::Link,
        txn: crate::txn::TxnHandle,
        key: tc_value::Value,
        value: tc_state::State,
    ) -> BoxFuture<'static, tc_error::TCResult<()>> {
        let resolver = self.clone();
        Box::pin(async move {
            let gateway = resolver
                .gateway
                .clone()
                .ok_or_else(|| tc_error::TCError::bad_gateway("no RPC gateway configured"))?;
            let (resolved, outbound_txn) = resolver
                .prepare_outbound(Method::Put, &target, &txn)
                .await?;
            gateway.put(resolved, outbound_txn, key, value).await
        })
    }

    fn post(
        &self,
        target: pathlink::Link,
        txn: crate::txn::TxnHandle,
        params: tc_ir::Map<tc_state::State>,
    ) -> BoxFuture<'static, tc_error::TCResult<tc_state::State>> {
        let resolver = self.clone();
        Box::pin(async move {
            if is_scalar_reflect_path(&target) {
                return dispatch_host_post(resolver.host_handler.clone(), target, params).await;
            }
            let gateway = resolver
                .gateway
                .clone()
                .ok_or_else(|| tc_error::TCError::bad_gateway("no RPC gateway configured"))?;
            let (resolved, outbound_txn) = resolver
                .prepare_outbound(Method::Post, &target, &txn)
                .await?;
            gateway.post(resolved, outbound_txn, params).await
        })
    }

    fn delete(
        &self,
        target: pathlink::Link,
        txn: crate::txn::TxnHandle,
        key: tc_value::Value,
    ) -> BoxFuture<'static, tc_error::TCResult<()>> {
        let resolver = self.clone();
        Box::pin(async move {
            let gateway = resolver
                .gateway
                .clone()
                .ok_or_else(|| tc_error::TCError::bad_gateway("no RPC gateway configured"))?;
            let (resolved, outbound_txn) = resolver
                .prepare_outbound(Method::Delete, &target, &txn)
                .await?;
            gateway.delete(resolved, outbound_txn, key).await
        })
    }
}

pub(super) fn token_claim_for_target(method: Method, target: &str) -> Option<tc_ir::Claim> {
    let target_path = if let Ok(url) = url::Url::parse(target) {
        url.path().to_string()
    } else {
        target.to_string()
    };

    let root = crate::uri::component_root(&target_path)?;
    let link = pathlink::Link::from_str(root).ok()?;
    let mask = match method {
        Method::Get | Method::Put | Method::Post | Method::Delete => umask::Mode::all(),
    };
    Some(tc_ir::Claim::new(link, mask))
}

pub(super) fn is_scalar_reflect_path(target: &pathlink::Link) -> bool {
    let Ok(path) = pathlink::PathBuf::from_str(&target.to_string()) else {
        return false;
    };
    path == pathlink::PathBuf::from(tc_ir::SCALAR_REFLECT_CLASS)
        || path == pathlink::PathBuf::from(tc_ir::SCALAR_REFLECT_IF_PARTS)
        || path == pathlink::PathBuf::from(tc_ir::OPDEF_REFLECT_FORM)
        || path == pathlink::PathBuf::from(tc_ir::OPDEF_REFLECT_LAST_ID)
        || path == pathlink::PathBuf::from(tc_ir::OPDEF_REFLECT_SCALARS)
}

pub(super) fn is_scalar_reflect_path_str(path: &str) -> bool {
    let Ok(parsed) = pathlink::PathBuf::from_str(path) else {
        return false;
    };
    parsed == pathlink::PathBuf::from(tc_ir::SCALAR_REFLECT_CLASS)
        || parsed == pathlink::PathBuf::from(tc_ir::SCALAR_REFLECT_IF_PARTS)
        || parsed == pathlink::PathBuf::from(tc_ir::OPDEF_REFLECT_FORM)
        || parsed == pathlink::PathBuf::from(tc_ir::OPDEF_REFLECT_LAST_ID)
        || parsed == pathlink::PathBuf::from(tc_ir::OPDEF_REFLECT_SCALARS)
}

pub(super) fn host_request(
    method: http::Method,
    target: pathlink::Link,
    body: Body,
) -> tc_error::TCResult<Request> {
    let target = target.to_string();
    let uri = http::Uri::from_str(&target)
        .map_err(|err| tc_error::TCError::bad_request(err.to_string()))?;
    http::Request::builder()
        .method(method)
        .uri(uri)
        .body(body)
        .map_err(|err| tc_error::TCError::bad_request(err.to_string()))
}

pub(super) async fn decode_host_state_response(
    resp: Response,
) -> tc_error::TCResult<tc_state::State> {
    use futures::stream;

    let status = resp.status();
    let body = hyper::body::to_bytes(resp.into_body())
        .await
        .map_err(|err| tc_error::TCError::internal(err.to_string()))?;

    if status.is_success() {
        if body.is_empty() || body.iter().all(|b| b.is_ascii_whitespace()) {
            return Ok(tc_state::State::None);
        }
        let stream = stream::iter(vec![Ok::<bytes::Bytes, std::io::Error>(body)]);
        return destream_json::try_decode(tc_state::null_transaction(), stream)
            .await
            .map_err(|err| tc_error::TCError::bad_request(err.to_string()));
    }

    let message = if body.is_empty() {
        status.to_string()
    } else {
        String::from_utf8_lossy(&body).to_string()
    };
    Err(status_to_error(status, message))
}

pub(super) fn status_to_error(
    status: hyper::StatusCode,
    message: String,
) -> tc_error::TCError {
    match status {
        hyper::StatusCode::BAD_REQUEST => tc_error::TCError::bad_request(message),
        hyper::StatusCode::CONFLICT => tc_error::TCError::conflict(message),
        hyper::StatusCode::METHOD_NOT_ALLOWED => tc_error::TCError::bad_request(message),
        hyper::StatusCode::NOT_FOUND => tc_error::TCError::not_found(message),
        hyper::StatusCode::UNAUTHORIZED => tc_error::TCError::unauthorized(message),
        _ => tc_error::TCError::internal(message),
    }
}

pub(super) async fn dispatch_host_post(
    handler: Arc<dyn KernelHandler>,
    target: pathlink::Link,
    params: tc_ir::Map<tc_state::State>,
) -> tc_error::TCResult<tc_state::State> {
    let stream = destream_json::encode(params)
        .map_err(|err| tc_error::TCError::bad_request(err.to_string()))?;
    let body = Body::wrap_stream(stream.map_err(|err| std::io::Error::other(err.to_string())));
    let req = host_request(http::Method::POST, target, body)?;
    let resp = handler.call(req).await;
    decode_host_state_response(resp).await
}
