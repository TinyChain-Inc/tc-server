use std::str::FromStr;
use std::sync::Arc;

use futures::future::BoxFuture;

use super::Method;
use crate::egress::EgressPolicy;
use crate::library::LibraryRegistry;
use crate::txn::TxnServer;
use crate::{KernelRequest, State};

#[derive(Clone)]
pub(super) struct KernelTxnResolver {
    pub(super) gateway: Option<Arc<dyn crate::gateway::RpcGateway>>,
    pub(super) library_registry: Option<Arc<LibraryRegistry>>,
    pub(super) egress: EgressPolicy,
    pub(super) txn_server: TxnServer,
}

enum OutboundTarget {
    Local(pathlink::Link),
    Remote(pathlink::Link),
}

impl KernelTxnResolver {
    async fn remote_bearer_token(
        &self,
        method: Method,
        target: &str,
        txn: &crate::txn::TxnHandle,
    ) -> tc_error::TCResult<String> {
        let claim = token_claim_for_target(method, target).ok_or_else(|| {
            tc_error::TCError::unauthorized(
                "cross-host dependency target has no canonical component root",
            )
        })?;
        self.txn_server.grant(txn, claim).await
    }

    async fn local_library(
        &self,
        method: Method,
        target: pathlink::Link,
        txn: crate::txn::TxnHandle,
        body: State,
    ) -> tc_error::TCResult<State> {
        let path = target.to_string();
        let registry = self
            .library_registry
            .as_ref()
            .ok_or_else(|| tc_error::TCError::not_found(path.clone()))?;
        let (routes, route, is_root) = registry
            .resolve_native(&path)
            .ok_or_else(|| tc_error::TCError::not_found(path.clone()))?;
        if is_root {
            return Err(tc_error::TCError::not_found(path));
        }

        super::kernel::execute_native(
            &routes,
            &route,
            KernelRequest {
                method,
                path: target,
                body: Some(body),
                txn,
            },
        )
        .await
    }

    async fn local_get(
        &self,
        target: pathlink::Link,
        txn: crate::txn::TxnHandle,
        key: tc_ir::Scalar,
    ) -> tc_error::TCResult<crate::State> {
        if target.to_string() == crate::uri::HOST_AUTH_CONTEXT {
            return crate::host::auth_context(&txn);
        }
        self.local_library(Method::Get, target, txn, State::from_scalar(key))
            .await
    }

    async fn local_put(
        &self,
        target: pathlink::Link,
        txn: crate::txn::TxnHandle,
        key: tc_ir::Scalar,
        value: crate::State,
    ) -> tc_error::TCResult<()> {
        self.local_library(
            Method::Put,
            target,
            txn,
            State::Tuple(vec![State::from_scalar(key), value]),
        )
        .await
        .map(|_| ())
    }

    async fn local_post(
        &self,
        target: pathlink::Link,
        txn: crate::txn::TxnHandle,
        params: tc_ir::Map<crate::State>,
    ) -> tc_error::TCResult<crate::State> {
        self.local_library(Method::Post, target, txn, State::Map(params))
            .await
    }

    async fn local_delete(
        &self,
        target: pathlink::Link,
        txn: crate::txn::TxnHandle,
        key: tc_ir::Scalar,
    ) -> tc_error::TCResult<()> {
        self.local_library(Method::Delete, target, txn, State::from_scalar(key))
            .await
            .map(|_| ())
    }

    async fn prepare_outbound(
        &self,
        method: Method,
        target: &pathlink::Link,
        txn: &crate::txn::TxnHandle,
    ) -> tc_error::TCResult<(OutboundTarget, crate::txn::TxnHandle)> {
        let target_str = target.to_string();
        let registry = self.library_registry.clone().ok_or_else(|| {
            tc_error::TCError::unauthorized("no library manifest loaded (egress is default-deny)")
        })?;
        let schema = registry.schema_for_txn(txn)?;
        let target_uri: http::Uri = target_str
            .parse()
            .map_err(|err| tc_error::TCError::bad_request(format!("invalid target URI: {err}")))?;
        let target_path = crate::uri::normalize_path(target_uri.path());
        let target_root = crate::uri::component_root(target_path).ok_or_else(|| {
            tc_error::TCError::bad_request(
                "egress target must be a TinyChain component root or subpath",
            )
        })?;

        if target_root == crate::uri::HOST_ROOT {
            if target_uri.authority().is_some() {
                return Err(tc_error::TCError::unauthorized(
                    "cross-host /host access is not allowed from library routes",
                ));
            }

            return Ok((OutboundTarget::Local(target.clone()), txn.clone()));
        }

        let target_root_link = pathlink::Link::from_str(target_root)
            .map_err(|err| tc_error::TCError::bad_request(err.to_string()))?;
        let dependency_allowed = schema.id() == &target_root_link
            || schema
                .dependencies()
                .iter()
                .any(|dep| dep == &target_root_link);

        if !dependency_allowed {
            return Err(tc_error::TCError::unauthorized(format!(
                "unauthorized dependency {target_root}"
            )));
        }

        let resolved = if target_uri.authority().is_none() && registry.has_route_root(target_root) {
            OutboundTarget::Local(
                pathlink::Link::from_str(&target_str)
                    .map_err(|err| tc_error::TCError::bad_request(err.to_string()))?,
            )
        } else {
            let resolved = self.egress.resolve_target(&schema, &target_str)?;
            let resolved = pathlink::Link::from_str(&resolved)
                .map_err(|err| tc_error::TCError::bad_request(err.to_string()))?;
            OutboundTarget::Remote(resolved)
        };

        let outbound_txn = match resolved {
            OutboundTarget::Remote(_) => {
                let token = self.remote_bearer_token(method, &target_str, txn).await?;
                txn.with_bearer_token(token)
            }
            OutboundTarget::Local(_) => txn.clone(),
        };
        Ok((resolved, outbound_txn))
    }
}

impl crate::gateway::RpcGateway for KernelTxnResolver {
    fn get(
        &self,
        target: pathlink::Link,
        txn: crate::txn::TxnHandle,
        key: tc_ir::Scalar,
    ) -> BoxFuture<'static, tc_error::TCResult<crate::State>> {
        let resolver = self.clone();
        Box::pin(async move {
            let (resolved, outbound_txn) = resolver
                .prepare_outbound(Method::Get, &target, &txn)
                .await?;
            match resolved {
                OutboundTarget::Local(target) => {
                    resolver.local_get(target, outbound_txn, key).await
                }
                OutboundTarget::Remote(target) => {
                    let _permit = outbound_txn
                        .resources()
                        .admit_outbound(outbound_txn.deadline())
                        .await?;
                    let gateway = resolver.gateway.clone().ok_or_else(|| {
                        tc_error::TCError::bad_gateway("no RPC gateway configured")
                    })?;
                    gateway.get(target, outbound_txn, key).await
                }
            }
        })
    }

    fn put(
        &self,
        target: pathlink::Link,
        txn: crate::txn::TxnHandle,
        key: tc_ir::Scalar,
        value: crate::State,
    ) -> BoxFuture<'static, tc_error::TCResult<()>> {
        let resolver = self.clone();
        Box::pin(async move {
            let (resolved, outbound_txn) = resolver
                .prepare_outbound(Method::Put, &target, &txn)
                .await?;
            match resolved {
                OutboundTarget::Local(target) => {
                    resolver.local_put(target, outbound_txn, key, value).await
                }
                OutboundTarget::Remote(target) => {
                    let _permit = outbound_txn
                        .resources()
                        .admit_outbound(outbound_txn.deadline())
                        .await?;
                    let gateway = resolver.gateway.clone().ok_or_else(|| {
                        tc_error::TCError::bad_gateway("no RPC gateway configured")
                    })?;
                    gateway.put(target, outbound_txn, key, value).await
                }
            }
        })
    }

    fn post(
        &self,
        target: pathlink::Link,
        txn: crate::txn::TxnHandle,
        params: tc_ir::Map<crate::State>,
    ) -> BoxFuture<'static, tc_error::TCResult<crate::State>> {
        let resolver = self.clone();
        Box::pin(async move {
            if is_scalar_reflect_path(&target) {
                return crate::reflect::execute(KernelRequest {
                    method: Method::Post,
                    path: target,
                    body: Some(State::Map(params)),
                    txn: txn.clone(),
                })
                .await;
            }
            let (resolved, outbound_txn) = resolver
                .prepare_outbound(Method::Post, &target, &txn)
                .await?;
            match resolved {
                OutboundTarget::Local(target) => {
                    resolver.local_post(target, outbound_txn, params).await
                }
                OutboundTarget::Remote(target) => {
                    let _permit = outbound_txn
                        .resources()
                        .admit_outbound(outbound_txn.deadline())
                        .await?;
                    let gateway = resolver.gateway.clone().ok_or_else(|| {
                        tc_error::TCError::bad_gateway("no RPC gateway configured")
                    })?;
                    gateway.post(target, outbound_txn, params).await
                }
            }
        })
    }

    fn delete(
        &self,
        target: pathlink::Link,
        txn: crate::txn::TxnHandle,
        key: tc_ir::Scalar,
    ) -> BoxFuture<'static, tc_error::TCResult<()>> {
        let resolver = self.clone();
        Box::pin(async move {
            let (resolved, outbound_txn) = resolver
                .prepare_outbound(Method::Delete, &target, &txn)
                .await?;
            match resolved {
                OutboundTarget::Local(target) => {
                    resolver.local_delete(target, outbound_txn, key).await
                }
                OutboundTarget::Remote(target) => {
                    let _permit = outbound_txn
                        .resources()
                        .admit_outbound(outbound_txn.deadline())
                        .await?;
                    let gateway = resolver.gateway.clone().ok_or_else(|| {
                        tc_error::TCError::bad_gateway("no RPC gateway configured")
                    })?;
                    gateway.delete(target, outbound_txn, key).await
                }
            }
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
        || path == pathlink::PathBuf::from(tc_ir::SCALAR_REFLECT_REF_PARTS)
        || path == pathlink::PathBuf::from(tc_ir::OPDEF_REFLECT_FORM)
        || path == pathlink::PathBuf::from(tc_ir::OPDEF_REFLECT_LAST_ID)
        || path == pathlink::PathBuf::from(tc_ir::OPDEF_REFLECT_SCALARS)
}
