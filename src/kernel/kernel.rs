use std::{str::FromStr, sync::Arc};

use futures::future::BoxFuture;
use tc_ir::TxnId;

use crate::egress::EgressPolicy;
use crate::library::LibraryRegistry;
use crate::txn_server::TxnServer;
use crate::uri::{component_root, normalize_path};
use crate::{Request, Response};

use super::dispatch::dispatch_or_not_found;
use super::resolver::{KernelTxnResolver, is_scalar_reflect_path_str};
use super::{KernelDispatch, KernelHandler, Method};

pub struct Kernel {
    pub(crate) lib_get_handler: Arc<dyn KernelHandler>,
    pub(crate) lib_put_handler: Arc<dyn KernelHandler>,
    pub(crate) lib_route_handler: Option<Arc<dyn KernelHandler>>,
    pub(crate) service_handler: Arc<dyn KernelHandler>,
    pub(crate) kernel_handler: Arc<dyn KernelHandler>,
    pub(crate) health_handler: Arc<dyn KernelHandler>,
    pub(crate) txn_manager: crate::txn::TxnManager,
    pub(crate) txn_server: TxnServer,
    pub(crate) egress: EgressPolicy,
    pub(crate) library_module: Option<Arc<LibraryRegistry>>,
    pub(crate) rpc_gateway: Option<Arc<dyn crate::gateway::RpcGateway>>,
    pub(crate) token_verifier: Arc<dyn crate::auth::TokenVerifier>,
    pub(crate) kernel_actor: Option<(pathlink::Link, crate::auth::Actor)>,
}

impl Kernel {
    pub fn builder() -> super::KernelBuilder {
        super::KernelBuilder::new()
    }

    pub fn dispatch(
        &self,
        method: Method,
        path: &str,
        req: Request,
    ) -> Option<BoxFuture<'static, Response>> {
        if path.starts_with("/state/") {
            return Some(self.kernel_handler.call(req));
        }

        if path.starts_with(crate::uri::LIB_ROOT_PREFIX) {
            return self
                .lib_route_handler
                .as_ref()
                .map(|handler| handler.call(req));
        }

        if path == crate::uri::SERVICE_ROOT || path.starts_with(crate::uri::SERVICE_ROOT_PREFIX) {
            return Some(self.service_handler.call(req));
        }

        match (method, path) {
            (Method::Get, crate::uri::LIB_ROOT) => Some(self.lib_get_handler.call(req)),
            (Method::Put, crate::uri::LIB_ROOT) => Some(self.lib_put_handler.call(req)),
            (Method::Get, "/") => Some(self.kernel_handler.call(req)),
            (Method::Get, crate::uri::HOST_METRICS) => Some(self.kernel_handler.call(req)),
            (Method::Get, crate::uri::HOST_PUBLIC_KEY) => Some(self.kernel_handler.call(req)),
            (Method::Get, crate::uri::HOST_LIBRARY_EXPORT) => Some(self.kernel_handler.call(req)),
            (Method::Post, path) if is_scalar_reflect_path_str(path) => {
                Some(self.kernel_handler.call(req))
            }
            (Method::Get, crate::uri::HEALTHZ) => Some(self.health_handler.call(req)),
            _ => None,
        }
    }

    pub fn txn_manager(&self) -> &crate::txn::TxnManager {
        &self.txn_manager
    }

    pub fn txn_server(&self) -> &TxnServer {
        &self.txn_server
    }

    pub fn rpc_gateway(&self) -> Option<&Arc<dyn crate::gateway::RpcGateway>> {
        self.rpc_gateway.as_ref()
    }

    pub fn token_verifier(&self) -> &Arc<dyn crate::auth::TokenVerifier> {
        &self.token_verifier
    }

    pub fn with_resolver(&self, handle: crate::txn::TxnHandle) -> crate::txn::TxnHandle {
        handle.with_resolver(self.build_txn_resolver())
    }

    fn build_txn_resolver(&self) -> Arc<dyn crate::gateway::RpcGateway> {
        Arc::new(KernelTxnResolver {
            gateway: self.rpc_gateway.as_ref().map(Arc::clone),
            library_registry: self.library_module.as_ref().map(Arc::clone),
            egress: self.egress.clone(),
            token_verifier: Arc::clone(&self.token_verifier),
            txn_manager: self.txn_manager.clone(),
            txn_server: self.txn_server.clone(),
            host_handler: Arc::clone(&self.kernel_handler),
        })
    }

    pub fn expire_transactions(&self) -> Vec<TxnId> {
        self.expire_transactions_at(std::time::Instant::now())
    }

    pub(crate) fn expire_transactions_at(&self, now: std::time::Instant) -> Vec<TxnId> {
        let expired = self.txn_server.expire_at(now);
        for txn_id in &expired {
            let _ = self.txn_manager.rollback(*txn_id);
        }
        expired
    }

    #[allow(clippy::too_many_arguments)]
    // This is the internal entrypoint for adapter request routing; splitting this signature would
    // obscure the core dispatch flow without reducing complexity.
    pub fn route_request<F>(
        &self,
        method: Method,
        path: &str,
        mut req: Request,
        txn_id: Option<TxnId>,
        body_is_none: bool,
        token: Option<&crate::auth::TokenContext>,
        mut bind_txn: F,
    ) -> Result<KernelDispatch, crate::txn::TxnError>
    where
        F: FnMut(&crate::txn::TxnHandle, &mut Request),
    {
        use crate::txn::TxnFlow;

        let path = normalize_path(path);

        if path == crate::uri::HEALTHZ
            || path == crate::uri::HOST_ROOT
            || (path.starts_with(crate::uri::HOST_ROOT_PREFIX)
                && path != crate::uri::HOST_LIBRARY_EXPORT)
        {
            return Ok(dispatch_or_not_found(self.dispatch(method, path, req)));
        }

        let is_component_root =
            component_root(path).is_some_and(|component_root| component_root == path);
        let (owner_id, bearer_token, claims) = match (txn_id, token) {
            (Some(txn_id), Some(token)) if !token.claims.is_empty() => {
                let owner_id = crate::txn::owner_id_from_token(txn_id, token)?;
                let claims = token
                    .claims
                    .iter()
                    .map(|(_, _, claim)| claim.clone())
                    .collect::<Vec<_>>();
                (
                    Some(owner_id),
                    Some(token.bearer_token.to_string()),
                    Some(claims),
                )
            }
            (_, Some(token)) => {
                let claims = if token.claims.is_empty() {
                    None
                } else {
                    Some(
                        token
                            .claims
                            .iter()
                            .map(|(_, _, claim)| claim.clone())
                            .collect::<Vec<_>>(),
                    )
                };
                (
                    Some(token.owner_id.to_string()),
                    Some(token.bearer_token.to_string()),
                    claims,
                )
            }
            _ => (None, None, None),
        };
        let owner_id = owner_id.as_deref();
        let bearer_token = bearer_token.as_deref();

        let flow = if txn_id.is_some()
            && body_is_none
            && is_component_root
            && matches!(method, Method::Post | Method::Delete)
        {
            let handle = self
                .txn_manager
                .get_with_owner(&txn_id.expect("txn_id checked above"), owner_id)?;

            let required = match method {
                Method::Post => umask::USER_EXEC,
                Method::Delete => umask::USER_WRITE,
                _ => umask::Mode::new(),
            };
            let handle = match claims {
                Some(claims) => handle.with_claims(claims),
                None => handle,
            };
            if bearer_token.is_none() {
                return Err(crate::txn::TxnError::Unauthorized);
            }

            let txn_link = pathlink::Link::from_str(&format!("/txn/{}", handle.id()))
                .map_err(|_| crate::txn::TxnError::Unauthorized)?;
            if !handle.has_claim(&txn_link, required) {
                return Err(crate::txn::TxnError::Unauthorized);
            }

            self.txn_server.forget(&handle.id());

            return Ok(KernelDispatch::Finalize {
                commit: method == Method::Post,
                result: match method {
                    Method::Post => self.txn_manager.commit(handle.id()),
                    Method::Delete => self.txn_manager.rollback(handle.id()),
                    _ => unreachable!("checked above"),
                },
            });
        } else {
            self.txn_manager
                .interpret_request(txn_id, owner_id, bearer_token)?
        };

        let dispatch = match flow {
            TxnFlow::Begin(handle) => {
                self.txn_server.touch(handle.id());
                let handle = if let Some(claims) = &claims {
                    for claim in claims {
                        let _ = self.txn_manager.record_claim(&handle.id(), claim.clone());
                    }
                    handle.with_claims(claims.clone())
                } else {
                    handle
                };
                let handle = self.with_resolver(handle);
                bind_txn(&handle, &mut req);
                dispatch_or_not_found(self.dispatch(method, path, req))
            }
            TxnFlow::Use(handle) => {
                self.txn_server.touch(handle.id());
                let handle = if let Some(claims) = &claims {
                    for claim in claims {
                        let _ = self.txn_manager.record_claim(&handle.id(), claim.clone());
                    }
                    handle.with_claims(claims.clone())
                } else {
                    handle
                };
                let handle = self.with_resolver(handle);
                bind_txn(&handle, &mut req);
                dispatch_or_not_found(self.dispatch(method, path, req))
            }
        };

        Ok(dispatch)
    }
}

impl Clone for Kernel {
    fn clone(&self) -> Self {
        Self {
            lib_get_handler: Arc::clone(&self.lib_get_handler),
            lib_put_handler: Arc::clone(&self.lib_put_handler),
            lib_route_handler: self.lib_route_handler.as_ref().map(Arc::clone),
            service_handler: Arc::clone(&self.service_handler),
            kernel_handler: Arc::clone(&self.kernel_handler),
            health_handler: Arc::clone(&self.health_handler),
            txn_manager: self.txn_manager.clone(),
            txn_server: self.txn_server.clone(),
            egress: self.egress.clone(),
            library_module: self.library_module.as_ref().map(Arc::clone),
            rpc_gateway: self.rpc_gateway.as_ref().map(Arc::clone),
            token_verifier: Arc::clone(&self.token_verifier),
            kernel_actor: self.kernel_actor.clone(),
        }
    }
}
