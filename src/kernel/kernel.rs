use std::sync::Arc;

use tc_error::TCResult;
use tc_ir::{Public, Scalar, TxnId};

use super::resolver::KernelTxnResolver;
use super::{BoundTransaction, KernelRequest, Method};
use crate::egress::EgressPolicy;
use crate::library::LibraryRegistry;
use crate::txn::TxnServer;
use crate::uri::{component_root, normalize_path};

pub struct Kernel {
    pub(crate) resources: crate::HostResources,
    pub(crate) txn_server: TxnServer,
    pub(crate) egress: EgressPolicy,
    pub(crate) library_module: Option<Arc<LibraryRegistry>>,
    pub(crate) rpc_gateway: Option<Arc<dyn crate::gateway::RpcGateway>>,
    pub(crate) token_verifier: Arc<dyn crate::auth::TokenVerifier>,
}

impl Kernel {
    pub fn builder() -> super::KernelBuilder {
        super::KernelBuilder::new()
    }

    /// Execute a decoded local route without constructing a transport request
    /// or response.
    pub async fn execute(&self, request: KernelRequest) -> TCResult<crate::State> {
        let path = request.path.to_string();
        if path == crate::uri::HOST_AUTH_CONTEXT {
            if request.method != Method::Get {
                return Err(tc_error::TCError::method_not_allowed(
                    request.method.as_str(),
                    &path,
                ));
            }
            return crate::host::auth_context(&request.txn);
        }
        if path.starts_with("/state/") {
            return crate::state::execute(request)
                .await?
                .ok_or_else(|| tc_error::TCError::not_found(path));
        }
        let registry = self
            .library_module
            .as_ref()
            .ok_or_else(|| tc_error::TCError::not_found(path.clone()))?;
        let (routes, route, is_root) = registry
            .resolve_native(&path)
            .ok_or_else(|| tc_error::TCError::not_found(path.clone()))?;
        if is_root {
            return Err(tc_error::TCError::not_found(path));
        }

        execute_native(&routes, &route, request).await
    }

    pub fn resources(&self) -> &crate::HostResources {
        &self.resources
    }

    /// Start the single host-owned transaction expiry worker.
    pub fn start_transaction_expiry(&self, runtime: &tokio::runtime::Handle) {
        self.txn_server.start_expiry(runtime);
    }

    #[cfg(test)]
    pub(crate) fn txn_server(&self) -> &TxnServer {
        &self.txn_server
    }

    pub fn rpc_gateway(&self) -> Option<&Arc<dyn crate::gateway::RpcGateway>> {
        self.rpc_gateway.as_ref()
    }

    pub fn token_verifier(&self) -> &Arc<dyn crate::auth::TokenVerifier> {
        &self.token_verifier
    }

    pub fn library_registry(&self) -> Option<&Arc<LibraryRegistry>> {
        self.library_module.as_ref()
    }

    pub fn with_resolver(&self, handle: crate::txn::TxnHandle) -> crate::txn::TxnHandle {
        handle.with_resolver(self.build_txn_resolver())
    }

    fn build_txn_resolver(&self) -> Arc<dyn crate::gateway::RpcGateway> {
        Arc::new(KernelTxnResolver {
            gateway: self.rpc_gateway.as_ref().map(Arc::clone),
            library_registry: self.library_module.as_ref().map(Arc::clone),
            egress: self.egress.clone(),
            txn_server: self.txn_server.clone(),
        })
    }

    pub(crate) async fn complete_transaction(
        &self,
        txn: crate::txn::TxnHandle,
        outcome: crate::txn::TransactionOutcome,
    ) -> TCResult<()> {
        self.txn_server.complete(txn, outcome).await
    }

    /// Bind the only transaction context used by native execution. Adapters
    /// authenticate and decode transport data, but cannot begin, reuse, or
    /// finalize transactions themselves.
    pub(crate) async fn bind_transaction(
        &self,
        method: Method,
        path: &str,
        body_is_none: bool,
        txn_id: Option<TxnId>,
        token: Option<&crate::auth::TokenContext>,
        deadline: crate::Deadline,
    ) -> TCResult<Option<BoundTransaction>> {
        let path = normalize_path(path);
        let is_component_root =
            component_root(path).is_some_and(|component_root| component_root == path);

        #[allow(clippy::collapsible_if)]
        if body_is_none && is_component_root && matches!(method, Method::Post | Method::Delete) {
            if let Some(txn_id) = txn_id {
                let required = if method == Method::Post {
                    umask::USER_EXEC
                } else {
                    umask::USER_WRITE
                };
                let component = component_root(path).filter(|root| *root != path);
                let outcome = if method == Method::Post {
                    crate::txn::TransactionOutcome::ExplicitCommit
                } else {
                    crate::txn::TransactionOutcome::ExplicitRollback
                };
                self.txn_server
                    .finish_authorized(txn_id, token, component, required, outcome)
                    .await?;
                return Ok(None);
            }
        }

        let component = component_root(path).filter(|root| *root != path);
        let handle = self.txn_server.bind(txn_id, token, component)?;
        let txn = self.with_resolver(handle).with_deadline(deadline);
        Ok(Some(BoundTransaction {
            txn,
            implicit: txn_id.is_none(),
        }))
    }
}

pub(crate) async fn execute_native(
    routes: &crate::ir::IrRoutes,
    route: &[pathlink::PathSegment],
    request: KernelRequest,
) -> TCResult<crate::State> {
    match request.method {
        Method::Get => {
            let key = scalar_body(request.body)?;
            routes.get(&request.txn, route, key).await
        }
        Method::Put => {
            let (key, value) = put_body(request.body)?;
            routes.put(&request.txn, route, key, value).await?;
            Ok(crate::State::None)
        }
        Method::Post => {
            let Some(crate::State::Map(params)) = request.body else {
                return Err(tc_error::TCError::bad_request(
                    "POST route requires a map request",
                ));
            };
            routes.post(&request.txn, route, params).await
        }
        Method::Delete => {
            let key = scalar_body(request.body)?;
            routes.delete(&request.txn, route, key).await?;
            Ok(crate::State::None)
        }
    }
}

fn scalar_body(body: Option<crate::State>) -> TCResult<Scalar> {
    match body.unwrap_or(crate::State::None) {
        crate::State::None => Ok(Scalar::default()),
        crate::State::Scalar(scalar) => Ok(scalar),
        _ => Err(tc_error::TCError::bad_request("expected a scalar request")),
    }
}

fn put_body(body: Option<crate::State>) -> TCResult<(Scalar, crate::State)> {
    let Some(crate::State::Tuple(mut values)) = body else {
        return Err(tc_error::TCError::bad_request(
            "PUT route requires a [key, value] tuple",
        ));
    };
    if values.len() != 2 {
        return Err(tc_error::TCError::bad_request(
            "PUT route requires a [key, value] tuple",
        ));
    }
    let value = values.pop().expect("tuple length checked");
    let key = scalar_body(Some(values.pop().expect("tuple length checked")))?;
    Ok((key, value))
}

impl Clone for Kernel {
    fn clone(&self) -> Self {
        Self {
            resources: self.resources.clone(),
            txn_server: self.txn_server.clone(),
            egress: self.egress.clone(),
            library_module: self.library_module.as_ref().map(Arc::clone),
            rpc_gateway: self.rpc_gateway.as_ref().map(Arc::clone),
            token_verifier: Arc::clone(&self.token_verifier),
        }
    }
}
