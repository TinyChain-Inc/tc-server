use std::sync::Arc;

use futures::future::BoxFuture;
use std::fmt;
use tc_ir::TxnId;

use crate::egress::EgressPolicy;
use crate::library::{LibraryHandlers, LibraryRuntime};
use crate::txn_server::TxnServer;
use crate::uri::{component_root, normalize_path};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Method {
    Get,
    Put,
    Post,
    Delete,
}

impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Put => "PUT",
            Method::Post => "POST",
            Method::Delete => "DELETE",
        }
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub trait KernelHandler<Request, Response>: Send + Sync + 'static {
    fn call(&self, req: Request) -> BoxFuture<'static, Response>;
}

impl<F, Fut, Request, Response> KernelHandler<Request, Response> for F
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: futures::Future<Output = Response> + Send + 'static,
{
    fn call(&self, req: Request) -> BoxFuture<'static, Response> {
        Box::pin((self)(req))
    }
}

pub struct Kernel<Request, Response> {
    lib_get_handler: Arc<dyn KernelHandler<Request, Response>>,
    lib_put_handler: Arc<dyn KernelHandler<Request, Response>>,
    lib_route_handler: Option<Arc<dyn KernelHandler<Request, Response>>>,
    service_handler: Arc<dyn KernelHandler<Request, Response>>,
    kernel_handler: Arc<dyn KernelHandler<Request, Response>>,
    health_handler: Arc<dyn KernelHandler<Request, Response>>,
    txn_manager: crate::txn::TxnManager,
    txn_server: TxnServer,
    egress: EgressPolicy,
    library_module: Option<Arc<LibraryRuntime<Request, Response>>>,
    rpc_gateway: Option<Arc<dyn crate::gateway::RpcGateway>>,
    token_verifier: Arc<dyn crate::auth::TokenVerifier>,
}

impl<Request, Response> Kernel<Request, Response>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
    pub fn builder() -> KernelBuilder<Request, Response> {
        KernelBuilder::new()
    }

    pub fn dispatch(
        &self,
        method: Method,
        path: &str,
        req: Request,
    ) -> Option<BoxFuture<'static, Response>> {
        if path.starts_with("/lib/")
            && let Some(handler) = &self.lib_route_handler
        {
            return Some(handler.call(req));
        }

        if path == "/service" || path.starts_with("/service/") {
            return Some(self.service_handler.call(req));
        }

        match (method, path) {
            (Method::Get, "/lib") => Some(self.lib_get_handler.call(req)),
            (Method::Put, "/lib") => Some(self.lib_put_handler.call(req)),
            (Method::Get, "/host/metrics") => Some(self.kernel_handler.call(req)),
            (Method::Get, "/host/public_key") => Some(self.kernel_handler.call(req)),
            (Method::Get, "/healthz") => Some(self.health_handler.call(req)),
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

    pub fn resolve_op(
        &self,
        txn_id: TxnId,
        bearer_token: Option<String>,
        op: tc_ir::OpRef,
    ) -> BoxFuture<'static, tc_error::TCResult<crate::gateway::RpcResponse>> {
        use futures::FutureExt;
        use tc_error::TCError;

        let gateway = match self.rpc_gateway.as_ref() {
            Some(gateway) => Arc::clone(gateway),
            None => {
                return futures::future::ready(Err(TCError::bad_gateway(
                    "no RPC gateway configured",
                )))
                .boxed();
            }
        };

        let module = match self.library_module.as_ref() {
            Some(module) => Arc::clone(module),
            None => {
                return futures::future::ready(Err(TCError::unauthorized(
                    "no library manifest loaded (egress is default-deny)",
                )))
                .boxed();
            }
        };

        let egress = self.egress.clone();
        let token_verifier = Arc::clone(&self.token_verifier);
        let txn_manager = self.txn_manager.clone();
        let txn_server = self.txn_server.clone();

        async move {
            fn encode_request_body(scalar: tc_ir::Scalar) -> Vec<u8> {
                match scalar {
                    tc_ir::Scalar::Value(tc_value::Value::None) => Vec::new(),
                    tc_ir::Scalar::Value(tc_value::Value::String(s)) => {
                        serde_json::to_vec(&s).unwrap_or_default()
                    }
                    tc_ir::Scalar::Value(tc_value::Value::Number(n)) => {
                        // Without a serde-backed `Number` encoding in v2 staging, fall back to a
                        // JSON string representation.
                        serde_json::to_vec(&n.to_string()).unwrap_or_default()
                    }
                    // For now, only value payloads are forwarded as request bodies.
                    tc_ir::Scalar::Ref(_) => Vec::new(),
                }
            }

            let (method, target, body) = match op {
                tc_ir::OpRef::Get((tc_ir::Subject::Link(link), key)) => {
                    (Method::Get, link.to_string(), encode_request_body(key))
                }
                tc_ir::OpRef::Put((tc_ir::Subject::Link(link), _key, value)) => {
                    (Method::Put, link.to_string(), encode_request_body(value))
                }
                tc_ir::OpRef::Post((tc_ir::Subject::Link(link), _params)) => {
                    (Method::Post, link.to_string(), Vec::new())
                }
                tc_ir::OpRef::Delete((tc_ir::Subject::Link(link), key)) => {
                    (Method::Delete, link.to_string(), encode_request_body(key))
                }
                _ => {
                    return Err(TCError::bad_request(
                        "cannot resolve OpRef subject Ref without a scope",
                    ));
                }
            };

            let bearer_token = match bearer_token {
                Some(token) => {
                    let ctx = token_verifier
                        .verify(token)
                        .await
                        .map_err(|_| TCError::unauthorized("invalid bearer token"))?;

                    let owner_id = if ctx.claims.is_empty() {
                        ctx.owner_id.clone()
                    } else {
                        txn_owner_id_from_token(txn_id, &ctx)
                            .map_err(|_| TCError::unauthorized("invalid bearer token"))?
                    };

                    match txn_manager.interpret_request(
                        Some(txn_id),
                        Some(&owner_id),
                        Some(&ctx.bearer_token),
                    ) {
                        Ok(crate::txn::TxnFlow::Begin(handle))
                        | Ok(crate::txn::TxnFlow::Use(handle)) => txn_server.touch(handle.id()),
                        Err(crate::txn::TxnError::NotFound) => {
                            return Err(TCError::bad_request("unknown transaction id"));
                        }
                        Err(crate::txn::TxnError::Unauthorized) => {
                            return Err(TCError::unauthorized("unauthorized transaction owner"));
                        }
                    };

                    let claim = token_claim_for_target(method, &target);
                    let ctx = match claim {
                        Some(claim) => {
                            let _ = txn_manager.record_claim(&txn_id, claim.clone());
                            token_verifier
                                .grant(ctx, claim)
                                .await
                                .map_err(|_| TCError::unauthorized("invalid bearer token"))?
                        }
                        None => ctx,
                    };

                    Some(ctx.bearer_token)
                }
                None => txn_manager.bearer_token(&txn_id),
            };

            let schema = module.state().schema();
            let resolved = egress.resolve_target(&schema, &target)?;

            gateway
                .request(method, resolved, txn_id, bearer_token, body)
                .await
                .map_err(|err| match err {
                    crate::gateway::RpcError::InvalidTarget(msg) => TCError::bad_request(msg),
                    crate::gateway::RpcError::Transport(msg) => TCError::bad_gateway(msg),
                })
        }
        .boxed()
    }

    pub fn expire_transactions(&self) -> Vec<TxnId> {
        self.expire_transactions_at(std::time::Instant::now())
    }

    fn expire_transactions_at(&self, now: std::time::Instant) -> Vec<TxnId> {
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
    ) -> Result<KernelDispatch<Response>, crate::txn::TxnError>
    where
        F: FnMut(&crate::txn::TxnHandle, &mut Request),
    {
        use crate::txn::TxnFlow;

        let path = normalize_path(path);

        if path == "/healthz" || path == "/host" || path.starts_with("/host/") {
            return Ok(dispatch_or_not_found(self.dispatch(method, path, req)));
        }

        let is_component_root =
            component_root(path).is_some_and(|component_root| component_root == path);
        let (owner_id, bearer_token) = match (txn_id, token) {
            (Some(txn_id), Some(token)) if !token.claims.is_empty() => {
                let owner_id = txn_owner_id_from_token(txn_id, token)?;
                (Some(owner_id), Some(token.bearer_token.to_string()))
            }
            (_, Some(token)) => (
                Some(token.owner_id.to_string()),
                Some(token.bearer_token.to_string()),
            ),
            _ => (None, None),
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
                bind_txn(&handle, &mut req);
                dispatch_or_not_found(self.dispatch(method, path, req))
            }
            TxnFlow::Use(handle) => {
                self.txn_server.touch(handle.id());
                bind_txn(&handle, &mut req);
                dispatch_or_not_found(self.dispatch(method, path, req))
            }
        };

        Ok(dispatch)
    }
}

fn token_claim_for_target(method: Method, target: &str) -> Option<tc_ir::Claim> {
    use std::str::FromStr;

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

fn txn_owner_id_from_token(
    txn_id: TxnId,
    token: &crate::auth::TokenContext,
) -> Result<String, crate::txn::TxnError> {
    use std::str::FromStr;

    let txn_link = format!("/txn/{txn_id}");
    let txn_link =
        pathlink::Link::from_str(&txn_link).map_err(|_| crate::txn::TxnError::Unauthorized)?;

    let mut owner: Option<(&str, &str)> = None;
    let mut lock: Option<(&str, &str)> = None;

    for (host, actor_id, claim) in &token.claims {
        if claim.link != txn_link {
            if claim.link.to_string().starts_with("/txn/") {
                return Err(crate::txn::TxnError::Unauthorized);
            }
            continue;
        }

        if claim.mask.has(umask::USER_EXEC) {
            if owner.is_some() {
                return Err(crate::txn::TxnError::Unauthorized);
            }
            owner = Some((host.as_str(), actor_id.as_str()));
        }

        if claim.mask.has(umask::USER_WRITE) {
            if lock.is_some() {
                return Err(crate::txn::TxnError::Unauthorized);
            }
            lock = Some((host.as_str(), actor_id.as_str()));
        }
    }

    if let Some(lock) = lock {
        if let Some(owner) = owner {
            if owner != lock {
                return Err(crate::txn::TxnError::Unauthorized);
            }
        } else {
            return Err(crate::txn::TxnError::Unauthorized);
        }
    }

    let Some((host, actor_id)) = owner else {
        return Err(crate::txn::TxnError::Unauthorized);
    };

    Ok(format!("{host}::{actor_id}"))
}

fn dispatch_or_not_found<Response>(
    fut: Option<BoxFuture<'static, Response>>,
) -> KernelDispatch<Response> {
    match fut {
        Some(fut) => KernelDispatch::Response(fut),
        None => KernelDispatch::NotFound,
    }
}

pub enum KernelDispatch<Response> {
    Response(BoxFuture<'static, Response>),
    Finalize {
        commit: bool,
        result: Result<(), crate::txn::TxnError>,
    },
    NotFound,
}

impl<Request, Response> Clone for Kernel<Request, Response>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
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
        }
    }
}

pub struct KernelBuilder<Request, Response> {
    lib_get_handler: Option<Arc<dyn KernelHandler<Request, Response>>>,
    lib_put_handler: Option<Arc<dyn KernelHandler<Request, Response>>>,
    lib_route_handler: Option<Arc<dyn KernelHandler<Request, Response>>>,
    service_handler: Option<Arc<dyn KernelHandler<Request, Response>>>,
    kernel_handler: Option<Arc<dyn KernelHandler<Request, Response>>>,
    health_handler: Option<Arc<dyn KernelHandler<Request, Response>>>,
    txn_manager: crate::txn::TxnManager,
    txn_server: TxnServer,
    egress: EgressPolicy,
    library_module: Option<Arc<LibraryRuntime<Request, Response>>>,
    rpc_gateway: Option<Arc<dyn crate::gateway::RpcGateway>>,
    token_verifier: Arc<dyn crate::auth::TokenVerifier>,
}

impl<Request: Send + 'static, Response: Send + 'static> Default
    for KernelBuilder<Request, Response>
{
    fn default() -> Self {
        Self {
            lib_get_handler: None,
            lib_put_handler: None,
            lib_route_handler: None,
            service_handler: None,
            kernel_handler: None,
            health_handler: None,
            txn_manager: crate::txn::TxnManager::new(),
            txn_server: TxnServer::default(),
            egress: EgressPolicy::default(),
            library_module: None,
            rpc_gateway: None,
            token_verifier: crate::auth::default_token_verifier(),
        }
    }
}

impl<Request: Send + 'static, Response: Send + 'static> KernelBuilder<Request, Response> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_lib_handler<H>(mut self, handler: H) -> Self
    where
        H: KernelHandler<Request, Response>,
    {
        self.lib_get_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_lib_put_handler<H>(mut self, handler: H) -> Self
    where
        H: KernelHandler<Request, Response>,
    {
        self.lib_put_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_service_handler<H>(mut self, handler: H) -> Self
    where
        H: KernelHandler<Request, Response>,
    {
        self.service_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_lib_route_handler<H>(mut self, handler: H) -> Self
    where
        H: KernelHandler<Request, Response>,
    {
        self.lib_route_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_kernel_handler<H>(mut self, handler: H) -> Self
    where
        H: KernelHandler<Request, Response>,
    {
        self.kernel_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_health_handler<H>(mut self, handler: H) -> Self
    where
        H: KernelHandler<Request, Response>,
    {
        self.health_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_library_module(
        mut self,
        module: Arc<LibraryRuntime<Request, Response>>,
        handlers: LibraryHandlers<Request, Response>,
    ) -> Self {
        self.library_module = Some(module);
        self.lib_get_handler = Some(handlers.get_handler());
        self.lib_put_handler = Some(handlers.put_handler());
        self.lib_route_handler = handlers.route_handler();
        self
    }

    pub fn with_host_id(mut self, host_id: impl Into<String>) -> Self {
        self.txn_manager = crate::txn::TxnManager::with_host_id(host_id.into());
        self
    }

    pub fn with_txn_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.txn_server = TxnServer::new(ttl);
        self
    }

    pub fn with_egress_policy(mut self, policy: EgressPolicy) -> Self {
        self.egress = policy;
        self
    }

    pub fn with_dependency_route(
        mut self,
        dependency_root: impl Into<String>,
        authority: impl Into<String>,
    ) -> Self {
        self.egress.route_dependency(dependency_root, authority);
        self
    }

    pub fn with_rpc_gateway<G>(mut self, gateway: G) -> Self
    where
        G: crate::gateway::RpcGateway,
    {
        self.rpc_gateway = Some(Arc::new(gateway));
        self
    }

    pub fn with_token_verifier<V>(mut self, verifier: V) -> Self
    where
        V: crate::auth::TokenVerifier,
    {
        self.token_verifier = Arc::new(verifier);
        self
    }

    #[cfg(feature = "rjwt-token")]
    pub fn with_rjwt_token_verifier(
        self,
        resolver: Arc<dyn crate::auth::RjwtActorResolver>,
    ) -> Self {
        self.with_token_verifier(crate::auth::RjwtTokenVerifier::new(resolver))
    }

    #[cfg(feature = "rjwt-token")]
    pub fn with_rjwt_keyring_token_verifier(
        self,
        keyring: crate::auth::KeyringActorResolver,
    ) -> Self {
        self.with_rjwt_token_verifier(Arc::new(keyring))
    }

    #[cfg(feature = "http-client")]
    pub fn with_http_rpc_gateway(self) -> Self {
        self.with_rpc_gateway(crate::http_client::HttpRpcGateway::new())
    }

    pub fn finish(self) -> Kernel<Request, Response> {
        if let Some(module) = &self.library_module
            && let Err(err) = module.hydrate_from_storage()
        {
            panic!("failed to hydrate library storage: {err}");
        }

        Kernel {
            lib_get_handler: self
                .lib_get_handler
                .unwrap_or_else(|| stub_handler("lib GET")),
            lib_put_handler: self
                .lib_put_handler
                .unwrap_or_else(|| stub_handler("lib PUT")),
            lib_route_handler: self.lib_route_handler,
            service_handler: self
                .service_handler
                .unwrap_or_else(|| stub_handler("service")),
            kernel_handler: self
                .kernel_handler
                .unwrap_or_else(|| stub_handler("host/metrics")),
            health_handler: self
                .health_handler
                .unwrap_or_else(|| stub_handler("healthz")),
            txn_manager: self.txn_manager,
            txn_server: self.txn_server,
            egress: self.egress,
            library_module: self.library_module,
            rpc_gateway: self.rpc_gateway,
            token_verifier: self.token_verifier,
        }
    }
}

fn stub_handler<Request: Send + 'static, Response: Send + 'static>(
    label: &str,
) -> Arc<dyn KernelHandler<Request, Response>> {
    let label = label.to_string();
    Arc::new(move |_req: Request| {
        let label = label.clone();
        async move { panic!("stub handler {label} not implemented") }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use std::sync::{Arc, Mutex};
    use tc_ir::{LibrarySchema, NetworkTime};

    fn ok_handler() -> impl KernelHandler<(), ()> {
        |_req: ()| async {}.boxed()
    }

    #[test]
    fn enforces_owner_claim_for_structured_tokens() {
        use std::str::FromStr;

        use tc_ir::Claim;
        use umask::USER_EXEC;

        let kernel: Kernel<(), ()> = Kernel::builder().with_lib_handler(ok_handler()).finish();
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(1), 1).with_trace([0; 32]);
        let txn_claim = Claim::new(
            pathlink::Link::from_str(&format!("/txn/{txn_id}")).expect("txn claim link"),
            USER_EXEC,
        );

        let token_a = crate::auth::TokenContext {
            owner_id: "ignored".to_string(),
            bearer_token: "a".to_string(),
            claims: vec![(
                "host-a".to_string(),
                "actor-a".to_string(),
                txn_claim.clone(),
            )],
        };

        let token_b = crate::auth::TokenContext {
            owner_id: "ignored".to_string(),
            bearer_token: "b".to_string(),
            claims: vec![("host-b".to_string(), "actor-b".to_string(), txn_claim)],
        };

        let _ = kernel
            .route_request(
                Method::Get,
                "/lib",
                (),
                Some(txn_id),
                true,
                Some(&token_a),
                |_txn, _req| {},
            )
            .expect("begin via claimed token");

        let result = kernel.route_request(
            Method::Get,
            "/lib",
            (),
            Some(txn_id),
            true,
            Some(&token_b),
            |_txn, _req| {},
        );
        assert!(matches!(result, Err(crate::txn::TxnError::Unauthorized)));
    }

    #[cfg(feature = "rjwt-token")]
    #[test]
    fn verifies_rjwt_and_pins_owner_by_txn_claim() {
        use std::str::FromStr;

        use crate::auth::TokenVerifier;
        use rjwt::Token;
        use tc_ir::Claim;
        use tc_value::Value;
        use umask::USER_EXEC;

        let kernel: Kernel<(), ()> = Kernel::builder().with_lib_handler(ok_handler()).finish();
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(1), 1).with_trace([0; 32]);

        let host = pathlink::Link::from_str("/host").expect("host link");
        let actor_a = rjwt::Actor::new(Value::from("actor-a"));
        let resolver =
            crate::auth::KeyringActorResolver::default().with_actor(host.clone(), actor_a.clone());
        let verifier = crate::auth::RjwtTokenVerifier::new(Arc::new(resolver));

        let claim = Claim::new(
            pathlink::Link::from_str(&format!("/txn/{txn_id}")).expect("txn claim"),
            USER_EXEC,
        );
        let token = Token::new(
            host.clone(),
            std::time::SystemTime::now(),
            std::time::Duration::from_secs(30),
            actor_a.id().clone(),
            claim,
        );
        let signed = actor_a.sign_token(token).expect("signed token");

        let token_ctx =
            futures::executor::block_on(TokenVerifier::verify(&verifier, signed.into_jwt()))
                .expect("verified token");

        let result = kernel.route_request(
            Method::Get,
            "/lib",
            (),
            Some(txn_id),
            true,
            Some(&token_ctx),
            |_txn, _req| {},
        );
        assert!(result.is_ok());

        let actor_b = rjwt::Actor::new(Value::from("actor-b"));
        let resolver_b =
            crate::auth::KeyringActorResolver::default().with_actor(host.clone(), actor_b.clone());
        let verifier_b = crate::auth::RjwtTokenVerifier::new(Arc::new(resolver_b));
        let claim_b = Claim::new(
            pathlink::Link::from_str(&format!("/txn/{txn_id}")).expect("txn claim"),
            USER_EXEC,
        );
        let token_b = Token::new(
            host,
            std::time::SystemTime::now(),
            std::time::Duration::from_secs(30),
            actor_b.id().clone(),
            claim_b,
        );
        let signed_b = actor_b.sign_token(token_b).expect("signed token");
        let token_ctx_b =
            futures::executor::block_on(TokenVerifier::verify(&verifier_b, signed_b.into_jwt()))
                .expect("verified token");

        let result = kernel.route_request(
            Method::Get,
            "/lib",
            (),
            Some(txn_id),
            true,
            Some(&token_ctx_b),
            |_txn, _req| {},
        );
        assert!(matches!(result, Err(crate::txn::TxnError::Unauthorized)));
    }

    #[test]
    fn tracks_active_txns_and_expires_them() {
        let kernel: Kernel<(), ()> = Kernel::builder()
            .with_txn_ttl(std::time::Duration::from_secs(10))
            .with_lib_handler(ok_handler())
            .finish();

        let dispatch = kernel
            .route_request(Method::Get, "/lib", (), None, true, None, |_txn, _req| {})
            .expect("dispatch");

        if let KernelDispatch::Response(fut) = dispatch {
            futures::executor::block_on(fut);
        } else {
            panic!("expected response dispatch");
        }

        let pending = kernel.txn_manager().pending_ids();
        assert_eq!(pending.len(), 1);
        let txn_id = pending[0];
        assert!(kernel.txn_server().contains(&txn_id));

        let expired = kernel
            .expire_transactions_at(std::time::Instant::now() + std::time::Duration::from_secs(11));
        assert_eq!(expired, vec![txn_id]);
        assert!(kernel.txn_manager().pending_ids().is_empty());
        assert!(!kernel.txn_server().contains(&txn_id));
    }

    #[derive(Clone, Default)]
    struct MockGateway {
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl crate::gateway::RpcGateway for MockGateway {
        fn request(
            &self,
            _method: Method,
            uri: String,
            _txn_id: TxnId,
            _bearer_token: Option<String>,
            _body: Vec<u8>,
        ) -> BoxFuture<'static, Result<crate::gateway::RpcResponse, crate::gateway::RpcError>>
        {
            self.calls.lock().expect("calls lock").push(uri);
            Box::pin(async move {
                Ok(crate::gateway::RpcResponse {
                    status: 200,
                    headers: vec![],
                    body: vec![],
                })
            })
        }
    }

    #[test]
    fn egress_is_default_deny_without_manifest_dependency() {
        let schema = LibrarySchema::new(
            "/lib/acme/a/1.0.0".parse().expect("schema id"),
            "1.0.0",
            vec![],
        );

        let module = Arc::new(crate::library::LibraryRuntime::new(schema, None, None));
        let handlers = crate::library::LibraryHandlers::without_route(ok_handler(), ok_handler());

        let gateway = MockGateway::default();

        let kernel: Kernel<(), ()> = Kernel::builder()
            .with_library_module(module, handlers)
            .with_dependency_route("/lib", "127.0.0.1:1234")
            .with_rpc_gateway(gateway)
            .finish();

        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(1), 1).with_trace([0; 32]);
        let op = tc_ir::OpRef::Get((
            tc_ir::Subject::Link("http://127.0.0.1:1234/lib".parse().expect("link")),
            tc_ir::Scalar::default(),
        ));

        let err = futures::executor::block_on(kernel.resolve_op(txn_id, None, op))
            .expect_err("expected unauthorized");
        assert_eq!(err.code(), tc_error::ErrorKind::Unauthorized);
    }
}
