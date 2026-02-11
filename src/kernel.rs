use std::{str::FromStr, sync::Arc};

use futures::{TryStreamExt, future::BoxFuture};
use std::fmt;
use tc_ir::TxnId;

use crate::egress::EgressPolicy;
use crate::library::{LibraryHandlers, LibraryRegistry};
use crate::txn_server::TxnServer;
use crate::uri::{component_root, normalize_path};
use crate::{Body, Request, Response};

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

pub trait KernelHandler: Send + Sync + 'static {
    fn call(&self, req: Request) -> BoxFuture<'static, Response>;
}

impl<F, Fut> KernelHandler for F
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: futures::Future<Output = Response> + Send + 'static,
{
    fn call(&self, req: Request) -> BoxFuture<'static, Response> {
        Box::pin((self)(req))
    }
}

pub struct Kernel {
    lib_get_handler: Arc<dyn KernelHandler>,
    lib_put_handler: Arc<dyn KernelHandler>,
    lib_route_handler: Option<Arc<dyn KernelHandler>>,
    service_handler: Arc<dyn KernelHandler>,
    kernel_handler: Arc<dyn KernelHandler>,
    health_handler: Arc<dyn KernelHandler>,
    txn_manager: crate::txn::TxnManager,
    txn_server: TxnServer,
    egress: EgressPolicy,
    library_module: Option<Arc<LibraryRegistry>>,
    rpc_gateway: Option<Arc<dyn crate::gateway::RpcGateway>>,
    token_verifier: Arc<dyn crate::auth::TokenVerifier>,
    kernel_actor: Option<(pathlink::Link, crate::auth::Actor)>,
}

impl Kernel {
    pub fn builder() -> KernelBuilder {
        KernelBuilder::new()
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

fn dispatch_or_not_found(fut: Option<BoxFuture<'static, Response>>) -> KernelDispatch {
    match fut {
        Some(fut) => KernelDispatch::Response(fut),
        None => KernelDispatch::NotFound,
    }
}

pub enum KernelDispatch {
    Response(BoxFuture<'static, Response>),
    Finalize {
        commit: bool,
        result: Result<(), crate::txn::TxnError>,
    },
    NotFound,
}

#[derive(Clone)]
struct KernelTxnResolver {
    gateway: Option<Arc<dyn crate::gateway::RpcGateway>>,
    library_registry: Option<Arc<LibraryRegistry>>,
    egress: EgressPolicy,
    token_verifier: Arc<dyn crate::auth::TokenVerifier>,
    txn_manager: crate::txn::TxnManager,
    txn_server: TxnServer,
    host_handler: Arc<dyn KernelHandler>,
}

impl KernelTxnResolver {
    async fn prepare_outbound(
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

fn is_scalar_reflect_path(target: &pathlink::Link) -> bool {
    let Ok(path) = pathlink::PathBuf::from_str(&target.to_string()) else {
        return false;
    };
    path == pathlink::PathBuf::from(tc_ir::SCALAR_REFLECT_CLASS)
        || path == pathlink::PathBuf::from(tc_ir::SCALAR_REFLECT_IF_PARTS)
        || path == pathlink::PathBuf::from(tc_ir::OPDEF_REFLECT_FORM)
        || path == pathlink::PathBuf::from(tc_ir::OPDEF_REFLECT_LAST_ID)
        || path == pathlink::PathBuf::from(tc_ir::OPDEF_REFLECT_SCALARS)
}

fn is_scalar_reflect_path_str(path: &str) -> bool {
    let Ok(parsed) = pathlink::PathBuf::from_str(path) else {
        return false;
    };
    parsed == pathlink::PathBuf::from(tc_ir::SCALAR_REFLECT_CLASS)
        || parsed == pathlink::PathBuf::from(tc_ir::SCALAR_REFLECT_IF_PARTS)
        || parsed == pathlink::PathBuf::from(tc_ir::OPDEF_REFLECT_FORM)
        || parsed == pathlink::PathBuf::from(tc_ir::OPDEF_REFLECT_LAST_ID)
        || parsed == pathlink::PathBuf::from(tc_ir::OPDEF_REFLECT_SCALARS)
}

fn host_request(
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

async fn decode_host_state_response(resp: Response) -> tc_error::TCResult<tc_state::State> {
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

fn status_to_error(status: hyper::StatusCode, message: String) -> tc_error::TCError {
    match status {
        hyper::StatusCode::BAD_REQUEST => tc_error::TCError::bad_request(message),
        hyper::StatusCode::CONFLICT => tc_error::TCError::conflict(message),
        hyper::StatusCode::METHOD_NOT_ALLOWED => tc_error::TCError::bad_request(message),
        hyper::StatusCode::NOT_FOUND => tc_error::TCError::not_found(message),
        hyper::StatusCode::UNAUTHORIZED => tc_error::TCError::unauthorized(message),
        _ => tc_error::TCError::internal(message),
    }
}

async fn dispatch_host_post(
    handler: Arc<dyn KernelHandler>,
    target: pathlink::Link,
    params: tc_ir::Map<tc_state::State>,
) -> tc_error::TCResult<tc_state::State> {
    let stream = destream_json::encode(params)
        .map_err(|err| tc_error::TCError::bad_request(err.to_string()))?;
    let body = Body::wrap_stream(
        stream.map_err(|err| std::io::Error::other(err.to_string())),
    );
    let req = host_request(http::Method::POST, target, body)?;
    let resp = handler.call(req).await;
    decode_host_state_response(resp).await
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

pub struct KernelBuilder {
    lib_get_handler: Option<Arc<dyn KernelHandler>>,
    lib_put_handler: Option<Arc<dyn KernelHandler>>,
    lib_route_handler: Option<Arc<dyn KernelHandler>>,
    service_handler: Option<Arc<dyn KernelHandler>>,
    kernel_handler: Option<Arc<dyn KernelHandler>>,
    health_handler: Option<Arc<dyn KernelHandler>>,
    txn_manager: crate::txn::TxnManager,
    txn_server: TxnServer,
    egress: EgressPolicy,
    library_module: Option<Arc<LibraryRegistry>>,
    rpc_gateway: Option<Arc<dyn crate::gateway::RpcGateway>>,
    token_verifier: Arc<dyn crate::auth::TokenVerifier>,
    kernel_actor: Option<(pathlink::Link, crate::auth::Actor)>,
}

impl Default for KernelBuilder {
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
            kernel_actor: None,
        }
    }
}

impl KernelBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_lib_handler<H>(mut self, handler: H) -> Self
    where
        H: KernelHandler,
    {
        self.lib_get_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_lib_put_handler<H>(mut self, handler: H) -> Self
    where
        H: KernelHandler,
    {
        self.lib_put_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_service_handler<H>(mut self, handler: H) -> Self
    where
        H: KernelHandler,
    {
        self.service_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_lib_route_handler<H>(mut self, handler: H) -> Self
    where
        H: KernelHandler,
    {
        self.lib_route_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_kernel_handler<H>(mut self, handler: H) -> Self
    where
        H: KernelHandler,
    {
        self.kernel_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_health_handler<H>(mut self, handler: H) -> Self
    where
        H: KernelHandler,
    {
        self.health_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_library_module(
        mut self,
        module: Arc<LibraryRegistry>,
        handlers: LibraryHandlers,
    ) -> Self {
        self.library_module = Some(module);
        self.lib_get_handler = Some(handlers.get_handler());
        self.lib_put_handler = Some(handlers.put_handler());
        self.lib_route_handler = handlers.route_handler();
        self
    }

    pub fn with_host_id(mut self, host_id: impl Into<String>) -> Self {
        let ttl = self.txn_manager.ttl();
        self.txn_manager = crate::txn::TxnManager::with_host_id_and_ttl(host_id.into(), ttl);
        self
    }

    pub fn with_txn_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.txn_server = TxnServer::new(ttl);
        self.txn_manager.set_ttl(ttl);
        self
    }

    pub fn with_egress_policy(mut self, policy: EgressPolicy) -> Self {
        self.egress = policy;
        self
    }

    pub fn with_dependency_route(
        mut self,
        dependency_root: impl Into<String>,
        authority: std::net::SocketAddr,
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

    pub fn with_kernel_actor(mut self, host: pathlink::Link, actor: crate::auth::Actor) -> Self {
        self.kernel_actor = Some((host, actor));
        self
    }

    pub fn with_rjwt_token_verifier(
        self,
        resolver: Arc<dyn crate::auth::RjwtActorResolver>,
    ) -> Self {
        self.with_token_verifier(crate::auth::RjwtTokenVerifier::new(resolver))
    }

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

    pub fn finish(self) -> Kernel {
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
            kernel_actor: self.kernel_actor,
        }
    }
}

fn stub_handler(label: &str) -> Arc<dyn KernelHandler> {
    let label = label.to_string();
    Arc::new(move |_req: Request| {
        let label = label.clone();
        async move { panic!("stub handler {label} not implemented") }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Body;
    use crate::resolve::Resolve;
    use futures::FutureExt;
    use std::sync::{Arc, Mutex};
    use tc_ir::{LibrarySchema, NetworkTime};

    fn ok_handler() -> impl KernelHandler {
        |_req: Request| async { Response::new(Body::empty()) }.boxed()
    }

    #[test]
    fn enforces_owner_claim_for_structured_tokens() {
        use std::str::FromStr;

        use tc_ir::Claim;
        use umask::USER_EXEC;

        let kernel = Kernel::builder().with_lib_handler(ok_handler()).finish();
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
                Request::new(Body::empty()),
                Some(txn_id),
                true,
                Some(&token_a),
                |_txn, _req| {},
            )
            .expect("begin via claimed token");

        let result = kernel.route_request(
            Method::Get,
            "/lib",
            Request::new(Body::empty()),
            Some(txn_id),
            true,
            Some(&token_b),
            |_txn, _req| {},
        );
        assert!(matches!(result, Err(crate::txn::TxnError::Unauthorized)));
    }

    #[test]
    fn verifies_rjwt_and_pins_owner_by_txn_claim() {
        use std::str::FromStr;

        use crate::auth::TokenVerifier;
        use rjwt::Token;
        use tc_ir::Claim;
        use tc_value::Value;
        use umask::USER_EXEC;

        let kernel = Kernel::builder().with_lib_handler(ok_handler()).finish();
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(1), 1).with_trace([0; 32]);

        let host = pathlink::Link::from_str(crate::uri::HOST_ROOT).expect("host link");
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
            Request::new(Body::empty()),
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
            Request::new(Body::empty()),
            Some(txn_id),
            true,
            Some(&token_ctx_b),
            |_txn, _req| {},
        );
        assert!(matches!(result, Err(crate::txn::TxnError::Unauthorized)));
    }

    #[test]
    fn tracks_active_txns_and_expires_them() {
        let kernel = Kernel::builder()
            .with_txn_ttl(std::time::Duration::from_secs(10))
            .with_lib_handler(ok_handler())
            .finish();

        let dispatch = kernel
            .route_request(
                Method::Get,
                "/lib",
                Request::new(Body::empty()),
                None,
                true,
                None,
                |_txn, _req| {},
            )
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

    #[test]
    fn finalize_requires_txn_claim_in_token() {
        let kernel = Kernel::builder().with_lib_handler(ok_handler()).finish();

        let begin = kernel.route_request(
            Method::Get,
            "/lib",
            Request::new(Body::empty()),
            None,
            true,
            None,
            |_txn, _req| {},
        );
        assert!(begin.is_ok());

        let pending = kernel.txn_manager().pending_ids();
        assert_eq!(pending.len(), 1);
        let txn_id = pending[0];

        let token = crate::auth::TokenContext::new("owner-a", "bearer-a");
        let finalize = kernel.route_request(
            Method::Post,
            "/lib",
            Request::new(Body::empty()),
            Some(txn_id),
            true,
            Some(&token),
            |_txn, _req| {},
        );

        assert!(matches!(finalize, Err(crate::txn::TxnError::Unauthorized)));
    }

    #[derive(Clone, Default)]
    struct MockGateway {
        calls: Arc<Mutex<Vec<pathlink::Link>>>,
    }

    impl crate::gateway::RpcGateway for MockGateway {
        fn get(
            &self,
            uri: pathlink::Link,
            _txn: crate::txn::TxnHandle,
            _key: tc_value::Value,
        ) -> BoxFuture<'static, tc_error::TCResult<tc_state::State>> {
            self.calls.lock().expect("calls lock").push(uri);
            Box::pin(async move { Ok(tc_state::State::None) })
        }

        fn put(
            &self,
            _uri: pathlink::Link,
            _txn: crate::txn::TxnHandle,
            _key: tc_value::Value,
            _value: tc_state::State,
        ) -> BoxFuture<'static, tc_error::TCResult<()>> {
            Box::pin(async move { Ok(()) })
        }

        fn post(
            &self,
            _uri: pathlink::Link,
            _txn: crate::txn::TxnHandle,
            _params: tc_ir::Map<tc_state::State>,
        ) -> BoxFuture<'static, tc_error::TCResult<tc_state::State>> {
            Box::pin(async move { Ok(tc_state::State::None) })
        }

        fn delete(
            &self,
            _uri: pathlink::Link,
            _txn: crate::txn::TxnHandle,
            _key: tc_value::Value,
        ) -> BoxFuture<'static, tc_error::TCResult<()>> {
            Box::pin(async move { Ok(()) })
        }
    }

    #[test]
    fn egress_is_default_deny_without_manifest_dependency() {
        let schema = LibrarySchema::new(
            "/lib/acme/a/1.0.0".parse().expect("schema id"),
            "1.0.0",
            vec![],
        );

        let registry =
            crate::library::LibraryRegistry::new(None, std::collections::BTreeMap::new());
        futures::executor::block_on(registry.insert_schema(schema)).expect("insert schema");
        let module = Arc::new(registry);
        let handlers = crate::library::LibraryHandlers::without_route(ok_handler(), ok_handler());

        let gateway = MockGateway::default();

        let kernel = Kernel::builder()
            .with_library_module(module, handlers)
            .with_dependency_route("/lib", "127.0.0.1:1234".parse().expect("addr"))
            .with_rpc_gateway(gateway)
            .finish();

        let op = tc_ir::OpRef::Get((
            tc_ir::Subject::Link("http://127.0.0.1:1234/lib".parse().expect("link")),
            tc_ir::Scalar::default(),
        ));

        let txn = kernel.with_resolver(kernel.txn_manager().begin());
        let err = futures::executor::block_on(op.resolve(&txn)).expect_err("expected unauthorized");
        assert_eq!(err.code(), tc_error::ErrorKind::Unauthorized);
    }

    #[test]
    fn resolves_scalar_ref_using_txn_context() {
        use tc_ir::{OpRef, Scalar, Subject, TCRef};
        use tc_state::State;
        use tc_value::Value;

        type GatewayCall = (Method, pathlink::Link, Option<Value>);

        #[derive(Clone)]
        struct RecordingGateway {
            calls: Arc<Mutex<Vec<GatewayCall>>>,
            responses: Arc<Mutex<Vec<State>>>,
        }

        impl RecordingGateway {
            fn new(responses: Vec<State>) -> Self {
                Self {
                    calls: Arc::new(Mutex::new(Vec::new())),
                    responses: Arc::new(Mutex::new(responses)),
                }
            }
        }

        impl crate::gateway::RpcGateway for RecordingGateway {
            fn get(
                &self,
                uri: pathlink::Link,
                _txn: crate::txn::TxnHandle,
                key: Value,
            ) -> BoxFuture<'static, tc_error::TCResult<State>> {
                self.calls
                    .lock()
                    .expect("calls lock")
                    .push((Method::Get, uri, Some(key)));

                let response = self.responses.lock().expect("responses lock").remove(0);

                Box::pin(async move { Ok(response) })
            }

            fn put(
                &self,
                _uri: pathlink::Link,
                _txn: crate::txn::TxnHandle,
                _key: Value,
                _value: State,
            ) -> BoxFuture<'static, tc_error::TCResult<()>> {
                Box::pin(async move { Ok(()) })
            }

            fn post(
                &self,
                _uri: pathlink::Link,
                _txn: crate::txn::TxnHandle,
                _params: tc_ir::Map<State>,
            ) -> BoxFuture<'static, tc_error::TCResult<State>> {
                Box::pin(async move { Ok(State::None) })
            }

            fn delete(
                &self,
                _uri: pathlink::Link,
                _txn: crate::txn::TxnHandle,
                _key: Value,
            ) -> BoxFuture<'static, tc_error::TCResult<()>> {
                Box::pin(async move { Ok(()) })
            }
        }

        let schema = LibrarySchema::new(
            "/lib/example-devco/example/1.0.0"
                .parse()
                .expect("schema id"),
            "1.0.0",
            vec![
                "/lib/example-devco/example/1.0.0"
                    .parse()
                    .expect("dependency root"),
            ],
        );

        let registry =
            crate::library::LibraryRegistry::new(None, std::collections::BTreeMap::new());
        futures::executor::block_on(registry.insert_schema(schema)).expect("insert schema");
        let module = Arc::new(registry);
        let handlers = crate::library::LibraryHandlers::without_route(ok_handler(), ok_handler());

        let gateway = RecordingGateway::new(vec![
            State::from(Value::from("key")),
            State::from(Value::from("ok")),
        ]);

        let kernel = Kernel::builder()
            .with_library_module(module, handlers)
            .with_dependency_route(
                "/lib/example-devco/example/1.0.0",
                "127.0.0.1:1234".parse().expect("addr"),
            )
            .with_rpc_gateway(gateway.clone())
            .finish();

        let txn = kernel.with_resolver(kernel.txn_manager().begin());

        let inner_op = OpRef::Get((
            Subject::Link(
                "http://127.0.0.1:1234/lib/example-devco/example/1.0.0/key"
                    .parse()
                    .expect("inner link"),
            ),
            Scalar::default(),
        ));

        let outer_op = OpRef::Get((
            Subject::Link(
                "http://127.0.0.1:1234/lib/example-devco/example/1.0.0/value"
                    .parse()
                    .expect("outer link"),
            ),
            Scalar::Ref(Box::new(TCRef::Op(inner_op))),
        ));

        let response = futures::executor::block_on(outer_op.resolve(&txn)).expect("resolve op");

        assert!(matches!(
            response,
            State::Scalar(Scalar::Value(Value::String(ref s))) if s == "ok"
        ));

        let calls = gateway.calls.lock().expect("calls lock");
        assert_eq!(calls.len(), 2);
        assert!(matches!(calls[1].2, Some(Value::String(ref s)) if s == "key"));
    }
}
