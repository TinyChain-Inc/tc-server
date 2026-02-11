use std::sync::Arc;

use crate::egress::EgressPolicy;
use crate::library::{LibraryHandlers, LibraryRegistry};
use crate::txn_server::TxnServer;
use crate::Request;

use super::{Kernel, KernelHandler};

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
