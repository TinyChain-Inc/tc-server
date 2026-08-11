use std::sync::Arc;

use super::Kernel;
use crate::egress::EgressPolicy;
use crate::library::LibraryRegistry;

pub struct KernelBuilder {
    resources: crate::HostResources,
    txn: crate::txn::TxnConfig,
    egress: EgressPolicy,
    library_module: Option<Arc<LibraryRegistry>>,
    rpc_gateway: Option<Arc<dyn crate::gateway::RpcGateway>>,
    token_verifier: Arc<dyn crate::auth::TokenVerifier>,
    token_verifier_explicit: bool,
    txn_finalize: Option<crate::txn::TxnFinalize>,
}

impl Default for KernelBuilder {
    fn default() -> Self {
        let txn = crate::txn::TxnConfig::default();
        let token_verifier = default_rjwt_verifier(&txn);
        Self {
            resources: crate::HostResources::default(),
            txn,
            egress: EgressPolicy::default(),
            library_module: None,
            rpc_gateway: None,
            token_verifier,
            token_verifier_explicit: false,
            txn_finalize: None,
        }
    }
}

impl KernelBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_library_module(mut self, module: Arc<LibraryRegistry>) -> Self {
        self.library_module = Some(module);
        self
    }

    pub fn with_resources(mut self, resources: crate::HostResources) -> Self {
        self.txn.resources = resources.clone();
        self.resources = resources;
        self
    }

    pub fn with_host_id(mut self, host_id: impl Into<String>) -> Self {
        let ttl = self.txn.ttl;
        let workspace = self.txn.workspace.clone();
        self.txn = crate::txn::TxnConfig::with_host_id(host_id);
        self.txn.ttl = ttl;
        self.txn.workspace = workspace;
        self.txn.resources = self.resources.clone();
        if !self.token_verifier_explicit {
            self.token_verifier = default_rjwt_verifier(&self.txn);
        }
        self
    }

    pub fn with_protocol_actor(mut self, host: pathlink::Link, actor: crate::auth::Actor) -> Self {
        self.txn.protocol_host = host;
        self.txn.protocol_actor = Arc::new(actor);
        if !self.token_verifier_explicit {
            self.token_verifier = default_rjwt_verifier(&self.txn);
        }
        self
    }

    pub fn with_txn_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.txn.ttl = ttl;
        self
    }

    pub fn with_workspace(mut self, workspace: crate::Workspace) -> Self {
        self.txn.workspace = Some(workspace);
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
        self.token_verifier_explicit = true;
        self
    }

    pub fn with_txn_finalize_hook<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(crate::txn::TxnHandle, bool) -> Fut + Send + Sync + 'static,
        Fut: futures::Future<Output = tc_error::TCResult<()>> + Send + 'static,
    {
        self.txn_finalize = Some(Arc::new(move |txn, commit| Box::pin(hook(txn, commit))));
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
        let keyring = keyring.with_actor(
            self.txn.protocol_host.clone(),
            self.txn.protocol_actor.as_ref().clone(),
        );
        self.with_rjwt_token_verifier(Arc::new(keyring))
    }

    #[cfg(feature = "http-client")]
    pub fn with_http_rpc_gateway(self) -> Self {
        self.with_rpc_gateway(crate::http_client::HttpRpcGateway::new())
    }

    pub fn finish(self) -> Kernel {
        let txn_server = crate::txn::TxnServer::new(
            self.txn,
            self.library_module.as_ref().map(Arc::clone),
            self.txn_finalize.as_ref().map(Arc::clone),
            Arc::clone(&self.token_verifier),
        );
        Kernel {
            resources: self.resources,
            txn_server,
            egress: self.egress,
            library_module: self.library_module,
            rpc_gateway: self.rpc_gateway,
            token_verifier: self.token_verifier,
        }
    }
}

fn default_rjwt_verifier(txn: &crate::txn::TxnConfig) -> Arc<dyn crate::auth::TokenVerifier> {
    let keyring = crate::auth::KeyringActorResolver::default().with_actor(
        txn.protocol_host.clone(),
        txn.protocol_actor.as_ref().clone(),
    );
    Arc::new(crate::auth::RjwtTokenVerifier::new(Arc::new(keyring)))
}
