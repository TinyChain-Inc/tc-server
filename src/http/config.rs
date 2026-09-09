use std::sync::Arc;
use std::time::Duration;

use tc_error::TCResult;

use crate::Kernel;
use crate::http::HttpHandler;

pub struct HttpRuntime {
    pub kernel: Kernel,
    pub router: super::HttpRouter,
}

pub struct HttpKernelConfig {
    pub application_roots: crate::storage::ApplicationRoots,
    pub workspace: crate::Workspace,
    pub limits: crate::HostLimits,
    protocol: crate::ProtocolAuthority,
    verifier: Arc<dyn crate::auth::TokenVerifier>,
    public_keys: crate::auth::PublicKeyStore,
    replication: Arc<dyn crate::replication::ClusterGateway>,
    rpc: Arc<dyn crate::gateway::RpcGateway>,
}

impl HttpKernelConfig {
    pub fn new(
        application_roots: crate::storage::ApplicationRoots,
        workspace: crate::Workspace,
        protocol: crate::ProtocolAuthority,
        verifier: Arc<dyn crate::auth::TokenVerifier>,
        public_keys: crate::auth::PublicKeyStore,
        replication: impl crate::replication::ClusterGateway,
        rpc: impl crate::gateway::RpcGateway,
    ) -> Self {
        Self {
            application_roots,
            workspace,
            limits: crate::HostLimits::default(),
            protocol,
            verifier,
            public_keys,
            replication: Arc::new(replication),
            rpc: Arc::new(rpc),
        }
    }

    pub fn with_txn_ttl(mut self, ttl: Duration) -> Self {
        self.limits.transaction_ttl = ttl;
        self
    }

    pub fn with_max_request_bytes(mut self, max_bytes: usize) -> Self {
        self.limits.ingress.request_body_bytes = max_bytes;
        self
    }
}

pub async fn build_http_runtime_with_config<H>(
    config: HttpKernelConfig,
    peer_handler: H,
) -> TCResult<HttpRuntime>
where
    H: HttpHandler,
{
    let protocol = config.protocol.clone();
    let applications = Arc::new(
        crate::ApplicationOwners::new(
            config.application_roots,
            protocol.clone(),
            config.replication,
        )
        .await?,
    );
    let resources = crate::HostResources::new(config.limits.clone());
    let services = crate::HostServices {
        applications: Arc::clone(&applications),
        rpc: config.rpc,
        resources,
        protocol,
        verifier: config.verifier,
        public_keys: config.public_keys,
    };
    let kernel = Kernel::new(services, config.workspace, config.limits.transaction_ttl).await?;
    let router = super::HttpRouter::new(peer_handler);
    Ok(HttpRuntime { kernel, router })
}
