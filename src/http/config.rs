use std::{sync::Arc, time::Duration};

use tc_error::TCResult;
use tc_ir::LibrarySchema;

use crate::http::HttpHandler;
use crate::library::{
    LibraryRegistry, default_library_schema, http::build_http_library_module_with_store,
};
use crate::storage::LibraryStore;
use crate::{Kernel, KernelBuilder};

pub struct HttpRuntime {
    pub kernel: Kernel,
    pub router: super::HttpRouter,
    pub registry: Arc<LibraryRegistry>,
}

/// Configuration options for building an HTTP kernel instance.
#[derive(Clone)]
pub struct HttpKernelConfig {
    pub library_store: Option<LibraryStore>,
    pub workspace: Option<crate::Workspace>,
    pub initial_schema: LibrarySchema,
    pub host_id: String,
    pub limits: crate::HostLimits,
}

impl Default for HttpKernelConfig {
    fn default() -> Self {
        Self {
            library_store: None,
            workspace: None,
            initial_schema: default_library_schema(),
            host_id: "tc-http-host".to_string(),
            limits: crate::HostLimits::default(),
        }
    }
}

impl HttpKernelConfig {
    /// Inject the bootstrap-owned library/artifact store.
    pub fn with_library_store(mut self, store: LibraryStore) -> Self {
        self.library_store = Some(store);
        self
    }

    /// Inject the bootstrap-owned transaction workspace.
    pub fn with_workspace(mut self, workspace: crate::Workspace) -> Self {
        self.workspace = Some(workspace);
        self
    }

    pub fn with_initial_schema(mut self, schema: LibrarySchema) -> Self {
        self.initial_schema = schema;
        self
    }

    pub fn with_host_id(mut self, host_id: impl Into<String>) -> Self {
        self.host_id = host_id.into();
        self
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

/// Build the native kernel and its HTTP-only router from one explicit bootstrap assembly.
pub async fn build_http_runtime_with_config<S, Ho, H, F, R>(
    config: HttpKernelConfig,
    service_handler: S,
    health_handler: H,
    host_handler: R,
    configure: F,
) -> TCResult<HttpRuntime>
where
    S: HttpHandler,
    Ho: HttpHandler,
    H: HttpHandler,
    F: FnOnce(&Arc<LibraryRegistry>, KernelBuilder) -> KernelBuilder,
    R: FnOnce(Arc<LibraryRegistry>) -> Ho,
{
    let module = build_http_library_module_with_store(
        config.initial_schema.clone(),
        config.library_store.clone(),
    )
    .await?;
    module.hydrate_from_storage().await?;
    let resources = crate::HostResources::new(config.limits.clone());
    let mut builder = Kernel::builder()
        .with_resources(resources)
        .with_host_id(config.host_id.clone())
        .with_http_rpc_gateway()
        .with_library_module(module.clone())
        .with_txn_ttl(config.limits.transaction_ttl);

    if let Some(workspace) = config.workspace.clone() {
        builder = builder.with_workspace(workspace);
    }

    let kernel = configure(&module, builder).finish();

    let router = super::HttpRouter::new(
        module.clone(),
        health_handler,
        host_handler(module.clone()),
        service_handler,
    );
    Ok(HttpRuntime {
        kernel,
        router,
        registry: module,
    })
}
