use std::{path::PathBuf, sync::Arc, time::Duration};

use tc_error::TCResult;
use tc_ir::LibrarySchema;

use crate::library::{
    LibraryRegistry, NativeLibrary, NativeLibraryHandler, default_library_schema,
    http::{build_http_library_module_with_store, http_library_handlers},
};
use crate::storage::{LibraryStore, load_library_root};
use crate::{Kernel, KernelBuilder, KernelHandler};

use super::native::{
    http_native_routes_handler, native_install_not_supported_handler, native_schema_get_handler,
};
/// Configuration options for building an HTTP kernel instance.
#[derive(Clone, Debug)]
pub struct HttpKernelConfig {
    pub data_dir: Option<PathBuf>,
    pub initial_schema: LibrarySchema,
    pub host_id: String,
    pub limits: crate::KernelLimits,
}

impl Default for HttpKernelConfig {
    fn default() -> Self {
        Self {
            data_dir: None,
            initial_schema: default_library_schema(),
            host_id: "tc-http-host".to_string(),
            limits: crate::KernelLimits::default(),
        }
    }
}

impl HttpKernelConfig {
    pub fn with_data_dir<P: Into<PathBuf>>(mut self, data_dir: P) -> Self {
        self.data_dir = Some(data_dir.into());
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
        self.limits.txn_ttl = ttl;
        self
    }

    pub fn with_max_request_bytes_unauth(mut self, max_bytes: usize) -> Self {
        self.limits.max_request_bytes_unauth = max_bytes;
        self
    }
}

/// Helper that wires the shared library module plus storage into a kernel configured for HTTP.
pub async fn build_http_kernel_with_config<S, K, H>(
    config: HttpKernelConfig,
    service_handler: S,
    kernel_handler: K,
    health_handler: H,
) -> TCResult<Kernel>
where
    S: KernelHandler,
    K: KernelHandler,
    H: KernelHandler,
{
    let (kernel, _module) = build_http_kernel_and_registry_with_config_and_builder(
        config,
        service_handler,
        health_handler,
        |_, builder| builder.with_kernel_handler(kernel_handler),
    )
    .await?;
    Ok(kernel)
}

pub async fn build_http_kernel_and_registry_with_config_and_builder<S, H, F>(
    config: HttpKernelConfig,
    service_handler: S,
    health_handler: H,
    configure: F,
) -> TCResult<(Kernel, Arc<LibraryRegistry>)>
where
    S: KernelHandler,
    H: KernelHandler,
    F: FnOnce(&Arc<LibraryRegistry>, KernelBuilder) -> KernelBuilder,
{
    let storage_root = config.data_dir.clone();
    let store = match storage_root {
        Some(root) => {
            let root_dir = load_library_root(root).await?;
            Some(LibraryStore::from_root(root_dir))
        }
        None => None,
    };

    let module = build_http_library_module_with_store(config.initial_schema.clone(), store).await?;
    module.hydrate_from_storage().await?;
    let handlers = http_library_handlers(&module);

    let builder = Kernel::builder()
        .with_host_id(config.host_id.clone())
        .with_http_rpc_gateway()
        .with_library_module(module.clone(), handlers)
        .with_service_handler(service_handler)
        .with_health_handler(health_handler)
        .with_txn_ttl(config.limits.txn_ttl);

    let kernel = configure(&module, builder).finish();

    Ok((kernel, module))
}

pub async fn build_http_kernel<S, K, H>(
    service_handler: S,
    kernel_handler: K,
    health_handler: H,
) -> TCResult<Kernel>
where
    S: KernelHandler,
    K: KernelHandler,
    H: KernelHandler,
{
    build_http_kernel_with_config(
        HttpKernelConfig::default(),
        service_handler,
        kernel_handler,
        health_handler,
    )
    .await
}

pub fn build_http_kernel_with_native_library<H, S, K, He>(
    library: NativeLibrary<H>,
    service_handler: S,
    kernel_handler: K,
    health_handler: He,
) -> Kernel
where
    H: NativeLibraryHandler,
    S: KernelHandler,
    K: KernelHandler,
    He: KernelHandler,
{
    build_http_kernel_with_native_library_and_config(
        library,
        HttpKernelConfig::default(),
        service_handler,
        kernel_handler,
        health_handler,
    )
}

pub fn build_http_kernel_with_native_library_and_config<H, S, K, He>(
    library: NativeLibrary<H>,
    config: HttpKernelConfig,
    service_handler: S,
    kernel_handler: K,
    health_handler: He,
) -> Kernel
where
    H: NativeLibraryHandler,
    S: KernelHandler,
    K: KernelHandler,
    He: KernelHandler,
{
    let native = Arc::new(library);
    let schema_handler = native_schema_get_handler(native.schema().clone());
    let install_handler = native_install_not_supported_handler();
    let routes_handler = http_native_routes_handler(native);

    Kernel::builder()
        .with_host_id(config.host_id.clone())
        .with_lib_handler(schema_handler)
        .with_lib_put_handler(install_handler)
        .with_lib_route_handler(routes_handler)
        .with_service_handler(service_handler)
        .with_kernel_handler(kernel_handler)
        .with_health_handler(health_handler)
        .finish()
}
