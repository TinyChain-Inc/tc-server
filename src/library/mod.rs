use std::sync::Arc;

use tc_error::TCResult;
use tc_ir::LibrarySchema;

use crate::KernelHandler;

mod install;
#[cfg(any(feature = "http-server", feature = "pyo3"))]
mod native;
mod registry;
mod route_meta;
mod routes;
mod runtime;
mod state;
mod util;

pub use install::{
    InstallArtifacts, InstallError, InstallRequest, decode_install_request_bytes,
    encode_install_payload_bytes,
};
pub use registry::LibraryRegistry;
pub use route_meta::{RouteMetadata, SchemaRoutes};
pub use routes::{LibraryHandlers, LibraryRoutes};
pub use runtime::LibraryRuntime;
pub use state::{LibraryState, default_library_schema};

#[cfg(any(feature = "http-server", feature = "pyo3"))]
pub use native::{NativeLibrary, NativeLibraryHandler};

#[cfg(any(feature = "http-server", feature = "pyo3"))]
pub mod http;

#[cfg(test)]
mod registry_tests;

type HandlerArc = Arc<dyn KernelHandler>;
type LibraryFactory =
    Arc<dyn Fn(Vec<u8>) -> TCResult<(LibrarySchema, SchemaRoutes, HandlerArc)> + Send + Sync>;
