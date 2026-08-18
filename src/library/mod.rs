use std::sync::Arc;

use futures::future::BoxFuture;
use tc_error::TCResult;
use tc_ir::LibrarySchema;
use umask::USER_WRITE;

use crate::storage::Artifact;

mod dir;
mod install;
#[cfg(any(feature = "http-server", feature = "pyo3"))]
mod native;
mod registry;
mod route_meta;
mod runtime;
mod state;
mod util;
pub(crate) mod view;

pub use install::{
    CompiledLibraryPackage, InstallError, decode_compiled_library_package,
    decode_install_request_bytes, encode_compiled_library_package,
};
#[cfg(any(feature = "http-server", feature = "pyo3"))]
pub use native::NativeLibrary;
pub use registry::LibraryRegistry;
pub use route_meta::{RouteMetadata, SchemaRoutes};
pub use runtime::LibraryRuntime;
pub use state::{LibraryState, default_library_schema};

#[cfg(any(feature = "http-server", feature = "pyo3"))]
pub mod http;

#[cfg(test)]
mod registry_tests;

/// A compiled library is either directly executable by the native kernel or is
/// retained for a transport-specific ABI such as WASM. The registry owns this
/// lifecycle, not HTTP handlers.
#[derive(Clone)]
pub struct CompiledLibrary {
    pub schema: LibrarySchema,
    pub classes: Vec<tc_state::ClassDef<pathlink::Link>>,
    pub routes: SchemaRoutes,
    pub artifact: Artifact,
    pub execution: LibraryExecution,
}

#[derive(Clone)]
pub enum LibraryExecution {
    Native(crate::ir::IrRoutes),
    Transport,
}

pub type LibraryCompiler =
    Arc<dyn Fn(Artifact) -> BoxFuture<'static, TCResult<CompiledLibrary>> + Send + Sync>;

#[derive(Debug)]
pub(crate) enum StageInstallError {
    Unauthorized(String),
    BadRequest(String),
    Internal(String),
}

impl StageInstallError {
    pub(crate) fn unauthorized(message: impl Into<String>) -> Self {
        Self::Unauthorized(message.into())
    }

    pub(crate) fn from_install_error(error: InstallError) -> Self {
        match error {
            InstallError::BadRequest(message) => Self::BadRequest(message),
            InstallError::Internal(message) => Self::Internal(message),
        }
    }
}

pub(crate) async fn decode_authorize_and_stage_install(
    registry: &LibraryRegistry,
    txn: &crate::txn::TxnHandle,
    body_bytes: &[u8],
) -> Result<String, StageInstallError> {
    let install_compiled_package =
        decode_install_request_bytes(body_bytes).map_err(StageInstallError::from_install_error)?;
    let schema = &install_compiled_package.schema;

    if !txn.has_claim(schema.id(), USER_WRITE) {
        return Err(StageInstallError::unauthorized(
            "unauthorized library install",
        ));
    }

    registry
        .stage_install_request(txn.id(), install_compiled_package)
        .await
        .map_err(StageInstallError::from_install_error)
}
