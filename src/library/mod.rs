use std::sync::Arc;

use tc_error::TCResult;
use tc_ir::LibrarySchema;
use umask::USER_WRITE;

use crate::{Body, KernelHandler, Response, StatusCode};

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

pub(crate) fn stage_install_error_response(error: StageInstallError) -> Response {
    match error {
        StageInstallError::Unauthorized(message) => {
            let mut response = Response::new(Body::from(message));
            *response.status_mut() = StatusCode::UNAUTHORIZED;
            response
        }
        StageInstallError::BadRequest(message) => {
            let mut response = Response::new(Body::from(message));
            *response.status_mut() = StatusCode::BAD_REQUEST;
            response
        }
        StageInstallError::Internal(message) => {
            let mut response = Response::new(Body::from(message));
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            response
        }
    }
}

pub(crate) async fn decode_authorize_and_stage_install(
    registry: &LibraryRegistry,
    txn: &crate::txn::TxnHandle,
    body_bytes: &[u8],
) -> Result<String, StageInstallError> {
    let install_request =
        decode_install_request_bytes(body_bytes).map_err(StageInstallError::from_install_error)?;

    let schema = match &install_request {
        InstallRequest::SchemaOnly(schema) => schema,
        InstallRequest::WithArtifacts(payload) => &payload.schema,
    };

    if !txn.has_claim(schema.id(), USER_WRITE) {
        return Err(StageInstallError::unauthorized(
            "unauthorized library install",
        ));
    }

    registry
        .stage_install_request(txn.id(), install_request)
        .await
        .map_err(StageInstallError::from_install_error)
}
