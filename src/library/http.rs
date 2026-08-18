use std::{collections::BTreeMap, sync::Arc};

use crate::http::{Body, HttpHandler, Request, Response, StatusCode};
use futures::FutureExt;
use hyper::body;
use tc_error::TCResult;
use tc_ir::LibrarySchema;

use crate::{
    ir::{IR_ARTIFACT_CONTENT_TYPE, WASM_ARTIFACT_CONTENT_TYPE, compile_ir_library},
    storage::LibraryStore,
    wasm::http_wasm_route_handler_from_bytes,
};

use super::{
    CompiledLibrary, LibraryCompiler, LibraryExecution, LibraryRegistry, RouteMetadata,
    SchemaRoutes, StageInstallError, decode_authorize_and_stage_install,
};

fn stage_install_error_response(error: StageInstallError) -> Response {
    let (status, message) = match error {
        StageInstallError::Unauthorized(message) => (StatusCode::UNAUTHORIZED, message),
        StageInstallError::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
        StageInstallError::Internal(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
    };
    http::Response::builder()
        .status(status)
        .body(Body::from(message))
        .expect("library install error response")
}

pub async fn build_http_library_module(
    initial_schema: LibrarySchema,
    store: Option<LibraryStore>,
) -> TCResult<Arc<LibraryRegistry>> {
    build_http_library_module_with_store(initial_schema, store).await
}

pub async fn build_http_library_module_with_store(
    initial_schema: LibrarySchema,
    store: Option<LibraryStore>,
) -> TCResult<Arc<LibraryRegistry>> {
    let wasm_compiler: LibraryCompiler = Arc::new(|artifact| {
        Box::pin(async move {
            let engine = wasmtime::Engine::default();
            let wasm = crate::wasm::WasmLibrary::from_bytes(&engine, &artifact.bytes)?;
            let routes = wasm
                .bindings()
                .iter()
                .map(|binding| {
                    (
                        binding.path.clone(),
                        RouteMetadata {
                            export: Some(binding.export.clone()),
                        },
                    )
                })
                .collect();
            Ok(CompiledLibrary {
                schema: wasm.schema().clone(),
                classes: Vec::new(),
                routes: SchemaRoutes::from_entries(routes)?,
                artifact,
                execution: LibraryExecution::Transport,
            })
        })
    });

    let ir_compiler: LibraryCompiler = Arc::new(|artifact| Box::pin(compile_ir_library(artifact)));

    let registry = LibraryRegistry::new(
        store,
        BTreeMap::from([
            (WASM_ARTIFACT_CONTENT_TYPE.to_string(), wasm_compiler),
            (IR_ARTIFACT_CONTENT_TYPE.to_string(), ir_compiler),
        ]),
    );
    registry.insert_schema(initial_schema).await?;
    Ok(Arc::new(registry))
}

pub fn schema_get_handler(registry: Arc<LibraryRegistry>) -> impl HttpHandler {
    move |_req: Request| {
        let registry = Arc::clone(&registry);
        async move { respond_with_listing(registry.list_dir(crate::uri::LIB_ROOT)) }.boxed()
    }
}

pub fn schema_put_handler(registry: Arc<LibraryRegistry>) -> impl HttpHandler {
    move |req: Request| {
        let registry = Arc::clone(&registry);
        let txn = req.extensions().get::<crate::txn::TxnHandle>().cloned();
        async move {
            let txn = match txn {
                Some(txn) => txn,
                None => return unauthorized_response("missing transaction context"),
            };

            let body_bytes = match body::to_bytes(req.into_body()).await {
                Ok(bytes) => bytes,
                Err(err) => {
                    return stage_install_error_response(StageInstallError::Internal(
                        err.to_string(),
                    ));
                }
            };

            match decode_authorize_and_stage_install(&registry, &txn, &body_bytes).await {
                Ok(_) => no_content_response(),
                Err(err) => stage_install_error_response(err),
            }
        }
        .boxed()
    }
}

fn respond_with_schema(schema: LibrarySchema) -> Response {
    let state = super::view::schema(&schema);
    crate::http::state_response(state)
}

fn respond_with_listing(listing: Option<tc_ir::Map<bool>>) -> Response {
    let Some(listing) = listing else {
        return http::Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .expect("dir not found response");
    };

    let state = super::view::listing(listing);
    crate::http::state_response(state)
}

fn unauthorized_response(message: impl Into<String>) -> Response {
    http::Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .body(Body::from(message.into()))
        .expect("unauthorized response")
}

fn no_content_response() -> Response {
    http::Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .expect("library install response")
}

pub fn routes_handler(registry: Arc<LibraryRegistry>) -> impl HttpHandler {
    move |req: Request| {
        let path = req.uri().path().to_string();
        let registry = Arc::clone(&registry);
        async move {
            match registry.resolve_runtime_for_path(&path) {
                Some((runtime, true)) => respond_with_schema(runtime.state.schema()),
                Some((runtime, false)) => match runtime.execution() {
                    // Native members are dispatched by HttpRouter through Kernel::execute.
                    Some(LibraryExecution::Native(_)) => http::Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Body::empty())
                        .expect("native route is not a transport route"),
                    Some(LibraryExecution::Transport) => match runtime.artifact() {
                        Some(artifact) if artifact.content_type == WASM_ARTIFACT_CONTENT_TYPE => {
                            match http_wasm_route_handler_from_bytes(artifact.bytes) {
                                Ok((handler, _, _)) => handler.call(req).await,
                                Err(err) => crate::http::tc_error_response(err),
                            }
                        }
                        _ => http::Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(Body::empty())
                            .expect("missing transport route"),
                    },
                    None => http::Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Body::empty())
                        .expect("missing route"),
                },
                None => respond_with_listing(registry.list_dir(&path)),
            }
        }
        .boxed()
    }
}
