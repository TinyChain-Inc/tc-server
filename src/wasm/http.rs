use std::sync::Arc;

use tc_error::{ErrorKind, TCError, TCResult};
use tc_ir::{LibrarySchema, TxnHeader, parse_route_path};
use tokio::sync::Mutex;

use crate::KernelHandler;
use crate::http::{Body, Request, Response, StatusCode};
use crate::library::{RouteMetadata, SchemaRoutes};
use crate::resolve::Resolve;
use crate::txn::TxnHandle;

use super::WasmLibrary;
use super::decode::try_decode_wasm_ref;

pub fn http_wasm_route_handler_from_bytes(
    bytes: Vec<u8>,
) -> TCResult<(impl KernelHandler, LibrarySchema, SchemaRoutes)> {
    let engine = wasmtime::Engine::default();
    let wasm = WasmLibrary::from_bytes(&engine, &bytes)?;
    let schema = wasm.schema().clone();
    let metadata_entries = wasm
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
    let schema_routes = SchemaRoutes::from_entries(metadata_entries)?;
    let shared = Arc::new(Mutex::new(wasm));

    let handler = move |req: Request| {
        let wasm = shared.clone();
        async move { http_handle_route(wasm, req).await }
    };

    Ok((handler, schema, schema_routes))
}

async fn http_handle_route(wasm: Arc<Mutex<WasmLibrary>>, req: Request) -> Response {
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    if !path.starts_with("/lib/") {
        return not_found(&path);
    }

    if method != crate::http::HttpMethod::GET {
        return method_not_allowed(&method, &path);
    }

    let header = match txn_header(&req) {
        Ok(header) => header,
        Err(err) => return error_response(err),
    };
    let txn = match req.extensions().get::<TxnHandle>().cloned() {
        Some(txn) => txn,
        None => return error_response(TCError::internal("missing transaction handle")),
    };

    let body = match hyper::body::to_bytes(req.into_body()).await {
        Ok(bytes) => bytes.to_vec(),
        Err(err) => return error_response(TCError::internal(err)),
    };

    let result = {
        let mut guard = wasm.lock().await;
        let schema_id = guard.schema().id().to_string();
        let schema_rel = schema_id.strip_prefix("/lib").unwrap_or(&schema_id);

        let relative = &path["/lib".len()..];
        let normalized = if relative.starts_with('/') {
            relative
        } else {
            return not_found(&path);
        };

        let normalized = if schema_rel.is_empty() {
            normalized
        } else if let Some(normalized) = normalized.strip_prefix(schema_rel) {
            normalized
        } else {
            return not_found(&path);
        };

        let segments = match parse_route_path(normalized) {
            Ok(segments) => segments,
            Err(err) => return error_response(err),
        };

        guard.call_route(&segments, &header, &body)
    };

    match result {
        Ok(bytes) => {
            // If the WASM route returns a TinyChain ref envelope (`TCRef` or `OpRef`) as JSON,
            // resolve it within the current transaction and return the resolved state.
            if let Some(r) = try_decode_wasm_ref(&bytes).await {
                match r {
                    tc_ir::TCRef::Op(op) => match op.resolve(&txn).await {
                        Ok(state) => return state_response(state),
                        Err(err) => return error_response(err),
                    },
                    tc_ir::TCRef::Id(_) => {
                        return error_response(TCError::bad_request(
                            "cannot resolve TCRef::Id without a scope".to_string(),
                        ));
                    }
                    tc_ir::TCRef::If(_) => {
                        return error_response(TCError::bad_request(
                            "cannot resolve TCRef::If without a scope".to_string(),
                        ));
                    }
                    tc_ir::TCRef::Cond(_) => {
                        return error_response(TCError::bad_request(
                            "cannot resolve TCRef::Cond without a scope".to_string(),
                        ));
                    }
                    tc_ir::TCRef::While(_) => {
                        return error_response(TCError::bad_request(
                            "cannot resolve TCRef::While without a scope".to_string(),
                        ));
                    }
                    tc_ir::TCRef::ForEach(_) => {
                        return error_response(TCError::bad_request(
                            "cannot resolve TCRef::ForEach without a scope".to_string(),
                        ));
                    }
                }
            }

            hyper::Response::builder()
                .status(StatusCode::OK)
                .header(crate::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(bytes))
                .unwrap()
        }
        Err(err) => error_response(err),
    }
}

fn txn_header(req: &Request) -> TCResult<TxnHeader> {
    req.extensions()
        .get::<TxnHandle>()
        .cloned()
        .ok_or_else(|| TCError::internal("missing transaction handle"))
        .map(|handle| handle.header())
}

fn method_not_allowed(method: &crate::http::HttpMethod, path: &str) -> Response {
    error_response(TCError::method_not_allowed(
        method.clone(),
        path.to_string(),
    ))
}

fn not_found(path: &str) -> Response {
    error_response(TCError::not_found(path))
}

fn error_response(err: TCError) -> Response {
    let status = match err.code() {
        ErrorKind::BadGateway | ErrorKind::BadRequest => StatusCode::BAD_REQUEST,
        ErrorKind::Conflict => StatusCode::CONFLICT,
        ErrorKind::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
        ErrorKind::NotFound => StatusCode::NOT_FOUND,
        ErrorKind::Unauthorized => StatusCode::UNAUTHORIZED,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };

    hyper::Response::builder()
        .status(status)
        .header(crate::http::header::CONTENT_TYPE, "text/plain")
        .body(Body::from(err.message().to_string()))
        .unwrap()
}

fn state_response(state: tc_state::State) -> Response {
    use futures::TryStreamExt;

    match destream_json::encode(state) {
        Ok(stream) => hyper::Response::builder()
            .status(StatusCode::OK)
            .header(crate::http::header::CONTENT_TYPE, "application/json")
            .body(Body::wrap_stream(
                stream.map_err(|err| std::io::Error::other(err.to_string())),
            ))
            .unwrap(),
        Err(err) => error_response(TCError::internal(err.to_string())),
    }
}
