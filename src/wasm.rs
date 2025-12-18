use std::{collections::HashMap, str::FromStr, sync::Arc};

use pathlink::PathSegment;
use serde::Deserialize;
use tc_error::{TCError, TCResult};
use tc_ir::{LibrarySchema, TxnHeader, parse_route_path};
use wasmtime::{Engine, Func, Instance, Memory, Module, Store};

use crate::KernelHandler;

#[cfg(any(feature = "http", feature = "pyo3"))]
use crate::library::{RouteMetadata, SchemaRoutes};

#[cfg(feature = "http")]
use {
    crate::txn::TxnHandle,
    hyper::{Body, Request, Response, StatusCode, body},
    tc_error::ErrorKind,
    tokio::sync::Mutex,
};

#[cfg(feature = "pyo3")]
use {
    crate::pyo3_runtime::{
        PyKernelRequest, PyKernelResponse, decode_state_from_bytes_async,
        py_state_handle_from_state, request_body_bytes,
    },
    futures::lock::Mutex as AsyncMutex,
    pyo3::{exceptions::PyValueError, prelude::*},
};

/// Loads a TinyChain-compatible Library embedded in a WASM module.
pub struct WasmLibrary {
    store: Store<()>,
    memory: Memory,
    alloc: Func,
    free: Func,
    schema: LibrarySchema,
    routes: HashMap<Vec<PathSegment>, Func>,
    bindings: Vec<RouteBinding>,
}

impl WasmLibrary {
    pub fn from_bytes(engine: &Engine, bytes: &[u8]) -> TCResult<Self> {
        let module = Module::new(engine, bytes).map_err(map_wasm_error)?;
        let mut store = Store::new(engine, ());
        let instance = Instance::new(&mut store, &module, &[]).map_err(map_wasm_error)?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| TCError::internal("WASM module must export memory"))?;
        let alloc = instance
            .get_func(&mut store, "alloc")
            .ok_or_else(|| TCError::internal("WASM module must export alloc"))?;
        let free = instance
            .get_func(&mut store, "free")
            .ok_or_else(|| TCError::internal("WASM module must export free"))?;

        let (schema, bindings) = Self::load_manifest(&mut store, &instance, &memory)?;

        let mut route_map = HashMap::new();
        for binding in &bindings {
            let func = instance
                .get_func(&mut store, &binding.export)
                .ok_or_else(|| {
                    TCError::internal(format!(
                        "missing export {} for route {}",
                        binding.export,
                        format_path(&binding.path)
                    ))
                })?;
            route_map.insert(binding.path.clone(), func);
        }

        Ok(Self {
            store,
            memory,
            alloc,
            free,
            schema,
            routes: route_map,
            bindings,
        })
    }

    pub fn schema(&self) -> &LibrarySchema {
        &self.schema
    }

    pub fn routes(&self) -> impl Iterator<Item = &Vec<PathSegment>> {
        self.routes.keys()
    }

    pub(crate) fn bindings(&self) -> &[RouteBinding] {
        &self.bindings
    }

    pub fn call_route(
        &mut self,
        path: &[PathSegment],
        header: &TxnHeader,
        body: &[u8],
    ) -> TCResult<Vec<u8>> {
        let func = self
            .routes
            .get(path)
            .cloned()
            .ok_or_else(|| TCError::not_found(format_path(path)))?;

        let header_bytes = encode_json(header)?;
        let (header_ptr, header_len) = self.write_buffer(&header_bytes)?;
        let (body_ptr, body_len) = if body.is_empty() {
            (0, 0)
        } else {
            self.write_buffer(body)?
        };

        let typed = func
            .typed::<(i32, i32, i32, i32), (i32, i32)>(&self.store)
            .map_err(map_wasm_error)?;
        let (result_ptr, result_len) = typed
            .call(
                &mut self.store,
                (header_ptr, header_len, body_ptr, body_len),
            )
            .map_err(map_wasm_error)?;

        let bytes = self.read_buffer(result_ptr, result_len)?;
        if header_len > 0 {
            self.free_buffer(header_ptr, header_len)?;
        }
        if body_len > 0 {
            self.free_buffer(body_ptr, body_len)?;
        }
        self.free_buffer(result_ptr, result_len)?;

        Ok(bytes)
    }

    fn load_manifest(
        store: &mut Store<()>,
        instance: &Instance,
        memory: &Memory,
    ) -> TCResult<(LibrarySchema, Vec<RouteBinding>)> {
        let func = instance
            .get_func(&mut *store, "tc_library_entry")
            .ok_or_else(|| TCError::internal("missing tc_library_entry export"))?;
        let typed = func
            .typed::<(), (i32, i32)>(&mut *store)
            .map_err(map_wasm_error)?;
        let (ptr, len) = typed.call(&mut *store, ()).map_err(map_wasm_error)?;
        let bytes = read_memory(store, memory, ptr, len)?;
        let manifest = decode_manifest(bytes)?;
        Ok((manifest.schema, manifest.routes))
    }

    fn write_buffer(&mut self, data: &[u8]) -> TCResult<(i32, i32)> {
        if data.is_empty() {
            return Ok((0, 0));
        }

        let len = i32::try_from(data.len())
            .map_err(|_| TCError::bad_request("payload too large for wasm memory"))?;
        let alloc = self
            .alloc
            .typed::<i32, i32>(&self.store)
            .map_err(map_wasm_error)?;
        let ptr = alloc.call(&mut self.store, len).map_err(map_wasm_error)?;
        self.memory
            .write(&mut self.store, ptr as usize, data)
            .map_err(map_wasm_error)?;
        Ok((ptr, len))
    }

    fn read_buffer(&mut self, ptr: i32, len: i32) -> TCResult<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        read_memory(&mut self.store, &self.memory, ptr, len)
    }

    fn free_buffer(&mut self, ptr: i32, len: i32) -> TCResult<()> {
        if len == 0 {
            return Ok(());
        }
        let free = self
            .free
            .typed::<(i32, i32), ()>(&self.store)
            .map_err(map_wasm_error)?;
        free.call(&mut self.store, (ptr, len))
            .map_err(map_wasm_error)?;
        Ok(())
    }
}

fn read_memory(store: &mut Store<()>, memory: &Memory, ptr: i32, len: i32) -> TCResult<Vec<u8>> {
    let mut buf = vec![0u8; len as usize];
    memory
        .read(store, ptr as usize, &mut buf)
        .map_err(map_wasm_error)?;
    Ok(buf)
}

fn map_wasm_error<E: std::fmt::Display>(err: E) -> TCError {
    TCError::internal(format!("wasm error: {err}"))
}

fn encode_json(header: &TxnHeader) -> TCResult<Vec<u8>> {
    serde_json::to_vec(header)
        .map_err(|err| TCError::internal(format!("failed to encode txn header: {err}")))
}

fn decode_manifest(bytes: Vec<u8>) -> TCResult<WasmManifest> {
    let manifest: ManifestSerde = serde_json::from_slice(&bytes)
        .map_err(|err| TCError::bad_request(format!("invalid wasm manifest: {err}")))?;
    let schema = parse_schema_value(&manifest.schema)?;
    let routes = manifest
        .routes
        .into_iter()
        .map(|entry| {
            let path = parse_route_path(&entry.path)
                .map_err(|err| TCError::bad_request(err.message().to_string()))?;
            Ok(RouteBinding {
                path,
                export: entry.export,
            })
        })
        .collect::<TCResult<Vec<_>>>()?;
    Ok(WasmManifest::new(schema, routes))
}

fn parse_schema_value(value: &serde_json::Value) -> TCResult<LibrarySchema> {
    let id = value
        .get("id")
        .and_then(|id| id.as_str())
        .ok_or_else(|| TCError::bad_request("manifest schema missing id"))?;
    let version = value
        .get("version")
        .and_then(|v| v.as_str())
        .ok_or_else(|| TCError::bad_request("manifest schema missing version"))?;
    let dependencies = value
        .get("dependencies")
        .and_then(|deps| deps.as_array())
        .ok_or_else(|| TCError::bad_request("manifest schema missing dependencies array"))?;

    let id = pathlink::Link::from_str(id)
        .map_err(|err| TCError::bad_request(format!("invalid schema id: {err}")))?;
    let deps = dependencies
        .iter()
        .map(|dep| {
            dep.as_str()
                .ok_or_else(|| TCError::bad_request("dependency must be a string"))
                .and_then(|link| {
                    pathlink::Link::from_str(link).map_err(|err| {
                        TCError::bad_request(format!("invalid dependency id: {err}"))
                    })
                })
        })
        .collect::<TCResult<Vec<_>>>()?;

    Ok(LibrarySchema::new(id, version, deps))
}

#[derive(Debug)]
struct WasmManifest {
    schema: LibrarySchema,
    routes: Vec<RouteBinding>,
}

#[derive(Debug)]
pub(crate) struct RouteBinding {
    path: Vec<PathSegment>,
    export: String,
}

impl WasmManifest {
    fn new(schema: LibrarySchema, routes: Vec<RouteBinding>) -> Self {
        Self { schema, routes }
    }
}

#[derive(Deserialize)]
struct ManifestSerde {
    schema: serde_json::Value,
    routes: Vec<RouteEntrySerde>,
}

#[derive(Deserialize)]
struct RouteEntrySerde {
    path: String,
    export: String,
}

fn format_path(path: &[PathSegment]) -> String {
    format!(
        "/{}",
        path.iter()
            .map(PathSegment::to_string)
            .collect::<Vec<_>>()
            .join("/")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use tc_ir::{Claim, NetworkTime, TxnId};
    use umask::Mode;

    pub(super) fn wasm_module() -> Vec<u8> {
        let manifest = r#"{"schema":{"id":"/library/example","version":"0.1.0","dependencies":[]},"routes":[{"path":"/hello","export":"hello"}]}"#;
        let response = r#""hello""#;

        let wat = format!(
            r#"(module
                (memory (export "memory") 1)
                (global $heap (mut i32) (i32.const 1024))
                (data (i32.const 8) "{manifest}")
                (data (i32.const 512) "{response}")
                (func (export "alloc") (param i32) (result i32)
                    (local $addr i32)
                    (local.set $addr (global.get $heap))
                    (global.set $heap (i32.add (global.get $heap) (local.get 0)))
                    (local.get $addr)
                )
                (func (export "free") (param i32) (param i32))
                (func (export "tc_library_entry") (result i32 i32)
                    (i32.const 8)
                    (i32.const {manifest_len})
                )
                (func (export "hello") (param i32 i32 i32 i32) (result i32 i32)
                    (i32.const 512)
                    (i32.const {response_len})
                )
            )"#,
            manifest = escape_wat_string(manifest),
            response = escape_wat_string(response),
            manifest_len = manifest.len(),
            response_len = response.len()
        );

        wat::parse_str(wat).expect("valid wat")
    }

    fn escape_wat_string(input: &str) -> String {
        input
            .replace("\\", "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
            .replace('\r', "\\r")
    }

    #[test]
    fn loads_manifest_and_invokes_route() {
        let engine = Engine::default();
        let bytes = wasm_module();
        let mut library = WasmLibrary::from_bytes(&engine, &bytes).expect("library");
        assert_eq!(library.schema().version(), "0.1.0");

        let header = TxnHeader::new(
            TxnId::from_parts(NetworkTime::from_nanos(1), 0),
            NetworkTime::from_nanos(1),
            Claim::new(
                pathlink::Link::from_str("/library/example").unwrap(),
                Mode::all(),
            ),
        );

        let path = parse_route_path("/hello").expect("route path");
        let bytes = library.call_route(&path, &header, &[]).expect("route call");

        assert_eq!(String::from_utf8(bytes).unwrap(), "\"hello\"");
    }
}

#[cfg(all(test, feature = "http"))]
mod http_tests {
    use super::*;
    use crate::{
        kernel::{Kernel, Method},
        library::http::{build_http_library_module, http_library_handlers},
        txn::TxnManager,
    };
    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
    use hyper::{Body, Request};
    use pathlink::Link;
    use std::str::FromStr;

    #[tokio::test]
    async fn kernel_dispatches_wasm_route() {
        let bytes = super::tests::wasm_module();
        let initial =
            tc_ir::LibrarySchema::new(Link::from_str("/library/initial").unwrap(), "0.0.1", vec![]);
        let module = build_http_library_module(initial, None);
        let handlers = http_library_handlers(&module);

        let kernel = Kernel::<Request<Body>, Response<Body>>::builder()
            .with_host_id("tc-wasm-test")
            .with_library_module(module, handlers)
            .with_service_handler(|_req| async { Response::new(Body::empty()) })
            .with_kernel_handler(|_req| async { Response::new(Body::empty()) })
            .with_health_handler(|_req| async { Response::new(Body::empty()) })
            .finish();

        let install_payload = serde_json::json!({
            "schema": {
                "id": "/library/example",
                "version": "0.1.0",
                "dependencies": []
            },
            "artifacts": [{
                "path": "/lib/wasm",
                "content_type": "application/wasm",
                "bytes": BASE64.encode(&bytes),
            }]
        });

        let txn_manager = TxnManager::with_host_id("tc-wasm-test");
        let txn = txn_manager.begin();

        let mut install_request = Request::builder()
            .method("PUT")
            .uri("/lib")
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Body::from(install_payload.to_string()))
            .expect("install request");
        install_request.extensions_mut().insert(txn.clone());

        let install_response = kernel
            .dispatch(Method::Put, "/lib", install_request)
            .expect("install handler")
            .await;
        assert_eq!(install_response.status(), StatusCode::NO_CONTENT);

        let mut request = Request::builder()
            .method("GET")
            .uri("/lib/hello")
            .body(Body::empty())
            .expect("request");
        request.extensions_mut().insert(txn.clone());

        let fut = kernel
            .dispatch(Method::Get, "/lib/hello", request)
            .expect("wasm handler");
        let response = fut.await;
        let body = body::to_bytes(response.into_body()).await.expect("body");
        assert_eq!(body.as_ref(), b"\"hello\"");
    }
}

#[cfg(feature = "http")]
pub fn http_wasm_route_handler_from_bytes(
    bytes: Vec<u8>,
) -> TCResult<(
    impl KernelHandler<Request<Body>, Response<Body>>,
    LibrarySchema,
    SchemaRoutes,
)> {
    let engine = Engine::default();
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

    let handler = move |req: Request<Body>| {
        let wasm = shared.clone();
        async move { http_handle_route(wasm, req).await }
    };

    Ok((handler, schema, schema_routes))
}

#[cfg(feature = "http")]
async fn http_handle_route(wasm: Arc<Mutex<WasmLibrary>>, req: Request<Body>) -> Response<Body> {
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    if !path.starts_with("/lib/") {
        return not_found(&path);
    }

    if method != hyper::Method::GET {
        return method_not_allowed(&method, &path);
    }

    let header = match txn_header(&req) {
        Ok(header) => header,
        Err(err) => return error_response(err),
    };

    let body = match body::to_bytes(req.into_body()).await {
        Ok(bytes) => bytes.to_vec(),
        Err(err) => return error_response(TCError::internal(err)),
    };

    let relative = &path["/lib".len()..];
    let normalized = if relative.starts_with('/') {
        relative
    } else {
        return not_found(&path);
    };

    let segments = match parse_route_path(normalized) {
        Ok(segments) => segments,
        Err(err) => return error_response(err),
    };

    let result = {
        let mut guard = wasm.lock().await;
        guard.call_route(&segments, &header, &body)
    };

    match result {
        Ok(bytes) => Response::builder()
            .status(StatusCode::OK)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Body::from(bytes))
            .unwrap(),
        Err(err) => error_response(err),
    }
}

#[cfg(feature = "http")]
fn txn_header(req: &Request<Body>) -> TCResult<TxnHeader> {
    req.extensions()
        .get::<TxnHandle>()
        .cloned()
        .ok_or_else(|| TCError::internal("missing transaction handle"))
        .map(|handle| handle.header())
}

#[cfg(feature = "http")]
fn method_not_allowed(method: &hyper::Method, path: &str) -> Response<Body> {
    error_response(TCError::method_not_allowed(
        method.clone(),
        path.to_string(),
    ))
}

#[cfg(feature = "http")]
fn not_found(path: &str) -> Response<Body> {
    error_response(TCError::not_found(path))
}

#[cfg(feature = "http")]
fn error_response(err: TCError) -> Response<Body> {
    let status = match err.code() {
        ErrorKind::BadGateway | ErrorKind::BadRequest => StatusCode::BAD_REQUEST,
        ErrorKind::Conflict => StatusCode::CONFLICT,
        ErrorKind::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
        ErrorKind::NotFound => StatusCode::NOT_FOUND,
        ErrorKind::Unauthorized => StatusCode::UNAUTHORIZED,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };

    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "text/plain")
        .body(Body::from(err.message().to_string()))
        .unwrap()
}

#[cfg(feature = "pyo3")]
pub fn pyo3_wasm_route_handler_from_bytes(
    bytes: Vec<u8>,
) -> TCResult<(
    impl KernelHandler<PyKernelRequest, PyResult<PyKernelResponse>>,
    LibrarySchema,
    SchemaRoutes,
)> {
    let engine = Engine::default();
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
    let shared = Arc::new(AsyncMutex::new(wasm));

    let handler = move |req: PyKernelRequest| {
        let wasm = shared.clone();
        async move { py_handle_route(wasm, req).await }
    };

    Ok((handler, schema, schema_routes))
}

#[cfg(feature = "pyo3")]
async fn py_handle_route(
    wasm: Arc<AsyncMutex<WasmLibrary>>,
    req: PyKernelRequest,
) -> PyResult<PyKernelResponse> {
    let method = req.method_enum();
    if method != crate::Method::Get {
        return Err(PyValueError::new_err(format!(
            "method {method} not supported for wasm routes"
        )));
    }

    let txn = req
        .txn_handle()
        .ok_or_else(|| PyValueError::new_err("missing transaction handle"))?;
    let header = txn.header();

    let path = req.path_owned();
    if !path.starts_with("/lib/") {
        return Ok(PyKernelResponse::new(404, None, None));
    }

    let body = request_body_bytes(req.body())?;

    let relative = &path["/lib".len()..];
    let normalized = if relative.starts_with('/') {
        relative
    } else {
        return Ok(PyKernelResponse::new(404, None, None));
    };

    let segments = parse_route_path(normalized)
        .map_err(|err| PyValueError::new_err(err.message().to_string()))?;

    let result = {
        let mut guard = wasm.lock().await;
        guard.call_route(&segments, &header, &body)
    };

    match result {
        Ok(bytes) => py_wasm_response(bytes, txn).await,
        Err(err) => Err(PyValueError::new_err(err.message().to_string())),
    }
}

#[cfg(feature = "pyo3")]
async fn py_wasm_response(bytes: Vec<u8>, txn: TxnHandle) -> PyResult<PyKernelResponse> {
    if bytes.is_empty() {
        return Ok(PyKernelResponse::new(200, None, None));
    }

    let decoded = decode_state_from_bytes_async(bytes, txn)
        .await
        .map_err(PyValueError::new_err)?;

    Python::with_gil(|py| {
        let body = py_state_handle_from_state(py, decoded)?;
        Ok(PyKernelResponse::new(200, None, Some(body)))
    })
}
