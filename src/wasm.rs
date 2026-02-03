use std::{collections::HashMap, str::FromStr, sync::Arc};

use pathlink::PathSegment;
use serde::Deserialize;
use tc_error::{TCError, TCResult};
use tc_ir::{LibrarySchema, TxnHeader, parse_route_path};
use wasmtime::{Engine, Func, Instance, Memory, Module, Store};

use crate::KernelHandler;

#[cfg(any(feature = "http-server", feature = "pyo3"))]
use crate::library::{RouteMetadata, SchemaRoutes};

#[cfg(feature = "http-server")]
use {
    crate::txn::TxnHandle,
    hyper::{Body, Request, Response, StatusCode, body},
    tc_error::ErrorKind,
    tokio::sync::Mutex,
};

#[cfg(feature = "pyo3")]
use {
    crate::pyo3_runtime::{PyKernelRequest, PyKernelResponse, request_body_bytes},
    crate::txn::TxnHandle,
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
            .typed::<(i32, i32, i32, i32), i64>(&self.store)
            .map_err(map_wasm_error)?;
        let packed = typed
            .call(
                &mut self.store,
                (header_ptr, header_len, body_ptr, body_len),
            )
            .map_err(map_wasm_error)?;
        let (result_ptr, result_len) = unpack_wasm_pair(packed);

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
        let typed = func.typed::<(), i64>(&mut *store).map_err(map_wasm_error)?;
        let packed = typed.call(&mut *store, ()).map_err(map_wasm_error)?;
        let (ptr, len) = unpack_wasm_pair(packed);
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

fn unpack_wasm_pair(value: i64) -> (i32, i32) {
    let value = value as u64;
    let ptr = (value & 0xffff_ffff) as u32 as i32;
    let len = (value >> 32) as u32 as i32;
    (ptr, len)
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
        crate::test_utils::wasm_hello_world_module()
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
                pathlink::Link::from_str("/lib/example-devco/example/0.1.0").unwrap(),
                Mode::all(),
            ),
        );

        let path = parse_route_path("/hello").expect("route path");
        let bytes = library.call_route(&path, &header, &[]).expect("route call");

        assert_eq!(String::from_utf8(bytes).unwrap(), "\"hello\"");
    }
}

#[cfg(all(test, feature = "http-server"))]
mod http_tests {
    use super::*;
    use crate::{
        kernel::{Kernel, Method},
        library::http::{build_http_library_module, http_library_handlers},
        HttpServer,
        txn::TxnManager,
    };
    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
    use hyper::{Body, Client, Request, StatusCode, body};
    use pathlink::Link;
    use std::net::TcpListener;
    use std::str::FromStr;
    use tc_ir::{Claim, NetworkTime, Transaction, TxnId};
    use umask::Mode;
    use wasm_encoder::{
        CodeSection, ConstExpr, DataSection, ExportKind, ExportSection, Function, FunctionSection,
        GlobalSection, GlobalType, Instruction, MemorySection, MemoryType, Module, TypeSection,
        ValType,
    };

    #[tokio::test]
    async fn kernel_dispatches_wasm_route() {
        let bytes = super::tests::wasm_module();
        let initial =
            tc_ir::LibrarySchema::new(Link::from_str("/lib/initial").unwrap(), "0.0.1", vec![]);
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
                "id": "/lib/example-devco/example/0.1.0",
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
            .uri("/lib/example-devco/example/0.1.0/hello")
            .body(Body::empty())
            .expect("request");
        request.extensions_mut().insert(txn.clone());

        let fut = kernel
            .dispatch(Method::Get, "/lib/example-devco/example/0.1.0/hello", request)
            .expect("wasm handler");
        let response = fut.await;
        let body = body::to_bytes(response.into_body()).await.expect("body");
        assert_eq!(body.as_ref(), b"\"hello\"");
    }

    fn wasm_static_response_module(
        schema: LibrarySchema,
        route_path: &'static str,
        export: &'static str,
        response_bytes: Vec<u8>,
    ) -> Vec<u8> {
        const MANIFEST_OFFSET: u32 = 8;
        const RESPONSE_OFFSET: u32 = 512;
        const HEAP_INIT: u32 = 1024;

        #[derive(Clone)]
        struct FixtureTxn {
            claim: Claim,
        }

        impl Default for FixtureTxn {
            fn default() -> Self {
                Self {
                    claim: Claim::new(
                        Link::from_str("/lib/example-devco/example/0.1.0").expect("claim link"),
                        Mode::all(),
                    ),
                }
            }
        }

        impl Transaction for FixtureTxn {
            fn id(&self) -> TxnId {
                TxnId::from_parts(NetworkTime::from_nanos(1), 0).with_trace([0; 32])
            }

            fn timestamp(&self) -> NetworkTime {
                NetworkTime::from_nanos(1)
            }

            fn claim(&self) -> &Claim {
                &self.claim
            }
        }

        #[derive(Clone)]
        struct FixtureLibrary {
            schema: LibrarySchema,
            routes: tc_ir::Dir<()>,
        }

        impl FixtureLibrary {
            fn new(schema: LibrarySchema) -> Self {
                Self {
                    schema,
                    routes: tc_ir::Dir::default(),
                }
            }
        }

        impl tc_ir::Library for FixtureLibrary {
            type Txn = FixtureTxn;
            type Routes = tc_ir::Dir<()>;

            fn schema(&self) -> &LibrarySchema {
                &self.schema
            }

            fn routes(&self) -> &Self::Routes {
                &self.routes
            }
        }

        let library = FixtureLibrary::new(schema);
        let routes = [tc_wasm::RouteExport::new(route_path, export)];
        let manifest = tc_wasm::manifest_bytes(&library, &routes);
        let manifest_len: u32 = manifest.len().try_into().expect("manifest fits in u32");
        let response_len: u32 = response_bytes
            .len()
            .try_into()
            .expect("response fits in u32");

        let manifest_packed: i64 = (((manifest_len as u64) << 32) | (MANIFEST_OFFSET as u64)) as i64;

        let mut module = Module::new();

        let mut types = TypeSection::new();
        types.function([ValType::I32], [ValType::I32]); // 0 alloc
        types.function([ValType::I32, ValType::I32], []); // 1 free
        types.function([], [ValType::I64]); // 2 entry
        types.function(
            [ValType::I32, ValType::I32, ValType::I32, ValType::I32],
            [ValType::I64],
        ); // 3 route

        module.section(&types);

        let mut functions = FunctionSection::new();
        functions.function(0);
        functions.function(1);
        functions.function(2);
        functions.function(3);
        module.section(&functions);

        let mut memories = MemorySection::new();
        memories.memory(MemoryType {
            minimum: 1,
            maximum: None,
            memory64: false,
            shared: false,
            page_size_log2: None,
        });
        module.section(&memories);

        let mut globals = GlobalSection::new();
        globals.global(
            GlobalType {
                val_type: ValType::I32,
                mutable: true,
                shared: false,
            },
            &ConstExpr::i32_const(HEAP_INIT as i32),
        );
        module.section(&globals);

        let mut exports = ExportSection::new();
        exports.export("memory", ExportKind::Memory, 0);
        exports.export("alloc", ExportKind::Func, 0);
        exports.export("free", ExportKind::Func, 1);
        exports.export("tc_library_entry", ExportKind::Func, 2);
        exports.export(export, ExportKind::Func, 3);
        module.section(&exports);

        let mut code = CodeSection::new();

        // alloc(len: i32) -> i32
        let mut alloc = Function::new([(1, ValType::I32)]);
        alloc.instruction(&Instruction::GlobalGet(0));
        alloc.instruction(&Instruction::LocalSet(1));
        alloc.instruction(&Instruction::GlobalGet(0));
        alloc.instruction(&Instruction::LocalGet(0));
        alloc.instruction(&Instruction::I32Add);
        alloc.instruction(&Instruction::GlobalSet(0));
        alloc.instruction(&Instruction::LocalGet(1));
        alloc.instruction(&Instruction::End);
        code.function(&alloc);

        // free(ptr: i32, len: i32) -> ()
        let mut free = Function::new([]);
        free.instruction(&Instruction::End);
        code.function(&free);

        // tc_library_entry() -> i64
        let mut entry = Function::new([]);
        entry.instruction(&Instruction::I64Const(manifest_packed));
        entry.instruction(&Instruction::End);
        code.function(&entry);

        // route(...) -> i64
        // Allocate a response buffer, copy response bytes, return (len<<32 | ptr).
        let mut route = Function::new([(1, ValType::I32)]);
        route.instruction(&Instruction::I32Const(response_len as i32));
        route.instruction(&Instruction::Call(0));
        route.instruction(&Instruction::LocalSet(4));
        route.instruction(&Instruction::LocalGet(4));
        route.instruction(&Instruction::I32Const(RESPONSE_OFFSET as i32));
        route.instruction(&Instruction::I32Const(response_len as i32));
        route.instruction(&Instruction::MemoryCopy {
            src_mem: 0,
            dst_mem: 0,
        });
        route.instruction(&Instruction::I32Const(response_len as i32));
        route.instruction(&Instruction::I64ExtendI32U);
        route.instruction(&Instruction::I64Const(32));
        route.instruction(&Instruction::I64Shl);
        route.instruction(&Instruction::LocalGet(4));
        route.instruction(&Instruction::I64ExtendI32U);
        route.instruction(&Instruction::I64Or);
        route.instruction(&Instruction::End);
        code.function(&route);

        module.section(&code);

        let mut data = DataSection::new();
        data.active(0, &ConstExpr::i32_const(MANIFEST_OFFSET as i32), manifest);
        data.active(
            0,
            &ConstExpr::i32_const(RESPONSE_OFFSET as i32),
            response_bytes,
        );
        module.section(&data);

        module.finish()
    }

    #[tokio::test]
    async fn http_wasm_route_resolves_opref_via_gateway() -> Result<(), Box<dyn std::error::Error>> {
        let b_root = "/lib/example-devco/b/0.1.0";
        let b_hello = "/lib/example-devco/b/0.1.0/hello";
        let b_route = "/hello";

        let initial_schema =
            tc_ir::LibrarySchema::new(Link::from_str("/lib/initial")?, "0.0.1", vec![]);
        let b_module = build_http_library_module(initial_schema, None);
        let b_handlers = http_library_handlers(&b_module);

        let b_kernel: Kernel<Request<Body>, Response<Body>> = Kernel::builder()
            .with_host_id("tc-http-b")
            .with_library_module(b_module, b_handlers)
            .with_service_handler(|_req| async { Response::new(Body::empty()) })
            .with_kernel_handler(|_req| async { Response::new(Body::empty()) })
            .with_health_handler(|_req| async { Response::new(Body::empty()) })
            .finish();

        let b_schema =
            tc_ir::LibrarySchema::new(Link::from_str(b_root)?, "0.1.0", vec![]);
        let wasm_b = wasm_static_response_module(b_schema, b_route, "hello", br#""hello""#.to_vec());

        let install_payload_b = serde_json::json!({
            "schema": {
                "id": b_root,
                "version": "0.1.0",
                "dependencies": [],
            },
            "artifacts": [{
                "path": "/lib/wasm",
                "content_type": "application/wasm",
                "bytes": BASE64.encode(&wasm_b),
            }]
        });

        let install_request_b = Request::builder()
            .method("PUT")
            .uri("/lib")
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Body::from(install_payload_b.to_string()))?;

        let install_response_b = b_kernel
            .dispatch(Method::Put, "/lib", install_request_b)
            .ok_or("missing install handler for /lib")?
            .await;

        assert_eq!(install_response_b.status(), StatusCode::NO_CONTENT);

        let b_listener = TcpListener::bind("127.0.0.1:0")?;
        let b_addr = b_listener.local_addr()?;
        let b_task = {
            let kernel = b_kernel.clone();
            tokio::spawn(async move {
                let _ = HttpServer::new(kernel).serve_listener(b_listener).await;
            })
        };

        let a_root = "/lib/example-devco/a/0.1.0";
        let a_from_b = "/lib/example-devco/a/0.1.0/from_b";
        let a_route = "/from_b";

        let a_schema = tc_ir::LibrarySchema::new(
            Link::from_str(a_root)?,
            "0.1.0",
            vec![Link::from_str(b_root)?],
        );
        let a_module = build_http_library_module(a_schema.clone(), None);
        let a_handlers = http_library_handlers(&a_module);

        let a_kernel: Kernel<Request<Body>, Response<Body>> = Kernel::builder()
            .with_host_id("tc-http-a")
            .with_library_module(a_module, a_handlers)
            .with_dependency_route(b_root, b_addr.to_string())
            .with_http_rpc_gateway()
            .with_service_handler(|_req| async { Response::new(Body::empty()) })
            .with_kernel_handler(|_req| async { Response::new(Body::empty()) })
            .with_health_handler(|_req| async { Response::new(Body::empty()) })
            .finish();

        let wasm_response = {
            let mut map = serde_json::Map::new();
            map.insert(
                b_hello.to_string(),
                serde_json::Value::Array(vec![serde_json::Value::Null]),
            );
            serde_json::Value::Object(map)
        };
        let wasm_a = wasm_static_response_module(a_schema, a_route, "from_b", wasm_response.to_string().into_bytes());

        let install_payload_a = serde_json::json!({
            "schema": {
                "id": a_root,
                "version": "0.1.0",
                "dependencies": [b_root],
            },
            "artifacts": [{
                "path": "/lib/wasm",
                "content_type": "application/wasm",
                "bytes": BASE64.encode(&wasm_a),
            }]
        });

        let install_request_a = Request::builder()
            .method("PUT")
            .uri("/lib")
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Body::from(install_payload_a.to_string()))?;

        let install_response_a = a_kernel
            .dispatch(Method::Put, "/lib", install_request_a)
            .ok_or("missing install handler for /lib")?
            .await;

        assert_eq!(install_response_a.status(), StatusCode::NO_CONTENT);

        let a_listener = TcpListener::bind("127.0.0.1:0")?;
        let a_addr = a_listener.local_addr()?;
        let a_task = {
            let kernel = a_kernel.clone();
            tokio::spawn(async move {
                let _ = HttpServer::new(kernel).serve_listener(a_listener).await;
            })
        };

        let request = Request::builder()
            .method("GET")
            .uri(format!("http://{a_addr}{a_from_b}"))
            .header(hyper::header::AUTHORIZATION, "Bearer demo-user")
            .body(Body::empty())?;

        let response = Client::new().request(request).await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body::to_bytes(response.into_body()).await?;
        assert_eq!(body.as_ref(), b"\"hello\"");

        a_task.abort();
        b_task.abort();
        Ok(())
    }
}

#[cfg(feature = "http-server")]
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

#[cfg(feature = "http-server")]
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

    let kernel = req
        .extensions()
        .get::<crate::Kernel<Request<Body>, Response<Body>>>()
        .cloned();

    let body = match body::to_bytes(req.into_body()).await {
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
        } else if normalized.starts_with(schema_rel) {
            &normalized[schema_rel.len()..]
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
            // resolve it within the current transaction and return the resolved RPC response.
            if let Some(kernel) = kernel
                && let Some(r) = try_decode_wasm_ref(&bytes).await
            {
                match r {
                    tc_ir::TCRef::Op(op) => match kernel.resolve_op(header.id(), None, op).await {
                        Ok(rpc) => return rpc_response(rpc),
                        Err(err) => return error_response(err),
                    },
                }
            }

            Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(Body::from(bytes))
                .unwrap()
        }
        Err(err) => error_response(err),
    }
}

#[cfg(feature = "http-server")]
fn txn_header(req: &Request<Body>) -> TCResult<TxnHeader> {
    req.extensions()
        .get::<TxnHandle>()
        .cloned()
        .ok_or_else(|| TCError::internal("missing transaction handle"))
        .map(|handle| handle.header())
}

#[cfg(feature = "http-server")]
fn method_not_allowed(method: &hyper::Method, path: &str) -> Response<Body> {
    error_response(TCError::method_not_allowed(
        method.clone(),
        path.to_string(),
    ))
}

#[cfg(feature = "http-server")]
fn not_found(path: &str) -> Response<Body> {
    error_response(TCError::not_found(path))
}

#[cfg(feature = "http-server")]
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

#[cfg(any(feature = "http-server", feature = "pyo3"))]
async fn try_decode_wasm_ref(bytes: &[u8]) -> Option<tc_ir::TCRef> {
    if let Ok(r) = try_decode_json_slice::<tc_ir::TCRef>(bytes).await {
        return Some(r);
    }

    if let Ok(op) = try_decode_json_slice::<tc_ir::OpRef>(bytes).await {
        return Some(tc_ir::TCRef::Op(op));
    }

    None
}

#[cfg(any(feature = "http-server", feature = "pyo3"))]
async fn try_decode_json_slice<T>(bytes: &[u8]) -> Result<T, String>
where
    T: destream::de::FromStream<Context = ()>,
{
    use std::io;

    if bytes.is_empty() {
        return Err("empty payload".to_string());
    }

    let stream = futures::stream::iter(vec![Ok::<bytes::Bytes, io::Error>(
        bytes::Bytes::copy_from_slice(bytes),
    )]);
    destream_json::try_decode((), stream)
        .await
        .map_err(|err| err.to_string())
}

#[cfg(feature = "http-server")]
fn rpc_response(rpc: crate::gateway::RpcResponse) -> Response<Body> {
    let status = StatusCode::from_u16(rpc.status).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut builder = Response::builder().status(status);

    for (name, value) in rpc.headers {
        if let Ok(name) = hyper::header::HeaderName::from_str(&name)
            && let Ok(value) = hyper::header::HeaderValue::from_str(&value)
        {
            if let Some(headers) = builder.headers_mut() {
                headers.insert(name, value);
            }
        }
    }

    builder.body(Body::from(rpc.body)).unwrap()
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

    let kernel = req.kernel();
    let bearer_token = crate::pyo3_runtime::py_bearer_token(&req);

    let result = {
        let mut guard = wasm.lock().await;
        let schema_id = guard.schema().id().to_string();
        let schema_rel = schema_id.strip_prefix("/lib").unwrap_or(&schema_id);

        let normalized = if schema_rel.is_empty() {
            normalized
        } else if normalized.starts_with(schema_rel) {
            &normalized[schema_rel.len()..]
        } else {
            return Ok(PyKernelResponse::new(404, None, None));
        };

        let segments = parse_route_path(normalized)
            .map_err(|err| PyValueError::new_err(err.message().to_string()))?;

        guard.call_route(&segments, &header, &body)
    };

    match result {
        Ok(bytes) => {
            // Match the HTTP adapter behavior: if the WASM route returns a TinyChain ref envelope
            // (`TCRef`/`OpRef`) as JSON, resolve it within the current transaction and return the
            // resolved RPC response.
            if let Some(kernel) = kernel
                && let Some(r) = try_decode_wasm_ref(&bytes).await
            {
                match r {
                    tc_ir::TCRef::Op(op) => match kernel
                        .resolve_op(txn.id(), bearer_token.clone(), op)
                        .await
                    {
                        Ok(rpc) => {
                            return Python::with_gil(|py| {
                                let body = if rpc.body.is_empty() {
                                    None
                                } else {
                                    Some(crate::pyo3_runtime::PyStateHandle::new(
                                        pyo3::types::PyBytes::new_bound(py, &rpc.body).into_py(py),
                                    ))
                                };

                                Ok(PyKernelResponse::new(
                                    rpc.status,
                                    Some(rpc.headers),
                                    body,
                                ))
                            });
                        }
                        Err(err) => return Err(PyValueError::new_err(err.message().to_string())),
                    },
                }
            }

            py_wasm_response(bytes, txn).await
        }
        Err(err) => Err(PyValueError::new_err(err.message().to_string())),
    }
}

#[cfg(feature = "pyo3")]
async fn py_wasm_response(bytes: Vec<u8>, _txn: TxnHandle) -> PyResult<PyKernelResponse> {
    if bytes.is_empty() {
        return Ok(PyKernelResponse::new(200, None, None));
    }

    Python::with_gil(|py| {
        let body = match std::str::from_utf8(&bytes) {
            Ok(text) => crate::pyo3_runtime::PyStateHandle::new(
                pyo3::types::PyString::new_bound(py, text).into_py(py),
            ),
            Err(_) => crate::pyo3_runtime::PyStateHandle::new(
                pyo3::types::PyBytes::new_bound(py, &bytes).into_py(py),
            ),
        };
        Ok(PyKernelResponse::new(200, None, Some(body)))
    })
}
