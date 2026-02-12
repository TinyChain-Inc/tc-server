use super::*;
use crate::http::Response;
use std::str::FromStr;
use tc_ir::{Claim, LibrarySchema, NetworkTime, TxnHeader, TxnId, parse_route_path};
use umask::Mode;
use wasmtime::Engine;

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

#[cfg(all(test, feature = "http-server"))]
mod http_tests {
    use super::*;
    use crate::http::{Body, StatusCode};
    use crate::{
        HttpServer,
        kernel::{Kernel, Method},
        library::http::{build_http_library_module, http_library_handlers},
        txn::TxnManager,
    };
    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
    use hyper::{Client, body};
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
        let module = build_http_library_module(initial, None)
            .await
            .expect("module");
        let handlers = http_library_handlers(&module);

        let kernel = Kernel::builder()
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
        let install_claim = Claim::new(
            Link::from_str("/lib/example-devco/example/0.1.0").expect("install link"),
            umask::USER_WRITE,
        );
        let txn = txn_manager.begin().with_claims(vec![install_claim]);

        let mut install_request = ::http::Request::builder()
            .method("PUT")
            .uri("/lib")
            .header(crate::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(install_payload.to_string()))
            .expect("install request");
        install_request.extensions_mut().insert(txn.clone());

        let install_response = kernel
            .dispatch(Method::Put, "/lib", install_request)
            .expect("install handler")
            .await;
        assert_eq!(install_response.status(), StatusCode::NO_CONTENT);

        let mut request = ::http::Request::builder()
            .method("GET")
            .uri("/lib/example-devco/example/0.1.0/hello")
            .body(Body::empty())
            .expect("request");
        request.extensions_mut().insert(txn.clone());

        let fut = kernel
            .dispatch(
                Method::Get,
                "/lib/example-devco/example/0.1.0/hello",
                request,
            )
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

        let manifest_packed: i64 =
            (((manifest_len as u64) << 32) | (MANIFEST_OFFSET as u64)) as i64;

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
    async fn http_wasm_route_resolves_opref_via_gateway() -> Result<(), Box<dyn std::error::Error>>
    {
        let b_root = "/lib/example-devco/b/0.1.0";
        let b_hello = "/lib/example-devco/b/0.1.0/hello";
        let b_route = "/hello";

        let initial_schema =
            tc_ir::LibrarySchema::new(Link::from_str("/lib/initial")?, "0.0.1", vec![]);
        let b_module = build_http_library_module(initial_schema, None)
            .await
            .expect("module");
        let b_handlers = http_library_handlers(&b_module);

        let b_kernel = Kernel::builder()
            .with_host_id("tc-http-b")
            .with_library_module(b_module, b_handlers)
            .with_service_handler(|_req| async { Response::new(Body::empty()) })
            .with_kernel_handler(|_req| async { Response::new(Body::empty()) })
            .with_health_handler(|_req| async { Response::new(Body::empty()) })
            .finish();

        let b_schema = tc_ir::LibrarySchema::new(Link::from_str(b_root)?, "0.1.0", vec![]);
        let wasm_b =
            wasm_static_response_module(b_schema, b_route, "hello", br#""hello""#.to_vec());

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

        let mut install_request_b = ::http::Request::builder()
            .method("PUT")
            .uri("/lib")
            .header(crate::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(install_payload_b.to_string()))?;
        let install_claim_b = Claim::new(Link::from_str(b_root)?, umask::USER_WRITE);
        let install_txn_b = b_kernel
            .txn_manager()
            .begin()
            .with_claims(vec![install_claim_b]);
        install_request_b.extensions_mut().insert(install_txn_b);

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
        let a_module = build_http_library_module(a_schema.clone(), None)
            .await
            .expect("module");
        let a_handlers = http_library_handlers(&a_module);

        let a_kernel = Kernel::builder()
            .with_host_id("tc-http-a")
            .with_library_module(a_module, a_handlers)
            .with_dependency_route(b_root, b_addr)
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
        let wasm_a = wasm_static_response_module(
            a_schema,
            a_route,
            "from_b",
            wasm_response.to_string().into_bytes(),
        );

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

        let mut install_request_a = ::http::Request::builder()
            .method("PUT")
            .uri("/lib")
            .header(crate::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(install_payload_a.to_string()))?;
        let install_claim_a = Claim::new(Link::from_str(a_root)?, umask::USER_WRITE);
        let install_txn_a = a_kernel
            .txn_manager()
            .begin()
            .with_claims(vec![install_claim_a]);
        install_request_a.extensions_mut().insert(install_txn_a);

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

        let request = ::http::Request::builder()
            .method("GET")
            .uri(format!("http://{a_addr}{a_from_b}"))
            .header(hyper::header::AUTHORIZATION, "Bearer demo-user")
            .body(Body::empty())?;

        let response = Client::new().request(request).await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body::to_bytes(response.into_body()).await?;
        assert_eq!(body.as_ref(), br#"{"/state/scalar/value/string":"hello"}"#);

        a_task.abort();
        b_task.abort();
        Ok(())
    }
}
