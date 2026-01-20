use std::net::TcpListener;
use std::str::FromStr;

use base64::Engine as _;
use hyper::{Body, Request, Response, StatusCode};
use tinychain::{
    HttpServer, Kernel, KernelHandler, Method,
    library::http::{build_http_library_module, http_library_handlers},
};

fn escape_wat_string(input: &str) -> String {
    input
        .replace("\\", "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn wasm_hello_module(lib_id: &str) -> Vec<u8> {
    let manifest = serde_json::json!({
        "schema": {
            "id": lib_id,
            "version": "0.1.0",
            "dependencies": [],
        },
        "routes": [{
            "path": "/hello",
            "export": "hello",
        }]
    })
    .to_string();

    let response = r#""hello""#;
    let manifest_packed = ((manifest.len() as u64) << 32) | 8u64;
    let response_packed = ((response.len() as u64) << 32) | 512u64;

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
            (func (export "tc_library_entry") (result i64)
                (i64.const {manifest_packed})
            )
            (func (export "hello") (param i32 i32 i32 i32) (result i64)
                (i64.const {response_packed})
            )
        )"#,
        manifest = escape_wat_string(&manifest),
        response = escape_wat_string(response),
        manifest_packed = manifest_packed,
        response_packed = response_packed
    );

    wat::parse_str(wat).expect("valid wat")
}

fn ok_handler() -> impl KernelHandler<Request<Body>, Response<Body>> {
    |_req: Request<Body>| {
        Box::pin(async move {
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::empty())
                .expect("ok response")
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let b_root = "/lib/example-devco/b/0.1.0";
    let b_hello = format!("{b_root}/hello");

    let initial_schema = tc_ir::LibrarySchema::new(
        pathlink::Link::from_str("/lib/initial")?,
        "0.0.1",
        vec![],
    );
    let module = build_http_library_module(initial_schema, None);
    let handlers = http_library_handlers(&module);

    let remote_kernel: Kernel<Request<Body>, Response<Body>> = Kernel::builder()
        .with_host_id("tc-http-remote")
        .with_library_module(module, handlers)
        .with_service_handler(ok_handler())
        .with_kernel_handler(ok_handler())
        .with_health_handler(ok_handler())
        .finish();

    let wasm_bytes = wasm_hello_module(b_root);
    let install_payload = serde_json::json!({
        "schema": {
            "id": b_root,
            "version": "0.1.0",
            "dependencies": [],
        },
        "artifacts": [{
            "path": "/lib/wasm",
            "content_type": "application/wasm",
            "bytes": base64::engine::general_purpose::STANDARD.encode(&wasm_bytes),
        }]
    });

    let install_request = Request::builder()
        .method("PUT")
        .uri("/lib")
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Body::from(install_payload.to_string()))?;

    let install_response = remote_kernel
        .dispatch(Method::Put, "/lib", install_request)
        .ok_or("missing install handler for /lib")?
        .await;

    if install_response.status() != StatusCode::NO_CONTENT {
        return Err(format!("install failed with status {}", install_response.status()).into());
    }

    let txn_id = remote_kernel.txn_manager().begin().id();

    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;

    let server_task = {
        let kernel = remote_kernel.clone();
        tokio::spawn(async move {
            let _ = HttpServer::new(kernel).serve_listener(listener).await;
        })
    };

    println!("remote host: http://{addr}");
    println!("installed B: {b_root}");

    let a_schema = tc_ir::LibrarySchema::new(
        pathlink::Link::from_str("/lib/example-devco/a/0.1.0")?,
        "0.1.0",
        vec![pathlink::Link::from_str(b_root)?],
    );
    let module = build_http_library_module(a_schema, None);
    let handlers = http_library_handlers(&module);

    let local_kernel: Kernel<Request<Body>, Response<Body>> = Kernel::builder()
        .with_host_id("tc-http-local")
        .with_library_module(module, handlers)
        .with_dependency_route(b_root, addr.to_string())
        .with_http_rpc_gateway()
        .finish();

    let op = tc_ir::OpRef::Get((
        tc_ir::Subject::Link(pathlink::Link::from_str(&b_hello)?),
        tc_ir::Scalar::default(),
    ));

    let response = local_kernel.resolve_op(txn_id, None, op).await?;
    println!("A → B GET {b_hello}: {}", response.status);
    println!("{}", String::from_utf8_lossy(&response.body));

    server_task.abort();
    Ok(())
}

