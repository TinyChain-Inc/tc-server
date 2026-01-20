use std::{env, io::Write, net::TcpListener, str::FromStr};

use base64::Engine as _;
use futures::FutureExt;
use hyper::{Body, Request, Response, StatusCode};
use pathlink::{Id, Link, PathBuf};
use tinychain::{HttpKernelConfig, HttpServer, Method, build_http_kernel_with_config};

fn lib_id(publisher: &str, name: &str, version: &str) -> Link {
    fn segment(s: &str) -> Id {
        s.parse().expect("valid path segment")
    }

    Link::from((
        None,
        PathBuf::from_slice(&[
            segment("lib"),
            segment(publisher),
            segment(name),
            segment(version),
        ]),
    ))
}

fn wasm_hello_module() -> Vec<u8> {
    let response = r#""hello""#;
    let response_packed = ((response.len() as u64) << 32) | 512u64;

    fn escape_wat_string(input: &str) -> String {
        input
            .replace("\\", "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
            .replace('\r', "\\r")
    }

    let lib_id = lib_id("example-devco", "b", "0.1.0");
    let manifest = serde_json::json!({
        "schema": {
            "id": lib_id.to_string(),
            "version": "0.1.0",
            "dependencies": [],
        },
        "routes": [{
            "path": "/hello",
            "export": "hello",
        }]
    })
    .to_string();
    let manifest_packed = ((manifest.len() as u64) << 32) | 8u64;

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind = env::args()
        .skip(1)
        .find_map(|arg| arg.strip_prefix("--bind=").map(str::to_string))
        .unwrap_or_else(|| "127.0.0.1:0".to_string());
    let bind_addr = std::net::SocketAddr::from_str(&bind)?;

    let lib_id = lib_id("example-devco", "b", "0.1.0");

    let ok_handler = |_req: Request<Body>| {
        async move {
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::empty())
                .expect("ok response")
        }
        .boxed()
    };

    let kernel = build_http_kernel_with_config(
        HttpKernelConfig::default().with_host_id("tc-http-rpc-host"),
        ok_handler.clone(),
        ok_handler.clone(),
        ok_handler,
    );

    let wasm_bytes = wasm_hello_module();
    let install_payload = serde_json::json!({
        "schema": {
            "id": lib_id.to_string(),
            "version": "0.1.0",
            "dependencies": []
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

    if let Some(fut) = kernel.dispatch(Method::Put, "/lib", install_request) {
        let response = fut.await;
        if response.status() != StatusCode::NO_CONTENT {
            let status = response.status();
            return Err(format!("install failed with status {status}").into());
        }
    } else {
        return Err("install handler missing for /lib".into());
    }

    let listener = TcpListener::bind(bind_addr)?;
    let addr = listener.local_addr()?;
    println!("{addr}");
    eprintln!("installed {lib_id}");
    std::io::stdout().flush().ok();

    HttpServer::new(kernel).serve_listener(listener).await?;
    Ok(())
}
