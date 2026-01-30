use std::{net::TcpListener, str::FromStr};

use pathlink::Link;
use tinychain::{
    HttpKernel, HttpServer, Kernel, TxnHandle,
    library::http::{build_http_library_module, http_library_handlers},
};

fn usage() -> ! {
    eprintln!("usage: http_wasm_opref_get <hello.wasm>");
    eprintln!("  - <hello.wasm> must be a TinyChain WASM library whose manifest schema id is a canonical, versioned /lib path");
    eprintln!("    (e.g. /lib/<publisher>/<name>/<semver>)");
    eprintln!("  - the library must export a GET route at /hello (served as <schema-id>/hello)");
    std::process::exit(2);
}

fn is_versioned_lib_root(path: &str) -> bool {
    let mut segments = path.trim_matches('/').split('/');
    segments.next() == Some("lib")
        && segments.next().is_some()
        && segments.next().is_some()
        && segments.next().is_some()
        && segments.next().is_none()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wasm_path = std::env::args().nth(1).unwrap_or_else(|| usage());
    let wasm_bytes = std::fs::read(wasm_path)?;

    // Host B: serve the WASM hello library over HTTP.
    let bootstrap_schema =
        tc_ir::LibrarySchema::new(Link::from_str("/lib/example-devco/bootstrap/0.0.0")?, "0.0.0", vec![]);
    let b_module = build_http_library_module(bootstrap_schema, None);
    let b_handlers = http_library_handlers(&b_module);

    // Install the library in-process (no HTTP `/lib` request envelope).
    let b_schema = b_module.install_wasm_bytes(wasm_bytes)?;
    let b_root = b_schema.id().to_string();
    if !is_versioned_lib_root(&b_root) {
        return Err(format!(
            "invalid library schema id {b_root}: expected canonical /lib/<publisher>/<name>/<semver>"
        )
        .into());
    }
    let b_hello = format!("{b_root}/hello");

    let b_kernel: HttpKernel = Kernel::builder()
        .with_host_id("tc-http-b")
        .with_library_module(b_module, b_handlers)
        .finish();

    let listener = TcpListener::bind("127.0.0.1:0")?;
    let b_addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let b_task = tokio::spawn(async move {
        let shutdown = async move {
            let _ = shutdown_rx.await;
        };
        HttpServer::new(b_kernel)
            .serve_listener_with_shutdown(listener, shutdown)
            .await
    });

    // Host A: no routes needed, just a schema which declares B as an allowed dependency.
    let a_schema = tc_ir::LibrarySchema::new(
        Link::from_str("/lib/example-devco/a/0.1.0")?,
        "0.1.0",
        vec![Link::from_str(&b_root)?],
    );
    let a_module = build_http_library_module(a_schema, None);
    let a_handlers = http_library_handlers(&a_module);

    let a_kernel: HttpKernel = Kernel::builder()
        .with_host_id("tc-http-a")
        .with_library_module(a_module, a_handlers)
        .with_dependency_route(b_root.clone(), b_addr.to_string())
        .with_http_rpc_gateway()
        .finish();

    // Begin a transaction locally. The kernel owns the txn lifecycle; callers only reference txn_id.
    let txn: TxnHandle = a_kernel
        .txn_manager()
        .begin_with_owner(Some("demo-user"), Some("demo-user"));

    let op = tc_ir::OpRef::Get((
        tc_ir::Subject::Link(Link::from_str(&b_hello)?),
        tc_ir::Scalar::Value(tc_value::Value::from("World")),
    ));

    let rpc = a_kernel.resolve_op(txn.id(), None, op).await?;
    if rpc.status != 200 {
        return Err(format!("unexpected response status: {}", rpc.status).into());
    }

    let body = String::from_utf8(rpc.body)?;
    if body != "\"Hello, World!\"" {
        return Err(format!("unexpected response body: {body}").into());
    }

    // Clean shutdown of host B.
    let _ = shutdown_tx.send(());
    let _ = b_task.await?;
    Ok(())
}
