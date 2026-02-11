# tc-server

`tc-server` is the transport adapter for TinyChain hosts. It wires the shared
`Kernel` into concrete runtimes (HTTP, PyO3, future WebSocket builds) without
adding bespoke routing or state machines. The crate stays thin on purpose: once
the kernel is compiled, every adapter clones the same instance so `/lib`,
`/service`, and `/state` behave identically no matter how a client connects.

## What lives here

- `kernel` – the transport-agnostic dispatcher plus helpers for binding
  transactions to requests.
- `http` – Hyper-based kernel plumbing, including helpers to hydrate
  per-library storage, serve `/lib` installs, and expose health/service metrics.
- `library` – tooling for `NativeLibrary` installers, `tc_library_routes!`,
  and route registries shared with WASM loaders.
- `txn`, `storage`, `pyo3_runtime`, and optional `wasm` support modules (including
  transaction-bound RPC resolution for `OpRef`/`Scalar::Ref` without adapter types).
- `op_plan` – the single host-side compiler for `OpDef` DAG planning; every adapter
  and installer must reuse this entrypoint rather than building execution plans
  in transport-specific code.
- Reference docs: see `AGENTS.md` for design guardrails and
  `PROTOCOL_COMPATIBILITY.md` for adapter expectations.

## Library discovery & export scope

Library discovery is served as a directory listing at the library root and at
any library namespace prefix. Each listing maps the immediate child segment to
a boolean flag indicating whether that child is a namespace (true) or a leaf
library (false). Requests that resolve to a leaf return the library schema.

Replication exports are always leaf-scoped: the export endpoint checks for a
claim on the concrete library ID (publisher + name + version) and returns only
that library’s payload. There is no global export of all libraries.

## Execution semantics

- **Scoped numeric ops:** During OpDef execution, a `POST` OpRef whose subject is a scoped ref
  like `$x/add` mirrors v1 behavior: the left operand is the subject ref (`$x`), and the right
  operand is passed as `{ "r": <number> }`. This implicit-left rule is only defined for `add`.
  All other subjects must resolve to concrete links (or `$self` in a library context).

## Building & testing

```bash
# HTTP server build (default features already enable it)
cargo build -p tc-server --features http-server

# HTTP client-only build (for PyO3 in-process hosts which proxy to remote HTTP hosts)
cargo build -p tc-server --no-default-features --features "http-client"

# PyO3 host (requires working Python toolchain)
cargo build -p tc-server --features "http-server pyo3"

# Run the crate’s test suite
cargo test -p tc-server --all-features
```

When developing the PyO3 adapter, also run the TinyChain Python client
integration tests (if available in your environment) to keep the shared
transaction flow in sync.

## Node binary (tc-server)

The repo ships a standalone `tc-server` binary for node operators. It exposes
the HTTP adapter and optional discovery at startup.

```bash
cargo build --bin tc-server --features \"http-server mdns k8s\"
```

Environment configuration:

- `TC_BIND` (default `0.0.0.0:8702`)
- `TC_DATA_DIR` (default `/tmp/tinychain`)
- `TC_PSK_HEX` (comma-separated hex keys)
- `TC_PEERS` (comma-separated `host:port` entries)
- `TC_K8S_DNS` / `TC_K8S_PORT` (headless service discovery)
- `TC_MDNS` (set to `1` to enable mDNS discovery)

## Examples

```bash
# See the TinyChain Python client repo for an end-to-end PyO3 + WASM + remote OpRef
# integration example which exercises the in-process kernel against a remote host.
```

To build the `tinychain` PyO3 module and run Python integration tests, follow the
setup instructions provided by the TinyChain Python client tooling you are using.

## HTTP quickstart

Use the curated builders so every adapter shares the same kernel wiring:

```rust
use hyper::{Body, Request, Response};
use std::convert::Infallible;
use tc_server::http::{
    HttpKernelConfig, HttpServer, build_http_kernel_with_config,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Point at a library data dir if you ship persisted WASM installs.
    let kernel = build_http_kernel_with_config(
        HttpKernelConfig::default().with_data_dir("./data"),
        |_req: Request<Body>| async {
            Ok::<_, Infallible>(Response::new(Body::from("service ok")))
        },
        |_req: Request<Body>| async {
            Ok::<_, Infallible>(Response::new(Body::from("host metrics")))
        },
        |_req: Request<Body>| async {
            Ok::<_, Infallible>(Response::new(Body::from("healthy")))
        },
    ).await?;

    HttpServer::new(kernel)
        .serve(([127, 0, 0, 1], 8700).into())
        .await?;

    Ok(())
}
```

Handlers only need to implement `Fn(Request<Body>) -> impl Future<Output =
Response<Body>>`. The kernel takes care of parsing transaction IDs and routing
top-level paths (`/lib`, `/service`, `/healthz`, etc.).

## Testnet CI and Kubernetes (node-only)

The testnet CI assets live in the monorepo root (not inside the `tc-server`
submodule). Run testnet commands from the repo root; see
`../ci/testnet/README.md` and `../k8s/README.md` for the Kind smoke test and
cloud deploy instructions.

## Discovery feature flags (node-only)

Peer discovery is feature-flagged to keep the default build minimal:

- `mdns`: LAN discovery for local/dev clusters.
- `k8s`: headless-service DNS discovery for Kubernetes clusters.

Once these flags land, build with them explicitly (example):

```bash
cargo build --features "http-server mdns"
```

### PSK security note (node-only)

PSKs protect the encrypted token exchange used for bootstrap replication. Use
TLS for any non-local deployment and rotate PSKs regularly. Treat the library
export path as high-value and keep it gated behind private networks or explicit
allowlists.

## Native `Library` example

`NativeLibrary` lets you publish in-process handlers without crossing the WASM
ABI. Pair it with the HTTP kernel to expose `/lib/...` routes immediately:

```rust
use hyper::{Body, Request, Response};
use pathlink::Link;
use tc_ir::{HandleGet, LibraryModule, LibrarySchema, tc_library_routes};
use tc_server::{
    http::{HttpServer, build_http_kernel_with_native_library},
    library::NativeLibrary,
    txn::TxnHandle,
    Value,
};

#[derive(Clone)]
struct Hello;

impl HandleGet<TxnHandle> for Hello {
    type Request = Value;
    type RequestContext = ();
    type Response = Value;
    type Error = tc_error::TCError;
    type Fut<'a> =
        futures::future::BoxFuture<'a, Result<Self::Response, Self::Error>>;

    fn get<'a>(
        &'a self,
        _txn: &'a TxnHandle,
        _request: Self::Request,
    ) -> tc_error::TCResult<Self::Fut<'a>> {
        Ok(Box::pin(async move { Ok(Value::from(42_u64)) }))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use std::str::FromStr;

    let schema = LibrarySchema::new(
        Link::from_str("/lib/examples/hello")?,
        "0.1.0",
        vec![],
    );
    let routes = tc_library_routes! { "/hello" => Hello }?;
    let module = LibraryModule::new(schema, routes);
    let library = NativeLibrary::new(module);

    let kernel = build_http_kernel_with_native_library(
        library,
        |_req: Request<Body>| async { Ok(Response::new(Body::empty())) },
        |_req: Request<Body>| async { Ok(Response::new(Body::empty())) },
        |_req: Request<Body>| async { Ok(Response::new(Body::from("ok"))) },
    );

    HttpServer::new(kernel)
        .serve(([127, 0, 0, 1], 8701).into())
        .await?;
    Ok(())
}
```

- `tc_ir/examples/hello_library.rs` provides a standalone example of building a
  `LibraryModule` and dispatching handlers without HTTP.
- `tc-server/src/http.rs` includes the `serves_native_library_route` async test,
  which exercises the `/lib/hello` path end-to-end and doubles as a reference
  client/server exchange.

## PyO3 adapter

Enable the `pyo3` feature to build the native Python module (`tinychain`).
`pyo3_runtime.rs` exposes `python_kernel_builder_with_config`, mirroring the HTTP
helpers so Python callers see the same `/lib` manifest and transact against the
shared kernel. Reuse the same `NativeLibrary` or WASM installs you would ship to
HTTP—adapters never diverge.

## Further reading

- `AGENTS.md` – crate-specific invariants for adapters, storage layout, and
  transaction orchestration.
- `PROTOCOL_COMPATIBILITY.md` – compatibility matrix for adapters/features.
- `ROADMAP.md` – upcoming work items and sequencing.
- Workspace-level `ARCHITECTURE.md` and `CODE_STYLE.md` for broader guidance.
