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
- `TC_DATA_DIR` (default `./var/tinychain`)
- `TC_PSK_HEX` (comma-separated hex keys)
- `TC_CLUSTER_ROOT` (default `/lib/example-devco`, used for cluster-scoped peer routes like `<cluster-root>/_cluster/peers`)
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
use hyper::{Body, Response};
use tinychain::{HostLimits, HostStorage};
use tinychain::http::{
    HttpKernelConfig, HttpServer, build_http_runtime_with_config,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = HostStorage::new(&HostLimits::default().storage);
    let library_store = storage.library_store("./data").await?;
    let workspace = storage.workspace("./workspace")?;
    let runtime = build_http_runtime_with_config(
        HttpKernelConfig::default()
            .with_library_store(library_store)
            .with_workspace(workspace),
        |_req| async { Response::new(Body::from("service ok")) },
        |_req| async { Response::new(Body::from("healthy")) },
        |_registry| {
            |_req| async { Response::new(Body::from("host metrics")) }
        },
        |_registry, builder| builder,
    ).await?;

    HttpServer::new(runtime.kernel, runtime.router)
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

## Auth Tokens and Trusted Installers

TinyChain v0.17 uses `rjwt` for recursive bearer tokens. New server-generated
actors and Python-minted install/runtime tokens use Falcon-512 by default. The
verifier still accepts legacy Ed25519 RJWT credentials so existing installers can
be rotated without downtime.

Trusted installer entries continue to use `host`, `actor_id`, `public_key_b64`,
and `allowed_lib_prefixes`. The public key bytes are parsed by `rjwt`, not by
TinyChain-specific key-length logic, so the same field can carry Falcon-512 keys
for new credentials or Ed25519 keys during the migration window.

Do not implement JWT parsing or signature handling in server adapters. HTTP,
PyO3, replication, and WASM route context all flow through the kernel
`TokenVerifier`.

## Native Libraries

Installed IR Libraries compile into the native `LibraryRegistry` and invoke one
another through the kernel without HTTP or serialization. Their schemas and
artifacts persist through the bootstrap-injected `LibraryStore`; collection
arguments continue to use the independently injected `Workspace`. Use the HTTP
bootstrap above for a transport host, or assemble `Kernel::builder` directly for
an embedded native host.

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
