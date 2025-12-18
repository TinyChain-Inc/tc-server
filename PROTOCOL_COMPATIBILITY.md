# TinyChain protocol compatibility guide

This note explains how to stand up a TinyChain-compatible server (or custom adapter) that honors the protocol described in the public v1 docs (https://docs.tinychain.net) while reusing the shared v2 crates in this workspace.

## Compatibility targets

- **HTTP semantics:** Match the v1 API shapes documented at docs.tinychain.net (transaction headers/params, `/state/...` paths, `/lib` installer shape, empty-body commit/rollback). The v1 GitBook remains the authoritative description of these verbs and resource layouts.
- **IR contract:** Route inbound requests into `tc-ir` handlers so the same capability manifests and IR types are exercised regardless of transport.
- **Transaction + auth surface:** Preserve the `txn_id` lifecycle and capability masks; adapters only translate protocol details and defer begin/commit/rollback to the shared kernel (`Kernel::route_request`).

## Canonical URI surface

TinyChain runtimes expose a fixed directory layout; your server must honor the same
namespaces and mirror them on disk (txfs) so clients and validators can reason about
resources without adapter-specific branches:

- `/state` → built-in data structures: `/state/chain` for chain-wrapped collections,
  `/state/collection` for shard-local stores (`Table`, `Tensor`, etc.),
  `/state/scalar` for scalar types (with `/state/scalar/value`, `/state/scalar/tuple`,
  `/state/scalar/map`), plus the forthcoming `/state/media` abstraction for large
  binaries.
- `/class` → class definitions shipped with a library or service.
- `/library` (alias `/lib`) → stateless standard libraries, published schemas, and WASM
  payloads.
- `/service` → publisher-owned, stateful services (queues, trainers, executors) that wrap
  collections in chains for synchronization.
- `/host` → runtime health/load stats; `/healthz` → liveness probe for load balancers.

Do **not** introduce alternative top-level directories or bespoke response envelopes.
Everything the kernel serves must live under these paths, and `<data-dir>/<segments>`
must match the URI segments exactly so HTTP, PyO3, and future adapters stay aligned.

## TLS expectations

- The reference HTTP adapter runs on Hyper. Enabling TLS simply wraps the
  listener with `rustls` (or another acceptor) and supplies the desired cert/key
  pair; the IR and kernel layers are unaffected. **Production deployments must
  terminate HTTPS**, though testnets/local dev can allow HTTP for convenience.
- TinyChain authorization is handled by recursive JWTs and capability masks, so
  TLS certificates do **not** have to share a global CA. Operators may use
  public CAs, pinned self-signed certs, or optional mTLS for selected ports.
- When TinyChain-hosted runtimes or control-plane services talk to each other in
  production, require mTLS and validate the client cert in your transport layer
  (Hyper, axum, etc.) before invoking `Kernel::route_request`. For public or
  end-user traffic, server-auth TLS plus capability tokens is sufficient.
- No matter which TLS configuration you choose, once the HTTP request reaches
  `Kernel::route_request` the protocol flow (txn IDs, capability enforcement,
  `/state` semantics) is identical.

## Fastest paths to a bespoke server

1. **Reuse the kernel, swap the transport.** Instantiate the reference kernel via `tinychain::build_http_kernel_with_config` (or the config-free builder) and mount it behind your own HTTP/gRPC/serverless adapter. Your adapter should:
   - Parse the same transaction cues as the v1 HTTP protocol (`?txn_id=...`, empty POST/DELETE for finalize) and attach them to the request extensions.
   - Translate HTTP-style verbs/paths into the `Route` + `State` types expected by `Kernel::route_request`.
   - Map `TCError` categories back to protocol status codes consistent with docs.tinychain.net.
2. **Use `tc-server` as a minimal kernel crate.** When you want the TinyChain router primitives without the built-in HTTP/PyO3/WASM adapters, depend on `tc-server` with `default-features = false`. This exposes `Dir`, `Route`, `Handler`, `Transaction`, `Kernel`, and the `TxnManager`/`TxnHandle` pair while skipping optional transports. Re-enable adapters explicitly via `features = ["http"]`, `features = ["pyo3"]`, or `features = ["wasm"]` as needed for your embedding.

   **Minimal feature set (for bespoke, non-published hosts):**
   - `default-features = false` to drop adapter and Wasmtime dependencies.
   - Keep `Kernel`, `TxnManager`, `Dir`, `Route`, `Handler`, and `Transaction` in scope so you can register your own Rust handlers (including hardware-accelerated code paths) behind TinyChain’s routing and auth surface.
   - Enable only the adapters you need (e.g., `features = ["http"]` for a private HTTP ingress) while keeping the rest of the kernel lean.
   - Rely on the shared transaction/auth contract so Python/WASM clients (or custom clients) can call your service without special casing.
3. **Embed `tc-ir` directly.** For the smallest possible footprint, link the `tc-ir` crate and register handlers with your own router. Follow `tc-ir/IR_INTERFACE_GUIDELINES.md` for interaction patterns and keep the transaction/auth surface identical to v1. This still lets publishers use the Python or WASM clients unchanged because the IR layer mirrors the documented protocol.
4. **Leverage manifests and examples.** The bundled WASM library schema (`tc-server/examples/library_schema_example.json`) plus the `tc-wasm` example module show how routes map to exports. Use them as a reference when validating your server’s `/lib` installer or manifest ingestion.

## Documentation breadcrumbs

- **Protocol reference:** docs.tinychain.net → HTTP resources, transaction semantics, collection routes, and client examples. Treat this as the contract your server must satisfy.
- **IR guidance:** `tc-ir/IR_INTERFACE_GUIDELINES.md` → handler expectations, transaction context, error/backpressure signaling.
- **Server lifecycle:** `tc-server/ROADMAP.md` → current transaction flow and roadmap for `/lib` installs and WASM routing.

If your adapter or bespoke host diverges from these contracts, note the deviation and link the rationale from `ARCHITECTURE.md` or the relevant README so downstream clients can understand compatibility limits.
