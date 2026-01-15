# tc-server Agent Notes

This crate is intentionally thin: all business logic lives in the shared `Kernel`
and its handlers, while the protocol layers (HTTP, PyO3, future bindings) simply
translate transport-specific details into the canonical TinyChain control flow.
Keep the following rules in mind whenever you extend the server:

## Kernel construction

* `Kernel::builder` must only be invoked in one place per binary. The Rust crate
  owns the builder; adapters receive a clone of the finished kernel.
* The PyO3 runtime uses `build_python_kernel` (re-exported from `lib.rs`) so that
  the same handler wiring is shared between HTTP and Python builds.
* When you need disk-backed libraries, call `build_http_kernel_with_config` or
  `build_python_kernel_with_config` and pass a `data_dir`. This hydrates the
  shared `LibraryDir` (layout: `<data-dir>/lib/<id>/<version>/...`) and
  registers all persisted WASM routes before any adapter handles requests.
* Every `tc-server` binary owns **exactly one** `data_dir`. `txfs` assumes the
  on-disk layout mirrors URI segments (`<data-dir>/lib/foo` ↔ `/lib/foo`);
  adapters must never mount additional roots or rewrite resource paths because
  it breaks transactional replay and cross-adapter hydration.
* The host exposes a fixed set of top-level routes: `/state` (with `/state/chain`
  for chain-wrapped collections, `/state/collection` for shard-local data,
  `/state/scalar` plus `/state/scalar/tuple`, `/state/scalar/map`,
  `/state/scalar/value` for tuples/maps/primitives, and soon `/state/media`),
  `/class`, `/lib`, `/service`, `/host`, and `/healthz`. Keep new features within these
  namespaces and reuse the standard response contracts so adapters stay aligned.
* The PyO3 adapter exposes the same kernel as HTTP. Once a WASM library is
  installed under `<data-dir>/lib/...`, PyO3 automatically exposes the same
  routes (no extra registration); keep the loaders in sync so Python clients and
  HTTP clients observe identical manifests and handlers.
* **Dispatch stays in Rust.** Adapters never re-implement handler routing in
  Python/JS/etc.—`PyKernelHandle::dispatch` simply hands off to the canonical
  `Kernel::route_request`, which already knows every installed `/lib` and
  `/service` route. The Python client ships stubs purely for documentation and
  IDE support; the real work always flows through the shared kernel so HTTP,
  PyO3, WebSocket, and future adapters observe identical behavior.
* Feature flags (`http`, `pyo3`, `ws`, `media`, etc.) only toggle which adapters are compiled. They
  must **never** create additional kernel instances or mutate global state.
* The WebSocket adapter (guarded by `ws`) must reuse the same kernel routing as HTTP: capability masks,
  transaction cues, and queue integration all flow through `Kernel::route_request`. Keep the adapter
  minimal—handle handshake/upgrade and stream frames into the shared handler logic. Add CI coverage for
  basic echo/stream tests whenever the feature is touched.

## Transaction protocol

* Transactions are owned by the kernel. Protocol layers may parse `txn_id`
  parameters or detect empty bodies, but all begin/continue/commit/rollback
  decisions end up in `Kernel::route_request`.
* There is exactly one transaction semantics implementation. Do not duplicate any part of the
  claim, token chaining, begin/continue, commit, or rollback logic in adapters; they only parse
  transport cues and delegate to the kernel.
* Ownership flows exactly as in TinyChain `host`:
  - Missing `txn_id` ⇒ kernel begins a transaction and returns a handle.
  - Subsequent calls include `?txn_id=...` and the kernel reuses the pending
    handle. Interfaces must attach the handle to the request extensions so
    handlers can access it.
  - Root-only finalize: empty `POST`/`DELETE` finalize **only** when sent to the canonical
    component root (derived from the manifest). Empty bodies to subpaths must be treated as
    ordinary requests, not commit/rollback.
* Only the kernel finalizes a transaction. Adapters must **not** call
  `TxnManager` directly except to parse/attach handles via `route_request`.
* The transaction owner enforces a **3-second** temporal locality window. Handlers
  that cannot respond inside that budget must short-circuit and push work onto a
  `While`-driven queue service: a single `While` loop whose state bridges many
  transactions, polling for pending work, running one unit, committing, and
  repeating. Persist the queue’s data in standard collections (e.g.,
  `/state/<publisher>/queues/<name>` tables referencing `/state/media/...` for
  large blobs). The kernel handles leasing/failover automatically; do **not**
  invent ad-hoc `claim`/`ack` verbs.

## Network egress (security boundary)

* TinyChain application code must never gain arbitrary network access. Any outbound HTTP client
  usage inside `tc-server` is an internal adapter used by the kernel gateway.
* Egress is default-deny and manifest-driven:
  - a library/service may only call its explicit dependency set (library-wide, non-transitive),
    authorized by canonical path;
  - absolute URIs are only permitted when their authority is explicitly whitelisted for that
    canonical path by host configuration/registry rules (avoid blacklist filtering).
* Enforce this uniformly across HTTP and PyO3: both adapters must route outbound calls through the
  same kernel gateway so dependency checks and token chaining cannot be bypassed.

## When extending tc-server

1. Add new handlers to the kernel builder and surface them through adapters via
   request routing, never by instantiating another kernel.
2. Update this document or `ROADMAP.md` if you adjust kernel construction or the
   transaction contract—future agents rely on this file to avoid regressions.
3. **Never** hide core functionality behind adapter-only feature flags. If a
   new capability is only reachable when `http` is enabled (or only when
   `pyo3` is enabled), that’s a regression. Always add transport-agnostic
   entry points first, then plug the adapters into that shared code.
4. When exposing new `State` variants (e.g., tensors/collections) or handler
   verbs to Python, extend the PyO3 bindings in `tc-server/src/pyo3_runtime.rs`
   so `PyState`/`PyKernelHandle` can deserialize them via destream.
