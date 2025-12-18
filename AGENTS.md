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
  `/class`, `/library` (alias
  `/lib`), `/service`, `/host`, and `/healthz`. Keep new features within these
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
* Ownership flows exactly as in TinyChain `host`:
  - Missing `txn_id` ⇒ kernel begins a transaction and returns a handle.
  - Subsequent calls include `?txn_id=...` and the kernel reuses the pending
    handle. Interfaces must attach the handle to the request extensions so
    handlers can access it.
  - Empty `POST` ⇒ commit, empty `DELETE` ⇒ rollback. Non-empty bodies are routed
    as regular requests even if they reuse `POST`/`DELETE`.
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

## When extending tc-server

1. Add new handlers to the kernel builder and surface them through adapters via
   request routing, never by instantiating another kernel.
2. Test changes through both the Rust unit tests and the Python integration test
   (`client/py/tests/test_backend.py`) so the shared transaction state machine
   stays in sync across interfaces.
3. When touching `/lib` installs, storage, or WASM routing, also run
   `cargo build -p tc-wasm --target wasm32-unknown-unknown --release &&
   .venv/bin/pytest client/py/tests/test_install_wasm_script.py` to ensure the
   PyO3 installer still handles the bundled example schema and the freshly built
   WASM artifact, persisting it under `<data-dir>/lib/...`.
3. Update this document or `ROADMAP.md` if you adjust kernel construction or the
   transaction contract—future agents rely on this file to avoid regressions.
4. **Never** hide core functionality behind adapter-only feature flags. If a
   new capability is only reachable when `http` is enabled (or only when
   `pyo3` is enabled), that’s a regression. Always add transport-agnostic
   entry points first, then plug the adapters into that shared code.
5. When exposing new `State` variants (e.g., tensors/collections) or handler
   verbs to Python, extend the PyO3 bindings in `tc-server/src/pyo3_runtime.rs`
   so `PyState`/`PyKernelHandle` can deserialize them via destream. Pair these
   changes with Python doc stubs under `client/py` per `ROADMAP.md` so Sphinx
   stays in sync.

## Python virtualenv (`.venv`)

* The repo ships with a `.venv` managed by the install script. Always activate
  it (`source .venv/bin/activate`) before running Python tests or reinstalling
  the PyO3 extension.
* `scripts/install_tc_server_python.sh` invokes `maturin develop` inside `.venv`
  so that `import tinychain as tc` resolves to the freshly built module.
* Keep `.venv` out of git status; wipe/recreate it only if the interpreter
  becomes corrupted (remember to reinstall maturin inside the new env).
* Run `cargo clippy --all-targets --all-features` (or the crate-specific subset)
  before submitting Rust changes—clippy catches cross-target issues that `cargo
  check` can miss, especially around PyO3 bindings and unused structs.
* Use `destream` as the default codec for HTTP bodies, WASM manifests, PyO3
  transfers, etc. `serde` is only acceptable for payloads that the protocol
  bounds tightly (e.g., query strings) or for test fixtures, and any such usage
  must be documented inline so we can migrate it later.
