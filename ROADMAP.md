# tc-server Roadmap

This crate now has the skeleton required to serve `/lib`, `/service`, and `/kernel`
endpoints, but it still lacks the transactional guarantees that the production TinyChain host
offers. This roadmap tracks the work necessary to align the reference server with the
behavior of the upstream `host` crate.

## Current transaction flow (as implemented today)

- **Begin:** Any HTTP or PyO3 request that omits `txn_id` triggers `TxnManager::begin`.
  The handler executes immediately within that transaction and the server records the new ID.
- **Continue:** Subsequent requests include `?txn_id=<id>` in the URI. The dispatcher
  loads the pending transaction and stores the `TxnHandle` (plus parsed body state) in
  the request extensions before invoking the kernel handler.
- **Finalize:** Sending an *empty* `POST` to the same path with `?txn_id=<id>` commits
  the transaction. Sending an empty `DELETE` rolls it back. Both return `204 No Content`
  on success or `400` if the ID is unknown.
- **Body parsing:** The HTTP layer buffers each request body, converts it into a placeholder
  `State`, and only treats the payload as “empty” when it consists solely of ASCII whitespace.
  This ensures real payloads won’t be mistaken for finalize calls once richer `State`/`Value`
  types land.

The `Kernel` owns this state machine so every protocol stack (HTTP, PyO3, future gRPC, etc.)
shares a single implementation: adapters identify the `txn_id` and whether the body is empty,
then delegate to `Kernel::route_request`, which decides whether to begin, continue, commit, or
roll back. Keeping this logic centralized is critical—moving begin/commit heuristics into a
protocol adapter would bypass shared tests and risks future regressions.

### Minimal publisher kit (in progress)

Goal: expose the router and transaction primitives directly from `tc-server` without forcing
publishers to pull in any adapters or the Wasmtime dependency. Actions:

- Re-export `Dir`, `Route`, `Handler`, and `Transaction` (from `tc-ir`) plus `Kernel` and
  `TxnManager`/`TxnHandle` from `tc-server::lib` so bespoke hosts can reuse the same routing
  semantics and auth-bearing transaction headers.
- Add a `wasm` feature gate (default-on) so `tc-server` can build with `default-features = false`
  for adapter-less kernels while preserving the current default behavior for HTTP/PyO3 stacks.
- Document the feature matrix in `PROTOCOL_COMPATIBILITY.md` so publishers know when to enable
  `http`, `pyo3`, or `wasm` while keeping bespoke server builds lean.
- Spell out the minimal, adapter-free feature set for bespoke hosts: `default-features = false`
  plus the router/transaction exports, so publishers can layer their own Rust handlers (including
  hardware-accelerated code paths) behind TinyChain’s transaction/auth surface without shipping
  any transport adapters they do not need.

## 1. Transaction foundation

**Objective:** Every handler runs inside a `Txn` with explicit commit/rollback hooks.

- Define a minimal `Txn` trait (exposed from `tc-ir`) with:
  - `id()` returning a `TxnId`.
  - `claim()` for authorization.
  - `commit()` / `rollback()` async hooks.
- Add a `TxnServer` façade that:
  - Creates new `Txn` handles (wrapping `txn_lock::TxnMapLock` for per-resource staging).
  - Tracks which transactions are pending commit.
  - Owns leadership: the origin node becomes the owner, while resource clusters provide
    `Leader` handles with `commit/rollback` callbacks.
- Update `Kernel::dispatch` to:
  1. Acquire (or receive) a `Txn` before invoking any handler.
  2. Ensure the HTTP layer ties a request to one of three modes:
     - `Begin`: create a txn, return its ID.
     - `Continue`: pass the txn ID in a header; server looks it up.
     - `Finalize`: explicit `commit` or `rollback`.
  3. On handler success, defer commit until the client sends `Finalize`; on error, auto-roll
     back and surface the failure.

## 2. Transactional `/lib` installs

**Objective:** Installing a library stages state under the current transaction and only
becomes visible after commit.

- Replace the ad-hoc `LibraryState` with a `TxnMapLock<TxnId, Vec<PathSegment>, LibrarySchema>`
  (or similar) so that each txn gets an isolated view of the directory tree.
- Reintroduce a staging-friendly `Dir` API, but back it with `TxnMapLock` rather than manual
  pending buffers. Operations:
  - `dir.write(txn_id).insert(path, handler)` returns a guard that stages the handler.
  - `dir.commit(txn_id)` finalizes staged entries (TxnMapLock already handles this).
  - `dir.rollback(txn_id)` discards staged entries.
- Define the `/lib` payload:
  ```json
  {
    "schema": { ... LibrarySchema ... },
    "artifacts": [
      { "path": "/lib/wasm", "content_type": "application/wasm", "bytes": "<base64>" }
    ]
  }
  ```
  The install handler:
  1. Reads the `Txn-Id` header (or creates one for `Begin`).
  2. Streams the payload via destream, persisting artifacts to a txn-scoped store
     (e.g., `txn_tmp/{txn_id}/artifact-id`).
  3. Registers staged handlers in the `Dir`.
  4. Returns `202 Accepted` plus the `Txn-Id`; caller finalizes later.
- On `commit`, the owner moves artifacts into the permanent store and publishes the schema;
  on `rollback`, it deletes the staged files and leaves the live directory untouched.
- Extend the spec with a concrete manifest/example for WASM installs (showing the `routes[*].wasm_export`
  metadata) plus a documented CLI or script that compiles a TinyChain library to `.wasm`, embeds the
  manifest, and issues the `/lib` install request. This becomes the reference workflow for proprietary
  publishers.
- TODO: Stream large artifacts directly to disk (no full in-memory buffering) when handling `/lib` installs.

## 3. Python + HTTP integration

**Objective:** Client-visible tests prove the transactional contract.

- Extend the PyO3 module with a thin HTTP client shim that can:
  - `begin_txn()` → returns `TxnId`.
  - `install_library(txn_id, schema, path_to_wasm)`.
  - `commit(txn_id)` / `rollback(txn_id)`.
- Add pytest cases that:
  1. Begin a txn, install a library, assert `GET /lib` w/out txn header still returns the old
     schema, but `GET /lib` with `Txn-Id` shows staged changes.
  2. Roll back and verify the new schema never appears.
  3. Reinstall, commit, and verify `/lib` reflects the new schema plus the library’s routes.
  4. Exercise error paths: double install in same txn, install after commit, etc.
- Wire these tests into CI: stand up `tc-server` under Pytest (possibly via `uvicorn`-style
  harness), run the scenarios, and ensure they pass before merging.

## 4. Milestone criteria

We consider the transactional foundation “done” when:

- `Kernel::dispatch` refuses to invoke handlers without a `Txn`, and HTTP clients can manage
  txn lifecycle through documented headers/APIs.
- `/lib` installs are invisible outside their txn until commit.
- `cargo test --features http` runs the new integration tests successfully.
- The Python test suite exercises begin/install/commit/rollback flows end-to-end.

## 5. WASM transaction/authorization bridge

**Objective:** Loaded WASM libraries receive the same transaction context and capability claims as native Rust libraries.

- Serialize `TxnId`, `NetworkTime`, and `Claim` using the existing `destream` request/response serialization stack so `tc-server` can hand that bundle to WASM modules without introducing a new binary format yet.
- Teach `TxnManager`/`TxnHandle` to retain the caller's `Claim` (parsed from control-plane tokens) so the WASM adapter can include it when constructing the serialized payload.
- Add a Wasmtime-backed adapter that:
  - Loads a WASM binary whose manifest points to a `tc_library_entry` export describing the schema/routes.
  - Maps each route to a WASM export (e.g., `/hello` → `hello`) and invokes it, passing pointers/lengths for the serialized transaction header + request body. Keep the `/lib` manifest layout aligned with existing TinyChain practice (`LibrarySchema` plus the `Library.__json__` body produced by the Python client): each method entry simply gains a `"wasm_export"` (or equivalent) field that names the exported function, so legacy clients continue to parse the manifest unmodified while the host gets the extra routing hint.
  - Requires each export to return a `destream`-encoded `State` envelope (matching the HTTP handler contract). On error, the payload carries a serialized `TCError`, so the host maps it back to the appropriate HTTP/PyO3 status code (authorization failure, bad request, etc.).
- Update `tc-wasm` to expose helpers that deserialize the transaction payload into a type implementing `tc_ir::Transaction`, and refresh the example module to export `tc_library_entry` plus per-route functions that consume the serialized header/body.
- Add integration tests that load the example WASM library under `tc-server`, exercise an authorized request, and confirm unauthorized requests fail both in the guest (via `Claim::allows`) and at the host boundary.
- Document (and implement) the `/lib` installer’s WASM path so it persists the uploaded artifacts,
  runs the Wasmtime loader on commit, and registers the resulting handlers in the standard routing table.

_(Once the end-to-end path is proven, we can revisit a tighter ABI if performance demands it.)_

### Open questions

- Finalize the exact exported function signature (pointer/length pairs, ABI-safe structs, or Wasmtime component model) and the memory management contract (`alloc`/`dealloc` hooks?) between host and guest. For now we are committing to the serialized interface only: both transaction context and request/response bodies move across the boundary via `destream`-encoded bytes, with no separate binary ABI or extra crate.
- Decide how route names map to WASM exports (direct string match vs manifest-driven table) and how versioning affects those exports.
- Specify the response encoding (pure JSON via `destream_json`, binary `State`, or something richer) and how errors/authorization failures are surfaced across the boundary.
- Determine module lifecycle rules inside `tc-server`: caching, concurrency limits, how artifacts are stored on disk, and how transaction-scoped state is handled when multiple requests hit the same WASM instance.
