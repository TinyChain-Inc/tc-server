# tc-server Roadmap

This crate now has the skeleton required to serve `/lib`, `/service`, and `/host`
endpoints, but it still lacks the transactional guarantees that the production TinyChain host
offers. This roadmap tracks the work necessary to align the reference server with the
behavior of the upstream `host` crate.

## Current transaction flow (as implemented today)

- **Begin:** Any HTTP or PyO3 request that omits `txn_id` triggers `TxnManager::begin`.
  The handler executes immediately within that transaction and the server records the new ID.
- **Continue:** Subsequent requests include `?txn_id=<id>` in the URI. The dispatcher
  loads the pending transaction and stores the `TxnHandle` (plus parsed body state) in
  the request extensions before invoking the kernel handler.
- **Finalize:** Sending an *empty* `POST` to the **canonical component root** with
  `?txn_id=<id>` commits the transaction. Sending an empty `DELETE` to the same root rolls it
  back. “Component root” means the canonical library/service path derived from its manifest
  at install time (or `/lib` for `/lib` installs). Finalize requests are **root-only**: an
  empty `POST`/`DELETE` to a subpath must be treated as an ordinary request, not a
  commit/rollback signal.
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
  semantics and transaction handling (begin/continue/finalize via `?txn_id=...`).
- Add a `wasm` feature gate (default-on) so `tc-server` can build with `default-features = false`
  for adapter-less kernels while preserving the current default behavior for HTTP/PyO3 stacks.
- Document the feature matrix in `PROTOCOL_COMPATIBILITY.md` so publishers know when to enable
  `http-server`, `http-client`, `pyo3`, or `wasm` while keeping bespoke server builds lean.
- Spell out the minimal, adapter-free feature set for bespoke hosts: `default-features = false`
  plus the router/transaction exports, so publishers can layer their own Rust handlers (including
  hardware-accelerated code paths) behind TinyChain’s transaction/auth surface without shipping
  any transport adapters they do not need.

## 1. Transaction foundation

**Objective:** Every handler runs inside a `Txn` with explicit commit/rollback hooks.

Design note: This section covers the v1 “cross-service transaction” core (txn workspaces,
per-resource staging, leader/participant finalize), but re-expressed using the v2 kernel shape.
The L0/L1 governance `ARCHITECTURE.md` references `txn_lock` at the consensus level; the runtime
work here is the concrete host-facing transaction implementation which makes cross-host TinyChain
ops atomic and deterministic.

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
     - `Continue`: pass the txn ID via `?txn_id=...`; server looks it up.
     - `Finalize`: explicit `commit` or `rollback`.
  3. On handler success, defer commit until the client sends `Finalize`; on error, auto-roll
     back and surface the failure.

### HTTP verb coverage (`GET`/`PUT`/`POST`/`DELETE`)

**Objective:** The kernel must be able to route all four standard verbs into library/service
handlers, matching the `Scalar::Op` verb surface and the v1 `Gateway` shape.

Plan:

1. **Promote verb-uniform dispatch.** Ensure the `Kernel::dispatch` routing table does not
   artificially restrict verbs for top-level namespaces:
   - `/lib`: keep `GET` and `PUT` for install/schema, and allow routed library subpaths to
     receive `GET`/`PUT`/`POST`/`DELETE` uniformly.
   - `/service`: route `/service/...` subpaths to the service handler for all four verbs.
2. **Mirror the v1 `Gateway` boundary.** Model the v2 “request executor” interface as
   `get/put/post/delete` so that local dispatch and remote HTTP proxying share one verb surface.
3. **Add minimal routing tests.** Add HTTP tests which verify that a library route can receive
   a non-`GET` request (e.g., `POST /lib/<...>/<route>`), and that a service route under
   `/service/...` is reachable with all four verbs (even if the handler returns
   `405 Method Not Allowed` for some).

### Root-only finalize (hardening)

**Objective:** Avoid accidental commit/rollback triggered by empty `POST`/`DELETE` requests to
non-root paths.

Plan:

1. **Move finalize detection into `Kernel::route_request`.** `TxnManager` should only validate
   `txn_id` existence and return a `TxnHandle`; the kernel decides whether a request is a normal
   op or a finalize op based on `(method, body_is_none, path)`.
2. **Define “component root” parsing.** Add a single helper which maps an inbound path to its
   canonical component root and remainder (if any):
   - `/lib` is a component root for `/lib` installs.
   - `/lib/<publisher>/<name>/<version>` is a component root for a library install.
   - `/service/<publisher>/<namespace>/<name>/<version>` is a component root for a service.
   This helper must be transport-agnostic and reused by HTTP and PyO3 routing.
3. **Enforce root-only finalize.** Only treat `(POST|DELETE, empty body, txn_id present)` as
   finalize when `path == component_root`. For all other paths, dispatch normally.
4. **Add regression tests.**
   - Start a txn by staging an install (e.g., `PUT /lib`).
   - Issue an empty `POST` to a non-root path (e.g., `/lib/hello?txn_id=...`) and assert the
     txn is still pending.
   - Issue an empty `POST` to the correct root (`/lib?txn_id=...`) and assert commit succeeds.

Implementation import notes (v1 reference points, repo-relative):

- **Txn lifecycle and workspaces.** Import the “txn server + per-txn workspace dir + expiry GC”
  pattern:
  - v1: `host/server/src/txn/server.rs` (`TxnServer::{create_txn, verify_txn, finalize}`)
  - v1: `host/server/src/txn/mod.rs` (`Txn` workspace + `subcontext_unique`)
- **Txn verification and signer resolution.** Import the “verify signed token by fetching actor
  keys via RPC” pattern:
  - v1: `host/server/src/txn/server.rs` (token verification uses an `rjwt::Resolve` impl backed by
    the txn RPC client)
- **Txn execution surface (`Gateway`).** Import the idea that the transaction context is also a
  remote procedure call gateway, so reference resolution can execute ops without special-casing
  transports:
  - v1: `host/transact/src/lib.rs` (`Transaction` + `Gateway` traits)
  - v1: `host/server/src/txn/mod.rs` (impl `Gateway<State>` for `Txn` as `get/put/post/delete`)
- **Local-vs-remote dispatch with egress hook.** Import the “loopback routes to kernel; remote
  routes to an RPC client; egress is enforced at the gateway boundary” layering:
  - v1: `host/server/src/client.rs` (`ClientInner` loopback routing; `Client` egress policy hook)
- **HTTP RPC implementation for `Gateway`.** Import the “txn_id + bearer token propagation” rule:
  - v1: `host/src/http/client.rs` (adds `txn_id` to the URI; forwards JWT; decodes responses)
- **Finalize protocol (commit/rollback).** Align finalize semantics with the shared kernel rule:
  - commit is an empty `POST` to the canonical component root derived from the manifest;
  - rollback is an empty `DELETE` to the same root;
  - both must be forwarded across hosts with the same `txn_id` and chained token.
  - root-only enforcement: an empty `POST`/`DELETE` to a subpath must never be interpreted as
    finalize.
- **Finalize propagation: commit/rollback messages.** Import the participant commit/rollback
  message semantics and handling:
  - v1: `host/server/src/cluster/public/mod.rs` (empty `PUT` = commit message, empty `DELETE` =
    rollback message for replicas/participants)
  - v1: `host/server/src/cluster/mod.rs` (`Cluster::{replicate_commit, replicate_rollback}`)
- **Participant tracking for finalize propagation.** Import the “record touched downstream links
  during the txn, then notify on finalize” concept (but enforce via manifest-declared deps in v2),
  while relying on cluster registration for replica discovery:
  - v1: `host/server/src/cluster/mod.rs` (`ClusterEgress` records downstream links; `replicate_txn_state`)
  - v1: `host/server/src/client.rs` (egress hook placement is in the gateway)
  - v2: replace v1’s “record and allow” placeholder with “check manifest allowlist and record”
    so finalize propagation only targets allowed dependencies, and use cluster startup registration
    to resolve the live replica set for each participant.
- **Token chaining on route.** Import the idea that the kernel extends a transaction’s capability
  token as it routes into additional components:
  - v1: `host/server/src/txn/mod.rs` (`Txn::grant` constructs/extends the token with a new claim)
  - v1: `host/server/src/cluster/mod.rs` (`Cluster::claim` attaches a signed claim when a cluster participates)
  - v2: when routing into a dependency, sign the minimal claim for that component and attach the
    resulting token to all subsequent outbound calls within that txn.

Additional v1 details to import (correctness and UX):

- **Token validation invariants.** Enforce strict validation for chained tokens (reserved path
  claims, inconsistent owner/lock claims, malformed leaders) so cross-host txns fail fast and
  safely:
  - v1: `host/server/src/txn/mod.rs` (`Txn::validate_token`)
- **Loopback detection policy.** Mirror v1’s “loopback routes to kernel, otherwise RPC” rule,
  including default-port edge cases and protocol-specific defaults:
  - v1: `host/server/src/client.rs` (`ClientInner::is_loopback`)
- **Wire encoding and bounded errors.** Define a canonical request/response encoding and keep
  error envelopes bounded so upstream failures cannot amplify payload size:
  - v1: `host/src/http/server.rs` (encoding negotiation, timeout, error transforms)
  - v1: `host/src/http/client.rs` (bounded upstream error body parsing)
- **Participant recording granularity.** Normalize recorded participants to the canonical
  component root (library/service root), not per-route URIs, so finalize propagation remains
  stable even as internals change:
  - v1: `host/server/src/cluster/mod.rs` (`ClusterEgress` normalization + `replicate_txn_state`)
- **Participant discovery via registration.** Do not rely solely on “touched link” inference:
  - require each class/library/service to register with its cluster at host startup,
  - resolve replica sets and canonical authorities via that registry (lead host or production DNS),
  - use the registry as the source of truth for routing finalize propagation.
- **Finalize propagation failure policy.** Define the expected behavior when some replicas or
  downstream participants cannot be reached during commit/rollback (retry vs fail commit vs mark
  degraded) and test it explicitly:
  - v1: `host/server/src/cluster/mod.rs` (`replicate_txn_state` failure handling)
- **Subcontext discipline.** Preserve the “subcontext per decode/encode” discipline so request
  body decoding, caching, and cleanup remain deterministic:
  - v1: `host/server/src/txn/mod.rs` (`subcontext_unique`)
  - v1: `host/src/http/{server,client}.rs` (uses subcontexts for request/response bodies)

Testing plan (cross-service transactions and failure cases):

- **Multi-service reads.** One transaction issues reads against multiple services (local and
  remote) and returns a combined result while preserving a single `txn_id` and chained token.
- **Multi-service writes + commit.** One transaction writes to multiple services (local and
  remote), then commits via empty `POST` to the canonical component root and verifies that:
  - all writes become visible after commit, and
  - no writes are visible before commit on any participant.
- **Rollback semantics.** Force an error in a downstream participant mid-txn and verify an empty
  `DELETE` rollback leaves no partial writes across all participants.
- **Partial outage on finalize.** Simulate one remote participant being unavailable during
  commit/rollback and assert the chosen failure policy (fail-fast vs retry vs degrade) is
  followed deterministically.
- **Replicated service participation.** Run a service with multiple live replicas (distinct hosts
  serving the same canonical component root), then execute a cross-service transaction which
  touches that service and verifies:
  - the leader/lock rules gate commit authority correctly,
  - replicas converge after commit (or roll back after rollback) according to the cluster’s
    majority correctness rule, and
  - failure of a minority of replicas does not corrupt the committed state; lagging replicas
    converge on restart via chain replication.

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
  1. Reads `?txn_id=...` (or begins a transaction when omitted).
  2. Streams the payload via destream, persisting artifacts to a txn-scoped store
     (e.g., `txn_tmp/{txn_id}/artifact-id`).
  3. Registers staged handlers in the `Dir`.
  4. Returns `202 Accepted` plus the minted `txn_id`; caller finalizes later.
- On `commit`, the owner moves artifacts into the permanent store and publishes the schema;
  on `rollback`, it deletes the staged files and leaves the live directory untouched.
- Extend the spec with a concrete manifest/example for WASM installs (showing the `routes[*].wasm_export`
  metadata) plus a documented CLI or script that compiles a TinyChain library to `.wasm`, embeds the
  manifest, and issues the `/lib` install request. This becomes the reference workflow for proprietary
  publishers.
- TODO: Stream large artifacts directly to disk (no full in-memory buffering) when handling `/lib` installs.

## 3. Python + HTTP integration

**Objective:** Client-visible tests prove the transactional contract.

- Add pytest cases that:
  1. Install a library via `PUT /lib` (no `txn_id`), capture the minted `txn_id`, and assert
     `GET /lib` without `txn_id` still returns the old schema while `GET /lib?txn_id=...` shows
     staged changes.
  2. Roll back and verify the new schema never appears.
  3. Reinstall, commit, and verify `/lib` reflects the new schema plus the library’s routes.
  4. Exercise error paths: double install in same txn, install after commit, etc.
- Wire these tests into CI: stand up `tc-server` under Pytest (possibly via `uvicorn`-style
  harness), run the scenarios, and ensure they pass before merging.

### Dependency egress enforcement (manifest-driven)

**Objective:** A library/service may only issue outbound TinyChain requests to manifest-declared
dependencies, uniformly across local and remote execution.

Notes:

- v1 already routes outbound calls through a host-owned egress gate and calls out the missing
  whitelist as a TODO; v2 should make that allowlist explicit (manifest-declared) and enforce it
  inside the kernel rather than relying on observed call graphs.
- Authorization is determined by the canonical dependency path declared at install time; overly
  permissive dependency paths are rejected during install (e.g., root or top-level namespace
  prefixes).

Plan:

1. **Persist dependencies with installs.** Treat `LibrarySchema.dependencies` as the source of
   truth for `/lib` packages, and mirror the same shape for `/service` manifests so the kernel
   can evaluate dependencies uniformly.
2. **IR prerequisites: `TCRef` + `Scalar::Ref`.** Import the v1 reference IR pattern into `tc-ir`
   so libraries can express “call B” as a typed reference instead of bespoke adapter logic:
   - Add `TCRef` (initially: `TCRef::Op(OpRef)` plus the minimum additional variants needed for
     parameterization and composition).
   - Add `Scalar::Ref(Box<TCRef>)` (and wire its `destream` encoding/decoding) so references can
     flow through the same request/response shapes as values.
   - Keep the representation transport-agnostic: both HTTP and PyO3 must serialize/deserialize
     `Scalar::Ref` symmetrically using the shared `destream_json` stack.
   - Defer the full `OpDef` executor/scheduler until after the first cross-host example works;
     the dependency example only needs the `OpRef` resolver path to issue a single outbound call.
2. **Enforce on outbound dispatch.** When executing a request on behalf of caller `A`, require
   the target’s canonical dependency path to be present in `A`’s dependency set. This check must
   run before any transport routing decision (local dispatch vs HTTP proxy).
3. **Exercise local + remote equivalence.** Add an integration test which installs three
   libraries `A`, `B`, and `C` where only `A → B` is declared, then verifies:
   - a call path from `A` into `B` succeeds;
   - a call path from `A` into `C` fails with `Unauthorized`;
   - the same behavior holds when `B` is served by a different HTTP host (i.e., `B` is accessed
     via a fully-qualified URI) without granting `A` any additional egress.
4. **Reference example (developer workflow).** Provide a minimal, end-to-end example which:
   - starts one HTTP host which serves `B`;
   - runs `A` in-process via PyO3 (no HTTP server), with an HTTP *client* egress adapter enabled;
   - installs `B` on the remote host;
   - installs `A` locally with `dependencies = [<canonical dependency path of B>]` and a method
     which calls `B` by fully-qualified URI;
   - invokes `A` from Python and demonstrates that `A → B` succeeds while `A → C` is rejected.

## 4. Milestone criteria

We consider the transactional foundation “done” when:

- `Kernel::dispatch` refuses to invoke handlers without a `Txn`, and HTTP clients can manage
  txn lifecycle through the documented `?txn_id=...` query parameter and finalize verbs.
- `/lib` installs are invisible outside their txn until commit.
- `cargo test --features http-server` runs the new integration tests successfully.
- The Python test suite exercises begin/install/commit/rollback flows end-to-end.
- The runtime enforces manifest-declared dependency edges for outbound calls, with parity across
  local dispatch and remote HTTP proxying.

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
