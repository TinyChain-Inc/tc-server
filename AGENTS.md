# tc-server Agent Notes

- Handwritten `unsafe` is prohibited. PyO3's Rust 2024
  `unsafe_op_in_unsafe_fn` allowance exists only for code emitted by PyO3 0.21
  macros; do not place unsafe blocks, functions, traits, or impls in the PyO3
  adapter or broaden that allowance.

This crate is intentionally thin: all business logic lives in the shared `Kernel`
and its handlers, while the protocol layers (HTTP, PyO3, future bindings) simply
translate transport-specific details into the canonical TinyChain control flow.
Keep the following rules in mind whenever you extend the server:

## Delegated ownership

* Each concern has one owner: the library registry owns library lifecycle, the
  kernel owns native execution and transaction orchestration, collections own
  collection behavior, and adapters own only their transport projection. Pass
  canonical values across these boundaries and delegate; never mirror another
  layer's routing, storage, transaction, or codec logic.
* When ownership moves, delete the old helper, handler, cache, or fallback in
  the same change. Do not retain parallel adapter and kernel implementations.
* Native kernel and library handlers return `State` only. HTTP calls `IntoView`
  once before wire encoding; PyO3 retains native state/view handles; remote RPC
  and WASM serialize only because they cross an actual boundary. Local library,
  service, class, resolver, and nested `OpDef` calls must pass `State` and the
  same `TxnHandle` directly with zero encode/decode operations.
* Server-native signatures use the canonical `crate::State` alias, which is
  `tc_state::State<TxnHandle>`. Do not import an unparameterized `tc_state::State`,
  default a route state, or carry `TxnHandle` as a phantom substitute for State.
* An implicit transaction remains live until its terminal view is consumed.
  Successful terminal projection requests commit through `TxnServer`; a dropped
  or failed projection only releases its guards and is rolled back by the
  `TxnServer`-owned TTL. Nested native calls never begin or finalize a transaction.
* Route modules may not import codec or adapter APIs, and codec modules may not
  resolve routes, invoke `OpDef`s, or inspect the library registry. Delete any
  result envelope or local loopback helper which makes routing understand
  serialization.
* Adapters and the kernel preserve the end-to-end contract in `BACKPRESSURE.md`.
  Admission must precede body materialization, graph fan-out, or task spawning;
  HTTP/PyO3/WASM consumers pull native views lazily, and disconnect/drop cancels
  production and releases the transaction. Do not add unbounded channels,
  detached request work, unlimited stream concurrency, or adapter-local buffering.
* Bootstrap constructs one immutable `HostLimits` and one shared `HostResources`.
  Kernel-issued `TxnHandle`s carry the request deadline and delegated admission
  capability through nested native calls. Terminal views own request permits until
  consumption or drop; adapter response construction is not completion.
* HTTP bodies are counted while streaming. Native JSON is decoded directly from
  that stream; only transport-owned artifact/callback routes may retain bounded
  bytes. Authenticated traffic never bypasses body limits.
* `TxnServer` runs the one host-level TTL worker. Abandoned terminal results do
  not enqueue rollback; their active record expires through the same finalize
  path as an explicit rollback. Never spawn, enqueue, or call transaction cleanup
  from an HTTP stream or PyO3 handle `Drop` implementation.

## Kernel construction

* `Kernel::builder` must only be invoked in one place per binary. The Rust crate
  owns the builder; adapters receive a clone of the finished kernel.
* `HostStorage` is the sole production `freqfs::Cache` constructor. Every host
  bootstrap creates one `HostStorage`, which owns exactly two distinct caches:
  one loads `workspace`, and one loads `data_dir`. See the canonical
  contract in [`../docs/storage.md`](../docs/storage.md).
* `data_dir` stores libraries and executable artifacts only. Its cache is typed
  for `LibraryFile`, and `LibraryStore` adds transactional install/hydration over
  `<data_dir>/lib/...`. Never place collection state or transaction work there.
* `workspace` stores persistent BTree/Table state plus every transaction-local
  collection delta and literal. Its cache is typed for `PersistentFile`; the
  host loads the workspace root once and delegates directories through
  `Workspace` and `TxnHandle`. Never place library artifacts there.
* HTTP bootstrap constructs `HostStorage` and injects `LibraryStore` plus
  `Workspace`. A PyO3 `KernelHandle` is its own local host instance and performs
  the same one-pair bootstrap. Adapters and lower crates must not construct an
  additional cache or reload either root.
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
  `Kernel::bind_transaction + Kernel::execute`, which already knows every installed `/lib` and
  `/service` route. The Python client ships stubs purely for documentation and
  IDE support; the real work always flows through the shared kernel so HTTP,
  PyO3, WebSocket, and future adapters observe identical behavior.
* **Wire-format ownership stays at adapters.** `tc-server` transport layers may
  encode/decode JSON (and future TBON/binary formats), but `tc-state`,
  `tc-collection`, `tc-ir`, and `tc-value` runtime semantics must remain
  format-neutral. Keep format-specific helper modules inside adapter boundaries.
* **Single Op compiler.** All `OpDef` compilation (DAG planning/topological
  scheduling) must go through the shared host-side entrypoint (`op_plan`); do
  not reimplement this logic in adapters, installers, or runtime registries.
* Feature flags (`http`, `pyo3`, `ws`, `media`, etc.) only toggle which adapters are compiled. They
  must **never** create additional kernel instances or mutate global state.
* The WebSocket adapter (guarded by `ws`) must reuse the same kernel routing as HTTP: capability masks,
  transaction cues, and queue integration all flow through `Kernel::bind_transaction + Kernel::execute`. Keep the adapter
  minimal—handle handshake/upgrade and stream frames into the shared handler logic. Add CI coverage for
  basic echo/stream tests whenever the feature is touched.

## Transaction protocol

* `TxnServer` is the sole transaction protocol authority. Protocol layers may
  parse `txn_id` parameters or detect empty bodies, and the kernel may classify
  route cues, but allocation, owner/leader pinning, participant continuation,
  token chaining, TTL, and commit/rollback decisions belong to `TxnServer`.
* There is exactly one transaction semantics implementation. Do not duplicate any part of the
  claim, token chaining, begin/continue, commit, or rollback logic in adapters; they only parse
  transport cues and delegate to the kernel.
* **Single state machine.** `TxnServer` owns authority, TTL, and the one atomic
  commit/rollback/finalize/cleanup transition. Explicit
  finalize and TTL expiry must call that same transition. Adapters and resource
  owners may not add cancellation queues, cleanup workers, or alternate paths.
* Explicit completion authorization and the `Active -> Finalizing` transition
  are one `TxnServer` operation. Never split them across kernel/adapters or return
  a transaction handle from an authorization-only finalize API. Owner and component
  leader checks, leader pinning, and active-handle replacement are one atomic record
  mutation. Failed finalization must leave participant staging intact and retryable.
* **Single transaction identity trait.** The only trait named `Transaction` lives in
  `tc-ir`. Lower crates may define narrow, semantically named delegated capabilities
  such as collection `StorageContext`, but may not duplicate identity, authority, or
  lifecycle state.
* **No invented transaction context.** `tc-state::StateContext` carries only a kernel-delegated
  collection allocation context; it never carries a transaction. Do not add `null_transaction`, default/fake transaction
  values, synthetic IDs, or adapter-local `Transaction` implementations to production code.
  `TxnHandle` and the direct kernel-issued `TxnHeader` are the only live runtime transaction
  contexts. Test fixtures may implement `Transaction` only inside `#[cfg(test)]` modules and
  must never enter production APIs.
* `TxnHandle` owns `<workspace>/txn/<txn-id>` lifecycle. Named BTree/Table values
  use canonical URI-derived children; literal values use unique transaction children.
  Collection code receives delegated directories and must never construct transaction
  workspace paths or a `<txn-id>/pending` layout.
* Ownership flows exactly as in TinyChain `host`:
  - Missing `txn_id` ⇒ `TxnServer` allocates an identity, makes the local host
    the protocol owner, and returns a host-signed handle. Caller claims remain
    separate application authorization context.
  - Subsequent calls include `?txn_id=...` and the kernel reuses the pending
    handle. Interfaces must attach the handle to the request extensions so
    handlers can access it.
  - Root-only finalize: empty `POST`/`DELETE` finalize **only** when sent to the canonical
    component root (derived from the manifest). Empty bodies to subpaths must be treated as
    ordinary requests, not commit/rollback.
* A second active transaction registry or manager is prohibited. Only `TxnServer`
  may retain protocol records, interpret transaction claims, invoke transactional
  finalize delegates, or delete a transaction workspace. The kernel exposes only
  thin bind and semantic-completion delegates; adapters never pass `commit: bool`.
* Finalize hooks are transaction-critical. Commit and rollback hook failures must
  be returned to the caller so the transaction can be retried or inspected; do
  not log-and-ignore participant finalize failures.
* Preserve the transaction RPC lower bound documented in
  `docs/protocol/transactions.md`: `W` required remote work RPCs plus one parallel
  finalize RPC for each of `P` recorded participants. Piggyback identity,
  authority, and enlistment on work. Do not add preliminary token, claim,
  prepare, probe, or acknowledgement RPCs when the existing signed transaction
  authority and work/finalize responses carry the same information.
* One request carries one absolute `Deadline` from ingress through authentication,
  native calls, projection, and finalization. Nested work and resource owners receive
  that value; they must not create a fresh timeout budget. Use `Deadline::wait` or
  `Deadline::run` rather than calling Tokio timeout primitives in adapters or RPC
  code. Outbound RPC inherits `TxnHandle::deadline`; only independent transport
  maintenance work may establish its own explicit deadline.
* `tc-ir::Method` is the sole native verb type. RPC gateways accept the same
  `Scalar`, `Map<State>`, and `State` values as local handlers and serialize only
  after crossing the remote-host boundary. Do not add server-local method enums,
  RPC request wrappers, or premature `Value` conversion.
* Use a free structural resolver function for the closed `TCRef` hierarchy. Add a
  resolver trait only when runtime-pluggable implementations actually exist; a
  trait mirroring fixed enum variants is redundant indirection.
* Bootstrap composes `HostLimits` from owner-specific ingress, execution, storage,
  and device limits. Pass only the relevant section to each owner; do not reintroduce
  a global policy manager or hand the full configuration to lower layers.
* `Kernel`, `HttpRouter`, `TxnServer`, `LibraryRegistry`, `HostStorage`, RPC
  clients, and PyO3 runtimes are instance-owned services, not globals. Bootstrap
  constructs each shared resource once and passes clones of its handle. Do not
  add static registries/counters, hidden singleton initialization, or lower-layer
  fallback constructors.
* `HttpClusterGateway` owns the replication HTTP connection pool. Construct one
  gateway at bootstrap and pass it through discovery, replication, installation,
  and finalization. Replication operations must not construct clients per request
  or add default wrappers alongside gateway-taking APIs.
* Storage maintenance versions have an owner-local namespace and are never
  represented as `TxnId`. Only kernel-issued protocol work enters `TxnServer` or
  the protocol transaction namespace.
* Transaction prepare/finalize bookkeeping uses one deterministic participant-set
  representation. Convert to vectors only at an external boundary; retries retain
  unresolved members in the same set rather than rebuilding ad hoc hash sets.
* A transaction must never create or depend on another transaction ID, even for a
  read-only lookup. Do not add sub-transactions, `TxnId -> version` maps, or
  transaction side tables. If committed data must be read outside an active
  transaction, use the storage layer's canonical committed snapshot API; if that
  API is missing, extend the storage primitive instead of minting a synthetic
  transaction.
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
* **Single egress path.** Outbound network access must flow through the kernel RPC gateway so
  dependency and token checks are enforced once; adapters must not introduce parallel egress code.

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

## Persistence delegation

* Delegate persistence to the owning component (e.g., each `Library` should read
  versions from its own `freqfs::Dir`/txfs directory, as in v1). Avoid central
  storage helpers or kernel/main orchestration that re-implements per-library
  persistence in aggregate; keep the registry/router thin.
* Staged library install state owns live replication participant metadata. Do not
  reintroduce a separate replication transaction tracker; finalization must read
  the prepared participant set from the staged `LibraryRegistry` transaction
  record and retry only unresolved participants.
* Keep discovery health separate from transaction obligations. Discovery code may
  prune stale peers from `PeerMembership`, but transaction prepare/finalize must
  snapshot its participant set once and fail closed against that immutable set.
* Do not implement quorum or majority shortcuts in HTTP replication fanout. Until
  a real consensus log exists, commit/rollback must fail closed unless every
  prepared participant eventually finalizes.
* Bootstrap/discovery replication may degrade, but it must return a structured
  report of installed, skipped, unavailable, and failed peers rather than hiding
  partial success behind logs.
* Crash/restart recovery for in-flight installs belongs in txfs-backed staged
  transaction recovery plus staged registry metadata recovery. Do not fake
  durability with adapter-local JSON files, process-global side tables, or
  transaction-version mappings.
* When porting v1 `SyncChain`/`BlockChain`, treat chains as durable append/replay
  logs for the existing kernel transaction protocol. Chain records must preserve
  the original `TxnId`, canonical component root, and prepared participant set;
  they must not introduce sub-transactions, alternate finalize paths, or client-
  visible recovery controls.

## v1 lessons worth preserving

* Delegate recursively: directories own traversal, leaves own persistence and route logic; the
  registry should only index and route.
* Keep claims leaf-scoped (`/lib/.../version`), and authorize at the concrete component root.
* Token chaining and transaction lifecycle live only in `TxnServer`; adapters and clients never
  mint, interpret, or extend transaction authority.
* Default-deny egress must remain kernel-enforced and manifest-driven.
* Replication should stay leaf-scoped and idempotent: discover via listings, request tokens per
  library path, and export/install per library (no global exports).
