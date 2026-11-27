# tc-server invariants

This file defines the repository-local host runtime rules.

## Ownership

The server selects and delivers transaction decisions; Chain owns durable history
and recovery ordering; collections apply local transactional state transitions.
Collection commit, rollback, reservations, visibility, and materialization remain
local data-structure responsibilities. Lower layers return errors without selecting
distributed decisions or terminating the host.

- Bootstrap creates one complete `Kernel`, `TxnServer`, `HostServices`,
  `HostResources`, `HostStorage`, and protocol authority. There
  are no optional production owners, gateways, finalizers, or hidden defaults.
- `HostServices`, `TxnHandle`, `Gateway`, and `OpExecutor` are trusted in-process
  capabilities. Their authority-bearing state is privately constructed and
  exposed only through semantic operations. Crate visibility is not an
  ownership boundary: do not add generic `inner`, `server`, `resources`,
  signer, token, or application-root getters.
- `HostStorage` creates one application-data cache, one host-control cache, and
  one collection-workspace cache. Application code receives delegated roots
  and never constructs caches or filesystem paths.
- Application roots are `Cluster<Dir<Library>>`, `Cluster<Dir<Class>>`, and
  `Cluster<Dir<Service>>`. The kernel selects a root once; recursive directories
  consume structure; the concrete application owns behavior and persistence.
- The shared `Kernel` state owns those three roots directly. Do not introduce an `app` or
  `application` module, aggregate owner, generic payload, or facade: TinyChain
  has no fourth domain object above Library, Class, and Service.
- `Cluster<T>` owns exact-resource claims, leadership, participation, propagation,
  and direct `Transact` delegation. Participation retains active calls and attempted
  dependencies until cutoff; it is not a decision ledger, recovery registry,
  reconciliation policy, or per-resource signing key.
- Ordinary enrollment returns the resolved participation to the call owner.
  Decisions validate their envelope before authenticating participation; directory
  dispatch does not pre-enroll decision requests.
- `TxnServer` owns allocation, verification, fixed expiry, the durable frontier,
  oldest-first cutoff scheduling, readiness, and workspace cleanup only.
- `TxnHandle` implements Gateway resource access separately from OpExecutor
  operation execution. Resource permissions remain with their owners; Gateway
  write-scope checks do not replace authorization.
- Graph scheduling, admission, deadlines, and `OpDef` execution remain here.
  IR values expose intrinsic dependency discovery; executor-local planning stays
  here. The executor is private host machinery, never a public execution API.
  Recursive State resolution and native routing use `tc_state`. Class
  validation reports structure and declaring-method ownership only; application
  link classification and dependency scopes remain server policy.

## Requests and adapters

- `Kernel::new` completes recovery validation and starts expiry before returning.
  Adapters receive the finished kernel.
- `Kernel::begin_request` and `BoundRequest` are the single top-level admission
  path. HTTP and PyO3 do not bind transactions, route applications, select
  outcomes, or stage resources independently. Delegated Gateway calls inherit
  transaction authority and execution limits; same-host dependencies do not
  construct another bound request or gain completion authority. Remote calls
  enter the receiving host as participants.
- `Kernel::begin_request` derives the host's absolute deadline. Callers may
  cancel earlier but cannot extend execution beyond host policy. The bound
  request's transaction accessor is the deliberate trusted-native capability edge used
  by native response handles; decoded or foreign values never construct one.
- `Cluster<Dir<T>>::lookup` selects the owning resource through transactional
  directory membership; traversal alone does not claim ancestor leadership.
  The selected owner admits the call. Live routing never scans for a
  semantic-version boundary. Missing nonempty suffixes return `NotFound`.
- Ordinary operations invoke the selected native verb closure directly. A
  locked exact bodyless PUT or DELETE is interpreted by the resolved Cluster as
  commit or rollback. An
  unlocked absent mutation is invalid; explicit JSON `null` is ordinary delete.
- Native handlers exchange `State<TxnHandle>` directly. HTTP encodes at the
  network boundary; WASM encodes at the sandbox boundary; PyO3 projection lives
  only in `client/rust`.
- HTTP owns parsing, bounded bodies, response streaming, and wire codecs. It has
  no semantic fallback callback. Readiness and canonical target classification belong to the kernel; adapters
  project native State into their transport representation. Unknown kernel targets return
  `NotFound`.
- The bound request selects the application media type and size limit and
  delegates definition and State decoding to the native codecs. HTTP and PyO3
  supply their bounded inputs and retain the request through projection.
- HTTP pull-decodes application JSON directly into its canonical `(Link, Scalar)`
  definition. Admission is a request-local capacity lease, never a binary body
  carrier. Only raw WASM is materialized as bytes because those bytes are the
  semantic application value; the lease remains held through installation and
  terminal response success.
- Domain and protocol values expose `destream` codecs; HTTP and persistence
  select JSON at their real boundaries and stream it in both directions. A
  contiguous buffer is permitted only for semantic bytes or an intrinsically
  contiguous AEAD/WASM call, and remains bounded and owner-local. Operator JSON
  configuration is not a TinyChain semantic wire type.
- Decoded IR and WASM are untrusted. Top-level ingress crosses a bound request;
  delegated execution inherits its absolute deadline, graph shape limits,
  shared operation budget, and capacity permits. `TxnHandle`, `Gateway`, and
  `OpExecutor` are trusted host SPI; decoded values cannot choose their declaring
  Class or widen a budget. A `TxnHandle` carries this enforcement context but
  does not interpret an `OpDef`; the private executor validates and interprets
  each definition once.

## Applications and storage

- Application definitions are canonical one-entry literals. The Library root
  accepts a literal definition or `Value::Bytes` containing raw WASM; Class and
  Service roots accept their literal definitions.
- Classes referenced by a Library are installed independently through the Class
  root and remain ordinary links in its member map. Concrete Library, Class, and
  Service values own validation, transactional versions, immutable conflict checks, routing,
  replication, and lifecycle.
- Committed layouts are strict: Library has `manifest.json` and optional
  `module.wasm`; Class has only `manifest.json`. Service owns its manifest and
  delegates native member storage through ordinary directory handles. Unsupported or
  ambiguous layouts fail without mutation.
- Every recursive directory owns a delegated `txfs::Dir`, including its typed
  manifest and optional module handles. Concrete resources retain decoded
  definitions and runtime state. `txfs` manages immediate files and membership;
  recursive application owners explicitly load and delegate child lifecycle.
  Bootstrap reads files through one
  ordinary `TxnServer`-allocated transaction. Application claims do not allocate
  collection workspaces.
- `HostStorage` may create a root immediately before its first `freqfs::Cache`
  load, and bootstrap may read bounded configuration outside those roots. Mark
  such direct filesystem calls with an inline bootstrap justification. Once a
  root is loaded, server code uses only its delegated `freqfs`/`txfs` handles.
- Conversions from reusable dependency errors belong to `tc-error` behind the
  dependency's optional feature. Server code uses those `From` implementations
  and must not maintain parallel error mappers.
- Bootstrap loads each recursive root once and constructs final Library, Class,
  and Service values directly. It validates the complete loaded Class graph
  without rebuilding a second runtime tree. Generic recursive loading belongs
  to `Cluster<Dir<T>>`; the kernel retains only Class-before-Library-before-Service
  orchestration.
- Runtime Library, Class, and Service values depend on storage and execution
  context and are not wire codecs. The one-entry literal definition is the sole
  application wire representation.
- Executable Services follow [SERVICE_CONTRACT.md](SERVICE_CONTRACT.md).
  Standalone named persistent collections remain unsupported; Service delegates
  member persistence and recovery to Chain. Do not add a server registry.

## Transactions and replication

- Ordinary owner PUT/POST/DELETE execution commits before returning its State
  for projection. Returned execution errors roll back through the locally owned,
  claimed coordinator. Participants, ownerless work, and locked decisions never
  coordinate another decision. There is no successful-mutation flag.
- GET execution and descendants are read-only, including forwarded POST work.
  Reject actual PUT/DELETE effects before mutation; pure calculations and fresh
  literal construction remain available.
- The coordinator is the first selected resource after ownership in signed claim
  order, never a mirrored or sorted path. Dependent calls branch authority and
  retain attempted dependencies, including failed deliveries, at their owner.
- Resource admission, enrollment, and sealing share one short mutex. A sealed
  resource rejects late work; active calls make a decision return `Conflict`.
  Tokens cover whole execution calls. An untouched replica may accept sealed
  decision-only participation after verifying the exact signed leader against
  its membership; this never admits application work. Keep participation until
  cutoff, without storing outcomes or adding a transaction ledger.
- Installation and exact deletion belong to the namespace root. Reserve
  descendant admission before structural work; reject enlisted descendants or
  affected replica identities that differ from the namespace's. Never remove
  the coordinating owner or claim descendants merely for structural traversal.
- Execute PUT/DELETE locally, then replicate successful work. Execute composed
  POST once and propagate its dependent calls. A failed peer may be evicted only
  while surviving replicas retain a strict majority.
- The owner signs commit or rollback on its transaction lock. Decision branches
  carry signed exact-resource ancestry and that outcome. Recipients authenticate
  backedges against their retained leader; only the local leader skips them,
  while replicas still apply. Reject contradictory authority or outcomes and
  allow duplicate diamond delivery through idempotent lifecycle methods.
  Leaders propagate decisions before local application; replicas do not forward.
  Delivery is fail-fast, without an outcome or retry ledger.
- Projection retains the request deadline, capacity, and shutdown ownership but
  selects no deferred decision. Failure or abandonment cannot undo a completed
  commit. Keep Chain's failed-task rejection. The request task retains admission
  through cancellation rollback. Eligible cleanup receives a fresh bounded host
  request deadline; it does not renew expiry or authorize new work. Cleanup failure
  is fatal. Direct collection callers own rollback, quiescence, and disposal after
  uncertain materialization.
- Immutable result snapshots can survive commit after mutation quiescence.
  Rollback, finalization, and expiry still require affected streams and guards
  to be released; retaining a result does not extend transaction expiry.
- Local cutoff finalization recursively visits application roots and sends no
  RPC. Failure withdraws readiness and terminates the host; uncertain canonical
  storage is never retried. Shutdown drains requests and joins an active finalizer
  before dropping the runtime. Recovery evidence and selection belong to Chain.
- Distributed protocol state belongs to its exact `Cluster`. `replicas` is its
  sole reserved child and uses ordinary GET, PUT, and DELETE. Discovery provides
  candidates only; add no global peer registry, join/action endpoint, or adapter
  control route. Bootstrap joins each exact directory and item separately,
  preserving the joining host's signed ownership while the seed adds leadership.
  Validate returned authority before adoption; an owner on a joining replica
  delegates its decision to the resource leader before local application.
- The [Service contract](SERVICE_CONTRACT.md#source-and-adaptation)
  documents deliberate differences from v1: read-only GET, returned-error
  rollback, strict delivery, and fatal uncertain materialization.

## Features and verification

- Supported features are `http-client`, `http-server`, `http2`, `wasm`, and `mdns`.
  Defaults enable both HTTP adapters, HTTP/2, and WASM. `http2` adds protocol
  support only to enabled HTTP adapters; HTTP/1 remains available. Features gate
  boundaries, not application owners or transaction behavior.
- PyO3 is not a server feature. Build the extension from `client/rust`.
- Run all targets/features, supported feature powersets, architecture checks,
  and relevant HTTP/WASM/Python/two-host acceptance after ownership changes.
- Keep handwritten unsafe code out of the crate. The foreign-runtime exception
  is owned and narrowly allowlisted elsewhere.
- Configure Wasmtime only in the bootstrap-owned compiler. Fuel, Store resource
  limits, compilation admission, blocking isolation, and checked ABI ranges are
  mandatory; JSON Library construction must not initialize Wasmtime.
- Production Library compilation accepts binary WebAssembly only. WAT text is
  converted explicitly in test fixtures. Keep Wasmtime's compiler, parallel
  compilation, demangling, and address symbolization features enabled; optional
  runtime facilities require an actual use before adding them.

## Transport projection

- Resources return native State; bound requests own execution and completion.
  Adapters encode ordinary results without resource-specific response flags or
  alternate serialization pipelines. Raw WASM is an upload representation only.
- Share native view acquisition through `TxnHandle::view` and synchronous deadline
  checks through `Deadline::check`. Adapters own encoding and foreign conversion;
  derive a pending result's deadline from its bound request.
- Preserve one absolute deadline and request ownership through terminal projection.
  Projection does not defer or revise the completed execution decision. Native
  handoff must not serialize values, and embedded callers explicitly drain their
  kernel after consuming or dropping retained results.
- Keep composed futures small: avoid nested inline collections and retain boxing
  at measured stack boundaries. End-to-end transport regressions run on ordinary
  worker stacks without inherited stack-size overrides.

- Supported application builds abort on Rust panic, including detached tasks.
  Panics do not run rollback or destructors; ordinary errors and cancellation keep
  their existing cleanup policy. Validate panic policy with built artifacts, not
  only the unwinding test harness. See `SERVICE_CONTRACT.md`.
