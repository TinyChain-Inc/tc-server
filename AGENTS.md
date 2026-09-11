# tc-server invariants

This file defines the repository-local host runtime rules. When this repository
is checked out by the TinyChain superproject, the
[workspace invariants](https://github.com/TinyChain-Inc/tcv2/blob/main/AGENTS.md)
provide additional, non-normative integration context.

## Ownership

- Bootstrap creates one complete `Kernel`, `TxnServer`, `HostServices`,
  `HostResources`, `HostStorage`, and protocol authority. There
  are no optional production owners, gateways, finalizers, or hidden defaults.
- `HostStorage` creates one application-data cache, one host-control cache, and
  one collection-workspace cache. Application code receives delegated roots
  and never constructs caches or filesystem paths. The superproject's
  [storage document](https://github.com/TinyChain-Inc/tcv2/blob/main/docs/storage.md)
  records the integrated layout.
- Application roots are `Cluster<Dir<Library>>`, `Cluster<Dir<Class>>`, and
  `Cluster<Dir<Service>>`. The kernel selects a root once; recursive directories
  consume structure; the concrete application owns behavior and persistence.
- The shared `Kernel` state owns those three roots directly. Do not introduce an `app` or
  `application` module, aggregate owner, generic payload, or facade: TinyChain
  has no fourth domain object above Library, Class, and Service.
- `Cluster<T>` owns exact-resource claims, leadership, propagation, and direct
  `Transact` delegation. It has no decision ledger, transaction registry,
  reconciliation policy, or per-resource signing key.
- `TxnServer` owns allocation, verification, fixed expiry, the durable frontier,
  oldest-first cutoff scheduling, readiness, and workspace cleanup only.
- Graph scheduling, admission, deadlines, and `OpDef` execution remain here.
  IR values expose intrinsic dependency discovery; executor-local planning stays
  here. Recursive State resolution and native routing use `tc_state`.

## Requests and adapters

- `Kernel::new` completes recovery validation and starts expiry before returning.
  Adapters receive the finished kernel.
- `Kernel::begin_request` and `KernelRequestGuard` are the single bound request
  path. HTTP and PyO3 do not bind transactions, route applications, select
  outcomes, or stage resources independently.
- `Cluster<Dir<T>>::lookup` claims every traversed directory and item before
  reading transactional state. Directory membership determines the terminal
  item; live routing never scans for a semantic-version boundary. Missing
  nonempty suffixes return `NotFound`.
- Ordinary operations invoke the selected native verb closure directly. A
  locked exact bodyless PUT or DELETE is interpreted by the resolved Cluster as
  commit or rollback. An
  unlocked absent mutation is invalid; explicit JSON `null` is ordinary delete.
- Native handlers exchange `State<TxnHandle>` directly. HTTP encodes at the
  network boundary; WASM encodes at the sandbox boundary; PyO3 projection lives
  only in `client/rust`.
- HTTP owns parsing, bounded bodies, response streaming, and wire codecs. It has
  no semantic fallback callback. Unknown kernel targets return `NotFound`.

## Applications and storage

- Application definitions are canonical one-entry literals. The Library root
  accepts a literal definition or `Value::Bytes` containing raw WASM; Class and
  Service roots accept their literal definitions.
- Classes referenced by a Library are installed independently through the Class
  root and remain ordinary links in its member map. Concrete Library, Class, and
  Service values own validation, transactional versions, immutable conflict checks, routing,
  replication, and lifecycle.
- Committed layouts are strict: Library has `manifest.json` and optional
  `module.wasm`; Class and Service have only `manifest.json`. Unsupported or
  ambiguous layouts fail without mutation.
- Every recursive directory owns a delegated `txfs::Dir`, including its typed
  manifest and optional module handles. Concrete resources retain decoded
  definitions and runtime state. Recursive `txfs` lifecycle publishes committed
  versions and discards abandoned versions. Bootstrap reads them through one
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
  without rebuilding a second runtime tree.
- Service execution and standalone named persistent collections are unsupported
  until Service and Chain own them. Do not add a server registry or placeholder.

## Transactions and replication

- Failed, cancelled, timed-out, or unprojectable requests send no rollback. The
  superproject's
  [transaction document](https://github.com/TinyChain-Inc/tcv2/blob/main/docs/protocol/transactions.md)
  describes the integrated protocol.
- The first owning Cluster coordinates a successful mutation after the complete
  request succeeds. Each exact leader propagates to replicas for its path and
  participants do not forward.
- Propagation is fail-fast and precedes local lifecycle application. Duplicate
  delivery replays idempotent resource behavior; no outcome or retry ledger is
  retained.
- Local cutoff finalization recursively visits application roots and sends no
  RPC. A failure prevents frontier advancement and readiness until the same
  cutoff succeeds.
- Divergence evidence fails closed. Canonical selection, replay, and repair
  belong to `tc-chain`, never bootstrap or HTTP replication.
- Distributed protocol state belongs to the exact `Cluster` it describes.
  `replicas` is the sole reserved Cluster child and uses ordinary GET, PUT, and
  DELETE with the Cluster lifecycle. Configuration, Kubernetes DNS, and mDNS
  provide bootstrap candidates only. Never add a global peer registry, a
  join/action endpoint, gateway-selected membership, or adapter-local control
  route.
- Replicate work before local mutation. A resource may evict failed replicas
  only while its surviving snapshot retains a strict majority; never turn
  write-time eviction into post-decision repair or server-owned reconciliation.
- Bootstrap traverses directory Clusters before terminal item Clusters and uses
  a separate ordinary transaction for each exact membership change. Joining a
  root alone is incorrect. Configured seeds gate readiness; discovery failure
  must not be hidden behind a healthy Kubernetes readiness response.

## Features and verification

- Supported features are `http-client`, `http-server`, `wasm`, and `mdns`.
  Defaults enable both HTTP features and WASM. Features gate boundaries, not
  application owners or transaction behavior.
- PyO3 is not a server feature. Build the extension from `client/rust`.
- Run all targets/features, supported feature powersets, architecture checks,
  and relevant HTTP/WASM/Python/two-host acceptance after ownership changes.
- Keep handwritten unsafe code out of the crate. The foreign-runtime exception
  is owned and narrowly allowlisted elsewhere.
