# tc-server invariants

This file defines the repository-local host runtime rules. When this repository
is checked out by the TinyChain superproject, the
[workspace invariants](https://github.com/TinyChain-Inc/tcv2/blob/main/AGENTS.md)
provide additional, non-normative integration context.

## Ownership

- Bootstrap creates one complete `Kernel`, `TxnServer`, `HostServices`,
  `HostRuntime`, `HostResources`, `HostStorage`, and protocol authority. There
  are no optional production owners, gateways, finalizers, or hidden defaults.
- `HostStorage` creates exactly one data cache and one transaction-workspace
  cache. Application code receives delegated roots and never constructs caches
  or filesystem paths. The superproject's
  [storage document](https://github.com/TinyChain-Inc/tcv2/blob/main/docs/storage.md)
  records the integrated layout.
- Application roots are `Cluster<Dir<Library>>`, `Cluster<Dir<Class>>`, and
  `Cluster<Dir<Service>>`. The kernel selects a root once; recursive directories
  consume structure; the concrete application owns behavior and persistence.
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
  reading transactional state. Missing nonempty suffixes return `NotFound`.
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
  Service values own validation, staging, immutable conflict checks, routing,
  replication, and lifecycle.
- Committed layouts are strict: Library has `manifest.json` and optional
  `module.wasm`; Class and Service have only `manifest.json`. Unsupported or
  ambiguous layouts fail without mutation.
- Application staging lives only in the transaction workspace. Concrete owners
  publish through `freqfs`; Library publishes its module before its manifest.
  Application storage never uses `txfs`.
- Bootstrap scans each root once, decodes private drafts, analyzes the complete
  graph once, and exposes only fully scoped immutable runtime values.
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

## Features and verification

- Supported features are `http-client`, `http-server`, `wasm`, and `mdns`.
  Defaults enable both HTTP features and WASM. Features gate boundaries, not
  application owners or transaction behavior.
- PyO3 is not a server feature. Build the extension from `client/rust`.
- Run all targets/features, supported feature powersets, architecture checks,
  and relevant HTTP/WASM/Python/two-host acceptance after ownership changes.
- Keep handwritten unsafe code out of the crate. The foreign-runtime exception
  is owned and narrowly allowlisted elsewhere.
