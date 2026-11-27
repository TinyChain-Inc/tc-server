# tc-server

`tc-server` is the TinyChain host runtime. It bootstraps shared resources,
executes graphs, hosts recursive applications, and exposes HTTP and WASM process
boundaries. Native State behavior belongs to `tc-state`. IR values expose intrinsic
dependencies; the server plans and schedules their execution. PyO3 belongs to
`client/rust`.

## Request path

Every top-level application request enters through `Kernel::begin_request`:

```text
BoundRequest
  -> parsed application Link
  -> Cluster<Dir<Library | Class | Service>>
  -> Handler<State>
  -> Transact
```

Directories own recursive membership and delegated `txfs` storage. Concrete
applications own validation, file handles, routing, replication, and lifecycle.
HTTP and other adapters project this native path.

WASM Libraries select an exported binding during native routing. The terminal
handler borrows the Library and invokes that binding without searching the route
again. Missing routes return `NotFound` for every verb before argument projection
or guest access; unsupported verbs on existing routes return `MethodNotAllowed`.

A `BoundRequest` owns admission capacity, the deadline, and shutdown tracking
through terminal projection. Entry checks precede body decoding; routing still
enforces resource-specific authorization. Dropping an unexecuted request or a
projected response selects no transaction outcome.

Nested `Gateway` calls inherit the transaction and execution scope. The dependency
resolver checks that scope and delegates to native dispatch or an outbound request;
a same-host dependency does not construct another `BoundRequest` or acquire
transaction-completion authority. Remote requests enter the receiving host as
participants, which do not select the owner's decision.

HTTP application requests share one lifecycle: bind, decode, execute and decide,
then project the response. Application definitions and WASM differ only in body
decoding. Ordinary results share State projection, and uploads complete without a
response body. Upload admission remains held through completion; streamed responses
retain the request until EOF. Projection errors cannot reverse a commit; execution
errors or cancellation trigger eligible owner rollback.

“Application” is descriptive vocabulary, not a server abstraction. The runtime
owns the Library, Class, and Service roots directly; there is no aggregate app
owner or generic application payload.

A root operation owns a flat set of suspended native invocations. Nested calls
submit continuations to its bounded queue and retain only a weak handle to that
process. The existing graph executor interprets every operation; invocation and
depth budgets bound suspended work, while graph permits bound active work.
Dropping the root releases queued and active invocations. The driver does not
select transaction outcomes; the enclosing bound request owns cancellation
rollback. Nested calls retain the original absolute deadline.

## Applications

- JSON Library, Class, and Service definitions are one-entry literals keyed by
  their canonical application identity.
- A precompiled Library is one raw `application/wasm` body containing its
  embedded definition.
- Application versions are immutable. Identical installation is a no-op;
  different content at the same identity conflicts.
- Services execute methods and own SyncChain-backed BTree, Table, and Tensor members;
  see the [Service contract](SERVICE_CONTRACT.md) for recovery and healthy joins.
  The [Service example](examples/service.md) covers installation, method calls,
  mutation, and restart using a canonical definition reusable by client authors.

The identity maps directly beneath `data_dir`; transactional versions are owned
by the recursive `txfs` directory. The workspace contains host-control and
collection transaction state, not a second application representation.

## Transactions

Resources claim and handle their own transactional versions. Exact authenticated
bodyless PUT and DELETE requests commit and roll back the resolved resource.
The owning request commits successful PUT/POST/DELETE execution before response
projection and rolls back returned execution errors through its claimed
coordinator. GET descendants are read-only, while pure calculations remain
available. Cancelled or timed-out execution completes eligible owner rollback in a
host-owned task. Rollback has a fresh cleanup deadline using the configured host
request duration; it does not renew transaction expiry or authorize new work.
Commit propagation retains the original deadline, and its failure never selects
rollback. Failed decoding selects no decision. Failed or abandoned projection cannot
undo a completed commit. Rollback failure or cleanup timeout terminates the host.
Recursive cutoff finalization discards abandoned versions, and uncertain canonical
materialization remains fatal.

## Features and tests

The features are `http-client`, `http-server`, `http2`, `wasm`, and `mdns`. The
default set enables both HTTP adapters, HTTP/2, and WASM. HTTP/1-only builds omit
`http2`; enabling `http2` alone does not activate either adapter.

The `wasm` feature enables Cranelift, parallel compilation, and guest backtrace
symbolization. See the [compiler boundary rules](AGENTS.md#features-and-verification)
for production input and required resource limits.

```bash
cargo test --all-targets --all-features
cargo check --no-default-features
cargo check --no-default-features --features http-client
cargo check --no-default-features --features http-server
cargo check --no-default-features --features http-client,http-server
cargo check --no-default-features --features http-client,http-server,http2
cargo check --no-default-features --features wasm
cargo test --no-default-features --features http-client,http-server,mdns --bin tc-server
```

The HTTP features are included in the mDNS command because the `tc-server`
binary requires them; testing `mdns` alone would compile only the library and
would not exercise discovery or advertisement.

Build and test the `tinychain_local` PyO3 extension from `client/rust`; there is
no PyO3 feature or module in this crate.

## Related documents

- [Host-specific invariants](AGENTS.md)
- [Service contract](SERVICE_CONTRACT.md)
- [Server roadmap](ROADMAP.md)

Release blog posts and roadmaps are non-normative and cannot override these
contracts.

### Graceful shutdown

SIGTERM and SIGINT withdraw readiness and stop request admission. Existing requests
retain their deadlines through uploads, response projection, and commit propagation.
HTTP drains connections, then cancels stalled transports once those deadlines have
expired. The host stops new expiry passes and joins any active finalization before
dropping Tokio. Active canonical writes have no additional shutdown timeout.

Embedded callers must await `Kernel::shutdown()` before dropping their runtime or
reopening storage. `begin_shutdown()` starts admission closure without waiting;
repeated or concurrent shutdown calls are safe. HTTP's shutdown helper delegates
to this lifecycle. Shutdown does not force unexpired transactions to materialize;
durable Chain history remains available for replay. SIGKILL or expiry of a
supervisor's termination grace period is abrupt termination, not a successful drain.

All ordinary application responses use the State codec, including WASM Library
GET responses, which encode a byte value as JSON. The former raw
`application/wasm` response is no longer supported; raw WASM uploads remain
supported. Outbound and bootstrap limits bound encoded bytes, including encoding
overhead, so the module size fitting those unchanged limits is smaller.
