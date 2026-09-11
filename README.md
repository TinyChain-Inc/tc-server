# tc-server

`tc-server` is the TinyChain host runtime. It bootstraps shared resources,
executes graphs, hosts recursive applications, and exposes HTTP and WASM process
boundaries. Native State behavior belongs to `tc-state`; graph analysis belongs
to `tc-ir`; PyO3 belongs to `client/rust`.

## Request path

Every supported application request follows:

```text
KernelRequestGuard
  -> parsed application Link
  -> Cluster<Dir<Library | Class | Service>>
  -> Handler<State>
  -> Transact
```

Directories own recursive membership and delegated `txfs` storage. Concrete
applications own validation, file handles, routing, replication, and lifecycle. HTTP and
other adapters only project this native path.

“Application” is descriptive vocabulary, not a server abstraction. The runtime
owns the Library, Class, and Service roots directly; there is no aggregate app
owner or generic application payload.

## Applications

- JSON Library, Class, and Service definitions are one-entry literals keyed by
  their canonical application identity.
- A precompiled Library is one raw `application/wasm` body containing its
  embedded definition.
- Application versions are immutable. Identical installation is a no-op;
  different content at the same identity conflicts.
- Service persistence and discovery are supported. Service execution is not.

The identity maps directly beneath `data_dir`; transactional versions are owned
by the recursive `txfs` directory. The workspace contains host-control and
collection transaction state, not a second application representation. The superproject
[storage contract](https://github.com/TinyChain-Inc/tcv2/blob/main/docs/storage.md)
is non-normative integration context.

## Transactions

Resources claim and handle their own transactional versions. Exact authenticated
bodyless PUT and DELETE requests commit and roll back the resolved resource.
Failed requests send no rollback; abandoned versions are discarded by recursive
cutoff finalization. The superproject
[transaction protocol](https://github.com/TinyChain-Inc/tcv2/blob/main/docs/protocol/transactions.md)
is non-normative integration context.

## Features and tests

The features are `http-client`, `http-server`, `wasm`, and `mdns`. The default
set enables both HTTP features and WASM.

```bash
cargo test --all-targets --all-features
cargo check --no-default-features
cargo check --no-default-features --features http-client
cargo check --no-default-features --features http-server
cargo check --no-default-features --features wasm
cargo test --no-default-features --features http-client,http-server,mdns --bin tc-server
```

The HTTP features are included in the mDNS command because the `tc-server`
binary requires them; testing `mdns` alone would compile only the library and
would not exercise discovery or advertisement.

Build and test the `tinychain_local` PyO3 extension from `client/rust`; there is
no PyO3 feature or module in this crate.

## Integration context

The workspace links below are non-normative for this standalone repository.

- [Host-specific invariants](AGENTS.md)
- [Workspace architecture](https://github.com/TinyChain-Inc/tcv2/blob/main/ARCHITECTURE.md)
- [Backpressure contract](https://github.com/TinyChain-Inc/tcv2/blob/main/BACKPRESSURE.md)
- [Server roadmap](ROADMAP.md)

Release blog posts and roadmaps are non-normative and cannot override these
contracts.
