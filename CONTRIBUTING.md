# Contributing to tc-server

Read this repository's [ownership rules](AGENTS.md) and
[Service contract](SERVICE_CONTRACT.md).

Run Cargo commands sequentially. Use `CARGO_BUILD_JOBS=1` by default, or explicitly
choose a positive job count appropriate to available memory. Before review,
select the affected library or named integration targets and strict Clippy.
Public/shared changes include their boundary regressions. The
complete owner suite, required before release, is:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Feature changes require supported feature-power checks. Run relevant
architecture/documentation checks and cross-adapter/two-host acceptance tests
when their ownership boundaries change; unrelated edits do not require them.

Changes to public routing, application storage, or transaction behavior update
the owning contract and, in a superproject checkout, its corresponding integration
guidance. Do not maintain a second protocol description here. PyO3 changes belong
in `client/rust`.

Keep unit-test implementations and shared fixtures under `tests/support/`.
Conditional path-based modules beside their implementation owners preserve
private access, test identities, and feature gates. Keep production logic beside
its owner; moving tests does not reduce runtime complexity.

Use explicit targets when checking a changed boundary. A test-name filter alone
does not prevent Cargo from compiling unrelated integration targets. Run these
commands from this crate; each fixture includes successful and rejected behavior:

```bash
# Credential parsing, signed claims, ownership, and inherited read-only scope
CARGO_BUILD_JOBS=1 cargo test --all-features --test auth
CARGO_BUILD_JOBS=1 cargo test --all-features --lib txn::tests::

# Local-before-fanout decisions and dependency GET -> POST -> write rejection
CARGO_BUILD_JOBS=1 cargo test --all-features --lib kernel::kernel::tests::request_protocol::
CARGO_BUILD_JOBS=1 cargo test --all-features --lib kernel::kernel::tests::class_instance_routes_bound_methods_with_concrete_self -- --exact

# Invocation admission/cancellation and graph scheduling
CARGO_BUILD_JOBS=1 cargo test --all-features --lib txn::handle::execution_driver_tests::
CARGO_BUILD_JOBS=1 cargo test --all-features --lib op_executor::

# Expiry/shutdown, fatal cleanup, recovery, and real HTTP replication/restart
CARGO_BUILD_JOBS=1 cargo test --all-features --lib txn::server::tests::
CARGO_BUILD_JOBS=1 cargo test --all-features --lib kernel::kernel::tests::rollback_propagation::
CARGO_BUILD_JOBS=1 cargo test --all-features --lib kernel::kernel::tests::service_recovery_preserves_expired_original_ids_and_rejects_invalid_frontiers -- --exact
CARGO_BUILD_JOBS=1 cargo test --all-features --test two_host
```

These focused commands supplement the complete suite; they are not an exhaustive
security audit. Run affected PyO3 lifecycle tests against a freshly built extension
when changing shared request ownership or completion.

Transactional Tensor composition is covered by `kernel::kernel::tests::tensor_service`
and the populated-Service test in `tests/two_host.rs`. These adapt v1's
`TensorChainTests` scenarios (independent dense/sparse members and service-method
writes) to current SyncChain routing and signed HTTP. Collection tests retain
local numerical, reservation, and mutation-failure guarantees; Chain tests retain
replay contracts. Run their suites explicitly, since dependency tests are not
included by `cargo test` here.

The Tensor subprocess fixture distinguishes an abrupt abort after durable commit
from failed canonical publication. The former must replay committed values; the
latter must terminate the host and reject reopening ambiguous storage. Neither
is a power-loss test. Test-only save observations are task-scoped to the selected
Service finalization and add no fields to production owners.
