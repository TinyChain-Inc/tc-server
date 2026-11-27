# Executable Service contract

A Service owns one immutable versioned application definition and an ordered map
of scalar or Chain attributes. Recursive application directories own version
membership. The Service owns neither a version registry nor a second definition
file. Supported persistent members are SyncChain-backed BTree, Table, and Tensor
values; other Chain variants fail explicitly.

## Native execution

Definitions use ordinary scalar values, operation definitions, and native GET
references declaring a Chain and its collection schema. They have no Service
envelope. Attribute hashing combines ordered names with native scalar or
transaction-consistent Chain hashes. Cache settings and physical filenames are
not application identity.

A Chain member retains its owner as `State::Chain`. Routing selects its native
handler once and invokes the selected closure. Scalar attributes delegate handler
selection to `tc-state`; only method binding constructs the native `$self` map.
Methods use the shared `BoundMethod` and executor with the Service's dependency
scope, absolute deadline, and operation budget.

Every application mutation of a persistent mutable collection must pass through
its owning Chain. Returned views grant read access, not mutation authority,
including full reads and identity transforms. Selection-write conveniences target
the original owner with explicit bounds, so accepted mutations are recorded and
replayed across transactions. Native, HTTP, and PyO3 callers share this boundary;
no adapter grants write authority to a returned collection view.

The selected Service Cluster executes PUT/DELETE locally before replicating
successful work. Replicated methods bind member writes to native `$self` and
must not directly write other resources; replicas execute without forwarding.
Composed POST executes once, binds `$self` to the application link, and retains
its dependent calls at the Service owner. GET inherits a read-only scope through
nested and remote calls: pure POST calculations and fresh literal construction
remain valid, but mutation fails before its effect. Recursive expression binding
belongs to `tc-ir`.

Resource call tokens cover execution. Decisions seal enrollment and conflict
while calls are active; attempted dependencies remain available for repeated
delivery until cutoff. Signed ancestry carries exact identity and outcome,
prevents cycles, and permits idempotent duplicate delivery along diamond paths.
An untouched replica accepts sealed decision-only participation only with exact
leadership authenticated against its replica membership; late application work
remains forbidden.
An ordinary owning PUT/POST/DELETE commits successful execution before response
projection and rolls back an execution error, cancellation, or timeout. Cleanup
has a fresh host request deadline; cleanup failure is fatal. Projection retains request
resources but cannot undo commit. The transaction contract owns these policies.

Installation and exact Service deletion use the namespace root as coordinator.
Structural work reserves descendant admission and rejects already-enlisted
owners or affected replica identities that differ from the namespace's. The
removed Service cannot become its deletion coordinator.

## Persistence and recovery

Creation and loading validate the complete definition before constructing members.
The prepared attributes carry native collection schemas into allocation; the
original immutable definition remains the installation and discovery value.

The transactional application directory owns `manifest.json` and immediate
membership. Service gives its ordinary `.native` child to native member storage:

```text
.native/<attribute>/subject/                 canonical collection
.native/<attribute>/wal/committed.chain_block retained Chain requests
.native/<attribute>/values/                  captured collection arguments
```

`txfs` returns native handles for all child directories and does not interpret,
version, or recursively synchronize their contents. `.native` is a Service layout
name, not a filesystem exception. Application directories explicitly load their
children as transactional directories; Service delegates its members to Chain.
The application file composition delegates collection and Chain codecs to their
owners. Bootstrap still owns separate application, control, and workspace caches.
The host delegates its configured operation capacity to each Chain queue.
Service establishes empty delegated member roots durably before construction;
its enclosing directory lifecycle publishes their namespace membership.

Service lifecycle recursively delegates to Chains before completing its enclosing
directory lifecycle. Commit waits for durable WAL and native collection publication.
Finalization completes every member's native compaction durability
before the host can delete transaction workspaces. There is no background commit.
Immutable results may retain collection snapshots across commit after mutations
finish. Rollback, finalization, and expiry still require release of affected
streams and guards. Chain failed-task checks remain enforced; the host owns
execution cancellation and rollback. Failed rollback and uncertain canonical
materialization terminate the host.

Startup checks unpublished owners' frontiers and strictly loads their subjects,
assembles the kernel, then validates and
recovers retained requests before readiness or expiry starts. A private TxnServer
path supplies original IDs and fresh workspaces, including expired committed IDs;
it rejects IDs at or below the durable host frontier. Normal ingress authentication
and expiry remain unchanged. Recovery failure returns an error without publishing
the kernel or starting expiry, including a failure after earlier transactions
have replayed. Chain retains the WAL until successful finalization. Host admission
uses existing request, execution, and storage backpressure settings without
a separate transaction-count limit. Identities remain tracked through successful
cutoff cleanup. Retained transaction state has no aggregate admission bound and
can accumulate beyond cache budgets when cleanup is delayed. Live admission
resumes after publication without waiting for the recovered backlog to drain.

## Healthy replica synchronization

Joining uses the existing authenticated exact-resource bootstrap and transactional
membership. The joining host supplies signed ownership inside the encrypted
bootstrap request; the seed extends it with leadership, which the joiner validates
before adoption. Both use the same transaction owner and ID. The resolved Cluster
stages membership and delegates native snapshot restoration to its Service before
collecting the membership for convergence. Definitions must match.
The authenticated source hash and every native member snapshot share that
ID. Snapshots use streamed State codecs and
Chain's native transactional restoration. The resulting aggregate hash must match
before membership and readiness are published. Deadline, cancellation, schema,
and integrity failures prevent successful joining; a reachable endpoint alone is
not authority.

This operation handles a healthy local owner. Corrupt referenced storage remains
unavailable. Ordinary restoration never discards damaged evidence or selects an
authoritative divergent history. Interrupted native compaction instead recovers
through Collection's atomic descriptor.
Authenticated damaged-storage replacement remains separate work.

## Source and adaptation

Attribute ownership, recursive routing, method binding, hashing, and lifecycle
delegation come from v1 `cluster/service.rs` (`Version`) and
`cluster/public/service.rs`, revision
`17ef342e8f7026e4c4a60d2044de9aeb1b145b91`. V2's existing directories replace the
outer version registry and `schemata` file. Native State/file composition and
open-then-recover are API adaptations. Original-ID startup capabilities and
transaction-bound snapshot joining integrate those owners with the v2 host.
This is not protocol equivalence with v1: read-only GET descendants, explicit
returned-error rollback, strict decision delivery, and fatal uncertain
materialization are deliberate differences.

Development fixtures for the native host layout must be recreated. There is no
migration reader. Restart tests demonstrate recovery boundaries, not physical
power-loss safety.


### Application panic policy

Ordinary I/O and execution errors reject an operation; their context determines
whether recovery is required. Poisoned cache bookkeeping is an invariant failure,
not permission to continue using the cache. Supported server and Python application
builds use `panic = "abort"` in development and release. A Rust panic, including one
in a detached task, aborts the process without graceful shutdown, rollback, or
running destructors. Chain recovery handles restart. Structured Python exceptions
and WASM guest errors remain ordinary errors.

Rust embedders must set this policy at their application/workspace root: dependency
profiles do not control the final application. The server executable and Python
module reject unwinding builds before creating host resources. Cargo's ordinary
test harness uses unwinding, so unit tests cannot certify the application policy;
see [Cargo's panic profile documentation](https://doc.rust-lang.org/cargo/reference/profiles.html#panic).

### Local integrity failure boundary

Decision authentication, active-call sealing, and remote propagation happen before
local lifecycle application. Once local commit or rollback starts, an error,
cancellation, or panic terminates the host through fatal recovery. A failed commit
never selects rollback. Recovery evidence is retained for restart.

Locally classified live-storage corruption also terminates the host, including
when discovered during lazy response projection or retained native result use.
The delegated storage context applies this policy before codecs erase error types.
Recovery provenance is local metadata, never wire authority: malformed requests
and remote errors cannot request local termination by their status or message.
Startup inspection remains fallible before host publication.
