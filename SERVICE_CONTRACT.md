# Executable Service contract

A Service owns one immutable versioned application definition and an ordered map
of scalar or Chain attributes. Recursive application directories own version
membership. The Service owns neither a version registry nor a second definition
file. Supported persistent members are SyncChain-backed BTree and Table values;
other Chain variants and persistent Tensor declarations fail explicitly.

## Native execution

Definitions use ordinary scalar values, operation definitions, and native GET
references declaring a Chain and its collection schema. They have no Service
envelope. Attribute hashing combines ordered names with native scalar or
transaction-consistent Chain hashes. Cache settings and physical filenames are
not application identity.

A Chain member retains its owner as `State::Chain`. Routing selects its native
handler once and invokes the selected closure. Scalar methods use the shared
`BoundMethod` and executor with the Service's dependency scope, absolute deadline,
and operation budget. Collection views retain their existing limitation: an
escaped mutable native collection handle is outside Chain interception.

The exact Service Cluster replicates PUT/DELETE before local execution. Replicated
methods bind their own member writes to native `$self` and must not directly write
other resources. Participants execute locally without forwarding. GET/POST methods
bind `$self` to the application link; composed writes therefore cross the owning
PUT/DELETE boundary. Recursive expression binding belongs to `tc-ir`, not Service.

## Persistence and recovery

The transactional application directory owns `manifest.json` and membership. Its
delegated `.native` subtree belongs to Service members:

```text
.native/<attribute>/subject/                 canonical collection
.native/<attribute>/wal/committed.chain_block retained Chain requests
.native/<attribute>/values/                  captured collection arguments
```

`txfs` does not interpret, version, or recursively synchronize this subtree.
The application file composition delegates collection and Chain codecs to their
owners. Bootstrap still owns separate application, control, and workspace caches.
The host delegates its configured operation capacity to each Chain queue.

Service lifecycle recursively delegates to Chains before completing its enclosing
directory lifecycle. Commit waits for durable WAL publication and logical
collection visibility. Finalization completes every member's canonical durability
before the host can delete transaction workspaces. There is no background commit.

Startup opens and validates all unpublished owners, assembles the kernel, then
recovers retained requests before readiness or expiry starts. A private TxnServer
path supplies original IDs and fresh workspaces, including expired committed IDs;
it rejects IDs at or below the durable host frontier. Normal ingress authentication
and expiry remain unchanged. Recovery failure returns an error without publishing
the kernel. Chain retains the WAL until successful finalization.

## Healthy replica synchronization

Joining uses the existing authenticated exact-resource bootstrap and transactional
membership. Definitions must match. The authenticated source hash and every native
member snapshot share one transaction ID. Snapshots use streamed State codecs and
Chain's native transactional restoration. The resulting aggregate hash must match
before membership and readiness are published. Deadline, cancellation, schema,
and integrity failures prevent successful joining; a reachable endpoint alone is
not authority.

This operation handles a healthy local owner. Corrupt storage or a surviving
materialization marker remains unavailable. Ordinary restoration never clears the
marker, discards damaged evidence, or selects an authoritative divergent history.
Authenticated damaged-storage replacement remains separate work.

## Source and adaptation

Attribute ownership, recursive routing, method binding, hashing, and lifecycle
delegation come from v1 `cluster/service.rs` (`Version`) and
`cluster/public/service.rs`, revision
`17ef342e8f7026e4c4a60d2044de9aeb1b145b91`. V2's existing directories replace the
outer version registry and `schemata` file. Native State/file composition and
open-then-recover are API adaptations. Original-ID startup capabilities and
transaction-bound snapshot joining integrate those owners with the v2 host.

Development fixtures for the native host layout must be recreated. There is no
migration reader. Restart tests demonstrate recovery boundaries, not physical
power-loss safety.
