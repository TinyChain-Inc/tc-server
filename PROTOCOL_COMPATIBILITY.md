# TinyChain protocol compatibility guide

This note explains how to stand up a TinyChain-compatible server (or custom adapter) that honors the protocol described in the public v1 docs (https://docs.tinychain.net) while reusing the shared v2 crates in this workspace.

## Compatibility targets

- **HTTP semantics:** Match the v1 API shapes documented at docs.tinychain.net (`txn_id` query parameter, `/state/...` paths, `/lib` installer shape, root-only empty-body commit/rollback). The v1 GitBook remains the authoritative description of these verbs and resource layouts.
- **IR contract:** Route inbound requests into `tc-ir` handlers so the same capability manifests and IR types are exercised regardless of transport.
- **Transaction + auth surface:** Preserve the `txn_id` lifecycle and capability masks; adapters only translate protocol details and defer begin/commit/rollback to the shared kernel (`Kernel::route_request`).

## Dependency authorization & URI routing

TinyChain application code must never gain arbitrary network egress. Cross-library calls are
expressed as standard TinyChain requests against canonical URIs (constructed via the `tc.uri.*`
helpers). The host remains the sole owner of both routing decisions and authorization checks.

**Rules:**

- **Explicit, non-transitive dependencies.** A library/service `A` may call `B` iff `B` appears in
  `A`’s manifest-declared dependency set (library-wide). If `A → B` and `B → C`, that does not
  imply `A → C`.
- **Default-deny egress.** Outbound RPC is rejected unless the target component is a declared
  dependency and the host has an explicit route/authority allowlist entry for it.
- **Authorize by canonical path, not network location.** Authorization is determined using the
  canonical resource path; whether `B` is served locally or remotely must not change the result.
- **Path-only vs absolute URIs.** Routing is derived from the URI shape:
  - path-only targets are executed locally if available, otherwise resolved to a remote authority
    via host configuration;
  - absolute targets are only permitted if the host accepts the authority for that canonical path.
  This prevents callers from supplying arbitrary authorities to bypass dependency restrictions.
- **Transport symmetry.** The same dependency rule applies whether the call is in-process (PyO3),
  over HTTP, or via another adapter: all requests route through `Kernel::route_request` so the
  transaction and capability contract stays identical.

## Cross-service transaction propagation

TinyChain’s transaction context may span multiple libraries/services, including those served by
remote hosts. Cross-service calls reuse the same transaction identifier, and the kernel must
propagate a capability token which accumulates minimal authorization claims as the transaction
touches additional components.

**Rules:**

- **Finalize semantics.** Commit is an empty `POST` to the canonical service or library root
  derived from the manifest; rollback is an empty `DELETE` to the same root. Empty-body
  `POST`/`DELETE` requests to subpaths must not finalize. The kernel interprets finalize
  requests uniformly across adapters.
- **Begin response includes the minted txn ID.** When a request omits `?txn_id=...`, the kernel
  begins a transaction and adapters return the minted ID in the `x-tc-txn-id` response header so
  callers can continue/finalize without minting their own IDs.
- **Transaction ownership is pinned.** For any active `txn_id`, the kernel rejects requests whose
  `Authorization: Bearer ...` token does not resolve to the same owner identity which began the
  transaction. Additionally, the kernel only accepts a previously-unseen `?txn_id=...` when a
  bearer token is present (so it can pin ownership before executing any handler). Adapters only
  forward the opaque bearer token; token signature verification and claim chaining are still
  pending.
- **Reserved transaction claim (v1-compatible).** When the bearer token carries structured claims,
  the kernel derives the transaction owner from the claim whose `link` is `/txn/<txn_id>` and whose
  mask includes `umask::USER_EXEC`. If the token also includes a `/txn/<txn_id>` claim with
  `umask::USER_WRITE` (a “lock” claim), it must match the same `(host, actor)` identity as the
  owner claim.
- **Transaction-owned HTTP egress.** When routing a request to a remote host, the transaction
  context constructs outbound HTTP requests via its shared gateway so `?txn_id=...` and
  `Authorization: Bearer ...` propagation is implemented once (not duplicated in adapters).
- **Transaction-owned token verification/chaining hooks.** Adapters treat the bearer token as
  opaque and forward it into the kernel. The transaction resolver resolves a stable owner identity
  via its configured `TokenVerifier` and uses the same hook to extend/chains tokens for outbound
  RPC as a transaction touches additional components. The default verifier is now RJWT-backed
  (same bearer token format as v1) once the host can resolve actor keys:
  - in-process via an in-memory keyring resolver (`KeyringActorResolver`);
  - over HTTP (with `http-client`) via `RpcActorResolver`, which fetches actor public keys from the
    issuer host using `GET /host/public_key?key=<actor-id>` (response body is a JSON string
    containing base64-encoded Ed25519 public key bytes).
- **Op references (partial).** The workspace includes a v1-shaped `tc_ir::OpRef` plus a minimal
  executor (`tc_server::resolve`) which can run `TCRef::Op` via the transaction-bound RPC gateway.
  Encoding `OpRef` parameters into the on-the-wire scalar format is still in progress.
- **Token chaining.** When the kernel routes a call into a library/service, it signs (or extends)
  the transaction token with the minimal claim required for that component and mode. As the
  transaction touches more components, the token accumulates one claim per participating
  library/service so downstream hosts can verify the full authorization chain.
- **Remote calls preserve txn context.** Outbound requests include the existing `txn_id` and the
  current chained token, so the remote host participates in the same transaction context rather
  than starting a new one.
- **Host endpoints are non-transactional.** `/host/...` and `/healthz` routes do not begin,
  continue, or finalize transactions. They may receive a `txn_id` query parameter (because the
  kernel always propagates it for RPC), but the host handler ignores it.

## Cluster replication, correctness, and restart convergence

TinyChain does not define a “repair” protocol. Correctness is determined by the cluster’s
replication rules:

- **Majority correctness.** For a given transaction, the version replicated across at least half
  of a cluster’s hosts is considered the correct version for that transaction’s outcome.
- **Restart convergence.** Hosts which missed the correct version converge on restart:
  - `SyncChain` replicas replicate directly from the correct version.
  - `BlockChain` replicas converge by backtracking to a correct prefix and replaying forward.

These semantics bound failure recovery to restart-time replication rather than an online repair
workflow.

## Participant discovery and canonicalization

Participant discovery is host-owned and deterministic:

- **Startup registration.** Each class/library/service registers with its cluster at host startup
  time, so the runtime can discover participants and replica sets without inferring them from
  ad-hoc “touched links” alone.
- **Canonical authorities.** Clusters typically have either:
  - a single lead host (development/testing), or
  - a production load balancer with a stable DNS record (production),
  which is used for registry lookups and URI canonicalization.

## Absolute URI security (whitelist)

When a request targets an absolute URI, the host must apply a restrictive, default-deny policy:

- **Whitelist only.** Outbound absolute URIs are only permitted when the authority is explicitly
  allowed by host configuration (typically derived from the cluster registry / canonical DNS
  authority). Avoid blacklist-style filtering.
- **Path-only preferred.** Publishers should prefer path-only URIs; hosts resolve them through the
  same canonicalization and registry rules, keeping egress predictable and minimizing SSRF risk.

## Install-and-serve within the same transaction

In the rare case that a library/service must serve requests in the same transaction in which it
is installed, the host may serve those requests after the install operation has completed
successfully within that transaction, identical to any other write-then-read flow.

## Retry policy (deferred)

Retry and backoff behavior for cross-host transaction RPC is intentionally deferred to a
productionization sprint; early implementations should prioritize correctness and determinism.

## Canonical URI surface

TinyChain runtimes expose a fixed directory layout; your server must honor the same
namespaces and mirror them on disk (txfs) so clients and validators can reason about
resources without adapter-specific branches:

- `/state` → built-in data structures: `/state/chain` for chain-wrapped collections,
  `/state/collection` for shard-local stores (`Table`, `Tensor`, etc.),
  `/state/scalar` for scalar types (with `/state/scalar/value`, `/state/scalar/tuple`,
  `/state/scalar/map`), plus the forthcoming `/state/media` abstraction for large
  binaries.
- `/class` → class definitions shipped with a library or service.
- `/lib` → stateless standard libraries, published schemas, and WASM payloads.
- `/service` → publisher-owned, stateful services (queues, trainers, executors) that wrap
  collections in chains for synchronization.
- `/host` → runtime health/load stats; `/healthz` → liveness probe for load balancers.

Do **not** introduce alternative top-level directories or bespoke response envelopes.
Everything the kernel serves must live under these paths, and `<data-dir>/<segments>`
must match the URI segments exactly so HTTP, PyO3, and future adapters stay aligned.

## TLS expectations

- The reference HTTP adapter runs on Hyper. Enabling TLS simply wraps the
  listener with `rustls` (or another acceptor) and supplies the desired cert/key
  pair; the IR and kernel layers are unaffected. **Production deployments must
  terminate HTTPS**, though testnets/local dev can allow HTTP for convenience.
- TinyChain authorization is handled by recursive JWTs and capability masks, so
  TLS certificates do **not** have to share a global CA. Operators may use
  public CAs, pinned self-signed certs, or optional mTLS for selected ports.
- When TinyChain-hosted runtimes or control-plane services talk to each other in
  production, require mTLS and validate the client cert in your transport layer
  (Hyper, axum, etc.) before invoking `Kernel::route_request`. For public or
  end-user traffic, server-auth TLS plus capability tokens is sufficient.
- No matter which TLS configuration you choose, once the HTTP request reaches
  `Kernel::route_request` the protocol flow (txn IDs, capability enforcement,
  `/state` semantics) is identical.

## Fastest paths to a bespoke server

1. **Reuse the kernel, swap the transport.** Instantiate the reference kernel via `tc_server::http::build_http_kernel_with_config` (or the config-free builder) and mount it behind your own HTTP/gRPC/serverless adapter. Your adapter should:
   - Parse the same transaction cues as the v1 HTTP protocol (`?txn_id=...`, and root-only empty POST/DELETE for finalize) and attach them to the request extensions.
   - Translate HTTP-style verbs/paths into the `Route` + `State` types expected by `Kernel::route_request`.
   - Map `TCError` categories back to protocol status codes consistent with docs.tinychain.net.
2. **Use `tc-server` as a minimal kernel crate.** When you want the TinyChain router primitives without the built-in HTTP/PyO3/WASM adapters, depend on `tc-server` with `default-features = false`. This exposes `Dir`, `Route`, `Handler`, `Transaction`, `Kernel`, and the `TxnManager`/`TxnHandle` pair while skipping optional transports. Re-enable adapters explicitly via `features = ["http-server"]`, `features = ["pyo3"]`, or `features = ["wasm"]` as needed for your embedding.
   - Use `features = ["http-client"]` when you need outbound HTTP proxying without compiling the HTTP server adapter.
   - Use `features = ["http-server"]` when you need the inbound HTTP server adapter.
   - Treat `TxnHandle` as the transaction context; when the kernel binds it to a request it carries the RPC resolver, so `Scalar::Ref` can be resolved without threading a `Kernel` reference through handlers.

   **Minimal feature set (for bespoke, non-published hosts):**
   - `default-features = false` to drop adapter and Wasmtime dependencies.
   - Keep `Kernel`, `TxnManager`, `Dir`, `Route`, `Handler`, and `Transaction` in scope so you can register your own Rust handlers (including hardware-accelerated code paths) behind TinyChain’s routing and auth surface.
   - Enable only the adapters you need (e.g., `features = ["http-server"]` for a private HTTP ingress) while keeping the rest of the kernel lean.
   - Rely on the shared transaction/auth contract so Python/WASM clients (or custom clients) can call your service without special casing.
3. **Embed `tc-ir` directly.** For the smallest possible footprint, link the `tc-ir` crate and register handlers with your own router. Follow `tc-ir/IR_INTERFACE_GUIDELINES.md` for interaction patterns and keep the transaction/auth surface identical to v1. This still lets publishers use the Python or WASM clients unchanged because the IR layer mirrors the documented protocol.
4. **Leverage manifests and examples.** The bundled WASM library schema (`tc-server/examples/library_schema_example.json`) plus the `tc-wasm` example module show how routes map to exports. Use them as a reference when validating your server’s `/lib` installer or manifest ingestion.

## Documentation breadcrumbs

- **Protocol reference:** docs.tinychain.net → HTTP resources, transaction semantics, collection routes, and client examples. Treat this as the contract your server must satisfy.
- **IR guidance:** `tc-ir/IR_INTERFACE_GUIDELINES.md` → handler expectations, transaction context, error/backpressure signaling.
- **Server lifecycle:** `tc-server/ROADMAP.md` → current transaction flow and roadmap for `/lib` installs and WASM routing.

If your adapter or bespoke host diverges from these contracts, note the deviation and link the rationale from `ARCHITECTURE.md` or the relevant README so downstream clients can understand compatibility limits.
