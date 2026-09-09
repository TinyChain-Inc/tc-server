# tc-server roadmap

This file lists unimplemented host work. Current behavior is documented in the
workspace architecture and normative storage and transaction contracts.

## Application acceptance

- Complete real two-host HTTP acceptance for JSON/WASM Libraries, independently
  installed Classes, Service persistence/deletion, duplicate work, lost
  acknowledgements, restart, and participant non-forwarding.
- Complete native, HTTP, WASM, PyO3, and Python parity evidence for the same
  recursive application path.
- Close the recursive-application issues only after committed CI evidence is
  linked from their live checklists.

## Service hosting

- Add executable Service behavior as the public owner of named persistent state.
- Delegate durable member state to the common Chain contract; do not restore a
  server collection registry or add a Service-specific transaction lifecycle.
- Keep unsupported Service execution explicit until the complete owner exists.

## Chain integration

- Integrate Chain-owned durable history, replay, canonical-state selection, and
  resynchronization through the boundary in `tc-chain/CHAIN_CONTRACT.md`.
- Preserve Cluster-owned exact-resource leadership and TxnServer-owned expiry
  and frontier scheduling. Do not add a server WAL, repair heuristic, or decision
  ledger while Chain is incomplete.

## Operational acceptance

- Exercise cutoff failure/readiness recovery and ambiguous committed storage.
- Verify bounded admission, cancellation, absolute deadlines, and feature
  powersets in release CI.
- Keep architecture and documentation checks aligned with each completed
  ownership boundary.
