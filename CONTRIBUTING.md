# Contributing to `tc-server`

`tc-server` is the TinyChain host runtime: it wires adapters (HTTP, PyO3,
WebSocket, future bindings) into the shared kernel, enforces the transaction
protocol, and persists installed libraries. Changes here impact every TinyChain
deployment, so keep the crate thin and transport-agnostic.

## How this crate fits into TinyChain

- Owns kernel construction, transaction lifecycle management, and the canonical
  routing for `/state`, `/class`, `/lib`, `/service`, `/host`, and `/healthz`.
- Hydrates on-disk library storage (`<data-dir>/lib/...`) so installed WASM
  artifacts become available to every adapter automatically.
- Provides reference binaries/tests that client runtimes and CI environments use
  to validate compatibility.

## Contribution workflow

1. Align proposals with `AGENTS.md`—a single kernel, no adapter-only
   feature forks, and strict adherence to the 3-second sync budget.
2. Keep formatting and linting clean: run `cargo fmt` and
   `cargo clippy --all-targets --all-features -D warnings` before sending
   changes.
3. Keep feature flags limited to adapter compilation; core logic must remain
   shared so HTTP, PyO3, and WASM loaders observe identical behavior.
4. Run `cargo test -p tc-server` plus any relevant client integration tests
   (e.g., the TinyChain Python client backend tests) before opening a PR.
5. Document observable behavior changes in `PROTOCOL_COMPATIBILITY.md` or
   `ROADMAP.md` when they affect external clients or rollout expectations.

## Local development with unpublished sibling crates

While the TinyChain crates are still being iterated as a set, this repository may
depend on sibling crates via relative `path = "../..."` dependencies in
`Cargo.toml`. To develop locally:

1. Check out the required sibling repositories next to this repo so the relative
   paths resolve (e.g., `../tc-ir`, `../tc-state`, `../tc-value`).
2. If your checkout layout differs, either:
   - adjust the `path = ...` entries in `Cargo.toml`, or
   - create a small Cargo workspace which includes these crates at your chosen
     paths.

### Shared CPython helper for PyO3 work

Building or testing `tc-server` with `--features pyo3` requires a shared
`libpython`. End users of the published Python client receive prebuilt wheels
and **do not** need this step. If your distro Python/venv already ships
`libpython3.x.so`, keep using it; otherwise run `./scripts/install_python_dev.sh`
to compile CPython (defaults to 3.12.4) with `--enable-shared` into
`<repo>/python312-shared`.

Set `TC_PY_VERSION` to pin another Python release or `TC_PYO3_PY_PREFIX` to
change the installation directory. After the script completes, export:

```bash
export PYO3_PYTHON="$PWD/python312-shared/bin/python3.12"
export LD_LIBRARY_PATH="$PWD/python312-shared/lib:$LD_LIBRARY_PATH"
```

Use this helper only when developing locally without a system-provided
libpython; CI and released wheels will continue to bundle prebuilt extensions.

## Rights and licensing

By contributing to this crate you represent that (a) the work is authored by
you (or you have the necessary rights to contribute it), (b) the contribution is
unencumbered by third-party intellectual property claims, and (c) you transfer
and assign all right, title, and interest in the contribution to The TinyChain
Contributors for distribution under the Apache 2.0 license (see `LICENSE`). No
other restrictions or encumbrances may attach to your contribution.
