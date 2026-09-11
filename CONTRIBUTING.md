# Contributing to tc-server

Read this repository's [ownership rules](AGENTS.md). The parent workspace
[contributor guide](https://github.com/TinyChain-Inc/tcv2/blob/main/CONTRIBUTING.md)
is non-normative integration context for contributors working in a superproject
checkout.

Before opening a pull request, run:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Also run supported feature-power checks and the repository architecture,
documentation, and relevant cross-adapter/two-host acceptance tests.

Changes to public routing, application storage, or transaction behavior update
the corresponding canonical workspace contract, not a duplicate server-local
protocol description. PyO3 changes belong in `client/rust`.
