# Install, mutate, and reopen a Service

[service.json](service.json) is a canonical definition shared with the populated
two-host integration test. It declares a scalar `label`, a SyncChain-backed BTree
`data`, a GET method `count`, a PUT method `insert`, and a POST method `append`.
The Python Service port can use this definition as an encoding fixture; Python
authoring is tracked separately in
[client #36](https://github.com/TinyChain-Inc/client/issues/36).

Run these commands from the `tc-server` checkout. They require Cargo and curl.
First generate a local example identity and token:

```bash
cargo run --example rjwt_install_token -- \
  --host http://127.0.0.1:8702 --actor example-admin \
  --lib /service/example-devco/btree/1.0.0
```

Despite the existing option name `--lib`, its value is the authorized resource
link. Save `secret_key_b64` and `bearer_token` from the output in shell variables
`EXAMPLE_SECRET` and `EXAMPLE_TOKEN`. Use only disposable credentials for this
example. Start the host in a separate terminal, with the same secret:

```bash
export TC_DATA_DIR="$PWD/service-example/data"
export TC_WORKSPACE="$PWD/service-example/workspace"
export RUST_MIN_STACK=33554432
cargo run --example http_rpc_native_host -- \
  --bind=127.0.0.1:8702 --actor-id=example-admin \
  --secret-key-b64="$EXAMPLE_SECRET"
```

The debug build requires the same 32 MiB worker stack bound used by the native
Service integration tests. The default worker stack overflows on this example's
method call. This setting is a demonstrated debug-runtime requirement, not a
performance measurement or a claim about release-build stack usage.

Install the definition, inspect its scalar, and invoke both mutation methods:

```bash
curl --fail-with-body -X PUT http://127.0.0.1:8702/service \
  -H "Authorization: Bearer $EXAMPLE_TOKEN" \
  -H 'Content-Type: application/json' --data-binary @examples/service.json

curl --fail-with-body http://127.0.0.1:8702/service/example-devco/btree/1.0.0/label
# "native"

curl --fail-with-body -X PUT http://127.0.0.1:8702/service/example-devco/btree/1.0.0/insert \
  -H "Authorization: Bearer $EXAMPLE_TOKEN" \
  -H 'Content-Type: application/json' --data '[null,[1]]'

curl --fail-with-body -X POST http://127.0.0.1:8702/service/example-devco/btree/1.0.0/append \
  -H "Authorization: Bearer $EXAMPLE_TOKEN" \
  -H 'Content-Type: application/json' --data '{"key":null,"value":[2]}'

curl --fail-with-body http://127.0.0.1:8702/service/example-devco/btree/1.0.0/count
# 2
```

Let each curl finish consuming its response before continuing: successful response
completion participates in commit. Stop the host with Ctrl-C, then restart it with
the same command, directories, identity, and secret. Repeat the final GET; the
count remains `2`. Do not delete the workspace: it also contains host control
records. For further writes after token expiry, generate another token using the
same secret via `--secret-key-b64`.

The automated counterpart also covers populated replica joining, both BTree and
Table members, subsequent replicated writes, and restart after transaction-only
workspace removal:

```bash
cargo test --all-features --test two_host \
  populated_service_join_replicates_methods_and_recovers_both_hosts
```

This demonstrates healthy restart and local WAL recovery, not physical power-loss
testing or repair of interrupted canonical materialization. The
[Service contract](../SERVICE_CONTRACT.md) defines that failure boundary.
