#![cfg(all(feature = "http-client", feature = "http-server"))]

use std::net::{SocketAddr, TcpListener};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hyper::{Body, Client, Request, StatusCode};
use pathlink::Link;
use tinychain::auth::{Actor, KeyringActorResolver, RjwtTokenVerifier, Token, wire_claim};
use tinychain::http::HttpServer;
use tinychain::replication::ReplicationIssuer;
use tinychain::{Claim, HostLimits, HostStorage, HttpGateway, ProtocolAuthority, Workspace};

#[path = "support/service.rs"]
mod service_fixture;
#[path = "support/runtime.rs"]
mod test_runtime;

#[test]
fn populated_service_join_replicates_methods_and_recovers_both_hosts() {
    let (seed_root, joining_root) = test_runtime::run(|| async {
        use futures::TryStreamExt;

        let seed_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let joining_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let joining_addr = joining_listener.local_addr().unwrap();
        let seed = prepare("service-seed").await;
        let joining = prepare("service-join").await;
        let installer = Actor::new("service-installer".into(), rjwt::AlgKind::Falcon512).unwrap();
        let host: Link = "/host".parse().unwrap();
        let keyring = actor_directory(
            &host,
            [seed.actor.clone(), joining.actor.clone(), installer.clone()],
        );
        let (seed_task, seed_shutdown, seed_root, _) =
            start(seed, seed_listener, keyring.clone()).await;
        for table in [false, true] {
            let kind = if table { "table" } else { "btree" };
            let identity: Link = format!("/service/example-devco/{kind}/1.0.0")
                .parse()
                .unwrap();
            let bearer = bearer_for(
                &installer,
                host.clone(),
                Claim::new(identity.clone(), umask::USER_WRITE),
            );
            let definition = std::collections::BTreeMap::from([(
                identity.to_string(),
                service_fixture::definition(table),
            )]);
            let encoded = destream_json::encode(definition)
                .unwrap()
                .try_fold(Vec::new(), |mut bytes, chunk| async move {
                    bytes.extend_from_slice(&chunk);
                    Ok(bytes)
                })
                .await
                .unwrap();
            if !table {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&encoded).unwrap(),
                    serde_json::from_str::<serde_json::Value>(include_str!(
                        "../examples/service.json"
                    ))
                    .unwrap()
                );
            }

            let response = put(seed_addr, "service", &bearer, encoded).await;
            assert_success(response).await;
            let key = if table { "[1]" } else { "null" };
            let response = put(
                seed_addr,
                format!("{identity}/insert").trim_start_matches('/'),
                &bearer,
                format!("[{key},[1]]"),
            )
            .await;
            assert_success(response).await;
        }

        let tensor_identity: Link = "/service/example-devco/tensor/1.0.0".parse().unwrap();
        let tensor_bearer = bearer_for(
            &installer,
            host.clone(),
            Claim::new(tensor_identity.clone(), umask::USER_WRITE),
        );

        let definition = std::collections::BTreeMap::from([(
            tensor_identity.to_string(),
            service_fixture::tensor_definition(),
        )]);

        let encoded = destream_json::encode(definition)
            .unwrap()
            .try_fold(Vec::new(), |mut bytes, chunk| async move {
                bytes.extend_from_slice(&chunk);
                Ok(bytes)
            })
            .await
            .unwrap();

        assert_success(put(seed_addr, "service", &tensor_bearer, encoded).await).await;
        for member in ["dense", "sparse"] {
            assert_success(
                put(
                    seed_addr,
                    format!("{tensor_identity}/write_{member}").trim_start_matches('/'),
                    &tensor_bearer,
                    "[[0],7]",
                )
                .await,
            )
            .await;
        }

        let (joining_task, joining_shutdown, joining_root, joining_kernel) =
            start(joining, joining_listener, keyring).await;
        joining_kernel
            .bootstrap_seed(
                &format!("http://{seed_addr}"),
                format!("http://{joining_addr}"),
            )
            .await
            .expect("populated Service join");
        assert!(joining_kernel.is_ready());
        for table in [false, true] {
            let kind = if table { "table" } else { "btree" };
            let identity: Link = format!("/service/example-devco/{kind}/1.0.0")
                .parse()
                .unwrap();
            assert_replica(seed_addr, &identity.to_string(), joining_addr).await;
            assert_service_count(joining_addr, &identity, 1).await;
            let bearer = bearer_for(
                &installer,
                host.clone(),
                Claim::new(identity.clone(), umask::USER_WRITE),
            );
            let key = if table { "[2]" } else { "null" };
            let response = put(
                joining_addr,
                format!("{identity}/insert").trim_start_matches('/'),
                &bearer,
                format!("[{key},[2]]"),
            )
            .await;
            assert_success(response).await;
            let key = if table { "[3]" } else { "null" };
            let response = Client::new()
                .request(
                    Request::builder()
                        .method("POST")
                        .uri(format!("http://{seed_addr}{identity}/append"))
                        .header(hyper::header::AUTHORIZATION, format!("Bearer {bearer}"))
                        .header(hyper::header::CONTENT_TYPE, "application/json")
                        .body(Body::from(format!(r#"{{"key":{key},"value":[3]}}"#)))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_success(response).await;
            for addr in [seed_addr, joining_addr] {
                assert_service_count(addr, &identity, 3).await;
            }
        }

        exercise_remote_post(seed_addr, joining_addr, &installer, &host).await;
        assert_replica(seed_addr, &tensor_identity.to_string(), joining_addr).await;
        for member in ["dense", "sparse"] {
            assert_tensor_value(joining_addr, member, 0, 7).await;
            assert_success(
                put(
                    joining_addr,
                    format!("{tensor_identity}/write_{member}").trim_start_matches('/'),
                    &tensor_bearer,
                    "[[4096],8]",
                )
                .await,
            )
            .await;

            assert_success(
                put(
                    joining_addr,
                    format!("{tensor_identity}/assign_{member}").trim_start_matches('/'),
                    &tensor_bearer,
                    "[null,null]",
                )
                .await,
            )
            .await;

            for address in [seed_addr, joining_addr] {
                assert_tensor_value(address, member, 0, 0).await;
                assert_tensor_value(address, member, 4096, 0).await;
            }

            // Selected writes still address the Chain-owned member, including
            // ranges spanning native blocks. Both replicas must replay the selector.
            assert_success(
                put(
                    joining_addr,
                    format!("{tensor_identity}/write_{member}").trim_start_matches('/'),
                    &tensor_bearer,
                    "[[[4095,4097,1]],9]",
                )
                .await,
            )
            .await;

            for address in [seed_addr, joining_addr] {
                assert_tensor_value(address, member, 4094, 0).await;
                assert_tensor_value(address, member, 4095, 9).await;
                assert_tensor_value(address, member, 4096, 9).await;
            }

            for (index, value) in [(0, 7), (4096, 8)] {
                assert_success(
                    put(
                        joining_addr,
                        format!("{tensor_identity}/write_{member}").trim_start_matches('/'),
                        &tensor_bearer,
                        format!("[[{index}],{value}]"),
                    )
                    .await,
                )
                .await;
            }
        }

        assert_success(
            put(
                seed_addr,
                format!("{tensor_identity}/write_sparse").trim_start_matches('/'),
                &tensor_bearer,
                "[[0],0]",
            )
            .await,
        )
        .await;

        for addr in [seed_addr, joining_addr] {
            assert_tensor_value(addr, "dense", 0, 7).await;
            assert_tensor_value(addr, "sparse", 0, 0).await;
            for member in ["dense", "sparse"] {
                assert_tensor_value(addr, member, 4095, 9).await;
                assert_tensor_value(addr, member, 4096, 8).await;
            }
        }

        let _ = seed_shutdown.send(());
        let _ = joining_shutdown.send(());
        seed_task.await.unwrap();
        joining_task.await.unwrap();
        (seed_root, joining_root)
    });

    // Stop expiry with the old runtimes before reopening either storage owner.
    for (label, root) in [("service-seed", seed_root), ("service-join", joining_root)] {
        let root = test_runtime::run(move || async move {
            std::fs::remove_dir_all(root.join("workspace/txn")).unwrap();
            let host = open(label, root.clone()).await;
            let keyring = actor_directory(&"/host".parse().unwrap(), [host.actor.clone()]);
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let (task, shutdown, _, kernel) = start(host, listener, keyring).await;
            assert!(kernel.is_ready());
            for kind in ["btree", "table"] {
                assert_service_count(
                    addr,
                    &format!("/service/example-devco/{kind}/1.0.0")
                        .parse()
                        .unwrap(),
                    if kind == "btree" { 4 } else { 3 },
                )
                .await;
            }

            assert_tensor_value(addr, "dense", 0, 7).await;
            assert_tensor_value(addr, "sparse", 0, 0).await;
            for member in ["dense", "sparse"] {
                assert_tensor_value(addr, member, 4095, 9).await;
                assert_tensor_value(addr, member, 4096, 8).await;
            }

            let _ = shutdown.send(());
            task.await.unwrap();
            root
        });

        std::fs::remove_dir_all(root).unwrap();
    }
}

async fn exercise_remote_post(
    seed: SocketAddr,
    joining: SocketAddr,
    installer: &Actor,
    host: &Link,
) {
    use futures::TryStreamExt;
    use pathlink::{PathLabel, label, path_label};

    use tc_ir::{After, Map, OpDef, OpRef, Scalar, Subject, TCRef};
    use tc_value::Value;

    const BTREE: PathLabel = path_label(&["service", "example-devco", "btree", "1.0.0"]);
    const ORCHESTRATOR: PathLabel =
        path_label(&["service", "example-devco", "orchestrator", "1.0.0"]);

    let endpoint: Link = format!("http://{joining}").parse().unwrap();
    let remote = Link::new(endpoint.into_host().unwrap(), BTREE.into());
    let append = |value: u64| {
        Scalar::from(TCRef::Op(OpRef::Post((
            Subject::Link(remote.clone().append(label("append"))),
            Map::from_iter([
                ("key".parse().unwrap(), Value::None.into()),
                (
                    "value".parse().unwrap(),
                    Value::Tuple(vec![value.into()]).into(),
                ),
            ]),
        ))))
    };

    let fail = TCRef::Op(OpRef::Put((
        Subject::Link(remote.clone().append(label("insert"))),
        Value::from(1_u64).into(),
        Value::Tuple(vec![5_u64.into()]).into(),
    )));

    let definition = Scalar::Map(Map::from_iter([
        (
            "append_remote".parse().unwrap(),
            Scalar::Op(OpDef::Post(vec![("result".parse().unwrap(), append(4))])),
        ),
        (
            "fail_remote".parse().unwrap(),
            Scalar::Op(OpDef::Post(vec![(
                "result".parse().unwrap(),
                TCRef::After(Box::new(After::new(append(5), fail.into()))).into(),
            )])),
        ),
        (
            "read_remote".parse().unwrap(),
            Scalar::Op(OpDef::Get((
                "key".parse().unwrap(),
                vec![("result".parse().unwrap(), append(6))],
            ))),
        ),
    ]));

    let identity = Link::from(ORCHESTRATOR);
    let bearer = bearer_for(
        installer,
        host.clone(),
        Claim::new(identity.clone(), umask::USER_WRITE),
    );

    let encoded = destream_json::encode(std::collections::BTreeMap::from([(
        identity.to_string(),
        definition,
    )]))
    .unwrap()
    .try_fold(Vec::new(), |mut bytes, chunk| async move {
        bytes.extend_from_slice(&chunk);
        Ok(bytes)
    })
    .await
    .unwrap();

    assert_success(put(seed, "service", &bearer, encoded).await).await;

    // This Service owns no mutable member: its POST must commit work delegated
    // over HTTP, and a later execution error must roll back that remote work.
    for (method, member, expected, message) in [
        ("POST", "append_remote", StatusCode::OK, None),
        (
            "POST",
            "fail_remote",
            StatusCode::BAD_REQUEST,
            Some("BTree insert expects a null key"),
        ),
        (
            "GET",
            "read_remote",
            StatusCode::UNAUTHORIZED,
            Some("GET execution cannot mutate"),
        ),
    ] {
        let target = identity.clone().append(label(member));
        let response = Client::new()
            .request(
                Request::builder()
                    .method(method)
                    .uri(format!("http://{seed}{target}"))
                    .header(hyper::header::AUTHORIZATION, format!("Bearer {bearer}"))
                    .header(hyper::header::CONTENT_TYPE, "application/json")
                    .body(if method == "POST" {
                        Body::from("{}")
                    } else {
                        Body::empty()
                    })
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = response.status();
        let body = hyper::body::to_bytes(response).await.unwrap();
        assert_eq!(
            status,
            expected,
            "{member}: {}",
            String::from_utf8_lossy(&body)
        );

        if let Some(message) = message {
            assert!(
                String::from_utf8_lossy(&body).contains(message),
                "{member}: {}",
                String::from_utf8_lossy(&body)
            );
        }

        for address in [seed, joining] {
            tokio::time::timeout(
                Duration::from_secs(3),
                assert_service_count(address, &BTREE.into(), 4),
            )
            .await
            .expect("remote decision must release its transaction reservations");
        }
    }
}

async fn assert_success(response: hyper::Response<Body>) {
    let status = response.status();
    // Consume the response to release its transport and request ownership.
    let body = hyper::body::to_bytes(response).await.unwrap();
    assert!(
        status.is_success(),
        "{status}: {}",
        String::from_utf8_lossy(&body)
    );
}

async fn assert_service_count(address: SocketAddr, identity: &Link, count: usize) {
    let response = Client::new()
        .get(format!("http://{address}{identity}/count").parse().unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = hyper::body::to_bytes(response).await.unwrap();
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!(count)
    );
}

async fn assert_tensor_value(address: SocketAddr, member: &str, index: u64, expected: u64) {
    let response = Client::new()
        .request(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "http://{address}/service/example-devco/tensor/1.0.0/{member}"
                ))
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .header(hyper::header::CONTENT_LENGTH, format!("[{index}]").len())
                .body(Body::from(format!("[{index}]")))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let bytes = hyper::body::to_bytes(response).await.unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
        serde_json::json!(expected)
    );
}

struct PreparedHost {
    root: std::path::PathBuf,
    roots: tinychain::ApplicationRoots,
    workspace: Workspace,
    protocol: ProtocolAuthority,
    actor: Actor,
}

async fn prepare(label: &str) -> PreparedHost {
    let unique = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    open(
        label,
        std::env::temp_dir().join(format!("tc-two-host-{label}-{unique}")),
    )
    .await
}

async fn open(label: &str, root: std::path::PathBuf) -> PreparedHost {
    let storage = HostStorage::new(&HostLimits::default().storage);
    let workspace = storage
        .workspace(root.join("workspace"))
        .expect("workspace");
    let roots = storage
        .application_roots(root.join("data"))
        .await
        .expect("application roots");
    let host = Link::from_str("/host").expect("host link");
    let (protocol, actor) = workspace
        .load_or_create_protocol_authority(&label.parse().expect("host ID"), host)
        .await
        .expect("protocol authority");
    PreparedHost {
        root,
        roots,
        workspace,
        protocol,
        actor,
    }
}

async fn put(
    address: std::net::SocketAddr,
    root: &str,
    bearer: &str,
    body: impl Into<Body>,
) -> hyper::Response<Body> {
    put_with_type(address, root, bearer, "application/json", body).await
}

async fn put_with_type(
    address: std::net::SocketAddr,
    root: &str,
    bearer: &str,
    content_type: &str,
    body: impl Into<Body>,
) -> hyper::Response<Body> {
    Client::new()
        .request(
            Request::builder()
                .method("PUT")
                .uri(format!("http://{address}/{root}"))
                .header(hyper::header::AUTHORIZATION, format!("Bearer {bearer}"))
                .header(hyper::header::CONTENT_TYPE, content_type)
                .body(body.into())
                .expect("install request"),
        )
        .await
        .expect("install response")
}

#[cfg(feature = "wasm")]
fn literal_wasm_module() -> Vec<u8> {
    fn wat_bytes(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("\\{byte:02x}")).collect()
    }

    let entry = br#"{"definition":{"/lib/example-devco/two-host-wasm/1.0.0":{"answer":42}},"routes":[{"path":"/answer","export":"answer"}]}"#;
    let response = b"42";
    let entry_result = (entry.len() as i64) << 32;
    let response_ptr = 2048_i64;
    let response_result = ((response.len() as i64) << 32) | response_ptr;
    let wat = format!(
        r#"(module
            (memory (export "memory") 1)
            (data (i32.const 0) "{}")
            (data (i32.const {response_ptr}) "{}")
            (func (export "alloc") (param i32) (result i32) i32.const 4096)
            (func (export "free") (param i32 i32))
            (func (export "tc_library_entry") (result i64) i64.const {entry_result})
            (func (export "answer") (param i32 i32 i32 i32) (result i64)
                i64.const {response_result}))"#,
        wat_bytes(entry),
        wat_bytes(response),
    );

    wat::parse_str(wat).unwrap()
}

async fn delete(
    address: std::net::SocketAddr,
    identity: &pathlink::Link,
    bearer: &str,
) -> hyper::Response<Body> {
    Client::new()
        .request(
            Request::builder()
                .method("DELETE")
                .uri(format!("http://{address}{identity}"))
                .header(hyper::header::AUTHORIZATION, format!("Bearer {bearer}"))
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(Body::from("null"))
                .expect("delete request"),
        )
        .await
        .expect("delete response")
}

fn bearer_for(actor: &Actor, host: Link, claim: Claim) -> String {
    actor
        .sign_token(
            Token::new(
                host,
                SystemTime::now(),
                Duration::from_secs(30),
                actor.id().clone(),
                wire_claim(claim),
            )
            .expect("valid token time"),
        )
        .expect("sign token")
        .into_jwt()
}

fn actor_directory(host: &Link, actors: impl IntoIterator<Item = Actor>) -> KeyringActorResolver {
    let directory = KeyringActorResolver::default();
    for actor in actors {
        directory
            .insert(host.clone(), actor)
            .expect("unique test actor");
    }
    directory
}

async fn start(
    host: PreparedHost,
    listener: TcpListener,
    keyring: KeyringActorResolver,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
    std::path::PathBuf,
    tinychain::Kernel,
) {
    start_with_psk(host, listener, keyring, [7; 32]).await
}

async fn start_with_psk(
    host: PreparedHost,
    listener: TcpListener,
    keyring: KeyringActorResolver,
    psk: [u8; 32],
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
    std::path::PathBuf,
    tinychain::Kernel,
) {
    let bootstrap = Arc::new(
        ReplicationIssuer::new(
            Arc::new(host.protocol.clone()),
            vec![aes_gcm_siv::Key::<aes_gcm_siv::Aes256GcmSiv>::from(psk)],
            keyring.clone(),
        )
        .expect("replication issuer"),
    );
    let limits = HostLimits::default();
    let gateway = HttpGateway::new();
    let services = tinychain::HostServices {
        application_roots: host.roots,
        replication: Arc::new(gateway.clone()),
        rpc: Arc::new(gateway),
        resources: tinychain::HostResources::new(limits).unwrap(),
        protocol: host.protocol,
        verifier: Arc::new(RjwtTokenVerifier::new(Arc::new(keyring.clone()))),
        actors: keyring,
        bootstrap,
        bootstrap_required: false,
    };

    let kernel = tinychain::Kernel::new(services, host.workspace)
        .await
        .expect("HTTP runtime");

    let server_kernel = kernel.clone();
    let (shutdown, stopping) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _ = HttpServer::new(server_kernel)
            .serve_listener_with_shutdown(listener, async move {
                let _ = stopping.await;
            })
            .await;
    });
    (task, shutdown, host.root, kernel)
}

#[tokio::test]
async fn a_library_commit_uses_the_same_routed_path_on_two_real_http_hosts() {
    let primary_listener = TcpListener::bind("127.0.0.1:0").expect("primary listener");
    let replica_listener = TcpListener::bind("127.0.0.1:0").expect("replica listener");
    let primary_addr = primary_listener.local_addr().expect("primary address");
    let replica_addr = replica_listener.local_addr().expect("replica address");
    let primary = prepare("primary").await;
    let replica = prepare("replica").await;
    let installer =
        Actor::new("two-host-installer".to_string(), rjwt::AlgKind::Falcon512).expect("installer");

    let host = Link::from_str("/host").expect("host link");
    let keyring = actor_directory(
        &host,
        [
            primary.actor.clone(),
            replica.actor.clone(),
            installer.clone(),
        ],
    );

    let (replica_task, replica_shutdown, replica_root, _) =
        start(replica, replica_listener, keyring.clone()).await;
    let (primary_task, primary_shutdown, primary_root, primary_kernel) =
        start(primary, primary_listener, keyring.clone()).await;
    primary_kernel
        .bootstrap_seed(
            &format!("http://{replica_addr}"),
            format!("http://{primary_addr}"),
        )
        .await
        .expect("resource-scoped bootstrap");

    let identity: pathlink::Link = "/lib/example-devco/two-host/1.0.0"
        .parse()
        .expect("identity");
    let bearer = bearer_for(
        &installer,
        host.clone(),
        Claim::new(identity.clone(), umask::USER_WRITE),
    );

    let body = format!(r#"{{"{identity}":{{"ok":true,"λ":7,"literal%name":8,"%2f":9}}}}"#);
    let response = put(primary_addr, "lib", &bearer, body).await;
    let status = response.status();
    let response_body = hyper::body::to_bytes(response.into_body())
        .await
        .expect("response body");
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "{}",
        String::from_utf8_lossy(&response_body)
    );

    let response = Client::new()
        .get(
            format!("http://{replica_addr}{identity}")
                .parse()
                .expect("URI"),
        )
        .await
        .expect("replica response");
    assert_eq!(response.status(), StatusCode::OK);

    for address in [primary_addr, replica_addr] {
        for (name, expected) in [("λ", 7u64), ("literal%name", 8), ("%2f", 9)] {
            let member = identity
                .clone()
                .append(name.parse::<pathlink::Id>().unwrap());

            let response = Client::new()
                .get(format!("http://{address}{member}").parse().unwrap())
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            let value: tc_value::Value = destream_json::try_decode((), response.into_body())
                .await
                .unwrap();

            assert_eq!(value, tc_value::Value::from(expected));
        }
    }

    #[cfg(feature = "wasm")]
    {
        let wasm: pathlink::Link = "/lib/example-devco/two-host-wasm/1.0.0"
            .parse()
            .expect("WASM identity");
        let bearer = bearer_for(
            &installer,
            host.clone(),
            Claim::new(wasm.clone(), umask::USER_WRITE),
        );
        let module = literal_wasm_module();
        let response = put_with_type(
            primary_addr,
            "lib",
            &bearer,
            "application/wasm",
            module.clone(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let response = Client::new()
            .get(
                format!("http://{replica_addr}{wasm}/answer")
                    .parse()
                    .expect("URI"),
            )
            .await
            .expect("replica WASM response");
        assert_eq!(response.status(), StatusCode::OK);
        let response = Client::new()
            .get(format!("http://{replica_addr}{wasm}").parse().expect("URI"))
            .await
            .expect("replica WASM module response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(hyper::header::CONTENT_TYPE),
            Some(&hyper::header::HeaderValue::from_static("application/json"))
        );

        let value: tc_value::Value = destream_json::try_decode((), response.into_body())
            .await
            .expect("WASM module State");

        let tc_value::Value::Bytes(bytes) = value else {
            panic!("expected a byte-valued State");
        };

        assert_eq!(bytes.as_ref(), module.as_slice());
    }

    let service: pathlink::Link = "/service/example-devco/catalog/1.0.0"
        .parse()
        .expect("Service identity");
    let bearer = bearer_for(
        &installer,
        host.clone(),
        Claim::new(service.clone(), umask::USER_WRITE),
    );
    let response = put(
        primary_addr,
        "service",
        &bearer,
        format!(r#"{{"{service}":{{"enabled":true}}}}"#),
    )
    .await;
    let status = response.status();
    let body = hyper::body::to_bytes(response.into_body())
        .await
        .expect("Service install response");
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "{}",
        String::from_utf8_lossy(&body)
    );
    let response = Client::new()
        .get(
            format!("http://{replica_addr}{service}")
                .parse()
                .expect("URI"),
        )
        .await
        .expect("replica Service response");
    assert_eq!(response.status(), StatusCode::OK);

    let bearer = bearer_for(
        &installer,
        host.clone(),
        Claim::new(identity.clone(), umask::USER_WRITE),
    );
    let response = delete(primary_addr, &identity, &bearer).await;
    let status = response.status();
    let error = hyper::body::to_bytes(response.into_body())
        .await
        .expect("delete response body");
    assert!(status.is_success(), "{}", String::from_utf8_lossy(&error));
    let response = Client::new()
        .get(
            format!("http://{replica_addr}{identity}")
                .parse()
                .expect("URI"),
        )
        .await
        .expect("replica deleted Library response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let class: pathlink::Link = "/class/example-devco/vector/1.0.0"
        .parse()
        .expect("Class identity");
    let bearer = bearer_for(
        &installer,
        host.clone(),
        Claim::new(class.clone(), umask::USER_WRITE),
    );
    let response = put(
        primary_addr,
        "class",
        &bearer,
        format!(
            r#"{{"{class}":{body}}}"#,
            body = include_str!("../../tc-state/fixtures/class_definition.json")
        ),
    )
    .await;
    let status = response.status();
    let body = hyper::body::to_bytes(response.into_body())
        .await
        .expect("Class install response");
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "{}",
        String::from_utf8_lossy(&body)
    );
    let response = Client::new()
        .get(
            format!("http://{replica_addr}{class}")
                .parse()
                .expect("URI"),
        )
        .await
        .expect("replica Class response");
    assert_eq!(response.status(), StatusCode::OK);

    // Current txfs intentionally has no crash-recovery journal. Wait for the
    // bounded cutoff to discard committed version files before reopening; a
    // crash with unresolved versions is fail-closed until Chain can reconcile.
    tokio::time::sleep(Duration::from_secs(7)).await;
    let _ = replica_shutdown.send(());
    let _ = replica_task.await;
    let restarted_listener = TcpListener::bind("127.0.0.1:0").expect("restart listener");
    let restarted_addr = restarted_listener.local_addr().expect("restart address");
    let restarted = open("replica", replica_root.clone()).await;
    let (restarted_task, restarted_shutdown, _, _) =
        start(restarted, restarted_listener, keyring).await;
    tokio::task::yield_now().await;
    for target in [&class, &service] {
        let response = Client::new()
            .get(
                format!("http://{restarted_addr}{target}")
                    .parse()
                    .expect("URI"),
            )
            .await
            .expect("restarted replica response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    let _ = primary_shutdown.send(());
    let _ = restarted_shutdown.send(());
    let _ = primary_task.await;
    let _ = restarted_task.await;
    std::fs::remove_dir_all(primary_root).expect("remove primary root");
    std::fs::remove_dir_all(replica_root).expect("remove replica root");
}

#[tokio::test]
async fn partial_replica_delivery_aborts_on_failed_rollback_and_recovers() {
    const CHILD: &str = "TC_FAILED_ROLLBACK_ROOT";
    let root = match std::env::var_os(CHILD) {
        Some(root) => std::path::PathBuf::from(root),
        None => {
            let unique = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos();

            let root = std::env::temp_dir().join(format!("tc-failed-rollback-{unique}"));
            std::fs::create_dir(&root).unwrap();
            #[cfg(unix)]
            let mut child = {
                let mut command = std::process::Command::new("timeout");
                command
                    .args([
                        "45s",
                        "sh",
                        "-c",
                        "ulimit -c 0; exec \"$@\"",
                        "rollback-host",
                    ])
                    .arg(std::env::current_exe().unwrap());

                command
            };

            #[cfg(not(unix))]
            let mut child = std::process::Command::new(std::env::current_exe().unwrap());

            let output = child
                .args([
                    "--exact",
                    "partial_replica_delivery_aborts_on_failed_rollback_and_recovers",
                    "--nocapture",
                ])
                .env(CHILD, &root)
                .output()
                .unwrap();

            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;

                assert_eq!(output.status.signal(), Some(6), "{output:?}");
            }

            #[cfg(not(unix))]
            assert!(!output.status.success(), "{output:?}");

            assert!(
                String::from_utf8_lossy(&output.stderr).contains("transaction recovery required"),
                "{output:?}"
            );

            assert!(
                root.join("primary").join("workspace").exists(),
                "fatal cleanup retains recovery evidence"
            );

            // A failed delivery is never published. Recover with a fresh owner,
            // rather than waiting for cleanup on the terminated host.
            let primary = open("rollback-primary", root.join("primary")).await;
            let keyring =
                actor_directory(&Link::from_str("/host").unwrap(), [primary.actor.clone()]);

            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let (task, shutdown, _, kernel) = start(primary, listener, keyring).await;
            let identity: Link =
                pathlink::path_label(&["lib", "example-devco", "rollback", "1.0.0"]).into();

            let response = Client::new()
                .get(format!("http://{address}{identity}").parse().unwrap())
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            drop(response);
            shutdown.send(()).unwrap();
            task.await.unwrap();
            drop(kernel);
            std::fs::remove_dir_all(root).unwrap();
            return;
        }
    };

    let primary_listener = TcpListener::bind("127.0.0.1:0").expect("primary listener");
    let replica_listener = TcpListener::bind("127.0.0.1:0").expect("replica listener");
    let primary_addr = primary_listener.local_addr().expect("primary address");
    let replica_addr = replica_listener.local_addr().expect("replica address");
    let primary = open("rollback-primary", root.join("primary")).await;
    let replica = open("rollback-replica", root.join("replica")).await;
    let installer =
        Actor::new("rollback-installer".to_string(), rjwt::AlgKind::Falcon512).expect("installer");

    let host = Link::from_str("/host").expect("host link");
    let keyring = actor_directory(
        &host,
        [
            primary.actor.clone(),
            replica.actor.clone(),
            installer.clone(),
        ],
    );

    let (replica_task, replica_shutdown, _replica_root, _) =
        start(replica, replica_listener, keyring.clone()).await;

    let (_primary_task, _primary_shutdown, _primary_root, primary_kernel) =
        start(primary, primary_listener, keyring).await;

    primary_kernel
        .bootstrap_seed(
            &format!("http://{replica_addr}"),
            format!("http://{primary_addr}"),
        )
        .await
        .expect("resource-scoped bootstrap");
    let membership = Client::new()
        .get(
            format!("http://{primary_addr}/lib/replicas")
                .parse()
                .expect("replica membership URI"),
        )
        .await
        .expect("replica membership response");
    let membership = hyper::body::to_bytes(membership.into_body())
        .await
        .expect("replica membership body");
    assert!(
        String::from_utf8_lossy(&membership).contains(&replica_addr.to_string()),
        "bootstrap must enroll the seed as the Library root replica"
    );
    let _ = replica_shutdown.send(());
    let _ = replica_task.await;
    assert!(
        Client::new()
            .get(
                format!("http://{replica_addr}/healthz")
                    .parse()
                    .expect("health URI"),
            )
            .await
            .is_err(),
        "aborted replica must stop accepting requests"
    );
    let identity: pathlink::Link = "/lib/example-devco/rollback/1.0.0"
        .parse()
        .expect("identity");
    let bearer = bearer_for(
        &installer,
        host.clone(),
        Claim::new(identity.clone(), umask::USER_WRITE),
    );

    let _ = put(
        primary_addr,
        "lib",
        &bearer,
        format!(r#"{{"{identity}":{{"ok":true}}}}"#),
    )
    .await;

    panic!("failed rollback must terminate the host before returning a response");
}

#[tokio::test]
async fn a_failed_replica_is_evicted_only_with_a_surviving_strict_majority() {
    let primary_listener = TcpListener::bind("127.0.0.1:0").expect("primary listener");
    let replica_a_listener = TcpListener::bind("127.0.0.1:0").expect("replica A listener");
    let replica_b_listener = TcpListener::bind("127.0.0.1:0").expect("replica B listener");
    let primary_addr = primary_listener.local_addr().expect("primary address");
    let replica_a_addr = replica_a_listener.local_addr().expect("replica A address");
    let replica_b_addr = replica_b_listener.local_addr().expect("replica B address");
    let primary = prepare("majority-primary").await;
    let replica_a = prepare("majority-replica-a").await;
    let replica_b = prepare("majority-replica-b").await;
    let installer =
        Actor::new("majority-installer".to_string(), rjwt::AlgKind::Falcon512).expect("installer");

    let host = Link::from_str("/host").expect("host link");
    // Independent verifiers must learn peer keys through authenticated protocol
    // work; sharing their cache would hide missing bootstrap enrollment.
    let primary_keys = actor_directory(&host, [primary.actor.clone(), installer.clone()]);
    let replica_a_keys = actor_directory(&host, [replica_a.actor.clone(), installer.clone()]);
    let replica_b_keys = actor_directory(&host, [replica_b.actor.clone(), installer.clone()]);

    let (replica_a_task, replica_a_shutdown, replica_a_root, _) =
        start(replica_a, replica_a_listener, replica_a_keys).await;

    let (replica_b_task, replica_b_shutdown, replica_b_root, replica_b_kernel) =
        start(replica_b, replica_b_listener, replica_b_keys).await;

    let (primary_task, primary_shutdown, primary_root, primary_kernel) =
        start(primary, primary_listener, primary_keys).await;

    primary_kernel
        .bootstrap_seed(
            &format!("http://{replica_a_addr}"),
            format!("http://{primary_addr}"),
        )
        .await
        .expect("first resource-scoped bootstrap");

    // Joining an existing group must introduce B's owner key to primary before
    // A propagates B's transaction decision there.
    replica_b_kernel
        .bootstrap_seed(
            &format!("http://{replica_a_addr}"),
            format!("http://{replica_b_addr}"),
        )
        .await
        .expect("bootstrap into an existing replica group");

    primary_kernel
        .bootstrap_seed(
            &format!("http://{replica_b_addr}"),
            format!("http://{primary_addr}"),
        )
        .await
        .expect("second resource-scoped bootstrap");

    drop(replica_b_kernel);

    let all_live: pathlink::Link = "/lib/example-devco/all-live/1.0.0"
        .parse()
        .expect("all-live identity");
    let bearer = bearer_for(
        &installer,
        host.clone(),
        Claim::new(all_live.clone(), umask::USER_WRITE),
    );
    let response = put(
        primary_addr,
        "lib",
        &bearer,
        format!(r#"{{"{all_live}":{{"ok":true}}}}"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    for replica in [replica_a_addr, replica_b_addr] {
        let response = Client::new()
            .get(
                format!("http://{replica}{all_live}")
                    .parse()
                    .expect("all-live URI"),
            )
            .await
            .expect("all-live replica response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    // Either existing peer must be able to lead after membership convergence.
    let from_a = pathlink::Link::from(
        all_live.path()[..all_live.path().len() - 2]
            .iter()
            .cloned()
            .chain([
                "from-a".parse::<pathlink::PathSegment>().unwrap(),
                "1.0.0".parse().unwrap(),
            ])
            .collect::<pathlink::PathBuf>(),
    );

    let bearer = bearer_for(
        &installer,
        host.clone(),
        Claim::new(from_a.clone(), umask::USER_WRITE),
    );

    assert_success(
        put(
            replica_a_addr,
            "lib",
            &bearer,
            format!(r#"{{"{from_a}":{{"ok":true}}}}"#),
        )
        .await,
    )
    .await;

    for replica in [primary_addr, replica_b_addr] {
        let response = Client::new()
            .get(format!("http://{replica}{from_a}").parse().unwrap())
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "peer-led write reaches every replica"
        );
    }

    let _ = replica_b_shutdown.send(());
    let _ = replica_b_task.await;
    let identity: pathlink::Link = "/lib/example-devco/majority/1.0.0"
        .parse()
        .expect("identity");
    let bearer = bearer_for(
        &installer,
        host.clone(),
        Claim::new(identity.clone(), umask::USER_WRITE),
    );
    let response = put(
        primary_addr,
        "lib",
        &bearer,
        format!(r#"{{"{identity}":{{"ok":true}}}}"#),
    )
    .await;
    let status = response.status();
    let body = hyper::body::to_bytes(response.into_body())
        .await
        .expect("majority write response");
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "{}",
        String::from_utf8_lossy(&body)
    );

    let response = Client::new()
        .get(
            format!("http://{replica_a_addr}{identity}")
                .parse()
                .expect("replica URI"),
        )
        .await
        .expect("surviving replica response");
    assert_eq!(response.status(), StatusCode::OK);
    let membership = Client::new()
        .get(
            format!("http://{primary_addr}/lib/replicas")
                .parse()
                .expect("replica membership URI"),
        )
        .await
        .expect("replica membership response");
    let membership = hyper::body::to_bytes(membership.into_body())
        .await
        .expect("replica membership body");
    assert!(!String::from_utf8_lossy(&membership).contains(&replica_b_addr.to_string()));

    let _ = primary_shutdown.send(());
    let _ = replica_a_shutdown.send(());
    let _ = primary_task.await;
    let _ = replica_a_task.await;
    std::fs::remove_dir_all(primary_root).expect("remove primary root");
    std::fs::remove_dir_all(replica_a_root).expect("remove replica A root");
    std::fs::remove_dir_all(replica_b_root).expect("remove replica B root");
}

#[tokio::test]
async fn bootstrap_copies_committed_state_before_admitting_membership() {
    let seed_listener = TcpListener::bind("127.0.0.1:0").expect("seed listener");
    let joining_listener = TcpListener::bind("127.0.0.1:0").expect("joining listener");
    let seed_addr = seed_listener.local_addr().expect("seed address");
    let joining_addr = joining_listener.local_addr().expect("joining address");
    let seed = prepare("populated-seed").await;
    let joining = prepare("joining-host").await;
    let installer =
        Actor::new("bootstrap-installer".to_string(), rjwt::AlgKind::Falcon512).expect("installer");

    let host = Link::from_str("/host").expect("host link");
    let keyring = actor_directory(
        &host,
        [seed.actor.clone(), joining.actor.clone(), installer.clone()],
    );
    let (seed_task, seed_shutdown, seed_root, _) =
        start(seed, seed_listener, keyring.clone()).await;
    let identity: pathlink::Link = "/lib/example-devco/nested/bootstrap/1.0.0"
        .parse()
        .expect("identity");
    let bearer = bearer_for(
        &installer,
        host.clone(),
        Claim::new(identity.clone(), umask::USER_WRITE),
    );
    let response = put(
        seed_addr,
        "lib",
        &bearer,
        format!(r#"{{"{identity}":{{"answer":42}}}}"#),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let (joining_task, joining_shutdown, joining_root, joining_kernel) =
        start(joining, joining_listener, keyring.clone()).await;

    joining_kernel
        .bootstrap_seed(
            &format!("http://{seed_addr}"),
            format!("http://{joining_addr}"),
        )
        .await
        .expect("bootstrap populated resource tree");

    assert!(joining_kernel.is_ready());
    let stale_endpoint = {
        let listener = TcpListener::bind("127.0.0.1:0").expect("stale seed listener");
        format!("http://{}", listener.local_addr().unwrap())
    };

    assert!(
        joining_kernel
            .bootstrap_seed(&stale_endpoint, format!("http://{joining_addr}"))
            .await
            .is_err()
    );

    assert!(
        joining_kernel.is_ready(),
        "a stale seed cannot undo bootstrap"
    );

    for resource in &[
        "/lib".to_string(),
        "/lib/example-devco".to_string(),
        "/lib/example-devco/nested".to_string(),
        "/lib/example-devco/nested/bootstrap".to_string(),
        identity.to_string(),
    ] {
        assert_replica(seed_addr, resource, joining_addr).await;
        assert_replica(joining_addr, resource, seed_addr).await;
    }
    let response = Client::new()
        .get(
            format!("http://{joining_addr}{identity}")
                .parse()
                .expect("joining host URI"),
        )
        .await
        .expect("joining host response");
    assert_eq!(response.status(), StatusCode::OK);

    // Reopening resets native replica maps. Restore source-listed descendants
    // before namespace validation; their complete persisted values stay intact.
    joining_shutdown.send(()).expect("stop joining host");
    joining_task.await.expect("drain joining host");
    drop(joining_kernel);
    let joining = open("joining-host", joining_root).await;
    let joining_listener = TcpListener::bind(joining_addr).expect("reopened joining listener");
    let (joining_task, joining_shutdown, joining_root, joining_kernel) =
        start(joining, joining_listener, keyring).await;

    joining_kernel
        .bootstrap_seed(
            &format!("http://{seed_addr}"),
            format!("http://{joining_addr}"),
        )
        .await
        .expect("bootstrap reopened populated resource tree");

    assert_replica(joining_addr, &identity.to_string(), seed_addr).await;
    let response = Client::new()
        .get(format!("http://{joining_addr}{identity}").parse().unwrap())
        .await
        .expect("reopened joining host response");

    assert_eq!(response.status(), StatusCode::OK);

    // Restored replica metadata covers the namespace. Its stable root owns
    // whole-application deletion and propagates the change back to the seed.
    let bearer = bearer_for(
        &installer,
        host,
        Claim::new(identity.clone(), umask::USER_WRITE),
    );
    let response = delete(joining_addr, &identity, &bearer).await;
    assert!(response.status().is_success());
    let response = Client::new()
        .get(
            format!("http://{seed_addr}{identity}")
                .parse()
                .expect("seed URI"),
        )
        .await
        .expect("seed response after joined deletion");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let _ = seed_shutdown.send(());
    let _ = joining_shutdown.send(());
    let _ = seed_task.await;
    let _ = joining_task.await;
    std::fs::remove_dir_all(seed_root).expect("remove seed root");
    std::fs::remove_dir_all(joining_root).expect("remove joining root");
}

async fn assert_replica(host: SocketAddr, resource: &str, replica: SocketAddr) {
    let response = Client::new()
        .get(
            format!("http://{host}{resource}/replicas")
                .parse()
                .expect("replica URI"),
        )
        .await
        .expect("replica membership response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = hyper::body::to_bytes(response.into_body())
        .await
        .expect("replica membership body");
    assert!(
        String::from_utf8_lossy(&body).contains(&replica.to_string()),
        "{resource} does not contain replica {replica}: {}",
        String::from_utf8_lossy(&body)
    );
}

#[tokio::test]
async fn bootstrap_rejects_a_seed_with_an_unknown_psk() {
    let seed_listener = TcpListener::bind("127.0.0.1:0").expect("seed listener");
    let joining_listener = TcpListener::bind("127.0.0.1:0").expect("joining listener");
    let seed_addr = seed_listener.local_addr().expect("seed address");
    let joining_addr = joining_listener.local_addr().expect("joining address");
    let seed = prepare("psk-seed").await;
    let joining = prepare("wrong-psk-host").await;
    let host = Link::from_str("/host").expect("host link");
    let keyring = actor_directory(&host, [seed.actor.clone(), joining.actor.clone()]);
    let (seed_task, seed_shutdown, seed_root, _) =
        start_with_psk(seed, seed_listener, keyring.clone(), [7; 32]).await;
    let (joining_task, joining_shutdown, joining_root, joining_kernel) =
        start_with_psk(joining, joining_listener, keyring, [8; 32]).await;
    assert!(
        joining_kernel
            .bootstrap_seed(
                &format!("http://{seed_addr}"),
                format!("http://{joining_addr}"),
            )
            .await
            .is_err()
    );

    let _ = seed_shutdown.send(());
    let _ = joining_shutdown.send(());
    let _ = seed_task.await;
    let _ = joining_task.await;
    std::fs::remove_dir_all(seed_root).expect("remove seed root");
    std::fs::remove_dir_all(joining_root).expect("remove joining root");
}

#[cfg(unix)]
#[tokio::test]
async fn sigterm_drains_host_and_preserves_acknowledged_service_writes() {
    use base64::Engine;
    use futures::TryStreamExt;

    struct Process(std::process::Child);

    impl Drop for Process {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let root = std::env::temp_dir().join(format!(
        "tc-sigterm-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    std::fs::create_dir(&root).unwrap();
    let installer = Actor::new("sigterm-installer".into(), rjwt::AlgKind::Falcon512).unwrap();
    let installers = serde_json::json!([{
        "host": "/host",
        "actor_id": "sigterm-installer",
        "algorithm": rjwt::AlgKind::Falcon512,
        "public_key_b64": base64::engine::general_purpose::STANDARD.encode(installer.verifying_key().to_bytes()),
        "allowed_prefixes": ["/service/example-devco"]
    }]).to_string();

    let identity: Link = "/service/example-devco/drain/1.0.0".parse().unwrap();
    let bearer = bearer_for(
        &installer,
        "/host".parse().unwrap(),
        Claim::new(identity.clone(), umask::USER_WRITE),
    );

    for reopen in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let log = std::fs::File::create(root.join(format!("server-{reopen}.log"))).unwrap();
        let mut process = Process(
            std::process::Command::new(env!("CARGO_BIN_EXE_tc-server"))
                .arg("--bind")
                .arg(address.to_string())
                .arg("--data-dir")
                .arg(root.join("data"))
                .arg("--workspace")
                .arg(root.join("workspace"))
                .args(["--host-id", "sigterm-host", "--request-ttl-secs", "600"])
                .arg("--trusted-installers-json")
                .arg(&installers)
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap(),
        );

        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                assert!(
                    process.0.try_wait().unwrap().is_none(),
                    "server exited; logs: {}",
                    root.display()
                );

                match Client::new()
                    .get(format!("http://{address}/healthz").parse().unwrap())
                    .await
                {
                    Ok(response) if response.status().is_success() => break,
                    _ => {}
                }

                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("server readiness");

        if !reopen {
            let definition = std::collections::BTreeMap::from([(
                identity.to_string(),
                service_fixture::definition(false),
            )]);

            let encoded = destream_json::encode(definition)
                .unwrap()
                .try_fold(Vec::new(), |mut bytes, chunk| async move {
                    bytes.extend_from_slice(&chunk);
                    Ok(bytes)
                })
                .await
                .unwrap();

            assert_success(put(address, "service", &bearer, encoded).await).await;
            assert_success(
                put(
                    address,
                    format!("{identity}/insert").trim_start_matches('/'),
                    &bearer,
                    "[null,[1]]",
                )
                .await,
            )
            .await;
        }

        assert_service_count(address, &identity, 1).await;
        // Keep an idle HTTP connection alive across SIGTERM; it must not pin drain.
        let keep_alive = Client::new();
        let response = keep_alive
            .get(format!("http://{address}/healthz").parse().unwrap())
            .await
            .unwrap();

        hyper::body::to_bytes(response).await.unwrap();
        assert!(
            std::process::Command::new("kill")
                .args(["-TERM", &process.0.id().to_string()])
                .status()
                .unwrap()
                .success()
        );

        let status = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Some(status) = process.0.try_wait().unwrap() {
                    break status;
                }

                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("SIGTERM drains host");

        assert!(status.success(), "{status}; logs: {}", root.display());
    }

    std::fs::remove_dir_all(root).unwrap();
}
