#![cfg(all(feature = "http-client", feature = "http-server"))]

use std::net::{SocketAddr, TcpListener};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hyper::{Body, Client, Request, StatusCode};
use pathlink::Link;
use tinychain::Claim;
use tinychain::auth::{Actor, KeyringActorResolver, RjwtTokenVerifier, Token, wire_claim};
use tinychain::http::HttpServer;
use tinychain::replication::ReplicationIssuer;
use tinychain::{HostLimits, HostStorage, HttpGateway, ProtocolAuthority, Workspace};

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
    format!(
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
    )
    .into_bytes()
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
        .sign_token(Token::new(
            host,
            SystemTime::now(),
            Duration::from_secs(30),
            actor.id().clone(),
            wire_claim(claim),
        ))
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
        resources: tinychain::HostResources::new(limits.clone()),
        protocol: host.protocol,
        verifier: Arc::new(RjwtTokenVerifier::new(Arc::new(keyring.clone()))),
        actors: keyring,
        bootstrap,
        bootstrap_required: false,
    };
    let kernel = tinychain::Kernel::new(services, host.workspace, limits.transaction_ttl)
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
    let installer = Actor::new_falcon512("two-host-installer".to_string()).expect("installer");
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
    let body = format!(r#"{{"{identity}":{{"ok":true}}}}"#);
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
            Some(&hyper::header::HeaderValue::from_static("application/wasm"))
        );
        assert_eq!(
            hyper::body::to_bytes(response.into_body())
                .await
                .expect("WASM module body"),
            module
        );
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
async fn partial_replica_delivery_is_discarded_at_cutoff() {
    let primary_listener = TcpListener::bind("127.0.0.1:0").expect("primary listener");
    let replica_listener = TcpListener::bind("127.0.0.1:0").expect("replica listener");
    let primary_addr = primary_listener.local_addr().expect("primary address");
    let replica_addr = replica_listener.local_addr().expect("replica address");
    let primary = prepare("rollback-primary").await;
    let replica = prepare("rollback-replica").await;
    let installer = Actor::new_falcon512("rollback-installer".to_string()).expect("installer");
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
        .expect("failed write response");
    assert!(
        !status.is_success(),
        "replica failure unexpectedly returned {status}: {}",
        String::from_utf8_lossy(&body)
    );
    tokio::time::sleep(Duration::from_secs(7)).await;
    let response = Client::new()
        .get(
            format!("http://{primary_addr}{identity}")
                .parse()
                .expect("URI"),
        )
        .await
        .expect("primary response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let _ = primary_shutdown.send(());
    let _ = primary_task.await;
    std::fs::remove_dir_all(primary_root).expect("remove primary root");
    std::fs::remove_dir_all(replica_root).expect("remove replica root");
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
    let installer = Actor::new_falcon512("majority-installer".to_string()).expect("installer");
    let host = Link::from_str("/host").expect("host link");
    let keyring = actor_directory(
        &host,
        [
            primary.actor.clone(),
            replica_a.actor.clone(),
            replica_b.actor.clone(),
            installer.clone(),
        ],
    );

    let (replica_a_task, replica_a_shutdown, replica_a_root, _) =
        start(replica_a, replica_a_listener, keyring.clone()).await;
    let (replica_b_task, replica_b_shutdown, replica_b_root, _) =
        start(replica_b, replica_b_listener, keyring.clone()).await;
    let (primary_task, primary_shutdown, primary_root, primary_kernel) =
        start(primary, primary_listener, keyring).await;
    for seed in [replica_a_addr, replica_b_addr] {
        primary_kernel
            .bootstrap_seed(&format!("http://{seed}"), format!("http://{primary_addr}"))
            .await
            .expect("resource-scoped bootstrap");
    }

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
    let installer = Actor::new_falcon512("bootstrap-installer".to_string()).expect("installer");
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
        start(joining, joining_listener, keyring).await;
    joining_kernel
        .bootstrap_seed(
            &format!("http://{seed_addr}"),
            format!("http://{joining_addr}"),
        )
        .await
        .expect("bootstrap populated resource tree");
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

    // Bootstrap must enroll the exact nested item, not only its `/lib` root.
    // Deleting from the joining host therefore propagates back to the seed.
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
