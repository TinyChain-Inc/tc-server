#![cfg(all(feature = "http-client", feature = "http-server"))]

use std::net::TcpListener;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hyper::{Body, Client, Request, StatusCode};
use pathlink::Link;
use tc_ir::Claim;
use tinychain::auth::{Actor, KeyringActorResolver, RjwtTokenVerifier, Token, wire_claim};
use tinychain::http::{HttpHandler, HttpKernelConfig, HttpServer, build_http_runtime_with_config};
use tinychain::replication::{HttpClusterGateway, PeerMembership};
use tinychain::{HostLimits, HostStorage, HttpRpcGateway, ProtocolAuthority, Workspace};

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
        .load_or_create_protocol_authority(label, host)
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

async fn start(
    host: PreparedHost,
    listener: TcpListener,
    peers: Vec<String>,
    keyring: KeyringActorResolver,
) -> (tokio::task::JoinHandle<()>, std::path::PathBuf) {
    let config = HttpKernelConfig::new(
        host.roots,
        host.workspace,
        host.protocol,
        Arc::new(RjwtTokenVerifier::new(Arc::new(keyring))),
        tinychain::auth::PublicKeyStore::default(),
        HttpClusterGateway::new(PeerMembership::new(peers)),
        HttpRpcGateway::new(),
    );
    let runtime = build_http_runtime_with_config(config, empty_handler())
        .await
        .expect("HTTP runtime");
    let task = tokio::spawn(async move {
        let _ = HttpServer::new(runtime.kernel, runtime.router)
            .serve_listener(listener)
            .await;
    });
    (task, host.root)
}

fn empty_handler() -> impl HttpHandler {
    |_| async {
        hyper::Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .expect("response")
    }
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
    let keyring = KeyringActorResolver::default()
        .with_actor(host.clone(), primary.actor.clone())
        .with_actor(host.clone(), replica.actor.clone())
        .with_actor(host.clone(), installer.clone());

    let (replica_task, replica_root) =
        start(replica, replica_listener, vec![], keyring.clone()).await;
    let (primary_task, primary_root) = start(
        primary,
        primary_listener,
        vec![format!("http://{replica_addr}")],
        keyring.clone(),
    )
    .await;

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
        let response = put_with_type(
            primary_addr,
            "lib",
            &bearer,
            "application/wasm",
            literal_wasm_module(),
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
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
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
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = Client::new()
        .get(
            format!("http://{replica_addr}{class}")
                .parse()
                .expect("URI"),
        )
        .await
        .expect("replica Class response");
    assert_eq!(response.status(), StatusCode::OK);

    replica_task.abort();
    let restarted_listener = TcpListener::bind("127.0.0.1:0").expect("restart listener");
    let restarted_addr = restarted_listener.local_addr().expect("restart address");
    let restarted = open("replica", replica_root.clone()).await;
    let (restarted_task, _) = start(restarted, restarted_listener, vec![], keyring).await;
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

    primary_task.abort();
    restarted_task.abort();
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
    let keyring = KeyringActorResolver::default()
        .with_actor(host.clone(), primary.actor.clone())
        .with_actor(host.clone(), replica.actor.clone())
        .with_actor(host.clone(), installer.clone());
    let (replica_task, replica_root) =
        start(replica, replica_listener, vec![], keyring.clone()).await;
    let (primary_task, primary_root) = start(
        primary,
        primary_listener,
        vec![
            format!("http://{replica_addr}"),
            "http://127.0.0.1:9".into(),
        ],
        keyring,
    )
    .await;
    let identity: pathlink::Link = "/lib/example-devco/rollback/1.0.0"
        .parse()
        .expect("identity");
    let bearer = bearer_for(
        &installer,
        host,
        Claim::new(identity.clone(), umask::USER_WRITE),
    );
    let response = put(
        primary_addr,
        "lib",
        &bearer,
        format!(r#"{{"{identity}":{{"ok":true}}}}"#),
    )
    .await;
    assert!(!response.status().is_success());
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let response = Client::new()
                .get(
                    format!("http://{replica_addr}{identity}")
                        .parse()
                        .expect("URI"),
                )
                .await
                .expect("replica response");
            if response.status() == StatusCode::NOT_FOUND {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("uncommitted replica work must disappear at cutoff");
    primary_task.abort();
    replica_task.abort();
    std::fs::remove_dir_all(primary_root).expect("remove primary root");
    std::fs::remove_dir_all(replica_root).expect("remove replica root");
}
