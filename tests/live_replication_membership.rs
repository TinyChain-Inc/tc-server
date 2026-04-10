use std::net::TcpListener;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use hyper::{Body, Client, Request, StatusCode};
use pathlink::Link;
use tc_ir::{Claim, LibrarySchema};
use tc_value::Value;
use tinychain::auth::{Actor, KeyringActorResolver, PublicKeyStore, Token};
use tinychain::http::{HttpServer, host_handler_with_public_keys};
use tinychain::kernel::Kernel;
use tinychain::library::LibraryRegistry;
use tinychain::library::http::{build_http_library_module, http_library_handlers};
use tinychain::library::{InstallArtifacts, default_library_schema, encode_install_payload_bytes};
use tinychain::replication::{
    PEERS_HEARTBEAT_PATH, PEERS_JOIN_PATH, PEERS_LEAVE_PATH, PEERS_PATH, PeerMembership,
    ReplicationIssuer, export_handler, leave_peer_cluster, live_replicating_install_put_handler,
    parse_psk_keys, peer_membership_handler, register_with_peer, replication_token_handler,
};
use tinychain::storage::Artifact;
use umask::USER_WRITE;

#[tokio::test]
async fn join_enables_live_write_forwarding() {
    let leader = start_live_server("live-leader", vec![]).await;
    let replica = start_live_server("live-replica", vec![]).await;
    let keys = shared_replication_keys();

    register_with_peer(
        &format!("http://{}", leader.addr),
        &format!("http://{}", replica.addr),
        &keys,
    )
    .await
    .expect("join cluster");

    let schema = sample_schema("/lib/example-devco/live-forward/0.1.0");
    install_with_write_token(&leader, &schema).await;

    wait_for_status(replica.addr, &schema.id().to_string(), StatusCode::OK).await;
}

#[tokio::test]
async fn leave_stops_live_write_forwarding() {
    let leader = start_live_server("leave-leader", vec![]).await;
    let replica = start_live_server("leave-replica", vec![]).await;
    let keys = shared_replication_keys();

    register_with_peer(
        &format!("http://{}", leader.addr),
        &format!("http://{}", replica.addr),
        &keys,
    )
    .await
    .expect("join cluster");

    let schema_a = sample_schema("/lib/example-devco/live-leave/0.1.0");
    install_with_write_token(&leader, &schema_a).await;
    wait_for_status(replica.addr, &schema_a.id().to_string(), StatusCode::OK).await;

    leave_peer_cluster(
        &format!("http://{}", leader.addr),
        &format!("http://{}", replica.addr),
        &keys,
    )
    .await
    .expect("leave cluster");

    let schema_b = sample_schema("/lib/example-devco/live-leave/0.2.0");
    install_with_write_token(&leader, &schema_b).await;

    tokio::time::sleep(Duration::from_millis(200)).await;
    let status = get_library_status(replica.addr, &schema_b.id().to_string()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

struct RunningServer {
    addr: std::net::SocketAddr,
    actor: Actor,
    _registry: Arc<LibraryRegistry>,
    _membership: PeerMembership,
}

fn sample_schema(id: &str) -> LibrarySchema {
    LibrarySchema::new(Link::from_str(id).expect("schema link"), "0.1.0", vec![])
}

fn ir_bytes_for_schema(schema: &LibrarySchema) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "schema": {
            "id": schema.id().to_string(),
            "version": schema.version(),
            "dependencies": [],
        },
        "routes": [
            {
                "path": "/ok",
                "value": { "ok": true }
            }
        ]
    }))
    .expect("ir manifest bytes")
}

fn install_payload_for_schema(schema: &LibrarySchema, artifact_bytes: Vec<u8>) -> Vec<u8> {
    let artifacts = vec![Artifact {
        path: schema.id().to_string(),
        content_type: tinychain::ir::IR_ARTIFACT_CONTENT_TYPE.to_string(),
        bytes: artifact_bytes,
    }];
    let payload = InstallArtifacts {
        schema: schema.clone(),
        artifacts,
    };
    encode_install_payload_bytes(&payload).expect("install payload bytes")
}

fn shared_replication_keys() -> Vec<aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>> {
    parse_psk_keys(&vec![
        "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
    ])
    .expect("psk keys")
}

async fn start_live_server(label: &str, initial_peers: Vec<String>) -> RunningServer {
    let schema = default_library_schema();
    let module = build_http_library_module(schema.clone(), None)
        .await
        .expect("module");
    let handlers = http_library_handlers(&module);

    let host = Link::from_str("/host").expect("host link");
    let actor = Actor::new(Value::from(format!("installer-{label}")));
    let keyring = KeyringActorResolver::default().with_actor(host, actor.clone());
    let public_keys = PublicKeyStore::default();
    let keys = shared_replication_keys();
    let issuer = Arc::new(ReplicationIssuer::new(
        Link::from_str("/host").expect("host link"),
        keys.clone(),
        keyring.clone(),
        public_keys.clone(),
    ));
    let membership = PeerMembership::new(initial_peers);
    let live_put = live_replicating_install_put_handler(module.clone(), membership.clone(), keys);

    let kernel = Kernel::builder()
        .with_host_id(format!("live-replication-{label}"))
        .with_http_rpc_gateway()
        .with_rjwt_keyring_token_verifier(keyring.clone())
        .with_library_module(module.clone(), handlers)
        .with_lib_put_handler(live_put)
        .with_service_handler(|_req| async move { hyper::Response::new(Body::empty()) })
        .with_kernel_handler(combine_host_handlers(
            host_handler_with_public_keys(public_keys),
            replication_token_handler(issuer.clone()),
            export_handler(module.clone()),
            peer_membership_handler(membership.clone(), issuer),
        ))
        .with_health_handler(|_req| async move { hyper::Response::new(Body::empty()) })
        .finish();

    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    let addr = listener.local_addr().expect("addr");
    let server = HttpServer::new(kernel);
    tokio::spawn(async move {
        let _ = server.serve_listener(listener).await;
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    RunningServer {
        addr,
        actor,
        _registry: module,
        _membership: membership,
    }
}

async fn install_with_write_token(server: &RunningServer, schema: &LibrarySchema) {
    let token = token_for_schema(&server.actor, schema, USER_WRITE);
    let payload = install_payload_for_schema(schema, ir_bytes_for_schema(schema));
    let response = put_install_payload(server.addr, Some(token), payload).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

fn token_for_schema(actor: &Actor, schema: &LibrarySchema, mask: umask::Mode) -> String {
    let host = Link::from_str("/host").expect("host link");
    let claim = Claim::new(schema.id().clone(), mask);
    let token = Token::new(
        host,
        std::time::SystemTime::now(),
        Duration::from_secs(30),
        actor.id().clone(),
        claim,
    );
    let signed = actor.sign_token(token).expect("sign token");
    signed.into_jwt()
}

async fn put_install_payload(
    addr: std::net::SocketAddr,
    bearer: Option<String>,
    payload: Vec<u8>,
) -> hyper::Response<Body> {
    let uri = format!("http://{addr}/lib");
    let mut req = Request::builder()
        .method("PUT")
        .uri(uri)
        .header(hyper::header::CONTENT_TYPE, "application/json");
    if let Some(token) = bearer {
        req = req.header(hyper::header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let request = req.body(Body::from(payload)).expect("request");
    Client::new().request(request).await.expect("response")
}

async fn get_library_status(addr: std::net::SocketAddr, path: &str) -> StatusCode {
    let uri = format!("http://{addr}{path}");
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .expect("request");
    Client::new()
        .request(request)
        .await
        .expect("response")
        .status()
}

async fn wait_for_status(addr: std::net::SocketAddr, path: &str, expected: StatusCode) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let status = get_library_status(addr, path).await;
        if status == expected {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for status {expected}; last status was {status}"
        );

        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn combine_host_handlers(
    public: impl tinychain::KernelHandler,
    token: impl tinychain::KernelHandler,
    export: impl tinychain::KernelHandler,
    peers: impl tinychain::KernelHandler,
) -> impl tinychain::KernelHandler {
    let public: Arc<dyn tinychain::KernelHandler> = Arc::new(public);
    let token: Arc<dyn tinychain::KernelHandler> = Arc::new(token);
    let export: Arc<dyn tinychain::KernelHandler> = Arc::new(export);
    let peers: Arc<dyn tinychain::KernelHandler> = Arc::new(peers);

    move |req: tinychain::Request| {
        let path = req.uri().path().to_string();
        let public = Arc::clone(&public);
        let token = Arc::clone(&token);
        let export = Arc::clone(&export);
        let peers = Arc::clone(&peers);
        async move {
            match path.as_str() {
                "/" => token.call(req).await,
                "/host/library/export" => export.call(req).await,
                PEERS_PATH | PEERS_JOIN_PATH | PEERS_LEAVE_PATH | PEERS_HEARTBEAT_PATH => {
                    peers.call(req).await
                }
                _ => public.call(req).await,
            }
        }
    }
}
