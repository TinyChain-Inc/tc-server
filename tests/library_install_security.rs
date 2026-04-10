use std::net::TcpListener;
use std::path::PathBuf;
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
    ReplicationIssuer, export_handler, parse_psk_keys, replicate_from_peers,
    replication_token_handler,
};
use tinychain::storage::Artifact;
use umask::{USER_READ, USER_WRITE};

#[tokio::test]
async fn install_rejects_missing_token() {
    let schema = sample_schema("/lib/example-devco/alpha/0.1.0");
    let server = start_server("missing-token").await;
    let payload = install_payload_for_schema(&schema, ir_bytes_for_schema(&schema));
    let response = put_install_payload(server.addr, None, payload).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn install_rejects_wrong_scope_token() {
    let schema = sample_schema("/lib/example-devco/alpha/0.1.0");
    let wrong_schema = sample_schema("/lib/example-devco/beta/0.1.0");
    let server = start_server("wrong-scope").await;
    let token = token_for_schema(&server.actor, &wrong_schema, USER_WRITE);
    let payload = install_payload_for_schema(&schema, ir_bytes_for_schema(&schema));
    let response = put_install_payload(server.addr, Some(token), payload).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn install_rejects_mismatched_manifest() {
    let schema = sample_schema("/lib/example-devco/alpha/0.1.0");
    let mismatched_schema = sample_schema("/lib/example-devco/beta/0.1.0");
    let server = start_server("manifest-mismatch").await;
    let token = token_for_schema(&server.actor, &schema, USER_WRITE);
    let payload = install_payload_for_schema(&schema, ir_bytes_for_schema(&mismatched_schema));
    let response = put_install_payload(server.addr, Some(token), payload).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn install_rejects_read_only_token() {
    let schema = sample_schema("/lib/example-devco/alpha/0.1.0");
    let server = start_server("read-only").await;
    let token = token_for_schema(&server.actor, &schema, USER_READ);
    let payload = install_payload_for_schema(&schema, ir_bytes_for_schema(&schema));
    let response = put_install_payload(server.addr, Some(token), payload).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn install_accepts_write_token() {
    let schema = sample_schema("/lib/example-devco/alpha/0.1.0");
    let server = start_server("write-allowed").await;
    let token = token_for_schema(&server.actor, &schema, USER_WRITE);
    let payload = install_payload_for_schema(&schema, ir_bytes_for_schema(&schema));
    let response = put_install_payload(server.addr, Some(token), payload).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let status = get_library_status(server.addr, &schema.id().to_string()).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn replication_bootstrap_propagates_library_across_three_hosts() {
    let schema = sample_schema("/lib/example-devco/cluster/0.1.0");

    let leader = start_server("leader").await;
    let replica_a = start_server("replica-a").await;
    let replica_b = start_server("replica-b").await;

    let install_token = token_for_schema(&leader.actor, &schema, USER_WRITE);
    let install_payload = install_payload_for_schema(&schema, ir_bytes_for_schema(&schema));
    let install_response =
        put_install_payload(leader.addr, Some(install_token), install_payload).await;
    assert_eq!(install_response.status(), StatusCode::NO_CONTENT);

    let leader_status = get_library_status(leader.addr, &schema.id().to_string()).await;
    assert_eq!(leader_status, StatusCode::OK);

    let keys = shared_replication_keys();
    let leader_peer = format!("http://{}", leader.addr);
    replicate_from_peers(&replica_a.registry, &[leader_peer], &keys).await;

    let replica_a_status = get_library_status(replica_a.addr, &schema.id().to_string()).await;
    assert_eq!(replica_a_status, StatusCode::OK);

    let replica_a_peer = format!("http://{}", replica_a.addr);
    replicate_from_peers(&replica_b.registry, &[replica_a_peer], &keys).await;

    let replica_b_status = get_library_status(replica_b.addr, &schema.id().to_string()).await;
    assert_eq!(replica_b_status, StatusCode::OK);
}

#[tokio::test]
async fn replication_bootstrap_tolerates_membership_churn_and_dead_peers() {
    let schema = sample_schema("/lib/example-devco/churn/0.1.0");
    let keys = shared_replication_keys();

    let leader = start_server("churn-leader").await;
    install_library_with_write_token(&leader, &schema).await;

    let seed = start_server("churn-seed").await;
    let leader_peer = format!("http://{}", leader.addr);
    replicate_from_peers(&seed.registry, &[leader_peer], &keys).await;
    let seed_status = get_library_status(seed.addr, &schema.id().to_string()).await;
    assert_eq!(seed_status, StatusCode::OK);

    let replacement = start_server("churn-replacement").await;
    let peers = vec![
        "http://127.0.0.1:1".to_string(),
        format!("http://{}", seed.addr),
    ];
    replicate_from_peers(&replacement.registry, &peers, &keys).await;

    let replacement_status = get_library_status(replacement.addr, &schema.id().to_string()).await;
    assert_eq!(replacement_status, StatusCode::OK);
}

#[tokio::test]
async fn scale_out_joiner_catches_up_after_existing_member_reconciles() {
    let schema_a = sample_schema("/lib/example-devco/scale-a/0.1.0");
    let schema_b = sample_schema("/lib/example-devco/scale-b/0.1.0");
    let keys = shared_replication_keys();

    let leader = start_server("scale-leader").await;
    install_library_with_write_token(&leader, &schema_a).await;

    let seed = start_server("scale-seed").await;
    let leader_peer = format!("http://{}", leader.addr);
    replicate_from_peers(&seed.registry, &[leader_peer.clone()], &keys).await;

    let seed_a = get_library_status(seed.addr, &schema_a.id().to_string()).await;
    assert_eq!(seed_a, StatusCode::OK);

    install_library_with_write_token(&leader, &schema_b).await;

    // Running members only sync on bootstrap/reconcile.
    let seed_b_before = get_library_status(seed.addr, &schema_b.id().to_string()).await;
    assert_eq!(seed_b_before, StatusCode::NOT_FOUND);

    replicate_from_peers(&seed.registry, &[leader_peer], &keys).await;
    let seed_b_after = get_library_status(seed.addr, &schema_b.id().to_string()).await;
    assert_eq!(seed_b_after, StatusCode::OK);

    let joiner = start_server("scale-joiner").await;
    let seed_peer = format!("http://{}", seed.addr);
    replicate_from_peers(&joiner.registry, &[seed_peer], &keys).await;

    let joiner_a = get_library_status(joiner.addr, &schema_a.id().to_string()).await;
    let joiner_b = get_library_status(joiner.addr, &schema_b.id().to_string()).await;
    assert_eq!(joiner_a, StatusCode::OK);
    assert_eq!(joiner_b, StatusCode::OK);
}

#[tokio::test]
async fn restart_rehydrates_and_catches_up_after_downtime() {
    let schema_v0 = sample_schema("/lib/example-devco/restart/0.1.0");
    let schema_v1 = sample_schema("/lib/example-devco/restart/0.2.0");
    let keys = shared_replication_keys();

    let leader = start_server("restart-leader").await;
    let mut replica = start_server("restart-replica").await;
    let leader_peer = format!("http://{}", leader.addr);

    install_library_with_write_token(&leader, &schema_v0).await;
    replicate_from_peers(&replica.registry, &[leader_peer.clone()], &keys).await;
    let replica_v0_before = get_library_status(replica.addr, &schema_v0.id().to_string()).await;
    assert_eq!(replica_v0_before, StatusCode::OK);

    replica.stop();
    install_library_with_write_token(&leader, &schema_v1).await;
    replica.restart().await;

    // Restart should hydrate previously persisted library state.
    let replica_v0_after_restart =
        get_library_status(replica.addr, &schema_v0.id().to_string()).await;
    assert_eq!(replica_v0_after_restart, StatusCode::OK);

    // New version should appear only after explicit reconcile.
    let replica_v1_before = get_library_status(replica.addr, &schema_v1.id().to_string()).await;
    assert_eq!(replica_v1_before, StatusCode::NOT_FOUND);

    replicate_from_peers(&replica.registry, &[leader_peer], &keys).await;
    let replica_v1_after = get_library_status(replica.addr, &schema_v1.id().to_string()).await;
    assert_eq!(replica_v1_after, StatusCode::OK);
}

struct RunningServer {
    label: String,
    addr: std::net::SocketAddr,
    storage_dir: PathBuf,
    actor: Actor,
    registry: Arc<LibraryRegistry>,
    server_task: Option<tokio::task::JoinHandle<()>>,
}

impl RunningServer {
    fn stop(&mut self) {
        if let Some(task) = self.server_task.take() {
            task.abort();
        }
    }

    async fn restart(&mut self) {
        self.stop();

        let mut restarted = start_server_with_storage(&self.label, self.storage_dir.clone()).await;
        self.addr = restarted.addr;
        self.actor = restarted.actor.clone();
        self.registry = restarted.registry.clone();
        self.server_task = restarted.server_task.take();
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.stop();
    }
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

fn temp_storage_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "tc-install-security-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

async fn start_server(label: &str) -> RunningServer {
    let storage_dir = temp_storage_dir(label);
    start_server_with_storage(label, storage_dir).await
}

async fn start_server_with_storage(label: &str, storage_dir: PathBuf) -> RunningServer {
    let schema = default_library_schema();
    let module = build_http_library_module(schema.clone(), Some(storage_dir.clone()))
        .await
        .expect("module");
    module.hydrate_from_storage().await.expect("hydrate");
    let handlers = http_library_handlers(&module);

    let host = Link::from_str("/host").expect("host link");
    let actor = Actor::new(Value::from(format!("install-tester-{label}")));
    let keyring = KeyringActorResolver::default().with_actor(host, actor.clone());
    let public_keys = PublicKeyStore::default();
    let replication_issuer = Arc::new(ReplicationIssuer::new(
        Link::from_str("/host").expect("host link"),
        shared_replication_keys(),
        keyring.clone(),
        public_keys.clone(),
    ));

    let kernel = Kernel::builder()
        .with_host_id(format!("test-install-security-{label}"))
        .with_http_rpc_gateway()
        .with_rjwt_keyring_token_verifier(keyring.clone())
        .with_library_module(module.clone(), handlers)
        .with_service_handler(|_req| async move { hyper::Response::new(Body::empty()) })
        .with_kernel_handler(combine_host_handlers(
            host_handler_with_public_keys(public_keys),
            replication_token_handler(replication_issuer),
            export_handler(module.clone()),
        ))
        .with_health_handler(|_req| async move { hyper::Response::new(Body::empty()) })
        .finish();

    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    let addr = listener.local_addr().expect("addr");

    let server = HttpServer::new(kernel);
    let server_task = tokio::spawn(async move {
        let _ = server.serve_listener(listener).await;
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    RunningServer {
        label: label.to_string(),
        addr,
        storage_dir,
        actor,
        registry: module,
        server_task: Some(server_task),
    }
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

async fn install_library_with_write_token(server: &RunningServer, schema: &LibrarySchema) {
    let token = token_for_schema(&server.actor, schema, USER_WRITE);
    let payload = install_payload_for_schema(schema, ir_bytes_for_schema(schema));
    let response = put_install_payload(server.addr, Some(token), payload).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

fn combine_host_handlers(
    public: impl tinychain::KernelHandler,
    token: impl tinychain::KernelHandler,
    export: impl tinychain::KernelHandler,
) -> impl tinychain::KernelHandler {
    let public: Arc<dyn tinychain::KernelHandler> = Arc::new(public);
    let token: Arc<dyn tinychain::KernelHandler> = Arc::new(token);
    let export: Arc<dyn tinychain::KernelHandler> = Arc::new(export);

    move |req: tinychain::Request| {
        let path = req.uri().path().to_string();
        let public = Arc::clone(&public);
        let token = Arc::clone(&token);
        let export = Arc::clone(&export);
        async move {
            match path.as_str() {
                "/" => token.call(req).await,
                "/host/library/export" => export.call(req).await,
                _ => public.call(req).await,
            }
        }
    }
}
