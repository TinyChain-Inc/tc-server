use std::net::TcpListener;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use hyper::{Body, StatusCode};
use pathlink::Link;
use tc_ir::LibrarySchema;
use tc_value::Value;
use tinychain::auth::{Actor, KeyringActorResolver, PublicKeyStore};
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

mod support;

use support::{
    begin_transaction, combine_host_handlers, finalize_install, get_library_status,
    put_install_payload, token_for_schema, token_for_schema_and_txn,
};

#[tokio::test]
async fn install_rejects_missing_token() {
    let schema = sample_schema("/lib/example-devco/alpha/0.1.0");
    let server = start_server("missing-token").await;
    let payload = install_payload_for_schema(&schema, ir_bytes_for_schema(&schema));
    let response = put_install_payload(server.addr, None, payload, None).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn install_rejects_wrong_scope_token() {
    let schema = sample_schema("/lib/example-devco/alpha/0.1.0");
    let wrong_schema = sample_schema("/lib/example-devco/beta/0.1.0");
    let server = start_server("wrong-scope").await;
    let token = token_for_schema(&server.actor, &wrong_schema, USER_WRITE);
    let payload = install_payload_for_schema(&schema, ir_bytes_for_schema(&schema));
    let response = put_install_payload(server.addr, Some(token), payload, None).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn install_rejects_mismatched_manifest() {
    let schema = sample_schema("/lib/example-devco/alpha/0.1.0");
    let mismatched_schema = sample_schema("/lib/example-devco/beta/0.1.0");
    let server = start_server("manifest-mismatch").await;
    let token = token_for_schema(&server.actor, &schema, USER_WRITE);
    let payload = install_payload_for_schema(&schema, ir_bytes_for_schema(&mismatched_schema));
    let response = put_install_payload(server.addr, Some(token), payload, None).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn install_rejects_read_only_token() {
    let schema = sample_schema("/lib/example-devco/alpha/0.1.0");
    let server = start_server("read-only").await;
    let token = token_for_schema(&server.actor, &schema, USER_READ);
    let payload = install_payload_for_schema(&schema, ir_bytes_for_schema(&schema));
    let response = put_install_payload(server.addr, Some(token), payload, None).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn install_accepts_write_token() {
    let schema = sample_schema("/lib/example-devco/alpha/0.1.0");
    let server = start_server("write-allowed").await;
    install_library_with_write_token(&server, &schema).await;

    let status = get_library_status(server.addr, &schema.id().to_string()).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn install_rejects_txn_hijack_by_different_authorized_actor() {
    let schema = sample_schema("/lib/example-devco/txn-hijack/0.1.0");
    let (server, attacker_actor) = start_server_with_secondary_actor("txn-hijack").await;

    let owner_begin = token_for_schema(&server.actor, &schema, USER_WRITE);
    let txn_id = begin_transaction(server.addr, owner_begin).await;

    // Attacker has a valid signer + write claim for the same schema, but does not own this txn.
    let attacker_token = token_for_schema_and_txn(&attacker_actor, &schema, USER_WRITE, txn_id);
    let payload = install_payload_for_schema(&schema, ir_bytes_for_schema(&schema));
    let hijack_put = put_install_payload(
        server.addr,
        Some(attacker_token.clone()),
        payload.clone(),
        Some(txn_id),
    )
    .await;
    assert_eq!(hijack_put.status(), StatusCode::UNAUTHORIZED);

    let hijack_finalize = finalize_install(server.addr, &attacker_token, txn_id, true).await;
    assert_eq!(hijack_finalize.status(), StatusCode::UNAUTHORIZED);

    let owner_token = token_for_schema_and_txn(&server.actor, &schema, USER_WRITE, txn_id);
    let owner_put = put_install_payload(
        server.addr,
        Some(owner_token.clone()),
        payload,
        Some(txn_id),
    )
    .await;
    assert_eq!(owner_put.status(), StatusCode::NO_CONTENT);
    let owner_commit = finalize_install(server.addr, &owner_token, txn_id, true).await;
    assert_eq!(owner_commit.status(), StatusCode::NO_CONTENT);

    let status = get_library_status(server.addr, &schema.id().to_string()).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn replication_bootstrap_propagates_library_across_three_hosts() {
    let schema = sample_schema("/lib/example-devco/cluster/0.1.0");

    let leader = start_server("leader").await;
    let replica_a = start_server("replica-a").await;
    let replica_b = start_server("replica-b").await;

    install_library_with_write_token(&leader, &schema).await;

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
    replicate_from_peers(&seed.registry, std::slice::from_ref(&leader_peer), &keys).await;

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
    replicate_from_peers(&replica.registry, std::slice::from_ref(&leader_peer), &keys).await;
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
    parse_psk_keys(&[
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

async fn start_server_with_secondary_actor(label: &str) -> (RunningServer, Actor) {
    let storage_dir = temp_storage_dir(label);
    let schema = default_library_schema();
    let module = build_http_library_module(schema.clone(), Some(storage_dir.clone()))
        .await
        .expect("module");
    module.hydrate_from_storage().await.expect("hydrate");
    let handlers = http_library_handlers(&module);

    let host = Link::from_str("/host").expect("host link");
    let actor = Actor::new(Value::from(format!("install-tester-{label}")));
    let secondary_actor = Actor::new(Value::from(format!("install-attacker-{label}")));
    let keyring = KeyringActorResolver::default()
        .with_actor(host.clone(), actor.clone())
        .with_actor(host, secondary_actor.clone());
    let public_keys = PublicKeyStore::default();
    let replication_issuer = Arc::new(ReplicationIssuer::new(
        Link::from_str("/host").expect("host link"),
        shared_replication_keys(),
        Actor::new(Value::from(format!("replication:security:{label}"))),
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
    (
        RunningServer {
            label: label.to_string(),
            addr,
            storage_dir,
            actor,
            registry: module,
            server_task: Some(server_task),
        },
        secondary_actor,
    )
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
        Actor::new(Value::from(format!("replication:security:{label}"))),
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

async fn install_library_with_write_token(server: &RunningServer, schema: &LibrarySchema) {
    let begin_token = token_for_schema(&server.actor, schema, USER_WRITE);
    let txn_id = begin_transaction(server.addr, begin_token).await;
    let token = token_for_schema_and_txn(&server.actor, schema, USER_WRITE, txn_id);
    let payload = install_payload_for_schema(schema, ir_bytes_for_schema(schema));
    let response =
        put_install_payload(server.addr, Some(token.clone()), payload, Some(txn_id)).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let commit = finalize_install(server.addr, &token, txn_id, true).await;
    assert_eq!(commit.status(), StatusCode::NO_CONTENT);
}
