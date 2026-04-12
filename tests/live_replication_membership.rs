use std::net::TcpListener;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
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
    PeerMembership, ReplicatedTxnTracker, ReplicationIssuer, export_handler, leave_peer_cluster,
    live_replicating_finalize_hook, live_replicating_install_put_handler, parse_psk_keys,
    peer_membership_handler, register_with_peer, replication_token_handler,
};
use tinychain::storage::Artifact;
use umask::USER_WRITE;

mod support;

use support::{
    begin_transaction, combine_host_handlers_with_peers, finalize_install, get_library_status,
    put_install_payload, token_for_schema, token_for_schema_and_txn,
};

#[tokio::test]
async fn join_enables_live_write_forwarding() {
    let leader = start_live_server("live-leader", vec![]).await;
    let replica = start_live_server("live-replica", vec![]).await;
    let keys = shared_replication_keys();

    register_with_peer(&format!("http://{}", leader.addr), &replica.identity, &keys)
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

    register_with_peer(&format!("http://{}", leader.addr), &replica.identity, &keys)
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

#[tokio::test]
async fn join_rejects_invalid_peer_identity_payloads() {
    let leader = start_live_server("join-validate-leader", vec![]).await;
    let keys = shared_replication_keys();
    let seed = format!("http://{}", leader.addr);
    let peer = "http://127.0.0.1:9703".to_string();

    let bad_actor_prefix = tinychain::replication::PeerIdentity {
        peer: peer.clone(),
        actor_id: "installer:bad-prefix".to_string(),
        public_key_b64: valid_public_key_b64("join-bad-prefix"),
    };
    let err = register_with_peer(&seed, &bad_actor_prefix, &keys)
        .await
        .expect_err("join should reject non-replication actor_id");
    assert!(
        err.message()
            .contains("peer actor_id must start with replication:"),
        "unexpected error: {}",
        err.message()
    );

    let missing_actor = tinychain::replication::PeerIdentity {
        peer: peer.clone(),
        actor_id: "   ".to_string(),
        public_key_b64: valid_public_key_b64("join-missing-actor"),
    };
    let err = register_with_peer(&seed, &missing_actor, &keys)
        .await
        .expect_err("join should reject empty actor_id");
    assert!(
        err.message().contains("missing peer actor_id"),
        "unexpected error: {}",
        err.message()
    );

    let invalid_key = tinychain::replication::PeerIdentity {
        peer,
        actor_id: "replication:join-invalid-key".to_string(),
        public_key_b64: "not-base64".to_string(),
    };
    let err = register_with_peer(&seed, &invalid_key, &keys)
        .await
        .expect_err("join should reject malformed public key");
    assert!(
        err.message().contains("invalid peer public_key_b64"),
        "unexpected error: {}",
        err.message()
    );

    let listing = tinychain::replication::list_peer_cluster(&seed)
        .await
        .expect("peer listing");
    assert!(
        listing.peers.is_empty(),
        "unexpected peers: {:?}",
        listing.peers
    );
    assert!(
        listing.identities.is_empty(),
        "unexpected identities: {:?}",
        listing.identities
    );
}

struct RunningServer {
    addr: std::net::SocketAddr,
    identity: tinychain::replication::PeerIdentity,
    _registry: Arc<LibraryRegistry>,
    _membership: PeerMembership,
}

fn shared_installer_actor() -> &'static Actor {
    static SHARED_ACTOR: OnceLock<Actor> = OnceLock::new();
    SHARED_ACTOR.get_or_init(|| Actor::new(Value::from("installer-shared")))
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

fn valid_public_key_b64(actor_id: &str) -> String {
    let actor = Actor::new(Value::from(actor_id.to_string()));
    BASE64.encode(actor.public_key().to_bytes())
}

async fn start_live_server(label: &str, initial_peers: Vec<String>) -> RunningServer {
    let schema = default_library_schema();
    let module = build_http_library_module(schema.clone(), None)
        .await
        .expect("module");
    let handlers = http_library_handlers(&module);

    let host = Link::from_str("/host").expect("host link");
    let actor = shared_installer_actor();
    let keyring = KeyringActorResolver::default().with_actor(host, actor.clone());
    let public_keys = PublicKeyStore::default();
    let keys = shared_replication_keys();
    let replication_actor = Actor::new(Value::from(format!("replication:{label}")));
    let issuer = Arc::new(ReplicationIssuer::new(
        Link::from_str("/host").expect("host link"),
        keys.clone(),
        replication_actor,
        keyring.clone(),
        public_keys.clone(),
    ));
    let membership = PeerMembership::new(initial_peers);
    let tracker = ReplicatedTxnTracker::default();
    let live_put = live_replicating_install_put_handler(
        module.clone(),
        membership.clone(),
        keys,
        tracker.clone(),
    );
    let finalize_hook = live_replicating_finalize_hook(membership.clone(), tracker);

    let kernel = Kernel::builder()
        .with_host_id(format!("live-replication-{label}"))
        .with_http_rpc_gateway()
        .with_rjwt_keyring_token_verifier(keyring.clone())
        .with_library_module(module.clone(), handlers)
        .with_lib_put_handler(live_put)
        .with_txn_finalize_hook(finalize_hook)
        .with_service_handler(|_req| async move { hyper::Response::new(Body::empty()) })
        .with_kernel_handler(combine_host_handlers_with_peers(
            host_handler_with_public_keys(public_keys),
            replication_token_handler(issuer.clone()),
            export_handler(module.clone()),
            peer_membership_handler(membership.clone(), issuer.clone()),
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
    let identity = issuer
        .self_identity(format!("http://{}", addr))
        .expect("replication identity");

    RunningServer {
        addr,
        identity,
        _registry: module,
        _membership: membership,
    }
}

async fn install_with_write_token(server: &RunningServer, schema: &LibrarySchema) {
    let begin_token = token_for_schema(shared_installer_actor(), schema, USER_WRITE);
    let txn_id = begin_transaction(server.addr, begin_token).await;
    let token = token_for_schema_and_txn(shared_installer_actor(), schema, USER_WRITE, txn_id);
    let payload = install_payload_for_schema(schema, ir_bytes_for_schema(schema));
    let response =
        put_install_payload(server.addr, Some(token.clone()), payload, Some(txn_id)).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let commit = finalize_install(server.addr, &token, txn_id, true).await;
    assert_eq!(commit.status(), StatusCode::NO_CONTENT);
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
