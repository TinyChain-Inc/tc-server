use super::*;
use crate::KernelHandler;
use crate::auth::{Actor, KeyringActorResolver, PublicKeyStore};
use crate::library::http::build_http_library_module;
use crate::replication::crypto::{
    decode_encrypted_payload, decrypt_token_with_key, encode_encrypted_payload,
    encrypt_path_with_key,
};
use crate::replication::{PeerMembership, PeerRoutes, peer_membership_handler};
use crate::storage::{LibraryStore, load_library_root};
use hyper::body::to_bytes;
use hyper::{Body, Request, StatusCode};
use pathlink::Link;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use tc_ir::LibrarySchema;

#[tokio::test]
async fn issues_replication_token() {
    let dir = std::env::temp_dir().join(format!(
        "tc-psk-rotation-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create dir");

    let schema = LibrarySchema::new(
        Link::from_str("/lib/example-devco/hello/0.1.0").expect("link"),
        "0.1.0",
        vec![],
    );
    let root = load_library_root(dir).await.expect("store root");
    let store = LibraryStore::from_root(root);
    let store = store.for_schema(&schema).await.expect("store");
    store
        .persist_artifact(
            &schema,
            &crate::storage::Artifact {
                path: schema.id().to_string(),
                content_type: crate::ir::WASM_ARTIFACT_CONTENT_TYPE.to_string(),
                bytes: b"fake-wasm".to_vec(),
            },
        )
        .await
        .expect("persist wasm");

    let _module = build_http_library_module(schema.clone(), None)
        .await
        .expect("module");

    let host = pathlink::Link::from_str("/host").expect("host");
    let keyring = KeyringActorResolver::default();
    let public_keys = PublicKeyStore::default();
    let keys = parse_psk_keys(&[
        "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
        "0000000000000000000000000000000000000000000000000000000000000002".to_string(),
    ])
    .expect("keys");

    let issuer = Arc::new(ReplicationIssuer::new(
        host,
        keys.clone(),
        Actor::new(tc_value::Value::String("replication:test".to_string())),
        keyring,
        public_keys,
    ));
    let token_handler = replication_token_handler(issuer.clone());

    let token = {
        let (nonce, encrypted) =
            encrypt_path_with_key(schema.id().to_string().as_str(), &keys[0]).expect("encrypt");
        let body = encode_encrypted_payload(&nonce, &encrypted).expect("encode body");
        let req = Request::builder()
            .method(hyper::Method::POST)
            .uri(TOKEN_PATH)
            .body(Body::from(body))
            .expect("request");
        let response = futures::executor::block_on(token_handler.call(req));
        assert_eq!(response.status(), StatusCode::OK);
        let body = futures::executor::block_on(to_bytes(response.into_body())).expect("body");
        let (nonce, encrypted) = decode_encrypted_payload(body).expect("nonce");
        decrypt_token_with_key(&keys[0], &nonce, &encrypted).expect("decrypt")
    };
    assert!(!token.is_empty());
}

#[tokio::test]
async fn cluster_membership_list_requires_post_and_cluster_root_path() {
    let host = pathlink::Link::from_str("/host").expect("host");
    let keyring = KeyringActorResolver::default();
    let public_keys = PublicKeyStore::default();
    let keys = parse_psk_keys(&[
        "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
    ])
    .expect("keys");

    let issuer = Arc::new(ReplicationIssuer::new(
        host,
        keys.clone(),
        Actor::new(tc_value::Value::String("replication:test".to_string())),
        keyring,
        public_keys,
    ));

    let routes = PeerRoutes::new("/lib/example-devco").expect("peer routes");
    let membership = PeerMembership::new(Vec::new());
    let peers_handler = peer_membership_handler(membership, issuer, routes.clone());

    let wrong_method = Request::builder()
        .method(hyper::Method::GET)
        .uri(routes.peers_path())
        .body(Body::empty())
        .expect("request");
    let wrong_method_resp = peers_handler.call(wrong_method).await;
    assert_eq!(wrong_method_resp.status(), StatusCode::METHOD_NOT_ALLOWED);

    let legacy = Request::builder()
        .method(hyper::Method::POST)
        .uri("/host/peers")
        .body(Body::empty())
        .expect("request");
    let legacy_resp = peers_handler.call(legacy).await;
    assert_eq!(legacy_resp.status(), StatusCode::NOT_FOUND);

    let payload = serde_json::json!({
        "peer": "http://10.0.0.2:8702"
    })
    .to_string();
    let (nonce, encrypted) = encrypt_path_with_key(&payload, &keys[0]).expect("encrypt");
    let body = encode_encrypted_payload(&nonce, &encrypted).expect("encode body");
    let req = Request::builder()
        .method(hyper::Method::POST)
        .uri(routes.peers_path())
        .body(Body::from(body))
        .expect("request");
    let response = peers_handler.call(req).await;
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body()).await.expect("body");
    let parsed: JsonValue = serde_json::from_slice(&body).expect("json body");
    assert!(parsed.get("replicas").is_some());
}

#[tokio::test]
async fn kernel_routes_cluster_membership_under_lib_prefix() {
    let host = pathlink::Link::from_str("/host").expect("host");
    let keys = parse_psk_keys(&[
        "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
    ])
    .expect("keys");
    let issuer = Arc::new(ReplicationIssuer::new(
        host,
        keys.clone(),
        Actor::new(tc_value::Value::String("replication:test".to_string())),
        KeyringActorResolver::default(),
        PublicKeyStore::default(),
    ));
    let routes = PeerRoutes::new("/lib/example-devco").expect("routes");
    let peers_handler =
        peer_membership_handler(PeerMembership::new(Vec::new()), issuer, routes.clone());

    let not_found = |_req: Request<Body>| async move {
        hyper::Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .expect("not found")
    };

    let kernel = crate::Kernel::builder()
        .with_lib_handler(not_found)
        .with_lib_put_handler(not_found)
        .with_lib_route_handler(not_found)
        .with_service_handler(not_found)
        .with_kernel_handler(peers_handler)
        .with_health_handler(not_found)
        .finish();

    let payload = serde_json::json!({
        "peer": "http://10.0.0.3:8702"
    })
    .to_string();
    let (nonce, encrypted) = encrypt_path_with_key(&payload, &keys[0]).expect("encrypt");
    let body = encode_encrypted_payload(&nonce, &encrypted).expect("body");

    let req = Request::builder()
        .method(hyper::Method::POST)
        .uri(routes.peers_path())
        .body(Body::from(body))
        .expect("request");

    let dispatch = kernel
        .route_request(
            crate::Method::Post,
            routes.peers_path(),
            req,
            None,
            false,
            None,
            |_txn, _req| {},
        )
        .expect("dispatch");
    let response = match dispatch {
        crate::KernelDispatch::Response(resp) => resp.await,
        _ => panic!("expected response dispatch"),
    };
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn fanout_prunes_stale_peers_within_single_operation() {
    let membership = PeerMembership::new(vec![
        "http://10.0.0.1:8702".to_string(),
        "http://10.0.0.2:8702".to_string(),
        "http://10.0.0.3:8702".to_string(),
    ]);

    let calls: Arc<Mutex<HashMap<String, usize>>> = Arc::new(Mutex::new(HashMap::new()));
    let calls_for_apply = calls.clone();

    let result = super::fanout_peers(&membership, "test operation", move |peer| {
        let calls = calls_for_apply.clone();
        async move {
            let mut calls = calls.lock().expect("calls lock");
            let count = calls.entry(peer.clone()).or_insert(0);
            *count += 1;
            drop(calls);

            if peer == "http://10.0.0.1:8702" {
                Ok(())
            } else {
                Err(tc_error::TCError::bad_gateway("simulated stale peer"))
            }
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "fanout should succeed after pruning stale peers"
    );

    let calls = calls.lock().expect("calls lock");
    assert_eq!(calls.get("http://10.0.0.1:8702"), Some(&1));
    assert_eq!(calls.get("http://10.0.0.2:8702"), Some(&3));
    assert_eq!(calls.get("http://10.0.0.3:8702"), Some(&3));

    assert_eq!(
        membership.active_peers(),
        vec!["http://10.0.0.1:8702".to_string()]
    );
}

#[tokio::test]
async fn fanout_rejects_when_all_peers_are_unreachable() {
    let membership = PeerMembership::new(vec![
        "http://10.0.1.1:8702".to_string(),
        "http://10.0.1.2:8702".to_string(),
    ]);

    let result = super::fanout_peers(&membership, "test operation", |_peer| async move {
        Err(tc_error::TCError::bad_gateway("simulated unreachable peer"))
    })
    .await;

    assert!(
        result.is_err(),
        "fanout must fail when no peer is reachable"
    );
    assert!(membership.active_peers().is_empty());
}
