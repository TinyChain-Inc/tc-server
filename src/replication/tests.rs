use super::*;
use crate::KernelHandler;
use crate::auth::{KeyringActorResolver, PublicKeyStore};
use crate::library::http::build_http_library_module;
use crate::replication::crypto::{
    decode_encrypted_payload, decrypt_token_with_key, encode_encrypted_payload,
    encrypt_path_with_key,
};
use crate::storage::{LibraryStore, load_library_root};
use hyper::body::to_bytes;
use hyper::{Body, Request, StatusCode};
use pathlink::Link;
use std::str::FromStr;
use std::sync::Arc;
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
    let keys = parse_psk_keys(&vec![
        "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
        "0000000000000000000000000000000000000000000000000000000000000002".to_string(),
    ])
    .expect("keys");

    let issuer = Arc::new(ReplicationIssuer::new(
        host,
        keys.clone(),
        keyring,
        public_keys,
    ));
    let token_handler = replication_token_handler(issuer.clone());

    let token = {
        let (nonce, encrypted) =
            encrypt_path_with_key(schema.id().to_string().as_str(), &keys[0]).expect("encrypt");
        let body = encode_encrypted_payload(&nonce, &encrypted).expect("encode body");
        let req = Request::builder()
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
