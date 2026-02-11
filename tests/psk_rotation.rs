use std::net::TcpListener;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use pathlink::Link;
use serde_json::json;
use tc_ir::LibrarySchema;
use tinychain::auth::{KeyringActorResolver, PublicKeyStore};
use tinychain::http::{HttpServer, host_handler_with_public_keys};
use tinychain::kernel::Kernel;
use tinychain::library::http::{build_http_library_module, http_library_handlers};
use tinychain::replication::{
    ReplicationIssuer, export_handler, fetch_library_export, parse_psk_keys,
    replication_token_handler, request_replication_token,
};
use tinychain::storage::{Artifact, LibraryStore};

#[tokio::test]
async fn rotates_psk_keys_for_export_integration() {
    let dir = std::env::temp_dir().join(format!(
        "tc-psk-rotation-it-{}",
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
    let artifact_bytes = serde_json::to_vec(&json!({
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
    .expect("ir manifest");

    let keys = parse_psk_keys(&vec![
        "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
        "0000000000000000000000000000000000000000000000000000000000000002".to_string(),
    ])
    .expect("keys");

    let store = LibraryStore::open(dir.clone()).await.expect("store");
    let store = store.for_schema(&schema).await.expect("store");
    store
        .persist_artifact(
            &schema,
            &Artifact {
                path: schema.id().to_string(),
                content_type: tinychain::ir::IR_ARTIFACT_CONTENT_TYPE.to_string(),
                bytes: artifact_bytes.clone(),
            },
        )
        .await
        .expect("persist ir");

    let addr = start_server(schema.clone(), Some(dir.clone()), keys.clone()).await;
    let peer = format!("http://{addr}");

    let token = request_replication_token(&peer, &schema.id().to_string(), &keys)
        .await
        .expect("token");

    let payload = fetch_library_export(&peer, &token)
        .await
        .expect("export")
        .expect("payload");
    assert_eq!(payload.schema.id(), schema.id());

    let keys_new_only = parse_psk_keys(&vec![
        "0000000000000000000000000000000000000000000000000000000000000002".to_string(),
    ])
    .expect("keys");
    let keys_old_only = parse_psk_keys(&vec![
        "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
    ])
    .expect("keys");
    let addr_new = start_server(schema.clone(), Some(dir), keys_new_only.clone()).await;
    let peer_new = format!("http://{addr_new}");

    let token_old =
        request_replication_token(&peer_new, &schema.id().to_string(), &keys_old_only).await;
    assert!(token_old.is_err());

    let token_new = request_replication_token(&peer_new, &schema.id().to_string(), &keys_new_only)
        .await
        .expect("token");
    let payload = fetch_library_export(&peer_new, &token_new)
        .await
        .expect("export")
        .expect("payload");
    assert_eq!(payload.schema.id(), schema.id());
}

async fn start_server(
    schema: LibrarySchema,
    storage_root: Option<std::path::PathBuf>,
    keys: Vec<aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>>,
) -> std::net::SocketAddr {
    let module = build_http_library_module(schema.clone(), storage_root)
        .await
        .expect("module");
    let handlers = http_library_handlers(&module);

    let keyring = KeyringActorResolver::default();
    let public_keys = PublicKeyStore::default();
    let issuer = Arc::new(ReplicationIssuer::new(
        Link::from_str("/host").expect("host"),
        keys,
        keyring.clone(),
        public_keys.clone(),
    ));

    let host_handler = combine_host_handlers(
        host_handler_with_public_keys(public_keys),
        replication_token_handler(issuer.clone()),
        export_handler(module.clone()),
    );

    let kernel = Kernel::builder()
        .with_host_id("test-replication")
        .with_http_rpc_gateway()
        .with_rjwt_keyring_token_verifier(keyring)
        .with_library_module(module, handlers)
        .with_service_handler(|_req| async move { hyper::Response::new(hyper::Body::empty()) })
        .with_kernel_handler(host_handler)
        .with_health_handler(|_req| async move { hyper::Response::new(hyper::Body::empty()) })
        .finish();

    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    let addr = listener.local_addr().expect("addr");

    let server = HttpServer::new(kernel);
    tokio::spawn(async move {
        let _ = server.serve_listener(listener).await;
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    addr
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
