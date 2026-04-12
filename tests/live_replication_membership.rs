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
use tinychain::library::http::{build_http_library_module, http_library_handlers};
use tinychain::library::{InstallArtifacts, encode_install_payload_bytes};
use tinychain::replication::{
    ReplicationIssuer, discover_library_paths, export_handler, fetch_library_export,
    parse_psk_keys, replication_token_handler, request_replication_token,
};
use tinychain::storage::Artifact;
use umask::USER_WRITE;

#[tokio::test]
async fn replication_export_tracks_new_installs() {
    let server = start_server(
        "replication-export",
        Some(unique_temp_dir("replication-export")),
    )
    .await;

    let schema_a = sample_schema("/lib/example-devco/live-forward/0.1.0");
    install_with_write_token(&server, &schema_a).await;

    let payload_a = fetch_payload_for_schema(&server, &schema_a).await;
    assert_eq!(payload_a.schema.id(), schema_a.id());

    let schema_b = sample_schema("/lib/example-devco/live-forward/0.2.0");
    install_with_write_token(&server, &schema_b).await;

    let payload_b = fetch_payload_for_schema(&server, &schema_b).await;
    assert_eq!(payload_b.schema.id(), schema_b.id());
}

#[tokio::test]
async fn discover_library_paths_lists_installed_libraries() {
    let server = start_server(
        "replication-discovery",
        Some(unique_temp_dir("replication-discovery")),
    )
    .await;

    let schema_a = sample_schema("/lib/example-devco/discovery/0.1.0");
    let schema_b = sample_schema("/lib/example-devco/discovery/0.2.0");

    install_with_write_token(&server, &schema_a).await;
    install_with_write_token(&server, &schema_b).await;

    let mut paths = discover_library_paths(&format!("http://{}", server.addr))
        .await
        .expect("discover paths");
    paths.sort();

    assert!(paths.iter().any(|path| path == &schema_a.id().to_string()));
    assert!(paths.iter().any(|path| path == &schema_b.id().to_string()));
}

struct RunningServer {
    addr: std::net::SocketAddr,
    actor: Actor,
    keys: Vec<aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>>,
}

fn unique_temp_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "tc-live-repl-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create dir");
    dir
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

async fn start_server(label: &str, storage_root: Option<std::path::PathBuf>) -> RunningServer {
    let schema = tinychain::library::default_library_schema();
    let module = build_http_library_module(schema, storage_root)
        .await
        .expect("module");
    let handlers = http_library_handlers(&module);

    let host = Link::from_str("/host").expect("host link");
    let actor = Actor::new(Value::from(format!("installer-{label}")));
    let keyring = KeyringActorResolver::default().with_actor(host.clone(), actor.clone());
    let public_keys = PublicKeyStore::default();
    let keys = shared_replication_keys();
    let issuer = Arc::new(ReplicationIssuer::new(
        host,
        keys.clone(),
        keyring.clone(),
        public_keys.clone(),
    ));

    let kernel = Kernel::builder()
        .with_host_id(format!("live-replication-{label}"))
        .with_http_rpc_gateway()
        .with_rjwt_keyring_token_verifier(keyring)
        .with_library_module(module.clone(), handlers)
        .with_service_handler(|_req| async move { hyper::Response::new(Body::empty()) })
        .with_kernel_handler(combine_host_handlers(
            host_handler_with_public_keys(public_keys),
            replication_token_handler(issuer.clone()),
            export_handler(module),
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

    RunningServer { addr, actor, keys }
}

async fn install_with_write_token(server: &RunningServer, schema: &LibrarySchema) {
    let token = token_for_schema(&server.actor, schema, USER_WRITE);
    let payload = install_payload_for_schema(schema, ir_bytes_for_schema(schema));
    let response = put_install_payload(server.addr, Some(token), payload).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

async fn fetch_payload_for_schema(
    server: &RunningServer,
    schema: &LibrarySchema,
) -> tinychain::library::InstallArtifacts {
    let peer = format!("http://{}", server.addr);
    let token = request_replication_token(&peer, &schema.id().to_string(), &server.keys)
        .await
        .expect("replication token");

    fetch_library_export(&peer, &token)
        .await
        .expect("export request")
        .expect("export payload")
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
