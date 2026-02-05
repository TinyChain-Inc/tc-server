use std::net::TcpListener;
use std::str::FromStr;
use std::time::Duration;

use hyper::{Body, Client, Request, StatusCode};
use pathlink::Link;
use tc_ir::{Claim, LibrarySchema};
use tc_value::Value;
use tinychain::auth::{Actor, KeyringActorResolver, Token};
use tinychain::http::HttpServer;
use tinychain::kernel::Kernel;
use tinychain::library::{InstallArtifacts, default_library_schema, encode_install_payload_bytes};
use tinychain::library::http::{build_http_library_module, http_library_handlers};
use tinychain::storage::Artifact;
use umask::USER_WRITE;

#[tokio::test]
async fn install_rejects_missing_token() {
    let schema = sample_schema("/lib/example-devco/alpha/0.1.0");
    let (addr, _actor) = start_server().await;
    let payload = install_payload_for_schema(&schema, ir_bytes_for_schema(&schema));
    let response = put_install_payload(addr, None, payload).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn install_rejects_wrong_scope_token() {
    let schema = sample_schema("/lib/example-devco/alpha/0.1.0");
    let wrong_schema = sample_schema("/lib/example-devco/beta/0.1.0");
    let (addr, actor) = start_server().await;
    let token = token_for_schema(&actor, &wrong_schema);
    let payload = install_payload_for_schema(&schema, ir_bytes_for_schema(&schema));
    let response = put_install_payload(addr, Some(token), payload).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn install_rejects_mismatched_manifest() {
    let schema = sample_schema("/lib/example-devco/alpha/0.1.0");
    let mismatched_schema = sample_schema("/lib/example-devco/beta/0.1.0");
    let (addr, actor) = start_server().await;
    let token = token_for_schema(&actor, &schema);
    let payload = install_payload_for_schema(&schema, ir_bytes_for_schema(&mismatched_schema));
    let response = put_install_payload(addr, Some(token), payload).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

fn sample_schema(id: &str) -> LibrarySchema {
    LibrarySchema::new(
        Link::from_str(id).expect("schema link"),
        "0.1.0",
        vec![],
    )
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

fn install_payload_for_schema(
    schema: &LibrarySchema,
    artifact_bytes: Vec<u8>,
) -> Vec<u8> {
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

async fn start_server() -> (std::net::SocketAddr, Actor) {
    let schema = default_library_schema();
    let module = build_http_library_module(schema.clone(), None)
        .await
        .expect("module");
    let handlers = http_library_handlers(&module);

    let host = Link::from_str("/host").expect("host link");
    let actor = Actor::new(Value::from("install-tester"));
    let keyring = KeyringActorResolver::default().with_actor(host, actor.clone());

    let kernel = Kernel::builder()
        .with_host_id("test-install-security")
        .with_http_rpc_gateway()
        .with_rjwt_keyring_token_verifier(keyring.clone())
        .with_library_module(module, handlers)
        .with_service_handler(|_req| async move { hyper::Response::new(Body::empty()) })
        .with_kernel_handler(|_req| async move { hyper::Response::new(Body::empty()) })
        .with_health_handler(|_req| async move { hyper::Response::new(Body::empty()) })
        .finish();

    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    let addr = listener.local_addr().expect("addr");

    let server = HttpServer::new(kernel);
    tokio::spawn(async move {
        let _ = server.serve_listener(listener).await;
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    (addr, actor)
}

fn token_for_schema(actor: &Actor, schema: &LibrarySchema) -> String {
    let host = Link::from_str("/host").expect("host link");
    let claim = Claim::new(schema.id().clone(), USER_WRITE);
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
