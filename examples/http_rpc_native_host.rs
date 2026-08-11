use std::{env, io::Write, net::TcpListener, str::FromStr};

use base64::Engine as _;
use futures::FutureExt;
use pathlink::Link;
use tinychain::auth::{Actor, KeyringActorResolver};
use tinychain::http::{Body, Request, Response, StatusCode, build_http_runtime_with_config};
use tinychain::{HttpKernelConfig, HttpServer};

const DEFAULT_ACTOR_ID: &str = "example-admin";

fn parse_alg(alg: &str) -> Result<rjwt::AlgKind, String> {
    match alg.trim().to_ascii_lowercase().as_str() {
        "falcon512" | "falcon-512" | "fn-dsa-512" => Ok(rjwt::AlgKind::Falcon512),
        "ed25519" | "eddsa" => Ok(rjwt::AlgKind::Ed25519),
        other => Err(format!("unsupported signature algorithm: {other}")),
    }
}

fn ok_handler(_req: Request) -> futures::future::BoxFuture<'static, Response> {
    async move {
        http::Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .expect("ok response")
    }
    .boxed()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut actor_id = DEFAULT_ACTOR_ID.to_string();
    let mut secret_key_b64 = None::<String>;
    let mut alg = rjwt::AlgKind::Falcon512;
    let bind = env::args()
        .skip(1)
        .fold("127.0.0.1:0".to_string(), |current_bind, arg| {
            if let Some(value) = arg.strip_prefix("--bind=") {
                value.to_string()
            } else if let Some(value) = arg.strip_prefix("--actor-id=") {
                actor_id = value.to_string();
                current_bind
            } else if let Some(value) = arg.strip_prefix("--secret-key-b64=") {
                secret_key_b64 = Some(value.to_string());
                current_bind
            } else if let Some(value) = arg.strip_prefix("--alg=") {
                alg = parse_alg(value).expect("unsupported --alg");
                current_bind
            } else {
                current_bind
            }
        });

    let bind_addr = std::net::SocketAddr::from_str(&bind)?;
    let listener = TcpListener::bind(bind_addr)?;
    let addr = listener.local_addr()?;
    let host_link = Link::from_str(&format!("http://{addr}"))?;
    let actor = if let Some(secret_key_b64) = secret_key_b64 {
        let secret_key_bytes = base64::engine::general_purpose::STANDARD.decode(secret_key_b64)?;
        let signing_key = rjwt::SigningKey::from_bytes(alg, &secret_key_bytes)?;
        Actor::with_verifying_key(actor_id, signing_key.verifying_key())
    } else {
        Actor::new_falcon512(actor_id)?
    };
    let keyring = KeyringActorResolver::default().with_actor(host_link, actor);

    let runtime = build_http_runtime_with_config(
        HttpKernelConfig::default().with_host_id("tc-http-rpc-native-host"),
        ok_handler,
        ok_handler,
        |_| ok_handler,
        |_, builder| builder.with_rjwt_keyring_token_verifier(keyring),
    )
    .await?;

    let example = serde_json::json!({
        "/lib/example-devco/example/0.1.0": {
            "hello": {
                "/state/scalar/op/get": [
                    "name",
                    [
                        ["template", "Hello, {{name}}!"],
                        ["result", {"$template/render": {"name": {"$name": []}}}]
                    ]
                ]
            }
        }
    });
    let package = tinychain::library::decode_install_request_bytes(&serde_json::to_vec(&example)?)
        .map_err(|err| std::io::Error::other(err.message().to_string()))?;
    runtime
        .registry
        .install_compiled_package(package)
        .await
        .map_err(|err| std::io::Error::other(err.message().to_string()))?;

    println!("{addr}");
    eprintln!("serving native kernel and HTTP router");
    std::io::stdout().flush().ok();

    HttpServer::new(runtime.kernel, runtime.router)
        .serve_listener(listener)
        .await?;
    Ok(())
}
