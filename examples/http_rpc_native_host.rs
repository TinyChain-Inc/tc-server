use std::{env, io::Write, net::TcpListener, str::FromStr};

use base64::Engine as _;
use pathlink::Link;
use tinychain::auth::{Actor, KeyringActorResolver};
use tinychain::{HostLimits, HostStorage, HttpServer};

const DEFAULT_ACTOR_ID: &str = "example-admin";

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
                alg = value.parse().expect("unsupported --alg");
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
        Actor::with_signing_key(actor_id, signing_key)
    } else {
        Actor::new_falcon512(actor_id)?
    };
    let keyring = KeyringActorResolver::default();
    keyring.insert(host_link.clone(), actor.clone())?;
    let data_dir =
        env::var_os("TC_DATA_DIR").ok_or("TC_DATA_DIR must name the host application data root")?;
    let workspace = env::var_os("TC_WORKSPACE")
        .ok_or("TC_WORKSPACE must name the host transaction workspace root")?;
    let storage = HostStorage::new(&HostLimits::default().storage);
    let application_roots = storage.application_roots(data_dir).await?;
    let workspace = storage.workspace(workspace)?;

    let verifier = tinychain::auth::RjwtTokenVerifier::new(std::sync::Arc::new(keyring.clone()));
    let protocol = tinychain::ProtocolAuthority::new(host_link, actor);
    let bootstrap = std::sync::Arc::new(tinychain::replication::ReplicationIssuer::local(
        &protocol,
        keyring.clone(),
    )?);
    let limits = HostLimits::default();
    let services = tinychain::HostServices {
        application_roots,
        replication: std::sync::Arc::new(tinychain::replication::LocalClusterGateway),
        rpc: std::sync::Arc::new(tinychain::LocalRpcGateway),
        resources: tinychain::HostResources::new(limits.clone())?,
        protocol,
        verifier: std::sync::Arc::new(verifier),
        actors: keyring,
        bootstrap,
        bootstrap_required: false,
    };
    let kernel = tinychain::Kernel::new(services, workspace, limits.transaction_ttl).await?;

    println!("{addr}");
    eprintln!("serving native kernel and HTTP router");
    std::io::stdout().flush().ok();

    HttpServer::new(kernel).serve_listener(listener).await?;
    Ok(())
}
