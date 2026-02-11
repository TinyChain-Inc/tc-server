use std::collections::HashSet;
use std::env;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use futures::FutureExt;
use hyper::{Body, Request, Response, StatusCode};
use tc_error::{TCError, TCResult};

use pathlink::Link;
use tinychain::auth::{KeyringActorResolver, PublicKeyStore};
use tinychain::http::{
    HttpKernelConfig, HttpServer, build_http_kernel_and_registry_with_config_and_builder,
    host_handler_with_public_keys,
};
use tinychain::kernel::KernelHandler;
use tinychain::replication::{
    ReplicationIssuer, discover_library_paths, export_handler, fetch_library_export,
    parse_psk_keys, parse_psk_list, replication_token_handler, request_replication_token,
};

const DEFAULT_BIND: &str = "0.0.0.0:8702";
const DEFAULT_DATA_DIR: &str = "/tmp/tinychain";
#[cfg(feature = "mdns")]
const SERVICE_TYPE: &str = "_tinychain._tcp.local.";

#[derive(Debug, Parser)]
#[command(name = "tc-server", about = "TinyChain node runtime")]
struct Config {
    #[arg(long, env = "TC_BIND", default_value = DEFAULT_BIND)]
    bind: String,

    #[arg(long, env = "TC_DATA_DIR", default_value = DEFAULT_DATA_DIR)]
    data_dir: PathBuf,

    #[arg(long, env = "TC_HOST_ID", default_value = "tc-server")]
    host_id: String,

    #[arg(long = "peer", env = "TC_PEERS", value_delimiter = ',', action = clap::ArgAction::Append)]
    peers: Vec<String>,

    #[arg(long = "psk", env = "TC_PSK_HEX", value_delimiter = ',', action = clap::ArgAction::Append)]
    psk_keys: Vec<String>,

    #[arg(long, env = "TC_MDNS")]
    mdns: bool,

    #[arg(long = "k8s-dns", env = "TC_K8S_DNS")]
    k8s_dns: Option<String>,

    #[arg(long = "k8s-port", env = "TC_K8S_PORT")]
    k8s_port: Option<u16>,

    #[arg(long = "advertise-ip", env = "TC_ADVERTISE_IP")]
    advertise_ip: Option<IpAddr>,

    #[arg(long, env = "TC_REPLICATE", value_parser = parse_replicate_env, default_value_t = true)]
    replicate: bool,

    #[arg(long = "no-replicate", action = clap::ArgAction::SetTrue)]
    no_replicate: bool,

    #[arg(long = "max-request-bytes", env = "TC_MAX_REQUEST_BYTES", default_value_t = 1 * 1024 * 1024)]
    max_request_bytes: usize,

    #[arg(
        long = "request-ttl-secs",
        env = "TC_REQUEST_TTL_SECS",
        default_value_t = 3
    )]
    request_ttl_secs: u64,
}

impl Config {
    fn parse() -> TCResult<Self> {
        let mut config = <Self as Parser>::parse();

        config.peers = flatten_list(config.peers);
        config.psk_keys = flatten_psk_list(config.psk_keys);

        let psk_value = if config.psk_keys.is_empty() {
            env::var("TC_PSK")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        } else {
            None
        };
        if let Some(value) = psk_value {
            config.psk_keys.push(value);
        }

        if config.no_replicate {
            config.replicate = false;
        }

        Ok(config)
    }

    fn bind_addr(&self) -> TCResult<SocketAddr> {
        SocketAddr::from_str(&self.bind)
            .map_err(|err| TCError::bad_request(format!("invalid bind address: {err}")))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::parse()?;
    let bind = config.bind_addr()?;
    tokio::fs::create_dir_all(&config.data_dir).await?;

    let kernel_config = HttpKernelConfig::default()
        .with_data_dir(config.data_dir.clone())
        .with_initial_schema(tinychain::library::default_library_schema())
        .with_host_id(config.host_id.clone())
        .with_txn_ttl(Duration::from_secs(config.request_ttl_secs))
        .with_max_request_bytes_unauth(config.max_request_bytes);

    let keyring = KeyringActorResolver::default();
    let public_keys = PublicKeyStore::default();
    let keys = parse_psk_keys(&config.psk_keys)?;
    let host = Link::from_str("/host")?;
    let issuer = Arc::new(ReplicationIssuer::new(
        host,
        keys.clone(),
        keyring.clone(),
        public_keys.clone(),
    ));

    let keyring_for_kernel = keyring.clone();
    let issuer_for_kernel = issuer.clone();
    let public_keys_for_kernel = public_keys.clone();
    let (kernel, registry) = build_http_kernel_and_registry_with_config_and_builder(
        kernel_config,
        ok_handler(),
        health_handler(),
        move |registry, builder| {
            let public: Arc<dyn KernelHandler> = Arc::new(host_handler_with_public_keys(
                public_keys_for_kernel.clone(),
            ));
            let token: Arc<dyn KernelHandler> =
                Arc::new(replication_token_handler(issuer_for_kernel.clone()));
            let export: Arc<dyn KernelHandler> = Arc::new(export_handler(registry.clone()));
            let host_handler = combined_host_handler(public, token, export);

            builder
                .with_kernel_handler(host_handler)
                .with_rjwt_keyring_token_verifier(keyring_for_kernel)
        },
    )
    .await?;

    let mut peers = config.peers.clone();

    if let Some(k8s_dns) = &config.k8s_dns {
        let port = config.k8s_port.unwrap_or(bind.port());
        let discovered = discover_k8s_peers(k8s_dns, port).await;
        peers.extend(discovered);
    }

    #[cfg(feature = "mdns")]
    if config.mdns {
        let discovered = discover_mdns_peers(Duration::from_secs(2)).await;
        peers.extend(discovered);
    }

    peers = dedupe_peers(peers);
    peers.retain(|peer| !is_self(peer, bind.ip(), config.advertise_ip, bind.port()));

    if config.replicate && !peers.is_empty() {
        replicate_from_peers(&registry, &peers, issuer.keys()).await;
    }

    #[cfg(feature = "mdns")]
    if let Some(advertise_ip) = if config.mdns {
        advertise_ip(bind, config.advertise_ip)
    } else {
        None
    } {
        match advertise_mdns(advertise_ip, bind.port()).await {
            Ok(()) => {}
            Err(err) => eprintln!("mdns advertise failed: {err}"),
        }
    }

    let server = HttpServer::new_with_limits(kernel, config.max_request_bytes);
    server.serve(bind).await?;
    Ok(())
}

fn ok_handler() -> impl KernelHandler {
    |_req: Request<Body>| {
        async move {
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::empty())
                .expect("ok response")
        }
        .boxed()
    }
}

fn health_handler() -> impl KernelHandler {
    |_req: Request<Body>| {
        async move {
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::from("ok"))
                .expect("health response")
        }
        .boxed()
    }
}

async fn replicate_from_peers(
    registry: &Arc<tinychain::library::LibraryRegistry>,
    peers: &[String],
    keys: &[aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>],
) {
    for peer in peers {
        let library_paths = match discover_library_paths(peer).await {
            Ok(paths) => paths,
            Err(err) => {
                eprintln!("replication from {peer} failed: {err}");
                continue;
            }
        };

        for path in library_paths {
            let token = match request_replication_token(peer, &path, keys).await {
                Ok(token) => token,
                Err(err) => {
                    eprintln!("replication from {peer} failed: {err}");
                    continue;
                }
            };

            match fetch_library_export(peer, &token).await {
                Ok(Some(payload)) => {
                    if let Err(err) = registry.install_payload(payload).await {
                        eprintln!("replication from {peer} failed: {}", err.message());
                    } else {
                        eprintln!("replicated library {path} from {peer}");
                    }
                }
                Ok(None) => eprintln!("peer {peer} has no exportable library for {path}"),
                Err(err) => eprintln!("replication from {peer} failed: {err}"),
            }
        }
    }
}

fn combined_host_handler(
    public: Arc<dyn KernelHandler>,
    token: Arc<dyn KernelHandler>,
    export: Arc<dyn KernelHandler>,
) -> impl KernelHandler {
    move |req: Request<Body>| {
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
        .boxed()
    }
}

fn flatten_list(items: Vec<String>) -> Vec<String> {
    items
        .into_iter()
        .flat_map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(|item| item.to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn flatten_psk_list(items: Vec<String>) -> Vec<String> {
    items
        .into_iter()
        .flat_map(|value| parse_psk_list(&value))
        .collect()
}

fn parse_replicate_env(value: &str) -> Result<bool, String> {
    match value.trim() {
        "" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        "1" | "true" | "yes" | "on" => Ok(true),
        other => Err(format!("invalid TC_REPLICATE value: {other}")),
    }
}

fn dedupe_peers(peers: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    peers
        .into_iter()
        .filter(|peer| seen.insert(peer.clone()))
        .collect()
}

fn is_self(peer: &str, bind_ip: IpAddr, advertise_ip: Option<IpAddr>, port: u16) -> bool {
    let Ok(addr) = peer.parse::<SocketAddr>() else {
        return false;
    };

    let self_ip = if bind_ip.is_unspecified() {
        advertise_ip
    } else {
        Some(bind_ip)
    };

    self_ip.is_some_and(|ip| ip == addr.ip() && port == addr.port())
}

#[cfg(feature = "mdns")]
fn advertise_ip(bind: SocketAddr, override_ip: Option<IpAddr>) -> Option<IpAddr> {
    if let Some(ip) = override_ip {
        return Some(ip);
    }

    if bind.ip().is_unspecified() {
        None
    } else {
        Some(bind.ip())
    }
}

async fn discover_k8s_peers(dns: &str, port: u16) -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(addrs) = (dns, port).to_socket_addrs() {
        for addr in addrs {
            out.push(addr.to_string());
        }
    }
    out
}

#[cfg(feature = "mdns")]
async fn discover_mdns_peers(timeout_duration: Duration) -> Vec<String> {
    use mdns_sd::{ServiceDaemon, ServiceEvent};
    use tokio::time::{Instant, timeout};

    let mut out = Vec::new();
    let daemon = match ServiceDaemon::new() {
        Ok(daemon) => daemon,
        Err(_) => return out,
    };

    let receiver = match daemon.browse(SERVICE_TYPE) {
        Ok(receiver) => receiver,
        Err(_) => return out,
    };

    let deadline = Instant::now() + timeout_duration;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }

        match timeout(remaining, receiver.recv_async()).await {
            Ok(Ok(ServiceEvent::ServiceResolved(info))) => {
                let port = info.get_port();
                for addr in info.get_addresses() {
                    out.push(SocketAddr::new(*addr, port).to_string());
                }
            }
            Ok(Ok(_)) => {}
            Ok(Err(_)) => break,
            Err(_) => break,
        }
    }

    out
}

#[cfg(feature = "mdns")]
async fn advertise_mdns(ip: IpAddr, port: u16) -> TCResult<()> {
    use mdns_sd::{ServiceDaemon, ServiceInfo};
    use std::collections::HashMap;

    let daemon = ServiceDaemon::new().map_err(|err| TCError::internal(err.to_string()))?;
    let hostname = env::var("TC_HOSTNAME")
        .or_else(|_| env::var("HOSTNAME"))
        .unwrap_or_else(|_| "tinychain".to_string());

    let hostname = if hostname.ends_with(".local") {
        hostname
    } else {
        format!("{hostname}.local.")
    };

    let service = ServiceInfo::new(
        SERVICE_TYPE,
        "node",
        &hostname,
        ip,
        port,
        HashMap::<String, String>::default(),
    )
    .map_err(|err| TCError::internal(err.to_string()))?;

    daemon
        .register(service)
        .map_err(|err| TCError::internal(err.to_string()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_psk_list() {
        let keys = parse_psk_list("a, b,,c");
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn dedupes_peers() {
        let peers = dedupe_peers(vec!["1.2.3.4:5".to_string(), "1.2.3.4:5".to_string()]);
        assert_eq!(peers.len(), 1);
    }
}
