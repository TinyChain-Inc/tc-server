use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use clap::Parser;
use futures::FutureExt;
use hyper::{Body, Request, Response, StatusCode};
use serde::Deserialize;
use tc_error::{TCError, TCResult};
use tc_value::Value;

use pathlink::Link;
use tinychain::auth::{
    KeyringActorResolver, PublicKeyStore, RjwtTokenVerifier, TokenContext, TokenVerifier,
};
use tinychain::http::{
    HttpKernelConfig, HttpServer, build_http_kernel_and_registry_with_config_and_builder,
    host_handler_with_public_keys,
};
use tinychain::kernel::KernelHandler;
use tinychain::replication::{
    PEERS_HEARTBEAT_PATH, PEERS_JOIN_PATH, PEERS_LEAVE_PATH, PEERS_PATH, PeerMembership,
    ReplicationIssuer, announce_self_to_cluster, export_handler,
    live_replicating_install_put_handler, parse_psk_keys, parse_psk_list, peer_membership_handler,
    replicate_from_peers, replication_token_handler,
};

const DEFAULT_BIND: &str = "0.0.0.0:8702";
const DEFAULT_DATA_DIR: &str = "/tmp/tinychain";
#[cfg(feature = "mdns")]
const SERVICE_TYPE: &str = "_tinychain._tcp.local.";

#[derive(Clone, Debug, Deserialize)]
struct TrustedInstaller {
    host: String,
    actor_id: String,
    public_key_b64: String,
    #[serde(default, alias = "allowed_lib_roots")]
    allowed_lib_prefixes: Vec<String>,
}

#[derive(Clone, Default)]
struct TrustedInstallerPolicy {
    by_actor: HashMap<(String, String), Vec<String>>,
}

impl TrustedInstallerPolicy {
    fn from_installers(installers: &[TrustedInstaller]) -> TCResult<Self> {
        let mut by_actor = HashMap::new();

        for installer in installers {
            let host = Link::from_str(&installer.host).map_err(|err| {
                TCError::bad_request(format!("invalid trusted installer host: {err}"))
            })?;
            let actor_id = installer.actor_id.trim();
            if actor_id.is_empty() {
                return Err(TCError::bad_request(
                    "trusted installer actor_id must not be empty",
                ));
            }

            let mut prefixes = Vec::new();
            for prefix in &installer.allowed_lib_prefixes {
                prefixes.push(normalize_lib_prefix(prefix)?);
            }

            if prefixes.is_empty() {
                return Err(TCError::bad_request(format!(
                    "trusted installer {actor_id} must define at least one allowed_lib_prefix"
                )));
            }

            by_actor.insert((host.to_string(), actor_id.to_string()), prefixes);
        }

        Ok(Self { by_actor })
    }

    fn validate_context(&self, ctx: &TokenContext) -> Result<(), tinychain::txn::TxnError> {
        if self.by_actor.is_empty() {
            return Ok(());
        }

        for (host, actor_id, claim) in &ctx.claims {
            let path = claim.link.to_string();

            if path.starts_with("/txn/") {
                continue;
            }

            // Replication tokens are host-internal and issued by the kernel.
            if host == "/host" {
                continue;
            }

            let Some(prefixes) = self.by_actor.get(&(host.clone(), actor_id.clone())) else {
                return Err(tinychain::txn::TxnError::Unauthorized);
            };

            if !path.starts_with("/lib/") {
                return Err(tinychain::txn::TxnError::Unauthorized);
            }

            if !prefixes
                .iter()
                .any(|prefix| path_matches_prefix(&path, prefix))
            {
                return Err(tinychain::txn::TxnError::Unauthorized);
            }
        }

        Ok(())
    }
}

#[derive(Clone)]
struct TrustedInstallerTokenVerifier {
    inner: RjwtTokenVerifier,
    policy: TrustedInstallerPolicy,
}

impl TrustedInstallerTokenVerifier {
    fn new(inner: RjwtTokenVerifier, policy: TrustedInstallerPolicy) -> Self {
        Self { inner, policy }
    }
}

impl TokenVerifier for TrustedInstallerTokenVerifier {
    fn verify(
        &self,
        bearer_token: String,
    ) -> futures::future::BoxFuture<'static, Result<TokenContext, tinychain::txn::TxnError>> {
        let inner = self.inner.clone();
        let policy = self.policy.clone();
        async move {
            let ctx = inner.verify(bearer_token).await?;
            policy.validate_context(&ctx)?;
            Ok(ctx)
        }
        .boxed()
    }

    fn grant(
        &self,
        token: TokenContext,
        claim: tc_ir::Claim,
    ) -> futures::future::BoxFuture<'static, Result<TokenContext, tinychain::txn::TxnError>> {
        self.inner.grant(token, claim)
    }
}

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

    #[arg(long = "trusted-installers-json", env = "TC_TRUSTED_INSTALLERS_JSON")]
    trusted_installers_json: Option<String>,

    #[arg(
        long = "trusted-installers-json-path",
        env = "TC_TRUSTED_INSTALLERS_JSON_PATH"
    )]
    trusted_installers_json_path: Option<PathBuf>,
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

        if config.trusted_installers_json.is_some() && config.trusted_installers_json_path.is_some()
        {
            return Err(TCError::bad_request(
                "set only one of TC_TRUSTED_INSTALLERS_JSON or TC_TRUSTED_INSTALLERS_JSON_PATH",
            ));
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

    let trusted_installers = load_trusted_installers(&config)?;
    let installer_policy = TrustedInstallerPolicy::from_installers(&trusted_installers)?;

    let public_keys = PublicKeyStore::default();
    let keyring = bootstrap_trusted_installers(
        KeyringActorResolver::default(),
        &public_keys,
        &trusted_installers,
    )?;

    let keys = parse_psk_keys(&config.psk_keys)?;
    let host = Link::from_str("/host")?;
    let issuer = Arc::new(ReplicationIssuer::new(
        host,
        keys.clone(),
        keyring.clone(),
        public_keys.clone(),
    ));

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
    peers = peers
        .into_iter()
        .filter_map(|peer| tinychain::replication::normalize_peer(&peer).ok())
        .collect();

    let membership = PeerMembership::new(peers.clone());

    let keyring_for_kernel = keyring.clone();
    let issuer_for_kernel = issuer.clone();
    let public_keys_for_kernel = public_keys.clone();
    let installer_policy_for_kernel = installer_policy.clone();
    let membership_for_kernel = membership.clone();
    let keys_for_kernel = keys.clone();
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
            let peers_handler: Arc<dyn KernelHandler> = Arc::new(peer_membership_handler(
                membership_for_kernel.clone(),
                issuer_for_kernel.clone(),
            ));
            let host_handler = combined_host_handler(public, token, export, peers_handler);
            let verifier = TrustedInstallerTokenVerifier::new(
                RjwtTokenVerifier::new(Arc::new(keyring_for_kernel)),
                installer_policy_for_kernel,
            );
            let live_put = live_replicating_install_put_handler(
                registry.clone(),
                membership_for_kernel.clone(),
                keys_for_kernel.clone(),
            );

            builder
                .with_kernel_handler(host_handler)
                .with_lib_put_handler(live_put)
                .with_token_verifier(verifier)
        },
    )
    .await?;

    let bootstrap_registry = Arc::clone(&registry);
    let bootstrap_membership = membership.clone();
    let bootstrap_peers = peers.clone();
    let bootstrap_keys = keys.clone();
    let bootstrap_replicate = config.replicate;
    let bootstrap_self_peer = self_peer(bind, config.advertise_ip);
    tokio::spawn(async move {
        if bootstrap_replicate && !bootstrap_peers.is_empty() {
            replicate_from_peers(&bootstrap_registry, &bootstrap_peers, &bootstrap_keys).await;
        }

        if let Some(self_peer) = bootstrap_self_peer {
            announce_self_to_cluster(&bootstrap_membership, &self_peer, &bootstrap_keys).await;
        }
    });

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

fn combined_host_handler(
    public: Arc<dyn KernelHandler>,
    token: Arc<dyn KernelHandler>,
    export: Arc<dyn KernelHandler>,
    peers: Arc<dyn KernelHandler>,
) -> impl KernelHandler {
    move |req: Request<Body>| {
        let path = req.uri().path().to_string();
        let public = Arc::clone(&public);
        let token = Arc::clone(&token);
        let export = Arc::clone(&export);
        let peers = Arc::clone(&peers);
        async move {
            match path.as_str() {
                "/" => token.call(req).await,
                "/host/library/export" => export.call(req).await,
                PEERS_PATH | PEERS_JOIN_PATH | PEERS_LEAVE_PATH | PEERS_HEARTBEAT_PATH => {
                    peers.call(req).await
                }
                _ => public.call(req).await,
            }
        }
        .boxed()
    }
}

fn self_peer(bind: SocketAddr, advertise_ip: Option<IpAddr>) -> Option<String> {
    let ip = if bind.ip().is_unspecified() {
        advertise_ip
    } else {
        Some(bind.ip())
    }?;

    Some(format!("http://{}:{}", ip, bind.port()))
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

fn load_trusted_installers(config: &Config) -> TCResult<Vec<TrustedInstaller>> {
    let raw = match (
        config.trusted_installers_json.as_ref(),
        config.trusted_installers_json_path.as_ref(),
    ) {
        (Some(json), None) => Some(json.clone()),
        (None, Some(path)) => Some(fs::read_to_string(path).map_err(|err| {
            TCError::bad_request(format!("failed to read trusted installers file: {err}"))
        })?),
        (None, None) => None,
        (Some(_), Some(_)) => {
            return Err(TCError::bad_request(
                "set only one of TC_TRUSTED_INSTALLERS_JSON or TC_TRUSTED_INSTALLERS_JSON_PATH",
            ));
        }
    };

    let Some(raw) = raw else {
        return Ok(Vec::new());
    };

    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    serde_json::from_str(trimmed).map_err(|err| {
        TCError::bad_request(format!(
            "invalid trusted installers JSON (expected array of installer entries): {err}"
        ))
    })
}

fn bootstrap_trusted_installers(
    mut keyring: KeyringActorResolver,
    public_keys: &PublicKeyStore,
    installers: &[TrustedInstaller],
) -> TCResult<KeyringActorResolver> {
    for installer in installers {
        let host = Link::from_str(&installer.host).map_err(|err| {
            TCError::bad_request(format!("invalid trusted installer host: {err}"))
        })?;

        let actor_id = installer.actor_id.trim();
        if actor_id.is_empty() {
            return Err(TCError::bad_request(
                "trusted installer actor_id must not be empty",
            ));
        }

        let key_bytes = base64::engine::general_purpose::STANDARD
            .decode(installer.public_key_b64.trim())
            .map_err(|err| {
                TCError::bad_request(format!("invalid installer public_key_b64: {err}"))
            })?;

        let verifying_key = rjwt::VerifyingKey::try_from(key_bytes.as_slice()).map_err(|err| {
            TCError::bad_request(format!("invalid installer public key bytes: {err}"))
        })?;

        let actor = tinychain::auth::Actor::with_public_key(
            Value::from(actor_id.to_string()),
            verifying_key,
        );

        keyring = keyring.with_actor(host, actor);
        public_keys.insert(actor_id.to_string(), verifying_key);
    }

    Ok(keyring)
}

fn normalize_lib_prefix(prefix: &str) -> TCResult<String> {
    let trimmed = prefix.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(TCError::bad_request(
            "trusted installer prefix must not be empty",
        ));
    }

    if !trimmed.starts_with("/lib/") {
        return Err(TCError::bad_request(format!(
            "trusted installer prefix must start with /lib/: {trimmed}"
        )));
    }

    Ok(trimmed.to_string())
}

fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    if path == prefix {
        return true;
    }

    path.strip_prefix(prefix)
        .is_some_and(|rest| rest.starts_with('/'))
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
