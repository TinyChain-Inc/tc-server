use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use pathlink::Link;

use tinychain::auth::{Actor, KeyringActorResolver, PublicKeyStore, RjwtTokenVerifier};
use tinychain::http::{HttpKernelConfig, HttpRuntime, HttpServer, build_http_runtime_with_config};
use tinychain::replication::{
    HttpClusterGateway, PeerMembership, PeerRoutes, ReplicationIssuer, announce_self_to_cluster,
    parse_psk_keys, peer_membership_handler,
};

mod config;
mod discovery;
mod trusted_installers;

use config::Config;
#[cfg(feature = "mdns")]
use discovery::{advertise_ip, advertise_mdns, discover_mdns_peers};
use discovery::{dedupe_peers, discover_k8s_peers, is_self, self_peer};
use trusted_installers::{
    TrustedInstallerPolicy, TrustedInstallerTokenVerifier, bootstrap_trusted_installers,
    load_trusted_installers,
};

#[tokio::main]
pub(crate) async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::parse()?;
    let bind = config.bind_addr()?;
    tokio::fs::create_dir_all(&config.data_dir).await?;
    tokio::fs::create_dir_all(&config.workspace).await?;
    let limits = tinychain::HostLimits::default();
    let storage = tinychain::storage::HostStorage::new(&limits.storage);
    let workspace = storage.workspace(&config.workspace)?;
    let application_roots = storage.application_roots(&config.data_dir).await?;

    let peer_routes = PeerRoutes::new(&config.cluster_root)?;
    let trusted_installers = load_trusted_installers(&config)?;
    let installer_policy =
        TrustedInstallerPolicy::from_installers(&trusted_installers, peer_routes.cluster_root())?;

    let public_keys = PublicKeyStore::default();
    let host = Link::from_str("/host")?;

    let keyring = bootstrap_trusted_installers(
        KeyringActorResolver::default(),
        &public_keys,
        &trusted_installers,
    )?;
    let keys = parse_psk_keys(&config.psk_keys)?;
    let (protocol_authority, replication_actor) = workspace
        .load_or_create_protocol_authority(&config.host_id, host.clone())
        .await?;
    let replication_actor_id = replication_actor.id().clone();
    public_keys.insert_actor(&replication_actor);
    let keyring = keyring.with_actor(
        host.clone(),
        Actor::with_verifying_key(
            replication_actor_id.clone(),
            replication_actor.verifying_key(),
        ),
    );
    let issuer = Arc::new(ReplicationIssuer::new(
        host,
        keys.clone(),
        replication_actor,
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
    let replication_gateway = HttpClusterGateway::new(membership.clone());

    let verifier = TrustedInstallerTokenVerifier::new(
        RjwtTokenVerifier::new(Arc::new(keyring.clone())),
        installer_policy,
        membership.clone(),
        replication_actor_id,
    );
    let kernel_config = HttpKernelConfig::new(
        application_roots,
        workspace,
        protocol_authority,
        Arc::new(verifier),
        public_keys,
        replication_gateway.clone(),
        tinychain::HttpRpcGateway::new(),
    )
    .with_txn_ttl(Duration::from_secs(config.request_ttl_secs))
    .with_max_request_bytes(config.max_request_bytes);
    let runtime = build_http_runtime_with_config(
        kernel_config,
        peer_membership_handler(membership.clone(), issuer.clone(), peer_routes.clone()),
    )
    .await?;

    let HttpRuntime { kernel, router } = runtime;
    if let Some(self_peer) = self_peer(bind, config.advertise_ip) {
        let membership = membership.clone();
        let routes = peer_routes.clone();
        let issuer = issuer.clone();
        let gateway = replication_gateway.clone();
        tokio::spawn(async move {
            let identity = match issuer.self_identity(self_peer) {
                Ok(identity) => identity,
                Err(err) => {
                    eprintln!("failed to build replication identity: {err}");
                    return;
                }
            };
            let report =
                announce_self_to_cluster(&membership, &identity, &routes, &keys, &issuer, &gateway)
                    .await;
            if !report.failed.is_empty() {
                eprintln!("cluster join completed with partial failures: {report:?}");
            }
        });
    }

    if let Some(k8s_dns) = config.k8s_dns.clone() {
        let membership_for_discovery = membership.clone();
        let discovery_port = config.k8s_port.unwrap_or(bind.port());
        let bind_ip = bind.ip();
        let advertise_ip = config.advertise_ip;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;

                let mut discovered = discover_k8s_peers(&k8s_dns, discovery_port).await;
                discovered = dedupe_peers(discovered);
                discovered.retain(|peer| !is_self(peer, bind_ip, advertise_ip, discovery_port));
                let resolved: HashSet<String> = discovered.iter().cloned().collect();
                for known in membership_for_discovery.snapshot_active_peers() {
                    if resolved.contains(&known) {
                        membership_for_discovery.record_discovery_success(&known);
                    } else {
                        membership_for_discovery.record_discovery_failure(&known);
                    }
                }

                for peer in discovered {
                    match tinychain::replication::normalize_peer(&peer) {
                        Ok(peer) => {
                            membership_for_discovery.upsert_active(peer);
                        }
                        Err(err) => {
                            eprintln!("k8s peer discovery ignored invalid peer {peer}: {err}");
                        }
                    }
                }
            }
        });
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

    HttpServer::new(kernel, router).serve(bind).await?;
    Ok(())
}

#[cfg(test)]
mod tests;
