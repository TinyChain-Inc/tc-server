use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pathlink::Link;

use tinychain::auth::{Actor, KeyringActorResolver, PublicKeyStore, RjwtTokenVerifier};
use tinychain::http::{
    HttpKernelConfig, HttpRuntime, HttpServer, build_http_runtime_with_config,
    host_handler_with_public_keys,
};
use tinychain::replication::{
    HttpClusterGateway, PeerMembership, PeerRoutes, ReplicationIssuer, export_handler,
    live_replicating_finalize_hook, parse_psk_keys, peer_membership_handler,
    replication_token_handler,
};

mod bootstrap;
mod config;
mod discovery;
mod handlers;
mod trusted_installers;

use bootstrap::{BootstrapContext, run_bootstrap_with_retries};
use config::{BootstrapReadinessMode, Config};
#[cfg(feature = "mdns")]
use discovery::{advertise_ip, advertise_mdns, discover_mdns_peers};
use discovery::{dedupe_peers, discover_k8s_peers, is_self, self_peer};
use handlers::{combined_host_handler, health_handler, ok_handler};
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
    let library_store = storage.library_store(&config.data_dir).await?;

    let kernel_config = HttpKernelConfig::default()
        .with_library_store(library_store)
        .with_workspace(workspace)
        .with_initial_schema(tinychain::library::default_library_schema())
        .with_host_id(config.host_id.clone())
        .with_txn_ttl(Duration::from_secs(config.request_ttl_secs))
        .with_max_request_bytes(config.max_request_bytes);

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
    let local_host_actor = tinychain::auth::Actor::new_falcon512(config.host_id.clone())
        .expect("generate Falcon-512 actor");
    public_keys.insert_actor(&local_host_actor);
    let keyring = keyring.with_actor(host.clone(), local_host_actor.clone());

    let keys = parse_psk_keys(&config.psk_keys)?;
    let replication_actor_id = format!("replication:{}", config.host_id);
    let replication_actor =
        Actor::new_falcon512(replication_actor_id.clone()).expect("generate Falcon-512 actor");
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
    let replication_gateway = HttpClusterGateway::new();

    let keyring_for_kernel = keyring.clone();
    let installer_policy_for_kernel = installer_policy.clone();
    let membership_for_kernel = membership.clone();
    let replication_actor_id_for_kernel = replication_actor_id.clone();
    let bootstrap_ready = Arc::new(AtomicBool::new(
        config.bootstrap_readiness == BootstrapReadinessMode::Lenient,
    ));
    let bootstrap_ready_for_kernel = Arc::clone(&bootstrap_ready);
    let finalize_gateway = replication_gateway.clone();
    let runtime = build_http_runtime_with_config(
        kernel_config,
        ok_handler(),
        health_handler(bootstrap_ready_for_kernel),
        |registry| {
            combined_host_handler(
                Arc::new(host_handler_with_public_keys(public_keys.clone())),
                Arc::new(replication_token_handler(issuer.clone())),
                Arc::new(export_handler(registry)),
                Arc::new(peer_membership_handler(
                    membership.clone(),
                    issuer.clone(),
                    peer_routes.clone(),
                )),
            )
        },
        move |registry, builder| {
            let verifier = TrustedInstallerTokenVerifier::new(
                RjwtTokenVerifier::new(Arc::new(keyring_for_kernel)),
                installer_policy_for_kernel,
                membership_for_kernel.clone(),
                replication_actor_id_for_kernel.clone(),
            );
            let finalize_hook =
                live_replicating_finalize_hook(registry.clone(), finalize_gateway.clone());

            builder
                .with_protocol_actor(
                    Link::from_str("/host").expect("host link"),
                    local_host_actor,
                )
                .with_txn_finalize_hook(finalize_hook)
                .with_token_verifier(verifier)
        },
    )
    .await?;

    let HttpRuntime {
        kernel,
        router,
        registry,
    } = runtime;
    let bootstrap_registry = Arc::clone(&registry);
    let bootstrap_membership = membership.clone();
    let bootstrap_peers = peers.clone();
    let bootstrap_keys = keys.clone();
    let bootstrap_routes = peer_routes.clone();
    let bootstrap_replicate = config.replicate;
    let bootstrap_max_attempts = config.bootstrap_max_attempts;
    let bootstrap_retry_delay = Duration::from_secs(config.bootstrap_retry_delay_secs);
    let bootstrap_self_peer = self_peer(bind, config.advertise_ip);
    let bootstrap_issuer = issuer.clone();
    let bootstrap_gateway = replication_gateway.clone();
    let bootstrap_readiness_mode = config.bootstrap_readiness;
    let bootstrap_ready_signal = Arc::clone(&bootstrap_ready);
    tokio::spawn(async move {
        let bootstrap_context = BootstrapContext {
            registry: &bootstrap_registry,
            membership: &bootstrap_membership,
            seed_peers: &bootstrap_peers,
            keys: &bootstrap_keys,
            routes: &bootstrap_routes,
            replicate: bootstrap_replicate,
            self_peer: bootstrap_self_peer,
            issuer: &bootstrap_issuer,
            gateway: &bootstrap_gateway,
        };

        let outcome = run_bootstrap_with_retries(
            bootstrap_context,
            bootstrap_max_attempts,
            bootstrap_retry_delay,
        )
        .await;

        eprintln!(
            "bootstrap attempts={} progress={} completed={}",
            outcome.attempts, outcome.progress, outcome.completed
        );

        if !outcome.completed {
            eprintln!("bootstrap ended without full convergence: {outcome:?}");
        }

        if bootstrap_readiness_mode == BootstrapReadinessMode::Strict {
            bootstrap_ready_signal.store(outcome.completed, Ordering::SeqCst);
        }
    });

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
