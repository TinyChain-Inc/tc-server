use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use pathlink::Link;

use tinychain::auth::{Actor, KeyringActorResolver, RjwtTokenVerifier};
use tinychain::http::HttpServer;
use tinychain::replication::{ReplicationIssuer, parse_psk_keys};

mod config;
mod discovery;
mod trusted_installers;

use config::Config;
#[cfg(feature = "mdns")]
use discovery::{MdnsService, advertise_ip};
use discovery::{dedupe_peers, discover_k8s_peers, is_self, self_peer};
use trusted_installers::{
    TrustedInstallerPolicy, TrustedInstallerTokenVerifier, bootstrap_trusted_installers,
    load_trusted_installers,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if !cfg!(panic = "abort") {
        return Err("tc-server requires panic=abort; rebuild with the application profile".into());
    }

    let config = Config::parse()?;
    #[cfg(unix)]
    let shutdown_signal = {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate())?;
        let mut interrupt = signal(SignalKind::interrupt())?;
        async move {
            tokio::select! {
                _ = terminate.recv() => {},
                _ = interrupt.recv() => {},
            }
        }
    };

    #[cfg(not(unix))]
    let shutdown_signal = async {
        if let Err(cause) = tokio::signal::ctrl_c().await {
            // A failed signal handler must still drain active work before exit.
            eprintln!("signal handler failed: {cause}");
        }
    };

    let stop_bootstrap = tokio_util::sync::CancellationToken::new();
    let mut bootstrap_task = None;
    let bind = config.bind_addr()?;
    let mut limits = tinychain::HostLimits {
        transaction_ttl: Duration::from_secs(config.request_ttl_secs),
        ..tinychain::HostLimits::default()
    };
    limits.ingress.request_body_bytes = config.max_request_bytes;

    let trusted_installers = load_trusted_installers(&config)?;
    let installer_policy = TrustedInstallerPolicy::from_installers(&trusted_installers)?;

    let storage = tinychain::storage::HostStorage::new(&limits.storage);
    let workspace = storage.workspace(&config.workspace)?;
    let application_roots = storage.application_roots(&config.data_dir).await?;

    let host = Link::from_str("/host")?;

    let keyring =
        bootstrap_trusted_installers(KeyringActorResolver::default(), &trusted_installers)?;
    let keys = parse_psk_keys(&config.psk_keys)?;
    let (protocol_authority, replication_actor) = workspace
        .load_or_create_protocol_authority(&config.host_id, host.clone())
        .await?;
    let replication_actor_id = replication_actor.id().clone();
    keyring.insert(
        host.clone(),
        Actor::with_verifying_key(
            replication_actor_id.clone(),
            replication_actor.verifying_key(),
        ),
    )?;
    let issuer = Arc::new(ReplicationIssuer::new(
        Arc::new(protocol_authority.clone()),
        keys.clone(),
        keyring.clone(),
    )?);

    let mut peers = config.peers.clone();

    #[cfg(feature = "mdns")]
    let mut mdns = if config.mdns {
        Some(MdnsService::new()?)
    } else {
        None
    };

    if let Some(k8s_dns) = &config.k8s_dns {
        let port = config.k8s_port.unwrap_or(bind.port());
        let discovered = discover_k8s_peers(k8s_dns, port).await;
        peers.extend(discovered);
    }

    #[cfg(feature = "mdns")]
    if let Some(mdns) = &mdns {
        peers.extend(mdns.discover(Duration::from_secs(2)).await?);
    }

    peers = dedupe_peers(peers);
    peers.retain(|peer| !is_self(peer, bind.ip(), config.advertise_ip, bind.port()));
    peers = peers
        .into_iter()
        .map(|peer| tinychain::replication::normalize_peer(&peer))
        .collect::<tc_error::TCResult<_>>()?;

    let gateway = tinychain::HttpGateway::new();

    let verifier = TrustedInstallerTokenVerifier::new(
        RjwtTokenVerifier::new(Arc::new(keyring.clone())),
        installer_policy,
    );

    let resources = tinychain::HostResources::new(limits)?;
    let services = tinychain::HostServices {
        application_roots,
        replication: Arc::new(gateway.clone()),
        rpc: Arc::new(gateway),
        resources,
        protocol: protocol_authority,
        verifier: Arc::new(verifier),
        actors: keyring,
        bootstrap: issuer,
        bootstrap_required: !peers.is_empty(),
    };
    let listener = std::net::TcpListener::bind(bind)?;
    listener.set_nonblocking(true)?;
    let bound = listener.local_addr()?;
    let self_endpoint = if peers.is_empty() {
        None
    } else {
        Some(self_peer(bound, config.advertise_ip).ok_or_else(|| {
            tc_error::TCError::bad_request(
                "peer bootstrap requires a concrete bind or advertised IP",
            )
        })?)
    };

    let kernel = tinychain::Kernel::new(services, workspace).await?;
    if let Some(self_endpoint) = self_endpoint {
        let bootstrap = kernel.clone();
        let stopping = stop_bootstrap.clone();
        bootstrap_task = Some(tokio::spawn(async move {
            let self_endpoint = tinychain::replication::normalize_peer(&self_endpoint)
                .expect("validated self endpoint");
            let mut pending = std::collections::VecDeque::from(peers);
            let mut attempted = std::collections::BTreeSet::new();
            let mut delay = Duration::from_millis(250);
            loop {
                if stopping.is_cancelled() {
                    return;
                }

                let mut failed = Vec::new();
                let mut succeeded = false;
                while let Some(seed) = pending.pop_front() {
                    if stopping.is_cancelled() {
                        return;
                    }

                    if !attempted.insert(seed.clone()) {
                        continue;
                    }
                    match bootstrap.bootstrap_seed(&seed, self_endpoint.clone()).await {
                        Ok(discovered) => {
                            succeeded = true;
                            pending.extend(
                                discovered
                                    .into_iter()
                                    .filter(|peer| peer != &self_endpoint)
                                    .filter(|peer| !attempted.contains(peer)),
                            );
                        }
                        Err(error) => {
                            eprintln!("seed bootstrap from {seed} failed: {error}");
                            failed.push(seed);
                        }
                    }
                }

                if succeeded {
                    return;
                }

                tokio::select! {
                    _ = stopping.cancelled() => return,
                    _ = tokio::time::sleep(delay) => {},
                }

                delay = (delay * 2).min(Duration::from_secs(10));
                attempted.clear();
                pending.extend(failed);
            }
        }));
    }

    #[cfg(feature = "mdns")]
    if let (Some(mdns), Some(advertise_ip)) = (&mut mdns, advertise_ip(bound, config.advertise_ip))
    {
        match mdns.advertise(config.host_id.as_str(), advertise_ip, bound.port()) {
            Ok(()) => {}
            Err(err) => eprintln!("mdns advertise failed: {err}"),
        }
    }

    let stopping = kernel.clone();
    let stop_retries = stop_bootstrap.clone();
    let result = HttpServer::new(kernel.clone())
        .serve_listener_with_shutdown(listener, async move {
            shutdown_signal.await;
            stopping.begin_shutdown();
            stop_retries.cancel();
            #[cfg(feature = "mdns")]
            drop(mdns);
        })
        .await;

    stop_bootstrap.cancel();
    if let Some(task) = bootstrap_task {
        task.await?;
    }

    kernel.shutdown().await;
    result?;
    Ok(())
}

#[cfg(test)]
#[path = "../tests/support/main_tests.rs"]
mod main_tests;
