use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use aes_gcm_siv::Aes256GcmSiv;
use tinychain::replication::{
    PeerMembership, PeerRoutes, ReplicationIssuer, ReplicationReport, announce_self_to_cluster,
    replicate_from_peers_targeted,
};

#[derive(Clone, Debug, Default)]
pub(crate) struct BootstrapOutcome {
    pub(crate) completed: bool,
    pub(crate) attempts: u8,
    pub(crate) progress: bool,
}

#[derive(Clone, Debug, Default)]
struct BootstrapStepOutcome {
    hard_failure: bool,
    replication: ReplicationReport,
}

pub(crate) async fn run_bootstrap_with_retries(
    registry: &Arc<tinychain::library::LibraryRegistry>,
    membership: &PeerMembership,
    seed_peers: &[String],
    keys: &[aes_gcm_siv::Key<Aes256GcmSiv>],
    routes: &PeerRoutes,
    replicate: bool,
    self_peer: Option<String>,
    issuer: &ReplicationIssuer,
    max_attempts: u8,
    retry_delay: Duration,
) -> BootstrapOutcome {
    if max_attempts == 0 {
        return BootstrapOutcome {
            completed: true,
            attempts: 0,
            progress: false,
        };
    }

    let mut overall_progress = false;
    let mut unresolved_paths: Option<HashSet<String>> = None;

    for attempt in 1..=max_attempts {
        let previous_unresolved = unresolved_paths.as_ref().map(HashSet::len);
        let step = run_bootstrap_step(
            registry,
            membership,
            seed_peers,
            keys,
            routes,
            replicate,
            self_peer.clone(),
            issuer,
            unresolved_paths.as_ref(),
        )
        .await;

        let mut next_unresolved = unresolved_paths
            .take()
            .unwrap_or_else(|| step.replication.discovered_paths());
        next_unresolved.extend(step.replication.failed_paths());

        for resolved in step.replication.resolved_paths() {
            next_unresolved.remove(&resolved);
        }

        let unresolved_count = next_unresolved.len();
        let pending_reduced = previous_unresolved.is_some_and(|count| unresolved_count < count);
        let attempt_progress = step.replication.made_install_progress() || pending_reduced;

        overall_progress |= attempt_progress;
        unresolved_paths = Some(next_unresolved);

        let completed = !step.hard_failure && unresolved_count == 0;

        if completed {
            return BootstrapOutcome {
                completed: true,
                attempts: attempt,
                progress: overall_progress,
            };
        }

        if !attempt_progress || attempt == max_attempts {
            return BootstrapOutcome {
                completed: false,
                attempts: attempt,
                progress: overall_progress,
            };
        }

        tokio::time::sleep(retry_delay).await;
    }

    BootstrapOutcome {
        completed: false,
        attempts: max_attempts,
        progress: overall_progress,
    }
}

async fn run_bootstrap_step(
    registry: &Arc<tinychain::library::LibraryRegistry>,
    membership: &PeerMembership,
    seed_peers: &[String],
    keys: &[aes_gcm_siv::Key<Aes256GcmSiv>],
    routes: &PeerRoutes,
    replicate: bool,
    self_peer: Option<String>,
    issuer: &ReplicationIssuer,
    target_paths: Option<&HashSet<String>>,
) -> BootstrapStepOutcome {
    let mut hard_failure = false;
    let mut peers = membership.snapshot_active_peers();
    let mut seen = peers.iter().cloned().collect::<HashSet<_>>();
    for peer in seed_peers {
        if seen.insert(peer.clone()) {
            peers.push(peer.clone());
        }
    }

    peers.sort();

    let replication = if replicate && !peers.is_empty() {
        let report = replicate_from_peers_targeted(registry, &peers, keys, target_paths).await;
        hard_failure |= report.has_hard_failures();

        if !report.is_clean() {
            eprintln!("replication bootstrap completed with partial failures: {report:?}");
        }

        report
    } else {
        ReplicationReport::default()
    };

    if let Some(self_peer) = self_peer {
        match issuer.self_identity(self_peer) {
            Ok(identity) => {
                let report =
                    announce_self_to_cluster(membership, &identity, routes, keys, issuer).await;

                if !report.failed.is_empty() {
                    eprintln!("cluster join completed with partial failures: {report:?}");
                }
            }
            Err(err) => {
                hard_failure = true;
                eprintln!("failed to build replication identity: {err}");
            }
        }
    }

    BootstrapStepOutcome {
        hard_failure,
        replication,
    }
}
