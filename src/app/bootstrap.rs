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

pub(crate) struct BootstrapContext<'a> {
    pub(crate) registry: &'a Arc<tinychain::library::LibraryRegistry>,
    pub(crate) membership: &'a PeerMembership,
    pub(crate) seed_peers: &'a [String],
    pub(crate) keys: &'a [aes_gcm_siv::Key<Aes256GcmSiv>],
    pub(crate) routes: &'a PeerRoutes,
    pub(crate) replicate: bool,
    pub(crate) self_peer: Option<String>,
    pub(crate) issuer: &'a ReplicationIssuer,
}

pub(crate) async fn run_bootstrap_with_retries(
    context: BootstrapContext<'_>,
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
            &context,
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
    context: &BootstrapContext<'_>,
    target_paths: Option<&HashSet<String>>,
) -> BootstrapStepOutcome {
    let mut hard_failure = false;
    let mut peers = context.membership.snapshot_active_peers();
    let mut seen = peers.iter().cloned().collect::<HashSet<_>>();
    for peer in context.seed_peers {
        if seen.insert(peer.clone()) {
            peers.push(peer.clone());
        }
    }

    peers.sort();

    let replication = if context.replicate && !peers.is_empty() {
        let report = replicate_from_peers_targeted(
            context.registry,
            &peers,
            context.keys,
            target_paths,
        )
        .await;
        hard_failure |= report.has_hard_failures();

        if !report.is_clean() {
            eprintln!("replication bootstrap completed with partial failures: {report:?}");
        }

        report
    } else {
        ReplicationReport::default()
    };

    if let Some(self_peer) = context.self_peer.clone() {
        match context.issuer.self_identity(self_peer) {
            Ok(identity) => {
                let report =
                    announce_self_to_cluster(
                        context.membership,
                        &identity,
                        context.routes,
                        context.keys,
                        context.issuer,
                    )
                    .await;

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
