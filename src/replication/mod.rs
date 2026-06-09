mod client;
mod crypto;
mod gateway;
mod handler;
mod http_util;
mod issuer;
mod membership;
mod peers;

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::library::{
    LibraryRegistry, decode_authorize_and_stage_install, stage_install_error_response,
};
use aes_gcm_siv::{Aes256GcmSiv, Key};
use futures::future::{FutureExt, join_all};
use hyper::body::to_bytes;
use hyper::{Body, Request, StatusCode};
use tc_error::{TCError, TCResult};

use self::http_util::{bad_request, empty_response, text_response};

pub const LIBRARY_EXPORT_PATH: &str = crate::uri::HOST_LIBRARY_EXPORT;
pub const PEERS_PATH_SUFFIX: &str = "/_cluster/peers";
pub const PEERS_JOIN_PATH_SUFFIX: &str = "/_cluster/peers/join";
pub const PEERS_LEAVE_PATH_SUFFIX: &str = "/_cluster/peers/leave";
pub const PEERS_HEARTBEAT_PATH_SUFFIX: &str = "/_cluster/peers/heartbeat";
pub const FORWARDED_HEADER: &str = "x-tc-replicated";
const TOKEN_PATH: &str = "/";
const REPLICATION_TTL: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug)]
pub struct ReplicationPolicy {
    pub max_fanout_attempts: usize,
    pub membership_failure_threshold: u8,
}

impl Default for ReplicationPolicy {
    fn default() -> Self {
        Self {
            max_fanout_attempts: 3,
            membership_failure_threshold: 3,
        }
    }
}

pub use client::{
    HttpClusterGateway, PeerClusterListing, discover_library_paths, fetch_compiled_library_package,
    fetch_library_listing, fetch_library_schema, heartbeat_peer, leave_peer_cluster,
    list_peer_cluster, normalize_peer, register_with_peer, request_replication_token,
};
pub use gateway::ClusterGateway;
pub use handler::{export_handler, replication_token_handler};
pub use issuer::{ReplicationIssuer, parse_psk_keys, parse_psk_list};
pub use membership::{PeerDescriptor, PeerIdentity, PeerMembership};
pub use peers::peer_membership_handler;

pub fn is_peer_membership_path(path: &str) -> bool {
    path.ends_with(PEERS_PATH_SUFFIX)
        || path.ends_with(PEERS_JOIN_PATH_SUFFIX)
        || path.ends_with(PEERS_LEAVE_PATH_SUFFIX)
        || path.ends_with(PEERS_HEARTBEAT_PATH_SUFFIX)
}

#[derive(Clone, Debug)]
pub struct PeerRoutes {
    cluster_root: String,
    peers: String,
    join: String,
    leave: String,
    heartbeat: String,
}

impl PeerRoutes {
    pub fn new(cluster_root: &str) -> TCResult<Self> {
        let cluster_root = normalize_cluster_root(cluster_root)?;
        Ok(Self {
            peers: format!("{cluster_root}{PEERS_PATH_SUFFIX}"),
            join: format!("{cluster_root}{PEERS_JOIN_PATH_SUFFIX}"),
            leave: format!("{cluster_root}{PEERS_LEAVE_PATH_SUFFIX}"),
            heartbeat: format!("{cluster_root}{PEERS_HEARTBEAT_PATH_SUFFIX}"),
            cluster_root,
        })
    }

    pub fn cluster_root(&self) -> &str {
        &self.cluster_root
    }

    pub fn peers_path(&self) -> &str {
        &self.peers
    }

    pub fn join_path(&self) -> &str {
        &self.join
    }

    pub fn leave_path(&self) -> &str {
        &self.leave
    }

    pub fn heartbeat_path(&self) -> &str {
        &self.heartbeat
    }

    pub fn matches(&self, path: &str) -> bool {
        path == self.peers || path == self.join || path == self.leave || path == self.heartbeat
    }
}

fn normalize_cluster_root(value: &str) -> TCResult<String> {
    let root = value.trim().trim_end_matches('/');
    if !root.starts_with("/lib/") {
        return Err(TCError::bad_request(format!(
            "invalid cluster root {root}: expected /lib/<publisher>"
        )));
    }

    if root == "/lib" || root == "/lib/" {
        return Err(TCError::bad_request(
            "invalid cluster root /lib: expected /lib/<publisher>",
        ));
    }

    Ok(root.to_string())
}

struct ParticipantFanoutError {
    delivered: Vec<String>,
    err: TCError,
}

#[must_use]
#[derive(Clone, Debug, Default)]
pub struct ReplicationReport {
    pub installed: Vec<String>,
    pub unavailable: Vec<String>,
    pub skipped: Vec<String>,
    pub failed: Vec<String>,
}

impl ReplicationReport {
    pub fn is_clean(&self) -> bool {
        self.unavailable.is_empty() && self.skipped.is_empty() && self.failed.is_empty()
    }

    fn record_installed(&mut self, peer: &str, path: &str) {
        self.installed.push(format!("{peer} {path}"));
    }

    fn record_unavailable(&mut self, peer: &str, err: impl std::fmt::Display) {
        self.unavailable.push(format!("{peer}: {err}"));
    }

    fn record_skipped(&mut self, peer: &str, path: &str) {
        self.skipped.push(format!("{peer} {path}"));
    }

    fn record_failed(&mut self, peer: &str, path: &str, err: impl std::fmt::Display) {
        self.failed.push(format!("{peer} {path}: {err}"));
    }
}

#[must_use]
#[derive(Clone, Debug, Default)]
pub struct ClusterJoinReport {
    pub contacted: Vec<String>,
    pub failed: Vec<String>,
    pub discovered: Vec<String>,
}

pub async fn replicate_from_peers(
    registry: &Arc<LibraryRegistry>,
    peers: &[String],
    keys: &[Key<Aes256GcmSiv>],
) -> ReplicationReport {
    replicate_from_peers_with_gateway(registry, peers, keys, &HttpClusterGateway).await
}

pub async fn replicate_from_peers_with_gateway<G>(
    registry: &Arc<LibraryRegistry>,
    peers: &[String],
    keys: &[Key<Aes256GcmSiv>],
    gateway: &G,
) -> ReplicationReport
where
    G: ClusterGateway,
{
    let mut report = ReplicationReport::default();

    for peer in peers {
        let library_paths = match gateway.discover_library_paths(peer).await {
            Ok(paths) => paths,
            Err(err) => {
                report.record_unavailable(peer, err);
                continue;
            }
        };

        for path in library_paths {
            let token = match gateway.request_replication_token(peer, &path, keys).await {
                Ok(token) => token,
                Err(err) => {
                    report.record_failed(peer, &path, err);
                    continue;
                }
            };

            match gateway.fetch_compiled_library_package(peer, &token).await {
                Ok(Some(payload)) => {
                    if let Err(err) = registry.install_compiled_package(payload).await {
                        report.record_failed(peer, &path, err.message());
                    } else {
                        report.record_installed(peer, &path);
                    }
                }
                Ok(None) => report.record_skipped(peer, &path),
                Err(err) => report.record_failed(peer, &path, err),
            }
        }
    }

    report
}

pub async fn announce_self_to_cluster(
    membership: &PeerMembership,
    self_identity: &PeerIdentity,
    routes: &PeerRoutes,
    keys: &[Key<Aes256GcmSiv>],
    issuer: &ReplicationIssuer,
) -> ClusterJoinReport {
    announce_self_to_cluster_with_gateway(
        membership,
        self_identity,
        routes,
        keys,
        issuer,
        &HttpClusterGateway,
    )
    .await
}

pub async fn announce_self_to_cluster_with_gateway<G>(
    membership: &PeerMembership,
    self_identity: &PeerIdentity,
    routes: &PeerRoutes,
    keys: &[Key<Aes256GcmSiv>],
    issuer: &ReplicationIssuer,
    gateway: &G,
) -> ClusterJoinReport
where
    G: ClusterGateway,
{
    let mut report = ClusterJoinReport::default();
    let mut pending = membership.active_peers();
    let mut visited = std::collections::HashSet::new();
    while let Some(seed) = pending.pop() {
        if seed == self_identity.peer || !visited.insert(seed.clone()) {
            continue;
        }

        match gateway
            .register_with_peer(&seed, self_identity, routes, keys)
            .await
        {
            Ok(discovered) => {
                report.contacted.push(seed.clone());
                membership.mark_success(&seed);
                for identity in discovered.identities {
                    if identity.peer == self_identity.peer {
                        continue;
                    }

                    if let Err(err) = issuer.register_peer_identity(&identity) {
                        report.failed.push(format!("{seed}: {err}"));
                        continue;
                    }

                    if membership.upsert_identity(identity.clone()) {
                        report.discovered.push(identity.peer.clone());
                        pending.push(identity.peer);
                    }
                }

                for peer in discovered.peers {
                    if peer == self_identity.peer {
                        continue;
                    }
                    if membership.upsert_active(peer.clone()) {
                        report.discovered.push(peer.clone());
                        pending.push(peer);
                    }
                }
            }
            Err(err) => {
                membership.mark_failure(&seed);
                report.failed.push(format!("{seed}: {err}"));
            }
        }
    }

    report
}

pub fn live_replicating_install_put_handler(
    registry: Arc<LibraryRegistry>,
    membership: PeerMembership,
    keys: Vec<Key<Aes256GcmSiv>>,
) -> impl crate::KernelHandler {
    move |req: Request<Body>| {
        let registry = Arc::clone(&registry);
        let membership = membership.clone();
        let keys = keys.clone();
        async move {
            let forwarded = req
                .headers()
                .get(FORWARDED_HEADER)
                .and_then(|value| value.to_str().ok())
                == Some("1");

            let txn = match req.extensions().get::<crate::txn::TxnHandle>().cloned() {
                Some(txn) => txn,
                None => {
                    return text_response(StatusCode::UNAUTHORIZED, "missing transaction context");
                }
            };

            let body = match to_bytes(req.into_body()).await {
                Ok(body) => body,
                Err(err) => return bad_request(err.to_string()),
            };

            let schema_path = match decode_authorize_and_stage_install(&registry, &txn, &body).await
            {
                Ok(schema_path) => schema_path,
                Err(err) => return stage_install_error_response(err),
            };

            if !forwarded {
                let install_bytes = body.to_vec();
                match forward_install_to_peers(
                    &membership,
                    &txn,
                    &schema_path,
                    install_bytes,
                    &keys,
                )
                .await
                {
                    Ok(participants) => {
                        registry.record_replication_participants(txn.id(), participants)
                    }
                    Err(err) => {
                        registry.discard_txn(txn.id());
                        return text_response(StatusCode::BAD_GATEWAY, err.to_string());
                    }
                }
            }

            empty_response(StatusCode::NO_CONTENT)
        }
        .boxed()
    }
}

pub async fn forward_install_to_peers(
    membership: &PeerMembership,
    txn: &crate::txn::TxnHandle,
    schema_path: &str,
    install_compiled_package: Vec<u8>,
    keys: &[Key<Aes256GcmSiv>],
) -> TCResult<Vec<String>> {
    forward_install_to_peers_with_gateway(
        membership,
        txn,
        schema_path,
        install_compiled_package,
        keys,
        &HttpClusterGateway,
    )
    .await
}

pub async fn forward_install_to_peers_with_gateway<G>(
    membership: &PeerMembership,
    txn: &crate::txn::TxnHandle,
    schema_path: &str,
    install_compiled_package: Vec<u8>,
    keys: &[Key<Aes256GcmSiv>],
    gateway: &G,
) -> TCResult<Vec<String>>
where
    G: ClusterGateway,
{
    let token = txn
        .raw_token()
        .ok_or_else(|| tc_error::TCError::unauthorized("missing bearer token"))?
        .to_string();
    let txn_id = txn.id();

    fanout_peers(
        membership,
        "install payload",
        ReplicationPolicy::default(),
        |peer| {
            let token = token.clone();
            let payload = install_compiled_package.clone();
            let gateway = gateway;
            async move {
                gateway
                    .request_replication_token(&peer, schema_path, keys)
                    .await
                    .map_err(|err| {
                        tc_error::TCError::bad_gateway(format!(
                            "failed to request replication token from {peer}: {err}"
                        ))
                    })?;

                gateway
                    .push_install_compiled_package(&peer, &token, txn_id, payload)
                    .await
            }
        },
    )
    .await
}

pub fn live_replicating_finalize_hook(
    registry: Arc<LibraryRegistry>,
) -> impl Fn(crate::txn::TxnHandle, bool) -> futures::future::BoxFuture<'static, TCResult<()>>
+ Send
+ Sync
+ 'static {
    move |txn: crate::txn::TxnHandle, commit: bool| {
        let registry = Arc::clone(&registry);
        async move {
            let Some(participants) = registry.replication_participants(txn.id()) else {
                return Ok(());
            };

            match forward_finalize_to_participants_progress(
                &participants,
                &txn,
                commit,
                &HttpClusterGateway,
            )
            .await
            {
                Ok(_) => Ok(()),
                Err(err) => {
                    registry.retain_unfinished_replication_participants(txn.id(), &err.delivered);
                    Err(err.err)
                }
            }
        }
        .boxed()
    }
}

pub async fn forward_finalize_to_peers(
    membership: &PeerMembership,
    txn: &crate::txn::TxnHandle,
    commit: bool,
) -> TCResult<()> {
    let participants = membership.active_peers();
    forward_finalize_to_participants(&participants, txn, commit, &HttpClusterGateway).await
}

pub async fn forward_finalize_to_peers_with_gateway<G>(
    membership: &PeerMembership,
    txn: &crate::txn::TxnHandle,
    commit: bool,
    gateway: &G,
) -> TCResult<()>
where
    G: ClusterGateway,
{
    let participants = membership.active_peers();
    forward_finalize_to_participants(&participants, txn, commit, gateway).await
}

async fn forward_finalize_to_participants<G>(
    participants: &[String],
    txn: &crate::txn::TxnHandle,
    commit: bool,
    gateway: &G,
) -> TCResult<()>
where
    G: ClusterGateway,
{
    let _ = forward_finalize_to_participants_progress(participants, txn, commit, gateway)
        .await
        .map_err(|err| err.err)?;
    Ok(())
}

async fn forward_finalize_to_participants_progress<G>(
    participants: &[String],
    txn: &crate::txn::TxnHandle,
    commit: bool,
    gateway: &G,
) -> Result<Vec<String>, ParticipantFanoutError>
where
    G: ClusterGateway,
{
    let token = txn
        .raw_token()
        .ok_or_else(|| ParticipantFanoutError {
            delivered: Vec::new(),
            err: tc_error::TCError::unauthorized("missing bearer token"),
        })?
        .to_string();
    let txn_id = txn.id();

    fanout_participants(
        participants,
        "finalize transaction",
        ReplicationPolicy::default(),
        |peer| {
            let token = token.clone();
            let gateway = gateway;
            async move {
                gateway
                    .finalize_install_txn(&peer, &token, txn_id, commit)
                    .await
            }
        },
    )
    .await
}

async fn fanout_peers<F, Fut>(
    membership: &PeerMembership,
    operation: &str,
    policy: ReplicationPolicy,
    apply: F,
) -> TCResult<Vec<String>>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = TCResult<()>>,
{
    let mut delivered = HashSet::new();
    let mut first_error = None;
    let mut had_targets = false;

    for _ in 1..=policy.max_fanout_attempts {
        let targets = membership
            .active_peers()
            .into_iter()
            .filter(|peer| !delivered.contains(peer))
            .collect::<Vec<_>>();

        if targets.is_empty() {
            break;
        }
        had_targets = true;

        let results = join_all(targets.into_iter().map(|peer| {
            let fut = apply(peer.clone());
            async move { (peer, fut.await) }
        }))
        .await;

        for (peer, result) in results {
            match result {
                Ok(()) => {
                    membership.mark_success(&peer);
                    delivered.insert(peer);
                }
                Err(err) => {
                    membership.mark_failure_with_membership_threshold(
                        &peer,
                        policy.membership_failure_threshold,
                    );
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }
    }

    let unresolved = membership
        .active_peers()
        .into_iter()
        .filter(|peer| !delivered.contains(peer))
        .collect::<Vec<_>>();

    if unresolved.is_empty() {
        if had_targets && delivered.is_empty() {
            Err(first_error.unwrap_or_else(|| {
                TCError::bad_gateway(format!("failed to {operation}: no reachable peers"))
            }))
        } else {
            let mut delivered = delivered.into_iter().collect::<Vec<_>>();
            delivered.sort();
            Ok(delivered)
        }
    } else {
        Err(first_error.unwrap_or_else(|| {
            TCError::bad_gateway(format!(
                "failed to {operation} on unresolved peers: {}",
                unresolved.join(", ")
            ))
        }))
    }
}

async fn fanout_participants<F, Fut>(
    participants: &[String],
    operation: &str,
    policy: ReplicationPolicy,
    apply: F,
) -> Result<Vec<String>, ParticipantFanoutError>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = TCResult<()>>,
{
    let mut delivered = HashSet::new();
    let mut first_error = None;

    for _ in 1..=policy.max_fanout_attempts {
        let targets = participants
            .iter()
            .filter(|peer| !delivered.contains(*peer))
            .cloned()
            .collect::<Vec<_>>();

        if targets.is_empty() {
            let mut delivered = delivered.into_iter().collect::<Vec<_>>();
            delivered.sort();
            return Ok(delivered);
        }

        let results = join_all(targets.into_iter().map(|peer| {
            let fut = apply(peer.clone());
            async move { (peer, fut.await) }
        }))
        .await;

        for (peer, result) in results {
            match result {
                Ok(()) => {
                    delivered.insert(peer);
                }
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }
    }

    let unresolved = participants
        .iter()
        .filter(|peer| !delivered.contains(*peer))
        .cloned()
        .collect::<Vec<_>>();

    if unresolved.is_empty() {
        let mut delivered = delivered.into_iter().collect::<Vec<_>>();
        delivered.sort();
        Ok(delivered)
    } else {
        let mut delivered = delivered.into_iter().collect::<Vec<_>>();
        delivered.sort();
        Err(ParticipantFanoutError {
            delivered,
            err: first_error.unwrap_or_else(|| {
                TCError::bad_gateway(format!(
                    "failed to {operation} on transaction participants: {}",
                    unresolved.join(", ")
                ))
            }),
        })
    }
}
