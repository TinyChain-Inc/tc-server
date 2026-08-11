mod client;
mod crypto;
mod gateway;
mod handler;
mod http_util;
mod issuer;
mod membership;
mod peers;

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::library::{LibraryRegistry, StageInstallError, decode_authorize_and_stage_install};
use crate::txn::ParticipantSet;
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
pub struct ParticipantFanoutPolicy {
    pub max_attempts: usize,
}

impl Default for ParticipantFanoutPolicy {
    fn default() -> Self {
        Self { max_attempts: 3 }
    }
}

pub use client::{HttpClusterGateway, PeerClusterListing, normalize_peer};
pub use gateway::ClusterGateway;
pub use handler::{export_handler, replication_token_handler};
pub use issuer::{ReplicationIssuer, parse_psk_keys, parse_psk_list};
pub use membership::{PeerDescriptor, PeerIdentity, PeerMembership};
pub use peers::peer_membership_handler;

fn stage_install_error_response(error: StageInstallError) -> hyper::Response<Body> {
    let (status, message) = match error {
        StageInstallError::Unauthorized(message) => (StatusCode::UNAUTHORIZED, message),
        StageInstallError::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
        StageInstallError::Internal(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
    };
    hyper::Response::builder()
        .status(status)
        .body(Body::from(message))
        .expect("replication install error response")
}

pub fn is_supported_replicated_path(path: &str) -> bool {
    path.starts_with("/lib/") || path.starts_with("/service/")
}

pub fn normalize_replicated_prefix(prefix: &str) -> TCResult<String> {
    let trimmed = prefix.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(TCError::bad_request(
            "trusted installer prefix must not be empty",
        ));
    }

    if !is_supported_replicated_path(trimmed) {
        return Err(TCError::bad_request(format!(
            "trusted installer prefix must start with /lib/ or /service/: {trimmed}"
        )));
    }

    Ok(trimmed.to_string())
}

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
    if !root.starts_with("/lib/") && !root.starts_with("/service/") {
        return Err(TCError::bad_request(format!(
            "invalid cluster root {root}: expected /lib/<publisher> or /service/<publisher>"
        )));
    }

    if root == "/lib" || root == "/lib/" || root == "/service" || root == "/service/" {
        return Err(TCError::bad_request(
            "invalid cluster root: expected /lib/<publisher> or /service/<publisher>",
        ));
    }

    Ok(root.to_string())
}

struct ParticipantFanoutError {
    delivered: ParticipantSet<String>,
    err: TCError,
}

#[must_use]
#[derive(Clone, Debug, Default)]
pub struct ReplicationReport {
    pub installed: Vec<String>,
    pub unavailable: Vec<String>,
    pub skipped: Vec<String>,
    pub failed: Vec<String>,
    discovered_paths: HashSet<String>,
    installed_paths: HashSet<String>,
    skipped_paths: HashSet<String>,
    failed_paths: HashSet<String>,
}

impl ReplicationReport {
    pub fn is_clean(&self) -> bool {
        self.unavailable.is_empty() && self.skipped.is_empty() && self.failed.is_empty()
    }

    fn record_installed(&mut self, peer: &str, path: &str) {
        self.installed.push(format!("{peer} {path}"));
        self.installed_paths.insert(path.to_string());
    }

    fn record_unavailable(&mut self, peer: &str, err: impl std::fmt::Display) {
        self.unavailable.push(format!("{peer}: {err}"));
    }

    fn record_skipped(&mut self, peer: &str, path: &str) {
        self.skipped.push(format!("{peer} {path}"));
        self.skipped_paths.insert(path.to_string());
    }

    fn record_failed(&mut self, peer: &str, path: &str, err: impl std::fmt::Display) {
        self.failed.push(format!("{peer} {path}: {err}"));
        self.failed_paths.insert(path.to_string());
    }

    fn record_discovered_path(&mut self, path: &str) {
        self.discovered_paths.insert(path.to_string());
    }

    pub fn discovered_paths(&self) -> HashSet<String> {
        self.discovered_paths.clone()
    }

    pub fn resolved_paths(&self) -> HashSet<String> {
        self.installed_paths
            .union(&self.skipped_paths)
            .cloned()
            .collect()
    }

    pub fn failed_paths(&self) -> HashSet<String> {
        self.failed_paths.clone()
    }

    pub fn has_hard_failures(&self) -> bool {
        !self.failed_paths.is_empty()
    }

    pub fn made_install_progress(&self) -> bool {
        !self.installed_paths.is_empty()
    }
}

#[must_use]
#[derive(Clone, Debug, Default)]
pub struct ClusterJoinReport {
    pub contacted: Vec<String>,
    pub failed: Vec<String>,
    pub discovered: Vec<String>,
}

pub async fn replicate_from_peers_targeted(
    registry: &Arc<LibraryRegistry>,
    peers: &[String],
    keys: &[Key<Aes256GcmSiv>],
    target_paths: Option<&HashSet<String>>,
    gateway: &impl ClusterGateway,
) -> ReplicationReport {
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
            report.record_discovered_path(&path);

            if target_paths.is_some_and(|targets| !targets.is_empty() && !targets.contains(&path)) {
                continue;
            }

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
    gateway: &impl ClusterGateway,
) -> ClusterJoinReport {
    let mut report = ClusterJoinReport::default();
    let mut pending = membership.snapshot_active_peers();
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
                membership.record_discovery_success(&seed);
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
                membership.record_discovery_failure(&seed);
                report.failed.push(format!("{seed}: {err}"));
            }
        }
    }

    report
}

pub fn live_replicating_install_put_handler(
    registry: Arc<LibraryRegistry>,
    membership: PeerMembership,
    gateway: HttpClusterGateway,
) -> impl crate::http::HttpHandler {
    move |req: Request<Body>| {
        let registry = Arc::clone(&registry);
        let membership = membership.clone();
        let gateway = gateway.clone();
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

            match decode_authorize_and_stage_install(&registry, &txn, &body).await {
                Ok(_) => {}
                Err(err) => return stage_install_error_response(err),
            }

            if !forwarded {
                let install_bytes = body.to_vec();
                match forward_install_to_peers(&membership, &txn, install_bytes, &gateway).await {
                    Ok(participants) => {
                        registry.record_replication_participants(txn.id(), participants)
                    }
                    Err(err) => {
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
    install_compiled_package: Vec<u8>,
    gateway: &impl ClusterGateway,
) -> TCResult<Vec<String>> {
    let token = txn
        .raw_token()
        .ok_or_else(|| tc_error::TCError::unauthorized("missing bearer token"))?
        .to_string();
    let txn_id = txn.id();

    let participants = membership.snapshot_active_peers().into_iter().collect();
    let delivered = fanout_participants(
        &participants,
        "install payload",
        ParticipantFanoutPolicy::default(),
        |peer| {
            let token = token.clone();
            let payload = install_compiled_package.clone();
            async move {
                gateway
                    .push_install_compiled_package(&peer, &token, txn_id, payload)
                    .await
            }
        },
    )
    .await
    .map_err(|err| err.err)?;

    Ok(delivered.into_iter().collect())
}

pub fn live_replicating_finalize_hook<G>(
    registry: Arc<LibraryRegistry>,
    gateway: G,
) -> impl Fn(crate::txn::TxnHandle, bool) -> futures::future::BoxFuture<'static, TCResult<()>>
+ Send
+ Sync
+ 'static
where
    G: ClusterGateway + Clone,
{
    move |txn: crate::txn::TxnHandle, commit: bool| {
        let registry = Arc::clone(&registry);
        let gateway = gateway.clone();
        async move {
            let Some(participants) = registry.replication_participants(txn.id()) else {
                return Ok(());
            };

            match forward_finalize_to_participants_progress(&participants, &txn, commit, &gateway)
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
    gateway: &impl ClusterGateway,
) -> TCResult<()> {
    let participants = membership.snapshot_active_peers().into_iter().collect();
    forward_finalize_to_participants(&participants, txn, commit, gateway).await
}

async fn forward_finalize_to_participants<G>(
    participants: &ParticipantSet<String>,
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
    participants: &ParticipantSet<String>,
    txn: &crate::txn::TxnHandle,
    commit: bool,
    gateway: &G,
) -> Result<ParticipantSet<String>, ParticipantFanoutError>
where
    G: ClusterGateway,
{
    let token = txn
        .raw_token()
        .ok_or_else(|| ParticipantFanoutError {
            delivered: ParticipantSet::default(),
            err: tc_error::TCError::unauthorized("missing bearer token"),
        })?
        .to_string();
    let txn_id = txn.id();

    fanout_participants(
        participants,
        "finalize transaction",
        ParticipantFanoutPolicy::default(),
        |peer| {
            let token = token.clone();
            async move {
                gateway
                    .finalize_install_txn(&peer, &token, txn_id, commit)
                    .await
            }
        },
    )
    .await
}

async fn fanout_participants<F, Fut>(
    participants: &ParticipantSet<String>,
    operation: &str,
    policy: ParticipantFanoutPolicy,
    apply: F,
) -> Result<ParticipantSet<String>, ParticipantFanoutError>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = TCResult<()>>,
{
    let mut delivered = ParticipantSet::default();
    let mut first_error = None;

    for _ in 1..=policy.max_attempts {
        let targets = participants
            .iter()
            .filter(|peer| !delivered.contains(*peer))
            .cloned()
            .collect::<Vec<_>>();

        if targets.is_empty() {
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
        Ok(delivered)
    } else {
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

#[cfg(test)]
mod rpc_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Default)]
    struct CountingGateway {
        token: AtomicUsize,
        work: AtomicUsize,
        finalize: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ClusterGateway for CountingGateway {
        async fn discover_library_paths(&self, _peer: &str) -> TCResult<Vec<String>> {
            unreachable!("transactional fanout must not perform discovery")
        }

        async fn request_replication_token(
            &self,
            _peer: &str,
            _path: &str,
            _keys: &[Key<Aes256GcmSiv>],
        ) -> TCResult<String> {
            self.token.fetch_add(1, Ordering::SeqCst);
            Ok("redundant-token".to_string())
        }

        async fn fetch_compiled_library_package(
            &self,
            _peer: &str,
            _token: &str,
        ) -> TCResult<Option<crate::library::CompiledLibraryPackage>> {
            unreachable!("transactional fanout must not fetch an artifact")
        }

        async fn register_with_peer(
            &self,
            _seed: &str,
            _joiner: &PeerIdentity,
            _routes: &PeerRoutes,
            _keys: &[Key<Aes256GcmSiv>],
        ) -> TCResult<PeerClusterListing> {
            unreachable!("transactional fanout must not register a peer")
        }

        async fn push_install_compiled_package(
            &self,
            _peer: &str,
            _token: &str,
            _txn_id: tc_ir::TxnId,
            _payload: Vec<u8>,
        ) -> TCResult<()> {
            self.work.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn finalize_install_txn(
            &self,
            _peer: &str,
            _token: &str,
            _txn_id: tc_ir::TxnId,
            _commit: bool,
        ) -> TCResult<()> {
            self.finalize.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn transactional_fanout_meets_the_failure_free_rpc_lower_bound() {
        let peers = vec![
            "http://peer-a".to_string(),
            "http://peer-b".to_string(),
            "http://peer-c".to_string(),
        ];
        let membership = PeerMembership::new(peers.clone());
        let txn = crate::txn::test_txn("rpc-minimality");
        let gateway = CountingGateway::default();

        let prepared =
            forward_install_to_peers(&membership, &txn, b"compiled-library".to_vec(), &gateway)
                .await
                .expect("prepare participants");
        let prepared = prepared.into_iter().collect();
        forward_finalize_to_participants(&prepared, &txn, true, &gateway)
            .await
            .expect("finalize participants");

        assert_eq!(prepared, peers.into_iter().collect());
        assert_eq!(gateway.token.load(Ordering::SeqCst), 0);
        let participant_count = prepared.iter().count();
        assert_eq!(gateway.work.load(Ordering::SeqCst), participant_count);
        assert_eq!(gateway.finalize.load(Ordering::SeqCst), participant_count);
    }
}
