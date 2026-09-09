#[cfg(feature = "http-client")]
mod client;
mod crypto;
mod gateway;
#[cfg(feature = "http-server")]
mod http_util;
mod issuer;
mod membership;
#[cfg(feature = "http-server")]
mod peers;

use std::future::Future;
use std::sync::Arc;

use aes_gcm_siv::{Aes256GcmSiv, Key};
use futures::{StreamExt, stream};
use tc_error::{TCError, TCResult};

/// One bounded canonical application body crossing a host boundary.
#[derive(Clone)]
pub struct CanonicalBody {
    pub identity: pathlink::Link,
    pub body: Arc<[u8]>,
    pub content_type: String,
}

pub const PEERS_PATH_SUFFIX: &str = "/_cluster/peers";
pub const PEERS_JOIN_PATH_SUFFIX: &str = "/_cluster/peers/join";
pub const PEERS_LEAVE_PATH_SUFFIX: &str = "/_cluster/peers/leave";
pub const PEERS_HEARTBEAT_PATH_SUFFIX: &str = "/_cluster/peers/heartbeat";
const DECISION_ATTEMPTS: usize = 3;
const FANOUT_CONCURRENCY: usize = 8;

#[cfg(feature = "http-client")]
pub use client::HttpClusterGateway;
pub use gateway::{ClusterGateway, LocalClusterGateway};
pub use issuer::{ReplicationIssuer, parse_psk_keys, parse_psk_list};
pub use membership::{PeerDescriptor, PeerIdentity, PeerMembership};
#[cfg(feature = "http-server")]
pub use peers::peer_membership_handler;

#[derive(Clone, Debug, Default)]
pub struct PeerClusterListing {
    pub peers: Vec<String>,
    pub identities: Vec<PeerIdentity>,
}

pub fn normalize_peer(peer: &str) -> TCResult<String> {
    let value = if peer.contains("://") {
        peer.to_string()
    } else {
        format!("http://{peer}")
    };
    let url = url::Url::parse(&value)
        .map_err(|error| TCError::bad_request(format!("invalid peer url: {error}")))?;
    let host = url
        .host_str()
        .ok_or_else(|| TCError::bad_request("peer URL missing host"))?;
    Ok(url.port().map_or_else(
        || format!("{}://{host}", url.scheme()),
        |port| format!("{}://{host}:{port}", url.scheme()),
    ))
}

pub fn is_supported_replicated_path(path: &str) -> bool {
    path.parse::<pathlink::Link>()
        .ok()
        .and_then(|link| {
            crate::application::split_application_link(&link)
                .ok()
                .map(|(_, segments, _)| segments)
        })
        .is_some_and(|segments| !segments.is_empty())
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
            "trusted installer prefix must start with /lib/, /class/, or /service/: {trimmed}"
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
    #[cfg(feature = "http-server")]
    peers: String,
    #[cfg(any(feature = "http-client", feature = "http-server"))]
    join: String,
    #[cfg(feature = "http-server")]
    leave: String,
    #[cfg(feature = "http-server")]
    heartbeat: String,
}

impl PeerRoutes {
    pub fn new(cluster_root: &str) -> TCResult<Self> {
        let cluster_root = normalize_cluster_root(cluster_root)?;
        Ok(Self {
            #[cfg(feature = "http-server")]
            peers: format!("{cluster_root}{PEERS_PATH_SUFFIX}"),
            #[cfg(any(feature = "http-client", feature = "http-server"))]
            join: format!("{cluster_root}{PEERS_JOIN_PATH_SUFFIX}"),
            #[cfg(feature = "http-server")]
            leave: format!("{cluster_root}{PEERS_LEAVE_PATH_SUFFIX}"),
            #[cfg(feature = "http-server")]
            heartbeat: format!("{cluster_root}{PEERS_HEARTBEAT_PATH_SUFFIX}"),
            cluster_root,
        })
    }

    pub fn cluster_root(&self) -> &str {
        &self.cluster_root
    }
}

fn normalize_cluster_root(value: &str) -> TCResult<String> {
    let root = value.trim().trim_end_matches('/');
    let supported = root
        .parse::<pathlink::Link>()
        .ok()
        .and_then(|link| {
            crate::application::split_application_link(&link).ok().map(
                |(kind, segments, suffix)| {
                    (kind == "lib" || kind == "service")
                        && !segments.is_empty()
                        && suffix.is_empty()
                },
            )
        })
        .unwrap_or(false);
    if !supported {
        return Err(TCError::bad_request(format!(
            "invalid cluster root {root}: expected /lib/<publisher> or /service/<publisher>"
        )));
    }

    Ok(root.to_string())
}

#[must_use]
#[derive(Clone, Debug, Default)]
pub struct ClusterJoinReport {
    pub contacted: Vec<String>,
    pub failed: Vec<String>,
    pub discovered: Vec<String>,
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

pub(crate) async fn forward_install_to_peers(
    peers: &std::collections::BTreeSet<String>,
    txn: &crate::txn::TxnHandle,
    application: CanonicalBody,
    gateway: &dyn ClusterGateway,
) -> TCResult<Vec<String>> {
    let token = txn
        .raw_token()
        .ok_or_else(|| tc_error::TCError::unauthorized("missing bearer token"))?
        .to_string();
    let txn_id = txn.id();

    let delivered = fanout_peers(peers, "install payload", |peer| {
        let token = token.clone();
        let application = application.clone();
        async move {
            gateway
                .put_application(&peer, &token, txn_id, application, txn.deadline())
                .await
        }
    })
    .await?;

    Ok(delivered.into_iter().collect())
}

pub(crate) async fn forward_delete_to_peers(
    peers: &std::collections::BTreeSet<String>,
    txn: &crate::txn::TxnHandle,
    identity: &pathlink::Link,
    gateway: &dyn ClusterGateway,
) -> TCResult<()> {
    let token = txn
        .raw_token()
        .ok_or_else(|| tc_error::TCError::unauthorized("missing bearer token"))?
        .to_string();
    let txn_id = txn.id();
    fanout_peers(peers, "delete application", |peer| {
        let token = token.clone();
        let identity = identity.clone();
        async move {
            gateway
                .delete_application(&peer, &token, txn_id, &identity, txn.deadline())
                .await
        }
    })
    .await
    .map(drop)
}

pub(crate) async fn forward_resource_decision(
    peers: &std::collections::BTreeSet<String>,
    txn: &crate::txn::TxnHandle,
    resource: &pathlink::PathBuf,
    commit: bool,
    gateway: &dyn ClusterGateway,
) -> TCResult<()> {
    let token = txn
        .raw_token()
        .ok_or_else(|| tc_error::TCError::unauthorized("missing bearer token"))?
        .to_string();
    let txn_id = txn.id();
    fanout_peers(peers, "deliver transaction decision", |peer| {
        let token = token.clone();
        let resource = resource.clone();
        async move {
            gateway
                .decide_resource(&peer, &token, txn_id, &resource, commit, txn.deadline())
                .await
        }
    })
    .await
    .map(drop)
}

async fn fanout_peers<F, Fut>(
    peers: &std::collections::BTreeSet<String>,
    operation: &str,
    apply: F,
) -> TCResult<std::collections::BTreeSet<String>>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = TCResult<()>>,
{
    let mut delivered = std::collections::BTreeSet::new();
    let mut first_error = None;

    for _ in 0..DECISION_ATTEMPTS {
        let targets = peers
            .iter()
            .filter(|peer| !delivered.contains(*peer))
            .cloned()
            .collect::<Vec<_>>();

        if targets.is_empty() {
            return Ok(delivered);
        }

        let results = stream::iter(targets.into_iter().map(|peer| {
            let fut = apply(peer.clone());
            async move { (peer, fut.await) }
        }))
        .buffer_unordered(FANOUT_CONCURRENCY)
        .collect::<Vec<_>>()
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

    let unresolved = peers
        .iter()
        .filter(|peer| !delivered.contains(*peer))
        .cloned()
        .collect::<Vec<_>>();

    if unresolved.is_empty() {
        Ok(delivered)
    } else {
        Err(first_error.unwrap_or_else(|| {
            TCError::bad_gateway(format!(
                "failed to {operation} on peers: {}",
                unresolved.join(", ")
            ))
        }))
    }
}

#[cfg(test)]
mod rpc_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Default)]
    struct CountingGateway {
        work: AtomicUsize,
        decisions: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ClusterGateway for CountingGateway {
        fn replicas(&self, _resource: &pathlink::PathBuf) -> std::collections::BTreeSet<String> {
            std::collections::BTreeSet::from(["http://replica".to_string()])
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

        async fn put_application(
            &self,
            _peer: &str,
            _token: &str,
            _txn_id: tc_ir::TxnId,
            application: CanonicalBody,
            _deadline: crate::Deadline,
        ) -> TCResult<()> {
            assert_eq!(application.identity.path()[0].as_str(), "class");
            self.work.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn delete_application(
            &self,
            _peer: &str,
            _token: &str,
            _txn_id: tc_ir::TxnId,
            _identity: &pathlink::Link,
            _deadline: crate::Deadline,
        ) -> TCResult<()> {
            self.work.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn decide_resource(
            &self,
            _peer: &str,
            _token: &str,
            _txn_id: tc_ir::TxnId,
            _resource: &pathlink::PathBuf,
            _commit: bool,
            _deadline: crate::Deadline,
        ) -> TCResult<()> {
            let attempt = self.decisions.fetch_add(1, Ordering::SeqCst);
            (!matches!(attempt, 3..=5))
                .then_some(())
                .ok_or_else(|| TCError::bad_gateway("lost resource decision acknowledgement"))
        }
    }

    #[tokio::test]
    async fn work_and_resource_decisions_need_no_probe_or_finalization_rpc() {
        let peers = vec![
            "http://peer-a".to_string(),
            "http://peer-b".to_string(),
            "http://peer-c".to_string(),
        ];
        let peer_set = peers.iter().cloned().collect();
        let kernel = crate::txn::test_kernel("rpc-minimality").await;
        let txn = kernel.test_txn().await;
        let gateway = Arc::new(CountingGateway::default());
        let classes = &kernel.runtime.applications.classes;
        let cluster = crate::cluster::Cluster::new(
            classes.path().clone(),
            classes.state().clone(),
            Arc::clone(classes.protocol()),
            Arc::clone(&gateway) as Arc<dyn ClusterGateway>,
            crate::cluster::Staging::default(),
        );
        cluster.claim(&txn).await.expect("first cluster claim");

        let prepared = forward_install_to_peers(
            &peer_set,
            &txn,
            CanonicalBody {
                identity: "/class/example-devco/test/1.0.0".parse().expect("identity"),
                body: Arc::from(b"literal-class".as_slice()),
                content_type: "application/json".to_string(),
            },
            gateway.as_ref(),
        )
        .await
        .expect("forward work");
        let prepared = prepared.into_iter().collect();
        let resource = "/class/example-devco/test/1.0.0"
            .parse()
            .expect("resource path");
        forward_resource_decision(&prepared, &txn, &resource, true, gateway.as_ref())
            .await
            .expect("decide resources");
        cluster
            .decide(&txn, crate::txn::TransactionOutcome::Rollback)
            .await
            .expect_err("first propagation fails visibly");
        cluster
            .decide(&txn, crate::txn::TransactionOutcome::Rollback)
            .await
            .expect("repeated decision retries propagation");

        assert_eq!(gateway.work.load(Ordering::SeqCst), peers.len());
        let decisions = gateway.decisions.load(Ordering::SeqCst);
        assert_eq!(decisions, peers.len() + DECISION_ATTEMPTS + 1);
    }
}
