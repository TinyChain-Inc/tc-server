mod client;
mod crypto;
mod handler;
mod http_util;
mod issuer;
mod membership;
mod peers;

#[cfg(test)]
mod tests;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::library::{
    LibraryRegistry, decode_authorize_and_stage_install, stage_install_error_response,
};
use aes_gcm_siv::{Aes256GcmSiv, Key};
use futures::FutureExt;
use hyper::body::to_bytes;
use hyper::{Body, Request, StatusCode};
use parking_lot::Mutex;
use tc_error::{TCError, TCResult};
use tc_ir::TxnId;

use self::http_util::{bad_request, empty_response, text_response};

pub const LIBRARY_EXPORT_PATH: &str = crate::uri::HOST_LIBRARY_EXPORT;
pub const PEERS_PATH_SUFFIX: &str = "/_cluster/peers";
pub const PEERS_JOIN_PATH_SUFFIX: &str = "/_cluster/peers/join";
pub const PEERS_LEAVE_PATH_SUFFIX: &str = "/_cluster/peers/leave";
pub const PEERS_HEARTBEAT_PATH_SUFFIX: &str = "/_cluster/peers/heartbeat";
pub const FORWARDED_HEADER: &str = "x-tc-replicated";
const TOKEN_PATH: &str = "/";
const REPLICATION_TTL: Duration = Duration::from_secs(30);

pub use client::{
    PeerClusterListing, discover_library_paths, fetch_library_export, fetch_library_listing,
    fetch_library_schema, heartbeat_peer, leave_peer_cluster, list_peer_cluster, normalize_peer,
    register_with_peer, request_replication_token,
};
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

#[derive(Clone, Default)]
pub struct ReplicatedTxnTracker {
    txns: Arc<Mutex<std::collections::BTreeSet<TxnId>>>,
}

impl ReplicatedTxnTracker {
    pub fn mark(&self, txn_id: TxnId) {
        self.txns.lock().insert(txn_id);
    }

    pub fn take(&self, txn_id: TxnId) -> bool {
        self.txns.lock().remove(&txn_id)
    }
}

pub async fn replicate_from_peers(
    registry: &Arc<LibraryRegistry>,
    peers: &[String],
    keys: &[Key<Aes256GcmSiv>],
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

pub async fn announce_self_to_cluster(
    membership: &PeerMembership,
    self_identity: &PeerIdentity,
    routes: &PeerRoutes,
    keys: &[Key<Aes256GcmSiv>],
    issuer: &ReplicationIssuer,
) {
    let mut pending = membership.active_peers();
    let mut visited = std::collections::HashSet::new();
    while let Some(seed) = pending.pop() {
        if seed == self_identity.peer || !visited.insert(seed.clone()) {
            continue;
        }

        match register_with_peer(&seed, self_identity, routes, keys).await {
            Ok(discovered) => {
                membership.mark_success(&seed);
                for identity in discovered.identities {
                    if identity.peer == self_identity.peer {
                        continue;
                    }

                    if let Err(err) = issuer.register_peer_identity(&identity) {
                        eprintln!(
                            "announce_self_to_cluster: invalid peer identity from {seed}: {err}"
                        );
                        continue;
                    }

                    if membership.upsert_identity(identity.clone()) {
                        pending.push(identity.peer);
                    }
                }

                for peer in discovered.peers {
                    if peer == self_identity.peer {
                        continue;
                    }
                    if membership.upsert_active(peer.clone()) {
                        pending.push(peer);
                    }
                }
            }
            Err(_) => membership.mark_failure(&seed),
        }
    }
}

pub fn live_replicating_install_put_handler(
    registry: Arc<LibraryRegistry>,
    membership: PeerMembership,
    keys: Vec<Key<Aes256GcmSiv>>,
    tracker: ReplicatedTxnTracker,
) -> impl crate::KernelHandler {
    move |req: Request<Body>| {
        let registry = Arc::clone(&registry);
        let membership = membership.clone();
        let keys = keys.clone();
        let tracker = tracker.clone();
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
                    Ok(()) => tracker.mark(txn.id()),
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
    install_payload: Vec<u8>,
    keys: &[Key<Aes256GcmSiv>],
) -> TCResult<()> {
    let token = txn
        .raw_token()
        .ok_or_else(|| tc_error::TCError::unauthorized("missing bearer token"))?
        .to_string();
    let txn_id = txn.id();

    fanout_peers(membership, "install payload", |peer| {
        let token = token.clone();
        let payload = install_payload.clone();
        async move {
            request_replication_token(&peer, schema_path, keys)
                .await
                .map_err(|err| {
                    tc_error::TCError::bad_gateway(format!(
                        "failed to request replication token from {peer}: {err}"
                    ))
                })?;

            client::push_install_payload(&peer, &token, txn_id, payload).await
        }
    })
    .await
}

pub fn live_replicating_finalize_hook(
    membership: PeerMembership,
    tracker: ReplicatedTxnTracker,
) -> impl Fn(crate::txn::TxnHandle, bool) -> futures::future::BoxFuture<'static, TCResult<()>>
+ Send
+ Sync
+ 'static {
    move |txn: crate::txn::TxnHandle, commit: bool| {
        let membership = membership.clone();
        let tracker = tracker.clone();
        async move {
            if !tracker.take(txn.id()) {
                return Ok(());
            }

            forward_finalize_to_peers(&membership, &txn, commit).await
        }
        .boxed()
    }
}

pub async fn forward_finalize_to_peers(
    membership: &PeerMembership,
    txn: &crate::txn::TxnHandle,
    commit: bool,
) -> TCResult<()> {
    let token = txn
        .raw_token()
        .ok_or_else(|| tc_error::TCError::unauthorized("missing bearer token"))?
        .to_string();
    let txn_id = txn.id();

    fanout_peers(membership, "finalize transaction", |peer| {
        let token = token.clone();
        async move { client::finalize_install_txn(&peer, &token, txn_id, commit).await }
    })
    .await
}

async fn fanout_peers<F, Fut>(
    membership: &PeerMembership,
    operation: &str,
    mut apply: F,
) -> TCResult<()>
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = TCResult<()>>,
{
    for peer in membership.active_peers() {
        match apply(peer.clone()).await {
            Ok(()) => membership.mark_success(&peer),
            Err(err) => {
                eprintln!("live replication: failed to {operation} on {peer}: {err}");
                membership.mark_failure(&peer);
                return Err(err);
            }
        }
    }

    Ok(())
}
