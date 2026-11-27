use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use parking_lot::Mutex;
use pathlink::{PathBuf, PathSegment};
use tc_error::{TCError, TCResult};
use tc_ir::{Route, Transact, TxnId};
use tokio_util::task::task_tracker::TaskTrackerToken;

use crate::replication::ClusterGateway;

#[cfg(feature = "http-client")]
mod bootstrap;
mod dir;
mod participation;
mod replicas;

#[cfg(feature = "http-client")]
pub(crate) use bootstrap::BootstrapSession;
#[cfg(all(feature = "http-client", feature = "http-server", test))]
pub(crate) use bootstrap::seed_replicas;
pub(crate) use dir::Dir;
pub(crate) use participation::Participation;
#[cfg(any(feature = "http-client", test))]
pub(crate) use replicas::replica_put_state;

const REPLICAS: &str = "replicas";

/// Hash a resource's transaction-visible state using `async_hash` composition.
pub(crate) trait AsyncHash: Send + Sync {
    fn hash(&self, txn: &crate::TxnHandle) -> impl Future<Output = TCResult<[u8; 32]>> + Send;
}

pub(crate) trait DirItem:
    Clone + AsyncHash + Route<crate::State> + Transact + Send + Sync + 'static
{
    fn identity(&self) -> &pathlink::Link;

    fn bind(&self, txn: &crate::TxnHandle) -> crate::TxnHandle;

    /// Immutable definitions are already installed by the tree walk.
    #[cfg(feature = "http-client")]
    fn synchronize(
        _cluster: &Cluster<Self>,
        _txn: &crate::TxnHandle,
        _seed: &str,
        _session: &BootstrapSession,
    ) -> impl Future<Output = TCResult<()>> + Send {
        std::future::ready(Ok(()))
    }
}

#[derive(Clone)]
pub(crate) struct Cluster<T> {
    path: PathBuf,
    state: T,
    protocol: Arc<crate::ProtocolAuthority>,
    replicas: txn_lock::map::TxnMapLock<TxnId, String, crate::replication::Replica>,
    gateway: Arc<dyn ClusterGateway>,
    participation: Arc<Mutex<BTreeMap<TxnId, Option<Arc<Participation>>>>>,
}

impl<T> fmt::Debug for Cluster<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cluster at {}", self.path)
    }
}

impl<T> Cluster<T> {
    pub(crate) fn new(
        path: PathBuf,
        state: T,
        protocol: Arc<crate::ProtocolAuthority>,
        gateway: Arc<dyn ClusterGateway>,
    ) -> Self {
        Self::with_replicas(path, state, protocol, gateway, [])
    }

    fn with_replicas(
        path: PathBuf,
        state: T,
        protocol: Arc<crate::ProtocolAuthority>,
        gateway: Arc<dyn ClusterGateway>,
        replicas: impl IntoIterator<Item = (String, crate::replication::Replica)>,
    ) -> Self {
        Self {
            path,
            state,
            protocol,
            replicas: txn_lock::map::TxnMapLock::from_committed(replicas),
            gateway,
            participation: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(crate) fn path(&self) -> &PathBuf {
        &self.path
    }

    pub(crate) fn state(&self) -> &T {
        &self.state
    }

    pub(crate) fn reserve_structure(&self, txn_id: TxnId) -> TCResult<()> {
        let mut entries = self.participation.lock();
        if entries.get(&txn_id).is_some_and(Option::is_some) {
            return Err(TCError::conflict(
                "structural mutation would remove or overlap an enlisted owner",
            ));
        }

        // The namespace owner reserves descendant admission under the same lock
        // as ordinary enrollment. Preflight alone would race with a new call.
        // No descendant leadership is claimed; the namespace owns the lifecycle.
        entries.insert(txn_id, None);
        Ok(())
    }
}

impl<T> Cluster<T>
where
    T: Clone + Route<crate::State> + Transact + AsyncHash,
{
    pub(crate) async fn invoke(
        &self,
        txn: &crate::TxnHandle,
        suffix: Box<[PathSegment]>,
        method: tc_ir::Method,
        body: Option<crate::State>,
        target: &str,
    ) -> TCResult<Option<crate::State>> {
        if self
            .decide_request(txn, &suffix, method, body.is_some())
            .await?
        {
            return Ok(None);
        }

        if body.is_none() && matches!(method, tc_ir::Method::Put | tc_ir::Method::Delete) {
            return Err(TCError::bad_request(
                "an ordinary PUT or DELETE request requires a body",
            ));
        }

        let handler = self
            .route(&suffix)
            .ok_or_else(|| TCError::not_found(target))?;
        let state = crate::kernel::invoke_handler(handler, txn, method, body).await?;
        Ok(Some(state))
    }
}

impl<T> Cluster<T>
where
    T: Clone,
{
    /// Enroll ordinary work; decision authentication belongs to `decide`.
    pub(crate) async fn claim(&self, txn: &crate::TxnHandle) -> TCResult<Arc<Participation>> {
        txn.claim_cluster(&self.path, &self.protocol)?;
        let claims = txn.protocol_snapshot().expect("claimed resource");
        let leader = claims.leaders.get(&self.path).expect("claimed leader");
        self.verify_leader(txn, leader).await?;

        // Claim authority can change while replica membership is being read.
        let claims = txn.protocol_snapshot().expect("verified resource claim");
        if claims.locked {
            return Err(TCError::conflict("a locked transaction cannot accept work"));
        }

        let owner = claims.owner.expect("claimed transaction owner");
        let current_leader = claims.leaders.get(&self.path).expect("claimed leader");
        if current_leader != leader {
            return Err(TCError::conflict(
                "resource transaction has different ownership",
            ));
        }

        let mut entries = self.participation.lock();
        let entry = entries
            .entry(txn.id())
            .or_insert_with(|| Some(Arc::new(Participation::new(owner.clone(), leader.clone()))))
            .as_ref()
            .ok_or_else(|| TCError::conflict("namespace structure is reserved"))?;

        if entry.owner != owner || entry.leader != *leader {
            return Err(TCError::conflict(
                "resource transaction has different ownership",
            ));
        }

        Ok(Arc::clone(entry))
    }

    pub(crate) async fn enter(
        &self,
        txn: &crate::TxnHandle,
    ) -> TCResult<(crate::TxnHandle, TaskTrackerToken)> {
        let participant = self.claim(txn).await?;
        let call = participant.admit()?;
        Ok((txn.for_resource(participant), call))
    }

    async fn verify_leader(
        &self,
        txn: &crate::TxnHandle,
        (host, actor): &(String, String),
    ) -> TCResult<()> {
        if self.protocol.host() == host && actor == self.protocol.actor_id() {
            return Ok(());
        }

        self.replica_snapshot(txn.id())
            .await?
            .iter()
            .any(|replica| replica.host == *host && replica.actor_id == *actor)
            .then_some(())
            .ok_or_else(|| TCError::unauthorized("resource leader is not an exact replica"))
    }
}
