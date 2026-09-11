use std::fmt;
use std::future::Future;
use std::sync::Arc;

use pathlink::{PathBuf, PathSegment};
use tc_error::{TCError, TCResult};
use tc_ir::{Route, Transact, TxnId};

use crate::replication::ClusterGateway;

#[cfg(feature = "http-client")]
mod bootstrap;
#[cfg(feature = "http-client")]
pub(crate) use bootstrap::BootstrapSession;
mod dir;
mod replicas;
pub(crate) use dir::Dir;
#[cfg(feature = "http-client")]
pub(crate) use replicas::replica_put_state;

const REPLICAS: &str = "replicas";

/// Hash a resource's transaction-visible state using `async_hash` composition.
pub(crate) trait AsyncHash: Send + Sync {
    fn hash(&self, txn_id: TxnId) -> impl Future<Output = TCResult<[u8; 32]>> + Send;
}

pub(crate) trait DirItem:
    Clone + AsyncHash + Route<crate::State> + Transact + Send + Sync + 'static
{
    fn identity(&self) -> &pathlink::Link;
    fn bind(&self, txn: &crate::TxnHandle) -> crate::TxnHandle;
}

#[derive(Clone)]
pub(crate) struct Cluster<T> {
    path: PathBuf,
    state: T,
    protocol: Arc<crate::ProtocolAuthority>,
    replicas: txn_lock::map::TxnMapLock<TxnId, String, crate::replication::Replica>,
    gateway: Arc<dyn ClusterGateway>,
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
        }
    }

    pub(crate) fn path(&self) -> &PathBuf {
        &self.path
    }

    pub(crate) fn state(&self) -> &T {
        &self.state
    }
}

impl<T> Cluster<T>
where
    T: Route<crate::State> + Transact + AsyncHash,
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
    pub(crate) async fn claim(&self, txn: &crate::TxnHandle) -> TCResult<()> {
        if txn.is_locked() {
            self.verify_leader(txn).await?;
        } else {
            txn.claim_cluster(&self.path, &self.protocol)?;
            self.verify_leader(txn).await?;
        }
        Ok(())
    }

    async fn verify_leader(&self, txn: &crate::TxnHandle) -> TCResult<()> {
        let (host, actor) = txn
            .leader(&self.path)
            .ok_or_else(|| TCError::conflict("resource was not claimed"))?;
        if host == self.protocol.host().to_string() && actor == self.protocol.actor_id() {
            return Ok(());
        }
        self.replica_snapshot(txn.id())
            .await?
            .iter()
            .any(|replica| replica.host == host && replica.actor_id == actor)
            .then_some(())
            .ok_or_else(|| TCError::unauthorized("resource leader is not an exact replica"))
    }
}
