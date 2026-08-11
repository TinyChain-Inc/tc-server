use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use freqfs::DirLock;
use pathlink::Link;
use tc_collection::PersistentFile;
use tc_error::{TCError, TCResult};
use tc_ir::TxnId;

const STATE: &str = "state";
const COLLECTION: &str = "collection";
const TXN: &str = "txn";

/// Host-owned collection and transaction workspace.
///
/// This is created exactly once at bootstrap. Lower layers receive child
/// directories and never reconstruct host or transaction paths themselves.
#[derive(Clone)]
pub struct Workspace {
    root: DirLock<PersistentFile>,
    next_temp: Arc<AtomicU64>,
}

impl Workspace {
    pub(crate) fn from_root(root: DirLock<PersistentFile>) -> Self {
        Self {
            root,
            next_temp: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn named_collection(
        &self,
        class: &str,
        uri: &Link,
    ) -> TCResult<DirLock<PersistentFile>> {
        let path: Vec<_> = uri.path().clone().into_iter().collect();
        let expected = [STATE, COLLECTION, class];
        if path.len() < expected.len()
            || path
                .iter()
                .zip(expected)
                .any(|(segment, expected)| segment.as_str() != expected)
        {
            return Err(TCError::bad_request(format!(
                "expected a canonical /state/collection/{class}/... URI, got {uri}"
            )));
        }

        let mut dir = self.root.clone();
        for segment in path {
            dir = child(dir, segment.to_string()).await?;
        }

        Ok(dir)
    }

    pub async fn transaction(&self, txn_id: TxnId) -> TCResult<DirLock<PersistentFile>> {
        let txns = child(self.root.clone(), TXN).await?;
        child(txns, txn_id.to_string()).await
    }

    pub async fn transaction_child(
        &self,
        txn_id: TxnId,
        path: &[String],
    ) -> TCResult<DirLock<PersistentFile>> {
        let mut dir = self.transaction(txn_id).await?;
        for segment in path {
            dir = child(dir, segment.clone()).await?;
        }
        Ok(dir)
    }

    pub fn unique_name(&self) -> String {
        self.next_temp.fetch_add(1, Ordering::Relaxed).to_string()
    }

    pub(crate) async fn remove_transaction(&self, txn_id: TxnId) -> TCResult<()> {
        let txns = child(self.root.clone(), TXN).await?;
        let mut txns = txns.write().await;
        txns.delete(&txn_id.to_string()).await;
        txns.sync().await.map_err(map_io)
    }

    #[cfg(test)]
    pub(crate) async fn has_transaction(&self, txn_id: TxnId) -> TCResult<bool> {
        let txns = child(self.root.clone(), TXN).await?;
        let txns = txns.read().await;
        Ok(txns.get_dir(&txn_id.to_string()).is_some())
    }
}

async fn child(
    dir: DirLock<PersistentFile>,
    name: impl Into<String>,
) -> TCResult<DirLock<PersistentFile>> {
    let mut dir = dir.write().await;
    dir.get_or_create_dir(name.into()).map_err(map_io)
}

fn map_io(err: impl std::fmt::Display) -> TCError {
    TCError::internal(err.to_string())
}
