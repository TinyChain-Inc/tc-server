use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use freqfs::DirLock;
use get_size::GetSize;
use pathlink::Link;
use tc_error::{TCError, TCResult};
use tc_ir::TxnId;

use crate::storage::{AuthorityRecord, ControlFile};

const AUTHORITY: &str = "authority";
const LATEST_FINALIZED: &str = "latest_finalized";
const LAST_ALLOCATED: &str = "last_allocated";

/// This is created exactly once at bootstrap. Lower layers receive child
/// directories and never reconstruct host or transaction paths themselves.
#[derive(Clone)]
pub struct Workspace {
    control: DirLock<ControlFile>,
    transactions: DirLock<tc_collection::PersistentFile>,
    next_temp: Arc<AtomicU64>,
}

impl Workspace {
    pub(crate) fn from_roots(
        control: DirLock<ControlFile>,
        transactions: DirLock<tc_collection::PersistentFile>,
    ) -> Self {
        Self {
            control,
            transactions,
            next_temp: Arc::new(AtomicU64::new(0)),
        }
    }

    async fn control_file(&self, name: &str) -> TCResult<Option<ControlFile>> {
        let file = {
            let root = self.control.read().await;
            root.get_file(name).cloned()
        };
        let Some(file) = file else {
            return Ok(None);
        };
        let file = file.read_owned::<ControlFile>().await.map_err(map_io)?;
        Ok(Some((*file).clone()))
    }

    async fn authority(&self) -> TCResult<Option<AuthorityRecord>> {
        self.control_file(AUTHORITY)
            .await?
            .map_or(Ok(None), |file| {
                let ControlFile::Authority(record) = file else {
                    return Err(TCError::internal("invalid protocol authority record"));
                };
                Ok(Some(record))
            })
    }

    pub(crate) async fn frontiers(&self) -> TCResult<(Option<TxnId>, Option<TxnId>)> {
        let read = |file| match file {
            None => Ok(None),
            Some(ControlFile::Frontier(txn_id)) => Ok(Some(txn_id)),
            Some(_) => Err(TCError::internal("invalid transaction frontier record")),
        };
        Ok((
            read(self.control_file(LATEST_FINALIZED).await?)?,
            read(self.control_file(LAST_ALLOCATED).await?)?,
        ))
    }

    pub async fn load_or_create_protocol_authority(
        &self,
        actor_id: &tc_ir::Id,
        host: Link,
    ) -> TCResult<(crate::txn::ProtocolAuthority, rjwt::Actor<String>)> {
        let record = match self.authority().await? {
            Some(record) => {
                if &record.actor_id != actor_id || record.algorithm != rjwt::AlgKind::Falcon512 {
                    return Err(TCError::internal(
                        "workspace protocol authority does not match this host",
                    ));
                }
                record
            }
            None => {
                let actor = rjwt::Actor::new_falcon512(actor_id.to_string())
                    .map_err(|err| TCError::internal(err.to_string()))?;
                let record = AuthorityRecord {
                    actor_id: actor_id.clone(),
                    algorithm: rjwt::AlgKind::Falcon512,
                    signing_key: actor
                        .signing_key_bytes()
                        .map_err(|err| TCError::internal(err.to_string()))?,
                };
                write_record(
                    &self.control,
                    AUTHORITY,
                    ControlFile::Authority(record.clone()),
                )
                .await?;
                record
            }
        };
        let signing_key = rjwt::SigningKey::from_bytes(record.algorithm, &record.signing_key)
            .map_err(|err| TCError::internal(err.to_string()))?;
        let replication_actor =
            rjwt::Actor::with_signing_key(record.actor_id.to_string(), signing_key);
        let protocol_key = rjwt::SigningKey::from_bytes(record.algorithm, &record.signing_key)
            .map_err(|err| TCError::internal(err.to_string()))?;
        let protocol_actor =
            rjwt::Actor::with_signing_key(record.actor_id.to_string(), protocol_key);
        Ok((
            crate::txn::ProtocolAuthority::new(host, protocol_actor),
            replication_actor,
        ))
    }

    pub(crate) async fn write_latest_finalized(&self, txn_id: TxnId) -> TCResult<()> {
        write_record(
            &self.control,
            LATEST_FINALIZED,
            ControlFile::Frontier(txn_id),
        )
        .await
    }

    pub(crate) async fn write_last_allocated(&self, txn_id: TxnId) -> TCResult<()> {
        write_record(&self.control, LAST_ALLOCATED, ControlFile::Frontier(txn_id)).await
    }

    pub async fn transaction(
        &self,
        txn_id: TxnId,
    ) -> TCResult<DirLock<tc_collection::PersistentFile>> {
        child(self.transactions.clone(), txn_id.to_string()).await
    }

    pub async fn transaction_child(
        &self,
        txn_id: TxnId,
        path: &[String],
    ) -> TCResult<DirLock<tc_collection::PersistentFile>> {
        let mut dir = self.transaction(txn_id).await?;
        for segment in path {
            dir = child(dir, segment.clone()).await?;
        }
        Ok(dir)
    }

    pub(crate) async fn transaction_ids(&self) -> TCResult<Vec<TxnId>> {
        let entries = {
            let txns = self.transactions.read().await;
            txns.iter()
                .map(|(name, entry)| match entry {
                    freqfs::DirEntry::Dir(_) => name.parse::<TxnId>().map_err(|error| {
                        TCError::internal(format!("invalid transaction workspace {name}: {error}"))
                    }),
                    freqfs::DirEntry::File(_) => Err(TCError::internal(format!(
                        "invalid file in transaction workspace root: {name}"
                    ))),
                })
                .collect::<TCResult<Vec<_>>>()?
        };
        let mut entries = entries;
        entries.sort();
        Ok(entries)
    }

    pub fn unique_name(&self) -> String {
        format!("tmp-{}", self.next_temp.fetch_add(1, Ordering::Relaxed))
    }

    pub(crate) async fn remove_through(&self, cutoff: TxnId) -> TCResult<()> {
        let names = {
            let txns = self.transactions.read().await;
            txns.iter()
                .map(|(name, entry)| match entry {
                    freqfs::DirEntry::Dir(_) => name
                        .parse::<TxnId>()
                        .map(|id| (id, name.clone()))
                        .map_err(|error| {
                            TCError::internal(format!(
                                "invalid transaction workspace {name}: {error}"
                            ))
                        }),
                    freqfs::DirEntry::File(_) => Err(TCError::internal(format!(
                        "invalid file in transaction workspace root: {name}"
                    ))),
                })
                .collect::<TCResult<Vec<_>>>()?
        };
        let mut txns = self.transactions.write().await;
        for (_, name) in names.into_iter().filter(|(id, _)| *id <= cutoff) {
            txns.delete(&name).await;
        }
        txns.sync().await.map_err(map_io)
    }

    #[cfg(test)]
    pub(crate) async fn has_transaction(&self, txn_id: TxnId) -> TCResult<bool> {
        let txns = self.transactions.read().await;
        Ok(txns.get_dir(&txn_id.to_string()).is_some())
    }
}

async fn write_record(dir: &DirLock<ControlFile>, name: &str, record: ControlFile) -> TCResult<()> {
    let size = record.get_size();
    let existing = {
        let dir = dir.read().await;
        dir.get_file(name).cloned()
    };
    if let Some(file) = existing {
        {
            let mut contents = file.write_owned::<ControlFile>().await.map_err(map_io)?;
            *contents = record;
        }
    } else {
        let mut dir = dir.write().await;
        dir.create_file(name.to_string(), record, size)
            .await
            .map_err(map_io)?;
    }
    dir.write().await.sync().await.map_err(map_io)
}

async fn child(
    dir: DirLock<tc_collection::PersistentFile>,
    name: impl Into<String>,
) -> TCResult<DirLock<tc_collection::PersistentFile>> {
    let mut dir = dir.write().await;
    let child = dir.get_or_create_dir(name.into()).map_err(map_io)?;
    dir.sync().await.map_err(map_io)?;
    Ok(child)
}

fn map_io(err: impl std::fmt::Display) -> TCError {
    TCError::internal(err.to_string())
}

#[cfg(test)]
mod tests {
    fn txn_id() -> tc_ir::TxnId {
        tc_ir::TxnId::from_parts(tc_ir::NetworkTime::from_nanos(1), 0).with_trace([1; 32])
    }

    fn workspace(name: &str) -> super::Workspace {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        crate::HostStorage::new(&crate::HostLimits::default().storage)
            .workspace(std::env::temp_dir().join(format!("tc-workspace-{name}-{unique}")))
            .expect("workspace")
    }

    #[tokio::test]
    async fn transaction_workspace_ids_are_recovered_without_records() {
        let workspace = workspace("transaction-id");
        workspace
            .transaction(txn_id())
            .await
            .expect("transaction dir");
        assert_eq!(workspace.transaction_ids().await.expect("IDs"), [txn_id()]);
    }

    #[tokio::test]
    async fn cleanup_removes_workspaces_through_the_frontier() {
        let workspace = workspace("cleanup-through");
        let newer =
            tc_ir::TxnId::from_parts(tc_ir::NetworkTime::from_nanos(2), 0).with_trace([2; 32]);
        workspace
            .transaction(txn_id())
            .await
            .expect("old transaction");
        workspace.transaction(newer).await.expect("new transaction");
        workspace.remove_through(txn_id()).await.expect("cleanup");
        assert_eq!(workspace.transaction_ids().await.expect("IDs"), [newer]);
    }
}
