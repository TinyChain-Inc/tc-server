use std::sync::Arc;

use freqfs::DirLock;
use get_size::GetSize;
use pathlink::Link;
use tc_error::{TCError, TCResult};
use tc_ir::TxnId;

use crate::storage::{HostRecord, WorkspaceFile};

const TXN: &str = "txn";
const HOST_RECORD: &str = "host";

/// This is created exactly once at bootstrap. Lower layers receive child
/// directories and never reconstruct host or transaction paths themselves.
#[derive(Clone)]
pub struct Workspace {
    root: DirLock<WorkspaceFile>,
    temp_seed: Arc<String>,
    host_updates: Arc<tokio::sync::Mutex<()>>,
}

impl Workspace {
    pub(crate) fn from_root(root: DirLock<WorkspaceFile>) -> Self {
        Self {
            root,
            temp_seed: Arc::new(format!(
                "{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            )),
            host_updates: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub(crate) async fn host_record(&self) -> TCResult<Option<HostRecord>> {
        let file = {
            let root = self.root.read().await;
            root.get_file(HOST_RECORD).cloned()
        };
        let Some(file) = file else {
            return Ok(None);
        };
        let file = file.read_owned::<WorkspaceFile>().await.map_err(map_io)?;
        let WorkspaceFile::Host(record) = &*file else {
            return Err(TCError::internal("invalid host workspace record"));
        };
        Ok(Some(record.clone()))
    }

    pub(crate) async fn write_host_record(&self, record: &HostRecord) -> TCResult<()> {
        write_record(&self.root, HOST_RECORD, WorkspaceFile::Host(record.clone())).await
    }

    pub async fn load_or_create_protocol_authority(
        &self,
        host_id: &str,
        host: Link,
    ) -> TCResult<(crate::txn::ProtocolAuthority, rjwt::Actor<String>)> {
        let expected_actor = format!("replication:{host_id}");
        let record = match self.host_record().await? {
            Some(record) => {
                if record.actor_id != expected_actor || record.algorithm != "falcon512" {
                    return Err(TCError::internal(
                        "workspace protocol authority does not match this host",
                    ));
                }
                record
            }
            None => {
                let actor = rjwt::Actor::new_falcon512(expected_actor.clone())
                    .map_err(|err| TCError::internal(err.to_string()))?;
                let record = HostRecord {
                    actor_id: expected_actor,
                    algorithm: "falcon512".into(),
                    signing_key: actor
                        .signing_key_bytes()
                        .map_err(|err| TCError::internal(err.to_string()))?,
                    latest_finalized: None,
                    last_allocated: None,
                };
                self.write_host_record(&record).await?;
                record
            }
        };
        let signing_key =
            rjwt::SigningKey::from_bytes(rjwt::AlgKind::Falcon512, &record.signing_key)
                .map_err(|err| TCError::internal(err.to_string()))?;
        let replication_actor = rjwt::Actor::with_signing_key(record.actor_id.clone(), signing_key);
        let protocol_key =
            rjwt::SigningKey::from_bytes(rjwt::AlgKind::Falcon512, &record.signing_key)
                .map_err(|err| TCError::internal(err.to_string()))?;
        let protocol_actor = rjwt::Actor::with_signing_key(record.actor_id, protocol_key);
        Ok((
            crate::txn::ProtocolAuthority::new(host_id, host, protocol_actor),
            replication_actor,
        ))
    }

    pub(crate) async fn update_host_frontier(
        &self,
        latest_finalized: Option<TxnId>,
        last_allocated: Option<TxnId>,
    ) -> TCResult<()> {
        let _update = self.host_updates.lock().await;
        let mut record = self
            .host_record()
            .await?
            .ok_or_else(|| TCError::internal("missing workspace protocol authority"))?;
        if let Some(latest) = latest_finalized {
            if record
                .latest_finalized
                .is_none_or(|current| latest > current)
            {
                record.latest_finalized = Some(latest);
            }
        }
        if let Some(last) = last_allocated {
            if record.last_allocated.is_none_or(|current| last > current) {
                record.last_allocated = Some(last);
            }
        }
        self.write_host_record(&record).await
    }

    pub async fn transaction(&self, txn_id: TxnId) -> TCResult<DirLock<WorkspaceFile>> {
        let txns = child(self.root.clone(), TXN).await?;
        child(txns, txn_id.to_string()).await
    }

    pub async fn transaction_child(
        &self,
        txn_id: TxnId,
        path: &[String],
    ) -> TCResult<DirLock<WorkspaceFile>> {
        let mut dir = self.transaction(txn_id).await?;
        for segment in path {
            dir = child(dir, segment.clone()).await?;
        }
        Ok(dir)
    }

    pub(crate) async fn transaction_ids(&self) -> TCResult<Vec<TxnId>> {
        let Some(txns) = ({
            let root = self.root.read().await;
            root.get_dir(TXN).cloned()
        }) else {
            return Ok(Vec::new());
        };
        let entries = {
            let txns = txns.read().await;
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
        format!(
            "{}-{}",
            self.temp_seed,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        )
    }

    #[cfg(test)]
    pub(crate) async fn remove_transaction(&self, txn_id: TxnId) -> TCResult<()> {
        let txns = child(self.root.clone(), TXN).await?;
        let mut txns = txns.write().await;
        txns.delete(&txn_id.to_string()).await;
        txns.sync().await.map_err(map_io)
    }

    pub(crate) async fn remove_through(&self, cutoff: TxnId) -> TCResult<()> {
        let Some(txns) = ({
            let root = self.root.read().await;
            root.get_dir(TXN).cloned()
        }) else {
            return Ok(());
        };
        let names = {
            let txns = txns.read().await;
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
        let mut txns = txns.write().await;
        for (_, name) in names.into_iter().filter(|(id, _)| *id <= cutoff) {
            txns.delete(&name).await;
        }
        txns.sync().await.map_err(map_io)
    }

    #[cfg(test)]
    pub(crate) async fn has_transaction(&self, txn_id: TxnId) -> TCResult<bool> {
        let txns = child(self.root.clone(), TXN).await?;
        let txns = txns.read().await;
        Ok(txns.get_dir(&txn_id.to_string()).is_some())
    }
}

async fn write_record(
    dir: &DirLock<WorkspaceFile>,
    name: &str,
    record: WorkspaceFile,
) -> TCResult<()> {
    let size = record.get_size();
    let existing = {
        let dir = dir.read().await;
        dir.get_file(name).cloned()
    };
    if let Some(file) = existing {
        let mut contents = file.write_owned::<WorkspaceFile>().await.map_err(map_io)?;
        *contents = record;
        drop(contents);
    } else {
        let mut dir = dir.write().await;
        dir.create_file(name.to_string(), record, size)
            .await
            .map_err(map_io)?;
    }
    dir.write().await.sync().await.map_err(map_io)
}

async fn child(
    dir: DirLock<WorkspaceFile>,
    name: impl Into<String>,
) -> TCResult<DirLock<WorkspaceFile>> {
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
