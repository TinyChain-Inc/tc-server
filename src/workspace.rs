use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use freqfs::DirLock;
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
    transactions: DirLock<crate::storage::ApplicationBlock>,
    next_temp: Arc<AtomicU64>,
}

impl Workspace {
    pub(crate) fn from_roots(
        control: DirLock<ControlFile>,
        transactions: DirLock<crate::storage::ApplicationBlock>,
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
                let actor = rjwt::Actor::new(actor_id.to_string(), rjwt::AlgKind::Falcon512)
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

    pub(crate) async fn transaction_child(
        &self,
        txn_id: TxnId,
        path: &[String],
    ) -> TCResult<DirLock<crate::storage::ApplicationBlock>> {
        let mut dir = child(self.transactions.clone(), txn_id.to_string()).await?;
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

    pub(crate) fn unique_name(&self) -> String {
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
}

async fn write_record(dir: &DirLock<ControlFile>, name: &str, record: ControlFile) -> TCResult<()> {
    let existing = {
        let dir = dir.read().await;
        dir.get_file(name).cloned()
    };
    if let Some(file) = existing {
        return file.replace_all(record).await.map_err(map_io);
    } else {
        let mut dir = dir.write().await;
        dir.create_file(name.to_string(), record)
            .await
            .map_err(map_io)?;
    }

    dir.sync_all().await.map_err(map_io)
}

async fn child(
    dir: DirLock<crate::storage::ApplicationBlock>,
    name: impl Into<String>,
) -> TCResult<DirLock<crate::storage::ApplicationBlock>> {
    let mut dir = dir.write().await;
    let child = dir.get_or_create_dir(name.into()).map_err(map_io)?;
    dir.sync().await.map_err(map_io)?;
    Ok(child)
}

#[cfg(test)]
#[path = "../tests/support/workspace.rs"]
mod tests;

// Preserve host-control wire diagnostics while retaining native integrity provenance.
fn map_io(error: std::io::Error) -> TCError {
    if freqfs::IntegrityError::is_marked(&error) {
        error.into()
    } else {
        TCError::internal(error)
    }
}
