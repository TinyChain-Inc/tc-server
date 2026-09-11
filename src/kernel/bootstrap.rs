use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use pathlink::PathBuf;
use semver::Version;
use tc_error::{TCError, TCResult};
use tc_ir::TxnId;

use crate::cluster::{Cluster, Dir, DirEntry};
use crate::replication::ClusterGateway;
use crate::storage::ApplicationBlock;

pub(super) fn load_dir<T, F, Fut>(
    txn_id: TxnId,
    storage: txfs::Dir<TxnId, ApplicationBlock>,
    path: PathBuf,
    root_name: &'static str,
    protocol: Arc<crate::ProtocolAuthority>,
    replicas: Arc<dyn ClusterGateway>,
    load: F,
) -> Pin<Box<dyn Future<Output = TCResult<Cluster<Dir<T>>>> + Send>>
where
    T: Clone + Send + Sync + 'static,
    F: Fn(TxnId, txfs::Dir<TxnId, ApplicationBlock>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = TCResult<T>> + Send + 'static,
{
    Box::pin(async move {
        let entries = storage.iter(txn_id).await.map_err(TCError::from)?;
        let mut members = Vec::new();
        for (name, entry) in entries {
            let name = (*name).clone();
            let txfs::DirEntry::Dir(child) = &*entry else {
                return Err(TCError::bad_request(format!(
                    "application files appear before a version at {path}/{name}",
                )));
            };
            let child_path = path.clone().append(name.clone());
            let member = if Version::parse(name.as_str()).is_ok() {
                if child_path.len() < 4 {
                    return Err(TCError::bad_request(
                        "an application version requires a publisher and resource path",
                    ));
                }
                let identity: pathlink::Link = child_path.to_string().parse().map_err(|error| {
                    TCError::bad_request(format!("invalid application identity: {error}"))
                })?;
                crate::uri::validate_identity(&identity, root_name)?;
                let item = load(txn_id, child.clone()).await?;
                DirEntry::Item(Cluster::new(
                    identity.path().clone(),
                    item,
                    Arc::clone(&protocol),
                    Arc::clone(&replicas),
                ))
            } else {
                if name.as_str() == ".txfs" {
                    return Err(TCError::bad_request(
                        ".txfs is a reserved application segment",
                    ));
                }
                DirEntry::Dir(
                    load_dir(
                        txn_id,
                        child.clone(),
                        child_path,
                        root_name,
                        Arc::clone(&protocol),
                        Arc::clone(&replicas),
                        load.clone(),
                    )
                    .await?,
                )
            };
            members.push((name, member));
        }
        Ok(Cluster::new(
            path.clone(),
            Dir::from_committed(storage, members),
            protocol,
            replicas,
        ))
    })
}
