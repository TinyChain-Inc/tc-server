use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use freqfs::{DirEntry as FsEntry, DirLock};
use pathlink::PathBuf;
use semver::Version;
use tc_error::{TCError, TCResult};
use tc_ir::Id;

use crate::cluster::{Cluster, Dir, DirEntry, Staging};
use crate::replication::ClusterGateway;
use crate::storage::{ApplicationFile, Leaf};

pub(super) struct BootstrapDir<T> {
    path: PathBuf,
    data: DirLock<ApplicationFile>,
    members: BTreeMap<Id, BootstrapEntry<T>>,
}

enum BootstrapEntry<T> {
    Dir(BootstrapDir<T>),
    Item(pathlink::Link, T),
}

impl<T> BootstrapDir<T>
where
    T: Send + 'static,
{
    pub(super) fn load<F, Fut>(
        root: DirLock<ApplicationFile>,
        canonical: DirLock<ApplicationFile>,
        path: PathBuf,
        root_name: &'static str,
        load: F,
    ) -> Pin<Box<dyn Future<Output = TCResult<Self>> + Send>>
    where
        F: Fn(Leaf) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = TCResult<T>> + Send + 'static,
    {
        Box::pin(async move {
            let mut members = BTreeMap::new();
            let entries = canonical
                .read()
                .await
                .iter()
                .map(|(name, entry)| (name.clone(), entry.clone()))
                .collect::<Vec<_>>();
            for (name, entry) in entries {
                let name: Id = name.parse().map_err(|error| {
                    TCError::bad_request(format!("invalid application path: {error}"))
                })?;
                if name.as_str() == ".txfs" {
                    return Err(TCError::bad_request(
                        ".txfs is a reserved application segment",
                    ));
                }
                let child_path = path.clone().append(name.clone());
                let FsEntry::Dir(child) = entry else {
                    return Err(TCError::bad_request(format!(
                        "unsupported application layout: {child_path} is a file"
                    )));
                };
                let child_entries = child
                    .read()
                    .await
                    .iter()
                    .map(|(_, entry)| entry.clone())
                    .collect::<Vec<_>>();
                let has_files = child_entries.iter().any(FsEntry::is_file);
                let has_dirs = child_entries.iter().any(FsEntry::is_dir);
                if has_files && has_dirs {
                    return Err(TCError::bad_request(format!(
                        "application leaf {child_path} contains a directory"
                    )));
                }
                let is_version = Version::parse(name.as_str()).is_ok();
                let member = if is_version {
                    if child_path.len() < 4 {
                        return Err(TCError::bad_request(
                            "an application version requires a publisher and resource path",
                        ));
                    }
                    if has_dirs {
                        return Err(TCError::bad_request(format!(
                            "application version {child_path} contains a directory"
                        )));
                    }
                    let identity: pathlink::Link =
                        child_path.to_string().parse().map_err(|error| {
                            TCError::bad_request(format!("invalid application identity: {error}"))
                        })?;
                    crate::application::validate_identity(&identity, root_name)?;
                    let item = load(Leaf::new(root.clone(), identity.clone())).await?;
                    BootstrapEntry::Item(identity, item)
                } else if has_files {
                    return Err(TCError::bad_request(format!(
                        "application files appear before a version: {child_path}"
                    )));
                } else {
                    BootstrapEntry::Dir(
                        Self::load(root.clone(), child, child_path, root_name, load.clone())
                            .await?,
                    )
                };
                members.insert(name, member);
            }
            Ok(Self {
                path,
                data: root,
                members,
            })
        })
    }

    pub(super) fn items(&self) -> Vec<&T> {
        let mut items = Vec::new();
        self.collect_items(&mut items);
        items
    }

    fn collect_items<'a>(&'a self, items: &mut Vec<&'a T>) {
        for member in self.members.values() {
            match member {
                BootstrapEntry::Dir(dir) => dir.collect_items(items),
                BootstrapEntry::Item(_, item) => items.push(item),
            }
        }
    }

    pub(super) fn try_map_items<U, F>(
        self,
        protocol: Arc<crate::ProtocolAuthority>,
        replicas: Arc<dyn ClusterGateway>,
        mut map: F,
    ) -> TCResult<Cluster<Dir<U>>>
    where
        U: Clone + Send + Sync + 'static,
        F: FnMut(T) -> TCResult<(U, Staging)>,
    {
        self.map_dir(&protocol, &replicas, &mut map)
    }

    fn map_dir<U, F>(
        self,
        protocol: &Arc<crate::ProtocolAuthority>,
        replicas: &Arc<dyn ClusterGateway>,
        map: &mut F,
    ) -> TCResult<Cluster<Dir<U>>>
    where
        U: Clone + Send + Sync + 'static,
        F: FnMut(T) -> TCResult<(U, Staging)>,
    {
        let mut members = Vec::with_capacity(self.members.len());
        for (name, member) in self.members {
            let member = match member {
                BootstrapEntry::Dir(dir) => DirEntry::Dir(dir.map_dir(protocol, replicas, map)?),
                BootstrapEntry::Item(identity, item) => {
                    let (item, staging) = map(item)?;
                    DirEntry::Item(Cluster::new(
                        identity.path().clone(),
                        item,
                        Arc::clone(protocol),
                        Arc::clone(replicas),
                        staging,
                    ))
                }
            };
            members.push((name, member));
        }
        Ok(Cluster::new(
            self.path.clone(),
            Dir::from_committed(self.path, self.data, members),
            Arc::clone(protocol),
            Arc::clone(replicas),
            Staging::default(),
        ))
    }
}
