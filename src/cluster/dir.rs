use std::{fmt, future::Future, pin::Pin, sync::Arc};

use pathlink::PathSegment;
use tc_error::{TCError, TCResult};
use tc_ir::{Handler, Id, Map, Route, Transact, TxnId};

use super::{Cluster, DirItem, REPLICAS, ResourceHash};
use crate::storage::ApplicationBlock;

#[derive(Clone)]
pub(crate) enum DirEntry<T> {
    Dir(Cluster<Dir<T>>),
    Item(Cluster<T>),
}

pub(crate) enum Resolved<T> {
    Dir {
        cluster: Cluster<Dir<T>>,
        unmatched: Box<[PathSegment]>,
    },
    Item {
        cluster: Cluster<T>,
        suffix: Box<[PathSegment]>,
        ancestors: Vec<(Cluster<Dir<T>>, Id)>,
    },
}

impl<T> Resolved<T> {
    pub(crate) fn exact_item(self) -> TCResult<Option<Cluster<T>>> {
        match self {
            Self::Item {
                cluster, suffix, ..
            } if suffix.is_empty() => Ok(Some(cluster)),
            Self::Dir { unmatched, .. } if !unmatched.is_empty() => Ok(None),
            Self::Item { .. } => Ok(None),
            Self::Dir { .. } => Err(TCError::conflict("application path is not an item")),
        }
    }
}

impl<T> fmt::Debug for DirEntry<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Dir(cluster) => cluster.fmt(f),
            Self::Item(cluster) => cluster.fmt(f),
        }
    }
}

impl<T> Transact for DirEntry<T>
where
    T: Clone + Send + Sync + Transact + 'static,
{
    fn commit(&self, txn_id: TxnId) -> impl Future<Output = TCResult<()>> + Send {
        Box::pin(async move {
            match self {
                Self::Dir(cluster) => cluster.commit(txn_id).await,
                Self::Item(cluster) => cluster.commit(txn_id).await,
            }
        })
    }

    fn rollback(&self, txn_id: &TxnId) -> impl Future<Output = TCResult<()>> + Send {
        Box::pin(async move {
            match self {
                Self::Dir(cluster) => cluster.rollback(txn_id).await,
                Self::Item(cluster) => cluster.rollback(txn_id).await,
            }
        })
    }

    fn finalize(&self, cutoff: &TxnId) -> impl Future<Output = TCResult<()>> + Send {
        Box::pin(async move {
            match self {
                Self::Dir(cluster) => cluster.finalize(cutoff).await,
                Self::Item(cluster) => cluster.finalize(cutoff).await,
            }
        })
    }
}

async fn remove_membership<T>(
    txn: &crate::TxnHandle,
    ancestors: Vec<(Cluster<Dir<T>>, Id)>,
) -> TCResult<()>
where
    T: Clone + Send + Sync + 'static,
{
    let mut ancestors = ancestors.into_iter().rev();
    let Some((parent, name)) = ancestors.next() else {
        return Err(TCError::conflict("an application root cannot be deleted"));
    };
    parent.state().delete(txn.id(), name).await?;
    txn.mark_resource_mutated(parent.path())?;
    let mut empty = parent.state().entries(txn.id()).await?.is_empty();
    for (parent, name) in ancestors {
        if !empty {
            break;
        }
        let Some(DirEntry::Dir(child)) = parent.state().entry(txn.id(), &name).await? else {
            break;
        };
        empty = child.state().entries(txn.id()).await?.is_empty();
        if empty {
            parent.state().delete(txn.id(), name).await?;
            txn.mark_resource_mutated(parent.path())?;
        }
    }
    Ok(())
}

#[derive(Clone)]
pub(crate) struct Dir<T> {
    storage: txfs::Dir<TxnId, ApplicationBlock>,
    members: txn_lock::map::TxnMapLock<TxnId, Id, DirEntry<T>>,
}

impl<T> fmt::Debug for Dir<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("recursive application directory")
    }
}

impl<T> Dir<T> {
    pub(crate) fn from_committed(
        storage: txfs::Dir<TxnId, ApplicationBlock>,
        members: impl IntoIterator<Item = (Id, DirEntry<T>)>,
    ) -> Self {
        Self {
            storage,
            members: txn_lock::map::TxnMapLock::from_committed(members),
        }
    }

    pub(crate) fn empty(storage: txfs::Dir<TxnId, ApplicationBlock>) -> Self {
        Self::from_committed(storage, [])
    }
}

impl<T> Dir<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub(crate) fn items(
        &self,
        txn_id: TxnId,
    ) -> Pin<Box<dyn Future<Output = TCResult<Vec<T>>> + Send + '_>> {
        Box::pin(async move {
            let entries = self.members.iter(txn_id).await.map_err(TCError::from)?;
            let mut items = Vec::new();
            for (_, entry) in entries {
                match &*entry {
                    DirEntry::Dir(dir) => items.extend(dir.state().items(txn_id).await?),
                    DirEntry::Item(item) => items.push(item.state().clone()),
                }
            }
            Ok(items)
        })
    }

    pub(crate) async fn entries(&self, txn_id: TxnId) -> TCResult<Map<bool>> {
        Ok(self
            .members
            .iter(txn_id)
            .await
            .map_err(TCError::from)?
            .map(|(name, entry)| ((*name).clone(), matches!(&*entry, DirEntry::Dir(_))))
            .collect())
    }

    pub(crate) async fn entry(&self, txn_id: TxnId, name: &Id) -> TCResult<Option<DirEntry<T>>> {
        self.members
            .get(txn_id, name)
            .await
            .map_err(TCError::from)
            .map(|entry| entry.map(|entry| entry.clone()))
    }

    async fn create(&self, txn_id: TxnId, name: Id, value: DirEntry<T>) -> TCResult<DirEntry<T>> {
        use txn_lock::map::Entry;
        match self
            .members
            .entry(txn_id, name)
            .await
            .map_err(TCError::from)?
        {
            Entry::Vacant(entry) => {
                entry.insert(value.clone());
                Ok(value)
            }
            Entry::Occupied(entry)
                if matches!(entry.get(), DirEntry::Dir(_)) == matches!(value, DirEntry::Dir(_)) =>
            {
                Ok(entry.get().clone())
            }
            Entry::Occupied(_) => Err(TCError::conflict(
                "an application item and directory cannot share a path",
            )),
        }
    }

    pub(crate) async fn delete(&self, txn_id: TxnId, name: Id) -> TCResult<()> {
        self.entry(txn_id, &name)
            .await?
            .ok_or_else(|| TCError::not_found(name.to_string()))?;
        self.members
            .remove(txn_id, &name)
            .await
            .map_err(TCError::from)
            .map(|_| ())?;
        self.storage
            .delete(txn_id, name)
            .await
            .map_err(TCError::from)?;
        Ok(())
    }
}

impl<T> Cluster<Dir<T>>
where
    T: Clone + Send + Sync + 'static,
{
    pub(crate) fn lookup<'a>(
        self,
        txn: &'a crate::TxnHandle,
        path: &'a [PathSegment],
    ) -> Pin<Box<dyn Future<Output = TCResult<Resolved<T>>> + Send + 'a>> {
        self.lookup_from(txn, path, Vec::new())
    }

    pub(crate) fn create_item_if_absent<'a, Verify, Create, Fut>(
        self,
        txn: &'a crate::TxnHandle,
        path: &'a [PathSegment],
        structural: &'a [Id],
        verify: Verify,
        create: Create,
    ) -> Pin<Box<dyn Future<Output = TCResult<()>> + Send + 'a>>
    where
        Verify: FnOnce(&T) -> TCResult<()> + Send + 'a,
        Create: FnOnce(Cluster<Dir<T>>, Vec<Id>) -> Fut + Send + 'a,
        Fut: Future<Output = TCResult<()>> + Send + 'a,
    {
        Box::pin(async move {
            match self.lookup(txn, path).await? {
                Resolved::Item {
                    cluster, suffix, ..
                } if suffix.is_empty() => verify(cluster.state()),
                Resolved::Item { .. } => Err(TCError::conflict(
                    "an application item blocks this immutable identity",
                )),
                Resolved::Dir { cluster, unmatched } => {
                    if unmatched.is_empty() {
                        return Err(TCError::conflict(
                            "a directory already occupies this immutable identity",
                        ));
                    }
                    let consumed = cluster.path().len().saturating_sub(1);
                    create(cluster, structural[consumed..].to_vec()).await
                }
            }
        })
    }

    fn lookup_from<'a>(
        self,
        txn: &'a crate::TxnHandle,
        path: &'a [PathSegment],
        mut ancestors: Vec<(Cluster<Dir<T>>, Id)>,
    ) -> Pin<Box<dyn Future<Output = TCResult<Resolved<T>>> + Send + 'a>> {
        Box::pin(async move {
            self.claim(txn).await?;
            let Some((segment, suffix)) = path.split_first() else {
                return Ok(Resolved::Dir {
                    cluster: self,
                    unmatched: Box::new([]),
                });
            };
            if segment.as_str() == REPLICAS {
                return Ok(Resolved::Dir {
                    cluster: self,
                    unmatched: path.into(),
                });
            }
            let Ok(name) = segment.as_str().parse::<Id>() else {
                return Ok(Resolved::Dir {
                    cluster: self,
                    unmatched: path.into(),
                });
            };
            let entry = self.state.entry(txn.id(), &name).await?;
            match entry {
                Some(DirEntry::Dir(dir)) => {
                    ancestors.push((self, name));
                    dir.lookup_from(txn, suffix, ancestors).await
                }
                Some(DirEntry::Item(item)) => {
                    item.claim(txn).await?;
                    ancestors.push((self, name));
                    Ok(Resolved::Item {
                        cluster: item.clone(),
                        suffix: suffix.into(),
                        ancestors,
                    })
                }
                None => Ok(Resolved::Dir {
                    cluster: self,
                    unmatched: path.into(),
                }),
            }
        })
    }

    pub(crate) fn create_item<'a, F, Fut>(
        self,
        txn: &'a crate::TxnHandle,
        path: &'a [Id],
        create: F,
    ) -> Pin<Box<dyn Future<Output = TCResult<Cluster<T>>> + Send + 'a>>
    where
        F: FnOnce(txfs::Dir<TxnId, ApplicationBlock>) -> Fut + Send + 'a,
        Fut: Future<Output = TCResult<T>> + Send + 'a,
    {
        Box::pin(async move {
            self.claim(txn).await?;
            let Some((name, suffix)) = path.split_first() else {
                return Err(TCError::bad_request("an application item requires a name"));
            };
            if name.as_str() == REPLICAS {
                return Err(TCError::bad_request("replicas is reserved by Cluster"));
            }
            if suffix.is_empty() {
                if self.state.entry(txn.id(), name).await?.is_some() {
                    return Err(TCError::conflict("application item already exists"));
                }
                let storage = self
                    .state
                    .storage
                    .create_dir(txn.id(), name.clone())
                    .await
                    .map_err(TCError::from)?;
                let item = create(storage).await?;
                let path = self.path().clone().append(name.clone());
                let replicas = self
                    .replica_snapshot(txn.id())
                    .await?
                    .into_iter()
                    .map(|replica| (replica.endpoint.clone(), replica));
                let cluster = Cluster::with_replicas(
                    path,
                    item,
                    Arc::clone(&self.protocol),
                    Arc::clone(&self.gateway),
                    replicas,
                );
                let DirEntry::Item(cluster) = self
                    .state
                    .create(txn.id(), name.clone(), DirEntry::Item(cluster))
                    .await?
                else {
                    return Err(TCError::internal("directory returned the wrong entry type"));
                };
                cluster.claim(txn).await?;
                txn.mark_resource_mutated(self.path())?;
                txn.mark_resource_mutated(cluster.path())?;
                return Ok(cluster);
            }
            let child = match self.state.entry(txn.id(), name).await? {
                Some(DirEntry::Dir(dir)) => dir,
                Some(DirEntry::Item(_)) => {
                    return Err(TCError::conflict(
                        "an application item blocks this directory path",
                    ));
                }
                None => {
                    let child_path = self.path().clone().append(name.clone());
                    let storage = self
                        .state
                        .storage
                        .create_dir(txn.id(), name.clone())
                        .await
                        .map_err(TCError::from)?;
                    let replicas = self
                        .replica_snapshot(txn.id())
                        .await?
                        .into_iter()
                        .map(|replica| (replica.endpoint.clone(), replica));
                    let child = Cluster::with_replicas(
                        child_path,
                        Dir::empty(storage),
                        Arc::clone(&self.protocol),
                        Arc::clone(&self.gateway),
                        replicas,
                    );
                    let DirEntry::Dir(child) = self
                        .state
                        .create(txn.id(), name.clone(), DirEntry::Dir(child))
                        .await?
                    else {
                        return Err(TCError::internal("directory returned the wrong entry type"));
                    };
                    txn.mark_resource_mutated(self.path())?;
                    child
                }
            };
            child.create_item(txn, suffix, create).await
        })
    }
}

impl<T> Cluster<Dir<T>>
where
    T: DirItem,
{
    pub(crate) async fn dispatch<'a>(
        self,
        txn: &'a crate::TxnHandle,
        target: &pathlink::Link,
        method: tc_ir::Method,
        body: Option<crate::State>,
        namespace: Box<dyn Handler<'a, crate::State> + 'a>,
    ) -> TCResult<Option<crate::State>> {
        let segments = target.path().get(1..).unwrap_or_default();
        match self.lookup(txn, segments).await? {
            Resolved::Dir { cluster, unmatched } => {
                if segments.is_empty()
                    && unmatched.is_empty()
                    && method == tc_ir::Method::Put
                    && body.is_some()
                    && !txn.is_locked()
                {
                    return crate::kernel::invoke_handler(namespace, txn, method, body)
                        .await
                        .map(Some);
                }
                cluster
                    .invoke(txn, unmatched, method, body, &target.to_string())
                    .await
            }
            Resolved::Item {
                cluster,
                suffix,
                ancestors,
            } => {
                let explicit_delete = method == tc_ir::Method::Delete
                    && suffix.is_empty()
                    && matches!(
                        &body,
                        Some(crate::State::None)
                            | Some(crate::State::Scalar(tc_ir::Scalar::Value(
                                tc_value::Value::None
                            )))
                    );
                let txn = cluster.state().bind(txn);
                let state = cluster
                    .clone()
                    .invoke(&txn, suffix, method, body, &target.to_string())
                    .await?;
                if explicit_delete {
                    remove_membership(&txn, ancestors).await?;
                    cluster
                        .replicate(
                            &txn,
                            cluster.state().identity(),
                            tc_ir::Method::Delete,
                            tc_ir::Scalar::Value(tc_value::Value::None),
                            None,
                        )
                        .await?;
                }
                Ok(state)
            }
        }
    }
}

impl<T> Transact for Dir<T>
where
    T: Clone + Send + Sync + Transact + 'static,
{
    async fn commit(&self, txn_id: TxnId) -> TCResult<()> {
        self.storage
            .commit(txn_id, false)
            .await
            .map_err(TCError::from)?;
        let (members, _) = self.members.read_and_commit(txn_id).await;
        for member in members.values() {
            member.commit(txn_id).await?;
        }
        Ok(())
    }

    async fn rollback(&self, txn_id: &TxnId) -> TCResult<()> {
        self.storage
            .rollback(*txn_id, false)
            .await
            .map_err(TCError::from)?;
        let (members, _) = self.members.read_and_rollback(*txn_id).await;
        for member in members.values() {
            member.rollback(txn_id).await?;
        }
        Ok(())
    }

    async fn finalize(&self, cutoff: &TxnId) -> TCResult<()> {
        if let Some(members) = self.members.read_and_finalize(*cutoff) {
            for member in members.values() {
                member.finalize(cutoff).await?;
            }
        }
        self.storage.finalize(*cutoff).await.map_err(TCError::from)
    }
}

impl<'a, T> Handler<'a, crate::State> for &'a Dir<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn get<'txn>(self: Box<Self>) -> Option<tc_ir::GetHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, _key| {
            Box::pin(async move { self.entries(txn.id()).await.map(crate::State::from) })
        }))
    }
}

impl<T> Route<crate::State> for Dir<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn route<'a>(
        &'a self,
        path: &[PathSegment],
    ) -> Option<Box<dyn Handler<'a, crate::State> + 'a>> {
        path.is_empty()
            .then(|| Box::new(self) as Box<dyn Handler<'a, crate::State>>)
    }
}

impl<T> ResourceHash for Dir<T>
where
    T: Clone + Send + Sync + 'static,
{
    async fn resource_hash(&self, txn_id: TxnId) -> TCResult<[u8; 32]> {
        let entries = self.entries(txn_id).await?;
        let ordered = entries
            .into_iter()
            .map(|(name, is_dir)| (name.to_string(), is_dir))
            .collect::<std::collections::BTreeMap<_, _>>();
        Ok(async_hash::Hash::<async_hash::Sha256>::hash(ordered).into())
    }
}

#[cfg(test)]
mod tests {
    use super::{Cluster, Resolved};
    use crate::replication::Replica;
    use tc_ir::{Scalar, Transact, TxnId};
    use txn_lock::map::Entry;
    #[tokio::test]
    async fn root_lifecycle_delegates_to_nested_cluster_replica_state() {
        let kernel = crate::txn::test_kernel("nested-cluster-lifecycle").await;
        let identity: pathlink::Link = "/service/example/nested/resource/1.0.0"
            .parse()
            .expect("Service identity");
        let segments = crate::uri::validate_identity(&identity, "service").expect("identity");
        let path = identity.path()[1..].to_vec();
        let definition = Scalar::Map(tc_ir::Map::new());
        let create_txn = kernel.test_txn().await;
        let create_txn_id = create_txn.id();
        kernel
            .inner
            .services
            .clone()
            .create_item(&create_txn, &segments, move |storage| {
                crate::service::Service::create(create_txn_id, storage, identity, definition)
            })
            .await
            .expect("create nested Service");
        kernel
            .inner
            .services
            .commit(create_txn_id)
            .await
            .expect("commit nested tree");
        let txn = kernel.test_txn().await;
        let Resolved::Item { ancestors, .. } = kernel
            .inner
            .services
            .clone()
            .lookup(&txn, &path)
            .await
            .expect("resolve nested Service")
        else {
            panic!("expected nested Service item");
        };
        let parent = &ancestors.last().expect("parent directory").0;
        let replica = Replica {
            endpoint: "http://replica.example".into(),
            host: "/host".into(),
            actor_id: "replica".into(),
            algorithm: rjwt::AlgKind::Falcon512,
            public_key_b64: "key".into(),
        };
        insert_replica(parent, txn.id(), replica.clone()).await;
        kernel
            .inner
            .services
            .commit(txn.id())
            .await
            .expect("recursive commit");
        assert!(has_replica(parent, txn.id(), &replica.endpoint).await);
        let rollback_txn = kernel.test_txn().await;
        parent
            .replicas
            .remove(rollback_txn.id(), &replica.endpoint)
            .await
            .expect("stage replica removal");
        kernel
            .inner
            .services
            .rollback(&rollback_txn.id())
            .await
            .expect("recursive rollback");
        assert!(has_replica(parent, rollback_txn.id(), &replica.endpoint).await);
        let pending_txn = kernel.test_txn().await;
        let pending = Replica {
            endpoint: "http://pending.example".into(),
            ..replica
        };
        insert_replica(parent, pending_txn.id(), pending.clone()).await;
        kernel
            .inner
            .services
            .finalize(&pending_txn.id())
            .await
            .expect("recursive finalization");
        assert!(!has_replica(parent, pending_txn.id(), &pending.endpoint).await);
    }
    async fn has_replica<T>(cluster: &Cluster<T>, txn_id: TxnId, endpoint: &str) -> bool {
        cluster
            .replica_snapshot(txn_id)
            .await
            .unwrap()
            .iter()
            .any(|known| known.endpoint == endpoint)
    }
    async fn insert_replica<T>(cluster: &Cluster<T>, txn_id: TxnId, replica: Replica) {
        let entry = cluster
            .replicas
            .entry(txn_id, replica.endpoint.clone())
            .await
            .unwrap();
        let Entry::Vacant(entry) = entry else {
            panic!("new replica already exists")
        };
        entry.insert(replica);
    }
}
