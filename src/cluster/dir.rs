use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use pathlink::PathSegment;
use semver::Version;
use tc_error::{TCError, TCResult};
use tc_ir::{Handler, Id, Map, Route, Transact, TxnId};

use crate::storage::ApplicationBlock;
use super::{AsyncHash, Cluster, DirItem, REPLICAS};

impl<T> Cluster<Dir<T>>
where
    T: Clone + Send + Sync + 'static,
{
    pub(crate) fn load<F, Fut>(
        txn_id: TxnId,
        storage: txfs::Dir<TxnId, ApplicationBlock>,
        path: pathlink::PathBuf,
        root_name: &'static str,
        protocol: Arc<crate::ProtocolAuthority>,
        gateway: Arc<dyn crate::replication::ClusterGateway>,
        load_item: F,
    ) -> Pin<Box<dyn Future<Output = TCResult<Self>> + Send>>
    where
        F: Fn(TxnId, txfs::Dir<TxnId, ApplicationBlock>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = TCResult<T>> + Send + 'static,
    {
        Box::pin(async move {
            let entries = storage
                .iter(txn_id)
                .await?
                .map(|(name, entry)| ((*name).clone(), (*entry).clone()))
                .collect::<Vec<_>>();
            let mut members = Vec::new();
            for (name, entry) in entries {
                let txfs::DirEntry::Dir(child) = entry else {
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
                    let identity = pathlink::Link::from(child_path);
                    crate::uri::validate_identity(&identity, root_name)?;
                    let item = load_item(txn_id, txfs::Dir::load(child).await?).await?;
                    DirEntry::Item(Cluster::new(
                        identity.path().clone(),
                        item,
                        Arc::clone(&protocol),
                        Arc::clone(&gateway),
                    ))
                } else {
                    if matches!(name.as_str(), ".txfs" | ".native") {
                        return Err(TCError::bad_request("reserved application storage segment"));
                    }
                    DirEntry::Dir(
                        Self::load(
                            txn_id,
                            txfs::Dir::load(child).await?,
                            child_path,
                            root_name,
                            Arc::clone(&protocol),
                            Arc::clone(&gateway),
                            load_item.clone(),
                        )
                        .await?,
                    )
                };
                members.push((name, member));
            }
            Ok(Cluster::new(
                path,
                Dir::from_committed(storage, members),
                protocol,
                gateway,
            ))
        })
    }
}

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

impl<T> Resolved<T>
where
    T: AsyncHash + Clone + Send + Sync + 'static,
{
    pub(crate) async fn exact_hash(self, txn: &crate::TxnHandle) -> TCResult<[u8; 32]> {
        match self.into_exact()? {
            DirEntry::Dir(cluster) => {
                let (txn, _call) = cluster.enter(txn).await?;
                cluster.state().hash(&txn).await
            }
            DirEntry::Item(cluster) => {
                let (txn, _call) = cluster.enter(txn).await?;
                cluster.state().hash(&txn).await
            }
        }
    }
}

impl<T> Resolved<T> {
    pub(super) fn into_exact(self) -> TCResult<DirEntry<T>> {
        match self {
            Self::Dir { cluster, unmatched } if unmatched.is_empty() => Ok(DirEntry::Dir(cluster)),
            Self::Item {
                cluster, suffix, ..
            } if suffix.is_empty() => Ok(DirEntry::Item(cluster)),
            _ => Err(TCError::not_found("bootstrap resource")),
        }
    }

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
                entry.insert(value.clone())?;
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
    pub(crate) async fn lookup(
        mut self,
        txn: &crate::TxnHandle,
        mut path: &[PathSegment],
    ) -> TCResult<Resolved<T>> {
        let mut ancestors = Vec::new();

        loop {
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

            let entry = self.state.entry(txn.id(), segment).await?;
            match entry {
                Some(DirEntry::Dir(dir)) => {
                    ancestors.push((self, segment.clone()));
                    self = dir;
                    path = suffix;
                }
                Some(DirEntry::Item(item)) => {
                    ancestors.push((self, segment.clone()));
                    return Ok(Resolved::Item {
                        cluster: item,
                        suffix: suffix.into(),
                        ancestors,
                    });
                }
                None => {
                    return Ok(Resolved::Dir {
                        cluster: self,
                        unmatched: path.into(),
                    });
                }
            }
        }
    }

    pub(crate) fn create_item_if_absent<'a, Input, Verify, Prepare, Create, Fut>(
        self,
        txn: &'a crate::TxnHandle,
        path: &'a [PathSegment],
        structural: &'a [Id],
        input: Input,
        verify: Verify,
        prepare: Prepare,
    ) -> Pin<Box<dyn Future<Output = TCResult<()>> + Send + 'a>>
    where
        Input: Send + 'a,
        Verify: FnOnce(&T, &Input) -> TCResult<()> + Send + 'a,
        Prepare: FnOnce(Input) -> ((tc_ir::Scalar, crate::State), Create) + Send + 'a,
        Create: FnOnce(txfs::Dir<TxnId, ApplicationBlock>) -> Fut + Send + 'a,
        Fut: Future<Output = TCResult<T>> + Send + 'a,
    {
        Box::pin(async move {
            self.check_structure(txn.id(), path).await?;
            match self.clone().lookup(txn, path).await? {
                Resolved::Item {
                    cluster, suffix, ..
                } if suffix.is_empty() => verify(cluster.state(), &input),
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
                    let peers = self.replication_peers(txn).await?;
                    let ((key, value), create) = prepare(input);
                    cluster
                        .create_item(txn, &structural[consumed..], create)
                        .await?;

                    self.replicate(
                        txn,
                        &peers,
                        &pathlink::Link::from(self.path().clone()),
                        key,
                        Some(value),
                    )
                    .await
                }
            }
        })
    }

    // A namespace decision recursively covers its members. Structural mutation
    // cannot overlap another enlisted descendant or a different replica set.
    async fn check_structure(&self, txn_id: TxnId, target: &[PathSegment]) -> TCResult<()> {
        let identities = self.replica_identities(txn_id).await?;
        let mut pending = vec![self.clone()];

        while let Some(directory) = pending.pop() {
            let entries = directory
                .state
                .members
                .iter(txn_id)
                .await
                .map_err(TCError::from)?;

            for (_, entry) in entries {
                match &*entry {
                    DirEntry::Dir(child) => {
                        child.reserve_structure(txn_id)?;
                        let suffix = &child.path()[self.path().len()..];
                        if target.starts_with(suffix)
                            && child.replica_identities(txn_id).await? != identities
                        {
                            return Err(TCError::conflict(
                                "structural mutation requires matching namespace replica sets",
                            ));
                        }

                        pending.push(child.clone());
                    }
                    DirEntry::Item(child) => {
                        child.reserve_structure(txn_id)?;
                        if target == &child.path()[self.path().len()..]
                            && child.replica_identities(txn_id).await? != identities
                        {
                            return Err(TCError::conflict(
                                "structural mutation requires matching namespace replica sets",
                            ));
                        }
                    }
                }
            }
        }

        Ok(())
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
                let item = create(txfs::Dir::load(storage).await?).await?;
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

                cluster.reserve_structure(txn.id())?;
                let DirEntry::Item(cluster) = self
                    .state
                    .create(txn.id(), name.clone(), DirEntry::Item(cluster))
                    .await?
                else {
                    unreachable!("directory returned the wrong entry type");
                };

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
                        Dir::empty(txfs::Dir::load(storage).await?),
                        Arc::clone(&self.protocol),
                        Arc::clone(&self.gateway),
                        replicas,
                    );

                    child.reserve_structure(txn.id())?;
                    let DirEntry::Dir(child) = self
                        .state
                        .create(txn.id(), name.clone(), DirEntry::Dir(child))
                        .await?
                    else {
                        unreachable!("directory returned the wrong entry type");
                    };

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
    pub(crate) fn dispatch<'a, Namespace>(
        self,
        txn: &'a crate::TxnHandle,
        target: &'a pathlink::Link,
        method: tc_ir::Method,
        body: Option<crate::State>,
        namespace: Namespace,
    ) -> impl Future<Output = TCResult<Option<crate::State>>> + Send + 'a
    where
        Namespace: Send + 'a,
        for<'handler> &'handler Namespace: Handler<'handler, crate::State>,
    {
        // Keep the three concrete directory dispatch frames out of the kernel's
        // recursive application polling frame on ordinary worker stacks.
        Box::pin(async move {
            let segments = target.path().get(1..).unwrap_or_default();
            match self.clone().lookup(txn, segments).await? {
                Resolved::Dir { cluster, unmatched } => {
                    if txn.is_locked() {
                        return cluster
                            .invoke(txn, unmatched, method, body, &target.to_string())
                            .await;
                    }

                    if method != tc_ir::Method::Get {
                        cluster.check_structure(txn.id(), &unmatched).await?;
                    }

                    let (txn, _call) = cluster.enter(txn).await?;
                    if segments.is_empty()
                        && unmatched.is_empty()
                        && method == tc_ir::Method::Put
                        && body.is_some()
                    {
                        return crate::kernel::invoke_handler(Box::new(&namespace), &txn, method, body)
                            .await
                            .map(Some);
                    }

                    cluster
                        .invoke(&txn, unmatched, method, body, &target.to_string())
                        .await
                }
                Resolved::Item {
                    cluster,
                    suffix,
                    ancestors,
                } => {
                    let explicit_delete = method == tc_ir::Method::Delete
                        && suffix.is_empty()
                        && body.as_ref().is_some_and(|state| {
                            matches!(
                                state,
                                tc_state::State::None
                                    | tc_state::State::Scalar(tc_ir::Scalar::Value(
                                        tc_value::Value::None
                                    ))
                            )
                        });

                    if explicit_delete && !txn.is_locked() {
                        let (txn, _call) = self.enter(txn).await?;
                        self.check_structure(txn.id(), segments).await?;
                        let peers = self.replication_peers(&txn).await?;

                        let txn = cluster.state().bind(&txn);
                        let state = cluster
                            .invoke(&txn, suffix, method, body, &target.to_string())
                            .await?;

                        remove_membership(&txn, ancestors).await?;

                        self.replicate(
                            &txn,
                            &peers,
                            cluster.state().identity(),
                            tc_ir::Scalar::default(),
                            None,
                        )
                        .await?;

                        return Ok(state);
                    }

                    if txn.is_locked() {
                        return cluster
                            .invoke(txn, suffix, method, body, &target.to_string())
                            .await;
                    }

                    let (txn, _call) = cluster.enter(txn).await?;
                    let txn = cluster.state().bind(&txn);
                    cluster
                        .invoke(&txn, suffix, method, body, &target.to_string())
                        .await
                }
            }
        })
    }
}

impl<T> Transact for Dir<T>
where
    T: Clone + Send + Sync + Transact + 'static,
{
    async fn commit(&self, txn_id: TxnId) -> TCResult<()> {
        self.storage.commit(txn_id).await.map_err(TCError::from)?;
        let (members, _) = self.members.read_and_commit(txn_id).await?;
        for member in members.values() {
            member.commit(txn_id).await?;
        }
        Ok(())
    }

    async fn rollback(&self, txn_id: &TxnId) -> TCResult<()> {
        self.storage
            .rollback(*txn_id)
            .await
            .map_err(TCError::from)?;

        let members = self.members.read_and_rollback(*txn_id).await?;
        for member in members.values() {
            member.rollback(txn_id).await?;
        }
        Ok(())
    }

    async fn finalize(&self, cutoff: &TxnId) -> TCResult<()> {
        if let Some(members) = self.members.read_and_finalize(*cutoff)? {
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

impl<T> AsyncHash for Dir<T>
where
    T: Clone + Send + Sync + 'static,
{
    async fn hash(&self, txn: &crate::TxnHandle) -> TCResult<[u8; 32]> {
        let entries = self.entries(txn.id()).await?;
        let ordered = entries
            .into_iter()
            .map(|(name, is_dir)| (name.to_string(), is_dir))
            .collect::<std::collections::BTreeMap<_, _>>();
        Ok(async_hash::Hash::<async_hash::Sha256>::hash(ordered).into())
    }
}

#[cfg(test)]
#[path = "../../tests/support/cluster_dir.rs"]
mod tests;
