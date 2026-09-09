use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use freqfs::DirLock;
use pathlink::{PathBuf, PathSegment};
use tc_error::{TCError, TCResult};
use tc_ir::{Handler, Id, Map, Route, Transact, TxnId};

use crate::replication::ClusterGateway;
use crate::storage::{ApplicationFile, Leaf, WorkspaceFile};

#[derive(Clone)]
pub(crate) enum HostResource {
    LibraryDir(Cluster<Dir<crate::library::Library>>),
    Library(Cluster<crate::library::Library>),
    ClassDir(Cluster<Dir<crate::class::Class>>),
    Class(Cluster<crate::class::Class>),
    ServiceDir(Cluster<Dir<crate::service::Service>>),
    Service(Cluster<crate::service::Service>),
}

macro_rules! host_resource {
    ($state:ty, $variant:ident) => {
        impl From<Cluster<$state>> for HostResource {
            fn from(cluster: Cluster<$state>) -> Self {
                Self::$variant(cluster)
            }
        }
    };
}

host_resource!(Dir<crate::library::Library>, LibraryDir);
host_resource!(crate::library::Library, Library);
host_resource!(Dir<crate::class::Class>, ClassDir);
host_resource!(crate::class::Class, Class);
host_resource!(Dir<crate::service::Service>, ServiceDir);
host_resource!(crate::service::Service, Service);

impl HostResource {
    pub(crate) fn path(&self) -> &PathBuf {
        match self {
            Self::LibraryDir(cluster) => cluster.path(),
            Self::Library(cluster) => cluster.path(),
            Self::ClassDir(cluster) => cluster.path(),
            Self::Class(cluster) => cluster.path(),
            Self::ServiceDir(cluster) => cluster.path(),
            Self::Service(cluster) => cluster.path(),
        }
    }

    pub(crate) async fn coordinate(
        &self,
        txn: &crate::TxnHandle,
        outcome: crate::txn::TransactionOutcome,
        require_mutation: bool,
    ) -> TCResult<()> {
        let Some(mut resources) = txn.lock_resources(self, require_mutation)? else {
            return Ok(());
        };
        resources.sort_by_key(|resource| std::cmp::Reverse(resource.path().len()));
        for resource in resources {
            resource.decide(txn, outcome).await?;
        }
        Ok(())
    }

    pub(crate) async fn decide(
        &self,
        txn: &crate::TxnHandle,
        outcome: crate::txn::TransactionOutcome,
    ) -> TCResult<()> {
        match self {
            Self::LibraryDir(cluster) => cluster.decide(txn, outcome).await,
            Self::Library(cluster) => cluster.decide(txn, outcome).await,
            Self::ClassDir(cluster) => cluster.decide(txn, outcome).await,
            Self::Class(cluster) => cluster.decide(txn, outcome).await,
            Self::ServiceDir(cluster) => cluster.decide(txn, outcome).await,
            Self::Service(cluster) => cluster.decide(txn, outcome).await,
        }
    }
}

pub(crate) struct ClaimState {
    pub(crate) signed: Option<Arc<crate::auth::SignedToken>>,
    pub(crate) coordinator: Option<HostResource>,
    pub(crate) resources: BTreeMap<PathBuf, (HostResource, bool)>,
    pub(crate) autocommit: bool,
}

#[derive(Clone, Default)]
pub(crate) struct Staging {
    active: Arc<RwLock<BTreeMap<TxnId, DirLock<WorkspaceFile>>>>,
}

impl Staging {
    pub(crate) fn bind(&self, txn_id: TxnId, dir: DirLock<WorkspaceFile>) {
        let mut active = self.active.write().expect("cluster staging write lock");
        active.entry(txn_id).or_insert(dir);
    }

    pub(crate) fn get(&self, txn_id: &TxnId) -> Option<DirLock<WorkspaceFile>> {
        self.active
            .read()
            .expect("cluster staging read lock")
            .get(txn_id)
            .cloned()
    }

    fn finalize(&self, cutoff: &TxnId) {
        self.active
            .write()
            .expect("cluster staging write lock")
            .retain(|txn_id, _| txn_id > cutoff);
    }
}

#[derive(Clone)]
pub(crate) struct Cluster<T> {
    path: PathBuf,
    state: Arc<T>,
    protocol: Arc<crate::ProtocolAuthority>,
    replicas: Arc<dyn ClusterGateway>,
    staging: Staging,
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
        replicas: Arc<dyn ClusterGateway>,
        staging: Staging,
    ) -> Self {
        Self {
            path,
            state: Arc::new(state),
            protocol,
            replicas,
            staging,
        }
    }

    pub(crate) fn path(&self) -> &PathBuf {
        &self.path
    }

    pub(crate) fn state(&self) -> &T {
        self.state.as_ref()
    }

    pub(crate) fn replicas(&self) -> &Arc<dyn ClusterGateway> {
        &self.replicas
    }

    pub(crate) fn protocol(&self) -> &Arc<crate::ProtocolAuthority> {
        &self.protocol
    }

    fn leads(&self, txn: &crate::TxnHandle) -> bool {
        txn.leader(&self.path)
            == Some((
                self.protocol.host.to_string(),
                self.protocol.actor.id().to_string(),
            ))
    }

    pub(crate) async fn replicate(
        &self,
        txn: &crate::TxnHandle,
        body: crate::replication::CanonicalBody,
    ) -> TCResult<()> {
        if !self.leads(txn) || !txn.has_signed_token() {
            return Ok(());
        }
        let peers = self.replicas.replicas(body.identity.path());
        if peers.is_empty() {
            return Ok(());
        }
        crate::replication::forward_install_to_peers(&peers, txn, body, self.replicas.as_ref())
            .await
            .map(drop)
    }

    pub(crate) async fn replicate_delete(
        &self,
        txn: &crate::TxnHandle,
        identity: &pathlink::Link,
    ) -> TCResult<()> {
        if !self.leads(txn) || !txn.has_signed_token() {
            return Ok(());
        }
        let peers = self.replicas.replicas(&self.path);
        if peers.is_empty() {
            return Ok(());
        }
        crate::replication::forward_delete_to_peers(&peers, txn, identity, self.replicas.as_ref())
            .await
    }

    pub(crate) async fn propagate_decision(
        &self,
        txn: &crate::TxnHandle,
        commit: bool,
    ) -> TCResult<()> {
        if !self.leads(txn) {
            return Ok(());
        }
        crate::replication::forward_resource_decision(
            &self.replicas.replicas(&self.path),
            txn,
            &self.path,
            commit,
            self.replicas.as_ref(),
        )
        .await
    }

    pub(crate) async fn decide(
        &self,
        txn: &crate::TxnHandle,
        outcome: crate::txn::TransactionOutcome,
    ) -> TCResult<()>
    where
        T: Transact,
    {
        self.propagate_decision(txn, outcome.commits()).await?;
        if outcome.commits() {
            self.state.commit(txn.id()).await
        } else {
            self.state.rollback(&txn.id()).await
        }
    }

    pub(crate) async fn decide_request(
        &self,
        txn: &crate::TxnHandle,
        suffix: &[PathSegment],
        method: tc_ir::Method,
        has_body: bool,
    ) -> TCResult<bool>
    where
        T: Transact,
    {
        if !txn.is_locked() {
            return Ok(false);
        }
        if !suffix.is_empty() || has_body {
            return Err(TCError::conflict(
                "a transaction decision must target an exact resource with no body",
            ));
        }
        let outcome = match method {
            tc_ir::Method::Put => crate::txn::TransactionOutcome::Commit,
            tc_ir::Method::Delete => crate::txn::TransactionOutcome::Rollback,
            _ => {
                return Err(TCError::conflict(
                    "a locked transaction accepts only bodyless PUT or DELETE",
                ));
            }
        };
        self.decide(txn, outcome).await?;
        Ok(true)
    }
}

impl<T> Cluster<T>
where
    T: Route<crate::State> + Transact,
{
    pub(crate) async fn invoke<'handler, 'txn>(
        &self,
        txn: &'txn crate::TxnHandle,
        suffix: Box<[PathSegment]>,
        method: tc_ir::Method,
        body: Option<crate::State>,
        root: Option<Box<dyn Handler<'handler, crate::State> + 'handler>>,
        target: &str,
    ) -> TCResult<Option<crate::State>>
    where
        'txn: 'handler,
    {
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

        let state = if let Some(root) =
            root.filter(|_| suffix.is_empty() && method == tc_ir::Method::Put)
        {
            crate::kernel::invoke_handler(root, txn, method, body).await?
        } else {
            let handler = self
                .route(&suffix)
                .ok_or_else(|| TCError::not_found(target))?;
            crate::kernel::invoke_handler(handler, txn, method, body).await?
        };
        Ok(Some(state))
    }
}

impl<T> Cluster<T>
where
    T: Clone,
    HostResource: From<Cluster<T>>,
{
    pub(crate) async fn claim(&self, txn: &crate::TxnHandle) -> TCResult<DirLock<WorkspaceFile>> {
        let resource = HostResource::from(self.clone());
        if txn.is_locked() {
            txn.leader(&self.path)
                .ok_or_else(|| TCError::conflict("decision target was not claimed"))?;
            txn.register_resource(resource)?;
        } else {
            txn.claim_cluster(&self.path, resource, &self.protocol)?;
        }
        let staging = txn.resource_context(&self.path).await?;
        self.staging.bind(txn.id(), staging.clone());
        Ok(staging)
    }
}

impl<T> Route<crate::State> for Cluster<T>
where
    T: Route<crate::State>,
{
    fn route<'a>(
        &'a self,
        path: &[PathSegment],
    ) -> Option<Box<dyn Handler<'a, crate::State> + 'a>> {
        self.state.route(path)
    }
}

impl<T> Transact for Cluster<T>
where
    T: Transact,
{
    fn commit(&self, txn_id: TxnId) -> impl Future<Output = TCResult<()>> + Send {
        self.state.commit(txn_id)
    }

    fn rollback(&self, txn_id: &TxnId) -> impl Future<Output = TCResult<()>> + Send {
        self.state.rollback(txn_id)
    }

    async fn finalize(&self, cutoff: &TxnId) -> TCResult<()> {
        self.state.finalize(cutoff).await?;
        self.staging.finalize(cutoff);
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) enum DirEntry<T> {
    Dir(Cluster<Dir<T>>),
    Item(Cluster<T>),
}

pub(crate) struct MembershipBinding<T> {
    ancestors: Vec<(Cluster<Dir<T>>, Id)>,
}

impl<T> MembershipBinding<T>
where
    T: Clone + Send + Sync + 'static,
    HostResource: From<Cluster<T>> + From<Cluster<Dir<T>>>,
{
    pub(crate) async fn remove(self, txn: &crate::TxnHandle) -> TCResult<()> {
        let mut ancestors = self.ancestors.into_iter().rev();
        let Some((parent, name)) = ancestors.next() else {
            return Err(TCError::conflict("an application root cannot be deleted"));
        };
        parent.state().delete(txn.id(), &name).await?;
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
                parent.state().delete(txn.id(), &name).await?;
                txn.mark_resource_mutated(parent.path())?;
            }
        }
        Ok(())
    }
}

pub(crate) enum Resolved<T> {
    Dir {
        cluster: Cluster<Dir<T>>,
        unmatched: Box<[Id]>,
    },
    Item {
        cluster: Cluster<T>,
        suffix: Box<[Id]>,
        binding: MembershipBinding<T>,
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

#[derive(Clone)]
pub(crate) struct Dir<T> {
    path: PathBuf,
    data: DirLock<ApplicationFile>,
    members: txn_lock::map::TxnMapLock<TxnId, Id, DirEntry<T>>,
    removed: Arc<RwLock<Removed<T>>>,
}

type Removed<T> = BTreeMap<TxnId, BTreeMap<Id, DirEntry<T>>>;

impl<T> fmt::Debug for Dir<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "directory at {}", self.path)
    }
}

impl<T> Dir<T> {
    pub(crate) fn from_committed(
        path: PathBuf,
        data: DirLock<ApplicationFile>,
        members: impl IntoIterator<Item = (Id, DirEntry<T>)>,
    ) -> Self {
        Self {
            path,
            data,
            members: txn_lock::map::TxnMapLock::from_committed(members),
            removed: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub(crate) fn empty(path: PathBuf, data: DirLock<ApplicationFile>) -> Self {
        Self::from_committed(path, data, [])
    }

    pub(crate) fn leaf(&self, identity: pathlink::Link) -> Leaf {
        Leaf::new(self.data.clone(), identity)
    }
}

impl<T> Dir<T>
where
    T: Clone + Send + Sync + 'static,
{
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

    pub(crate) async fn delete(&self, txn_id: TxnId, name: &Id) -> TCResult<()> {
        let removed = self
            .entry(txn_id, name)
            .await?
            .ok_or_else(|| TCError::not_found(name.to_string()))?;
        self.members
            .remove(txn_id, name)
            .await
            .map_err(TCError::from)
            .map(drop)?;
        self.removed
            .write()
            .expect("removed application members write lock")
            .entry(txn_id)
            .or_default()
            .insert(name.clone(), removed);
        Ok(())
    }

    fn finalize_tree<'a>(
        &'a self,
        cutoff: &'a TxnId,
    ) -> Pin<Box<dyn Future<Output = TCResult<()>> + Send + 'a>>
    where
        T: Transact,
    {
        Box::pin(async move {
            let members = self.members.read_and_finalize(*cutoff);
            let removed = {
                let mut versions = self
                    .removed
                    .write()
                    .expect("removed application members write lock");
                let expired = versions
                    .keys()
                    .copied()
                    .take_while(|txn_id| txn_id <= cutoff)
                    .collect::<Vec<_>>();
                expired
                    .into_iter()
                    .filter_map(|txn_id| versions.remove(&txn_id))
                    .flat_map(BTreeMap::into_values)
                    .collect::<Vec<_>>()
            };
            if let Some(members) = members {
                let children = members.into_values().collect::<Vec<_>>();
                for child in children {
                    match child.as_ref() {
                        DirEntry::Dir(dir) => {
                            dir.state.finalize_tree(cutoff).await?;
                            dir.staging.finalize(cutoff);
                        }
                        DirEntry::Item(item) => item.finalize(cutoff).await?,
                    }
                }
            }
            for child in removed {
                match child {
                    DirEntry::Dir(dir) => dir.finalize(cutoff).await?,
                    DirEntry::Item(item) => item.finalize(cutoff).await?,
                }
            }
            Ok(())
        })
    }
}

impl<T> Cluster<Dir<T>>
where
    T: Clone + Send + Sync + 'static,
    HostResource: From<Cluster<T>> + From<Cluster<Dir<T>>>,
{
    pub(crate) fn lookup<'a>(
        self,
        txn: &'a crate::TxnHandle,
        path: &'a [Id],
    ) -> Pin<Box<dyn Future<Output = TCResult<Resolved<T>>> + Send + 'a>> {
        self.lookup_from(txn, path, Vec::new())
    }

    pub(crate) async fn exact(
        self,
        txn: &crate::TxnHandle,
        path: &[Id],
    ) -> TCResult<Option<Cluster<T>>> {
        self.lookup(txn, path).await?.exact_item()
    }

    fn lookup_from<'a>(
        self,
        txn: &'a crate::TxnHandle,
        path: &'a [Id],
        mut ancestors: Vec<(Cluster<Dir<T>>, Id)>,
    ) -> Pin<Box<dyn Future<Output = TCResult<Resolved<T>>> + Send + 'a>> {
        Box::pin(async move {
            self.claim(txn).await?;
            let Some((name, suffix)) = path.split_first() else {
                return Ok(Resolved::Dir {
                    cluster: self,
                    unmatched: Box::new([]),
                });
            };
            let entry = self.state.entry(txn.id(), name).await?.or_else(|| {
                txn.is_locked()
                    .then(|| {
                        self.state
                            .removed
                            .read()
                            .expect("removed application members read lock")
                            .get(&txn.id())
                            .and_then(|removed| removed.get(name))
                            .cloned()
                    })
                    .flatten()
            });
            match entry {
                Some(DirEntry::Dir(dir)) => {
                    ancestors.push((self, name.clone()));
                    dir.lookup_from(txn, suffix, ancestors).await
                }
                Some(DirEntry::Item(item)) => {
                    item.claim(txn).await?;
                    ancestors.push((self, name.clone()));
                    Ok(Resolved::Item {
                        cluster: item.clone(),
                        suffix: suffix.into(),
                        binding: MembershipBinding { ancestors },
                    })
                }
                None => Ok(Resolved::Dir {
                    cluster: self,
                    unmatched: path.into(),
                }),
            }
        })
    }

    pub(crate) fn insert<'a>(
        self,
        txn: &'a crate::TxnHandle,
        path: &'a [Id],
        item: Cluster<T>,
    ) -> Pin<Box<dyn Future<Output = TCResult<Cluster<T>>> + Send + 'a>> {
        Box::pin(async move {
            self.claim(txn).await?;
            let Some((name, suffix)) = path.split_first() else {
                return Err(TCError::bad_request("an application item requires a name"));
            };
            if suffix.is_empty() {
                let DirEntry::Item(item) = self
                    .state
                    .create(txn.id(), name.clone(), DirEntry::Item(item))
                    .await?
                else {
                    return Err(TCError::internal("directory returned the wrong entry type"));
                };
                item.claim(txn).await?;
                txn.mark_resource_mutated(self.path())?;
                return Ok(item);
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
                    let child = Cluster::new(
                        child_path.clone(),
                        Dir::empty(child_path, self.state.data.clone()),
                        Arc::clone(&self.protocol),
                        Arc::clone(self.replicas()),
                        Staging::default(),
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
            child.insert(txn, suffix, item).await
        })
    }
}

impl<T> Cluster<Dir<T>>
where
    T: crate::application::ApplicationItem,
    HostResource: From<Cluster<T>> + From<Cluster<Dir<T>>>,
{
    pub(crate) async fn stage_item(&self, txn: &crate::TxnHandle, item: T) -> TCResult<Cluster<T>> {
        let identity = item.identity().clone();
        let segments = crate::application::identity_segments(&identity)?;
        if let Some(existing) = self.clone().lookup(txn, &segments).await?.exact_item()? {
            return if existing.state().has_same_content(&item) {
                Ok(existing)
            } else {
                Err(TCError::conflict(format!(
                    "immutable application version {identity} has conflicting content"
                )))
            };
        }
        let staging = item.staging();
        let cluster = Cluster::new(
            identity.path().clone(),
            item,
            Arc::clone(self.protocol()),
            Arc::clone(self.replicas()),
            staging,
        );
        let cluster = self.clone().insert(txn, &segments, cluster).await?;
        cluster.state().stage(txn).await?;
        txn.mark_resource_mutated(cluster.path())?;
        Ok(cluster)
    }
}

impl<T> Transact for Dir<T>
where
    T: Clone + Send + Sync + Transact + 'static,
{
    async fn commit(&self, txn_id: TxnId) -> TCResult<()> {
        self.members.commit(txn_id);
        Ok(())
    }

    async fn rollback(&self, txn_id: &TxnId) -> TCResult<()> {
        self.members.rollback(txn_id);
        Ok(())
    }

    async fn finalize(&self, cutoff: &TxnId) -> TCResult<()> {
        self.finalize_tree(cutoff).await
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
