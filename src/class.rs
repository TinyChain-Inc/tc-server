use std::sync::Arc;

use pathlink::{Link, PathSegment};
use tc_error::{TCError, TCResult};
use tc_ir::{
    DeleteHandler, GetHandler, Handler, PostHandler, Public, PutHandler, Route, Transact, TxnId,
};
use tc_state::{ClassBody, ClassDef};

use crate::storage::ApplicationBlock;

const MANIFEST: &str = "manifest.json";
pub(crate) const MAX_CLASS_BYTES: usize = 1024 * 1024;

pub(crate) struct Root<'a>(pub(crate) &'a crate::cluster::Cluster<crate::cluster::Dir<Class>>);

impl<'a, 'runtime: 'a> Handler<'a, crate::State> for Root<'runtime> {
    fn put<'txn>(self: Box<Self>) -> Option<PutHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, key, value| {
            Box::pin(async move {
                let (identity, definition) = crate::literal::into_put(key, value)?;
                let segments = crate::uri::validate_identity(&identity, "class")?;
                if !txn.may_mutate(&identity, self.0.path()) {
                    return Err(TCError::unauthorized("unauthorized Class install"));
                }
                let body = ClassBody::try_from(definition.clone())
                    .map_err(|error| TCError::bad_request(error.to_string()))?;
                let class = ClassDef::from_body(identity.clone(), body);
                let expected = class.clone();
                let conflict_identity = identity.clone();
                let path = identity.path()[1..].to_vec();
                self.0
                    .clone()
                    .create_item_if_absent(
                        txn,
                        &path,
                        &segments,
                        move |existing| {
                            (existing.class == expected).then_some(()).ok_or_else(|| {
                                TCError::conflict(format!(
                                    "immutable Class version {conflict_identity} has conflicting content"
                                ))
                            })
                        },
                        move |parent, remaining| async move {
                            self.0
                                .replicate(
                                    txn,
                                    &"/class".parse().expect("Class root"),
                                    tc_ir::Method::Put,
                                    tc_ir::Scalar::Value(tc_value::Value::Link(identity.clone())),
                                    Some(crate::State::from_scalar(definition)),
                                )
                                .await?;
                            parent
                                .create_item(txn, &remaining, move |storage| {
                                    Class::create(txn.id(), storage, class)
                                })
                                .await
                                .map(|_| ())
                        },
                    )
                    .await
            })
        }))
    }
}

#[derive(Clone)]
pub struct Class {
    storage: txfs::Dir<TxnId, ApplicationBlock>,
    class: ClassDef,
    scope: Arc<crate::txn::DependencyScope>,
}

impl Class {
    pub(crate) async fn create(
        txn_id: TxnId,
        storage: txfs::Dir<TxnId, ApplicationBlock>,
        class: ClassDef,
    ) -> TCResult<Self> {
        storage
            .create_file(
                txn_id,
                MANIFEST.parse().expect("manifest file name"),
                ApplicationBlock::Manifest(class.identity().clone(), class.body().definition()),
            )
            .await
            .map_err(TCError::from)?;
        Ok(Self::new(storage, class))
    }

    pub(crate) async fn load(
        txn_id: TxnId,
        storage: txfs::Dir<TxnId, ApplicationBlock>,
    ) -> TCResult<Self> {
        let mut entries = storage.iter(txn_id).await.map_err(TCError::from)?;
        let name: tc_ir::Id = MANIFEST.parse().expect("manifest file name");
        let (entry_name, entry) = entries
            .next()
            .ok_or_else(|| TCError::bad_request("Class manifest is missing"))?;
        if entries.next().is_some() {
            return Err(TCError::bad_request("unsupported Class layout"));
        }
        if entry_name != name {
            return Err(TCError::bad_request("Class manifest is missing"));
        }
        let txfs::DirEntry::File(manifest) = &*entry else {
            return Err(TCError::bad_request("unsupported Class layout"));
        };
        let block = manifest
            .read::<ApplicationBlock>(txn_id)
            .await
            .map_err(TCError::from)?;
        let ApplicationBlock::Manifest(identity, definition) = &*block else {
            return Err(TCError::bad_request("unsupported Class layout"));
        };
        crate::uri::validate_identity(identity, "class")?;
        let body = ClassBody::try_from(definition.clone())
            .map_err(|error| TCError::bad_request(error.to_string()))?;
        let class = ClassDef::from_body(identity.clone(), body);
        Ok(Self::new(storage, class))
    }

    fn new(storage: txfs::Dir<TxnId, ApplicationBlock>, class: ClassDef) -> Self {
        let scope = crate::txn::DependencyScope::new(
            class.identity().clone(),
            crate::ir::application_requirements(class.prototype().values()),
        );
        Self {
            storage,
            class,
            scope: Arc::new(scope),
        }
    }

    pub(crate) fn scope(&self) -> Arc<crate::txn::DependencyScope> {
        Arc::clone(&self.scope)
    }
    pub(crate) fn identity(&self) -> &Link {
        self.class.identity()
    }
    pub(crate) fn definition(&self) -> &ClassDef {
        &self.class
    }
}

impl Transact for Class {
    async fn commit(&self, txn_id: TxnId) -> TCResult<()> {
        self.storage
            .commit(txn_id, true)
            .await
            .map_err(TCError::from)
    }
    async fn rollback(&self, txn_id: &TxnId) -> TCResult<()> {
        self.storage
            .rollback(*txn_id, true)
            .await
            .map_err(TCError::from)
    }
    async fn finalize(&self, txn_id: &TxnId) -> TCResult<()> {
        self.storage.finalize(*txn_id).await.map_err(TCError::from)
    }
}

impl crate::cluster::DirItem for Class {
    fn identity(&self) -> &Link {
        self.identity()
    }
    fn bind(&self, txn: &crate::TxnHandle) -> crate::TxnHandle {
        txn.for_application(self.scope())
    }
}

impl crate::cluster::AsyncHash for Class {
    async fn hash(&self, _txn_id: TxnId) -> TCResult<[u8; 32]> {
        Ok(*self.class.digest())
    }
}

impl Route<crate::State> for Class {
    fn route<'a>(
        &'a self,
        path: &[PathSegment],
    ) -> Option<Box<dyn Handler<'a, crate::State> + 'a>> {
        if path.is_empty() {
            Some(Box::new(self))
        } else {
            self.class.route(path)
        }
    }
}

impl<'a> Handler<'a, crate::State> for &'a Class {
    fn get<'txn>(self: Box<Self>) -> Option<GetHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, key| {
            Box::pin(async move { self.class.get(txn, &[], key).await })
        }))
    }
    fn post<'txn>(self: Box<Self>) -> Option<PostHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, params| {
            Box::pin(async move { self.class.post(txn, &[], params).await })
        }))
    }
    fn delete<'txn>(self: Box<Self>) -> Option<DeleteHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, key| {
            Box::pin(async move {
                if !matches!(key, tc_ir::Scalar::Value(tc_value::Value::None)) {
                    return Err(TCError::bad_request(
                        "Class deletion requires an explicit JSON null",
                    ));
                }
                if !txn.may_mutate(self.identity(), self.identity().path()) {
                    return Err(TCError::unauthorized("unauthorized Class deletion"));
                }
                Ok(())
            })
        }))
    }
}
