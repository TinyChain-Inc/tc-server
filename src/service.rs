use pathlink::{Link, PathSegment};
use tc_error::{TCError, TCResult};
use tc_ir::{DeleteHandler, GetHandler, Handler, PutHandler, Route, Scalar, Transact, TxnId};

use crate::storage::ApplicationBlock;

const MANIFEST: &str = "manifest.json";

pub(crate) struct Root<'a>(pub(crate) &'a crate::cluster::Cluster<crate::cluster::Dir<Service>>);

impl<'a, 'runtime: 'a> Handler<'a, crate::State> for Root<'runtime> {
    fn put<'txn>(self: Box<Self>) -> Option<PutHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, key, value| {
            Box::pin(async move {
                let (identity, definition) = crate::literal::into_put(key, value)?;
                let segments = crate::uri::validate_identity(&identity, "service")?;
                if !txn.has_claim(&identity, umask::USER_WRITE) {
                    return Err(TCError::unauthorized("unauthorized Service install"));
                }
                let expected = definition.clone();
                let conflict_identity = identity.clone();
                let path = identity.path()[1..].to_vec();
                self.0
                    .clone()
                    .create_item_if_absent(
                        txn,
                        &path,
                        &segments,
                        move |existing| {
                            (existing.definition == expected)
                                .then_some(())
                                .ok_or_else(|| {
                                    TCError::conflict(format!(
                                        "immutable Service version {conflict_identity} has conflicting content"
                                    ))
                                })
                        },
                        move |parent, remaining| async move {
                            self.0
                                .replicate(
                                    txn,
                                    &"/service".parse().expect("Service root"),
                                    tc_ir::Method::Put,
                                    tc_ir::Scalar::Value(tc_value::Value::Link(identity.clone())),
                                    Some(crate::State::from_scalar(definition.clone())),
                                )
                                .await?;
                            parent
                                .create_item(txn, &remaining, move |storage| {
                                    Service::create(txn.id(), storage, identity, definition)
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
pub struct Service {
    storage: txfs::Dir<TxnId, ApplicationBlock>,
    identity: Link,
    definition: Scalar,
}

impl Service {
    pub(crate) async fn create(
        txn_id: TxnId,
        storage: txfs::Dir<TxnId, ApplicationBlock>,
        identity: Link,
        definition: Scalar,
    ) -> TCResult<Self> {
        crate::uri::validate_identity(&identity, "service")?;
        storage
            .create_file(
                txn_id,
                MANIFEST.parse().expect("manifest file name"),
                ApplicationBlock::Manifest(identity.clone(), definition.clone()),
            )
            .await
            .map_err(TCError::from)?;
        Ok(Self {
            storage,
            identity,
            definition,
        })
    }

    pub(crate) async fn load(
        txn_id: TxnId,
        storage: txfs::Dir<TxnId, ApplicationBlock>,
    ) -> TCResult<Self> {
        let mut entries = storage.iter(txn_id).await.map_err(TCError::from)?;
        let name: tc_ir::Id = MANIFEST.parse().expect("manifest file name");
        let (entry_name, entry) = entries
            .next()
            .ok_or_else(|| TCError::bad_request("Service manifest is missing"))?;
        if entries.next().is_some() {
            return Err(TCError::bad_request("unsupported Service layout"));
        }
        if entry_name != name {
            return Err(TCError::bad_request("Service manifest is missing"));
        }
        let txfs::DirEntry::File(manifest) = &*entry else {
            return Err(TCError::bad_request("unsupported Service layout"));
        };
        let block = manifest
            .read::<ApplicationBlock>(txn_id)
            .await
            .map_err(TCError::from)?;
        let ApplicationBlock::Manifest(identity, definition) = &*block else {
            return Err(TCError::bad_request("unsupported Service layout"));
        };
        crate::uri::validate_identity(identity, "service")?;
        Ok(Self {
            storage,
            identity: identity.clone(),
            definition: definition.clone(),
        })
    }

    pub(crate) fn identity(&self) -> &Link {
        &self.identity
    }
}

impl Transact for Service {
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

impl crate::cluster::DirItem for Service {
    fn identity(&self) -> &Link {
        self.identity()
    }
    fn bind(&self, txn: &crate::TxnHandle) -> crate::TxnHandle {
        txn.clone()
    }
}

impl crate::cluster::ResourceHash for Service {
    async fn resource_hash(&self, _txn_id: TxnId) -> TCResult<[u8; 32]> {
        Ok(async_hash::Hash::<async_hash::Sha256>::hash(&self.definition).into())
    }
}

impl<'a> Handler<'a, crate::State> for &'a Service {
    fn get<'txn>(self: Box<Self>) -> Option<GetHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |_txn, _key| {
            Box::pin(async move { Ok(crate::State::from_scalar(self.definition.clone())) })
        }))
    }

    fn delete<'txn>(self: Box<Self>) -> Option<DeleteHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, key| {
            Box::pin(async move {
                if !matches!(key, Scalar::Value(tc_value::Value::None)) {
                    return Err(TCError::bad_request(
                        "Service deletion requires an explicit JSON null",
                    ));
                }
                if !txn.has_claim(self.identity(), umask::USER_WRITE) {
                    return Err(TCError::unauthorized("unauthorized Service deletion"));
                }
                Ok(())
            })
        }))
    }
}

impl Route<crate::State> for Service {
    fn route<'a>(
        &'a self,
        path: &[PathSegment],
    ) -> Option<Box<dyn Handler<'a, crate::State> + 'a>> {
        path.is_empty()
            .then(|| Box::new(self) as Box<dyn Handler<'a, crate::State>>)
    }
}
