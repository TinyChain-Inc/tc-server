//! One versioned Service. Attribute ownership, hashing, and recursive delegation
//! are ported from v1 `cluster/service.rs` (`Version`) and
//! `cluster/public/service.rs`, revision 17ef342e8f7026e4c4a60d2044de9aeb1b145b91.
//! V2's recursive application directories own version membership and definitions.

use std::sync::Arc;

use async_hash::{Digest, Hash, Sha256};
use pathlink::{Link, PathSegment};
use safecast::TryCastFrom;
use tc_collection::{Collection, collection::CollectionSchema};
use tc_error::{TCError, TCResult};
use tc_ir::{
    DeleteHandler, GetHandler, Handler, Map, OpDef, OpRef, PutHandler, Route, Scalar, Subject,
    TCRef, Transact, TxnId,
};
use tc_value::Value;

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
                if !txn.may_mutate(&identity, self.0.path()) {
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
                                    Service::create(
                                        txn.id(), storage, identity, definition,
                                        txn.execution_limits().max_op_invocations,
                                    )
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

/// An attribute of one Service version, as in v1.
#[derive(Clone)]
enum Attr {
    Chain(tc_chain::SyncChain<crate::TxnHandle, ApplicationBlock>),
    Scalar(Scalar),
}

impl From<Attr> for crate::State {
    fn from(attr: Attr) -> Self {
        match attr {
            Attr::Chain(chain) => Self::Chain(chain),
            Attr::Scalar(scalar) => Self::from_scalar(scalar),
        }
    }
}

#[derive(Clone)]
pub struct Service {
    storage: txfs::Dir<TxnId, ApplicationBlock>,
    identity: Link,
    definition: Scalar,
    attrs: Map<Attr>,
    scope: Arc<crate::txn::DependencyScope>,
}

impl Service {
    pub(crate) async fn create(
        txn_id: TxnId,
        storage: txfs::Dir<TxnId, ApplicationBlock>,
        identity: Link,
        definition: Scalar,
        capacity: usize,
    ) -> TCResult<Self> {
        let proto = validate(&identity, &definition)?;
        let scope = Arc::new(crate::txn::DependencyScope::new(
            identity.clone(),
            crate::ir::application_requirements(proto.values()),
        ));
        let mut attrs = Map::new();
        let mut native = None;
        for (name, scalar) in proto {
            let attr = if let Some(schema) = collection_schema(&scalar)? {
                let native = match &native {
                    Some(native) => freqfs::DirLock::clone(native),
                    None => native.insert(storage.create_native().await?).clone(),
                };
                let member = native.write().await.create_dir(name.to_string())?;
                let (subject, wal, values) = {
                    let mut member = member.write().await;
                    (
                        member.create_dir("subject".into())?,
                        member.create_dir("wal".into())?,
                        member.create_dir("values".into())?,
                    )
                };
                let subject = Collection::create(subject, schema)?;
                let chain = tc_chain::SyncChain::create(
                    subject,
                    wal,
                    values,
                    tc_chain::TxnTaskQueue::new(capacity),
                )
                .await?;
                Attr::Chain(chain)
            } else {
                Attr::Scalar(scalar)
            };
            attrs.insert(name, attr);
        }
        storage
            .create_file(
                txn_id,
                MANIFEST.parse().expect("manifest name"),
                ApplicationBlock::Manifest(identity.clone(), definition.clone()),
            )
            .await?;
        Ok(Self {
            storage,
            identity,
            definition,
            attrs,
            scope,
        })
    }

    /// Open native members without replay; the complete unpublished kernel supplies
    /// original-ID capabilities afterward, before readiness or expiry processing.
    pub(crate) async fn load(
        txn_id: TxnId,
        storage: txfs::Dir<TxnId, ApplicationBlock>,
        capacity: usize,
    ) -> TCResult<Self> {
        let (identity, definition) = {
            let mut entries = storage.iter(txn_id).await?;
            let (name, entry) = entries
                .next()
                .ok_or_else(|| TCError::bad_request("Service manifest is missing"))?;
            if entries.next().is_some() || name.as_str() != MANIFEST {
                return Err(TCError::bad_request("unsupported Service layout"));
            }
            let txfs::DirEntry::File(manifest) = &*entry else {
                return Err(TCError::bad_request("unsupported Service layout"));
            };
            let block = manifest.read::<ApplicationBlock>(txn_id).await?;
            let ApplicationBlock::Manifest(identity, definition) = &*block else {
                return Err(TCError::bad_request("unsupported Service manifest"));
            };
            (identity.clone(), definition.clone())
        };
        let proto = validate(&identity, &definition)?;
        let scope = Arc::new(crate::txn::DependencyScope::new(
            identity.clone(),
            crate::ir::application_requirements(proto.values()),
        ));
        let native = storage.native().await?;
        let mut attrs = Map::new();
        for (name, scalar) in proto {
            let attr = if let Some(schema) = collection_schema(&scalar)? {
                let native = native
                    .as_ref()
                    .ok_or_else(|| TCError::bad_request("missing Service native storage"))?;
                let member = required_dir(native, name.as_str()).await?;
                if member
                    .read()
                    .await
                    .names()
                    .any(|name| !matches!(name.as_str(), "subject" | "wal" | "values"))
                {
                    return Err(TCError::bad_request("unsupported Chain member layout"));
                }
                let wal = required_dir(&member, "wal").await?;
                // Empty capture stores have no files and freqfs does not persist
                // empty directories. Every referenced capture is strictly loaded by Chain.
                let values = member.write().await.get_or_create_dir("values".into())?;
                let chain = tc_chain::SyncChain::open(
                    || async {
                        let subject = required_dir(&member, "subject").await?;
                        Collection::load(subject, schema).await
                    },
                    wal,
                    values,
                    tc_chain::TxnTaskQueue::new(capacity),
                )
                .await?;
                Attr::Chain(chain)
            } else {
                Attr::Scalar(scalar)
            };
            attrs.insert(name, attr);
        }
        if let Some(native) = native {
            let native = native.read().await;
            if native
                .names()
                .any(|name| !matches!(attrs.get(name.as_str()), Some(Attr::Chain(_))))
            {
                return Err(TCError::bad_request("unexpected Service native member"));
            }
        }
        Ok(Self {
            storage,
            identity,
            definition,
            attrs,
            scope,
        })
    }

    pub(crate) async fn recover(
        &self,
        server: &crate::txn::TxnServer,
        kernel: &Arc<crate::kernel::KernelInner>,
    ) -> TCResult<()> {
        for attr in self.attrs.values() {
            if let Attr::Chain(chain) = attr {
                Box::pin(chain.recover::<crate::State, _, _>(|id| {
                    let txn = server.bind_recovery(id, Arc::clone(kernel));
                    async move { txn }
                }))
                .await?;
            }
        }
        Ok(())
    }

    #[cfg(feature = "http-client")]
    pub(crate) async fn synchronize(
        &self,
        txn: &crate::TxnHandle,
        seed: &str,
        session: &crate::cluster::BootstrapSession,
    ) -> TCResult<()> {
        use crate::cluster::AsyncHash;

        if hex::encode(self.hash(txn).await?) == session.state_hash() {
            return Ok(());
        }
        for (name, attr) in &self.attrs {
            if let Attr::Chain(chain) = attr {
                let target = self.identity.clone().append(name.clone());
                let snapshot = crate::replication::read_seed_snapshot(
                    seed,
                    session.token(),
                    txn,
                    &target,
                    txn.application_body_limit(),
                )
                .await?;
                let snapshot = Collection::try_cast_from(snapshot, |_| {
                    TCError::bad_gateway("Service member snapshot is not a collection")
                })?;
                chain.restore_from(txn, &snapshot).await?;
            }
        }
        if hex::encode(self.hash(txn).await?) != session.state_hash() {
            return Err(TCError::conflict(
                "Service snapshot hash does not match its authenticated source",
            ));
        }
        txn.mark_resource_mutated(self.identity.path())
    }

    pub(crate) fn identity(&self) -> &Link {
        &self.identity
    }

    fn as_state(&self) -> crate::State {
        crate::State::Map(
            self.attrs
                .iter()
                .map(|(id, attr)| (id.clone(), attr.clone().into()))
                .collect(),
        )
    }
}

impl Transact for Service {
    async fn commit(&self, txn_id: TxnId) -> TCResult<()> {
        for attr in self.attrs.values() {
            if let Attr::Chain(chain) = attr {
                chain.commit(txn_id).await?;
            }
        }
        self.storage.commit(txn_id, true).await.map_err(Into::into)
    }

    async fn rollback(&self, txn_id: &TxnId) -> TCResult<()> {
        for attr in self.attrs.values() {
            if let Attr::Chain(chain) = attr {
                chain.rollback(txn_id).await?;
            }
        }
        self.storage
            .rollback(*txn_id, true)
            .await
            .map_err(Into::into)
    }

    async fn finalize(&self, txn_id: &TxnId) -> TCResult<()> {
        for attr in self.attrs.values() {
            if let Attr::Chain(chain) = attr {
                chain.finalize(txn_id).await?;
            }
        }
        self.storage.finalize(*txn_id).await.map_err(Into::into)
    }
}

impl crate::cluster::DirItem for Service {
    fn identity(&self) -> &Link {
        &self.identity
    }

    fn bind(&self, txn: &crate::TxnHandle) -> crate::TxnHandle {
        txn.for_application(Arc::clone(&self.scope))
    }
}

impl crate::cluster::AsyncHash for Service {
    async fn hash(&self, txn: &crate::TxnHandle) -> TCResult<[u8; 32]> {
        if self.attrs.is_empty() {
            return Ok(async_hash::default_hash::<Sha256>().into());
        }

        let mut hash = Sha256::new();
        for (name, attr) in &self.attrs {
            let value = match attr {
                Attr::Chain(chain) => <[u8; 32]>::from(chain.hash(txn).await?),
                Attr::Scalar(scalar) => Hash::<Sha256>::hash(scalar).into(),
            };
            let mut attribute = Sha256::new();
            attribute.update(Hash::<Sha256>::hash(name));
            attribute.update(value);
            hash.update(attribute.finalize());
        }
        Ok(hash.finalize().into())
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
                if !matches!(key, Scalar::Value(Value::None)) {
                    return Err(TCError::bad_request(
                        "Service deletion requires an explicit JSON null",
                    ));
                }
                if !txn.may_mutate(self.identity(), self.identity().path()) {
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
        let Some((name, suffix)) = path.split_first() else {
            return Some(Box::new(self));
        };
        match self.attrs.get(name.as_str())? {
            Attr::Chain(chain) => chain.route(suffix),
            Attr::Scalar(scalar) => {
                let scalar = if suffix.is_empty() {
                    scalar
                } else {
                    let Scalar::Map(map) = scalar else {
                        return None;
                    };
                    crate::ir::member(map, suffix)?
                };
                Some(crate::ir::route_scalar(self.as_state(), scalar))
            }
        }
    }
}

async fn required_dir(
    parent: &freqfs::DirLock<ApplicationBlock>,
    name: &str,
) -> TCResult<freqfs::DirLock<ApplicationBlock>> {
    parent
        .read()
        .await
        .get_dir(name)
        .cloned()
        .ok_or_else(|| TCError::bad_request(format!("missing Service storage {name}")))
}

fn collection_schema(scalar: &Scalar) -> TCResult<Option<CollectionSchema>> {
    let Scalar::Ref(reference) = scalar else {
        return if scalar.is_ref() {
            Err(TCError::bad_request(
                "Service attributes must be literal scalars or Chains",
            ))
        } else {
            Ok(None)
        };
    };
    let TCRef::Op(OpRef::Get((Subject::Link(class), collection))) = reference.as_ref() else {
        return Err(TCError::bad_request("expected a native Chain declaration"));
    };
    if class.host().is_some() || class.path().as_ref() != &tc_chain::SYNC_CHAIN[..] {
        return Err(TCError::bad_request("unsupported Chain variant"));
    }
    let Scalar::Ref(reference) = collection else {
        return Err(TCError::bad_request("invalid collection declaration"));
    };
    let TCRef::Op(OpRef::Get((Subject::Link(class), schema))) = reference.as_ref() else {
        return Err(TCError::bad_request("invalid collection declaration"));
    };
    if class.host().is_some() {
        return Err(TCError::bad_request("collection class must be native"));
    }
    let schema = Value::try_cast_from(schema.clone(), |_| {
        TCError::bad_request("invalid collection schema")
    })?;
    CollectionSchema::try_cast_from((class.path().clone(), schema), |_| {
        TCError::bad_request("unsupported persistent collection schema")
    })
    .map(Some)
}

fn validate(identity: &Link, definition: &Scalar) -> TCResult<Map<Scalar>> {
    crate::uri::validate_identity(identity, "service")?;
    let Scalar::Map(proto) = definition else {
        return Err(TCError::bad_request(
            "Service definition must be an attribute map",
        ));
    };
    let mut attrs = Map::new();
    for (name, scalar) in proto {
        if name.as_str() == "replicas" {
            return Err(TCError::bad_request(
                "replicas is a reserved Service member",
            ));
        }
        let scalar = match scalar.clone() {
            Scalar::Op(op) => {
                op.validate()?;
                let op = if matches!(op, OpDef::Put(_) | OpDef::Delete(_)) {
                    let op = op.reference_self(identity);
                    let mut external_write = false;
                    for (_, value) in op.form() {
                        value.visit_referenced_methods(&mut |_, method| {
                            external_write |=
                                matches!(method, tc_ir::Method::Put | tc_ir::Method::Delete);
                        });
                    }
                    if external_write {
                        return Err(TCError::bad_request(
                            "replicated Service methods cannot write other resources",
                        ));
                    }
                    op
                } else {
                    op.dereference_self(identity)
                };
                Scalar::Op(op)
            }
            scalar => {
                collection_schema(&scalar)?;
                scalar
            }
        };
        attrs.insert(name.clone(), scalar);
    }
    Ok(attrs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declarations_and_replicated_methods_fail_before_storage_allocation() {
        let identity = "/service/test/native/1.0.0".parse().unwrap();
        let definition =
            |value| Scalar::Map([("data".parse().unwrap(), value)].into_iter().collect());
        let get = |path: &str, key| {
            Scalar::from(TCRef::Op(OpRef::Get((
                Subject::Link(path.parse().unwrap()),
                key,
            ))))
        };
        for value in [
            get("/state/chain/block", Scalar::default()),
            get(
                "/state/chain/sync",
                get("/state/collection/tensor", Scalar::default()),
            ),
            get(
                "/state/chain/sync",
                get(
                    "/state/collection/btree",
                    Value::from("invalid schema").into(),
                ),
            ),
            Scalar::Tuple(vec![get("/service/test/other/1.0.0", Scalar::default())]),
        ] {
            assert!(validate(&identity, &definition(value)).is_err());
        }
        let external = Scalar::Op(OpDef::Put((
            "key".parse().unwrap(),
            "value".parse().unwrap(),
            vec![(
                "write".parse().unwrap(),
                Scalar::from(TCRef::Op(OpRef::Put((
                    Subject::Link("/service/test/other/1.0.0/data".parse().unwrap()),
                    Scalar::default(),
                    Scalar::default(),
                )))),
            )],
        )));
        assert!(validate(&identity, &definition(external)).is_err());
        assert!(validate(&identity, &definition(Value::from("literal").into())).is_ok());
    }
}
