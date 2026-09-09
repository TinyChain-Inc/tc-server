use pathlink::PathSegment;
use tc_error::{TCError, TCResult};
use tc_ir::{Handler, Route, Scalar, Transact, TxnId};

use crate::application::{MAX_DEFINITION_BYTES, decode_definition, definition_digest};
use crate::cluster::Staging;
use crate::storage::{Leaf, MANIFEST};

#[derive(Clone)]
pub struct Service {
    leaf: Leaf,
    identity: pathlink::Link,
    definition: Scalar,
    digest: crate::application::Digest,
    manifest: std::sync::Arc<[u8]>,
    staging: Staging,
}

impl Service {
    pub(crate) fn identity(&self) -> &pathlink::Link {
        &self.identity
    }
    pub(crate) fn digest(&self) -> crate::application::Digest {
        self.digest
    }
    pub(crate) fn canonical_body(&self) -> std::sync::Arc<[u8]> {
        std::sync::Arc::clone(&self.manifest)
    }
    pub(crate) fn staging(&self) -> Staging {
        self.staging.clone()
    }
    fn new(
        leaf: Leaf,
        identity: pathlink::Link,
        definition: Scalar,
        manifest: std::sync::Arc<[u8]>,
        staging: Staging,
    ) -> TCResult<Self> {
        crate::application::validate_identity(&identity, "service")?;
        if &identity != leaf.identity() {
            return Err(TCError::bad_request(
                "Service identity does not match its resource path",
            ));
        }
        let digest = definition_digest(&identity, &definition);
        Ok(Self {
            leaf,
            identity,
            definition,
            digest,
            manifest,
            staging,
        })
    }

    pub(crate) fn from_definition(
        leaf: Leaf,
        identity: pathlink::Link,
        definition: Scalar,
        manifest: std::sync::Arc<[u8]>,
    ) -> TCResult<Self> {
        Self::new(leaf, identity, definition, manifest, Staging::default())
    }

    pub(crate) async fn load(leaf: Leaf, staging: Staging) -> TCResult<Self> {
        let mut files = leaf.files().await?;
        let file = files
            .remove(MANIFEST)
            .filter(|_| files.is_empty())
            .ok_or_else(|| TCError::bad_request("unsupported Service layout"))?;
        let crate::storage::ApplicationFile::Manifest(manifest) = file else {
            return Err(TCError::bad_request("unsupported Service layout"));
        };
        let (identity, definition) = decode_definition(&manifest, MAX_DEFINITION_BYTES).await?;
        Self::new(leaf, identity, definition, manifest, staging)
    }

    async fn stage_delete(&self, txn: &crate::TxnHandle) -> TCResult<()> {
        let staging = self
            .staging
            .get(&txn.id())
            .ok_or_else(|| TCError::internal("Service was not enlisted before deletion"))?;
        self.leaf.stage_delete(&staging).await
    }
}

impl Transact for Service {
    async fn commit(&self, txn_id: TxnId) -> TCResult<()> {
        let Some(staging) = self.staging.get(&txn_id) else {
            return Ok(());
        };
        self.leaf.commit_manifest(&staging).await
    }

    fn rollback(&self, _: &TxnId) -> impl std::future::Future<Output = TCResult<()>> + Send {
        std::future::ready(Ok(()))
    }

    fn finalize(&self, _: &TxnId) -> impl std::future::Future<Output = TCResult<()>> + Send {
        std::future::ready(Ok(()))
    }
}

impl crate::application::ApplicationItem for Service {
    fn identity(&self) -> &pathlink::Link {
        self.identity()
    }

    fn digest(&self) -> crate::application::Digest {
        self.digest()
    }

    fn bind(&self, txn: &crate::TxnHandle) -> crate::TxnHandle {
        txn.clone()
    }

    fn staging(&self) -> Staging {
        self.staging.clone()
    }

    fn has_same_content(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }

    async fn stage(&self, txn: &crate::TxnHandle) -> TCResult<()> {
        let staging = self
            .staging
            .get(&txn.id())
            .ok_or_else(|| TCError::internal("Service was not enlisted before staging"))?;
        self.leaf
            .stage_manifest(&staging, std::sync::Arc::clone(&self.manifest))
            .await
    }
}

impl<'a> Handler<'a, crate::State> for &'a Service {
    fn get<'txn>(self: Box<Self>) -> Option<tc_ir::GetHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |_txn, _key| {
            Box::pin(async move { Ok(crate::State::from_scalar(self.definition.clone())) })
        }))
    }

    fn delete<'txn>(self: Box<Self>) -> Option<tc_ir::DeleteHandler<'a, 'txn, crate::State>>
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
                self.stage_delete(txn).await?;
                txn.mark_resource_mutated(self.identity().path())
            })
        }))
    }
}

impl Route<crate::State> for Service {
    fn route<'a>(
        &'a self,
        path: &[PathSegment],
    ) -> Option<Box<dyn Handler<'a, crate::State> + 'a>> {
        Some(if path.is_empty() {
            Box::new(self) as Box<dyn Handler<'a, crate::State>>
        } else {
            Box::new(UnsupportedService) as Box<dyn Handler<'a, crate::State>>
        })
    }
}

struct UnsupportedService;

impl<'a> Handler<'a, crate::State> for UnsupportedService {}
