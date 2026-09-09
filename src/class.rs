use std::collections::BTreeMap;
use std::sync::Arc;

use pathlink::PathSegment;
use tc_error::{TCError, TCResult};
use tc_ir::{DeleteHandler, GetHandler, Handler, PostHandler, Public, Route, Transact, TxnId};
use tc_state::{ClassBody, ClassDef, ClassParent};

use crate::application::decode_definition;
use crate::cluster::{Cluster, Dir, Staging};
use crate::storage::{Leaf, MANIFEST};

pub(crate) const MAX_CLASS_BYTES: usize = 1024 * 1024;
type Requirements = crate::application::Requirements;

#[derive(Clone)]
pub struct Class {
    leaf: Leaf,
    manifest: std::sync::Arc<[u8]>,
    class: ClassDef,
    scope: Arc<crate::application::ApplicationScope>,
    staging: Staging,
}

pub(crate) struct ClassDraft {
    leaf: Leaf,
    manifest: Arc<[u8]>,
    class: ClassDef,
    staging: Staging,
}

impl ClassDraft {
    fn new(
        leaf: Leaf,
        identity: pathlink::Link,
        body: ClassBody,
        manifest: std::sync::Arc<[u8]>,
        staging: Staging,
    ) -> TCResult<Self> {
        crate::application::validate_identity(&identity, "class")?;
        if &identity != leaf.identity() {
            return Err(TCError::bad_request(
                "Class identity does not match its resource path",
            ));
        }
        let class = ClassDef::from_body(identity, body);
        class
            .validate_digest()
            .map_err(|error| TCError::bad_request(error.to_string()))?;
        Ok(Self {
            leaf,
            manifest,
            class,
            staging,
        })
    }

    pub(crate) fn identity(&self) -> &pathlink::Link {
        self.class.identity()
    }

    pub(crate) fn digest(&self) -> crate::application::Digest {
        *self.class.digest()
    }

    pub(crate) fn class_def(&self) -> &ClassDef {
        &self.class
    }

    pub(crate) fn staging(&self) -> Staging {
        self.staging.clone()
    }

    pub(crate) fn canonical_body(&self) -> Arc<[u8]> {
        Arc::clone(&self.manifest)
    }

    pub(crate) fn finish(self, scope: Arc<crate::application::ApplicationScope>) -> Class {
        Class {
            leaf: self.leaf,
            manifest: self.manifest,
            class: self.class,
            scope,
            staging: self.staging,
        }
    }
}

impl Class {
    pub(crate) fn scope(&self) -> Arc<crate::application::ApplicationScope> {
        Arc::clone(&self.scope)
    }

    pub(crate) fn from_definition(
        leaf: Leaf,
        identity: pathlink::Link,
        body: ClassBody,
        manifest: std::sync::Arc<[u8]>,
    ) -> TCResult<ClassDraft> {
        ClassDraft::new(leaf, identity, body, manifest, Staging::default())
    }

    pub(crate) fn identity(&self) -> &pathlink::Link {
        self.class.identity()
    }
    pub(crate) fn digest(&self) -> crate::application::Digest {
        *self.class.digest()
    }
    async fn stage_delete(&self, txn: &crate::TxnHandle) -> TCResult<()> {
        let staging = self
            .staging
            .get(&txn.id())
            .ok_or_else(|| TCError::internal("Class was not enlisted before deletion"))?;
        self.leaf.stage_delete(&staging).await
    }
}

impl Transact for Class {
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

impl crate::application::ApplicationItem for Class {
    fn identity(&self) -> &pathlink::Link {
        self.identity()
    }

    fn digest(&self) -> crate::application::Digest {
        self.digest()
    }

    fn bind(&self, txn: &crate::TxnHandle) -> crate::TxnHandle {
        txn.for_application(self.scope())
    }

    fn staging(&self) -> Staging {
        self.staging.clone()
    }

    fn has_same_content(&self, other: &Self) -> bool {
        self.class == other.class
    }

    async fn stage(&self, txn: &crate::TxnHandle) -> TCResult<()> {
        let staging = self
            .staging
            .get(&txn.id())
            .ok_or_else(|| TCError::internal("Class was not enlisted before staging"))?;
        self.leaf
            .stage_manifest(&staging, Arc::clone(&self.manifest))
            .await
    }
}

impl ClassDraft {
    pub(crate) async fn load(leaf: Leaf, staging: Staging) -> TCResult<Self> {
        let mut files = leaf.files().await?;
        let file = files
            .remove(MANIFEST)
            .filter(|_| files.is_empty())
            .ok_or_else(|| TCError::bad_request("unsupported Class layout"))?;
        let crate::storage::ApplicationFile::Manifest(manifest) = file else {
            return Err(TCError::bad_request("unsupported Class layout"));
        };
        let (identity, definition) = decode_definition(&manifest, MAX_CLASS_BYTES).await?;
        let body = ClassBody::try_from(definition)
            .map_err(|error| TCError::bad_request(error.to_string()))?;
        Self::new(leaf, identity, body, manifest, staging)
    }
}

impl Route<crate::State> for Class {
    fn route<'a>(
        &'a self,
        path: &[PathSegment],
    ) -> Option<Box<dyn Handler<'a, crate::State> + 'a>> {
        if path.is_empty() {
            return Some(Box::new(self));
        }
        self.class.route(path)
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
                if !txn.has_claim(self.identity(), umask::USER_WRITE) {
                    return Err(TCError::unauthorized("unauthorized Class deletion"));
                }
                self.stage_delete(txn).await?;
                txn.mark_resource_mutated(self.identity().path())
            })
        }))
    }
}

impl Cluster<Dir<Class>> {
    pub(crate) async fn validate_batch(
        &self,
        txn: &crate::TxnHandle,
        definitions: &[ClassDef],
    ) -> TCResult<BTreeMap<pathlink::Link, Requirements>> {
        let mut candidates = definitions
            .iter()
            .map(|definition| (definition.identity().clone(), definition.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut pending = definitions.to_vec();
        while let Some(definition) = pending.pop() {
            let ClassParent::Class(parent) = definition.parent() else {
                continue;
            };
            if candidates.contains_key(parent) {
                continue;
            }
            let parent_definition = self
                .get(txn, parent)
                .await?
                .ok_or_else(|| TCError::bad_request(format!("missing Class parent {parent}")))?;
            pending.push(parent_definition.clone());
            candidates.insert(parent.clone(), parent_definition);
        }
        tc_state::analyze_classes(
            &candidates,
            definitions
                .iter()
                .map(|definition| definition.identity().clone()),
        )
        .map_err(|error| TCError::bad_request(error.to_string()))
    }

    pub(crate) async fn get(
        &self,
        txn: &crate::TxnHandle,
        identity: &pathlink::Link,
    ) -> TCResult<Option<ClassDef>> {
        self.clone()
            .lookup(txn, &crate::application::identity_segments(identity)?)
            .await
            .and_then(|class| class.exact_item())
            .map(|class| class.map(|class| class.state().class.clone()))
    }
}
