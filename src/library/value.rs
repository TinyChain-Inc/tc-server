//! One immutable Library application value.

use std::sync::Arc;

use async_hash::{Digest as _, Hash, Sha256};
use pathlink::{Link, PathSegment};
use tc_error::{TCError, TCResult};
use tc_ir::{DeleteHandler, GetHandler, Handler, Map, Route, Scalar, Transact, TxnId};

use crate::application::{decode_definition, definition_digest};
use crate::cluster::Staging;
use crate::storage::Leaf;

const MANIFEST: &str = "manifest.json";
const MODULE: &str = "module.wasm";
pub(crate) const MAX_LIBRARY_BYTES: usize = 64 * 1024 * 1024;

#[cfg(feature = "wasm")]
pub(super) type WasmRuntime = Option<Arc<tokio::sync::Mutex<crate::wasm::WasmLibrary>>>;

#[derive(Clone)]
pub struct Library {
    leaf: Leaf,
    identity: Link,
    members: Map<Scalar>,
    manifest: Arc<[u8]>,
    module: Option<Arc<[u8]>>,
    digest: crate::application::Digest,
    #[cfg(feature = "wasm")]
    wasm: WasmRuntime,
    scope: Arc<crate::application::ApplicationScope>,
    staging: Staging,
}

pub(crate) struct LibraryDraft {
    leaf: Leaf,
    identity: Link,
    members: Map<Scalar>,
    manifest: Arc<[u8]>,
    module: Option<Arc<[u8]>>,
    digest: crate::application::Digest,
    #[cfg(feature = "wasm")]
    wasm: WasmRuntime,
    requirements: crate::application::Requirements,
    staging: Staging,
}

impl LibraryDraft {
    pub(super) fn new(
        leaf: Leaf,
        identity: Link,
        manifest: Arc<[u8]>,
        module: Option<Arc<[u8]>>,
        analysis: crate::ir::LibraryAnalysis,
        staging: Staging,
    ) -> TCResult<Self> {
        crate::application::validate_identity(&identity, "lib")?;
        if &identity != leaf.identity() {
            return Err(TCError::bad_request(
                "Library identity does not match its resource path",
            ));
        }
        let digest = library_digest(&identity, &analysis.members, module.as_deref());
        Ok(Self {
            leaf,
            identity,
            members: analysis.members,
            manifest,
            module,
            digest,
            #[cfg(feature = "wasm")]
            wasm: None,
            requirements: analysis.requirements,
            staging,
        })
    }

    #[cfg(feature = "wasm")]
    pub(super) fn new_wasm(
        leaf: Leaf,
        identity: Link,
        manifest: Arc<[u8]>,
        module: Arc<[u8]>,
        analysis: crate::ir::LibraryAnalysis,
        wasm: Arc<tokio::sync::Mutex<crate::wasm::WasmLibrary>>,
        staging: Staging,
    ) -> TCResult<Self> {
        let mut draft = Self::new(leaf, identity, manifest, Some(module), analysis, staging)?;
        draft.wasm = Some(wasm);
        Ok(draft)
    }

    pub(crate) fn identity(&self) -> &Link {
        &self.identity
    }
    pub(crate) fn digest(&self) -> crate::application::Digest {
        self.digest
    }
    pub(crate) fn requirements(&self) -> &crate::application::Requirements {
        &self.requirements
    }
    pub(crate) fn canonical_body(&self) -> (Arc<[u8]>, &'static str) {
        self.module
            .as_ref()
            .map(|module| (Arc::clone(module), "application/wasm"))
            .unwrap_or_else(|| (Arc::clone(&self.manifest), "application/json"))
    }
    pub(crate) fn staging(&self) -> Staging {
        self.staging.clone()
    }
    pub(crate) fn finish(self, scope: Arc<crate::application::ApplicationScope>) -> Library {
        Library {
            leaf: self.leaf,
            identity: self.identity,
            members: self.members,
            manifest: self.manifest,
            module: self.module,
            digest: self.digest,
            #[cfg(feature = "wasm")]
            wasm: self.wasm,
            scope,
            staging: self.staging,
        }
    }
}

impl Library {
    pub(crate) fn scope(&self) -> Arc<crate::application::ApplicationScope> {
        Arc::clone(&self.scope)
    }

    pub(crate) async fn from_definition(
        leaf: Leaf,
        identity: Link,
        definition: Scalar,
        manifest: Arc<[u8]>,
    ) -> TCResult<LibraryDraft> {
        let analysis = crate::ir::compile_ir_library(definition)?;
        LibraryDraft::new(leaf, identity, manifest, None, analysis, Staging::default())
    }

    pub(crate) fn digest(&self) -> crate::application::Digest {
        self.digest
    }

    async fn stage_delete(&self, txn: &crate::TxnHandle) -> TCResult<()> {
        let staging = self
            .staging
            .get(&txn.id())
            .ok_or_else(|| TCError::internal("Library was not enlisted before deletion"))?;
        self.leaf
            .stage_file(&staging, "delete", crate::storage::WorkspaceFile::Delete)
            .await
    }
}

impl Transact for Library {
    async fn commit(&self, txn_id: TxnId) -> TCResult<()> {
        let Some(staging) = self.staging.get(&txn_id) else {
            return Ok(());
        };
        let mut files = self.leaf.staged_files(&staging).await?;
        if files.is_empty() {
            return Ok(());
        }
        let expected = if self.module.is_some() {
            &[MANIFEST, MODULE][..]
        } else {
            &[MANIFEST][..]
        };
        if matches!(
            files.remove("delete"),
            Some(crate::storage::WorkspaceFile::Delete)
        ) {
            return if files.is_empty() {
                self.leaf.remove(expected).await
            } else {
                Err(TCError::internal(
                    "mixed staged Library deletion and install",
                ))
            };
        }
        let manifest = match files.remove(MANIFEST) {
            Some(crate::storage::WorkspaceFile::Manifest(bytes)) => bytes,
            _ => return Err(TCError::internal("invalid staged Library manifest")),
        };
        let module = match files.remove(MODULE) {
            Some(crate::storage::WorkspaceFile::Module(bytes)) => Some(bytes),
            None => None,
            _ => return Err(TCError::internal("invalid staged Library module")),
        };
        if !files.is_empty() || module.is_some() != self.module.is_some() {
            return Err(TCError::internal("invalid staged Library layout"));
        }
        let canonical = self.leaf.prepare_publish(expected).await?;
        if let Some(module) = module {
            self.leaf
                .publish(
                    &canonical,
                    MODULE,
                    crate::storage::ApplicationFile::Module(module),
                )
                .await?;
        }
        self.leaf
            .publish(
                &canonical,
                MANIFEST,
                crate::storage::ApplicationFile::Manifest(manifest),
            )
            .await
    }

    fn rollback(&self, _: &TxnId) -> impl std::future::Future<Output = TCResult<()>> + Send {
        std::future::ready(Ok(()))
    }
    fn finalize(&self, _: &TxnId) -> impl std::future::Future<Output = TCResult<()>> + Send {
        std::future::ready(Ok(()))
    }
}

impl crate::application::ApplicationItem for Library {
    fn identity(&self) -> &Link {
        &self.identity
    }
    fn digest(&self) -> crate::application::Digest {
        self.digest
    }
    fn bind(&self, txn: &crate::TxnHandle) -> crate::TxnHandle {
        txn.for_application(self.scope())
    }
    fn staging(&self) -> Staging {
        self.staging.clone()
    }
    fn has_same_content(&self, other: &Self) -> bool {
        self.digest == other.digest && self.members == other.members && self.module == other.module
    }
    async fn stage(&self, txn: &crate::TxnHandle) -> TCResult<()> {
        let staging = self
            .staging
            .get(&txn.id())
            .ok_or_else(|| TCError::internal("Library was not enlisted before staging"))?;
        if let Some(module) = &self.module {
            self.leaf
                .stage_file(
                    &staging,
                    MODULE,
                    crate::storage::WorkspaceFile::Module(Arc::clone(module)),
                )
                .await?;
        }
        self.leaf
            .stage_file(
                &staging,
                MANIFEST,
                crate::storage::WorkspaceFile::Manifest(Arc::clone(&self.manifest)),
            )
            .await
    }
}

impl LibraryDraft {
    pub(crate) async fn load(
        compiler: super::compiler::Compiler,
        leaf: Leaf,
        staging: Staging,
    ) -> TCResult<Self> {
        let mut files = leaf.files().await?;
        let manifest = match files.remove(MANIFEST) {
            Some(crate::storage::ApplicationFile::Manifest(bytes)) => bytes,
            _ => return Err(TCError::bad_request("Library manifest is missing")),
        };
        let (identity, definition) = decode_definition(&manifest, MAX_LIBRARY_BYTES).await?;
        if let Some(module) = files.remove(MODULE) {
            if !files.is_empty() {
                return Err(TCError::bad_request("unsupported Library layout"));
            }
            let crate::storage::ApplicationFile::Module(module) = module else {
                return Err(TCError::bad_request("unsupported Library module"));
            };
            compiler
                .wasm(
                    leaf,
                    staging,
                    module,
                    Some((identity, definition, manifest)),
                )
                .await
        } else {
            if !files.is_empty() {
                return Err(TCError::bad_request("unsupported Library layout"));
            }
            let analysis = crate::ir::compile_ir_library(definition)?;
            LibraryDraft::new(leaf, identity, manifest, None, analysis, staging)
        }
    }
}

impl Route<crate::State> for Library {
    fn route<'a>(
        &'a self,
        path: &[PathSegment],
    ) -> Option<Box<dyn Handler<'a, crate::State> + 'a>> {
        if path.is_empty() {
            return Some(Box::new(self));
        }
        #[cfg(feature = "wasm")]
        if let Some(wasm) = &self.wasm {
            return Some(Box::new(crate::wasm::WasmRoute::new(
                Arc::clone(wasm),
                path.to_vec(),
            )));
        }
        crate::ir::route_member(&self.identity, &self.members, path)
    }
}

impl<'a> Handler<'a, crate::State> for &'a Library {
    fn get<'txn>(self: Box<Self>) -> Option<GetHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |_txn, _key| {
            Box::pin(
                async move { Ok(crate::State::from_scalar(Scalar::Map(self.members.clone()))) },
            )
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
                        "Library deletion requires an explicit JSON null",
                    ));
                }
                if !txn.has_claim(&self.identity, umask::USER_WRITE) {
                    return Err(TCError::unauthorized("unauthorized Library deletion"));
                }
                self.stage_delete(txn).await?;
                txn.mark_resource_mutated(self.identity.path())
            })
        }))
    }
}

pub(crate) fn library_digest(
    identity: &Link,
    members: &Map<Scalar>,
    module: Option<&[u8]>,
) -> crate::application::Digest {
    let definition = definition_digest(identity, members);
    let Some(module) = module else {
        return definition;
    };
    let module: [u8; 32] = Sha256::digest(module).into();
    Hash::<Sha256>::hash((definition, module)).into()
}
