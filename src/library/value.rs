//! One immutable Library resource backed by a transactional filesystem directory.

use std::sync::Arc;

use pathlink::{Link, PathSegment};
use tc_error::{TCError, TCResult};
use tc_ir::{DeleteHandler, GetHandler, Handler, Map, PutHandler, Route, Scalar, Transact, TxnId};

use crate::storage::ApplicationBlock;

const MANIFEST: &str = "manifest.json";
const MODULE: &str = "module.wasm";
#[cfg(feature = "http-client")]
pub(crate) const MAX_LIBRARY_BYTES: usize = 64 * 1024 * 1024;

pub(crate) struct Root<'a> {
    libraries: &'a crate::cluster::Cluster<crate::cluster::Dir<Library>>,
    #[cfg(feature = "wasm")]
    compiler: &'a crate::library::compiler::Compiler,
}

impl<'a> Root<'a> {
    pub(crate) fn new(
        libraries: &'a crate::cluster::Cluster<crate::cluster::Dir<Library>>,
        #[cfg(feature = "wasm")] compiler: &'a crate::library::compiler::Compiler,
    ) -> Self {
        Self {
            libraries,
            #[cfg(feature = "wasm")]
            compiler,
        }
    }
}

impl<'a, 'runtime: 'a> Handler<'a, crate::State> for Root<'runtime> {
    fn put<'txn>(self: Box<Self>) -> Option<PutHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, key, value| {
            Box::pin(async move {
                match value {
                    crate::State::Scalar(Scalar::Value(tc_value::Value::Bytes(module)))
                        if matches!(key, Scalar::Value(tc_value::Value::None)) =>
                    {
                        #[cfg(feature = "wasm")]
                        {
                            let wasm = self.compiler.module(&module).await?;
                            let identity = wasm.identity().clone();
                            let definition = wasm.definition().clone();
                            crate::uri::validate_identity(&identity, "lib")?;
                            if !txn.has_claim(&identity, umask::USER_WRITE) {
                                return Err(TCError::unauthorized("unauthorized Library install"));
                            }
                            let analysis = crate::ir::compile_ir_library(definition.clone())?;
                            if wasm.bindings().any(|binding| {
                                crate::ir::member(&analysis.members, &binding.path).is_none()
                            }) {
                                return Err(TCError::bad_request(
                                    "a WASM export does not correspond to an embedded Library member",
                                ));
                            }
                            let module_hash = wasm.module_hash();
                            let segments = crate::uri::validate_identity(&identity, "lib")?;
                            let expected_members = analysis.members.clone();
                            let conflict_identity = identity.clone();
                            let path = identity.path()[1..].to_vec();
                            self
                                .libraries
                                .clone()
                                .create_item_if_absent(
                                    txn,
                                    &path,
                                    &segments,
                                    move |existing| {
                                        existing
                                            .same_wasm(&expected_members, module_hash)
                                            .then_some(())
                                            .ok_or_else(|| {
                                                TCError::conflict(format!(
                                                    "immutable Library version {conflict_identity} has conflicting content"
                                                ))
                                            })
                                    },
                                    move |parent, remaining| async move {
                                        self.libraries
                                            .replicate(
                                                txn,
                                                &"/lib".parse().expect("Library root"),
                                                tc_ir::Method::Put,
                                                Scalar::Value(tc_value::Value::None),
                                                Some(crate::State::from_scalar(Scalar::Value(
                                                    tc_value::Value::Bytes(module.clone()),
                                                ))),
                                            )
                                            .await?;
                                        parent
                                            .create_item(txn, &remaining, move |storage| {
                                                Library::create_wasm(
                                                    txn.id(),
                                                    storage,
                                                    identity,
                                                    analysis,
                                                    module,
                                                    wasm,
                                                )
                                            })
                                            .await
                                            .map(|_| ())
                                    },
                                )
                                .await
                        }
                        #[cfg(not(feature = "wasm"))]
                        {
                            let _ = module;
                            Err(TCError::new(
                                tc_error::ErrorKind::NotImplemented,
                                "this host does not support WASM Libraries",
                            ))
                        }
                    }
                    value => {
                        let (identity, definition) = crate::literal::into_put(key, value)?;
                        let segments = crate::uri::validate_identity(&identity, "lib")?;
                        if !txn.has_claim(&identity, umask::USER_WRITE) {
                            return Err(TCError::unauthorized("unauthorized Library install"));
                        }
                        let analysis = crate::ir::compile_ir_library(definition.clone())?;
                        let expected_members = analysis.members.clone();
                        let conflict_identity = identity.clone();
                        let path = identity.path()[1..].to_vec();
                        self
                            .libraries
                            .clone()
                            .create_item_if_absent(
                                txn,
                                &path,
                                &segments,
                                move |existing| {
                                    existing.same_json(&expected_members).then_some(()).ok_or_else(
                                        || {
                                            TCError::conflict(format!(
                                                "immutable Library version {conflict_identity} has conflicting content"
                                            ))
                                        },
                                    )
                                },
                                move |parent, remaining| async move {
                                    self.libraries
                                        .replicate(
                                            txn,
                                            &"/lib".parse().expect("Library root"),
                                            tc_ir::Method::Put,
                                            Scalar::Value(tc_value::Value::Link(identity.clone())),
                                            Some(crate::State::from_scalar(definition)),
                                        )
                                        .await?;
                                    parent
                                        .create_item(txn, &remaining, move |storage| {
                                            Library::create_json(
                                                txn.id(),
                                                storage,
                                                identity,
                                                analysis,
                                            )
                                        })
                                        .await
                                        .map(|_| ())
                                },
                            )
                            .await
                    }
                }
            })
        }))
    }
}

#[derive(Clone)]
pub struct Library {
    storage: txfs::Dir<TxnId, ApplicationBlock>,
    identity: Link,
    members: Map<Scalar>,
    #[cfg(feature = "wasm")]
    wasm: Option<crate::wasm::WasmLibrary>,
    scope: Arc<crate::txn::DependencyScope>,
}

impl Library {
    pub(crate) async fn create_json(
        txn_id: TxnId,
        storage: txfs::Dir<TxnId, ApplicationBlock>,
        identity: Link,
        analysis: crate::ir::LibraryAnalysis,
    ) -> TCResult<Self> {
        let scope = Arc::new(crate::txn::DependencyScope::new(
            identity.clone(),
            analysis.requirements,
        ));
        storage
            .create_file(
                txn_id,
                MANIFEST.parse().expect("manifest file name"),
                ApplicationBlock::Manifest(identity.clone(), Scalar::Map(analysis.members.clone())),
            )
            .await
            .map_err(TCError::from)?;
        Ok(Self {
            storage,
            identity,
            members: analysis.members,
            #[cfg(feature = "wasm")]
            wasm: None,
            scope,
        })
    }

    #[cfg(feature = "wasm")]
    pub(crate) async fn create_wasm(
        txn_id: TxnId,
        storage: txfs::Dir<TxnId, ApplicationBlock>,
        identity: Link,
        analysis: crate::ir::LibraryAnalysis,
        module_bytes: Arc<[u8]>,
        wasm: crate::wasm::WasmLibrary,
    ) -> TCResult<Self> {
        let scope = Arc::new(crate::txn::DependencyScope::new(
            identity.clone(),
            analysis.requirements,
        ));
        storage
            .create_file(
                txn_id,
                MODULE.parse().expect("module file name"),
                ApplicationBlock::Module(module_bytes),
            )
            .await
            .map_err(TCError::from)?;
        storage
            .create_file(
                txn_id,
                MANIFEST.parse().expect("manifest file name"),
                ApplicationBlock::Manifest(identity.clone(), Scalar::Map(analysis.members.clone())),
            )
            .await
            .map_err(TCError::from)?;
        Ok(Self {
            storage,
            identity,
            members: analysis.members,
            wasm: Some(wasm),
            scope,
        })
    }

    pub(crate) async fn load(
        compiler: super::compiler::Compiler,
        txn_id: TxnId,
        storage: txfs::Dir<TxnId, ApplicationBlock>,
    ) -> TCResult<Self> {
        let manifest_name: tc_ir::Id = MANIFEST.parse().expect("manifest file name");
        let module_name: tc_ir::Id = MODULE.parse().expect("module file name");
        let mut manifest = None;
        let mut module = None;
        let entries = storage.iter(txn_id).await.map_err(TCError::from)?;
        for (name, entry) in entries {
            let txfs::DirEntry::File(file) = &*entry else {
                return Err(TCError::bad_request("unsupported Library layout"));
            };
            if name == manifest_name {
                manifest = Some(file.clone());
            } else if name == module_name {
                module = Some(file.clone());
            } else {
                return Err(TCError::bad_request("unsupported Library layout"));
            }
        }
        let manifest =
            manifest.ok_or_else(|| TCError::bad_request("Library manifest is missing"))?;
        let block = manifest
            .read::<ApplicationBlock>(txn_id)
            .await
            .map_err(TCError::from)?;
        let ApplicationBlock::Manifest(identity, definition) = &*block else {
            return Err(TCError::bad_request("unsupported Library manifest"));
        };
        crate::uri::validate_identity(identity, "lib")?;
        let analysis = crate::ir::compile_ir_library(definition.clone())?;
        match module {
            None => {
                let scope = Arc::new(crate::txn::DependencyScope::new(
                    identity.clone(),
                    analysis.requirements,
                ));
                Ok(Self {
                    storage,
                    identity: identity.clone(),
                    members: analysis.members,
                    #[cfg(feature = "wasm")]
                    wasm: None,
                    scope,
                })
            }
            Some(module) => {
                let block = module
                    .read::<ApplicationBlock>(txn_id)
                    .await
                    .map_err(TCError::from)?;
                let ApplicationBlock::Module(bytes) = &*block else {
                    return Err(TCError::bad_request("unsupported Library module"));
                };
                #[cfg(feature = "wasm")]
                {
                    let wasm = compiler.module(bytes).await?;
                    if wasm.identity() != identity || wasm.definition() != definition {
                        return Err(TCError::internal(
                            "embedded WASM definition does not match manifest.json",
                        ));
                    }
                    if wasm.bindings().any(|binding| {
                        crate::ir::member(&analysis.members, &binding.path).is_none()
                    }) {
                        return Err(TCError::bad_request(
                            "a WASM export does not correspond to an embedded Library member",
                        ));
                    }
                    let scope = Arc::new(crate::txn::DependencyScope::new(
                        identity.clone(),
                        analysis.requirements,
                    ));
                    Ok(Self {
                        storage,
                        identity: identity.clone(),
                        members: analysis.members,
                        wasm: Some(wasm),
                        scope,
                    })
                }
                #[cfg(not(feature = "wasm"))]
                {
                    let _ = (compiler, bytes);
                    Err(TCError::new(
                        tc_error::ErrorKind::NotImplemented,
                        "this host does not support WASM Libraries",
                    ))
                }
            }
        }
    }

    pub(crate) fn identity(&self) -> &Link {
        &self.identity
    }
    pub(crate) fn scope(&self) -> Arc<crate::txn::DependencyScope> {
        Arc::clone(&self.scope)
    }
    fn same_json(&self, members: &Map<Scalar>) -> bool {
        #[cfg(feature = "wasm")]
        if self.wasm.is_some() {
            return false;
        }
        &self.members == members
    }
    #[cfg(feature = "wasm")]
    fn same_wasm(&self, members: &Map<Scalar>, module_hash: [u8; 32]) -> bool {
        self.members == *members
            && self
                .wasm
                .as_ref()
                .is_some_and(|wasm| wasm.module_hash() == module_hash)
    }
}

impl Transact for Library {
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

impl crate::cluster::DirItem for Library {
    fn identity(&self) -> &Link {
        self.identity()
    }
    fn bind(&self, txn: &crate::TxnHandle) -> crate::TxnHandle {
        txn.for_application(self.scope())
    }
}

impl crate::cluster::ResourceHash for Library {
    async fn resource_hash(&self, _txn_id: TxnId) -> TCResult<[u8; 32]> {
        let definition: [u8; 32] =
            async_hash::Hash::<async_hash::Sha256>::hash(&self.members).into();
        #[cfg(feature = "wasm")]
        let module = self
            .wasm
            .as_ref()
            .map(crate::wasm::WasmLibrary::module_hash);
        #[cfg(not(feature = "wasm"))]
        let module: Option<[u8; 32]> = None;
        Ok(async_hash::Hash::<async_hash::Sha256>::hash((definition, module)).into())
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
                wasm.clone(),
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
            Box::pin(async move {
                #[cfg(feature = "wasm")]
                if self.wasm.is_some() {
                    let module: tc_ir::Id = MODULE.parse().expect("module file name");
                    let block = self
                        .storage
                        .read_file::<ApplicationBlock>(_txn.id(), &module)
                        .await
                        .map_err(TCError::from)?;
                    let ApplicationBlock::Module(bytes) = &*block else {
                        return Err(TCError::internal("invalid WASM Library module"));
                    };
                    return Ok(crate::State::from(tc_value::Value::Bytes(bytes.clone())));
                }
                Ok(crate::State::from_scalar(Scalar::Map(self.members.clone())))
            })
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
                Ok(())
            })
        }))
    }
}
