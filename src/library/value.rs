//! One immutable Library resource backed by a transactional filesystem directory.

use std::sync::Arc;

use pathlink::{Link, PathSegment};
use tc_error::{TCError, TCResult};
use tc_ir::{DeleteHandler, GetHandler, Handler, Map, PutHandler, Route, Scalar, Transact, TxnId};

use crate::storage::ApplicationBlock;

const MANIFEST: &str = "manifest.json";

const MODULE: &str = "module.wasm";

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

impl<'a> Handler<'a, crate::State> for &'a Root<'_> {
    fn put<'txn>(self: Box<Self>) -> Option<PutHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, key, value| {
            Box::pin(async move {
                #[cfg(not(feature = "wasm"))]
                let module_type = None::<std::convert::Infallible>;

                let (identity, definition, module) = match (key, value) {
                    (
                        Scalar::Value(tc_value::Value::None),
                        crate::State::Scalar(Scalar::Value(tc_value::Value::Bytes(bytes))),
                    ) => {
                        #[cfg(feature = "wasm")]
                        {
                            let module = self
                                .compiler
                                .module(Arc::clone(&bytes), txn.deadline())
                                .await?;

                            (
                                module.identity().clone(),
                                module.definition().clone(),
                                Some((bytes, module)),
                            )
                        }

                        #[cfg(not(feature = "wasm"))]
                        {
                            let _ = bytes;
                            return Err(TCError::new(
                                tc_error::ErrorKind::NotImplemented,
                                "this host does not support WASM Libraries",
                            ));
                        }
                    }
                    (key, value) => {
                        let (identity, definition) = crate::literal::into_put(key, value)?;
                        #[cfg(feature = "wasm")]
                        let module_type = None;

                        (identity, definition, module_type)
                    }
                };

                let segments = crate::uri::validate_identity(&identity, "lib")?;
                if !txn.may_mutate(&identity, self.libraries.path()) {
                    return Err(TCError::unauthorized("unauthorized Library install"));
                }

                let analysis = crate::ir::compile_ir_library(definition.clone())?;
                let (runtime, bytes) = match module {
                    #[cfg(feature = "wasm")]
                    Some((bytes, module)) => {
                        (LibraryRuntime::wasm(analysis.members, module)?, Some(bytes))
                    }
                    None => (LibraryRuntime::Json(analysis.members), None),
                    #[cfg(not(feature = "wasm"))]
                    Some(never) => match never {},
                };

                self.libraries
                    .clone()
                    .create_item_if_absent(
                        txn,
                        &identity.path()[1..],
                        &segments,
                        runtime,
                        |existing, runtime| {
                            existing.runtime.same_content(runtime).then_some(()).ok_or_else(|| {
                                TCError::conflict(format!(
                                    "immutable Library version {identity} has conflicting content"
                                ))
                            })
                        },
                        |runtime| {
                            let (key, value) = match &bytes {
                                Some(bytes) => (
                                    Scalar::default(),
                                    crate::State::from(tc_value::Value::from(Arc::clone(bytes))),
                                ),
                                None => (
                                    tc_value::Value::from(identity.clone()).into(),
                                    crate::State::from(definition),
                                ),
                            };

                            ((key, value), |storage| {
                                Library::create(
                                    txn.id(),
                                    storage,
                                    identity.clone(),
                                    analysis.requirements,
                                    runtime,
                                    bytes,
                                )
                            })
                        },
                    )
                    .await
            })
        }))
    }
}

#[derive(Clone)]
enum LibraryRuntime {
    Json(Map<Scalar>),
    #[cfg(feature = "wasm")]
    Wasm {
        members: Map<Scalar>,
        module: Arc<crate::wasm::WasmLibrary>,
    },
}

impl LibraryRuntime {
    fn members(&self) -> &Map<Scalar> {
        match self {
            Self::Json(members) => members,
            #[cfg(feature = "wasm")]
            Self::Wasm { members, .. } => members,
        }
    }

    #[cfg(feature = "wasm")]
    fn wasm(members: Map<Scalar>, module: crate::wasm::WasmLibrary) -> TCResult<Self> {
        if module
            .bindings()
            .any(|binding| crate::ir::member(&members, &binding.path).is_none())
        {
            return Err(TCError::bad_request(
                "a WASM export does not correspond to an embedded Library member",
            ));
        }

        Ok(Self::Wasm {
            members,
            module: Arc::new(module),
        })
    }

    fn module_hash(&self) -> Option<[u8; 32]> {
        match self {
            Self::Json(_) => None,
            #[cfg(feature = "wasm")]
            Self::Wasm { module, .. } => Some(module.module_hash()),
        }
    }

    fn same_content(&self, other: &Self) -> bool {
        self.module_hash() == other.module_hash() && self.members() == other.members()
    }

    fn hash(&self) -> [u8; 32] {
        let definition: [u8; 32] =
            async_hash::Hash::<async_hash::Sha256>::hash(self.members()).into();

        async_hash::Hash::<async_hash::Sha256>::hash((definition, self.module_hash())).into()
    }
}

#[derive(Clone)]
pub(crate) struct Library {
    storage: txfs::Dir<TxnId, ApplicationBlock>,
    identity: Link,
    runtime: LibraryRuntime,
    scope: Arc<crate::txn::DependencyScope>,
}

impl Library {
    fn new(
        storage: txfs::Dir<TxnId, ApplicationBlock>,
        identity: Link,
        requirements: crate::txn::Requirements,
        runtime: LibraryRuntime,
    ) -> Self {
        let scope = Arc::new(crate::txn::DependencyScope::new(
            identity.clone(),
            requirements,
        ));

        Self {
            storage,
            identity,
            runtime,
            scope,
        }
    }

    async fn create(
        txn_id: TxnId,
        storage: txfs::Dir<TxnId, ApplicationBlock>,
        identity: Link,
        requirements: crate::txn::Requirements,
        runtime: LibraryRuntime,
        module: Option<Arc<[u8]>>,
    ) -> TCResult<Self> {
        if let Some(bytes) = module {
            storage
                .create_file(
                    txn_id,
                    MODULE.parse().expect("module file name"),
                    ApplicationBlock::Module(bytes),
                )
                .await?;
        }

        storage
            .create_file(
                txn_id,
                MANIFEST.parse().expect("manifest file name"),
                ApplicationBlock::Manifest(
                    identity.clone(),
                    Box::new(Scalar::Map(runtime.members().clone())),
                ),
            )
            .await?;

        Ok(Self::new(storage, identity, requirements, runtime))
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
        let analysis = crate::ir::compile_ir_library(Scalar::clone(definition))?;
        match module {
            None => Ok(Self::new(
                storage,
                identity.clone(),
                analysis.requirements,
                LibraryRuntime::Json(analysis.members),
            )),
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
                    let wasm = compiler
                        .module(Arc::clone(bytes), compiler.deadline())
                        .await?;

                    if wasm.identity() != identity || wasm.definition() != definition.as_ref() {
                        return Err(TCError::internal(
                            "embedded WASM definition does not match manifest.json",
                        ));
                    }

                    Ok(Self::new(
                        storage,
                        identity.clone(),
                        analysis.requirements,
                        LibraryRuntime::wasm(analysis.members, wasm)?,
                    ))
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
}

impl Transact for Library {
    async fn commit(&self, txn_id: TxnId) -> TCResult<()> {
        self.storage.commit(txn_id).await.map_err(TCError::from)
    }

    async fn rollback(&self, txn_id: &TxnId) -> TCResult<()> {
        self.storage.rollback(*txn_id).await.map_err(TCError::from)
    }

    async fn finalize(&self, txn_id: &TxnId) -> TCResult<()> {
        self.storage.finalize(*txn_id).await.map_err(TCError::from)
    }
}

impl crate::cluster::DirItem for Library {
    fn identity(&self) -> &Link {
        &self.identity
    }

    fn bind(&self, txn: &crate::TxnHandle) -> crate::TxnHandle {
        txn.for_application(Arc::clone(&self.scope))
    }
}

impl crate::cluster::AsyncHash for Library {
    async fn hash(&self, _txn: &crate::TxnHandle) -> TCResult<[u8; 32]> {
        Ok(self.runtime.hash())
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
        if let LibraryRuntime::Wasm { module, .. } = &self.runtime {
            return module.route(path);
        }

        crate::ir::route_member(&self.identity, self.runtime.members(), path)
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
                if matches!(self.runtime, LibraryRuntime::Wasm { .. }) {
                    let module: tc_ir::Id = MODULE.parse().expect("module file name");
                    let block = self
                        .storage
                        .read_file::<ApplicationBlock>(_txn.id(), &module)
                        .await
                        .map_err(TCError::from)?;

                    let ApplicationBlock::Module(bytes) = &*block else {
                        return Err(TCError::internal("invalid WASM Library module"));
                    };

                    return Ok(crate::State::from(tc_value::Value::from(bytes.clone())));
                }

                Ok(crate::State::from(Scalar::Map(
                    self.runtime.members().clone(),
                )))
            })
        }))
    }

    fn delete<'txn>(self: Box<Self>) -> Option<DeleteHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, key| {
            Box::pin(
                async move { crate::literal::check_delete(txn, &self.identity, &key, "Library") },
            )
        }))
    }
}
