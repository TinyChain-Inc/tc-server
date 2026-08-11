use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use tc_error::{TCError, TCResult};
use tc_ir::{Id, LibrarySchema, Map, TxnId};

use super::install::{CompiledLibraryPackage, InstallError};
use super::runtime::LibraryRuntime;
use super::util::{canonical_link, is_path_prefix, normalize_path, schemas_equivalent};
use super::{CompiledLibrary, LibraryCompiler, LibraryExecution};
use crate::storage::{Artifact, LibraryStore};
use crate::txn::{ParticipantSet, TxnHandle};
use crate::uri;

#[derive(Clone)]
pub struct LibraryRegistry {
    entries: Arc<RwLock<BTreeMap<String, Arc<LibraryRuntime>>>>,
    staged: Arc<RwLock<BTreeMap<TxnId, StagedTxn>>>,
    store: Option<LibraryStore>,
    compilers: BTreeMap<String, LibraryCompiler>,
}

#[derive(Clone, Default)]
struct StagedTxn {
    installs: Vec<StagedInstall>,
    replication_participants: ParticipantSet<String>,
}

#[derive(Clone)]
struct StagedInstall {
    schema_path: String,
    runtime: Arc<LibraryRuntime>,
    install: PreparedInstall,
}

#[derive(Clone)]
enum PreparedInstall {
    Payload(CompiledLibrary),
}

impl LibraryRegistry {
    pub fn new(store: Option<LibraryStore>, compilers: BTreeMap<String, LibraryCompiler>) -> Self {
        Self {
            entries: Arc::new(RwLock::new(BTreeMap::new())),
            staged: Arc::new(RwLock::new(BTreeMap::new())),
            store,
            compilers,
        }
    }

    pub async fn insert_schema(&self, schema: LibrarySchema) -> TCResult<()> {
        let key = canonical_link(schema.id());
        let store = match self.store.as_ref() {
            Some(store) => Some(store.for_schema(&schema).await?),
            None => None,
        };
        let runtime = Arc::new(LibraryRuntime::new(schema, store));
        self.entries
            .write()
            .expect("library registry write lock")
            .insert(key, runtime);

        Ok(())
    }

    pub fn list_dir(&self, path: &str) -> Option<Map<bool>> {
        let path = normalize_path(path);
        let entries = self.entries.read().expect("library registry read lock");
        let mut out = Map::new();
        let mut has_match = path == uri::LIB_ROOT;

        for id in entries.keys() {
            if !is_path_prefix(&path, id) {
                continue;
            }

            has_match = true;

            if path == *id {
                continue;
            }

            let rest = id.strip_prefix(&path).unwrap_or(id).trim_start_matches('/');

            if rest.is_empty() {
                continue;
            }

            let mut segments = rest.split('/');
            let child = segments.next().expect("non-empty rest segment");
            let is_dir = segments.next().is_some();
            let Ok(child_id) = child.parse::<Id>() else {
                continue;
            };
            let entry = out.entry(child_id).or_insert(is_dir);
            if is_dir {
                *entry = true;
            }
        }

        if has_match { Some(out) } else { None }
    }

    pub fn resolve_runtime_for_path(&self, path: &str) -> Option<(Arc<LibraryRuntime>, bool)> {
        let path = normalize_path(path);
        let entries = self.entries.read().expect("library registry read lock");
        let mut best: Option<(&String, Arc<LibraryRuntime>)> = None;

        for (id, runtime) in entries.iter() {
            if !is_path_prefix(id, &path) {
                continue;
            }

            let replace = match &best {
                Some((best_id, _)) => id.len() > best_id.len(),
                None => true,
            };

            if replace {
                best = Some((id, Arc::clone(runtime)));
            }
        }

        best.map(|(id, runtime)| (runtime, id == &path))
    }

    pub(crate) fn resolve_native(
        &self,
        path: &str,
    ) -> Option<(crate::ir::IrRoutes, Vec<pathlink::PathSegment>, bool)> {
        let (runtime, is_root) = self.resolve_runtime_for_path(path)?;
        let root = runtime.state.schema().id().to_string();
        let relative = path.strip_prefix(&root)?;
        let route = tc_ir::parse_route_path(relative).ok()?;
        match runtime.execution()? {
            LibraryExecution::Native(routes) => Some((routes, route, is_root)),
            LibraryExecution::Transport => None,
        }
    }

    pub fn has_route_root(&self, root: &str) -> bool {
        let root = normalize_path(root);
        self.entries
            .read()
            .expect("library registry read lock")
            .contains_key(&root)
    }

    pub fn schema_for_txn(&self, txn: &TxnHandle) -> TCResult<LibrarySchema> {
        let mut best: Option<(usize, LibrarySchema)> = None;

        for claim in txn.claims().iter().chain(std::iter::once(txn.claim())) {
            let path = canonical_link(&claim.link);
            if let Some((runtime, _)) = self.resolve_runtime_for_path(&path) {
                let schema = runtime.state.schema();
                let score = schema.id().to_string().len();
                let replace = best.as_ref().is_none_or(|(len, _)| score > *len);
                if replace {
                    best = Some((score, schema));
                }
            }
        }

        if let Some((_, schema)) = best {
            return Ok(schema);
        }

        let entries = self.entries.read().expect("library registry read lock");
        if entries.len() == 1 {
            let schema = entries
                .values()
                .next()
                .expect("single entry")
                .state
                .schema();
            return Ok(schema);
        }

        Err(TCError::unauthorized(
            "no library manifest loaded (egress is default-deny)",
        ))
    }

    pub async fn install_schema(&self, schema: LibrarySchema) -> Result<(), InstallError> {
        if let Some(store) = self.store.as_ref() {
            let store = store
                .for_schema(&schema)
                .await
                .map_err(|err| InstallError::internal(err.to_string()))?;
            store
                .persist_schema_immediate(&schema)
                .await
                .map_err(|err| InstallError::internal(err.to_string()))?;
        }

        self.insert_schema(schema)
            .await
            .map_err(|err| InstallError::internal(err.to_string()))
    }

    pub async fn install_compiled_package(
        &self,
        payload: CompiledLibraryPackage,
    ) -> Result<(), InstallError> {
        let prepared = self.prepare_compiled_package(payload).await?;
        self.apply_prepared_install(&prepared).await
    }

    pub async fn stage_install_request(
        &self,
        txn_id: TxnId,
        payload: CompiledLibraryPackage,
    ) -> Result<String, InstallError> {
        self.stage_compiled_package(txn_id, payload).await
    }

    pub async fn stage_compiled_package(
        &self,
        txn_id: TxnId,
        payload: CompiledLibraryPackage,
    ) -> Result<String, InstallError> {
        let prepared = self.prepare_compiled_package(payload).await?;
        let schema_path = prepared.schema_path.clone();
        self.record_staged_install(txn_id, prepared.clone());
        self.stage_prepared_storage(txn_id, &prepared).await?;
        Ok(schema_path)
    }

    pub fn has_staged_txn(&self, txn_id: TxnId) -> bool {
        self.staged
            .read()
            .expect("library staged read lock")
            .contains_key(&txn_id)
    }

    pub fn record_replication_participants(&self, txn_id: TxnId, participants: Vec<String>) {
        let mut staged = self.staged.write().expect("library staged write lock");
        let txn = staged.entry(txn_id).or_default();
        txn.replication_participants = participants.into_iter().collect();
    }

    pub(crate) fn replication_participants(&self, txn_id: TxnId) -> Option<ParticipantSet<String>> {
        self.staged
            .read()
            .expect("library staged read lock")
            .get(&txn_id)
            .map(|txn| txn.replication_participants.clone())
            .filter(|participants| !participants.is_empty())
    }

    pub(crate) fn retain_unfinished_replication_participants(
        &self,
        txn_id: TxnId,
        delivered: &ParticipantSet<String>,
    ) {
        let mut staged = self.staged.write().expect("library staged write lock");
        if let Some(txn) = staged.get_mut(&txn_id) {
            txn.replication_participants.retain_unresolved(delivered);
        }
    }

    pub async fn finalize_txn(&self, txn_id: TxnId, commit: bool) -> TCResult<()> {
        let staged = self
            .staged
            .read()
            .expect("library staged read lock")
            .get(&txn_id)
            .map(|txn| txn.installs.clone())
            .unwrap_or_default();

        for install in &staged {
            self.finalize_prepared_storage(txn_id, commit, install)
                .await
                .map_err(|err| TCError::internal(err.message().to_string()))?;
        }

        if commit {
            for install in &staged {
                self.apply_prepared_runtime(install);
            }
        }

        self.staged
            .write()
            .expect("library staged write lock")
            .remove(&txn_id);

        Ok(())
    }

    pub async fn export_compiled_package_for_claims(
        &self,
        txn: &TxnHandle,
    ) -> Result<Option<CompiledLibraryPackage>, TCError> {
        let runtimes = {
            let entries = self.entries.read().expect("library registry read lock");
            entries.values().cloned().collect::<Vec<_>>()
        };

        for runtime in runtimes {
            let schema = runtime.state.schema();
            if txn.has_claim(schema.id(), umask::USER_READ) {
                let Some(store) = &runtime.store else {
                    return Ok(None);
                };
                let artifact = match store.load_artifact(&schema).await? {
                    Some(artifact) => artifact,
                    None => return Ok(None),
                };

                let artifacts = vec![Artifact {
                    path: schema.id().to_string(),
                    content_type: artifact.content_type,
                    bytes: artifact.bytes,
                }];

                return Ok(Some(CompiledLibraryPackage { schema, artifacts }));
            }
        }

        Err(TCError::unauthorized("unauthorized"))
    }

    pub async fn hydrate_from_storage(&self) -> TCResult<()> {
        let store = match &self.store {
            Some(store) => store,
            None => return Ok(()),
        };

        let entries = store.discover_schemas().await?;
        for schema in entries {
            let runtime = self.runtime_for_schema(&schema).await?;
            runtime.hydrate_from_storage(&self.compilers).await?;
        }

        Ok(())
    }

    fn record_staged_install(&self, txn_id: TxnId, install: StagedInstall) {
        let mut staged = self.staged.write().expect("library staged write lock");
        let txn = &mut staged.entry(txn_id).or_default().installs;
        if let Some(existing) = txn
            .iter_mut()
            .find(|existing| existing.schema_path == install.schema_path)
        {
            *existing = install;
            return;
        }

        txn.push(install);
    }

    async fn prepare_compiled_package(
        &self,
        payload: CompiledLibraryPackage,
    ) -> Result<StagedInstall, InstallError> {
        let runtime = self
            .runtime_for_schema(&payload.schema)
            .await
            .map_err(|err| InstallError::internal(err.to_string()))?;

        let mut artifacts = payload
            .artifacts
            .into_iter()
            .filter(|artifact| self.compilers.contains_key(&artifact.content_type));
        let artifact = artifacts.next().ok_or_else(|| {
            let supported = self
                .compilers
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            InstallError::bad_request(format!(
                "missing supported artifact (expected one of: {supported})"
            ))
        })?;
        if artifacts.next().is_some() {
            return Err(InstallError::bad_request(
                "package contains multiple executable artifacts",
            ));
        }

        let compiler = self
            .compilers
            .get(&artifact.content_type)
            .ok_or_else(|| InstallError::bad_request("unsupported artifact content type"))?;

        let compiled = compiler(artifact.clone())
            .await
            .map_err(|err| InstallError::internal(err.to_string()))?;

        if !schemas_equivalent(&compiled.schema, &payload.schema) {
            return Err(InstallError::bad_request(
                "manifest schema does not match descriptor",
            ));
        }

        let schema_path = compiled.schema.id().to_string();

        Ok(StagedInstall {
            schema_path,
            runtime,
            install: PreparedInstall::Payload(compiled),
        })
    }

    async fn apply_prepared_install(&self, install: &StagedInstall) -> Result<(), InstallError> {
        self.persist_prepared_storage(install).await?;
        self.apply_prepared_runtime(install);

        Ok(())
    }

    fn apply_prepared_runtime(&self, install: &StagedInstall) {
        match &install.install {
            PreparedInstall::Payload(compiled) => {
                install.runtime.replace_compiled(compiled.clone())
            }
        }
    }

    async fn persist_prepared_storage(&self, install: &StagedInstall) -> Result<(), InstallError> {
        let Some(store) = install.runtime.store.as_ref() else {
            return Ok(());
        };

        match &install.install {
            PreparedInstall::Payload(compiled) => store
                .persist_artifact_immediate(&compiled.schema, &compiled.artifact)
                .await
                .map_err(|err| InstallError::internal(err.to_string())),
        }
    }

    async fn stage_prepared_storage(
        &self,
        txn_id: TxnId,
        install: &StagedInstall,
    ) -> Result<(), InstallError> {
        let Some(store) = install.runtime.store.as_ref() else {
            return Ok(());
        };

        match &install.install {
            PreparedInstall::Payload(compiled) => store
                .stage_artifact(txn_id, &compiled.schema, &compiled.artifact)
                .await
                .map_err(|err| InstallError::internal(err.to_string())),
        }
    }

    async fn finalize_prepared_storage(
        &self,
        txn_id: TxnId,
        commit: bool,
        install: &StagedInstall,
    ) -> Result<(), InstallError> {
        let Some(store) = install.runtime.store.as_ref() else {
            return Ok(());
        };

        store
            .finalize_txn(txn_id, commit)
            .await
            .map_err(|err| InstallError::internal(err.to_string()))
    }

    async fn runtime_for_schema(&self, schema: &LibrarySchema) -> TCResult<Arc<LibraryRuntime>> {
        let key = canonical_link(schema.id());
        if let Some(existing) = self
            .entries
            .read()
            .expect("library registry read lock")
            .get(&key)
            .cloned()
        {
            return Ok(existing);
        }

        let store = match self.store.as_ref() {
            Some(store) => Some(store.for_schema(schema).await?),
            None => None,
        };
        let runtime = Arc::new(LibraryRuntime::new(schema.clone(), store));

        let mut entries = self.entries.write().expect("library registry write lock");
        let entry = entries.entry(key).or_insert_with(|| Arc::clone(&runtime));
        Ok(Arc::clone(entry))
    }
}
