use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use tc_error::{TCError, TCResult};
use tc_ir::{Id, LibrarySchema, Map};

use crate::{
    storage::{Artifact, LibraryStore},
    txn::TxnHandle,
    uri,
};

use super::LibraryFactory;
use super::install::{InstallArtifacts, InstallError};
use super::runtime::LibraryRuntime;
use super::util::{canonical_link, is_path_prefix, normalize_path, schemas_equivalent};

#[derive(Clone)]
pub struct LibraryRegistry {
    entries: Arc<RwLock<BTreeMap<String, Arc<LibraryRuntime>>>>,
    store: Option<LibraryStore>,
    factories: BTreeMap<String, LibraryFactory>,
}

impl LibraryRegistry {
    pub fn new(store: Option<LibraryStore>, factories: BTreeMap<String, LibraryFactory>) -> Self {
        Self {
            entries: Arc::new(RwLock::new(BTreeMap::new())),
            store,
            factories,
        }
    }

    pub async fn insert_schema(&self, schema: LibrarySchema) -> TCResult<()> {
        let key = canonical_link(schema.id());
        let store = match self.store.as_ref() {
            Some(store) => Some(store.for_schema(&schema).await?),
            None => None,
        };
        let runtime = Arc::new(LibraryRuntime::new(schema, store, self.factories.clone()));
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

        for (id, _) in entries.iter() {
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
        let runtime = self
            .runtime_for_schema(&schema)
            .await
            .map_err(|err| InstallError::internal(err.to_string()))?;
        runtime.state.replace_schema(schema);
        runtime.routes.clear();

        if let Some(store) = runtime.store.as_ref() {
            store
                .persist_schema(&runtime.state.schema())
                .await
                .map_err(|err| InstallError::internal(err.to_string()))?;
        }

        Ok(())
    }

    pub async fn install_payload(&self, payload: InstallArtifacts) -> Result<(), InstallError> {
        let runtime = self
            .runtime_for_schema(&payload.schema)
            .await
            .map_err(|err| InstallError::internal(err.to_string()))?;

        let artifact = payload
            .artifacts
            .into_iter()
            .find(|artifact| self.factories.contains_key(&artifact.content_type))
            .ok_or_else(|| {
                let supported = self
                    .factories
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ");
                InstallError::bad_request(format!(
                    "missing supported artifact (expected one of: {supported})"
                ))
            })?;

        let factory = self
            .factories
            .get(&artifact.content_type)
            .ok_or_else(|| InstallError::bad_request("unsupported artifact content type"))?;

        let (manifest_schema, schema_routes, handler) = factory(artifact.bytes.clone())
            .map_err(|err| InstallError::internal(err.to_string()))?;

        if !schemas_equivalent(&manifest_schema, &payload.schema) {
            return Err(InstallError::bad_request(
                "manifest schema does not match descriptor",
            ));
        }

        runtime
            .state
            .replace_with_routes(manifest_schema.clone(), schema_routes);
        runtime.routes.replace_arc(handler);

        if let Some(store) = runtime.store.as_ref() {
            store
                .persist_artifact(
                    &manifest_schema,
                    &Artifact {
                        content_type: artifact.content_type,
                        bytes: artifact.bytes,
                        path: artifact.path,
                    },
                )
                .await
                .map_err(|err| InstallError::internal(err.to_string()))?;
        }

        Ok(())
    }

    pub async fn export_payload_for_claims(
        &self,
        txn: &TxnHandle,
    ) -> Result<Option<InstallArtifacts>, TCError> {
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

                return Ok(Some(InstallArtifacts { schema, artifacts }));
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
            runtime.hydrate_from_storage().await?;
        }

        Ok(())
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
        let runtime = Arc::new(LibraryRuntime::new(
            schema.clone(),
            store,
            self.factories.clone(),
        ));

        let mut entries = self.entries.write().expect("library registry write lock");
        let entry = entries.entry(key).or_insert_with(|| Arc::clone(&runtime));
        Ok(Arc::clone(entry))
    }
}
