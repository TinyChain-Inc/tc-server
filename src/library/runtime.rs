use std::{collections::BTreeMap, sync::Arc};

use tc_error::{TCError, TCResult};
use tc_ir::LibrarySchema;
use umask::Mode;

use crate::storage::{Artifact, LibraryStore};

use super::{LibraryFactory, LibraryRoutes, LibraryState};

pub struct LibraryRuntime {
    pub(crate) state: LibraryState,
    pub(crate) routes: LibraryRoutes,
    pub(crate) store: Option<LibraryStore>,
    pub(crate) factories: BTreeMap<String, LibraryFactory>,
}

impl LibraryRuntime {
    pub fn new(
        initial_schema: LibrarySchema,
        store: Option<LibraryStore>,
        factories: BTreeMap<String, LibraryFactory>,
    ) -> Self {
        Self {
            state: LibraryState::new(initial_schema),
            routes: LibraryRoutes::new(),
            store,
            factories,
        }
    }

    pub async fn hydrate_from_storage(&self) -> TCResult<()> {
        let store = match &self.store {
            Some(store) => store,
            None => return Ok(()),
        };

        let schema = self.state.schema();
        let artifact = match store.load_artifact(&schema).await? {
            Some(artifact) => artifact,
            None => return Ok(()),
        };

        let factory = match self.factories.get(&artifact.content_type) {
            Some(factory) => Arc::clone(factory),
            None => return Ok(()),
        };
        let (manifest_schema, schema_routes, handler) = factory(artifact.bytes)?;

        if manifest_schema != schema {
            return Err(TCError::internal("persisted schema mismatch"));
        }

        self.state
            .replace_with_routes(manifest_schema, schema_routes);
        self.routes.replace_arc(handler);

        Ok(())
    }

    pub async fn install_artifact_bytes(
        &self,
        content_type: &str,
        bytes: Vec<u8>,
        txn: Option<&crate::txn::TxnHandle>,
    ) -> TCResult<LibrarySchema> {
        let factory = self
            .factories
            .get(content_type)
            .ok_or_else(|| TCError::bad_request("artifact installs are not supported"))?;

        let (manifest_schema, schema_routes, handler) = factory(bytes.clone())?;

        match txn {
            Some(txn)
                if !txn.has_claim(
                    manifest_schema.id(),
                    Mode::new().with_class_perm(umask::USER, umask::WRITE),
                ) =>
            {
                return Err(TCError::unauthorized("unauthorized library install"));
            }
            _ => {}
        }

        self.state
            .replace_with_routes(manifest_schema.clone(), schema_routes);
        self.routes.replace_arc(handler);

        if let Some(store) = &self.store {
            store
                .persist_artifact(
                    &manifest_schema,
                    &Artifact {
                        content_type: content_type.to_string(),
                        bytes,
                        path: manifest_schema.id().to_string(),
                    },
                )
                .await?;
        }

        Ok(manifest_schema)
    }
}

impl Clone for LibraryRuntime {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            routes: self.routes.clone(),
            store: self.store.clone(),
            factories: self.factories.clone(),
        }
    }
}
