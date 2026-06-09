use std::{collections::BTreeMap, sync::Arc};

use tc_error::{TCError, TCResult};
use tc_ir::LibrarySchema;

use crate::storage::LibraryStore;

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
