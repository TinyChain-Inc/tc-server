use std::sync::{Arc, RwLock};

use tc_error::{TCError, TCResult};
use tc_ir::LibrarySchema;

use crate::storage::{Artifact, LibraryStore};

use super::{CompiledLibrary, LibraryCompiler, LibraryExecution, LibraryState};

pub struct LibraryRuntime {
    pub(crate) state: LibraryState,
    execution: Arc<RwLock<Option<LibraryExecution>>>,
    artifact: Arc<RwLock<Option<Artifact>>>,
    pub(crate) store: Option<LibraryStore>,
}

impl LibraryRuntime {
    pub fn new(initial_schema: LibrarySchema, store: Option<LibraryStore>) -> Self {
        Self {
            state: LibraryState::new(initial_schema),
            execution: Arc::new(RwLock::new(None)),
            artifact: Arc::new(RwLock::new(None)),
            store,
        }
    }

    pub fn replace_execution(&self, execution: LibraryExecution) {
        self.execution
            .write()
            .expect("library execution write lock")
            .replace(execution);
    }

    pub fn execution(&self) -> Option<LibraryExecution> {
        self.execution
            .read()
            .expect("library execution read lock")
            .clone()
    }

    pub fn replace_compiled(&self, compiled: CompiledLibrary) {
        self.state
            .replace_with_routes(compiled.schema, compiled.routes);
        self.artifact
            .write()
            .expect("library artifact write lock")
            .replace(compiled.artifact);
        self.replace_execution(compiled.execution);
    }

    pub fn artifact(&self) -> Option<Artifact> {
        self.artifact
            .read()
            .expect("library artifact read lock")
            .clone()
    }

    pub async fn hydrate_from_storage(
        &self,
        compilers: &std::collections::BTreeMap<String, LibraryCompiler>,
    ) -> TCResult<()> {
        let store = match &self.store {
            Some(store) => store,
            None => return Ok(()),
        };

        let schema = self.state.schema();
        let artifact = match store.load_artifact(&schema).await? {
            Some(artifact) => artifact,
            None => return Ok(()),
        };

        let compiler = match compilers.get(&artifact.content_type) {
            Some(compiler) => Arc::clone(compiler),
            None => return Ok(()),
        };
        let compiled = compiler(artifact).await?;

        if compiled.schema != schema {
            return Err(TCError::internal("persisted schema mismatch"));
        }

        self.replace_compiled(compiled);

        Ok(())
    }
}

impl Clone for LibraryRuntime {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            execution: Arc::clone(&self.execution),
            artifact: Arc::clone(&self.artifact),
            store: self.store.clone(),
        }
    }
}
