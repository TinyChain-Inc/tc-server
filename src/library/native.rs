use std::sync::Arc;

use tc_ir::{Dir, Library, LibraryModule, LibrarySchema};

#[derive(Clone)]
pub struct NativeLibrary<H> {
    schema: LibrarySchema,
    routes: Arc<Dir<H>>,
}

impl<H> NativeLibrary<H>
where
    H: Clone + Send + Sync,
{
    pub fn new(module: LibraryModule<crate::State, Dir<H>>) -> Self {
        Self {
            schema: module.schema().clone(),
            routes: Arc::new(module.routes().clone()),
        }
    }

    pub fn schema(&self) -> &LibrarySchema {
        &self.schema
    }

    pub fn routes(&self) -> Arc<Dir<H>> {
        Arc::clone(&self.routes)
    }
}
