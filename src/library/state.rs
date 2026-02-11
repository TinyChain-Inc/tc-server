use std::{
    str::FromStr,
    sync::{Arc, RwLock},
};

use pathlink::Link;
use tc_ir::{Library, LibraryModule, LibrarySchema};

use crate::txn::TxnHandle;

use super::SchemaRoutes;

#[derive(Clone)]
pub struct LibraryState {
    inner: Arc<RwLock<LibraryModule<TxnHandle, SchemaRoutes>>>,
}

impl LibraryState {
    pub fn new(initial: LibrarySchema) -> Self {
        Self {
            inner: Arc::new(RwLock::new(LibraryModule::new(
                initial,
                SchemaRoutes::new(),
            ))),
        }
    }

    pub fn schema(&self) -> LibrarySchema {
        self.inner
            .read()
            .expect("library schema read lock")
            .schema()
            .clone()
    }

    pub fn replace_schema(&self, schema: LibrarySchema) {
        *self.inner.write().expect("library schema write lock") =
            LibraryModule::new(schema, SchemaRoutes::new());
    }

    pub fn replace_with_routes(&self, schema: LibrarySchema, routes: SchemaRoutes) {
        *self.inner.write().expect("library schema write lock") =
            LibraryModule::new(schema, routes);
    }
}

/// Default schema used when no persisted library is present on disk yet.
pub fn default_library_schema() -> LibrarySchema {
    LibrarySchema::new(
        Link::from_str("/lib/default").expect("default library link"),
        "0.0.0",
        vec![],
    )
}
