use std::sync::Arc;

use tc_error::TCError;
use tc_ir::{
    Dir, HandleDelete, HandleGet, HandlePost, HandlePut, Library, LibraryModule, LibrarySchema,
};
use tc_value::Value;

use crate::txn::TxnHandle;

#[derive(Clone)]
pub struct NativeLibrary<H> {
    schema: LibrarySchema,
    routes: Arc<Dir<H>>,
}

impl<H> NativeLibrary<H>
where
    H: Clone,
{
    pub fn new(module: LibraryModule<TxnHandle, Dir<H>>) -> Self {
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

pub trait NativeLibraryHandler: HandleGet<TxnHandle, Request = Value, RequestContext = (), Response = Value, Error = TCError>
    + HandlePut<TxnHandle, Request = Value, RequestContext = (), Response = Value, Error = TCError>
    + HandlePost<TxnHandle, Request = Value, RequestContext = (), Response = Value, Error = TCError>
    + HandleDelete<TxnHandle, Request = Value, RequestContext = (), Response = Value, Error = TCError>
    + Clone
    + Send
    + Sync
    + 'static
{
}

impl<T> NativeLibraryHandler for T where
    T: HandleGet<
            TxnHandle,
            Request = Value,
            RequestContext = (),
            Response = Value,
            Error = TCError,
        > + HandlePut<
            TxnHandle,
            Request = Value,
            RequestContext = (),
            Response = Value,
            Error = TCError,
        > + HandlePost<
            TxnHandle,
            Request = Value,
            RequestContext = (),
            Response = Value,
            Error = TCError,
        > + HandleDelete<
            TxnHandle,
            Request = Value,
            RequestContext = (),
            Response = Value,
            Error = TCError,
        > + Clone
        + Send
        + Sync
        + 'static
{
}
