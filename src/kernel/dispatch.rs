use futures::future::BoxFuture;

use crate::Response;

pub enum KernelDispatch {
    Response(BoxFuture<'static, Response>),
    Finalize {
        commit: bool,
        result: Result<(), crate::txn::TxnError>,
    },
    NotFound,
}

pub(super) fn dispatch_or_not_found(fut: Option<BoxFuture<'static, Response>>) -> KernelDispatch {
    match fut {
        Some(fut) => KernelDispatch::Response(fut),
        None => KernelDispatch::NotFound,
    }
}
