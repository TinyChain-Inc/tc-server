use futures::future::BoxFuture;

use crate::Response;

#[allow(clippy::large_enum_variant)]
pub enum KernelDispatch {
    Response(BoxFuture<'static, Response>),
    Finalize {
        commit: bool,
        txn: crate::txn::TxnHandle,
    },
    NotFound,
}

pub(super) fn dispatch_or_not_found(fut: Option<BoxFuture<'static, Response>>) -> KernelDispatch {
    match fut {
        Some(fut) => KernelDispatch::Response(fut),
        None => KernelDispatch::NotFound,
    }
}
