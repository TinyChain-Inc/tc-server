use crate::State;
use tc_ir::Method;

/// A transport-neutral local route invocation.
pub struct KernelRequest {
    pub method: Method,
    pub path: pathlink::Link,
    /// `None` is an absent request body; `Some(State::None)` is an explicit
    /// runtime value.
    pub body: Option<State>,
    pub txn: crate::txn::TxnHandle,
}

/// A transaction bound for native execution.
pub(crate) struct BoundTransaction {
    pub(crate) txn: crate::txn::TxnHandle,
    pub(crate) implicit: bool,
}
