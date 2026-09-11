use crate::State;
use futures::future::BoxFuture;
use pathlink::Link;
use tc_error::TCResult;
use tc_ir::{Map, Scalar};

use crate::txn::TxnHandle;

pub trait RpcGateway: Send + Sync + 'static {
    fn get(&self, target: Link, txn: TxnHandle, key: Scalar)
    -> BoxFuture<'static, TCResult<State>>;

    fn put(
        &self,
        target: Link,
        txn: TxnHandle,
        key: Scalar,
        value: State,
    ) -> BoxFuture<'static, TCResult<()>>;

    fn post(
        &self,
        target: Link,
        txn: TxnHandle,
        params: Map<State>,
    ) -> BoxFuture<'static, TCResult<State>>;

    fn delete(&self, target: Link, txn: TxnHandle, key: Scalar)
    -> BoxFuture<'static, TCResult<()>>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LocalRpcGateway;

fn local_only<T: Send + 'static>() -> BoxFuture<'static, TCResult<T>> {
    Box::pin(async {
        Err(tc_error::TCError::bad_gateway(
            "local host has no RPC transport",
        ))
    })
}

impl RpcGateway for LocalRpcGateway {
    fn get(&self, _: Link, _: TxnHandle, _: Scalar) -> BoxFuture<'static, TCResult<State>> {
        local_only()
    }

    fn put(&self, _: Link, _: TxnHandle, _: Scalar, _: State) -> BoxFuture<'static, TCResult<()>> {
        local_only()
    }

    fn post(&self, _: Link, _: TxnHandle, _: Map<State>) -> BoxFuture<'static, TCResult<State>> {
        local_only()
    }

    fn delete(&self, _: Link, _: TxnHandle, _: Scalar) -> BoxFuture<'static, TCResult<()>> {
        local_only()
    }
}
