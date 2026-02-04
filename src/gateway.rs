use futures::future::BoxFuture;
use pathlink::Link;
use tc_error::TCResult;
use tc_ir::Map;
use tc_state::State;
use tc_value::Value;

use crate::txn::TxnHandle;

pub trait RpcGateway: Send + Sync + 'static {
    fn get(
        &self,
        target: Link,
        txn: TxnHandle,
        key: Value,
    ) -> BoxFuture<'static, TCResult<State>>;

    fn put(
        &self,
        target: Link,
        txn: TxnHandle,
        key: Value,
        value: State,
    ) -> BoxFuture<'static, TCResult<()>>;

    fn post(
        &self,
        target: Link,
        txn: TxnHandle,
        params: Map<State>,
    ) -> BoxFuture<'static, TCResult<State>>;

    fn delete(
        &self,
        target: Link,
        txn: TxnHandle,
        key: Value,
    ) -> BoxFuture<'static, TCResult<()>>;
}
