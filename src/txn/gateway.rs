use futures::future::BoxFuture;
use pathlink::Link;
use tc_error::TCError;
use tc_state::State;
use tc_value::Value;

use crate::gateway::RpcGateway;

use super::TxnHandle;

impl RpcGateway for TxnHandle {
    fn get(
        &self,
        target: Link,
        txn: TxnHandle,
        key: Value,
    ) -> BoxFuture<'static, tc_error::TCResult<State>> {
        match &self.resolver {
            Some(resolver) => resolver.get(target, txn, key),
            None => {
                Box::pin(async move { Err(TCError::bad_gateway("no txn resolver configured")) })
            }
        }
    }

    fn put(
        &self,
        target: Link,
        txn: TxnHandle,
        key: Value,
        value: State,
    ) -> BoxFuture<'static, tc_error::TCResult<()>> {
        match &self.resolver {
            Some(resolver) => resolver.put(target, txn, key, value),
            None => {
                Box::pin(async move { Err(TCError::bad_gateway("no txn resolver configured")) })
            }
        }
    }

    fn post(
        &self,
        target: Link,
        txn: TxnHandle,
        params: tc_ir::Map<State>,
    ) -> BoxFuture<'static, tc_error::TCResult<State>> {
        match &self.resolver {
            Some(resolver) => resolver.post(target, txn, params),
            None => {
                Box::pin(async move { Err(TCError::bad_gateway("no txn resolver configured")) })
            }
        }
    }

    fn delete(
        &self,
        target: Link,
        txn: TxnHandle,
        key: Value,
    ) -> BoxFuture<'static, tc_error::TCResult<()>> {
        match &self.resolver {
            Some(resolver) => resolver.delete(target, txn, key),
            None => {
                Box::pin(async move { Err(TCError::bad_gateway("no txn resolver configured")) })
            }
        }
    }
}
