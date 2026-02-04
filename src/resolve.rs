use futures::future::BoxFuture;
use tc_error::{TCError, TCResult};
use tc_ir::{Map, OpRef, Subject, TCRef};
use tc_state::State;
use tc_value::Value;

use crate::gateway::RpcGateway;

pub trait Resolve: Send + Sync + 'static {
    fn resolve(
        &self,
        txn: &crate::txn::TxnHandle,
    ) -> BoxFuture<'static, TCResult<State>>;
}

impl Resolve for OpRef {
    fn resolve(
        &self,
        txn: &crate::txn::TxnHandle,
    ) -> BoxFuture<'static, TCResult<State>> {
        let txn = txn.clone();
        let op = self.clone();
        Box::pin(async move {
            match op {
                OpRef::Get((Subject::Link(link), key)) => {
                    let key = scalar_to_value(&txn, key).await?;
                    txn.get(link, txn.clone(), key).await
                }
                OpRef::Put((Subject::Link(link), key, value)) => {
                    let key = scalar_to_value(&txn, key).await?;
                    let body = scalar_to_state(&txn, value).await?;
                    txn.put(link, txn.clone(), key, body)
                        .await
                        .map(|()| State::default())
                }
                OpRef::Post((Subject::Link(link), _params)) => {
                    let params = resolve_params(&txn, _params).await?;
                    txn.post(link, txn.clone(), params).await
                }
                OpRef::Delete((Subject::Link(link), key)) => {
                    let key = scalar_to_value(&txn, key).await?;
                    txn.delete(link, txn.clone(), key)
                        .await
                        .map(|()| State::default())
                }
                OpRef::Get((Subject::Ref(_, _), _))
                | OpRef::Put((Subject::Ref(_, _), _, _))
                | OpRef::Post((Subject::Ref(_, _), _))
                | OpRef::Delete((Subject::Ref(_, _), _)) => {
                    Err(TCError::bad_request(
                        "cannot resolve OpRef subject Ref without a scope",
                    ))
                }
            }
        })
    }
}

impl Resolve for TCRef {
    fn resolve(
        &self,
        txn: &crate::txn::TxnHandle,
    ) -> BoxFuture<'static, TCResult<State>> {
        match self {
            TCRef::Op(op) => op.resolve(txn),
        }
    }
}

async fn scalar_to_value(
    txn: &crate::txn::TxnHandle,
    scalar: tc_ir::Scalar,
) -> TCResult<Value> {
    match scalar {
        tc_ir::Scalar::Value(value) => Ok(value),
        tc_ir::Scalar::Ref(r) => {
            let response = r.resolve(txn).await?;
            match response {
                State::None => Ok(Value::None),
                State::Scalar(tc_ir::Scalar::Value(value)) => Ok(value),
                State::Scalar(tc_ir::Scalar::Ref(_)) => Err(TCError::bad_request(
                    "resolved ref returned scalar ref; expected value".to_string(),
                )),
                _ => Err(TCError::bad_request(
                    "resolved ref returned non-scalar state".to_string(),
                )),
            }
        }
    }
}

async fn scalar_to_state(
    txn: &crate::txn::TxnHandle,
    scalar: tc_ir::Scalar,
) -> TCResult<State> {
    match scalar {
        tc_ir::Scalar::Value(value) => Ok(State::from(value)),
        tc_ir::Scalar::Ref(r) => {
            r.resolve(txn).await
        }
    }
}

async fn resolve_params(
    txn: &crate::txn::TxnHandle,
    params: Map<tc_ir::Scalar>,
) -> TCResult<Map<State>> {
    let mut resolved = Map::new();
    for (key, value) in params {
        let value = scalar_to_state(txn, value).await?;
        resolved.insert(key, value);
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use std::sync::{Arc, Mutex};
    use tc_state::State;
    use tc_value::Value;

    type GatewayCall = (pathlink::Link, crate::txn::TxnHandle, Value);

    #[derive(Clone, Default)]
    struct MockGateway {
        calls: Arc<Mutex<Vec<GatewayCall>>>,
    }

    impl crate::gateway::RpcGateway for MockGateway {
        fn get(
            &self,
            target: pathlink::Link,
            txn: crate::txn::TxnHandle,
            key: Value,
        ) -> BoxFuture<'static, TCResult<State>> {
            self.calls
                .lock()
                .expect("calls lock")
                .push((target, txn, key));
            futures::future::ready(Ok(State::None)).boxed()
        }

        fn put(
            &self,
            _target: pathlink::Link,
            _txn: crate::txn::TxnHandle,
            _key: Value,
            _value: State,
        ) -> BoxFuture<'static, TCResult<()>> {
            futures::future::ready(Ok(())).boxed()
        }

        fn post(
            &self,
            _target: pathlink::Link,
            _txn: crate::txn::TxnHandle,
            _params: Map<State>,
        ) -> BoxFuture<'static, TCResult<State>> {
            futures::future::ready(Ok(State::None)).boxed()
        }

        fn delete(
            &self,
            _target: pathlink::Link,
            _txn: crate::txn::TxnHandle,
            _key: Value,
        ) -> BoxFuture<'static, TCResult<()>> {
            futures::future::ready(Ok(())).boxed()
        }
    }

    #[test]
    fn resolves_opref_via_gateway() {
        let gateway = MockGateway::default();
        let op = OpRef::Get((
            Subject::Link("/lib/acme/foo/1.0.0".parse().expect("link")),
            tc_ir::Scalar::Value(tc_value::Value::None),
        ));

        let txn = crate::txn::TxnManager::with_host_id("test-host")
            .begin_with_owner(Some("owner"), Some("tok"));
        let txn = txn.with_resolver(Arc::new(gateway.clone()));

        futures::executor::block_on(op.resolve(&txn)).expect("resolve");

        let calls = gateway.calls.lock().expect("calls lock").clone();
        assert_eq!(calls.len(), 1);
        let (uri, txn_call, key) = &calls[0];
        assert_eq!(uri.to_string(), "/lib/acme/foo/1.0.0");
        assert_eq!(txn_call.id(), txn.id());
        assert_eq!(txn_call.authorization_header(), Some("Bearer tok".to_string()));
        assert_eq!(key, &Value::None);
    }
}
