use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use tc_error::TCResult;
use tc_ir::{OpRef, TCRef};
use tc_state::State;

pub trait Resolve: Send + Sync + 'static {
    fn resolve(&self, txn: &crate::txn::TxnHandle) -> BoxFuture<'static, TCResult<State>>;
}

impl Resolve for OpRef {
    fn resolve(&self, txn: &crate::txn::TxnHandle) -> BoxFuture<'static, TCResult<State>> {
        let txn = txn.clone();
        let values = Arc::new(HashMap::new());
        let scalar = tc_ir::Scalar::Ref(Box::new(TCRef::Op(self.clone())));

        Box::pin(
            async move { crate::op_executor::resolve_scalar(scalar, &values, &txn, None).await },
        )
    }
}

impl Resolve for TCRef {
    fn resolve(&self, txn: &crate::txn::TxnHandle) -> BoxFuture<'static, TCResult<State>> {
        let txn = txn.clone();
        let values = Arc::new(HashMap::new());
        let scalar = tc_ir::Scalar::Ref(Box::new(self.clone()));

        Box::pin(
            async move { crate::op_executor::resolve_scalar(scalar, &values, &txn, None).await },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use std::sync::{Arc, Mutex};
    use tc_ir::{Cond, Map, Scalar, Subject};
    use tc_state::State;
    use tc_value::Value;

    type GatewayCall = (pathlink::Link, crate::txn::TxnHandle, Value);
    type PostCall = (pathlink::Link, crate::txn::TxnHandle, Map<State>);

    #[derive(Clone, Default)]
    struct MockGateway {
        calls: Arc<Mutex<Vec<GatewayCall>>>,
        post_calls: Arc<Mutex<Vec<PostCall>>>,
        get_response: Arc<Mutex<State>>,
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
            let state = self.get_response.lock().expect("get response").clone();
            futures::future::ready(Ok(state)).boxed()
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
            target: pathlink::Link,
            txn: crate::txn::TxnHandle,
            params: Map<State>,
        ) -> BoxFuture<'static, TCResult<State>> {
            self.post_calls
                .lock()
                .expect("post calls lock")
                .push((target, txn, params));
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
        assert_eq!(
            txn_call.authorization_header(),
            Some("Bearer tok".to_string())
        );
        assert_eq!(key, &Value::None);
    }

    #[test]
    fn tcref_cond_uses_shared_resolver_path() {
        let gateway = MockGateway::default();
        *gateway.get_response.lock().expect("get response") =
            State::Scalar(Scalar::Value(Value::Bool(true)));
        let cond_ref = TCRef::Op(OpRef::Get((
            Subject::Link("/lib/acme/foo/1.0.0".parse().expect("link")),
            Scalar::Value(Value::None),
        )));
        let cond = TCRef::Cond(Box::new(Cond::new(
            cond_ref,
            Scalar::Value(Value::from("yes")),
            Scalar::Value(Value::from("no")),
        )));

        let txn = crate::txn::TxnManager::with_host_id("test-host")
            .begin_with_owner(Some("owner"), Some("tok"));
        let txn = txn.with_resolver(Arc::new(gateway.clone()));

        let state = futures::executor::block_on(cond.resolve(&txn)).expect("resolve cond");
        assert!(matches!(
            state,
            State::Scalar(Scalar::Value(Value::String(ref s))) if s == "yes"
        ));

        let calls = gateway.calls.lock().expect("calls lock").clone();
        assert_eq!(calls.len(), 1);
    }

    #[test]
    fn tcref_id_without_scope_reports_unknown_id() {
        let id_ref = TCRef::Id("$missing".parse().expect("id ref"));
        let txn = crate::txn::TxnManager::with_host_id("test-host")
            .begin_with_owner(Some("owner"), Some("tok"));

        let err = futures::executor::block_on(id_ref.resolve(&txn)).expect_err("expected error");
        assert!(err.message().contains("unknown id $missing"));
    }

    #[test]
    fn opref_post_subject_ref_link_value_routes_via_gateway() {
        let gateway = MockGateway::default();
        let txn = crate::txn::TxnManager::with_host_id("test-host")
            .begin_with_owner(Some("owner"), Some("tok"));
        let txn = txn.with_resolver(Arc::new(gateway.clone()));

        let mut values = HashMap::new();
        values.insert(
            "target".parse().expect("Id"),
            State::from(Value::Link("/lib/acme/foo/1.0.0".parse().expect("link"))),
        );
        let values = Arc::new(values);
        let scalar = Scalar::Ref(Box::new(TCRef::Op(OpRef::Post((
            Subject::Ref(
                "$target".parse().expect("IdRef"),
                "bar".parse().expect("suffix"),
            ),
            Map::new(),
        )))));

        futures::executor::block_on(crate::op_executor::resolve_scalar(
            scalar, &values, &txn, None,
        ))
        .expect("resolve ref subject post");

        let calls = gateway.post_calls.lock().expect("post calls").clone();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0.to_string(), "/lib/acme/foo/1.0.0/bar");
    }
}
