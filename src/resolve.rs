use futures::{FutureExt, future::BoxFuture};
use tc_ir::{OpRef, Subject, TCRef, TxnId};

use crate::{
    Method,
    gateway::{RpcError, RpcGateway, RpcResponse},
};

pub trait Resolve: Send + Sync + 'static {
    fn resolve_op(
        &self,
        txn_id: TxnId,
        bearer_token: Option<String>,
        op: OpRef,
    ) -> BoxFuture<'static, Result<RpcResponse, RpcError>>;

    fn resolve_ref(
        &self,
        txn_id: TxnId,
        bearer_token: Option<String>,
        r: TCRef,
    ) -> BoxFuture<'static, Result<RpcResponse, RpcError>> {
        match r {
            TCRef::Op(op) => self.resolve_op(txn_id, bearer_token, op),
        }
    }
}

impl<G> Resolve for G
where
    G: RpcGateway + ?Sized,
{
    fn resolve_op(
        &self,
        txn_id: TxnId,
        bearer_token: Option<String>,
        op: OpRef,
    ) -> BoxFuture<'static, Result<RpcResponse, RpcError>> {
        let (method, uri) = match op {
            OpRef::Get((Subject::Link(link), _key)) => (Method::Get, link.to_string()),
            OpRef::Put((Subject::Link(link), _key, _value)) => (Method::Put, link.to_string()),
            OpRef::Post((Subject::Link(link), _params)) => (Method::Post, link.to_string()),
            OpRef::Delete((Subject::Link(link), _key)) => (Method::Delete, link.to_string()),
            OpRef::Get((Subject::Ref(_, _), _))
            | OpRef::Put((Subject::Ref(_, _), _, _))
            | OpRef::Post((Subject::Ref(_, _), _))
            | OpRef::Delete((Subject::Ref(_, _), _)) => {
                return futures::future::ready(Err(RpcError::InvalidTarget(
                    "cannot resolve OpRef subject Ref without a scope".to_string(),
                )))
                .boxed();
            }
        };

        self.request(method, uri, txn_id, bearer_token, Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use std::sync::{Arc, Mutex};
    use tc_ir::NetworkTime;

    type GatewayCall = (Method, String, TxnId, Option<String>, Vec<u8>);

    #[derive(Clone, Default)]
    struct MockGateway {
        calls: Arc<Mutex<Vec<GatewayCall>>>,
    }

    impl RpcGateway for MockGateway {
        fn request(
            &self,
            method: Method,
            uri: String,
            txn_id: TxnId,
            bearer_token: Option<String>,
            body: Vec<u8>,
        ) -> BoxFuture<'static, Result<RpcResponse, RpcError>> {
            self.calls
                .lock()
                .expect("calls lock")
                .push((method, uri, txn_id, bearer_token, body));
            futures::future::ready(Ok(RpcResponse {
                status: 200,
                headers: vec![],
                body: vec![],
            }))
            .boxed()
        }
    }

    #[test]
    fn resolves_opref_via_gateway() {
        let gateway = MockGateway::default();
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(1), 1).with_trace([0; 32]);

        let op = OpRef::Get((
            Subject::Link("/lib/acme/foo/1.0.0".parse().expect("link")),
            tc_ir::Scalar::Value(tc_value::Value::None),
        ));

        futures::executor::block_on((&gateway as &dyn RpcGateway).resolve_op(
            txn_id,
            Some("tok".into()),
            op,
        ))
        .expect("resolve");

        let calls = gateway.calls.lock().expect("calls lock").clone();
        assert_eq!(calls.len(), 1);
        let (method, uri, id, token, body) = &calls[0];
        assert_eq!(*method, Method::Get);
        assert_eq!(uri, "/lib/acme/foo/1.0.0");
        assert_eq!(*id, txn_id);
        assert_eq!(token.as_deref(), Some("tok"));
        assert!(body.is_empty());
    }
}
