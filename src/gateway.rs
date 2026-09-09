use crate::State;
use futures::future::BoxFuture;
use pathlink::Link;
use tc_error::TCResult;
use tc_ir::{Map, Scalar};

use crate::txn::TxnHandle;

#[cfg(any(feature = "http-client", feature = "http-server"))]
pub(crate) const EXPECTED_DIGEST_HEADER: &str = "x-tc-application-digest";

#[derive(Clone)]
pub enum RpcTarget {
    Host(Link),
    Application {
        link: Link,
        expected_digest: crate::application::Digest,
    },
}

impl RpcTarget {
    pub fn into_parts(self) -> (Link, Option<crate::application::Digest>) {
        match self {
            Self::Host(link) => (link, None),
            Self::Application {
                link,
                expected_digest,
            } => (link, Some(expected_digest)),
        }
    }
}

pub trait RpcGateway: Send + Sync + 'static {
    fn get(
        &self,
        target: RpcTarget,
        txn: TxnHandle,
        key: Scalar,
    ) -> BoxFuture<'static, TCResult<State>>;

    fn put(
        &self,
        target: RpcTarget,
        txn: TxnHandle,
        key: Scalar,
        value: State,
    ) -> BoxFuture<'static, TCResult<()>>;

    fn post(
        &self,
        target: RpcTarget,
        txn: TxnHandle,
        params: Map<State>,
    ) -> BoxFuture<'static, TCResult<State>>;

    fn delete(
        &self,
        target: RpcTarget,
        txn: TxnHandle,
        key: Scalar,
    ) -> BoxFuture<'static, TCResult<()>>;
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
    fn get(&self, _: RpcTarget, _: TxnHandle, _: Scalar) -> BoxFuture<'static, TCResult<State>> {
        local_only()
    }

    fn put(
        &self,
        _: RpcTarget,
        _: TxnHandle,
        _: Scalar,
        _: State,
    ) -> BoxFuture<'static, TCResult<()>> {
        local_only()
    }

    fn post(
        &self,
        _: RpcTarget,
        _: TxnHandle,
        _: Map<State>,
    ) -> BoxFuture<'static, TCResult<State>> {
        local_only()
    }

    fn delete(&self, _: RpcTarget, _: TxnHandle, _: Scalar) -> BoxFuture<'static, TCResult<()>> {
        local_only()
    }
}
