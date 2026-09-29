use async_trait::async_trait;
use tc_error::TCResult;

/// Replication preserves the active transaction's identity and deadline.
/// The explicit token may be a peer-issued bootstrap token rather than the
/// transaction's local authorization token.
#[async_trait]
pub trait ClusterGateway: Send + Sync + 'static {
    async fn put(
        &self,
        peer: &str,
        token: &str,
        txn: &crate::TxnHandle,
        target: &pathlink::Link,
        key: tc_ir::Scalar,
        value: crate::State,
    ) -> TCResult<()>;

    async fn delete(
        &self,
        peer: &str,
        token: &str,
        txn: &crate::TxnHandle,
        target: &pathlink::Link,
        key: tc_ir::Scalar,
    ) -> TCResult<()>;

    async fn decide_resource(
        &self,
        peer: &str,
        token: &str,
        txn: &crate::TxnHandle,
        resource: &pathlink::PathBuf,
        commit: bool,
    ) -> TCResult<()>;
}

/// Explicit single-host cluster capability.
#[derive(Clone, Copy, Debug, Default)]
pub struct LocalClusterGateway;

#[async_trait]
impl ClusterGateway for LocalClusterGateway {
    async fn put(
        &self,
        _peer: &str,
        _token: &str,
        _txn: &crate::TxnHandle,
        _target: &pathlink::Link,
        _key: tc_ir::Scalar,
        _value: crate::State,
    ) -> TCResult<()> {
        Err(tc_error::TCError::bad_gateway("local cluster has no peers"))
    }

    async fn delete(
        &self,
        _peer: &str,
        _token: &str,
        _txn: &crate::TxnHandle,
        _target: &pathlink::Link,
        _key: tc_ir::Scalar,
    ) -> TCResult<()> {
        Err(tc_error::TCError::bad_gateway("local cluster has no peers"))
    }

    async fn decide_resource(
        &self,
        _peer: &str,
        _token: &str,
        _txn: &crate::TxnHandle,
        _resource: &pathlink::PathBuf,
        _commit: bool,
    ) -> TCResult<()> {
        Err(tc_error::TCError::bad_gateway("local cluster has no peers"))
    }
}
