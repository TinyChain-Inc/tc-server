use async_trait::async_trait;
use tc_error::TCResult;
use tc_ir::TxnId;

#[async_trait]
pub trait ClusterGateway: Send + Sync + 'static {
    async fn put(
        &self,
        peer: &str,
        token: &str,
        txn_id: TxnId,
        target: &pathlink::Link,
        key: tc_ir::Scalar,
        value: crate::State,
        deadline: crate::Deadline,
    ) -> TCResult<()>;

    async fn delete(
        &self,
        peer: &str,
        token: &str,
        txn_id: TxnId,
        target: &pathlink::Link,
        key: tc_ir::Scalar,
        deadline: crate::Deadline,
    ) -> TCResult<()>;

    async fn decide_resource(
        &self,
        peer: &str,
        token: &str,
        txn_id: TxnId,
        resource: &pathlink::PathBuf,
        commit: bool,
        deadline: crate::Deadline,
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
        _txn_id: TxnId,
        _target: &pathlink::Link,
        _key: tc_ir::Scalar,
        _value: crate::State,
        _deadline: crate::Deadline,
    ) -> TCResult<()> {
        Err(tc_error::TCError::bad_gateway("local cluster has no peers"))
    }

    async fn delete(
        &self,
        _peer: &str,
        _token: &str,
        _txn_id: TxnId,
        _target: &pathlink::Link,
        _key: tc_ir::Scalar,
        _deadline: crate::Deadline,
    ) -> TCResult<()> {
        Err(tc_error::TCError::bad_gateway("local cluster has no peers"))
    }

    async fn decide_resource(
        &self,
        _peer: &str,
        _token: &str,
        _txn_id: TxnId,
        _resource: &pathlink::PathBuf,
        _commit: bool,
        _deadline: crate::Deadline,
    ) -> TCResult<()> {
        Err(tc_error::TCError::bad_gateway("local cluster has no peers"))
    }
}
