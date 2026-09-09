use aes_gcm_siv::{Aes256GcmSiv, Key};
use async_trait::async_trait;
use tc_error::TCResult;
use tc_ir::TxnId;

use super::{PeerClusterListing, PeerIdentity, PeerRoutes};

use super::CanonicalBody;

#[async_trait]
pub trait ClusterGateway: Send + Sync + 'static {
    fn replicas(&self, resource: &pathlink::PathBuf) -> std::collections::BTreeSet<String>;

    async fn register_with_peer(
        &self,
        seed: &str,
        joiner: &PeerIdentity,
        routes: &PeerRoutes,
        keys: &[Key<Aes256GcmSiv>],
    ) -> TCResult<PeerClusterListing>;

    async fn put_application(
        &self,
        peer: &str,
        token: &str,
        txn_id: TxnId,
        application: CanonicalBody,
        deadline: crate::Deadline,
    ) -> TCResult<()>;

    async fn delete_application(
        &self,
        peer: &str,
        token: &str,
        txn_id: TxnId,
        identity: &pathlink::Link,
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
    fn replicas(&self, _resource: &pathlink::PathBuf) -> std::collections::BTreeSet<String> {
        std::collections::BTreeSet::new()
    }

    async fn register_with_peer(
        &self,
        _seed: &str,
        _joiner: &PeerIdentity,
        _routes: &PeerRoutes,
        _keys: &[Key<Aes256GcmSiv>],
    ) -> TCResult<PeerClusterListing> {
        Err(tc_error::TCError::bad_gateway("local cluster has no peers"))
    }

    async fn put_application(
        &self,
        _peer: &str,
        _token: &str,
        _txn_id: TxnId,
        _application: CanonicalBody,
        _deadline: crate::Deadline,
    ) -> TCResult<()> {
        Err(tc_error::TCError::bad_gateway("local cluster has no peers"))
    }

    async fn delete_application(
        &self,
        _peer: &str,
        _token: &str,
        _txn_id: TxnId,
        _identity: &pathlink::Link,
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
