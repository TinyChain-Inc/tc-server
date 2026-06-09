use aes_gcm_siv::{Aes256GcmSiv, Key};
use async_trait::async_trait;
use tc_error::TCResult;
use tc_ir::TxnId;

use crate::library::CompiledLibraryPackage;

use super::{PeerClusterListing, PeerIdentity, PeerRoutes};

#[async_trait]
pub trait ClusterGateway: Send + Sync + 'static {
    async fn discover_library_paths(&self, peer: &str) -> TCResult<Vec<String>>;

    async fn request_replication_token(
        &self,
        peer: &str,
        path: &str,
        keys: &[Key<Aes256GcmSiv>],
    ) -> TCResult<String>;

    async fn fetch_compiled_library_package(
        &self,
        peer: &str,
        token: &str,
    ) -> TCResult<Option<CompiledLibraryPackage>>;

    async fn register_with_peer(
        &self,
        seed: &str,
        joiner: &PeerIdentity,
        routes: &PeerRoutes,
        keys: &[Key<Aes256GcmSiv>],
    ) -> TCResult<PeerClusterListing>;

    async fn push_install_compiled_package(
        &self,
        peer: &str,
        token: &str,
        txn_id: TxnId,
        payload: Vec<u8>,
    ) -> TCResult<()>;

    async fn finalize_install_txn(
        &self,
        peer: &str,
        token: &str,
        txn_id: TxnId,
        commit: bool,
    ) -> TCResult<()>;
}
