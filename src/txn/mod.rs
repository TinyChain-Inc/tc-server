mod config;
mod handle;
pub(crate) mod server;
mod token;
pub(crate) mod wire;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) async fn test_txn(host_id: &str) -> TxnHandle {
    let kernel = test_kernel(host_id).await;
    kernel.test_txn().await
}

#[cfg(test)]
pub(crate) async fn test_txn_with_workspace(
    host_id: &str,
    workspace: crate::Workspace,
) -> TxnHandle {
    let kernel = test_kernel_with(host_id, std::time::Duration::from_secs(3), workspace).await;
    kernel.test_txn().await
}

#[cfg(test)]
pub(crate) async fn test_kernel(host_id: &str) -> crate::Kernel {
    test_kernel_with(
        host_id,
        std::time::Duration::from_secs(3),
        test_workspace(host_id),
    )
    .await
}

#[cfg(test)]
pub(crate) async fn test_kernel_with(
    host_id: &str,
    ttl: std::time::Duration,
    workspace: crate::Workspace,
) -> crate::Kernel {
    test_kernel_with_limits(host_id, ttl, workspace, crate::HostResources::default()).await
}

#[cfg(test)]
pub(crate) async fn test_kernel_with_limits(
    host_id: &str,
    ttl: std::time::Duration,
    workspace: crate::Workspace,
    resources: crate::HostResources,
) -> crate::Kernel {
    let (authority, _) = workspace
        .load_or_create_protocol_authority(
            host_id,
            crate::uri::HOST_ROOT.parse().expect("host root"),
        )
        .await
        .expect("test protocol authority");
    let applications = test_applications_with(host_id, authority.clone()).await;
    let config = TxnConfig::new(authority.clone(), workspace.clone(), resources.clone(), ttl);
    let verifier = server::test_verifier(&config);
    crate::Kernel::new(
        crate::HostServices {
            applications,
            rpc: std::sync::Arc::new(crate::gateway::LocalRpcGateway),
            resources,
            protocol: authority,
            verifier,
            public_keys: crate::auth::PublicKeyStore::default(),
        },
        workspace,
        ttl,
    )
    .await
    .expect("construct test kernel")
}

#[cfg(all(test, not(feature = "wasm")))]
pub(crate) async fn test_applications(name: &str) -> std::sync::Arc<crate::ApplicationOwners> {
    let workspace = test_workspace(&format!("apps-{name}"));
    let (authority, _) = workspace
        .load_or_create_protocol_authority(name, crate::uri::HOST_ROOT.parse().expect("host root"))
        .await
        .expect("test protocol authority");
    test_applications_with(name, authority).await
}

#[cfg(test)]
pub(crate) async fn test_applications_with(
    name: &str,
    authority: crate::ProtocolAuthority,
) -> std::sync::Arc<crate::ApplicationOwners> {
    let root = test_path(&format!("apps-{name}"));
    let storage = crate::HostStorage::new(&crate::HostLimits::default().storage);
    let roots = storage
        .application_roots(root)
        .await
        .expect("test application roots");
    std::sync::Arc::new(
        crate::ApplicationOwners::new(
            roots,
            authority,
            std::sync::Arc::new(crate::replication::LocalClusterGateway),
        )
        .await
        .expect("test applications"),
    )
}

#[cfg(test)]
pub(crate) fn test_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "tc-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

#[cfg(test)]
pub(crate) fn test_workspace(name: &str) -> crate::Workspace {
    crate::HostStorage::new(&crate::HostLimits::default().storage)
        .workspace(test_path(&format!("txn-{name}")))
        .expect("test workspace")
}

pub(crate) use config::TxnConfig;
pub use config::{ProtocolAuthority, TxnError};
pub use handle::{AuthContext, TxnHandle};
pub(crate) use server::{TransactionOutcome, TxnServer};
pub(crate) use token::{protocol_snapshot, validate_signed_token};
