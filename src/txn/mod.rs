mod config;
mod handle;
mod scope;
pub(crate) mod server;
mod token;
pub(crate) mod wire;

#[cfg(test)]
#[path = "../../tests/support/txn.rs"]
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
            &host_id.parse().expect("test host ID"),
            crate::uri::HOST_ROOT.parse().expect("host root"),
        )
        .await
        .expect("test protocol authority");
    let storage = crate::HostStorage::new(&crate::HostLimits::default().storage);
    let application_roots = storage
        .application_roots(test_path(&format!("apps-{host_id}")))
        .await
        .expect("test application roots");
    let config = TxnConfig::new(authority.clone(), workspace.clone(), resources.clone(), ttl);
    let verifier = server::test_verifier(&config);
    let actors = crate::auth::KeyringActorResolver::default();
    let bootstrap = std::sync::Arc::new(
        crate::replication::ReplicationIssuer::local(&authority, actors.clone())
            .expect("test replication issuer"),
    );
    crate::Kernel::new(
        crate::HostServices {
            application_roots,
            replication: std::sync::Arc::new(crate::replication::LocalClusterGateway),
            rpc: std::sync::Arc::new(crate::gateway::LocalRpcGateway),
            resources,
            protocol: authority,
            verifier,
            actors,
            bootstrap,
            bootstrap_required: false,
        },
        workspace,
        ttl,
    )
    .await
    .expect("construct test kernel")
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

pub use crate::auth::AuthContext;
pub use config::ProtocolAuthority;
pub(crate) use config::TxnConfig;
pub use handle::TxnHandle;
pub(crate) use scope::{DependencyScope, Requirements};
pub(crate) use server::{TransactionOutcome, TxnServer};
pub(crate) use token::{protocol_snapshot, validate_signed_token};
