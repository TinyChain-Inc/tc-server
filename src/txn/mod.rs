mod config;
mod gateway;
mod handle;
mod participants;
mod server;
mod token;
pub(crate) mod wire;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) fn test_txn(host_id: &str) -> TxnHandle {
    TxnServer::test(host_id).test_txn()
}

#[cfg(test)]
pub(crate) fn test_txn_with_workspace(host_id: &str, workspace: crate::Workspace) -> TxnHandle {
    TxnServer::test_with(host_id, std::time::Duration::from_secs(3), Some(workspace)).test_txn()
}

pub(crate) use config::TxnConfig;
pub use config::TxnError;
pub use handle::{AuthContext, TxnHandle};
pub(crate) use participants::ParticipantSet;
pub(crate) use server::{TransactionOutcome, TxnFinalize, TxnServer};
pub(crate) use token::{owner_id_from_token, validate_signed_token};
