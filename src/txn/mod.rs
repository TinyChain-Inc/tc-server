mod config;
mod handle;
mod scope;
pub(crate) mod server;
mod token;
pub(crate) mod wire;

#[cfg(test)]
#[path = "../../tests/support/txn.rs"]
mod tests;

pub use config::ProtocolAuthority;
pub(crate) use config::TxnConfig;
pub use handle::TxnHandle;
pub(crate) use scope::{DependencyScope, Requirements};
pub(crate) use server::{TransactionOutcome, TxnServer};
#[cfg(test)]
pub(crate) use tests::{
    test_kernel, test_kernel_with, test_kernel_with_limits, test_path, test_txn, test_workspace,
};
pub(crate) use token::{protocol_snapshot, validate_signed_token};
