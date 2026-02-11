mod client;
mod crypto;
mod handler;
mod issuer;

#[cfg(test)]
mod tests;

use std::time::Duration;

pub const LIBRARY_EXPORT_PATH: &str = crate::uri::HOST_LIBRARY_EXPORT;
const TOKEN_PATH: &str = "/";
const REPLICATION_TTL: Duration = Duration::from_secs(30);

pub use client::{
    discover_library_paths, fetch_library_export, fetch_library_listing, fetch_library_schema,
    request_replication_token,
};
pub use handler::{export_handler, replication_token_handler};
pub use issuer::{parse_psk_keys, parse_psk_list, ReplicationIssuer};
