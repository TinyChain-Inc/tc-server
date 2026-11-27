#![deny(unsafe_code)]
#![warn(unreachable_pub)]

#[cfg(test)]
#[path = "../tests/support/runtime.rs"]
mod test_runtime;

pub mod auth;
pub use auth::Claim;

mod class;
mod cluster;
pub mod gateway;
mod host;
mod ir;
pub mod kernel;
mod literal;

pub use literal::{Definition, wasm_installation};

mod op_executor;
#[cfg(feature = "http-client")]
pub(crate) mod outbound_http;
pub mod replication;
pub mod resources;
mod service;
pub mod uri;

pub use gateway::LocalRpcGateway;
pub use kernel::{BodyContract, BoundRequest, HostServices, Kernel, Method};
pub use tc_ir::{Handler, Route, Transaction};

mod library;
pub mod storage;
pub use resources::{
    Deadline, ExecutionLimits, HostLimits, HostResources, IngressLimits, StorageLimits,
};
pub use storage::{ApplicationRoots, HostStorage};

pub mod txn;

pub use txn::{ProtocolAuthority, TxnHandle};

pub mod workspace;
pub use workspace::Workspace;

pub type State = tc_state::State<TxnHandle>;
pub use tc_value::Value;

#[cfg(feature = "wasm")]
mod wasm;

pub mod http;
#[cfg(any(feature = "http-client", feature = "http-server"))]
mod http_body;
#[cfg(feature = "http-client")]
pub mod http_client;

#[cfg(feature = "http-server")]
pub use http::HttpServer;
#[cfg(feature = "http-client")]
pub use http_client::HttpGateway;
