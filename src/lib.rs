#![deny(unsafe_code)]
#![warn(unreachable_pub)]

pub mod auth;
pub use auth::Claim;
pub mod class;
mod cluster;
pub mod gateway;
pub mod host;
pub mod ir;
pub mod kernel;
mod literal;
mod op_executor;
#[cfg(feature = "http-client")]
pub(crate) mod outbound_http;
pub mod replication;
pub mod resources;
pub mod service;
pub mod uri;

pub use gateway::{LocalRpcGateway, RpcGateway as RpcClient};
pub use kernel::{BodyContract, HostServices, Kernel, KernelRequestGuard, Method};
pub use tc_ir::{Handler, Route, Transaction};

pub mod library;
pub mod storage;
pub use resources::{
    CapacitySnapshot, Deadline, DeviceLimits, ExecutionLimits, HostLimits, HostResources,
    IngressLimits, StorageLimits,
};
pub use storage::{ApplicationRoots, HostStorage};

pub mod txn;
pub use txn::ProtocolAuthority;
pub use txn::TxnHandle;
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
