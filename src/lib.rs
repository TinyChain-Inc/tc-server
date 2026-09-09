#![deny(unsafe_code)]

mod application;
pub mod auth;
mod cluster;
pub use application::ApplicationOwners;
pub mod class;
pub mod gateway;
pub mod host;
pub mod ir;
pub mod kernel;
pub mod op_executor;
#[cfg(feature = "http-client")]
pub(crate) mod outbound_http;
pub mod replication;
pub mod resources;
pub mod service;
pub mod uri;

pub use hyper::{Body, Method as HttpMethod, StatusCode, header};
pub use kernel::{BodyContract, HostServices, Kernel, KernelRequestGuard, Method};
pub type Request = hyper::Request<hyper::Body>;
pub type Response = hyper::Response<hyper::Body>;
pub use gateway::{LocalRpcGateway, RpcGateway as RpcClient};
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
pub mod wasm;

pub mod http;

#[cfg(feature = "http-client")]
pub mod http_client;

#[cfg(feature = "http-server")]
pub use http::{HttpKernelConfig, HttpRuntime, HttpServer, build_http_runtime_with_config};
#[cfg(feature = "http-client")]
pub use http_client::HttpRpcGateway;
