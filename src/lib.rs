#![deny(unsafe_code)]

pub mod auth;
pub mod egress;
pub mod gateway;
pub mod host;
pub mod ir;
pub mod kernel;
pub mod op_executor;
pub mod op_plan;
pub(crate) mod outbound_http;
pub mod reflect;
pub mod replication;
pub mod resolve;
pub mod resources;
pub mod state;
pub mod uri;

pub use hyper::{Body, Method as HttpMethod, StatusCode, header};
pub use kernel::{Kernel, KernelBuilder, KernelRequest, Method};
pub type Request = hyper::Request<hyper::Body>;
pub type Response = hyper::Response<hyper::Body>;
pub use gateway::RpcGateway as RpcClient;
pub use tc_ir::{Dir, Handler, Route, Transaction, parse_route_path};

pub mod library;
#[cfg(any(feature = "http-server", feature = "pyo3"))]
pub use library::NativeLibrary;

pub mod storage;
pub use resources::{
    CapacitySnapshot, Deadline, DeviceLimits, ExecutionLimits, HostLimits, HostResources,
    IngressLimits, StorageLimits,
};
pub use storage::HostStorage;

pub mod txn;
pub use txn::TxnHandle;
pub mod workspace;
pub use workspace::Workspace;

pub type State = tc_state::State<TxnHandle>;
pub use tc_value::Value;

#[cfg(feature = "wasm")]
pub mod wasm;

#[cfg(any(feature = "http-server", feature = "pyo3"))]
pub mod http;

#[cfg(feature = "http-client")]
pub mod http_client;

#[cfg(feature = "http-server")]
pub use http::{HttpKernelConfig, HttpRuntime, HttpServer, build_http_runtime_with_config};
#[cfg(feature = "http-client")]
pub use http_client::HttpRpcGateway;

#[cfg(feature = "pyo3")]
pub mod pyo3_runtime;

#[cfg(feature = "pyo3")]
pub use pyo3_runtime::{
    KernelHandle as PyKernelHandle, PyKernelRequest, PyResponse, register_python_api,
};
