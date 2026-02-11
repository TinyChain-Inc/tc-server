pub mod auth;
pub mod egress;
pub mod gateway;
pub mod ir;
pub mod kernel;
pub mod op_executor;
pub mod op_plan;
pub mod reflect;
pub mod replication;
pub mod resolve;
pub mod state;
pub mod txn_server;
pub mod uri;

pub use hyper::Method as HttpMethod;
pub use hyper::{Body, StatusCode, header};
pub use kernel::{Kernel, KernelBuilder, KernelDispatch, KernelHandler, Method};
pub type Request = hyper::Request<hyper::Body>;
pub type Response = hyper::Response<hyper::Body>;
pub use gateway::RpcGateway as RpcClient;
pub use tc_ir::{
    Dir, HandleDelete, HandleGet, HandlePost, HandlePut, Handler, Route, Transaction,
    parse_route_path,
};

#[cfg(test)]
pub(crate) mod test_utils;

pub mod library;
#[cfg(any(feature = "http-server", feature = "pyo3"))]
pub use library::NativeLibrary;

pub mod storage;

pub mod txn;
pub use txn::{TxnHandle, TxnManager};
pub use txn_server::TxnServer;

pub use tc_state::State;
pub use tc_value::Value;

#[derive(Clone, Copy, Debug)]
pub struct KernelLimits {
    pub txn_ttl: std::time::Duration,
    pub max_request_bytes_unauth: usize,
}

impl Default for KernelLimits {
    fn default() -> Self {
        Self {
            txn_ttl: std::time::Duration::from_secs(3),
            max_request_bytes_unauth: 1 * 1024 * 1024,
        }
    }
}

#[cfg(feature = "wasm")]
pub mod wasm;

#[cfg(any(feature = "http-server", feature = "pyo3"))]
pub mod http;

#[cfg(feature = "http-client")]
pub mod http_client;

#[cfg(feature = "http-client")]
pub use http_client::HttpRpcGateway;

#[cfg(feature = "http-server")]
pub use http::{HttpKernelConfig, HttpServer, build_http_kernel, build_http_kernel_with_config};

#[cfg(feature = "pyo3")]
pub mod pyo3_runtime;

#[cfg(feature = "pyo3")]
use pyo3::prelude::*;

#[cfg(feature = "pyo3")]
pub use pyo3_runtime::{
    KernelHandle as PyKernelHandle, PyKernelRequest, PyKernelResponse, register_python_api,
};

#[cfg(feature = "pyo3")]
#[allow(dead_code)]
pub(crate) fn build_python_kernel(
    lib: Py<PyAny>,
    service: Py<PyAny>,
    metrics: Option<Py<PyAny>>,
) -> Kernel {
    pyo3_runtime::python_kernel_builder_with_config(
        lib,
        service,
        metrics,
        pyo3_runtime::PyKernelConfig::default(),
    )
}

#[cfg(feature = "pyo3")]
#[allow(dead_code)]
pub(crate) fn build_python_kernel_with_config(
    lib: Py<PyAny>,
    service: Py<PyAny>,
    metrics: Option<Py<PyAny>>,
    config: pyo3_runtime::PyKernelConfig,
) -> Kernel {
    pyo3_runtime::python_kernel_builder_with_config(lib, service, metrics, config)
}
