pub mod auth;
pub mod egress;
pub mod gateway;
pub mod kernel;
pub mod resolve;
pub mod txn_server;
pub mod uri;

pub use kernel::{Kernel, KernelBuilder, KernelDispatch, KernelHandler, Method};
pub use tc_ir::{
    Dir, HandleDelete, HandleGet, HandlePost, HandlePut, Handler, Route, Transaction,
    parse_route_path,
};

#[cfg(test)]
pub(crate) mod test_utils;

pub mod library;
pub use library::NativeLibrary;

pub mod storage;

pub mod txn;
pub use txn::{TxnHandle, TxnManager};
pub use txn_server::TxnServer;

pub use tc_state::State;
pub use tc_value::Value;

#[cfg(feature = "wasm")]
pub mod wasm;

#[cfg(feature = "http-server")]
pub mod http;

#[cfg(feature = "http-client")]
pub(crate) mod http_client;

#[cfg(feature = "http-server")]
pub use http::{
    HttpKernel, HttpKernelConfig, HttpServer, build_http_kernel, build_http_kernel_with_config,
};

#[cfg(feature = "pyo3")]
pub mod pyo3_runtime;

#[cfg(feature = "pyo3")]
use pyo3::prelude::*;

#[cfg(feature = "pyo3")]
pub use pyo3_runtime::{
    KernelHandle as PyKernelHandle, PyKernel, PyKernelRequest, PyKernelResponse,
    register_python_api,
};

#[cfg(feature = "pyo3")]
#[allow(dead_code)]
pub(crate) fn build_python_kernel(
    lib: Py<PyAny>,
    service: Py<PyAny>,
    metrics: Option<Py<PyAny>>,
) -> PyKernel {
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
) -> PyKernel {
    pyo3_runtime::python_kernel_builder_with_config(lib, service, metrics, config)
}

#[cfg(feature = "pyo3")]
#[allow(deprecated)]
#[pymodule]
fn tinychain(_py: Python<'_>, module: &PyModule) -> PyResult<()> {
    register_python_api(module)
}
