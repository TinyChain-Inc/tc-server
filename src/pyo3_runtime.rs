// PyO3 0.21 macros emit Rust 2024 unsafe operations in generated FFI glue.
// Handwritten unsafe code remains prohibited by the repository source guard.
#![allow(unsafe_op_in_unsafe_fn)]

use pyo3::prelude::PyAnyMethods;

mod kernel;
mod state_handle_conversions;
mod types;
mod wire;

pub(super) fn tc_error(err: tc_error::TCError) -> pyo3::PyErr {
    let exception = pyo3::exceptions::PyRuntimeError::new_err(err.message().to_string());
    pyo3::Python::with_gil(|py| {
        let value = exception.value_bound(py);
        let _ = value.setattr("code", err.code().to_string());
        if let Some(pressure) = err.pressure() {
            let _ = value.setattr("reason", pressure.reason().to_string());
            let _ = value.setattr("resource", pressure.resource());
            let _ = value.setattr("retry_after_ms", pressure.retry_after_ms());
            let _ = value.setattr("reliability", pressure.reliability().to_string());
        }
    });
    exception
}

pub use kernel::{KernelHandle, PyBackend, register_python_api};
pub use types::{PyKernelConfig, PyKernelRequest, PyResponse, PyStateHandle};

fn runtime() -> std::io::Result<std::sync::Arc<tokio::runtime::Runtime>> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()
        .map(std::sync::Arc::new)
}

fn wait<F>(runtime: &tokio::runtime::Runtime, future: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (send, receive) = std::sync::mpsc::sync_channel(1);
    runtime.spawn(async move {
        let _ = send.send(future.await);
    });
    pyo3::Python::with_gil(|py| {
        py.allow_threads(move || receive.recv().expect("PyO3 runtime task"))
    })
}
