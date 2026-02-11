#![allow(unsafe_op_in_unsafe_fn)]

mod kernel;
mod state;
mod types;
mod wire;

pub use kernel::{KernelHandle, PyBackend, register_python_api};
pub use types::{PyKernelConfig, PyKernelRequest, PyKernelResponse, PyStateHandle, StateHandle};

pub(crate) use kernel::python_kernel_builder_with_config;
#[allow(unused_imports)]
pub(crate) use wire::{encode_state_to_bytes, py_bearer_token, request_body_bytes};

include!("pyo3_runtime/tests.rs");
