#![allow(unsafe_op_in_unsafe_fn)]

mod conversions;
mod kernel;
mod state;
mod state_handle_conversions;
mod types;
mod wire;

pub use kernel::{KernelHandle, PyBackend, register_python_api};
pub use types::{PyKernelConfig, PyKernelRequest, PyKernelResponse, PyStateHandle, StateHandle};

pub(crate) use kernel::python_kernel_builder_with_config;

include!("pyo3_runtime/tests.rs");
