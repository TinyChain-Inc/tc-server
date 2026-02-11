use std::{fmt, marker::PhantomData, path::PathBuf, sync::Arc};

use pyo3::prelude::*;
use tc_ir::LibrarySchema;

use crate::{Kernel, Method, library::default_library_schema};

use super::wire::parse_method;

#[derive(Clone)]
pub struct PyKernelConfig {
    pub data_dir: Option<PathBuf>,
    pub initial_schema: LibrarySchema,
    pub host_id: String,
    pub limits: crate::KernelLimits,
}

impl Default for PyKernelConfig {
    fn default() -> Self {
        Self {
            data_dir: None,
            initial_schema: default_library_schema(),
            host_id: "tc-py-host".to_string(),
            limits: crate::KernelLimits::default(),
        }
    }
}

pub(super) fn apply_config_overrides(
    mut config: PyKernelConfig,
    request_ttl_secs: Option<u64>,
    max_request_bytes_unauth: Option<usize>,
) -> PyKernelConfig {
    if let Some(secs) = request_ttl_secs.filter(|value| *value > 0) {
        config.limits.txn_ttl = std::time::Duration::from_secs(secs);
    }
    if let Some(max_bytes) = max_request_bytes_unauth.filter(|value| *value > 0) {
        config.limits.max_request_bytes_unauth = max_bytes;
    }
    config
}

/// Wrapper around a typed state value shared between handlers.
///
/// This intentionally mirrors the generics on `Handler` so the same concrete state
/// types can flow between verb implementations and higher-level bindings without
/// serializing/deserializing when a call chain stays in-process (e.g. PyO3 eager
/// execution). The handle simply holds an `Arc<T>` and exposes lightweight helpers
/// to borrow or clone that pointer; downcasting happens through the same visitor
/// logic used for handlers in general rather than through a per-handle vtable.
pub struct StateHandle<State> {
    inner: Arc<State>,
    _marker: PhantomData<State>,
}

impl<State> StateHandle<State> {
    /// Create a new handle from an owned value.
    pub fn new(state: State) -> Self {
        Self {
            inner: Arc::new(state),
            _marker: PhantomData,
        }
    }

    /// Wrap an existing `Arc` without copying.
    pub fn from_arc(state: Arc<State>) -> Self {
        Self {
            inner: state,
            _marker: PhantomData,
        }
    }

    /// Clone the inner `Arc` for use in another call-chain.
    pub fn to_arc(&self) -> Arc<State> {
        Arc::clone(&self.inner)
    }

    /// Consume the handle and return the inner `Arc`.
    pub fn into_inner(self) -> Arc<State> {
        self.inner
    }
}

impl<State> Clone for StateHandle<State> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            _marker: PhantomData,
        }
    }
}

impl<State> AsRef<State> for StateHandle<State> {
    fn as_ref(&self) -> &State {
        &self.inner
    }
}

impl<State: fmt::Debug> fmt::Debug for StateHandle<State> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateHandle").finish()
    }
}

#[derive(Clone)]
pub(super) struct PyWrapper<T> {
    inner: T,
}

impl<T> PyWrapper<T> {
    pub(super) fn new(inner: T) -> Self {
        Self { inner }
    }

    pub(super) fn inner(&self) -> &T {
        &self.inner
    }
}

#[pyclass(name = "StateHandle")]
#[derive(Clone, Debug)]
pub struct PyStateHandle {
    handle: StateHandle<Py<PyAny>>,
}

#[pymethods]
impl PyStateHandle {
    #[new]
    pub fn new(value: Py<PyAny>) -> Self {
        Self {
            handle: StateHandle::new(value),
        }
    }

    pub fn clone_handle(&self) -> Self {
        self.clone()
    }

    pub fn value(&self) -> Py<PyAny> {
        self.handle.as_ref().clone()
    }
}

impl PyStateHandle {
    pub fn inner(&self) -> &StateHandle<Py<PyAny>> {
        &self.handle
    }
}

impl From<StateHandle<Py<PyAny>>> for PyStateHandle {
    fn from(handle: StateHandle<Py<PyAny>>) -> Self {
        Self { handle }
    }
}

impl From<PyStateHandle> for StateHandle<Py<PyAny>> {
    fn from(handle: PyStateHandle) -> Self {
        handle.handle
    }
}

#[pyclass(name = "KernelRequest")]
#[derive(Clone)]
pub struct PyKernelRequest {
    pub(super) method: Method,
    pub(super) path: String,
    pub(super) headers: Vec<(String, String)>,
    pub(super) body: Option<PyStateHandle>,
    // Retained for future Python adapter hooks; kept in-sync with the HTTP kernel path.
    #[allow(dead_code)]
    pub(super) kernel: Option<Arc<Kernel>>,
}

impl fmt::Debug for PyKernelRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KernelRequest")
            .field("method", &self.method.as_str())
            .field("path", &self.path)
            .field("headers", &self.headers)
            .finish()
    }
}

#[pymethods]
impl PyKernelRequest {
    #[new]
    pub fn new(
        method: &str,
        path: &str,
        headers: Option<Vec<(String, String)>>,
        body: Option<PyStateHandle>,
    ) -> PyResult<Self> {
        Ok(Self {
            method: parse_method(method)?,
            path: path.to_string(),
            headers: headers.unwrap_or_default(),
            body,
            kernel: None,
        })
    }

    #[getter]
    pub fn method(&self) -> &'static str {
        self.method.as_str()
    }

    #[getter]
    pub fn path(&self) -> &str {
        &self.path
    }

    #[getter]
    pub fn headers(&self) -> Vec<(String, String)> {
        self.headers.clone()
    }

    #[getter]
    pub fn body(&self) -> Option<PyStateHandle> {
        self.body.clone()
    }
}

impl PyKernelRequest {
    pub(crate) fn method_enum(&self) -> Method {
        self.method
    }

    pub(crate) fn path_owned(&self) -> String {
        self.path.clone()
    }

    // Reserved for upcoming Python adapter integration; not wired yet.
    #[allow(dead_code)]
    pub(crate) fn bind_kernel(&mut self, kernel: Arc<Kernel>) {
        self.kernel = Some(kernel);
    }

    // Reserved for upcoming Python adapter integration; not wired yet.
    #[allow(dead_code)]
    pub(crate) fn kernel(&self) -> Option<Arc<Kernel>> {
        self.kernel.clone()
    }
}

#[pyclass(name = "KernelResponse")]
#[derive(Clone, Debug)]
pub struct PyKernelResponse {
    status: u16,
    pub(super) headers: Vec<(String, String)>,
    body: Option<PyStateHandle>,
}

#[pymethods]
impl PyKernelResponse {
    #[new]
    pub fn new(
        status: u16,
        headers: Option<Vec<(String, String)>>,
        body: Option<PyStateHandle>,
    ) -> Self {
        Self {
            status,
            headers: headers.unwrap_or_default(),
            body,
        }
    }

    #[getter]
    pub fn status(&self) -> u16 {
        self.status
    }

    #[getter]
    pub fn headers(&self) -> Vec<(String, String)> {
        self.headers.clone()
    }

    #[getter]
    pub fn body(&self) -> Option<PyStateHandle> {
        self.body.clone()
    }
}

impl PyKernelResponse {
    pub(super) fn clone_inner(&self) -> Self {
        self.clone()
    }
}
