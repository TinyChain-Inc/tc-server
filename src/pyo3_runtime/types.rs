use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use futures::TryStreamExt;
use pyo3::prelude::*;
use pyo3::types::PyString;
use tc_ir::{IntoView, LibrarySchema};

use super::wire::parse_method;
use crate::Method;
use crate::library::default_library_schema;

#[derive(Clone)]
pub struct PyKernelConfig {
    pub data_dir: Option<PathBuf>,
    pub workspace: Option<PathBuf>,
    pub initial_schema: LibrarySchema,
    pub host_id: String,
    pub limits: crate::HostLimits,
}

impl Default for PyKernelConfig {
    fn default() -> Self {
        Self {
            data_dir: None,
            workspace: None,
            initial_schema: default_library_schema(),
            host_id: "tc-py-host".to_string(),
            limits: crate::HostLimits::default(),
        }
    }
}

pub(super) fn apply_config_overrides(
    mut config: PyKernelConfig,
    request_ttl_secs: Option<u64>,
    max_request_bytes_unauth: Option<usize>,
) -> PyKernelConfig {
    if let Some(secs) = request_ttl_secs.filter(|value| *value > 0) {
        config.limits.transaction_ttl = std::time::Duration::from_secs(secs);
    }
    if let Some(max_bytes) = max_request_bytes_unauth.filter(|value| *value > 0) {
        config.limits.ingress.request_body_bytes = max_bytes;
    }
    config
}

#[pyclass(name = "StateHandle")]
#[derive(Clone)]
pub struct PyStateHandle {
    value: Option<Py<PyAny>>,
    state: Option<crate::State>,
    txn: Option<crate::TxnHandle>,
    runtime: Option<Arc<tokio::runtime::Runtime>>,
    finalize: Option<Arc<PendingFinalize>>,
}

struct PendingFinalize {
    runtime: Arc<tokio::runtime::Runtime>,
    pending: Mutex<Option<(Arc<crate::Kernel>, crate::TxnHandle)>>,
    deadline: crate::Deadline,
    _request: Arc<crate::resources::CapacityPermit>,
}

impl PendingFinalize {
    fn ensure_active(&self) -> PyResult<()> {
        if !self.deadline.is_expired() {
            return Ok(());
        }

        let err = self
            .pending
            .lock()
            .expect("finalize lock")
            .as_ref()
            .map(|(_, txn)| txn.deadline().exceeded());
        let Some(err) = err else {
            return Ok(());
        };
        self.pending.lock().expect("finalize lock").take();
        Err(super::tc_error(err))
    }

    fn finish(&self) -> PyResult<()> {
        let Some((kernel, txn)) = self.pending.lock().expect("finalize lock").take() else {
            return Ok(());
        };
        super::wait(&self.runtime, async move {
            kernel
                .complete_transaction(txn, crate::txn::TransactionOutcome::Succeeded)
                .await
        })
        .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))
    }
}

#[pymethods]
impl PyStateHandle {
    #[new]
    pub fn new(value: Py<PyAny>) -> Self {
        Self {
            value: Some(value),
            state: None,
            txn: None,
            runtime: None,
            finalize: None,
        }
    }

    pub fn clone_handle(&self) -> Self {
        self.clone()
    }

    pub fn value(&self) -> PyResult<Py<PyAny>> {
        if let Some(value) = &self.value {
            return Ok(value.clone());
        }

        if let Some(finalize) = &self.finalize {
            finalize.ensure_active()?;
        }

        let runtime = self.runtime.as_ref().map(Arc::clone).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("native state has no owning runtime")
        })?;
        let state = self
            .state
            .clone()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("state handle is empty"))?;
        let txn = self.txn.clone().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("state view has no transaction")
        })?;
        let bytes = super::wait(&runtime, async move {
            let view = state.into_view(txn).await.map_err(|err| err.to_string())?;
            let stream = destream_json::encode(view).map_err(|err| err.to_string())?;
            stream
                .try_fold(Vec::new(), |mut bytes, chunk| async move {
                    bytes.extend_from_slice(&chunk);
                    Ok(bytes)
                })
                .await
                .map_err(|err| err.to_string())
        })
        .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
        if let Some(finalize) = &self.finalize {
            finalize.finish()?;
        }
        Python::with_gil(|py| Ok(PyString::new_bound(py, &String::from_utf8_lossy(&bytes)).into()))
    }
}

impl PyStateHandle {
    pub(crate) fn from_state(
        state: crate::State,
        txn: crate::TxnHandle,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        Self {
            value: None,
            state: Some(state),
            txn: Some(txn),
            runtime: Some(runtime),
            finalize: None,
        }
    }

    pub(crate) fn from_terminal_state(
        state: crate::State,
        kernel: Arc<crate::Kernel>,
        txn: crate::TxnHandle,
        runtime: Arc<tokio::runtime::Runtime>,
        request: Arc<crate::resources::CapacityPermit>,
    ) -> Self {
        let deadline = txn.deadline();
        Self {
            value: None,
            state: Some(state),
            txn: Some(txn.clone()),
            runtime: Some(Arc::clone(&runtime)),
            finalize: Some(Arc::new(PendingFinalize {
                runtime,
                pending: Mutex::new(Some((kernel, txn))),
                deadline,
                _request: request,
            })),
        }
    }

    pub(crate) fn state(&self) -> Option<crate::State> {
        self.state.clone()
    }
}

#[pyclass(name = "KernelRequest")]
#[derive(Clone)]
pub struct PyKernelRequest {
    pub(super) method: Method,
    pub(super) path: String,
    pub(super) headers: Vec<(String, String)>,
    pub(super) body: Option<PyStateHandle>,
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
}

#[pyclass(name = "Response")]
#[derive(Clone)]
pub struct PyResponse {
    status: u16,
    pub(super) headers: Vec<(String, String)>,
    body: Option<PyStateHandle>,
}

#[pymethods]
impl PyResponse {
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
