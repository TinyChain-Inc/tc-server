use futures::stream;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

use super::types::PyStateHandle;

pub(crate) fn request_body_bytes(body: Option<PyStateHandle>) -> PyResult<Vec<u8>> {
    let handle = match body {
        Some(handle) => handle,
        None => return Ok(Vec::new()),
    };

    Python::with_gil(|py| {
        let value = handle.value()?;
        let any = value.bind(py);
        if let Ok(bytes) = any.downcast::<PyBytes>() {
            return Ok(bytes.as_bytes().to_vec());
        }
        if let Ok(string) = any.downcast::<PyString>() {
            return Ok(string.to_str()?.as_bytes().to_vec());
        }
        Err(PyValueError::new_err("expected bytes or string body"))
    })
}

pub(crate) fn state_from_handle(
    body: Option<PyStateHandle>,
    txn: crate::TxnHandle,
    runtime: &tokio::runtime::Runtime,
) -> PyResult<Option<crate::State>> {
    let Some(handle) = body else {
        return Ok(None);
    };
    if let Some(state) = handle.state() {
        return Ok(Some(state));
    }

    let bytes = request_body_bytes(Some(handle))?;
    let stream = stream::iter(vec![Ok::<_, std::io::Error>(bytes.into())]);
    super::wait(runtime, async move {
        destream_json::try_decode(txn, stream).await
    })
    .map(Some)
    .map_err(|err| PyValueError::new_err(err.to_string()))
}
