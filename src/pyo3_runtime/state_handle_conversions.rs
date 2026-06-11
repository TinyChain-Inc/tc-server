use pyo3::Bound;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

use crate::State;

use super::state::PyState;
use super::types::PyStateHandle;

pub(crate) fn py_state_handle_from_state(state: State) -> PyResult<PyStateHandle> {
    Python::with_gil(|py| {
        let initializer = PyState::initializer_from_state(state);
        let py_state = Py::new(py, initializer)?;
        Ok(PyStateHandle::new(py_state.into_py(py)))
    })
}

pub(crate) fn request_body_state(body: Option<PyStateHandle>) -> PyResult<Option<State>> {
    let handle = match body {
        Some(handle) => handle,
        None => return Ok(None),
    };

    Python::with_gil(|py| {
        let value = handle.value();
        let any = value.bind(py);
        if any.is_instance_of::<PyState>() {
            extract_state(any).map(Some)
        } else {
            Ok(None)
        }
    })
}

pub(crate) fn request_body_raw_bytes(body: Option<PyStateHandle>) -> PyResult<Option<Vec<u8>>> {
    let handle = match body {
        Some(handle) => handle,
        None => return Ok(None),
    };

    Python::with_gil(|py| {
        let value = handle.value();
        let any = value.bind(py);
        if any.is_instance_of::<PyState>() {
            return Ok(None);
        }
        if let Ok(bytes) = any.downcast::<PyBytes>() {
            return Ok(Some(bytes.as_bytes().to_vec()));
        }
        if let Ok(string) = any.downcast::<PyString>() {
            return Ok(Some(string.to_str()?.as_bytes().to_vec()));
        }
        Err(PyValueError::new_err(
            "expected tinychain.State, bytes, or string body",
        ))
    })
}

fn extract_state(any: &Bound<'_, PyAny>) -> PyResult<State> {
    if any.is_instance_of::<PyState>() {
        let state_ref: PyRef<'_, PyState> = any.extract()?;
        Ok(state_ref.clone_state())
    } else {
        Err(PyValueError::new_err("expected tinychain.State body"))
    }
}

pub(crate) fn encode_state_to_bytes(state: State) -> PyResult<Vec<u8>> {
    if state.is_none() {
        Ok(Vec::new())
    } else {
        encode_state_via_destream(state)
    }
}

fn encode_state_via_destream(state: State) -> PyResult<Vec<u8>> {
    let stream =
        destream_json::encode(state).map_err(|err| PyValueError::new_err(err.to_string()))?;
    futures::executor::block_on(async move {
        use futures::TryStreamExt;

        stream
            .map_err(|err| PyValueError::new_err(err.to_string()))
            .try_fold(Vec::new(), |mut acc, chunk| async move {
                acc.extend_from_slice(&chunk);
                Ok(acc)
            })
            .await
    })
}
