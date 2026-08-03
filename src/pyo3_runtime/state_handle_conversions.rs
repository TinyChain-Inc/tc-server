use pyo3::Bound;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

use crate::State;

use super::state::PyState;
use super::state_json::encode_state_json_bytes;
use super::types::PyStateHandle;

pub(crate) fn py_state_handle_from_bytes(bytes: Vec<u8>) -> PyResult<PyStateHandle> {
    Python::with_gil(|py| {
        Ok(PyStateHandle::new(PyBytes::new_bound(py, &bytes).into_py(py)))
    })
}

pub(crate) fn py_state_handle_from_state(state: State) -> PyResult<PyStateHandle> {
    Python::with_gil(|py| {
        let state = Py::new(py, PyState::initializer_from_state(state))?;
        Ok(PyStateHandle::new(state.into_py(py)))
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

pub(crate) fn request_body_bytes(body: Option<PyStateHandle>) -> PyResult<Vec<u8>> {
    let handle = match body {
        Some(handle) => handle,
        None => return Ok(Vec::new()),
    };

    Python::with_gil(|py| {
        let value = handle.value();
        let any = value.bind(py);
        if any.is_instance_of::<PyState>() {
            return extract_state(any).and_then(encode_state_to_bytes);
        }
        if let Ok(bytes) = any.downcast::<PyBytes>() {
            return Ok(bytes.as_bytes().to_vec());
        }
        if let Ok(string) = any.downcast::<PyString>() {
            return Ok(string.to_str()?.as_bytes().to_vec());
        }
        Err(PyValueError::new_err(
            "expected tinychain.State, bytes, or string body",
        ))
    })
}

pub(crate) async fn request_body_bytes_async(body: Option<PyStateHandle>) -> PyResult<Vec<u8>> {
    let state = request_body_state(body.clone())?;
    if let Some(state) = state {
        return encode_state_to_bytes_async(state).await;
    }

    request_body_bytes(body)
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
        encode_state_via_tc_state(state)
    }
}

pub(crate) async fn encode_state_to_bytes_async(state: State) -> PyResult<Vec<u8>> {
    if state.is_none() {
        Ok(Vec::new())
    } else {
        encode_state_json_bytes(state)
            .await
            .map_err(PyValueError::new_err)
    }
}

fn encode_state_via_tc_state(state: State) -> PyResult<Vec<u8>> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            encode_state_json_bytes(state)
                .await
                .map_err(PyValueError::new_err)
        })
}
