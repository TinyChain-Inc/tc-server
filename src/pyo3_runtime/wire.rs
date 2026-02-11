use std::io;

use bytes::Bytes;
use futures::{TryStreamExt, executor, stream};
use pyo3::Bound;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use tc_ir::{LibrarySchema, Scalar, TxnId};
use tc_value::Value;
use url::form_urlencoded;

use crate::{Body, Method, Request, Response, State, StatusCode};
use tc_state::null_transaction;

use super::state::PyState;
use super::types::{PyKernelRequest, PyKernelResponse, PyStateHandle};

pub(super) fn parse_method(method: &str) -> PyResult<Method> {
    match method.to_ascii_uppercase().as_str() {
        "GET" => Ok(Method::Get),
        "PUT" => Ok(Method::Put),
        "POST" => Ok(Method::Post),
        "DELETE" => Ok(Method::Delete),
        other => Err(PyValueError::new_err(format!(
            "unsupported method: {other}"
        ))),
    }
}

pub(super) fn http_request_from_py(request: &PyKernelRequest) -> PyResult<Request> {
    let method = match request.method_enum() {
        Method::Get => crate::HttpMethod::GET,
        Method::Put => crate::HttpMethod::PUT,
        Method::Post => crate::HttpMethod::POST,
        Method::Delete => crate::HttpMethod::DELETE,
    };

    let mut builder = http::Request::builder().method(method).uri(request.path());
    for (name, value) in request.headers() {
        builder = builder.header(name.as_str(), value.as_str());
    }

    let body_bytes = request_body_bytes(request.body())?;
    http_request_from_py_with_body(request, body_bytes)
}

pub(super) fn http_request_from_py_with_body(
    request: &PyKernelRequest,
    body_bytes: Vec<u8>,
) -> PyResult<Request> {
    let method = match request.method_enum() {
        Method::Get => crate::HttpMethod::GET,
        Method::Put => crate::HttpMethod::PUT,
        Method::Post => crate::HttpMethod::POST,
        Method::Delete => crate::HttpMethod::DELETE,
    };

    let mut builder = http::Request::builder().method(method).uri(request.path());
    for (name, value) in request.headers() {
        builder = builder.header(name.as_str(), value.as_str());
    }

    builder
        .body(Body::from(body_bytes))
        .map_err(|err| PyValueError::new_err(err.to_string()))
}

pub(super) async fn py_request_from_http(req: Request) -> PyResult<PyKernelRequest> {
    let (parts, body) = req.into_parts();
    let method = parse_method(parts.method.as_str())?;
    let path = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| parts.uri.path().to_string());

    let headers = parts
        .headers
        .iter()
        .filter_map(|(name, value)| {
            Some((name.as_str().to_string(), value.to_str().ok()?.to_string()))
        })
        .collect::<Vec<_>>();

    let body_bytes = hyper::body::to_bytes(body)
        .await
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
    let body = if body_bytes.is_empty() {
        None
    } else {
        let state = decode_state_from_bytes(body_bytes.to_vec())?;
        Some(py_state_handle_from_state(state)?)
    };

    Ok(PyKernelRequest {
        method,
        path,
        headers,
        body,
        kernel: None,
    })
}

pub(super) async fn py_response_from_http(response: Response) -> PyResult<PyKernelResponse> {
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            Some((name.as_str().to_string(), value.to_str().ok()?.to_string()))
        })
        .collect::<Vec<_>>();

    let body_bytes = hyper::body::to_bytes(response.into_body())
        .await
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
    let body = if body_bytes.is_empty() {
        None
    } else if status >= 400 {
        Some(Python::with_gil(|py| {
            PyStateHandle::new(PyBytes::new_bound(py, &body_bytes).into_py(py))
        }))
    } else {
        let state = decode_state_from_bytes(body_bytes.to_vec())?;
        Some(py_state_handle_from_state(state)?)
    };

    Ok(PyKernelResponse::new(status, Some(headers), body))
}

pub(super) async fn py_response_to_http(response: PyKernelResponse) -> PyResult<Response> {
    let mut builder = http::Response::builder().status(response.status());
    for (name, value) in response.headers() {
        builder = builder.header(name.as_str(), value.as_str());
    }

    let body_bytes = request_body_bytes(response.body())?;
    if !body_bytes.is_empty() {
        builder = builder.header(crate::header::CONTENT_TYPE, "application/json");
    }

    builder
        .body(Body::from(body_bytes))
        .map_err(|err| PyValueError::new_err(err.to_string()))
}

fn py_state_handle_from_state(state: State) -> PyResult<PyStateHandle> {
    Python::with_gil(|py| {
        let initializer = PyState::initializer_from_state(state);
        let py_state = Py::new(py, initializer)?;
        Ok(PyStateHandle::new(py_state.into_py(py)))
    })
}

async fn decode_state_from_bytes_async(bytes: Vec<u8>) -> Result<State, String> {
    let stream = stream::iter(vec![Ok::<Bytes, io::Error>(Bytes::from(bytes))]);
    destream_json::try_decode(null_transaction(), stream)
        .await
        .map_err(|err| err.to_string())
}

fn decode_state_from_bytes(bytes: Vec<u8>) -> PyResult<State> {
    executor::block_on(decode_state_from_bytes_async(bytes)).map_err(PyValueError::new_err)
}

pub(super) fn py_error_response(err: PyErr) -> Response {
    http::Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from(err.to_string()))
        .expect("python error response")
}

pub(crate) fn py_bearer_token(request: &PyKernelRequest) -> Option<String> {
    request.headers.iter().find_map(|(key, value)| {
        if !key.eq_ignore_ascii_case("authorization") {
            return None;
        }

        let (scheme, token) = value.split_once(' ')?;
        if !scheme.eq_ignore_ascii_case("bearer") {
            return None;
        }

        let token = token.trim();
        if token.is_empty() {
            None
        } else {
            Some(token.to_string())
        }
    })
}

pub(super) fn parse_path_and_txn_id(path: &str) -> PyResult<(String, Option<TxnId>)> {
    use std::str::FromStr;

    if let Some((base, query)) = path.split_once('?') {
        let txn = form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .find(|(k, _)| k.eq_ignore_ascii_case("txn_id"))
            .map(|(_, v)| v);

        let parsed = match txn {
            Some(value) => Some(
                TxnId::from_str(&value)
                    .map_err(|_| PyValueError::new_err("invalid transaction id"))?,
            ),
            None => None,
        };

        Ok((base.to_string(), parsed))
    } else {
        Ok((path.to_string(), None))
    }
}

async fn decode_schema_from_bytes_async(bytes: Vec<u8>) -> Result<LibrarySchema, String> {
    let stream = stream::iter(vec![Ok::<Bytes, io::Error>(Bytes::from(bytes))]);
    destream_json::try_decode((), stream)
        .await
        .map_err(|err| err.to_string())
}

pub(super) fn decode_schema_from_json(json: &str) -> PyResult<LibrarySchema> {
    executor::block_on(decode_schema_from_bytes_async(json.as_bytes().to_vec()))
        .map_err(PyValueError::new_err)
}

async fn decode_scalar_from_bytes_async(bytes: Vec<u8>) -> Result<Scalar, String> {
    let stream = stream::iter(vec![Ok::<Bytes, io::Error>(Bytes::from(bytes))]);
    destream_json::try_decode((), stream)
        .await
        .map_err(|err| err.to_string())
}

pub(super) fn decode_scalar_from_bytes(bytes: Vec<u8>) -> PyResult<Scalar> {
    executor::block_on(decode_scalar_from_bytes_async(bytes)).map_err(PyValueError::new_err)
}

pub(crate) fn request_body_bytes(body: Option<PyStateHandle>) -> PyResult<Vec<u8>> {
    let handle = match body {
        Some(handle) => handle,
        None => return Ok(Vec::new()),
    };
    Python::with_gil(|py| {
        let value = handle.value();
        let any = value.bind(py);
        if let Some(state) = try_extract_state(any)? {
            if state.is_none() {
                return Ok(Vec::new());
            }

            return encode_state_to_bytes(state);
        }
        if let Ok(bytes) = any.downcast::<PyBytes>() {
            Ok(bytes.as_bytes().to_vec())
        } else if let Ok(string) = any.downcast::<PyString>() {
            let text = string.to_string();
            let trimmed = text.trim();
            if trimmed.is_empty() {
                Ok(Vec::new())
            } else if trimmed.starts_with('{') || trimmed.starts_with('[') {
                Ok(text.into_bytes())
            } else {
                encode_state_to_bytes(State::from(Value::from(text)))
            }
        } else {
            Err(PyValueError::new_err(
                "request body must be a str or bytes-like object",
            ))
        }
    })
}

fn try_extract_state(any: &Bound<'_, PyAny>) -> PyResult<Option<State>> {
    if any.is_instance_of::<PyState>() {
        let state_ref: PyRef<'_, PyState> = any.extract()?;
        Ok(Some(state_ref.clone_state()))
    } else {
        Ok(None)
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
    executor::block_on(encode_state_via_destream_async(state)).map_err(PyValueError::new_err)
}

async fn encode_state_via_destream_async(state: State) -> Result<Vec<u8>, String> {
    let stream = destream_json::encode(state).map_err(|err| err.to_string())?;
    stream
        .map_err(|err| err.to_string())
        .try_fold(Vec::new(), |mut acc, chunk| async move {
            acc.extend_from_slice(&chunk);
            Ok(acc)
        })
        .await
}

pub(super) fn py_body_is_none(body: Option<PyStateHandle>) -> bool {
    match body {
        None => true,
        Some(handle) => Python::with_gil(|py| {
            let value = handle.value();
            let any = value.bind(py);
            match try_extract_state(any) {
                Ok(Some(state)) => state.is_none(),
                _ => false,
            }
        }),
    }
}
