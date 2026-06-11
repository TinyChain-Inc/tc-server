use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tc_ir::TxnId;

use crate::{Body, Method, Request, Response, StatusCode};

use super::state_handle_conversions::{
    encode_state_to_bytes, py_state_handle_from_state, request_body_state,
};
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

    let body = if let Some(native) = parts.extensions.get::<crate::http::NativeStateBody>() {
        if native.is_none() {
            None
        } else {
            Some(py_state_handle_from_state(native.clone_state())?)
        }
    } else {
        let body_bytes = hyper::body::to_bytes(body)
            .await
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        if body_bytes.is_empty() {
            None
        } else {
            return Err(PyValueError::new_err(
                "missing native request body extension for non-empty payload",
            ));
        }
    };

    Ok(PyKernelRequest {
        method,
        path,
        headers,
        body,
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

    match response
        .extensions()
        .get::<crate::http::NativeStateResponse>()
    {
        Some(native) if status < 400 => {
            let body = if native.is_none() {
                None
            } else {
                Some(py_state_handle_from_state(native.clone_state())?)
            };

            return Ok(PyKernelResponse::new(status, Some(headers), body));
        }
        _ => {}
    }

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
        Some(Python::with_gil(|py| {
            PyStateHandle::new(PyBytes::new_bound(py, &body_bytes).into_py(py))
        }))
    };

    Ok(PyKernelResponse::new(status, Some(headers), body))
}

pub(super) async fn py_response_to_http(response: PyKernelResponse) -> PyResult<Response> {
    let mut builder = http::Response::builder().status(response.status());
    for (name, value) in response.headers() {
        builder = builder.header(name.as_str(), value.as_str());
    }

    let native_state = request_body_state(response.body())?;
    let body_bytes = match native_state.as_ref() {
        None => Vec::new(),
        Some(state) if state.is_none() => Vec::new(),
        Some(state) => encode_state_to_bytes(state.clone())?,
    };
    if !body_bytes.is_empty() {
        builder = builder.header(crate::header::CONTENT_TYPE, "application/json");
    }

    let mut response = builder
        .body(Body::from(body_bytes))
        .map_err(|err| PyValueError::new_err(err.to_string()))?;

    if let Some(state) = native_state {
        response
            .extensions_mut()
            .insert(crate::http::NativeStateResponse::new(state));
    }

    Ok(response)
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
    crate::txn::wire::split_path_and_txn_id(path)
        .map_err(|_| PyValueError::new_err("invalid transaction id"))
}
