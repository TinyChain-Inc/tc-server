use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use tc_ir::TxnId;

use crate::Method;

use super::types::PyKernelRequest;

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
