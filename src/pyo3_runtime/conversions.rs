use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use tc_ir::{Map, Scalar};
use tc_value::Value;

use crate::State;

pub(super) fn state_to_json_string(state: &State) -> PyResult<String> {
    let value = state_to_json_value(state)?;
    serde_json::to_string(&value).map_err(|err| PyValueError::new_err(err.to_string()))
}

fn state_to_json_value(state: &State) -> PyResult<serde_json::Value> {
    match state {
        State::None => Ok(serde_json::Value::Null),
        State::Scalar(scalar) => scalar_to_json_value(scalar),
        State::Map(map) => state_map_to_json_value(map),
        State::Tuple(items) => items
            .iter()
            .map(state_to_json_value)
            .collect::<PyResult<Vec<_>>>()
            .map(serde_json::Value::Array),
        State::Collection(_) => encode_to_json_value(state.clone()),
    }
}

fn scalar_to_json_value(scalar: &Scalar) -> PyResult<serde_json::Value> {
    match scalar {
        Scalar::Value(value) => value_to_json_value(value),
        Scalar::Map(map) => scalar_map_to_json_value(map),
        Scalar::Tuple(items) => items
            .iter()
            .map(scalar_to_json_value)
            .collect::<PyResult<Vec<_>>>()
            .map(serde_json::Value::Array),
        Scalar::Ref(_) | Scalar::Op(_) => encode_to_json_value(scalar.clone()),
    }
}

fn state_map_to_json_value(map: &Map<State>) -> PyResult<serde_json::Value> {
    let mut object = serde_json::Map::new();
    for (key, value) in map.iter() {
        object.insert(key.to_string(), state_to_json_value(value)?);
    }
    Ok(serde_json::Value::Object(object))
}

fn scalar_map_to_json_value(map: &Map<Scalar>) -> PyResult<serde_json::Value> {
    let mut object = serde_json::Map::new();
    for (key, value) in map.iter() {
        object.insert(key.to_string(), scalar_to_json_value(value)?);
    }
    Ok(serde_json::Value::Object(object))
}

fn value_to_json_value(value: &Value) -> PyResult<serde_json::Value> {
    encode_to_json_value(value.clone())
}

fn encode_to_json_value<T>(value: T) -> PyResult<serde_json::Value>
where
    T: for<'en> destream::IntoStream<'en>,
{
    let stream =
        destream_json::encode(value).map_err(|err| PyValueError::new_err(err.to_string()))?;
    let bytes = futures::executor::block_on(async move {
        use futures::TryStreamExt;
        stream
            .map_err(|err| err.to_string())
            .try_fold(Vec::new(), |mut acc, chunk| async move {
                acc.extend_from_slice(&chunk);
                Ok(acc)
            })
            .await
    })
    .map_err(PyValueError::new_err)?;

    serde_json::from_slice(&bytes).map_err(|err| PyValueError::new_err(err.to_string()))
}
