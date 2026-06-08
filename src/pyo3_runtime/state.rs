use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyType};
use pyo3::{Bound, PyClassInitializer, PyRef};

use tc_ir::{Map, Scalar};
use tc_state::{Collection, Tensor};
use tc_value::Value;

use crate::State;

use super::types::PyWrapper;
#[pyclass(name = "State", subclass)]
#[derive(Clone)]
pub struct PyState {
    inner: PyWrapper<State>,
}

#[pymethods]
impl PyState {
    #[new]
    pub fn new() -> PyClassInitializer<Self> {
        Self::initializer_from_state(State::None)
    }

    pub fn is_none(&self) -> bool {
        self.state().is_none()
    }

    pub fn to_json(&self) -> PyResult<String> {
        let value = state_to_json_value(self.state())?;
        serde_json::to_string(&value).map_err(|err| PyValueError::new_err(err.to_string()))
    }
}

impl PyState {
    fn from_state(state: State) -> Self {
        Self {
            inner: PyWrapper::new(state),
        }
    }

    pub(super) fn clone_state(&self) -> State {
        self.state().clone()
    }

    fn state(&self) -> &State {
        self.inner.inner()
    }

    pub(super) fn initializer_from_state(state: State) -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyState::from_state(state))
    }
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
macro_rules! define_state_subclass {
    ($name:ident, $py_name:literal, $base:ty) => {
        #[pyclass(name = $py_name, extends = $base, subclass)]
        pub struct $name;

        #[pymethods]
        impl $name {
            #[new]
            pub fn new() -> PyClassInitializer<Self> {
                <$base>::initializer_from_state(State::None).add_subclass($name)
            }
        }
    };
}

define_state_subclass!(PyScalar, "Scalar", PyState);
define_state_subclass!(PyCollection, "Collection", PyState);

#[pyclass(name = "Tensor", extends = PyCollection)]
pub struct PyTensor;

#[pymethods]
impl PyTensor {
    #[new]
    pub fn new() -> PyClassInitializer<Self> {
        PyState::initializer_from_state(State::None)
            .add_subclass(PyCollection)
            .add_subclass(PyTensor)
    }

    #[classmethod]
    pub fn dense_f32(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        shape: Vec<usize>,
        values: Vec<f32>,
    ) -> PyResult<Py<PyTensor>> {
        let tensor = Tensor::dense_f32(shape, values).map_err(PyValueError::new_err)?;
        new_py_tensor(py, tensor)
    }

    #[classmethod]
    pub fn dense_u64(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        shape: Vec<usize>,
        values: Vec<u64>,
    ) -> PyResult<Py<PyTensor>> {
        let tensor = Tensor::dense_u64(shape, values).map_err(PyValueError::new_err)?;
        new_py_tensor(py, tensor)
    }

    pub fn dtype<'py>(slf: PyRef<'py, Self>) -> PyResult<&'static str> {
        PyTensor::with_tensor(slf, |tensor| {
            Ok(match tensor {
                Tensor::F32(_) => "f32",
                Tensor::U64(_) => "u64",
            })
        })
    }

    pub fn shape<'py>(slf: PyRef<'py, Self>) -> PyResult<Vec<usize>> {
        PyTensor::with_tensor(slf, |tensor| Ok(tensor.shape().to_vec()))
    }

    pub fn values<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<PyObject> {
        PyTensor::with_tensor(slf, |tensor| match tensor {
            Tensor::F32(_) => {
                let values = tensor.flattened_f32().map_err(PyValueError::new_err)?;
                let list = PyList::new_bound(py, &values);
                Ok(list.into_py(py))
            }
            Tensor::U64(_) => {
                let values = tensor.flattened_u64().map_err(PyValueError::new_err)?;
                let list = PyList::new_bound(py, &values);
                Ok(list.into_py(py))
            }
        })
    }
}

fn new_py_tensor(py: Python<'_>, tensor: Tensor) -> PyResult<Py<PyTensor>> {
    Py::new(
        py,
        PyState::initializer_from_state(State::Collection(Collection::Tensor(tensor)))
            .add_subclass(PyCollection)
            .add_subclass(PyTensor),
    )
}

impl PyTensor {
    fn with_tensor<'py, R, F>(slf: PyRef<'py, Self>, f: F) -> PyResult<R>
    where
        F: FnOnce(&Tensor) -> PyResult<R>,
    {
        let collection_ref: PyRef<'py, PyCollection> = slf.into_super();
        let state_ref: PyRef<'py, PyState> = collection_ref.into_super();
        match state_ref.state() {
            State::Collection(Collection::Tensor(tensor)) => f(tensor),
            _ => Err(PyValueError::new_err(
                "tensor does not reference a collection state",
            )),
        }
    }
}

#[pyclass(name = "Value", extends = PyScalar)]
pub struct PyValue;

#[pymethods]
impl PyValue {
    #[new]
    pub fn new() -> PyClassInitializer<Self> {
        PyState::initializer_from_state(State::None)
            .add_subclass(PyScalar)
            .add_subclass(PyValue)
    }

    #[classmethod]
    pub fn none(_cls: &Bound<'_, PyType>, py: Python<'_>) -> PyResult<Py<PyValue>> {
        let initializer = PyState::initializer_from_state(State::from(Value::None))
            .add_subclass(PyScalar)
            .add_subclass(PyValue);
        Py::new(py, initializer)
    }
}
