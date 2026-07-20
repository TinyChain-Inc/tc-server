use std::ops::Bound as StdBound;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use freqfs::Cache;
use pathlink::PathBuf as TcPathBuf;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyType};
use pyo3::{Bound, PyClassInitializer, PyRef};
use tc_collection::btree::{BTree, BTreeColumnSchema, PersistentFile};
use tc_ir::{NetworkTime, TxnId};
use tc_state::BTreeCollection;
use tc_value::class::NativeClass as _;
use tc_value::{Value, ValueType};

use tc_state::{Collection, Tensor};

use crate::State;

use super::conversions::state_to_json_string;
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
        state_to_json_string(self.state())
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

#[pyclass(name = "Tensor", extends = PyState)]
pub struct PyTensor;

#[pymethods]
impl PyTensor {
    #[new]
    pub fn new() -> PyClassInitializer<Self> {
        PyState::initializer_from_state(State::None).add_subclass(PyTensor)
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

    #[classmethod]
    pub fn dense_f64(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        shape: Vec<usize>,
        values: Vec<f64>,
    ) -> PyResult<Py<PyTensor>> {
        let tensor = Tensor::dense_f64(shape, values).map_err(PyValueError::new_err)?;
        new_py_tensor(py, tensor)
    }

    pub fn dtype<'py>(slf: PyRef<'py, Self>) -> PyResult<&'static str> {
        PyTensor::with_tensor(slf, |tensor| {
            Ok(match tensor {
                Tensor::F32(_) => "f32",
                Tensor::F64(_) => "f64",
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
            Tensor::F64(_) => {
                let values = tensor.flattened_f64().map_err(PyValueError::new_err)?;
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

#[pyclass(name = "BTree", extends = PyState)]
pub struct PyBTree;

#[pymethods]
impl PyBTree {
    #[new]
    #[pyo3(signature = (schema, rows=None))]
    pub fn new(
        schema: Vec<(String, String)>,
        rows: Option<Vec<Vec<String>>>,
    ) -> PyResult<PyClassInitializer<Self>> {
        let schema = schema
            .into_iter()
            .map(|(name, dtype)| {
                parse_btree_value_type(&dtype).map(|dtype| BTreeColumnSchema {
                    name,
                    dtype,
                    max_size: None,
                })
            })
            .collect::<PyResult<Vec<_>>>()?;

        if schema.is_empty() {
            return Err(PyValueError::new_err(
                "BTree schema must have at least one column",
            ));
        }

        let btree = create_native_btree(schema, rows.unwrap_or_default())?;
        Ok(
            PyState::initializer_from_state(State::Collection(Collection::BTree(btree)))
                .add_subclass(PyBTree),
        )
    }

    pub fn insert(slf: PyRef<'_, Self>, row: Vec<String>) -> PyResult<()> {
        with_btree(slf, |collection| {
            let txn_id = next_txn_id();
            run_local(async {
                collection
                    .btree
                    .insert_row(txn_id, row.into_iter().map(Value::from).collect())
                    .await
                    .map_err(|err| err.to_string())?;
                collection
                    .btree
                    .commit(txn_id)
                    .map_err(|err| err.to_string())?;
                collection
                    .btree
                    .finalize(txn_id)
                    .await
                    .map_err(|err| err.to_string())?;
                Ok(())
            })
        })
    }

    pub fn delete(slf: PyRef<'_, Self>, row: Vec<String>) -> PyResult<()> {
        with_btree(slf, |collection| {
            let txn_id = next_txn_id();
            run_local(async {
                collection
                    .btree
                    .delete_row(txn_id, row.into_iter().map(Value::from).collect())
                    .await
                    .map_err(|err| err.to_string())?;
                collection
                    .btree
                    .commit(txn_id)
                    .map_err(|err| err.to_string())?;
                collection
                    .btree
                    .finalize(txn_id)
                    .await
                    .map_err(|err| err.to_string())?;
                Ok(())
            })
        })
    }

    pub fn contains(slf: PyRef<'_, Self>, row: Vec<String>) -> PyResult<bool> {
        with_btree(slf, |collection| {
            let txn_id = next_txn_id();
            run_local(async {
                Ok(collection
                    .btree
                    .contains_row(
                        txn_id,
                        &row.into_iter().map(Value::from).collect::<Vec<Value>>(),
                    )
                    .await)
            })
        })
    }

    pub fn rows<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<PyObject> {
        with_btree(slf, |collection| {
            let txn_id = next_txn_id();
            let mut rows: Vec<Vec<Value>> = Vec::new();
            run_local(async {
                collection
                    .btree
                    .for_each_row_in_order(
                        txn_id,
                        (StdBound::<Value>::Unbounded, StdBound::<Value>::Unbounded),
                        false,
                        |row| rows.push(row),
                    )
                    .await;
                Ok(())
            })?;

            let py_rows = rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|value| match value {
                            Value::String(text) => Ok(text.into_py(py)),
                            Value::None => Ok(py.None()),
                            other => Err(PyValueError::new_err(format!(
                                "PyO3 BTree row decoding currently supports string/none values, got {other:?}",
                            ))),
                        })
                        .collect::<PyResult<Vec<PyObject>>>()
                })
                .collect::<PyResult<Vec<Vec<PyObject>>>>()?;

            Ok(PyList::new_bound(py, py_rows).into_py(py))
        })
    }
}

fn with_btree<R>(
    slf: PyRef<'_, PyBTree>,
    on_btree: impl FnOnce(&BTreeCollection) -> PyResult<R>,
) -> PyResult<R> {
    let state_ref: PyRef<'_, PyState> = slf.into_super();
    match state_ref.state() {
        State::Collection(Collection::BTree(btree)) => on_btree(btree),
        _ => Err(PyValueError::new_err(
            "native BTree handle does not reference a BTree collection state",
        )),
    }
}

fn run_local<F, T>(future: F) -> PyResult<T>
where
    F: std::future::Future<Output = Result<T, String>>,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
        .map_err(PyValueError::new_err)
}

fn next_txn_id() -> TxnId {
    static NEXT_TXN_NONCE: AtomicU64 = AtomicU64::new(1);
    let nanos = NEXT_TXN_NONCE.fetch_add(1, Ordering::Relaxed);
    TxnId::from_parts(NetworkTime::from_nanos(nanos), 0)
}

fn create_native_btree(
    schema: Vec<BTreeColumnSchema>,
    rows: Vec<Vec<String>>,
) -> PyResult<BTreeCollection> {
    static NEXT_BTREE_ROOT: AtomicU64 = AtomicU64::new(1);
    let nonce = NEXT_BTREE_ROOT.fetch_add(1, Ordering::Relaxed);
    let root =
        std::path::PathBuf::from(format!("/tmp/tc-pyo3-btree-{}-{nonce}", std::process::id()));

    std::fs::create_dir_all(root.join("persistent"))
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
    std::fs::create_dir_all(root.join("txn"))
        .map_err(|err| PyValueError::new_err(err.to_string()))?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let _enter = runtime.enter();

    let cache = Cache::<PersistentFile>::new(16 * 1024 * 1024, None);
    let persistent = Arc::clone(&cache)
        .load(root.join("persistent"))
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
    let txn = Arc::clone(&cache)
        .load(root.join("txn"))
        .map_err(|err| PyValueError::new_err(err.to_string()))?;

    let key_types = schema
        .iter()
        .map(|column| column.dtype.clone())
        .collect::<Vec<ValueType>>();

    let btree = BTree::with_key_types(persistent, txn, key_types);
    if !rows.is_empty() {
        let txn_id = next_txn_id();
        runtime
            .block_on(async {
                for row in rows {
                    btree
                        .insert_row(txn_id, row.into_iter().map(Value::from).collect())
                        .await
                        .map_err(|err| err.to_string())?;
                }

                btree.commit(txn_id).map_err(|err| err.to_string())?;
                btree
                    .finalize(txn_id)
                    .await
                    .map_err(|err| err.to_string())?;

                Ok(())
            })
            .map_err(|err: String| PyValueError::new_err(err))?;
    }

    Ok(BTreeCollection::with_schema(schema, btree))
}

fn parse_btree_value_type(dtype: &str) -> PyResult<ValueType> {
    let path = dtype
        .parse::<TcPathBuf>()
        .map_err(|_| PyValueError::new_err(format!("invalid BTree column dtype path {dtype}")))?;

    ValueType::from_path(path.as_ref()).ok_or_else(|| {
        PyValueError::new_err(format!(
            "unsupported BTree column dtype {dtype}; expected a /state/scalar/value/* type"
        ))
    })
}

fn new_py_tensor(py: Python<'_>, tensor: Tensor) -> PyResult<Py<PyTensor>> {
    Py::new(
        py,
        PyState::initializer_from_state(State::Collection(Collection::Tensor(tensor)))
            .add_subclass(PyTensor),
    )
}

impl PyTensor {
    fn with_tensor<'py, R, F>(slf: PyRef<'py, Self>, f: F) -> PyResult<R>
    where
        F: FnOnce(&Tensor) -> PyResult<R>,
    {
        let state_ref: PyRef<'py, PyState> = slf.into_super();
        match state_ref.state() {
            State::Collection(Collection::Tensor(tensor)) => f(tensor),
            _ => Err(PyValueError::new_err(
                "tensor does not reference a collection state",
            )),
        }
    }
}
