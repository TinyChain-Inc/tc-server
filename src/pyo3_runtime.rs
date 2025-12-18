#![allow(unsafe_op_in_unsafe_fn)]

use std::{fmt, io, marker::PhantomData, path::PathBuf, sync::Arc};

use bytes::Bytes;
use futures::{FutureExt, TryStreamExt, executor, stream};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyModule, PyString, PyType};
use pyo3::{Bound, PyClassInitializer, PyRef};
use tc_ir::{LibrarySchema, Scalar, TxnId};
use url::form_urlencoded;

use crate::{
    Kernel, KernelDispatch, KernelHandler, Method, State,
    library::{
        InstallError, InstallRequest, LibraryHandlers, LibraryRouteFactory, LibraryRoutes,
        LibraryRuntime, LibraryState, apply_wasm_install, decode_install_request_bytes,
        default_library_schema,
    },
    storage::LibraryDir,
    txn::{TxnError, TxnHandle, TxnManager},
};
use tc_state::{Collection, StateContext, Tensor};
use tc_value::Value;

pub type PyKernel = Kernel<PyKernelRequest, PyResult<PyKernelResponse>>;

#[derive(Clone)]
pub struct PyKernelConfig {
    pub data_dir: Option<PathBuf>,
    pub initial_schema: LibrarySchema,
    pub host_id: String,
}

impl Default for PyKernelConfig {
    fn default() -> Self {
        Self {
            data_dir: None,
            initial_schema: default_library_schema(),
            host_id: "tc-py-host".to_string(),
        }
    }
}

/// Wrapper around a typed state value shared between handlers.
///
/// This intentionally mirrors the generics on `Handler` so the same concrete state
/// types can flow between verb implementations and higher-level bindings without
/// serializing/deserializing when a call chain stays in-process (e.g. PyO3 eager
/// execution). The handle simply holds an `Arc<T>` and exposes lightweight helpers
/// to borrow or clone that pointer; downcasting happens through the same visitor
/// logic used for handlers in general rather than through a per-handle vtable.
pub struct StateHandle<State> {
    inner: Arc<State>,
    _marker: PhantomData<State>,
}

impl<State> StateHandle<State> {
    /// Create a new handle from an owned value.
    pub fn new(state: State) -> Self {
        Self {
            inner: Arc::new(state),
            _marker: PhantomData,
        }
    }

    /// Wrap an existing `Arc` without copying.
    pub fn from_arc(state: Arc<State>) -> Self {
        Self {
            inner: state,
            _marker: PhantomData,
        }
    }

    /// Clone the inner `Arc` for use in another call-chain.
    pub fn to_arc(&self) -> Arc<State> {
        Arc::clone(&self.inner)
    }

    /// Consume the handle and return the inner `Arc`.
    pub fn into_inner(self) -> Arc<State> {
        self.inner
    }
}

impl<State> Clone for StateHandle<State> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            _marker: PhantomData,
        }
    }
}

impl<State> AsRef<State> for StateHandle<State> {
    fn as_ref(&self) -> &State {
        &self.inner
    }
}

impl<State: fmt::Debug> fmt::Debug for StateHandle<State> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateHandle").finish()
    }
}

#[derive(Clone)]
struct PyWrapper<T> {
    inner: T,
}

impl<T> PyWrapper<T> {
    fn new(inner: T) -> Self {
        Self { inner }
    }

    fn inner(&self) -> &T {
        &self.inner
    }
}

#[pyclass(name = "StateHandle")]
#[derive(Clone, Debug)]
pub struct PyStateHandle {
    handle: StateHandle<Py<PyAny>>,
}

#[pymethods]
impl PyStateHandle {
    #[new]
    pub fn new(value: Py<PyAny>) -> Self {
        Self {
            handle: StateHandle::new(value),
        }
    }

    pub fn clone_handle(&self) -> Self {
        self.clone()
    }

    pub fn value(&self) -> Py<PyAny> {
        self.handle.as_ref().clone()
    }
}

impl PyStateHandle {
    pub fn inner(&self) -> &StateHandle<Py<PyAny>> {
        &self.handle
    }
}

impl From<StateHandle<Py<PyAny>>> for PyStateHandle {
    fn from(handle: StateHandle<Py<PyAny>>) -> Self {
        Self { handle }
    }
}

impl From<PyStateHandle> for StateHandle<Py<PyAny>> {
    fn from(handle: PyStateHandle) -> Self {
        handle.handle
    }
}

#[pyclass(name = "KernelRequest")]
#[derive(Clone, Debug)]
pub struct PyKernelRequest {
    method: Method,
    path: String,
    headers: Vec<(String, String)>,
    body: Option<PyStateHandle>,
    txn: Option<crate::txn::TxnHandle>,
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
            txn: None,
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

    pub(crate) fn bind_txn_handle(&mut self, handle: crate::txn::TxnHandle) {
        self.txn = Some(handle);
    }

    pub(crate) fn txn_handle(&self) -> Option<crate::txn::TxnHandle> {
        self.txn.clone()
    }
}

#[pyclass(name = "KernelResponse")]
#[derive(Clone, Debug)]
pub struct PyKernelResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Option<PyStateHandle>,
}

#[pymethods]
impl PyKernelResponse {
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

impl PyKernelResponse {
    fn clone_inner(&self) -> Self {
        self.clone()
    }
}

pub(crate) fn python_kernel_builder_with_config(
    lib: Py<PyAny>,
    service: Py<PyAny>,
    metrics: Option<Py<PyAny>>,
    config: PyKernelConfig,
) -> PyKernel {
    let _ = lib; // /lib is managed by the Rust kernel.
    let storage = config
        .data_dir
        .as_ref()
        .map(|dir| LibraryDir::new(dir.clone()));
    let module = build_pyo3_library_module(config.initial_schema.clone(), storage);
    let library_handlers = pyo3_library_handlers(&module);

    Kernel::builder()
        .with_host_id(config.host_id.clone())
        .with_library_module(module, library_handlers)
        .with_service_handler(python_handler(service))
        .with_kernel_handler(python_handler(metrics.unwrap_or_else(stub_py_handler)))
        .with_health_handler(python_health_handler())
        .finish()
}

fn build_pyo3_library_module(
    initial_schema: LibrarySchema,
    storage: Option<LibraryDir>,
) -> Arc<LibraryRuntime<PyKernelRequest, PyResult<PyKernelResponse>>> {
    let factory: LibraryRouteFactory<_, _> = Arc::new(|bytes: Vec<u8>| {
        let (handler, schema, schema_routes) =
            crate::wasm::pyo3_wasm_route_handler_from_bytes(bytes)?;
        let handler: Arc<dyn KernelHandler<PyKernelRequest, PyResult<PyKernelResponse>>> =
            Arc::new(handler);
        Ok((schema, schema_routes, handler))
    });

    Arc::new(LibraryRuntime::new(initial_schema, storage, Some(factory)))
}

fn pyo3_library_handlers(
    module: &Arc<LibraryRuntime<PyKernelRequest, PyResult<PyKernelResponse>>>,
) -> LibraryHandlers<PyKernelRequest, PyResult<PyKernelResponse>> {
    let state = module.state();
    let routes = module.routes();
    let storage = module.storage();
    let factory = module.route_factory();
    let get = py_library_get_handler(state.clone());
    let put = py_library_put_handler(state, routes.clone(), storage, factory.clone());

    if factory.is_some() {
        let route = py_routes_handler(&routes);
        LibraryHandlers::with_route(get, put, route)
    } else {
        LibraryHandlers::without_route(get, put)
    }
}

pub(crate) fn python_handler(
    callback: Py<PyAny>,
) -> impl KernelHandler<PyKernelRequest, PyResult<PyKernelResponse>> {
    move |req: PyKernelRequest| {
        let callback = callback.clone();
        async move {
            Python::with_gil(|py| -> PyResult<PyKernelResponse> {
                let callable = callback.bind(py);
                let arg = Py::new(py, req.clone())?;
                let raw = callable.call1((arg,))?;
                let response: Py<PyKernelResponse> = raw.extract()?;
                Ok(response.borrow(py).clone_inner())
            })
        }
        .boxed()
    }
}

pub(crate) fn python_health_handler()
-> impl KernelHandler<PyKernelRequest, PyResult<PyKernelResponse>> {
    move |_req: PyKernelRequest| async move { Ok(PyKernelResponse::new(200, None, None)) }.boxed()
}

pub(crate) fn stub_py_handler() -> Py<PyAny> {
    Python::with_gil(|py| {
        let module = PyModule::from_code_bound(
            py,
            r#"def _stub(_req):
    raise RuntimeError("kernel handler not installed")
"#,
            "<kernel>",
            "kernel_stub",
        )
        .expect("stub module");
        module.getattr("_stub").expect("stub attr").into_py(py)
    })
}

#[pyclass(name = "KernelHandle")]
pub struct KernelHandle {
    inner: Arc<PyKernel>,
    txn_manager: TxnManager,
}

impl Clone for KernelHandle {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            txn_manager: self.txn_manager.clone(),
        }
    }
}

impl KernelHandle {
    fn from_kernel(kernel: PyKernel) -> Self {
        let txn_manager = kernel.txn_manager().clone();
        Self {
            inner: Arc::new(kernel),
            txn_manager,
        }
    }
}

#[pymethods]
impl KernelHandle {
    #[new]
    #[pyo3(signature = (lib, service, metrics=None, data_dir=None))]
    pub fn new(
        lib: Py<PyAny>,
        service: Py<PyAny>,
        metrics: Option<Py<PyAny>>,
        data_dir: Option<PathBuf>,
    ) -> Self {
        let config = PyKernelConfig {
            data_dir,
            ..PyKernelConfig::default()
        };
        Self::from_kernel(python_kernel_builder_with_config(
            lib, service, metrics, config,
        ))
    }

    #[classmethod]
    pub fn with_library_schema(_cls: &Bound<'_, PyType>, schema_json: &str) -> PyResult<Self> {
        let schema = decode_schema_from_json(schema_json)?;
        let module = build_pyo3_library_module(schema, None);
        let handlers = pyo3_library_handlers(&module);
        let kernel = Kernel::builder()
            .with_host_id("tc-py-kernel")
            .with_library_module(module, handlers)
            .finish();
        Ok(Self::from_kernel(kernel))
    }

    pub fn dispatch(&self, request: PyKernelRequest) -> PyResult<PyKernelResponse> {
        let method = request.method_enum();
        let raw_path = request.path_owned();
        let (route_path, txn_id) = parse_path_and_txn_id(&raw_path)?;
        let body_is_none = py_body_is_none(request.body());

        match self.inner.route_request(
            method,
            &route_path,
            request,
            txn_id,
            body_is_none,
            |handle, req| {
                req.bind_txn_handle(handle.clone());
            },
        ) {
            Ok(KernelDispatch::Response(resp)) => executor::block_on(resp),
            Ok(KernelDispatch::Finalize { commit: _, result }) => {
                let status = if result.is_ok() { 204 } else { 400 };
                Ok(PyKernelResponse::new(status, None, None))
            }
            Ok(KernelDispatch::NotFound) => Err(PyValueError::new_err(format!(
                "no handler for method {method} path {route_path}"
            ))),
            Err(TxnError::NotFound) => Err(PyValueError::new_err("unknown transaction id")),
        }
    }
}

#[pyclass(name = "Backend")]
pub struct PyBackend {
    kernel: KernelHandle,
}

#[pymethods]
impl PyBackend {
    #[new]
    #[pyo3(signature = (lib, service, metrics=None, data_dir=None))]
    pub fn new(
        lib: Py<PyAny>,
        service: Py<PyAny>,
        metrics: Option<Py<PyAny>>,
        data_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            kernel: KernelHandle::new(lib, service, metrics, data_dir),
        }
    }

    pub fn healthz(&self) -> PyResult<()> {
        let request = PyKernelRequest::new("GET", "/healthz", None, None)?;
        let response = self.kernel.dispatch(request)?;
        if response.status() == 200 {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "healthz handler returned status {}",
                response.status()
            )))
        }
    }
}

pub fn register_python_api(module: &PyModule) -> PyResult<()> {
    module.add_class::<KernelHandle>()?;
    module.add_class::<PyStateHandle>()?;
    module.add_class::<PyState>()?;
    module.add_class::<PyScalar>()?;
    module.add_class::<PyCollection>()?;
    module.add_class::<PyTensor>()?;
    module.add_class::<PyValue>()?;
    module.add_class::<PyKernelRequest>()?;
    module.add_class::<PyKernelResponse>()?;
    module.add_class::<PyBackend>()
}

fn parse_method(method: &str) -> PyResult<Method> {
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

fn parse_path_and_txn_id(path: &str) -> PyResult<(String, Option<TxnId>)> {
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

fn py_library_get_handler(
    state: LibraryState,
) -> impl KernelHandler<PyKernelRequest, PyResult<PyKernelResponse>> {
    move |_req: PyKernelRequest| {
        let state = state.clone();
        async move {
            let schema = state.schema();
            let json = schema_to_json_string_async(schema)
                .await
                .map_err(PyValueError::new_err)?;
            Python::with_gil(|py| {
                let body = PyStateHandle::new(PyString::new_bound(py, &json).into_py(py));
                Ok(PyKernelResponse::new(200, None, Some(body)))
            })
        }
        .boxed()
    }
}

fn py_routes_handler(
    routes: &LibraryRoutes<PyKernelRequest, PyResult<PyKernelResponse>>,
) -> impl KernelHandler<PyKernelRequest, PyResult<PyKernelResponse>> {
    let routes = routes.clone();
    move |req: PyKernelRequest| {
        let handler = routes.current_handler();
        async move {
            if let Some(handler) = handler {
                handler.call(req).await
            } else {
                Ok(PyKernelResponse::new(404, None, None))
            }
        }
        .boxed()
    }
}

fn install_error_to_py(err: InstallError) -> PyErr {
    PyValueError::new_err(err.message().to_string())
}

fn py_library_put_handler(
    state: LibraryState,
    routes: LibraryRoutes<PyKernelRequest, PyResult<PyKernelResponse>>,
    storage: Option<LibraryDir>,
    route_factory: Option<LibraryRouteFactory<PyKernelRequest, PyResult<PyKernelResponse>>>,
) -> impl KernelHandler<PyKernelRequest, PyResult<PyKernelResponse>> {
    move |req: PyKernelRequest| {
        let state = state.clone();
        let routes = routes.clone();
        let storage = storage.clone();
        let route_factory = route_factory.clone();
        async move {
            let bytes = request_body_bytes(req.body())?;
            match decode_install_request_bytes(&bytes).map_err(install_error_to_py)? {
                InstallRequest::SchemaOnly(schema) => {
                    state.replace_schema(schema);
                    routes.clear();
                    if let Some(storage) = &storage {
                        storage
                            .persist_schema(&state.schema())
                            .map_err(|err| PyValueError::new_err(err.to_string()))?;
                    }
                    Ok(PyKernelResponse::new(204, None, None))
                }
                InstallRequest::WithArtifacts(payload) => {
                    apply_wasm_install(
                        &state,
                        &routes,
                        storage.as_ref(),
                        route_factory.as_ref(),
                        payload,
                    )
                    .map_err(install_error_to_py)?;
                    Ok(PyKernelResponse::new(204, None, None))
                }
            }
        }
        .boxed()
    }
}

async fn schema_to_json_string_async(schema: LibrarySchema) -> Result<String, String> {
    let stream = destream_json::encode(schema).map_err(|err| err.to_string())?;
    let bytes = stream
        .map_err(|err| err.to_string())
        .try_fold(Vec::new(), |mut acc, chunk| async move {
            acc.extend_from_slice(&chunk);
            Ok(acc)
        })
        .await?;
    String::from_utf8(bytes).map_err(|err| err.to_string())
}

async fn decode_schema_from_bytes_async(bytes: Vec<u8>) -> Result<LibrarySchema, String> {
    let stream = stream::iter(vec![Ok::<Bytes, io::Error>(Bytes::from(bytes))]);
    destream_json::try_decode((), stream)
        .await
        .map_err(|err| err.to_string())
}

fn decode_schema_from_json(json: &str) -> PyResult<LibrarySchema> {
    executor::block_on(decode_schema_from_bytes_async(json.as_bytes().to_vec()))
        .map_err(PyValueError::new_err)
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
            match state {
                State::None | State::Scalar(Scalar::Value(Value::None)) => return Ok(Vec::new()),
                _ => {
                    return encode_state_to_bytes(state);
                }
            }
        }
        if let Ok(bytes) = any.downcast::<PyBytes>() {
            Ok(bytes.as_bytes().to_vec())
        } else if let Ok(string) = any.downcast::<PyString>() {
            Ok(string.to_string().into_bytes())
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

fn encode_state_to_bytes(state: State) -> PyResult<Vec<u8>> {
    match state {
        State::None | State::Scalar(Scalar::Value(Value::None)) => Ok(Vec::new()),
        other => encode_state_via_destream(other),
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

pub(crate) async fn decode_state_from_bytes_async(
    bytes: Vec<u8>,
    txn: TxnHandle,
) -> Result<State, String> {
    let stream = stream::iter(vec![Ok::<Bytes, io::Error>(Bytes::from(bytes))]);
    destream_json::try_decode(StateContext::with_data(txn), stream)
        .await
        .map_err(|err| err.to_string())
}

pub(crate) fn py_state_handle_from_state(py: Python<'_>, state: State) -> PyResult<PyStateHandle> {
    let py_state = initialize_py_state(py, state)?;
    Ok(PyStateHandle::new(py_state))
}

fn initialize_py_state(py: Python<'_>, state: State) -> PyResult<Py<PyAny>> {
    match state {
        State::Collection(collection @ Collection::Tensor(_)) => {
            let initializer = PyState::initializer_from_state(State::Collection(collection))
                .add_subclass(PyCollection)
                .add_subclass(PyTensor);
            Py::new(py, initializer).map(|value| value.into_py(py))
        }
        other => Py::new(py, PyState::initializer_from_state(other)).map(|value| value.into_py(py)),
    }
}

fn py_body_is_none(body: Option<PyStateHandle>) -> bool {
    match body {
        None => true,
        Some(handle) => matches!(
            Python::with_gil(|py| {
                let value = handle.value();
                let any = value.bind(py);
                try_extract_state(any)
            }),
            Ok(Some(State::None)) | Ok(Some(State::Scalar(Scalar::Value(Value::None))))
        ),
    }
}
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
        let bytes = encode_state_to_bytes(self.clone_state())?;
        String::from_utf8(bytes).map_err(|err| PyValueError::new_err(err.to_string()))
    }
}

impl PyState {
    fn from_state(state: State) -> Self {
        Self {
            inner: PyWrapper::new(state),
        }
    }

    fn clone_state(&self) -> State {
        self.state().clone()
    }

    fn state(&self) -> &State {
        self.inner.inner()
    }

    fn initializer_from_state(state: State) -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyState::from_state(state))
    }
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
