#![allow(unsafe_op_in_unsafe_fn)]

use std::{fmt, io, marker::PhantomData, path::PathBuf, sync::Arc};

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use bytes::Bytes;
use futures::{FutureExt, TryStreamExt, executor, stream};
use pathlink::Link;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyModule, PyString, PyType};
use pyo3::{Bound, PyClassInitializer, PyRef};
use tc_ir::{LibrarySchema, OpRef, Scalar, Subject, TxnId};
use url::form_urlencoded;

use crate::{
    Body, Kernel, KernelDispatch, KernelHandler, Method, Request, Response, State, StatusCode,
    resolve::Resolve,
    library::{default_library_schema, http as http_library},
    storage::load_library_root,
    txn::{TxnError, TxnManager},
};
use tc_state::{Collection, Tensor, null_transaction};
use tc_value::Value;

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
#[derive(Clone)]
pub struct PyKernelRequest {
    method: Method,
    path: String,
    headers: Vec<(String, String)>,
    body: Option<PyStateHandle>,
    // Retained for future Python adapter hooks; kept in-sync with the HTTP kernel path.
    #[allow(dead_code)]
    kernel: Option<Arc<Kernel>>,
}

impl fmt::Debug for PyKernelRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KernelRequest")
            .field("method", &self.method.as_str())
            .field("path", &self.path)
            .field("headers", &self.headers)
            .finish()
    }
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
            kernel: None,
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

    // Reserved for upcoming Python adapter integration; not wired yet.
    #[allow(dead_code)]
    pub(crate) fn bind_kernel(&mut self, kernel: Arc<Kernel>) {
        self.kernel = Some(kernel);
    }

    // Reserved for upcoming Python adapter integration; not wired yet.
    #[allow(dead_code)]
    pub(crate) fn kernel(&self) -> Option<Arc<Kernel>> {
        self.kernel.clone()
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
) -> Kernel {
    let _ = lib; // /lib is managed by the Rust kernel.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let storage_root = config.data_dir.clone();
    // PyO3 boundary is synchronous; block here by design to build the library module.
    let module = match storage_root {
        Some(root) => {
            let root_dir = runtime.block_on(load_library_root(root))
                .expect("library root");
            runtime.block_on(http_library::build_http_library_module_with_store(
                config.initial_schema.clone(),
                Some(crate::storage::LibraryStore::from_root(root_dir)),
            ))
            .expect("library module")
        }
        None => runtime.block_on(http_library::build_http_library_module(
            config.initial_schema.clone(),
            None,
        ))
        .expect("library module"),
    };
    // PyO3 boundary is synchronous; hydrate storage by blocking here.
    runtime.block_on(module.hydrate_from_storage()).expect("library hydrate");
    let library_handlers = http_library::http_library_handlers(&module);

    let mut builder = Kernel::builder()
        .with_host_id(config.host_id.clone())
        .with_library_module(module, library_handlers)
        .with_service_handler(python_handler(service))
        .with_kernel_handler(python_handler(metrics.unwrap_or_else(stub_py_handler)))
        .with_health_handler(python_health_handler());

    #[cfg(feature = "http-client")]
    {
        builder = builder.with_http_rpc_gateway();
    }

    builder.finish()
}

fn block_on_tokio<F>(fut: F) -> F::Output
where
    F: std::future::Future,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(fut)
}


pub(crate) fn python_handler(callback: Py<PyAny>) -> impl KernelHandler {
    move |req: Request| {
        let callback = callback.clone();
        async move {
            let py_req = match py_request_from_http(req).await {
                Ok(req) => req,
                Err(err) => return py_error_response(err),
            };

            let py_response = match Python::with_gil(|py| -> PyResult<PyKernelResponse> {
                let callable = callback.bind(py);
                let arg = Py::new(py, py_req.clone())?;
                let raw = callable.call1((arg,))?;
                let response: Py<PyKernelResponse> = raw.extract()?;
                Ok(response.borrow(py).clone_inner())
            }) {
                Ok(response) => response,
                Err(err) => return py_error_response(err),
            };

            match py_response_to_http(py_response).await {
                Ok(response) => response,
                Err(err) => py_error_response(err),
            }
        }
        .boxed()
    }
}

pub(crate) fn python_health_handler() -> impl KernelHandler {
    move |_req: Request| async move {
        http::Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .expect("health response")
    }
    .boxed()
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
    inner: Arc<Kernel>,
    txn_manager: TxnManager,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl Clone for KernelHandle {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            txn_manager: self.txn_manager.clone(),
            runtime: Arc::clone(&self.runtime),
        }
    }
}

impl KernelHandle {
    fn from_kernel(kernel: Kernel) -> Self {
        let txn_manager = kernel.txn_manager().clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        Self {
            inner: Arc::new(kernel),
            txn_manager,
            runtime: Arc::new(runtime),
        }
    }

    // PyO3 boundary is synchronous; block on async work within the dedicated single-thread runtime.
    fn block_on<F>(&self, fut: F) -> F::Output
    where
        F: std::future::Future,
    {
        self.runtime.block_on(fut)
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

    /// Construct a local kernel handle with no Python service handlers installed.
    ///
    /// This is intended for tooling/tests which only need the Rust `/lib` and `/healthz` handlers
    /// (e.g. WASM installs into a local `data_dir`) without providing Python callbacks.
    #[classmethod]
    #[pyo3(signature = (data_dir=None))]
    pub fn local(_cls: &Bound<'_, PyType>, data_dir: Option<PathBuf>) -> Self {
        let stub = stub_py_handler();
        Self::new(stub.clone(), stub, None, data_dir)
    }

    #[classmethod]
    pub fn with_library_schema(_cls: &Bound<'_, PyType>, schema_json: &str) -> PyResult<Self> {
        let schema = decode_schema_from_json(schema_json)?;
        let module = block_on_tokio(http_library::build_http_library_module(schema, None))
            .expect("module");
        let handlers = http_library::http_library_handlers(&module);
        let kernel = Kernel::builder()
            .with_host_id("tc-py-kernel")
            .with_library_module(module, handlers)
            .finish();
        Ok(Self::from_kernel(kernel))
    }

    #[classmethod]
    #[pyo3(signature = (schema_json, token_host, actor_id, public_key_b64, data_dir=None))]
    pub fn with_library_schema_rjwt(
        _cls: &Bound<'_, PyType>,
        schema_json: &str,
        token_host: &str,
        actor_id: &str,
        public_key_b64: &str,
        data_dir: Option<PathBuf>,
    ) -> PyResult<Self> {
        use std::str::FromStr;

        let schema = decode_schema_from_json(schema_json)?;
        let storage_root = data_dir.clone();
        let module = block_on_tokio(http_library::build_http_library_module(
            schema,
            storage_root,
        ))
        .expect("module");
        let handlers = http_library::http_library_handlers(&module);
        let host = Link::from_str(token_host)
            .map_err(|_| PyValueError::new_err("invalid token host"))?;
        let key_bytes = STANDARD
            .decode(public_key_b64)
            .map_err(|_| PyValueError::new_err("invalid public key base64"))?;
        let verifying_key = rjwt::VerifyingKey::try_from(key_bytes.as_slice())
            .map_err(|_| PyValueError::new_err("invalid public key"))?;
        let actor = crate::auth::Actor::with_public_key(Value::from(actor_id), verifying_key);
        let keyring = crate::auth::KeyringActorResolver::default().with_actor(host, actor);
        let kernel = Kernel::builder()
            .with_host_id("tc-py-kernel")
            .with_library_module(module, handlers)
            .with_rjwt_keyring_token_verifier(keyring)
            .finish();
        Ok(Self::from_kernel(kernel))
    }

    #[classmethod]
    #[pyo3(signature = (schema_json, dependency_root, authority, data_dir=None))]
    pub fn with_library_schema_and_dependency_route(
        _cls: &Bound<'_, PyType>,
        schema_json: &str,
        dependency_root: &str,
        authority: &str,
        data_dir: Option<PathBuf>,
    ) -> PyResult<Self> {
        let authority = authority
            .parse()
            .map_err(|_| PyValueError::new_err("invalid dependency route authority"))?;
        let schema = decode_schema_from_json(schema_json)?;
        let storage_root = data_dir.clone();
        let module = block_on_tokio(http_library::build_http_library_module(
            schema,
            storage_root,
        ))
        .expect("module");
        let handlers = http_library::http_library_handlers(&module);
        let kernel = Kernel::builder()
            .with_host_id("tc-py-kernel")
            .with_library_module(module, handlers)
            .with_dependency_route(dependency_root, authority)
            .with_http_rpc_gateway()
            .finish();
        Ok(Self::from_kernel(kernel))
    }

    #[classmethod]
    #[pyo3(signature = (schema_json, dependency_root, authority, token_host, actor_id, public_key_b64, data_dir=None))]
    #[allow(clippy::too_many_arguments)]
    pub fn with_library_schema_and_dependency_route_rjwt(
        _cls: &Bound<'_, PyType>,
        schema_json: &str,
        dependency_root: &str,
        authority: &str,
        token_host: &str,
        actor_id: &str,
        public_key_b64: &str,
        data_dir: Option<PathBuf>,
    ) -> PyResult<Self> {
        use std::str::FromStr;

        let authority = authority
            .parse()
            .map_err(|_| PyValueError::new_err("invalid dependency route authority"))?;
        let schema = decode_schema_from_json(schema_json)?;
        let storage_root = data_dir.clone();
        let module = block_on_tokio(http_library::build_http_library_module(
            schema,
            storage_root,
        ))
        .expect("module");
        let handlers = http_library::http_library_handlers(&module);
        let host = Link::from_str(token_host)
            .map_err(|_| PyValueError::new_err("invalid token host"))?;
        let key_bytes = STANDARD
            .decode(public_key_b64)
            .map_err(|_| PyValueError::new_err("invalid public key base64"))?;
        let verifying_key = rjwt::VerifyingKey::try_from(key_bytes.as_slice())
            .map_err(|_| PyValueError::new_err("invalid public key"))?;
        let actor = crate::auth::Actor::with_public_key(Value::from(actor_id), verifying_key);
        let keyring = crate::auth::KeyringActorResolver::default().with_actor(host, actor);
        let kernel = Kernel::builder()
            .with_host_id("tc-py-kernel")
            .with_library_module(module, handlers)
            .with_dependency_route(dependency_root, authority)
            .with_http_rpc_gateway()
            .with_rjwt_keyring_token_verifier(keyring)
            .finish();
        Ok(Self::from_kernel(kernel))
    }

    #[pyo3(signature = (path, body=None, bearer_token=None))]
    pub fn resolve_get(
        &self,
        path: &str,
        body: Option<PyStateHandle>,
        bearer_token: Option<String>,
    ) -> PyResult<PyKernelResponse> {
        use std::str::FromStr;

        let Some(component_root) = crate::uri::component_root(path) else {
            return Err(PyValueError::new_err("invalid target path"));
        };

        let token = match bearer_token.as_deref() {
            Some(token) => Some(
                self.block_on(self.inner.token_verifier().verify(token.to_string()))
                    .map_err(|_| PyValueError::new_err("invalid bearer token"))?,
            ),
            None => None,
        };

        let txn_handle = match token.as_ref() {
            Some(token) => self
                .txn_manager
                .begin_with_owner(Some(&token.owner_id), Some(&token.bearer_token)),
            None => self.txn_manager.begin(),
        };
        let txn = self.inner.with_resolver(txn_handle.clone());
        let txn_id = txn_handle.id();

        let scalar = if let Some(body) = body.clone() {
            let bytes = request_body_bytes(Some(body))?;
            if bytes.is_empty() {
                Scalar::default()
            } else {
                decode_scalar_from_bytes(bytes)?
            }
        } else {
            Scalar::default()
        };

        let op = OpRef::Get((
            Subject::Link(Link::from_str(path).map_err(|_| PyValueError::new_err("invalid path"))?),
            scalar,
        ));

        let resolved = self.block_on(op.resolve(&txn));

        let rollback_op = OpRef::Delete((
            Subject::Link(
                Link::from_str(component_root)
                    .map_err(|_| PyValueError::new_err("invalid component root"))?,
            ),
            Scalar::default(),
        ));

        let response = match resolved {
            Ok(state) => Python::with_gil(|py| {
                let body_bytes = encode_state_to_bytes(state)?;
                let body = if body_bytes.is_empty() {
                    None
                } else {
                    Some(PyStateHandle::new(
                        PyBytes::new_bound(py, &body_bytes).into_py(py),
                    ))
                };

                Ok(PyKernelResponse::new(200, None, body))
            }),
            Err(err) if err.message().starts_with("no egress route for ") => {
                let local_token = match token.as_ref() {
                    Some(token) => Some(token.clone()),
                    None => match txn_handle.raw_token() {
                        Some(raw) => Some(
                            self.block_on(self.inner.token_verifier().verify(raw.to_string()))
                                .map_err(|_| PyValueError::new_err("invalid bearer token"))?,
                        ),
                        None => None,
                    },
                };

                // If this target is not routed for egress, fall back to local dispatch within the
                // same transaction and still rollback afterwards. This keeps per-call ergonomics
                // stable: a GET either hits a local installed handler or routes through the RPC
                // gateway based on kernel config, not caller metadata.
                let request = PyKernelRequest::new(
                    "GET",
                    path,
                    bearer_token
                        .as_deref()
                        .map(|token| vec![("authorization".to_string(), format!("Bearer {token}"))]),
                    body,
                )?;
                let request = http_request_from_py(&request)?;

                match self.inner.route_request(
                    Method::Get,
                    path,
                    request,
                    Some(txn_id),
                    true,
                    local_token.as_ref(),
                    |handle, req| {
                        req.extensions_mut().insert(handle.clone());
                    },
                ) {
                    Ok(KernelDispatch::Response(resp)) => {
                        self.block_on(async move { py_response_from_http(resp.await).await })
                    }
                    Ok(KernelDispatch::Finalize { commit: _, result }) => {
                        let status = if result.is_ok() { 204 } else { 400 };
                        Ok(PyKernelResponse::new(status, None, None))
                    }
                    Ok(KernelDispatch::NotFound) => Ok(PyKernelResponse::new(404, None, None)),
                    Err(TxnError::NotFound) => Err(PyValueError::new_err("unknown transaction id")),
                    Err(TxnError::Unauthorized) => {
                        Err(PyValueError::new_err("unauthorized transaction owner"))
                    }
                }
            }
            Err(err) => Err(PyValueError::new_err(err.message().to_string())),
        };

    let _ = self.block_on(rollback_op.resolve(&txn));
    let _ = self.txn_manager.rollback(txn_id);

    response
}

    pub fn dispatch(&self, request: PyKernelRequest) -> PyResult<PyKernelResponse> {
        let method = request.method_enum();
        let raw_path = request.path_owned();
        let (route_path, txn_id) = parse_path_and_txn_id(&raw_path)?;
        let body_is_none = py_body_is_none(request.body());
        let bearer = py_bearer_token(&request);
        let inbound_bearer = bearer.clone();
        let token = match bearer {
            Some(token) => Some(
                self.block_on(self.inner.token_verifier().verify(token))
                    .map_err(|_| PyValueError::new_err("invalid bearer token"))?,
            ),
            None => None,
        };
        let inbound_txn_id = txn_id;
        let mut minted_txn_id: Option<TxnId> = None;
        let mut minted_token: Option<String> = None;
        let request = http_request_from_py(&request)?;

        match self.inner.route_request(
            method,
            &route_path,
            request,
            inbound_txn_id,
            body_is_none,
            token.as_ref(),
            |handle, req| {
                minted_txn_id = Some(handle.id());
                if inbound_bearer.is_none() {
                    minted_token = handle.raw_token().map(str::to_string);
                }
                req.extensions_mut().insert(handle.clone());
            },
        ) {
            Ok(KernelDispatch::Response(resp)) => {
                let mut response =
                    self.block_on(async move { py_response_from_http(resp.await).await })?;
                if inbound_txn_id.is_none()
                    && let Some(txn_id) = minted_txn_id
                {
                    response
                        .headers
                        .push(("x-tc-txn-id".to_string(), txn_id.to_string()));
                    if let Some(token) = minted_token {
                        response
                            .headers
                            .push(("x-tc-bearer-token".to_string(), token));
                    }
                }
                Ok(response)
            }
            Ok(KernelDispatch::Finalize { commit: _, result }) => {
                let status = if result.is_ok() { 204 } else { 400 };
                Ok(PyKernelResponse::new(status, None, None))
            }
            Ok(KernelDispatch::NotFound) => Err(PyValueError::new_err(format!(
                "no handler for method {method} path {route_path}"
            ))),
            Err(TxnError::NotFound) => Err(PyValueError::new_err("unknown transaction id")),
            Err(TxnError::Unauthorized) => {
                Err(PyValueError::new_err("unauthorized transaction owner"))
            }
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

pub fn register_python_api(module: &Bound<'_, PyModule>) -> PyResult<()> {
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

fn http_request_from_py(request: &PyKernelRequest) -> PyResult<Request> {
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
    builder
        .body(Body::from(body_bytes))
        .map_err(|err| PyValueError::new_err(err.to_string()))
}

async fn py_request_from_http(req: Request) -> PyResult<PyKernelRequest> {
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

async fn py_response_from_http(response: Response) -> PyResult<PyKernelResponse> {
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
    let body = if body_bytes.is_empty() || status >= 400 {
        None
    } else {
        let state = decode_state_from_bytes(body_bytes.to_vec())?;
        Some(py_state_handle_from_state(state)?)
    };

    Ok(PyKernelResponse::new(status, Some(headers), body))
}

async fn py_response_to_http(response: PyKernelResponse) -> PyResult<Response> {
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

fn py_error_response(err: PyErr) -> Response {
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

#[cfg(all(test, feature = "pyo3", feature = "http-server", feature = "wasm"))]
mod tests {
    use super::*;
    use futures::FutureExt;
    use crate::{Body, Request, Response, StatusCode};
    use hyper::body::to_bytes;
    use pathlink::Link;
    use std::{net::TcpListener, str::FromStr};
    use tower::Service;
    use url::form_urlencoded;

    #[derive(Clone)]
    struct KernelService {
        kernel: crate::Kernel,
    }

    impl KernelService {
        fn new(kernel: crate::Kernel) -> Self {
            Self { kernel }
        }
    }

    impl Service<Request> for KernelService {
        type Response = Response;
        type Error = hyper::Error;
        type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: Request) -> Self::Future {
            let uri = req.uri().clone();
            let method = req.method().clone();
            let path = uri.path().to_owned();
            let kernel = self.kernel.clone();

            Box::pin(async move {
                let method = match method {
                    crate::HttpMethod::GET => crate::Method::Get,
                    crate::HttpMethod::PUT => crate::Method::Put,
                    crate::HttpMethod::POST => crate::Method::Post,
                    crate::HttpMethod::DELETE => crate::Method::Delete,
                    _ => {
                        return Ok(http::Response::builder()
                            .status(StatusCode::METHOD_NOT_ALLOWED)
                            .body(Body::empty())
                            .expect("method not allowed"));
                    }
                };

                let (req, body_is_none) = match parse_body(req).await {
                    Ok(pair) => pair,
                    Err(resp) => return Ok(resp),
                };

                let txn_id = match parse_txn_id(&req) {
                    Ok(id) => id,
                    Err(()) => {
                        return Ok(http::Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from("invalid transaction id"))
                            .expect("bad request"));
                    }
                };

                let bearer = req
                    .headers()
                    .get(crate::header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|auth| auth.split_once(' '))
                    .and_then(|(scheme, token)| {
                        if scheme.eq_ignore_ascii_case("bearer") {
                            Some(token.trim().to_string())
                        } else {
                            None
                        }
                    });

                let token = match bearer {
                    Some(token) => match kernel.token_verifier().verify(token).await {
                        Ok(token) => Some(token),
                        Err(crate::txn::TxnError::Unauthorized) => {
                            return Ok(http::Response::builder()
                                .status(StatusCode::UNAUTHORIZED)
                                .body(Body::empty())
                                .expect("unauthorized"));
                        }
                        Err(crate::txn::TxnError::NotFound) => None,
                    },
                    None => None,
                };

                match kernel.route_request(
                    method,
                    &path,
                    req,
                    txn_id,
                    body_is_none,
                    token.as_ref(),
                    |handle, req| {
                        req.extensions_mut().insert(handle.clone());
                    },
                ) {
                    Ok(crate::KernelDispatch::Response(resp)) => Ok(resp.await),
                    Ok(crate::KernelDispatch::Finalize { commit: _, result }) => {
                        let status = if result.is_ok() {
                            StatusCode::NO_CONTENT
                        } else {
                            StatusCode::BAD_REQUEST
                        };
                        Ok(http::Response::builder()
                            .status(status)
                            .body(Body::empty())
                            .expect("finalize response"))
                    }
                    Ok(crate::KernelDispatch::NotFound) => Ok(http::Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Body::empty())
                        .expect("not found")),
                    Err(crate::txn::TxnError::NotFound) => Ok(http::Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("unknown transaction id"))
                        .expect("bad request")),
                    Err(crate::txn::TxnError::Unauthorized) => Ok(http::Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body(Body::empty())
                        .expect("unauthorized")),
                }
            })
        }
    }

    async fn parse_body(req: Request) -> Result<(Request, bool), Response> {
        let (parts, body) = req.into_parts();
        let body_bytes = to_bytes(body).await.map_err(|_| {
            http::Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from("failed to read request body"))
                .expect("internal error")
        })?;
        let body_is_none = body_bytes.iter().all(|b| b.is_ascii_whitespace());
        Ok((
            Request::from_parts(parts, Body::from(body_bytes)),
            body_is_none,
        ))
    }

    fn parse_txn_id(req: &Request) -> Result<Option<tc_ir::TxnId>, ()> {
        let query = req.uri().query().unwrap_or("");
        let txn_id_param = form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .find(|(k, _)| k.eq_ignore_ascii_case("txn_id"))
            .map(|(_, v)| v);

        match txn_id_param {
            Some(raw) => raw.parse::<tc_ir::TxnId>().map(Some).map_err(|_| ()),
            None => Ok(None),
        }
    }

    fn ok_py_handler() -> impl KernelHandler {
        |_req: Request| async move { Response::new(Body::empty()) }.boxed()
    }

    fn wasm_module() -> Vec<u8> {
        crate::test_utils::wasm_hello_world_module()
    }

    #[tokio::test]
    async fn pyo3_kernel_resolves_remote_opref_over_http() {
        use tc_ir::OpRef;

        let bytes = wasm_module();
        let initial =
            tc_ir::LibrarySchema::new(Link::from_str("/lib/initial").unwrap(), "0.0.1", vec![]);
        let module =
            crate::library::http::build_http_library_module(initial, None)
                .await
                .expect("module");
        let handlers = crate::library::http::http_library_handlers(&module);

        let remote_kernel: crate::Kernel = crate::Kernel::builder()
            .with_host_id("tc-remote-test")
            .with_library_module(module, handlers)
            .with_service_handler(|_req| async move {
                http::Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("service"))
                    .expect("service response")
            })
            .with_kernel_handler(|_req| async move { Response::new(Body::empty()) })
            .with_health_handler(|_req| async move { Response::new(Body::empty()) })
            .finish();

        let install_payload = serde_json::json!({
            "schema": {
                "id": "/lib/example-devco/example/0.1.0",
                "version": "0.1.0",
                "dependencies": []
            },
            "artifacts": [{
                "path": "/lib/wasm",
                "content_type": "application/wasm",
                "bytes": base64::engine::general_purpose::STANDARD.encode(&bytes),
            }]
        });

        let mut install_request = http::Request::builder()
            .method("PUT")
            .uri("/lib")
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Body::from(install_payload.to_string()))
            .expect("install request");
        let install_claim = tc_ir::Claim::new(
            Link::from_str("/lib/example-devco/example/0.1.0").unwrap(),
            umask::USER_WRITE,
        );
        let install_txn = remote_kernel
            .txn_manager()
            .begin()
            .with_claims(vec![install_claim]);
        install_request.extensions_mut().insert(install_txn);

        let install_response = remote_kernel
            .dispatch(crate::Method::Put, "/lib", install_request)
            .expect("install handler")
            .await;
        assert_eq!(install_response.status(), StatusCode::NO_CONTENT);

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let server_kernel = remote_kernel.clone();
        let server = hyper::Server::from_tcp(listener)
            .expect("hyper server")
            .serve(tower::make::Shared::new(KernelService::new(server_kernel)));

        let server_task = tokio::spawn(async move { server.await.expect("server") });

        let dependency_root = "/lib/example-devco/example/0.1.0";
        let local_schema = tc_ir::LibrarySchema::new(
            Link::from_str("/lib/local").unwrap(),
            "0.1.0",
            vec![Link::from_str(dependency_root).unwrap()],
        );
        let local_module =
            crate::library::http::build_http_library_module(local_schema, None)
                .await
                .expect("module");
        let local_handlers = crate::library::http::http_library_handlers(&local_module);

        let local_kernel: Kernel = crate::Kernel::builder()
            .with_host_id("tc-py-local")
            .with_library_module(local_module, local_handlers)
            .with_dependency_route(dependency_root, addr)
            .with_http_rpc_gateway()
            .with_service_handler(ok_py_handler())
            .with_kernel_handler(ok_py_handler())
            .with_health_handler(ok_py_handler())
            .finish();

        let remote_txn = remote_kernel.txn_manager().begin();
        let owner_id = remote_txn
            .owner_id()
            .expect("remote txn owner");
        let bearer = remote_txn
            .raw_token()
            .expect("remote txn bearer token");
        let _ = local_kernel.txn_manager().interpret_request(
            Some(remote_txn.id()),
            Some(owner_id),
            Some(bearer),
        );
        let txn = local_kernel.with_resolver(remote_txn);
        let op = OpRef::Get((
            tc_ir::Subject::Link(
                Link::from_str("/lib/example-devco/example/0.1.0/hello").unwrap(),
            ),
            tc_ir::Scalar::default(),
        ));

        let state = op.resolve(&txn).await.expect("resolve response");

        assert!(matches!(
            state,
            tc_state::State::Scalar(tc_ir::Scalar::Value(tc_value::Value::String(ref s))) if s == "hello"
        ));

        server_task.abort();
    }

    #[tokio::test]
    async fn pyo3_kernel_rejects_unauthorized_egress() {
        use tc_ir::OpRef;

        let local_schema = tc_ir::LibrarySchema::new(
            Link::from_str("/lib/local").unwrap(),
            "0.1.0",
            vec![Link::from_str("/lib").unwrap()],
        );
        let local_module =
            crate::library::http::build_http_library_module(local_schema, None)
                .await
                .expect("module");
        let local_handlers = crate::library::http::http_library_handlers(&local_module);

        let local_kernel: Kernel = crate::Kernel::builder()
            .with_host_id("tc-py-local")
            .with_library_module(local_module, local_handlers)
            .with_dependency_route("/lib", "127.0.0.1:1".parse().expect("addr"))
            .with_http_rpc_gateway()
            .with_service_handler(ok_py_handler())
            .with_kernel_handler(ok_py_handler())
            .with_health_handler(ok_py_handler())
            .finish();

        let txn = local_kernel.with_resolver(local_kernel.txn_manager().begin());
        let op = OpRef::Get((
            tc_ir::Subject::Link(Link::from_str("/service").unwrap()),
            tc_ir::Scalar::default(),
        ));

        let err = op.resolve(&txn).await.expect_err("unauthorized");

        assert_eq!(err.code(), tc_error::ErrorKind::Unauthorized);
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

async fn decode_scalar_from_bytes_async(bytes: Vec<u8>) -> Result<Scalar, String> {
    let stream = stream::iter(vec![Ok::<Bytes, io::Error>(Bytes::from(bytes))]);
    destream_json::try_decode((), stream)
        .await
        .map_err(|err| err.to_string())
}

fn decode_scalar_from_bytes(bytes: Vec<u8>) -> PyResult<Scalar> {
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
