#![allow(unsafe_op_in_unsafe_fn)]

use std::{fmt, io, marker::PhantomData, path::PathBuf, sync::Arc};

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
    Kernel, KernelDispatch, KernelHandler, Method, State,
    library::{
        InstallError, InstallRequest, LibraryHandlers, LibraryRouteFactory, LibraryRoutes,
        LibraryRuntime, LibraryState, apply_wasm_install, decode_install_request_bytes,
        default_library_schema,
    },
    storage::LibraryDir,
    txn::{TxnError, TxnManager},
};
use tc_state::{Collection, Tensor};
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
    fn from_kernel(kernel: PyKernel) -> Self {
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

    #[classmethod]
    pub fn with_library_schema_and_dependency_route(
        _cls: &Bound<'_, PyType>,
        schema_json: &str,
        dependency_root: &str,
        authority: &str,
    ) -> PyResult<Self> {
        let schema = decode_schema_from_json(schema_json)?;
        let module = build_pyo3_library_module(schema, None);
        let handlers = pyo3_library_handlers(&module);
        let kernel = Kernel::builder()
            .with_host_id("tc-py-kernel")
            .with_library_module(module, handlers)
            .with_dependency_route(dependency_root, authority)
            .with_http_rpc_gateway()
            .finish();
        Ok(Self::from_kernel(kernel))
    }

    #[pyo3(signature = (path, bearer_token=None))]
    pub fn resolve_get(
        &self,
        path: &str,
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

        let txn_id = match token.as_ref() {
            Some(token) => self
                .txn_manager
                .begin_with_owner(Some(&token.owner_id), Some(&token.bearer_token))
                .id(),
            None => self.txn_manager.begin().id(),
        };

        let op = OpRef::Get((
            Subject::Link(Link::from_str(path).map_err(|_| PyValueError::new_err("invalid path"))?),
            Scalar::default(),
        ));

        let resolved = self
            .block_on(self.inner.resolve_op(txn_id, bearer_token.clone(), op))
            .map_err(|err| PyValueError::new_err(err.message().to_string()));

        let rollback_op = OpRef::Delete((
            Subject::Link(
                Link::from_str(component_root)
                    .map_err(|_| PyValueError::new_err("invalid component root"))?,
            ),
            Scalar::default(),
        ));

        let _ = self.block_on(self.inner.resolve_op(txn_id, bearer_token, rollback_op));
        let _ = self.txn_manager.rollback(txn_id);

        let response = resolved?;
        Python::with_gil(|py| {
            let body = if response.body.is_empty() {
                None
            } else {
                Some(PyStateHandle::new(
                    PyBytes::new_bound(py, &response.body).into_py(py),
                ))
            };

            Ok(PyKernelResponse::new(
                response.status,
                Some(response.headers),
                body,
            ))
        })
    }

    pub fn dispatch(&self, request: PyKernelRequest) -> PyResult<PyKernelResponse> {
        let method = request.method_enum();
        let raw_path = request.path_owned();
        let (route_path, txn_id) = parse_path_and_txn_id(&raw_path)?;
        let body_is_none = py_body_is_none(request.body());
        let bearer = py_bearer_token(&request);
        let token = match bearer {
            Some(token) => Some(
                self.block_on(self.inner.token_verifier().verify(token))
                    .map_err(|_| PyValueError::new_err("invalid bearer token"))?,
            ),
            None => None,
        };
        let inbound_txn_id = txn_id;
        let mut minted_txn_id: Option<TxnId> = None;

        match self.inner.route_request(
            method,
            &route_path,
            request,
            inbound_txn_id,
            body_is_none,
            token.as_ref(),
            |handle, req| {
                minted_txn_id = Some(handle.id());
                req.bind_txn_handle(handle.clone());
            },
        ) {
            Ok(KernelDispatch::Response(resp)) => {
                let mut response = self.block_on(resp)?;
                if inbound_txn_id.is_none()
                    && let Some(txn_id) = minted_txn_id
                {
                    response
                        .headers
                        .push(("x-tc-txn-id".to_string(), txn_id.to_string()));
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

fn py_bearer_token(request: &PyKernelRequest) -> Option<String> {
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
    use base64::Engine as _;
    use futures::FutureExt;
    use hyper::{Body, Request, Response, StatusCode, body::to_bytes};
    use pathlink::Link;
    use std::{net::TcpListener, str::FromStr};
    use tower::Service;
    use url::form_urlencoded;

    #[derive(Clone)]
    struct KernelService {
        kernel: crate::HttpKernel,
    }

    impl KernelService {
        fn new(kernel: crate::HttpKernel) -> Self {
            Self { kernel }
        }
    }

    impl Service<Request<Body>> for KernelService {
        type Response = Response<Body>;
        type Error = hyper::Error;
        type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: Request<Body>) -> Self::Future {
            let uri = req.uri().clone();
            let method = req.method().clone();
            let path = uri.path().to_owned();
            let kernel = self.kernel.clone();

            Box::pin(async move {
                let method = match method {
                    hyper::Method::GET => crate::Method::Get,
                    hyper::Method::PUT => crate::Method::Put,
                    hyper::Method::POST => crate::Method::Post,
                    hyper::Method::DELETE => crate::Method::Delete,
                    _ => {
                        return Ok(Response::builder()
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
                        return Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from("invalid transaction id"))
                            .expect("bad request"));
                    }
                };

                match kernel.route_request(
                    method,
                    &path,
                    req,
                    txn_id,
                    body_is_none,
                    None,
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
                        Ok(Response::builder()
                            .status(status)
                            .body(Body::empty())
                            .expect("finalize response"))
                    }
                    Ok(crate::KernelDispatch::NotFound) => Ok(Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Body::empty())
                        .expect("not found")),
                    Err(crate::txn::TxnError::NotFound) => Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("unknown transaction id"))
                        .expect("bad request")),
                    Err(crate::txn::TxnError::Unauthorized) => Ok(Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body(Body::empty())
                        .expect("unauthorized")),
                }
            })
        }
    }

    async fn parse_body(req: Request<Body>) -> Result<(Request<Body>, bool), Response<Body>> {
        let (parts, body) = req.into_parts();
        let body_bytes = to_bytes(body).await.map_err(|_| {
            Response::builder()
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

    fn parse_txn_id(req: &Request<Body>) -> Result<Option<tc_ir::TxnId>, ()> {
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

    fn ok_py_response() -> PyResult<PyKernelResponse> {
        Ok(PyKernelResponse::new(200, None, None))
    }

    fn ok_py_handler() -> impl KernelHandler<PyKernelRequest, PyResult<PyKernelResponse>> {
        |_req: PyKernelRequest| async move { ok_py_response() }.boxed()
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
        let module = crate::library::http::build_http_library_module(initial, None);
        let handlers = crate::library::http::http_library_handlers(&module);

        let remote_kernel: crate::HttpKernel = crate::Kernel::builder()
            .with_host_id("tc-remote-test")
            .with_library_module(module, handlers)
            .with_service_handler(|_req| async move {
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("service"))
                    .expect("service response")
            })
            .with_kernel_handler(|_req| async move { Response::new(Body::empty()) })
            .with_health_handler(|_req| async move { Response::new(Body::empty()) })
            .finish();

        let install_payload = serde_json::json!({
            "schema": {
                "id": "/lib/example",
                "version": "0.1.0",
                "dependencies": []
            },
            "artifacts": [{
                "path": "/lib/wasm",
                "content_type": "application/wasm",
                "bytes": base64::engine::general_purpose::STANDARD.encode(&bytes),
            }]
        });

        let install_request = Request::builder()
            .method("PUT")
            .uri("/lib")
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Body::from(install_payload.to_string()))
            .expect("install request");

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

        let local_schema = tc_ir::LibrarySchema::new(
            Link::from_str("/lib/local").unwrap(),
            "0.1.0",
            vec![Link::from_str("/lib").unwrap()],
        );
        let local_module = build_pyo3_library_module(local_schema, None);
        let local_handlers = pyo3_library_handlers(&local_module);

        let local_kernel: PyKernel = crate::Kernel::builder()
            .with_host_id("tc-py-local")
            .with_library_module(local_module, local_handlers)
            .with_dependency_route("/lib", addr.to_string())
            .with_http_rpc_gateway()
            .with_service_handler(ok_py_handler())
            .with_kernel_handler(ok_py_handler())
            .with_health_handler(ok_py_handler())
            .finish();

        let txn_id = remote_kernel.txn_manager().begin().id();
        let op = OpRef::Get((
            tc_ir::Subject::Link(Link::from_str("/lib/hello").unwrap()),
            tc_ir::Scalar::default(),
        ));

        let response = local_kernel
            .resolve_op(txn_id, None, op)
            .await
            .expect("resolve response");

        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"\"hello\"");

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
        let local_module = build_pyo3_library_module(local_schema, None);
        let local_handlers = pyo3_library_handlers(&local_module);

        let local_kernel: PyKernel = crate::Kernel::builder()
            .with_host_id("tc-py-local")
            .with_library_module(local_module, local_handlers)
            .with_dependency_route("/lib", "127.0.0.1:1")
            .with_http_rpc_gateway()
            .with_service_handler(ok_py_handler())
            .with_kernel_handler(ok_py_handler())
            .with_health_handler(ok_py_handler())
            .finish();

        let txn_id = local_kernel.txn_manager().begin().id();
        let op = OpRef::Get((
            tc_ir::Subject::Link(Link::from_str("/service").unwrap()),
            tc_ir::Scalar::default(),
        ));

        let err = local_kernel
            .resolve_op(txn_id, None, op)
            .await
            .expect_err("unauthorized");

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
