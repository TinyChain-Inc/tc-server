use std::{path::PathBuf, sync::Arc};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use futures::FutureExt;
use pathlink::Link;
use pyo3::Bound;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule, PyType};
use tc_ir::{Claim, OpRef, Scalar, Subject, TxnId};
use tc_value::Value;
use umask;

use crate::{
    Body, Kernel, KernelDispatch, KernelHandler, Method, Request, StatusCode,
    auth::TokenContext,
    library::http as http_library,
    resolve::Resolve,
    storage::load_library_root,
    txn::{TxnError, TxnManager},
};

use super::state::{PyCollection, PyScalar, PyState, PyTensor, PyValue};
use super::types::{
    PyKernelConfig, PyKernelRequest, PyKernelResponse, PyStateHandle, apply_config_overrides,
};
use super::wire::{
    decode_scalar_from_bytes, decode_schema_from_json, encode_state_to_bytes, http_request_from_py,
    http_request_from_py_with_body, parse_path_and_txn_id, py_bearer_token, py_body_is_none,
    py_error_response, py_request_from_http, py_response_from_http, py_response_to_http,
    request_body_bytes,
};

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
            let root_dir = runtime
                .block_on(load_library_root(root))
                .expect("library root");
            runtime
                .block_on(http_library::build_http_library_module_with_store(
                    config.initial_schema.clone(),
                    Some(crate::storage::LibraryStore::from_root(root_dir)),
                ))
                .expect("library module")
        }
        None => runtime
            .block_on(http_library::build_http_library_module(
                config.initial_schema.clone(),
                None,
            ))
            .expect("library module"),
    };
    // PyO3 boundary is synchronous; hydrate storage by blocking here.
    runtime
        .block_on(module.hydrate_from_storage())
        .expect("library hydrate");
    let library_handlers = http_library::http_library_handlers(&module);

    let kernel_handler = crate::reflect::host_reflect_handler(Arc::new(python_handler(
        metrics.unwrap_or_else(stub_py_handler),
    )));
    let kernel_handler: Arc<dyn KernelHandler> = Arc::new(kernel_handler);
    let state_handler = crate::state::state_handler();
    let kernel_handler = move |req: Request| {
        let state_handler = state_handler.clone();
        let kernel_handler = kernel_handler.clone();
        async move {
            if req.uri().path().starts_with("/state/")
                && !crate::reflect::is_reflect_path(req.uri().path())
            {
                return state_handler.call(req).await;
            }
            kernel_handler.call(req).await
        }
        .boxed()
    };

    let mut builder = Kernel::builder()
        .with_host_id(config.host_id.clone())
        .with_library_module(module, library_handlers)
        .with_service_handler(python_handler(service))
        .with_kernel_handler(kernel_handler)
        .with_health_handler(python_health_handler())
        .with_txn_ttl(config.limits.txn_ttl);

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
    move |_req: Request| {
        async move {
            http::Response::builder()
                .status(StatusCode::OK)
                .body(Body::empty())
                .expect("health response")
        }
        .boxed()
    }
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
    config: PyKernelConfig,
}

impl Clone for KernelHandle {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            txn_manager: self.txn_manager.clone(),
            runtime: Arc::clone(&self.runtime),
            config: self.config.clone(),
        }
    }
}

impl KernelHandle {
    fn from_kernel(kernel: Kernel, config: PyKernelConfig) -> Self {
        let txn_manager = kernel.txn_manager().clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        Self {
            inner: Arc::new(kernel),
            txn_manager,
            runtime: Arc::new(runtime),
            config,
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
    #[pyo3(signature = (lib, service, metrics=None, data_dir=None, request_ttl_secs=None, max_request_bytes_unauth=None))]
    pub fn new(
        lib: Py<PyAny>,
        service: Py<PyAny>,
        metrics: Option<Py<PyAny>>,
        data_dir: Option<PathBuf>,
        request_ttl_secs: Option<u64>,
        max_request_bytes_unauth: Option<usize>,
    ) -> Self {
        let config = PyKernelConfig {
            data_dir,
            ..PyKernelConfig::default()
        };
        let config = apply_config_overrides(config, request_ttl_secs, max_request_bytes_unauth);
        let kernel = python_kernel_builder_with_config(lib, service, metrics, config.clone());
        Self::from_kernel(kernel, config)
    }

    /// Construct a local kernel handle with no Python service handlers installed.
    ///
    /// This is intended for tooling/tests which only need the Rust `/lib` and `/healthz` handlers
    /// (e.g. WASM installs into a local `data_dir`) without providing Python callbacks.
    #[classmethod]
    #[pyo3(signature = (data_dir=None, request_ttl_secs=None, max_request_bytes_unauth=None))]
    pub fn local(
        _cls: &Bound<'_, PyType>,
        data_dir: Option<PathBuf>,
        request_ttl_secs: Option<u64>,
        max_request_bytes_unauth: Option<usize>,
    ) -> Self {
        let stub = stub_py_handler();
        Self::new(
            stub.clone(),
            stub,
            None,
            data_dir,
            request_ttl_secs,
            max_request_bytes_unauth,
        )
    }

    #[classmethod]
    #[pyo3(signature = (schema_json, request_ttl_secs=None, max_request_bytes_unauth=None))]
    pub fn with_library_schema(
        _cls: &Bound<'_, PyType>,
        schema_json: &str,
        request_ttl_secs: Option<u64>,
        max_request_bytes_unauth: Option<usize>,
    ) -> PyResult<Self> {
        let schema = decode_schema_from_json(schema_json)?;
        let module =
            block_on_tokio(http_library::build_http_library_module(schema, None)).expect("module");
        let handlers = http_library::http_library_handlers(&module);
        let config = apply_config_overrides(
            PyKernelConfig::default(),
            request_ttl_secs,
            max_request_bytes_unauth,
        );
        let kernel_handler =
            crate::http::host_handler_with_public_keys(crate::auth::PublicKeyStore::default());
        let kernel = Kernel::builder()
            .with_host_id("tc-py-kernel")
            .with_library_module(module, handlers)
            .with_kernel_handler(kernel_handler)
            .with_txn_ttl(config.limits.txn_ttl)
            .finish();
        Ok(Self::from_kernel(kernel, config))
    }

    #[classmethod]
    #[pyo3(signature = (schema_json, token_host, actor_id, public_key_b64, data_dir=None, request_ttl_secs=None, max_request_bytes_unauth=None))]
    #[allow(clippy::too_many_arguments)]
    pub fn with_library_schema_rjwt(
        _cls: &Bound<'_, PyType>,
        schema_json: &str,
        token_host: &str,
        actor_id: &str,
        public_key_b64: &str,
        data_dir: Option<PathBuf>,
        request_ttl_secs: Option<u64>,
        max_request_bytes_unauth: Option<usize>,
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
        let host =
            Link::from_str(token_host).map_err(|_| PyValueError::new_err("invalid token host"))?;
        let key_bytes = STANDARD
            .decode(public_key_b64)
            .map_err(|_| PyValueError::new_err("invalid public key base64"))?;
        let verifying_key = rjwt::VerifyingKey::try_from(key_bytes.as_slice())
            .map_err(|_| PyValueError::new_err("invalid public key"))?;
        let actor = crate::auth::Actor::with_public_key(Value::from(actor_id), verifying_key);
        let keyring = crate::auth::KeyringActorResolver::default().with_actor(host, actor);
        let config = PyKernelConfig {
            data_dir,
            ..PyKernelConfig::default()
        };
        let config = apply_config_overrides(config, request_ttl_secs, max_request_bytes_unauth);
        let kernel_handler =
            crate::http::host_handler_with_public_keys(crate::auth::PublicKeyStore::default());
        let kernel = Kernel::builder()
            .with_host_id("tc-py-kernel")
            .with_library_module(module, handlers)
            .with_rjwt_keyring_token_verifier(keyring)
            .with_kernel_handler(kernel_handler)
            .with_txn_ttl(config.limits.txn_ttl)
            .finish();
        Ok(Self::from_kernel(kernel, config))
    }

    #[classmethod]
    #[pyo3(signature = (schema_json, dependency_root, authority, data_dir=None, request_ttl_secs=None, max_request_bytes_unauth=None))]
    pub fn with_library_schema_and_dependency_route(
        _cls: &Bound<'_, PyType>,
        schema_json: &str,
        dependency_root: &str,
        authority: &str,
        data_dir: Option<PathBuf>,
        request_ttl_secs: Option<u64>,
        max_request_bytes_unauth: Option<usize>,
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
        let config = PyKernelConfig {
            data_dir,
            ..PyKernelConfig::default()
        };
        let config = apply_config_overrides(config, request_ttl_secs, max_request_bytes_unauth);
        let kernel = Kernel::builder()
            .with_host_id("tc-py-kernel")
            .with_library_module(module, handlers)
            .with_dependency_route(dependency_root, authority)
            .with_http_rpc_gateway()
            .with_txn_ttl(config.limits.txn_ttl)
            .finish();
        Ok(Self::from_kernel(kernel, config))
    }

    #[classmethod]
    #[pyo3(signature = (schema_json, dependency_root, authority, token_host, actor_id, public_key_b64, data_dir=None, request_ttl_secs=None, max_request_bytes_unauth=None))]
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
        request_ttl_secs: Option<u64>,
        max_request_bytes_unauth: Option<usize>,
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
        let host =
            Link::from_str(token_host).map_err(|_| PyValueError::new_err("invalid token host"))?;
        let key_bytes = STANDARD
            .decode(public_key_b64)
            .map_err(|_| PyValueError::new_err("invalid public key base64"))?;
        let verifying_key = rjwt::VerifyingKey::try_from(key_bytes.as_slice())
            .map_err(|_| PyValueError::new_err("invalid public key"))?;
        let actor = crate::auth::Actor::with_public_key(Value::from(actor_id), verifying_key);
        let keyring = crate::auth::KeyringActorResolver::default().with_actor(host, actor);
        let config = PyKernelConfig {
            data_dir,
            ..PyKernelConfig::default()
        };
        let config = apply_config_overrides(config, request_ttl_secs, max_request_bytes_unauth);
        let kernel = Kernel::builder()
            .with_host_id("tc-py-kernel")
            .with_library_module(module, handlers)
            .with_dependency_route(dependency_root, authority)
            .with_http_rpc_gateway()
            .with_rjwt_keyring_token_verifier(keyring)
            .with_txn_ttl(config.limits.txn_ttl)
            .finish();
        Ok(Self::from_kernel(kernel, config))
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

        let mut txn_handle = match token.as_ref() {
            Some(token) => self
                .txn_manager
                .begin_with_owner(Some(&token.owner_id), Some(&token.bearer_token)),
            None => self.txn_manager.begin(),
        };
        let read_claim = Claim::new(
            Link::from_str(component_root)
                .map_err(|_| PyValueError::new_err("invalid component root"))?,
            umask::USER_READ,
        );
        let _ = self
            .txn_manager
            .record_claim(&txn_handle.id(), read_claim.clone());
        txn_handle = txn_handle.with_claims(vec![read_claim]);
        let txn = self.inner.with_resolver(txn_handle.clone());
        let txn_id = txn_handle.id();

        let scalar = if let Some(body) = body.clone() {
            let bytes = request_body_bytes(Some(body))?;
            if bearer_token.is_none() && bytes.len() > self.config.limits.max_request_bytes_unauth {
                return Err(PyValueError::new_err("request payload too large"));
            }
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
            Err(err)
                if err.message().starts_with("no egress route for ")
                    || err.message().starts_with("no RPC gateway configured") =>
            {
                let local_token = match token.as_ref() {
                    Some(token) => Some(token.clone()),
                    None => match txn_handle.raw_token() {
                        Some(raw) => match self
                            .block_on(self.inner.token_verifier().verify(raw.to_string()))
                        {
                            Ok(ctx) => Some(ctx),
                            Err(_) => Some(TokenContext::new(raw.to_string(), raw.to_string())),
                        },
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
                    bearer_token.as_deref().map(|token| {
                        vec![("authorization".to_string(), format!("Bearer {token}"))]
                    }),
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
        let body_bytes = if body_is_none {
            Vec::new()
        } else {
            let bytes = request_body_bytes(request.body())?;
            if bearer.is_none() && bytes.len() > self.config.limits.max_request_bytes_unauth {
                return Ok(PyKernelResponse::new(413, None, None));
            }
            bytes
        };
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
        let mut request = http_request_from_py_with_body(&request, body_bytes.clone())?;
        if !body_bytes.is_empty() {
            request
                .extensions_mut()
                .insert(crate::http::RequestBody::new(Bytes::from(body_bytes)));
        }

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
    #[pyo3(signature = (lib, service, metrics=None, data_dir=None, request_ttl_secs=None, max_request_bytes_unauth=None))]
    pub fn new(
        lib: Py<PyAny>,
        service: Py<PyAny>,
        metrics: Option<Py<PyAny>>,
        data_dir: Option<PathBuf>,
        request_ttl_secs: Option<u64>,
        max_request_bytes_unauth: Option<usize>,
    ) -> Self {
        Self {
            kernel: KernelHandle::new(
                lib,
                service,
                metrics,
                data_dir,
                request_ttl_secs,
                max_request_bytes_unauth,
            ),
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
