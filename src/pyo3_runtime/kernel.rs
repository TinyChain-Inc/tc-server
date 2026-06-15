use std::{path::PathBuf, sync::Arc};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use futures::FutureExt;
use pathlink::Link;
use pyo3::Bound;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyModule, PyType};
use tc_ir::{Claim, OpRef, Scalar, Subject};
use umask;

use crate::{
    Body, Kernel, KernelDispatch, KernelHandler, Request, StatusCode,
    library::decode_compiled_library_package,
    library::decode_install_request_bytes,
    library::http as http_library,
    resolve::Resolve,
    storage::load_library_root,
    txn::{TxnError, TxnManager},
};

use super::state::{PyState, PyTensor};
use super::state_handle_conversions::{
    encode_state_to_bytes, py_state_handle_from_state, request_body_raw_bytes, request_body_state,
};
use super::types::{
    PyKernelConfig, PyKernelRequest, PyKernelResponse, PyStateHandle, apply_config_overrides,
};
use super::wire::{
    http_request_from_py_with_body, parse_path_and_txn_id, py_bearer_token, py_error_response,
    py_request_from_http, py_response_from_http, py_response_to_http,
};

fn parse_rjwt_alg(alg: &str) -> PyResult<rjwt::AlgKind> {
    match alg.trim().to_ascii_lowercase().as_str() {
        "falcon512" | "falcon-512" | "fn-dsa-512" => Ok(rjwt::AlgKind::Falcon512),
        "ed25519" | "eddsa" => Ok(rjwt::AlgKind::Ed25519),
        other => Err(PyValueError::new_err(format!(
            "unsupported signature algorithm: {other}"
        ))),
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

    let kernel_handler = crate::reflect::reflect_overlay_handler(Arc::new(python_handler(
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
    #[pyo3(signature = (definition_json, routes=None, token=None, data_dir=None, request_ttl_secs=None, max_request_bytes_unauth=None))]
    #[allow(clippy::too_many_arguments)]
    pub fn with_library_definition(
        _cls: &Bound<'_, PyType>,
        definition_json: &str,
        routes: Option<Vec<(String, String)>>,
        token: Option<&Bound<'_, PyAny>>,
        data_dir: Option<PathBuf>,
        request_ttl_secs: Option<u64>,
        max_request_bytes_unauth: Option<usize>,
    ) -> PyResult<Self> {
        use std::str::FromStr;

        let package = decode_install_request_bytes(definition_json.as_bytes())
            .map_err(|err| PyValueError::new_err(err.message().to_string()))?;
        let storage_root = data_dir.clone();
        let module = block_on_tokio(http_library::build_http_library_module(
            package.schema.clone(),
            storage_root,
        ))
        .expect("module");
        block_on_tokio(module.install_compiled_package(package))
            .map_err(|err| PyValueError::new_err(err.message().to_string()))?;
        block_on_tokio(module.hydrate_from_storage()).expect("library hydrate");
        let handlers = http_library::http_library_handlers(&module);
        let config = PyKernelConfig {
            data_dir,
            ..PyKernelConfig::default()
        };
        let config = apply_config_overrides(config, request_ttl_secs, max_request_bytes_unauth);
        let stub = stub_py_handler();
        let kernel_handler =
            crate::http::host_handler_with_public_keys(crate::auth::PublicKeyStore::default());
        let mut builder = Kernel::builder()
            .with_host_id("tc-py-kernel")
            .with_library_module(module, handlers)
            .with_service_handler(python_handler(stub))
            .with_kernel_handler(kernel_handler)
            .with_health_handler(python_health_handler())
            .with_txn_ttl(config.limits.txn_ttl);

        for (dependency_root, authority) in routes.unwrap_or_default() {
            let authority = authority
                .parse()
                .map_err(|_| PyValueError::new_err("invalid dependency route authority"))?;
            builder = builder.with_dependency_route(&dependency_root, authority);
        }

        #[cfg(feature = "http-client")]
        {
            builder = builder.with_http_rpc_gateway();
        }

        if let Some(token) = token {
            let token_host: String = token.getattr("host")?.extract()?;
            let actor_id: String = token.getattr("actor_id")?.extract()?;
            let public_key_b64: String = token.getattr("public_key_b64")?.extract()?;
            let alg: String = token
                .getattr("alg")
                .ok()
                .and_then(|value| value.extract::<String>().ok())
                .unwrap_or_else(|| "falcon512".to_string());
            let secret_key_b64: Option<String> = token
                .getattr("secret_key_b64")
                .ok()
                .and_then(|value| value.extract::<String>().ok())
                .filter(|value| !value.trim().is_empty());
            let host = Link::from_str(&token_host)
                .map_err(|_| PyValueError::new_err("invalid token host"))?;
            let key_bytes = STANDARD
                .decode(public_key_b64.as_bytes())
                .map_err(|_| PyValueError::new_err("invalid public key base64"))?;
            let verifying_key = crate::auth::verifying_key_from_bytes(key_bytes.as_slice())
                .map_err(|_| PyValueError::new_err("invalid public key"))?;
            let actor = crate::auth::Actor::with_verifying_key(actor_id.clone(), verifying_key);
            let keyring =
                crate::auth::KeyringActorResolver::default().with_actor(host.clone(), actor);
            builder = builder.with_rjwt_keyring_token_verifier(keyring);

            if let Some(secret_key_b64) = secret_key_b64 {
                let key_bytes = STANDARD
                    .decode(secret_key_b64.as_bytes())
                    .map_err(|_| PyValueError::new_err("invalid secret key base64"))?;
                let alg = parse_rjwt_alg(&alg)?;
                let signing_key = rjwt::SigningKey::from_bytes(alg, key_bytes.as_slice())
                    .map_err(|_| PyValueError::new_err("invalid secret key"))?;
                let actor = crate::auth::Actor::with_signing_key(actor_id, signing_key);
                builder = builder.with_protocol_actor(host, actor);
            }
        }

        Ok(Self::from_kernel(builder.finish(), config))
    }

    #[pyo3(signature = (path, body=None, token=None))]
    pub fn resolve_get(
        &self,
        path: &str,
        body: Option<PyStateHandle>,
        token: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyKernelResponse> {
        use std::str::FromStr;

        let Some(component_root) = crate::uri::component_root(path) else {
            return Err(PyValueError::new_err("invalid target path"));
        };

        let bearer_token = match token {
            Some(token) => Some(token.getattr("bearer_token")?.extract::<String>()?),
            None => None,
        };

        let token = match bearer_token.as_deref() {
            Some(bearer) => Some(
                self.block_on(self.inner.token_verifier().verify(bearer.to_string()))
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
        self.txn_manager
            .record_claim(&txn_handle.id(), read_claim.clone())
            .map_err(|_| PyValueError::new_err("unknown transaction id"))?;
        txn_handle = txn_handle.with_claims(vec![read_claim]);
        let txn = self.inner.with_resolver(txn_handle.clone());

        let scalar = if let Some(body) = body.clone() {
            if let Some(state) = request_body_state(Some(body.clone()))? {
                let bytes = encode_state_to_bytes(state.clone())?;
                if bearer_token.is_none()
                    && bytes.len() > self.config.limits.max_request_bytes_unauth
                {
                    return Err(PyValueError::new_err("request payload too large"));
                }

                match state {
                    crate::State::None => Scalar::default(),
                    crate::State::Scalar(scalar) => scalar,
                    _ => return Err(PyValueError::new_err("expected scalar request body")),
                }
            } else {
                return Err(PyValueError::new_err("expected tinychain.State body"));
            }
        } else {
            Scalar::default()
        };

        let op = OpRef::Get((
            Subject::Link(Link::from_str(path).map_err(|_| PyValueError::new_err("invalid path"))?),
            scalar,
        ));

        let resolved = self.block_on(op.resolve(&txn));

        let response = match resolved {
            Ok(state) => {
                let body = if state.is_none() {
                    None
                } else {
                    Some(py_state_handle_from_state(state)?)
                };

                Ok(PyKernelResponse::new(200, None, body))
            }
            Err(err) => Err(PyValueError::new_err(err.message().to_string())),
        };

        self.block_on(self.inner.finalize_transaction(txn_handle, false))
            .map_err(|err| PyValueError::new_err(err.message().to_string()))?;

        response
    }

    pub fn dispatch(&self, request: PyKernelRequest) -> PyResult<PyKernelResponse> {
        let method = request.method_enum();
        let raw_path = request.path_owned();
        let (route_path, txn_id) = parse_path_and_txn_id(&raw_path)?;
        let native_state = request_body_state(request.body())?;
        let raw_body = request_body_raw_bytes(request.body())?;
        let body_is_none = native_state
            .as_ref()
            .map(|state| state.is_none())
            .unwrap_or(true);
        let bearer = py_bearer_token(&request);
        let body_bytes = if let Some(bytes) = raw_body {
            if bearer.is_none() && bytes.len() > self.config.limits.max_request_bytes_unauth {
                return Ok(PyKernelResponse::new(413, None, None));
            }
            bytes
        } else if let Some(state) = native_state.as_ref() {
            if state.is_none() {
                Vec::new()
            } else {
                let bytes = encode_state_to_bytes(state.clone())?;
                if bearer.is_none() && bytes.len() > self.config.limits.max_request_bytes_unauth {
                    return Ok(PyKernelResponse::new(413, None, None));
                }
                bytes
            }
        } else {
            Vec::new()
        };
        let token = match bearer {
            Some(token) => Some(
                self.block_on(self.inner.token_verifier().verify(token))
                    .map_err(|_| PyValueError::new_err("invalid bearer token"))?,
            ),
            None => None,
        };
        let inbound_txn_id = txn_id;
        let mut minted_txn: Option<crate::txn::TxnHandle> = None;
        let mut request = http_request_from_py_with_body(&request, body_bytes.clone())?;
        match native_state {
            Some(state) if !state.is_none() => {
                request
                    .extensions_mut()
                    .insert(crate::http::NativeStateBody::new(state));
            }
            _ => {}
        }
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
                minted_txn = Some(handle.clone());
                req.extensions_mut().insert(handle.clone());
            },
        ) {
            Ok(KernelDispatch::Response(resp)) => {
                let response =
                    self.block_on(async move { py_response_from_http(resp.await).await })?;
                if let (true, Some(txn)) = (inbound_txn_id.is_none(), minted_txn) {
                    let commit = response.status() < 400;
                    let result = self.block_on(self.inner.finalize_transaction(txn, commit));
                    if result.is_err() {
                        return Ok(PyKernelResponse::new(400, None, None));
                    }
                }

                Ok(response)
            }
            Ok(KernelDispatch::Finalize { commit, txn }) => {
                let result = self.block_on(self.inner.finalize_transaction(txn, commit));
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

    pub fn install_compiled_package(
        &self,
        package_json: &str,
        bearer_token: String,
    ) -> PyResult<PyKernelResponse> {
        let payload = decode_compiled_library_package(package_json.as_bytes())
            .map_err(|err| PyValueError::new_err(err.message().to_string()))?;
        let registry = self
            .inner
            .library_registry()
            .cloned()
            .ok_or_else(|| PyValueError::new_err("no library registry configured"))?;
        let token = self
            .block_on(self.inner.token_verifier().verify(bearer_token))
            .map_err(|_| PyValueError::new_err("invalid bearer token"))?;
        let claims = token
            .claims
            .iter()
            .map(|(_, _, claim)| claim.clone())
            .collect::<Vec<_>>();
        let txn = self
            .txn_manager
            .begin_with_owner(Some(&token.owner_id), Some(&token.bearer_token))
            .with_claims(claims);

        if !txn.has_claim(payload.schema.id(), umask::USER_WRITE) {
            return Ok(PyKernelResponse::new(401, None, None));
        }

        let status = match self.block_on(registry.stage_install_request(txn.id(), payload)) {
            Ok(_) => {
                let result = self.block_on(self.inner.finalize_transaction(txn, true));
                if result.is_ok() { 204 } else { 400 }
            }
            Err(_) => 400,
        };

        Ok(PyKernelResponse::new(status, None, None))
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
    module.add_class::<PyTensor>()?;
    module.add_class::<PyKernelRequest>()?;
    module.add_class::<PyKernelResponse>()?;
    module.add_class::<PyBackend>()
}
