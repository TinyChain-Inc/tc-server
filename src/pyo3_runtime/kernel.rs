use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use pathlink::Link;
use pyo3::Bound;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyModule, PyType};
use umask;

use super::state_handle_conversions::{request_body_bytes, state_from_handle};
use super::types::{
    PyKernelConfig, PyKernelRequest, PyResponse, PyStateHandle, apply_config_overrides,
};
use super::wire::{parse_path_and_txn_id, py_bearer_token};
use crate::kernel::BoundTransaction;
use crate::library::{
    decode_compiled_library_package, decode_install_request_bytes, http as http_library,
};
use crate::storage::HostStorage;
use crate::{Kernel, KernelRequest};

fn parse_rjwt_alg(alg: &str) -> PyResult<rjwt::AlgKind> {
    match alg.trim().to_ascii_lowercase().as_str() {
        "falcon512" | "falcon-512" | "fn-dsa-512" => Ok(rjwt::AlgKind::Falcon512),
        "ed25519" | "eddsa" => Ok(rjwt::AlgKind::Ed25519),
        other => Err(PyValueError::new_err(format!(
            "unsupported signature algorithm: {other}"
        ))),
    }
}

fn python_kernel_builder_with_config(
    lib: Py<PyAny>,
    service: Py<PyAny>,
    metrics: Option<Py<PyAny>>,
    config: PyKernelConfig,
    runtime: &tokio::runtime::Runtime,
) -> Kernel {
    let _ = lib; // /lib is managed by the Rust kernel.
    let data_dir = config.data_dir.clone();
    let initial_schema = config.initial_schema.clone();
    let storage_limits = config.limits.storage.clone();
    let (storage, module) = super::wait(runtime, async move {
        let storage = HostStorage::new(&storage_limits);
        let module = match data_dir.as_ref() {
            Some(root) => {
                let store = storage.library_store(root).await.expect("library store");
                http_library::build_http_library_module_with_store(initial_schema, Some(store))
                    .await
                    .expect("library module")
            }
            None => http_library::build_http_library_module(initial_schema, None)
                .await
                .expect("library module"),
        };
        module
            .hydrate_from_storage()
            .await
            .expect("library hydrate");
        (storage, module)
    });

    let _ = (lib, service, metrics);

    let resources = crate::HostResources::new(config.limits.clone());
    let mut builder = Kernel::builder()
        .with_resources(resources)
        .with_host_id(config.host_id.clone())
        .with_library_module(module)
        .with_txn_ttl(config.limits.transaction_ttl);

    if let Some(workspace) = config.workspace.as_ref() {
        builder = builder.with_workspace(storage.workspace(workspace).expect("Python workspace"));
    }

    #[cfg(feature = "http-client")]
    {
        builder = builder.with_http_rpc_gateway();
    }

    builder.finish()
}

fn stub_py_handler() -> Py<PyAny> {
    Python::with_gil(|py| py.None())
}

#[pyclass(name = "KernelHandle")]
pub struct KernelHandle {
    inner: Arc<Kernel>,
    runtime: Arc<tokio::runtime::Runtime>,
    config: PyKernelConfig,
}

impl Clone for KernelHandle {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            runtime: Arc::clone(&self.runtime),
            config: self.config.clone(),
        }
    }
}

impl KernelHandle {
    fn from_kernel(
        kernel: Kernel,
        config: PyKernelConfig,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        kernel.start_transaction_expiry(runtime.handle());
        Self {
            inner: Arc::new(kernel),
            runtime,
            config,
        }
    }

    fn wait<F>(&self, fut: F) -> F::Output
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        super::wait(&self.runtime, fut)
    }
}

#[pymethods]
impl KernelHandle {
    #[new]
    #[pyo3(signature = (lib, service, metrics=None, data_dir=None, workspace=None, request_ttl_secs=None, max_request_bytes_unauth=None))]
    pub fn new(
        lib: Py<PyAny>,
        service: Py<PyAny>,
        metrics: Option<Py<PyAny>>,
        data_dir: Option<PathBuf>,
        workspace: Option<PathBuf>,
        request_ttl_secs: Option<u64>,
        max_request_bytes_unauth: Option<usize>,
    ) -> Self {
        let config = PyKernelConfig {
            data_dir,
            workspace,
            ..PyKernelConfig::default()
        };
        let config = apply_config_overrides(config, request_ttl_secs, max_request_bytes_unauth);
        let runtime = super::runtime().expect("tokio runtime");
        let kernel =
            python_kernel_builder_with_config(lib, service, metrics, config.clone(), &runtime);
        Self::from_kernel(kernel, config, runtime)
    }

    /// Construct a local kernel handle with no Python service handlers
    /// installed.
    ///
    /// This is intended for tooling/tests which only need the Rust `/lib` and
    /// `/healthz` handlers (e.g. WASM installs into a local `data_dir`)
    /// without providing Python callbacks.
    #[classmethod]
    #[pyo3(signature = (data_dir=None, workspace=None, request_ttl_secs=None, max_request_bytes_unauth=None))]
    pub fn local(
        _cls: &Bound<'_, PyType>,
        data_dir: Option<PathBuf>,
        workspace: Option<PathBuf>,
        request_ttl_secs: Option<u64>,
        max_request_bytes_unauth: Option<usize>,
    ) -> Self {
        let stub = stub_py_handler();
        Self::new(
            stub.clone(),
            stub,
            None,
            data_dir,
            workspace,
            request_ttl_secs,
            max_request_bytes_unauth,
        )
    }

    #[classmethod]
    #[pyo3(signature = (definition_json, routes=None, token=None, data_dir=None, workspace=None, request_ttl_secs=None, max_request_bytes_unauth=None))]
    #[allow(clippy::too_many_arguments)]
    pub fn with_library_definition(
        _cls: &Bound<'_, PyType>,
        definition_json: &str,
        routes: Option<Vec<(String, String)>>,
        token: Option<&Bound<'_, PyAny>>,
        data_dir: Option<PathBuf>,
        workspace: Option<PathBuf>,
        request_ttl_secs: Option<u64>,
        max_request_bytes_unauth: Option<usize>,
    ) -> PyResult<Self> {
        use std::str::FromStr;

        let runtime = super::runtime().expect("tokio runtime");
        let package = decode_install_request_bytes(definition_json.as_bytes())
            .map_err(|err| PyValueError::new_err(err.message().to_string()))?;
        let storage_data_dir = data_dir.clone();
        let (storage, store) = super::wait(&runtime, async move {
            let storage = HostStorage::new(&crate::HostLimits::default().storage);
            let store = match storage_data_dir.as_ref() {
                Some(path) => Some(storage.library_store(path).await?),
                None => None,
            };
            Ok::<_, tc_error::TCError>((storage, store))
        })
        .map_err(|err| PyValueError::new_err(err.message().to_string()))?;
        let module = super::wait(&runtime, async move {
            let module =
                http_library::build_http_library_module_with_store(package.schema.clone(), store)
                    .await?;
            module
                .install_compiled_package(package)
                .await
                .map_err(|err| tc_error::TCError::bad_request(err.message().to_string()))?;
            module.hydrate_from_storage().await?;
            Ok::<_, tc_error::TCError>(module)
        })
        .map_err(|err| PyValueError::new_err(err.message().to_string()))?;
        let config = PyKernelConfig {
            data_dir,
            workspace,
            ..PyKernelConfig::default()
        };
        let config = apply_config_overrides(config, request_ttl_secs, max_request_bytes_unauth);
        let resources = crate::HostResources::new(config.limits.clone());
        let mut builder = Kernel::builder()
            .with_resources(resources)
            .with_host_id("tc-py-kernel")
            .with_library_module(module)
            .with_txn_ttl(config.limits.transaction_ttl);

        if let Some(workspace) = config.workspace.as_ref() {
            let workspace = storage
                .workspace(workspace)
                .map_err(|err| PyValueError::new_err(err.message().to_string()))?;
            builder = builder.with_workspace(workspace);
        }

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
            let actor = if let Some(secret_key_b64) = secret_key_b64 {
                let key_bytes = STANDARD
                    .decode(secret_key_b64.as_bytes())
                    .map_err(|_| PyValueError::new_err("invalid secret key base64"))?;
                let alg = parse_rjwt_alg(&alg)?;
                let signing_key = rjwt::SigningKey::from_bytes(alg, key_bytes.as_slice())
                    .map_err(|_| PyValueError::new_err("invalid secret key"))?;
                crate::auth::Actor::with_signing_key(actor_id, signing_key)
            } else {
                let verifying_key = crate::auth::verifying_key_from_bytes(key_bytes.as_slice())
                    .map_err(|_| PyValueError::new_err("invalid public key"))?;
                crate::auth::Actor::with_verifying_key(actor_id, verifying_key)
            };
            let keyring = crate::auth::KeyringActorResolver::default()
                .with_actor(host.clone(), actor.clone());
            builder = builder.with_rjwt_keyring_token_verifier(keyring);
            builder = builder.with_protocol_actor(host, actor);
        }

        Ok(Self::from_kernel(builder.finish(), config, runtime))
    }

    #[pyo3(signature = (path, body=None, token=None))]
    pub fn resolve_get(
        &self,
        path: &str,
        body: Option<PyStateHandle>,
        token: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyResponse> {
        if !crate::uri::component_root(path).is_some_and(|root| {
            root == path
                && !matches!(
                    root,
                    crate::uri::LIB_ROOT | crate::uri::SERVICE_ROOT | crate::uri::HOST_ROOT
                )
        }) {
            return Err(PyValueError::new_err(
                "resolve_get accepts a component root, not an arbitrary route path",
            ));
        }

        let headers = token
            .map(|token| {
                Ok::<_, PyErr>(vec![(
                    "authorization".to_string(),
                    format!(
                        "Bearer {}",
                        token.getattr("bearer_token")?.extract::<String>()?
                    ),
                )])
            })
            .transpose()?;
        self.dispatch(PyKernelRequest::new("GET", path, headers, body)?)
    }

    pub fn dispatch(&self, request: PyKernelRequest) -> PyResult<PyResponse> {
        let resources = self.inner.resources().clone();
        let deadline = resources.deadline();
        let request_permit = Arc::new(
            self.wait(async move { resources.admit_request(deadline).await })
                .map_err(super::tc_error)?,
        );
        let method = request.method_enum();
        let raw_path = request.path_owned();
        let (route_path, txn_id) = parse_path_and_txn_id(&raw_path)?;
        let body_is_none = request.body().is_none();
        let bearer = py_bearer_token(&request);
        let token = match bearer {
            Some(token) => Some(
                self.wait(self.inner.token_verifier().verify(token))
                    .map_err(|_| PyValueError::new_err("invalid bearer token"))?,
            ),
            None => None,
        };
        let kernel = Arc::clone(&self.inner);
        let bind_path = route_path.clone();
        let binding = self.wait(async move {
            kernel
                .bind_transaction(
                    method,
                    &bind_path,
                    body_is_none,
                    txn_id,
                    token.as_ref(),
                    deadline,
                )
                .await
        });
        match binding {
            Ok(None) => Ok(PyResponse::new(204, None, None)),
            Ok(Some(BoundTransaction { txn, implicit })) => {
                if method == crate::Method::Get
                    && (route_path == crate::uri::LIB_ROOT
                        || self
                            .inner
                            .library_registry()
                            .and_then(|registry| registry.resolve_runtime_for_path(&route_path))
                            .is_some_and(|(_, is_root)| is_root))
                {
                    let registry = self
                        .inner
                        .library_registry()
                        .ok_or_else(|| PyValueError::new_err("no library registry configured"))?;
                    let state = if route_path == crate::uri::LIB_ROOT {
                        registry
                            .list_dir(crate::uri::LIB_ROOT)
                            .map(crate::library::view::listing)
                    } else {
                        registry
                            .resolve_runtime_for_path(&route_path)
                            .filter(|(_, is_root)| *is_root)
                            .map(|(runtime, _)| {
                                crate::library::view::schema(&runtime.state.schema())
                            })
                    };
                    let Some(state) = state else {
                        return Ok(PyResponse::new(404, None, None));
                    };
                    let body = if implicit {
                        PyStateHandle::from_terminal_state(
                            state,
                            Arc::clone(&self.inner),
                            txn,
                            Arc::clone(&self.runtime),
                            Arc::clone(&request_permit),
                        )
                    } else {
                        PyStateHandle::from_state(state, txn, Arc::clone(&self.runtime))
                    };
                    return Ok(PyResponse::new(200, None, Some(body)));
                }

                if method == crate::Method::Get
                    && (route_path == crate::uri::CLASS_ROOT
                        || route_path.starts_with(crate::uri::CLASS_ROOT_PREFIX))
                    && !self
                        .inner
                        .library_registry()
                        .is_some_and(|registry| registry.has_class(&route_path))
                {
                    let registry = self
                        .inner
                        .library_registry()
                        .ok_or_else(|| PyValueError::new_err("no Class registry configured"))?;
                    let Some(state) = registry
                        .list_class_dir(&route_path)
                        .map(crate::library::view::listing)
                    else {
                        return Ok(PyResponse::new(404, None, None));
                    };
                    let body = if implicit {
                        PyStateHandle::from_terminal_state(
                            state,
                            Arc::clone(&self.inner),
                            txn,
                            Arc::clone(&self.runtime),
                            Arc::clone(&request_permit),
                        )
                    } else {
                        PyStateHandle::from_state(state, txn, Arc::clone(&self.runtime))
                    };
                    return Ok(PyResponse::new(200, None, Some(body)));
                }

                if route_path == crate::uri::LIB_ROOT && method == crate::Method::Put {
                    let registry =
                        self.inner.library_registry().cloned().ok_or_else(|| {
                            PyValueError::new_err("no library registry configured")
                        })?;
                    let bytes = request_body_bytes(request.body())?;
                    let install_txn = txn.clone();
                    let result = self.wait(async move {
                        crate::library::decode_authorize_and_stage_install(
                            &registry,
                            &install_txn,
                            &bytes,
                        )
                        .await
                    });
                    let success = result.is_ok();
                    if implicit {
                        let kernel = Arc::clone(&self.inner);
                        let outcome = crate::txn::TransactionOutcome::from_success(success);
                        self.wait(async move { kernel.complete_transaction(txn, outcome).await })
                            .map_err(|err| PyValueError::new_err(err.to_string()))?;
                    }
                    return Ok(PyResponse::new(if success { 204 } else { 400 }, None, None));
                }

                let body = state_from_handle(request.body(), txn.clone(), &self.runtime)?;
                let path = Link::from_str(&route_path)
                    .map_err(|err| PyValueError::new_err(err.to_string()))?;
                let native_request = KernelRequest {
                    method,
                    path,
                    body,
                    txn: txn.clone(),
                };
                let transport_artifact = self
                    .inner
                    .library_registry()
                    .and_then(|registry| registry.resolve_runtime_for_path(&route_path))
                    .filter(|(runtime, is_root)| {
                        !*is_root
                            && runtime.execution().is_some_and(|execution| {
                                matches!(execution, crate::library::LibraryExecution::Transport)
                            })
                    })
                    .and_then(|(runtime, _)| runtime.artifact())
                    .filter(|artifact| {
                        artifact.content_type == crate::ir::WASM_ARTIFACT_CONTENT_TYPE
                    });
                let result = match transport_artifact {
                    Some(artifact) => {
                        let bytes = artifact.bytes.clone();
                        self.wait(async move {
                            crate::wasm::execute_artifact(&bytes, native_request).await
                        })
                    }
                    None => {
                        let kernel = Arc::clone(&self.inner);
                        self.wait(async move { kernel.execute(native_request).await })
                    }
                };
                let (response, deferred_finalize) = match result {
                    Ok(state) => {
                        let body = if implicit {
                            PyStateHandle::from_terminal_state(
                                state,
                                Arc::clone(&self.inner),
                                txn.clone(),
                                Arc::clone(&self.runtime),
                                Arc::clone(&request_permit),
                            )
                        } else {
                            PyStateHandle::from_state(state, txn.clone(), Arc::clone(&self.runtime))
                        };
                        (PyResponse::new(200, None, Some(body)), implicit)
                    }
                    Err(err) => {
                        if implicit {
                            let kernel = Arc::clone(&self.inner);
                            self.wait(async move {
                                kernel
                                    .complete_transaction(
                                        txn,
                                        crate::txn::TransactionOutcome::Failed,
                                    )
                                    .await
                            })
                            .map_err(super::tc_error)?;
                        }
                        return Err(super::tc_error(err));
                    }
                };
                let finalize_failed = if implicit && !deferred_finalize {
                    let kernel = Arc::clone(&self.inner);
                    self.wait(async move {
                        kernel
                            .complete_transaction(txn, crate::txn::TransactionOutcome::Succeeded)
                            .await
                    })
                    .is_err()
                } else {
                    false
                };
                if finalize_failed {
                    return Ok(PyResponse::new(400, None, None));
                }
                Ok(response)
            }
            Err(err) => Err(super::tc_error(err)),
        }
    }

    pub fn install_compiled_package(
        &self,
        package_json: &str,
        bearer_token: String,
    ) -> PyResult<PyResponse> {
        let payload = decode_compiled_library_package(package_json.as_bytes())
            .map_err(|err| PyValueError::new_err(err.message().to_string()))?;
        let registry = self
            .inner
            .library_registry()
            .cloned()
            .ok_or_else(|| PyValueError::new_err("no library registry configured"))?;
        let token = self
            .wait(self.inner.token_verifier().verify(bearer_token))
            .map_err(|_| PyValueError::new_err("invalid bearer token"))?;
        let kernel = Arc::clone(&self.inner);
        let deadline = kernel.resources().deadline();
        let txn = match self.wait(async move {
            kernel
                .bind_transaction(
                    crate::Method::Put,
                    crate::uri::LIB_ROOT,
                    false,
                    None,
                    Some(&token),
                    deadline,
                )
                .await
        }) {
            Ok(Some(BoundTransaction { txn, .. })) => txn,
            Ok(None) => {
                return Err(PyValueError::new_err("unexpected transaction finalization"));
            }
            Err(err) => return Err(PyValueError::new_err(format!("transaction error: {err:?}"))),
        };

        if !txn.has_claim(payload.schema.id(), umask::USER_WRITE) {
            let kernel = Arc::clone(&self.inner);
            self.wait(async move {
                kernel
                    .complete_transaction(txn, crate::txn::TransactionOutcome::Failed)
                    .await
            })
            .map_err(super::tc_error)?;
            return Ok(PyResponse::new(401, None, None));
        }

        let txn_id = txn.id();
        let status =
            match self.wait(async move { registry.stage_install_request(txn_id, payload).await }) {
                Ok(_) => {
                    let kernel = Arc::clone(&self.inner);
                    let result = self.wait(async move {
                        kernel
                            .complete_transaction(txn, crate::txn::TransactionOutcome::Succeeded)
                            .await
                    });
                    if result.is_ok() { 204 } else { 400 }
                }
                Err(_) => {
                    let kernel = Arc::clone(&self.inner);
                    let result = self.wait(async move {
                        kernel
                            .complete_transaction(txn, crate::txn::TransactionOutcome::Failed)
                            .await
                    });
                    if result.is_ok() { 400 } else { 500 }
                }
            };

        Ok(PyResponse::new(status, None, None))
    }
}

#[pyclass(name = "Backend")]
pub struct PyBackend {
    _kernel: KernelHandle,
}

#[pymethods]
impl PyBackend {
    #[new]
    #[pyo3(signature = (lib, service, metrics=None, data_dir=None, workspace=None, request_ttl_secs=None, max_request_bytes_unauth=None))]
    pub fn new(
        lib: Py<PyAny>,
        service: Py<PyAny>,
        metrics: Option<Py<PyAny>>,
        data_dir: Option<PathBuf>,
        workspace: Option<PathBuf>,
        request_ttl_secs: Option<u64>,
        max_request_bytes_unauth: Option<usize>,
    ) -> Self {
        Self {
            _kernel: KernelHandle::new(
                lib,
                service,
                metrics,
                data_dir,
                workspace,
                request_ttl_secs,
                max_request_bytes_unauth,
            ),
        }
    }

    pub fn healthz(&self) -> PyResult<()> {
        // Health reports adapter availability; it is not a native state route.
        Ok(())
    }
}

pub fn register_python_api(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<KernelHandle>()?;
    module.add_class::<PyStateHandle>()?;
    module.add_class::<PyKernelRequest>()?;
    module.add_class::<PyResponse>()?;
    module.add_class::<PyBackend>()
}
