use std::sync::Arc;

use pathlink::{Link, PathSegment};
use tc_error::{TCError, TCResult};
use tc_ir::{Handler, Scalar, Transact, TxnId};
use tc_value::Value;

use super::Method;
use super::types::KernelTarget;
use crate::txn::TxnServer;
type Root<T> = crate::cluster::Cluster<crate::cluster::Dir<T>>;

#[path = "resolver.rs"]
mod resolver;

pub struct HostServices {
    pub application_roots: crate::storage::ApplicationRoots,
    pub replication: Arc<dyn crate::replication::ClusterGateway>,
    pub rpc: Arc<dyn crate::gateway::RpcGateway>,
    pub resources: crate::HostResources,
    pub protocol: crate::ProtocolAuthority,
    pub verifier: Arc<dyn crate::auth::TokenVerifier>,
    pub actors: crate::auth::KeyringActorResolver,
    pub bootstrap: Arc<crate::replication::ReplicationIssuer>,
    pub bootstrap_required: bool,
}

pub(crate) struct KernelInner {
    libraries: Root<crate::library::Library>,
    classes: Root<crate::class::Class>,
    services: Root<crate::service::Service>,
    #[cfg(feature = "wasm")]
    compiler: crate::library::compiler::Compiler,
    rpc: Arc<dyn crate::gateway::RpcGateway>,
    state: tc_state::Static<crate::TxnHandle>,
    resources: crate::HostResources,
    actors: crate::auth::KeyringActorResolver,
    bootstrap: Arc<crate::replication::ReplicationIssuer>,
}

impl KernelInner {
    async fn load(
        txn_id: TxnId,
        roots: crate::storage::ApplicationRoots,
        protocol: crate::ProtocolAuthority,
        replication: Arc<dyn crate::replication::ClusterGateway>,
        rpc: Arc<dyn crate::gateway::RpcGateway>,
        resources: crate::HostResources,
        actors: crate::auth::KeyringActorResolver,
        bootstrap: Arc<crate::replication::ReplicationIssuer>,
    ) -> TCResult<Self> {
        let protocol = Arc::new(protocol);
        let (class_root, library_root, service_root) = roots.into_parts();
        let path = |root: &str| std::iter::once(root.parse().expect("application root")).collect();
        let compiler = crate::library::compiler::Compiler::new(&resources)?;
        let library_compiler = compiler.clone();
        let (classes, libraries, services) = tokio::try_join!(
            Root::<crate::class::Class>::load(
                txn_id,
                class_root,
                path("class"),
                "class",
                Arc::clone(&protocol),
                Arc::clone(&replication),
                crate::class::Class::load,
            ),
            Root::<crate::library::Library>::load(
                txn_id,
                library_root,
                path("lib"),
                "lib",
                Arc::clone(&protocol),
                Arc::clone(&replication),
                move |txn_id, storage| {
                    crate::library::Library::load(library_compiler.clone(), txn_id, storage)
                },
            ),
            Root::<crate::service::Service>::load(
                txn_id,
                service_root,
                path("service"),
                "service",
                Arc::clone(&protocol),
                Arc::clone(&replication),
                crate::service::Service::load,
            ),
        )?;

        let loaded_classes = classes.state().items(txn_id).await?;
        let definitions = loaded_classes
            .iter()
            .map(|class| (class.identity().clone(), class.definition().clone()))
            .collect::<std::collections::BTreeMap<_, _>>();
        tc_state::validate_classes(&definitions, definitions.keys().cloned())
            .map_err(|error| TCError::bad_request(error.to_string()))?;

        Ok(Self {
            libraries,
            classes,
            services,
            #[cfg(feature = "wasm")]
            compiler,
            rpc,
            state: tc_state::Static::default(),
            resources,
            actors,
            bootstrap,
        })
    }

    async fn bootstrap(
        &self,
        txn: &crate::TxnHandle,
        state: crate::State,
    ) -> TCResult<crate::State> {
        let crate::State::Tuple(payload) = state else {
            return Err(TCError::bad_request(
                "GET /host requires an encrypted nonce and ciphertext",
            ));
        };
        let [nonce, ciphertext]: [crate::State; 2] = payload.try_into().map_err(|_| {
            TCError::bad_request("GET /host requires an encrypted nonce and ciphertext")
        })?;
        let bytes = |state| match state {
            crate::State::Scalar(tc_ir::Scalar::Value(tc_value::Value::Bytes(bytes))) => Ok(bytes),
            _ => Err(TCError::bad_request(
                "GET /host encrypted message fields must be bytes",
            )),
        };
        let nonce = bytes(nonce)?;
        let ciphertext = bytes(ciphertext)?;
        let (resource, key) = self
            .bootstrap
            .open_request(txn.id(), &nonce, &ciphertext)
            .await?;
        let hash = match resource.path().first().map(PathSegment::as_str) {
            Some("class") => {
                self.classes
                    .clone()
                    .lookup(txn, &resource.path()[1..])
                    .await?
                    .exact_hash(txn)
                    .await?
            }
            Some("lib") => {
                self.libraries
                    .clone()
                    .lookup(txn, &resource.path()[1..])
                    .await?
                    .exact_hash(txn)
                    .await?
            }
            Some("service") => {
                self.services
                    .clone()
                    .lookup(txn, &resource.path()[1..])
                    .await?
                    .exact_hash(txn)
                    .await?
            }
            _ => {
                return Err(TCError::bad_request(
                    "bootstrap must name an application Cluster",
                ));
            }
        };
        self.bootstrap
            .seal_response(txn.id(), resource, hash, &key)
            .await
            .map(|(nonce, ciphertext)| {
                crate::State::Tuple(vec![
                    crate::State::from(tc_value::Value::Bytes(nonce.into())),
                    crate::State::from(tc_value::Value::Bytes(ciphertext.into())),
                ])
            })
    }

    pub(super) async fn dispatch(
        &self,
        txn: &crate::TxnHandle,
        target: &Link,
        method: Method,
        body: Option<crate::State>,
    ) -> TCResult<(Option<crate::State>, bool)> {
        match target.path().first().map(PathSegment::as_str) {
            Some("lib") => {
                self.libraries
                    .clone()
                    .dispatch(
                        txn,
                        target,
                        method,
                        body,
                        Box::new(crate::library::Root::new(
                            &self.libraries,
                            #[cfg(feature = "wasm")]
                            &self.compiler,
                        )),
                    )
                    .await
            }
            Some("class") => {
                self.classes
                    .clone()
                    .dispatch(
                        txn,
                        target,
                        method,
                        body,
                        Box::new(crate::class::Root(&self.classes)),
                    )
                    .await
            }
            Some("service") => {
                self.services
                    .clone()
                    .dispatch(
                        txn,
                        target,
                        method,
                        body,
                        Box::new(crate::service::Root(&self.services)),
                    )
                    .await
            }
            _ => Err(TCError::not_found(target.to_string())),
        }
    }

    fn metrics(&self) -> crate::State {
        self.resources.state()
    }

    fn public_key(&self, actor_id: &str) -> TCResult<crate::State> {
        self.actors.public_key_state(actor_id)
    }

    pub(crate) async fn resolve_class(
        &self,
        txn: &crate::TxnHandle,
        identity: &Link,
    ) -> TCResult<crate::cluster::Cluster<crate::class::Class>> {
        self.classes
            .clone()
            .lookup(txn, &identity.path()[1..])
            .await?
            .exact_item()?
            .ok_or_else(|| TCError::not_found(identity.to_string()))
    }

    async fn dispatch_state(
        &self,
        txn: &crate::TxnHandle,
        target: &[PathSegment],
        method: Method,
        body: Option<crate::State>,
    ) -> TCResult<Option<crate::State>> {
        let handler =
            tc_ir::Route::route(&self.state, target).ok_or_else(|| TCError::not_found("/state"))?;
        invoke_handler(handler, txn, method, body).await.map(Some)
    }

    async fn finalize(&self, cutoff: &TxnId) -> TCResult<()> {
        self.libraries.finalize(cutoff).await?;
        self.classes.finalize(cutoff).await?;
        self.services.finalize(cutoff).await
    }
}

#[derive(Clone)]
pub struct Kernel {
    txn_server: TxnServer,
    inner: std::sync::Arc<super::KernelInner>,
    bootstrap_ready: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl Kernel {
    #[cfg(feature = "http-client")]
    pub async fn bootstrap_seed(
        &self,
        seed: &str,
        self_endpoint: String,
    ) -> TCResult<std::collections::BTreeSet<String>> {
        let identity = self.inner.bootstrap.self_identity(self_endpoint)?;
        let mut peers = self.inner.classes.bootstrap(self, seed, &identity).await?;
        peers.extend(
            self.inner
                .libraries
                .bootstrap(self, seed, &identity)
                .await?,
        );
        peers.extend(self.inner.services.bootstrap(self, seed, &identity).await?);
        self.bootstrap_ready
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(peers)
    }

    #[cfg(feature = "http-client")]
    pub(crate) async fn bootstrap_resource(
        &self,
        seed: &str,
        resource: &pathlink::Link,
        identity: &crate::replication::Replica,
    ) -> TCResult<(crate::TxnHandle, crate::cluster::BootstrapSession)> {
        let request = self
            .begin_request(Method::Get, &resource.to_string(), true, None)
            .await?;
        request.execute(None).await?;
        let session = crate::replication::bootstrap_seed(
            seed,
            request.txn().id(),
            resource,
            identity,
            &self.inner.bootstrap,
        )
        .await?;
        Ok((request.txn().clone(), session))
    }

    #[cfg(feature = "http-client")]
    pub(crate) async fn bootstrap_child(
        &self,
        seed: &str,
        txn: &crate::TxnHandle,
        resource: &pathlink::Link,
        identity: &crate::replication::Replica,
    ) -> TCResult<crate::cluster::BootstrapSession> {
        crate::replication::bootstrap_seed(
            seed,
            txn.id(),
            resource,
            identity,
            &self.inner.bootstrap,
        )
        .await
    }

    #[cfg(feature = "http-client")]
    pub(crate) async fn update_bootstrap_membership(
        &self,
        txn: &crate::TxnHandle,
        target: &pathlink::Link,
        body: crate::State,
    ) -> TCResult<()> {
        self.inner
            .dispatch(txn, target, Method::Put, Some(body))
            .await
            .map(|_| ())
    }

    #[cfg(feature = "http-client")]
    pub(crate) async fn install_seed_item(
        &self,
        txn: &crate::TxnHandle,
        identity: pathlink::Link,
        value: crate::State,
    ) -> TCResult<()> {
        let root = identity
            .path()
            .first()
            .ok_or_else(|| TCError::bad_gateway("seed returned an empty application identity"))?;
        let target = format!("/{root}");
        let authority = self.txn_server.protocol_authority();
        let actor = authority.actor_id().to_string();
        let txn = txn.with_auth_context(crate::auth::AuthContext::new(actor.clone()).with_claim(
            authority.host().to_string(),
            actor,
            crate::Claim::new(identity.clone(), umask::USER_WRITE),
        ));
        let body = match value {
            crate::State::Scalar(Scalar::Value(tc_value::Value::Bytes(bytes))) => {
                crate::State::Tuple(vec![
                    crate::State::None,
                    crate::State::from(tc_value::Value::Bytes(bytes)),
                ])
            }
            value => crate::State::Tuple(vec![
                crate::State::from(tc_value::Value::Link(identity)),
                value,
            ]),
        };
        self.execute(
            KernelTarget::Application(target.parse().expect("application root")),
            txn,
            Method::Put,
            Some(body),
        )
        .await
        .map(|_| ())
    }

    pub(crate) async fn coordinate(
        &self,
        txn: &crate::TxnHandle,
        outcome: crate::txn::TransactionOutcome,
        require_mutation: bool,
    ) -> TCResult<()> {
        let Some(coordinator) = txn.lock_coordinator(require_mutation)? else {
            return Ok(());
        };
        let target = pathlink::Link::from(coordinator);
        let applied = self
            .inner
            .dispatch(
                txn,
                &target,
                if outcome.commits() {
                    Method::Put
                } else {
                    Method::Delete
                },
                None,
            )
            .await?;
        if applied.0.is_some() {
            return Err(TCError::internal(
                "resource decision returned an ordinary response",
            ));
        }
        Ok(())
    }

    pub async fn new(
        services: HostServices,
        workspace: crate::Workspace,
        ttl: std::time::Duration,
    ) -> TCResult<Self> {
        let bootstrap_ready = !services.bootstrap_required;
        let txn = crate::txn::TxnConfig::new(
            services.protocol.clone(),
            workspace,
            services.resources.clone(),
            ttl,
        );
        let txn_server = crate::txn::TxnServer::load(txn, services.verifier).await?;
        let bootstrap_txn = txn_server.allocate().await?;
        let inner = std::sync::Arc::new(
            KernelInner::load(
                bootstrap_txn,
                services.application_roots,
                services.protocol,
                services.replication,
                services.rpc,
                services.resources,
                services.actors,
                services.bootstrap,
            )
            .await?,
        );
        let kernel = Self {
            txn_server,
            inner,
            bootstrap_ready: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
                bootstrap_ready,
            )),
        };
        let finalizer = std::sync::Arc::clone(&kernel.inner);
        kernel
            .txn_server
            .start_expiry(&tokio::runtime::Handle::current(), move |cutoff| {
                let finalizer = std::sync::Arc::clone(&finalizer);
                async move { finalizer.finalize(&cutoff).await }
            });
        Ok(kernel)
    }

    pub(super) async fn execute(
        &self,
        target: KernelTarget,
        txn: crate::TxnHandle,
        method: Method,
        body: Option<crate::State>,
    ) -> TCResult<(Option<crate::State>, bool)> {
        match &target {
            KernelTarget::Application(application) => self
                .inner
                .dispatch(&txn, application, method, body)
                .await
                .map(|(state, exact_item)| {
                    let raw_bytes = exact_item
                        && crate::uri::application_root(application) == Some("lib")
                        && matches!(
                            &state,
                            Some(crate::State::Scalar(Scalar::Value(Value::Bytes(_))))
                        );
                    (state, raw_bytes)
                }),
            _ if txn.is_locked() => Err(TCError::conflict(
                "a transaction decision must target an exact transactional resource",
            )),
            KernelTarget::State(state_target) => self
                .inner
                .dispatch_state(&txn, state_target, method, body)
                .await
                .map(|state| (state, false)),
            KernelTarget::AuthContext => {
                if method != Method::Get {
                    return Err(tc_error::TCError::method_not_allowed(
                        method.as_str(),
                        crate::uri::HOST_AUTH_CONTEXT,
                    ));
                }
                crate::host::auth_context(&txn).map(|state| (Some(state), false))
            }
            KernelTarget::Host => {
                if method != Method::Get {
                    return Err(TCError::method_not_allowed(method, crate::uri::HOST_ROOT));
                }
                let body = body.ok_or_else(|| TCError::bad_request("GET /host requires a body"))?;
                self.inner
                    .bootstrap(&txn, body)
                    .await
                    .map(|state| (Some(state), false))
            }
        }
    }

    pub(crate) fn deadline(&self) -> crate::Deadline {
        self.txn_server.resources().deadline()
    }

    #[cfg(feature = "http-server")]
    pub(crate) async fn admit_host_request(
        &self,
    ) -> TCResult<(crate::Deadline, crate::resources::CapacityPermit)> {
        let deadline = self.deadline();
        let permit = self.txn_server.resources().admit_request(deadline).await?;
        Ok((deadline, permit))
    }

    #[cfg(feature = "http-server")]
    pub(crate) async fn admit_connection(&self) -> TCResult<crate::resources::CapacityPermit> {
        let deadline = self.deadline();
        self.txn_server.resources().admit_connection(deadline).await
    }

    #[cfg(test)]
    pub(crate) async fn test_txn(&self) -> crate::TxnHandle {
        self.txn_server
            .bind(None, None, std::sync::Arc::clone(&self.inner))
            .await
            .expect("begin test transaction")
    }

    #[cfg(test)]
    pub(crate) fn test_txn_server(&self) -> &TxnServer {
        &self.txn_server
    }

    #[cfg(test)]
    pub(crate) fn test_libraries(&self) -> Root<crate::library::Library> {
        self.inner.libraries.clone()
    }

    #[cfg(test)]
    pub(crate) fn test_classes(&self) -> Root<crate::class::Class> {
        self.inner.classes.clone()
    }

    #[cfg(test)]
    pub(crate) fn test_services(&self) -> Root<crate::service::Service> {
        self.inner.services.clone()
    }

    #[cfg(test)]
    pub(crate) async fn test_bind(
        &self,
        txn_id: Option<TxnId>,
        context: Option<&crate::auth::AuthContext>,
    ) -> TCResult<crate::TxnHandle> {
        self.txn_server
            .bind(txn_id, context, std::sync::Arc::clone(&self.inner))
            .await
    }

    pub fn is_ready(&self) -> bool {
        self.txn_server.is_ready()
            && self
                .bootstrap_ready
                .load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn health(&self, method: Method) -> TCResult<crate::State> {
        if method != Method::Get {
            return Err(TCError::method_not_allowed(method, crate::uri::HOST_HEALTH));
        }
        if !self.is_ready() {
            return Err(TCError::new(
                tc_error::ErrorKind::Unavailable,
                "host is not ready",
            ));
        }
        use tc_ir::{Id, Map};

        Ok(crate::State::Map(Map::from_iter([
            (
                "status".parse::<Id>().expect("health field"),
                crate::State::from(tc_value::Value::from("ok")),
            ),
            (
                "resources".parse::<Id>().expect("health field"),
                self.inner.metrics(),
            ),
        ])))
    }

    pub fn metrics(&self, method: Method) -> TCResult<crate::State> {
        (method == Method::Get)
            .then(|| self.inner.metrics())
            .ok_or_else(|| TCError::method_not_allowed(method, crate::uri::HOST_METRICS))
    }

    pub fn public_key(&self, method: Method, actor_id: &str) -> TCResult<crate::State> {
        if method != Method::Get {
            return Err(TCError::method_not_allowed(
                method,
                crate::uri::HOST_PUBLIC_KEY,
            ));
        }
        self.inner.public_key(actor_id)
    }

    pub async fn begin_request(
        &self,
        method: Method,
        raw_path: &str,
        body_is_none: bool,
        bearer: Option<String>,
    ) -> TCResult<Box<super::KernelRequestGuard>> {
        let deadline = self.deadline();
        let (path, txn_id) = crate::txn::wire::split_path_and_txn_id(raw_path)?;
        let link: pathlink::Link = path
            .parse()
            .map_err(|error| TCError::bad_request(format!("invalid request target: {error}")))?;
        let target = match link.path().first() {
            Some(_) if crate::uri::application_root(&link).is_some() => {
                KernelTarget::Application(link)
            }
            _ if path == crate::uri::HOST_AUTH_CONTEXT => KernelTarget::AuthContext,
            _ if path == crate::uri::HOST_ROOT => KernelTarget::Host,
            _ if link
                .path()
                .first()
                .is_some_and(|root| root.as_str() == "state") =>
            {
                KernelTarget::State(link.path()[1..].into())
            }
            _ => return Err(TCError::not_found(link.to_string())),
        };
        let permit = self.txn_server.resources().admit_request(deadline).await?;
        let token = match bearer {
            Some(bearer) => Some(
                deadline
                    .wait(self.txn_server.verifier().verify(bearer))
                    .await?
                    .map_err(|_| tc_error::TCError::unauthorized("invalid bearer token"))?,
            ),
            None => None,
        };
        let txn = if matches!(target, KernelTarget::Host) && token.is_none() {
            let txn_id = txn_id.ok_or_else(|| {
                TCError::bad_request("GET /host bootstrap requires the original txn_id")
            })?;
            self.txn_server
                .bind_seed(txn_id, std::sync::Arc::clone(&self.inner))?
        } else {
            self.txn_server
                .bind(txn_id, token.as_ref(), std::sync::Arc::clone(&self.inner))
                .await?
        }
        .restrict_deadline(deadline);
        if body_is_none && matches!(method, Method::Put | Method::Delete) && !txn.is_locked() {
            return Err(TCError::bad_request(
                "an ordinary PUT or DELETE request requires an explicit body",
            ));
        }
        Ok(Box::new(super::KernelRequestGuard::new(
            self.clone(),
            method,
            target,
            txn,
            permit,
        )))
    }
}

pub(crate) async fn invoke_handler<'handler, 'txn>(
    handler: Box<dyn Handler<'handler, crate::State> + 'handler>,
    txn: &'txn crate::TxnHandle,
    method: Method,
    body: Option<crate::State>,
) -> TCResult<crate::State>
where
    'txn: 'handler,
{
    match method {
        Method::Get => {
            handler
                .get()
                .ok_or_else(|| TCError::method_not_allowed(method, "native handler"))?(
                txn,
                scalar_body(body)?,
            )
            .await
        }
        Method::Put => {
            let (key, value) = put_body(body)?;
            handler
                .put()
                .ok_or_else(|| TCError::method_not_allowed(method, "native handler"))?(
                txn, key, value,
            )
            .await?;
            Ok(crate::State::default())
        }
        Method::Post => {
            let Some(crate::State::Map(params)) = body else {
                return Err(TCError::bad_request("POST route requires a map request"));
            };
            handler
                .post()
                .ok_or_else(|| TCError::method_not_allowed(method, "native handler"))?(
                txn, params
            )
            .await
        }
        Method::Delete => {
            handler
                .delete()
                .ok_or_else(|| TCError::method_not_allowed(method, "native handler"))?(
                txn,
                scalar_body(body)?,
            )
            .await?;
            Ok(crate::State::default())
        }
    }
}

#[cfg(test)]
#[path = "../../tests/support/kernel.rs"]
mod tests;

fn scalar_body(body: Option<crate::State>) -> TCResult<Scalar> {
    match body.unwrap_or(crate::State::None) {
        crate::State::None => Ok(Scalar::default()),
        crate::State::Scalar(scalar) => Ok(scalar),
        _ => Err(tc_error::TCError::bad_request("expected a scalar request")),
    }
}

fn put_body(body: Option<crate::State>) -> TCResult<(Scalar, crate::State)> {
    let Some(crate::State::Tuple(mut values)) = body else {
        return Err(tc_error::TCError::bad_request(
            "PUT route requires a [key, value] tuple",
        ));
    };
    if values.len() != 2 {
        return Err(tc_error::TCError::bad_request(
            "PUT route requires a [key, value] tuple",
        ));
    }
    let value = values.pop().expect("tuple length checked");
    let key = scalar_body(Some(values.pop().expect("tuple length checked")))?;
    Ok((key, value))
}
