use std::sync::Arc;

use pathlink::{Link, PathSegment};
use safecast::TryCastFrom;
use tc_error::{TCError, TCResult};
use tc_ir::{Handler, Scalar, Transact, TxnId};

use crate::txn::TxnServer;
use super::Method;
use super::types::KernelTarget;

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
    async fn load(txn_id: TxnId, services: HostServices) -> TCResult<Self> {
        let HostServices {
            application_roots: roots,
            protocol,
            replication,
            rpc,
            resources,
            actors,
            bootstrap,
            ..
        } = services;
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
                {
                    let capacity = resources.limits().execution.max_op_invocations;
                    move |id, storage| crate::service::Service::load(id, storage, capacity)
                },
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

    pub(crate) fn register_peer_identity(
        &self,
        replica: &crate::replication::Replica,
    ) -> TCResult<()> {
        self.bootstrap.register_peer_identity(replica)
    }

    async fn bootstrap(
        &self,
        txn: &crate::TxnHandle,
        state: crate::State,
    ) -> TCResult<crate::State> {
        let tc_state::State::Tuple(payload) = state else {
            return Err(TCError::bad_request(
                "GET /host requires an encrypted nonce and ciphertext",
            ));
        };

        let [nonce, ciphertext]: [crate::State; 2] = payload.try_into().map_err(|_| {
            TCError::bad_request("GET /host requires an encrypted nonce and ciphertext")
        })?;

        let bytes = |state| match tc_value::Value::opt_cast_from(state) {
            Some(tc_value::Value::Bytes(bytes)) => Ok(bytes),
            _ => Err(TCError::bad_request(
                "GET /host encrypted message fields must be bytes",
            )),
        };

        let nonce = bytes(nonce)?;
        let ciphertext = bytes(ciphertext)?;
        let (resource, key, token, peer) = self
            .bootstrap
            .open_request(txn.id(), &nonce, &ciphertext)
            .await?;

        let txn = txn.adopt_claims(&token).await?;
        let claims = txn.protocol_snapshot().expect("verified bootstrap owner");
        if claims.owner != Some((peer.host, peer.actor_id))
            || claims.locked
            || !claims.leaders.is_empty()
            || txn
                .auth_context()
                .expect("verified bootstrap token")
                .claims
                .len()
                != 1
        {
            return Err(TCError::unauthorized(
                "bootstrap requires the joining host's ownership claim",
            ));
        }

        let hash = match resource.path().first().map(PathSegment::as_str) {
            Some("class") => {
                self.classes
                    .clone()
                    .lookup(&txn, &resource.path()[1..])
                    .await?
                    .exact_hash(&txn)
                    .await?
            }
            Some("lib") => {
                self.libraries
                    .clone()
                    .lookup(&txn, &resource.path()[1..])
                    .await?
                    .exact_hash(&txn)
                    .await?
            }
            Some("service") => {
                self.services
                    .clone()
                    .lookup(&txn, &resource.path()[1..])
                    .await?
                    .exact_hash(&txn)
                    .await?
            }
            _ => {
                return Err(TCError::bad_request(
                    "bootstrap must name an application Cluster",
                ));
            }
        };

        self.bootstrap
            .seal_response(
                txn.raw_token().expect("claimed bootstrap resource"),
                hash,
                &key,
            )
            .await
            .map(|(nonce, ciphertext)| {
                crate::State::from(vec![
                    crate::State::from(tc_value::Value::from(nonce)),
                    crate::State::from(tc_value::Value::from(ciphertext)),
                ])
            })
    }

    pub(crate) async fn dispatch(
        &self,
        txn: &crate::TxnHandle,
        target: &Link,
        method: Method,
        body: Option<crate::State>,
    ) -> TCResult<Option<crate::State>> {
        txn.check_active()?;
        match target.path().first().map(PathSegment::as_str) {
            Some("lib") => {
                self.libraries
                    .clone()
                    .dispatch(
                        txn,
                        target,
                        method,
                        body,
                        crate::library::Root::new(
                            &self.libraries,
                            #[cfg(feature = "wasm")]
                            &self.compiler,
                        ),
                    )
                    .await
            }
            Some("class") => {
                self.classes
                    .clone()
                    .dispatch(txn, target, method, body, crate::class::Root(&self.classes))
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
                        crate::service::Root(&self.services),
                    )
                    .await
            }
            _ => Err(TCError::not_found(target.to_string())),
        }
        .map_err(|error| txn.storage_error(error))
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
    drain: std::sync::Arc<super::drain::Drain>,
    inner: std::sync::Arc<super::KernelInner>,
    bootstrap_ready: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl Kernel {
    /// Permanently withdraw readiness and reject new request admission.
    pub fn begin_shutdown(&self) {
        self.drain.begin(|| self.deadline());
    }

    pub(crate) fn fatal_recovery(&self, error: impl std::fmt::Display) -> ! {
        self.begin_shutdown();
        self.txn_server
            .fatal_recovery(format_args!("transaction recovery required: {error}"))
    }

    /// Drain admitted requests and join finalization before dropping the runtime.
    /// This does not force unexpired transactions into canonical storage. An active
    /// finalization is never cancelled; the caller must keep polling this future.
    /// Concurrent calls and a replacement waiter after cancellation are supported.
    pub async fn shutdown(&self) {
        self.begin_shutdown();
        self.drain.wait().await;
        self.txn_server.stop_expiry().await;
    }

    #[cfg(feature = "http-server")]
    pub(crate) async fn shutdown_started(&self) -> crate::Deadline {
        self.drain.stopped().await
    }

    #[cfg(feature = "http-client")]
    pub async fn bootstrap_seed(
        &self,
        seed: &str,
        self_endpoint: String,
    ) -> TCResult<std::collections::BTreeSet<String>> {
        let _request = self.drain.admit()?;
        // Startup is gated by bootstrap_required until a seed succeeds. Later
        // discovery candidates may be stale; failure must not erase that success.
        // Transaction storage readiness is checked independently by is_ready.
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
    pub(crate) async fn bootstrap_probe(
        &self,
        seed: &str,
        resource: &pathlink::Link,
        identity: &crate::replication::Replica,
    ) -> TCResult<(
        Box<crate::BoundRequest>,
        crate::TxnHandle,
        crate::cluster::BootstrapSession,
    )> {
        let request = self
            .begin_request(Method::Get, &resource.to_string(), true, None)
            .await?;

        request.txn().claim_owner()?;
        let session = self
            .bootstrap_child(seed, request.txn(), resource, identity)
            .await?;

        let txn = request.txn().adopt_claims(session.token()).await?;
        let peer = session.replica();
        if txn.is_locked()
            || txn.leader(resource.path()) != Some((peer.host.clone(), peer.actor_id.clone()))
        {
            return Err(TCError::unauthorized(
                "bootstrap resource leader does not match its seed",
            ));
        }

        Ok((request, txn, session))
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
            txn.deadline(),
        )
        .await
    }

    #[cfg(feature = "http-client")]
    pub(crate) async fn update_bootstrap_membership(
        &self,
        txn: &crate::TxnHandle,
        target: &pathlink::Link,
        replica: &crate::replication::Replica,
        state_hash: &str,
    ) -> TCResult<()> {
        let body = crate::State::from(vec![
            crate::State::from(tc_value::Value::from(replica.endpoint.clone())),
            crate::cluster::replica_put_state(replica, state_hash.to_string()),
        ]);

        self.inner
            .dispatch(txn, target, Method::Put, Some(body))
            .await?;

        Ok(())
    }

    #[cfg(feature = "http-client")]
    pub(crate) async fn reconcile_bootstrap_membership(
        &self,
        target: &pathlink::Link,
        replicas: &[crate::replication::Replica],
        state_hash: &str,
    ) -> TCResult<()> {
        let request = self
            .begin_request(Method::Put, &target.to_string(), false, None)
            .await?;

        request.txn().claim_owner()?;
        let txn = request
            .txn()
            .grant_claim(crate::Claim::new(target.clone(), umask::USER_WRITE))
            .await?;

        for replica in replicas {
            self.update_bootstrap_membership(&txn, target, replica, state_hash)
                .await?;
        }

        self.coordinate(&txn, crate::txn::TransactionOutcome::Commit, false)
            .await
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
            value if matches!(&value, tc_state::State::Scalar(scalar) if matches!(&scalar, tc_ir::Scalar::Value(value) if matches!(&value, tc_value::Value::Bytes(_)))) =>
            {
                let Some(tc_value::Value::Bytes(bytes)) = tc_value::Value::opt_cast_from(value)
                else {
                    unreachable!("matched seed module bytes")
                };

                crate::wasm_installation(bytes)?
            }
            value => crate::State::from(vec![
                crate::State::from(tc_value::Value::from(identity)),
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
        automatic: bool,
    ) -> TCResult<()> {
        let Some(coordinator) = txn.lock_coordinator(outcome, automatic)? else {
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

        assert!(
            applied.is_none(),
            "resource decision returned an ordinary response"
        );

        Ok(())
    }

    pub async fn new(
        services: HostServices,
        workspace: crate::Workspace,
    ) -> TCResult<Self> {
        let bootstrap_ready = !services.bootstrap_required;
        let txn = crate::txn::TxnConfig::new(
            services.protocol.clone(),
            workspace,
            services.resources.clone(),
        );
        let txn_server = crate::txn::TxnServer::load(txn, services.verifier.clone()).await?;
        let bootstrap_txn = txn_server.allocate_bootstrap().await?;
        let inner = std::sync::Arc::new(KernelInner::load(bootstrap_txn, services).await?);
        let kernel = Self {
            txn_server,
            drain: std::sync::Arc::new(super::drain::Drain::new()),
            inner,
            bootstrap_ready: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
                bootstrap_ready,
            )),
        };

        let finalizer = std::sync::Arc::clone(&kernel.inner);
        for service in kernel.inner.services.state().items(bootstrap_txn).await? {
            Box::pin(service.recover(&kernel.txn_server, &kernel.inner)).await?;
        }
        kernel.txn_server.finish_recovery().await?;
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
    ) -> TCResult<Option<crate::State>> {
        let txn = if method == Method::Get {
            txn.read_only()
        } else {
            txn
        };

        match &target {
            KernelTarget::Application(application) => {
                self.inner.dispatch(&txn, application, method, body).await
            }
            _ if txn.is_locked() => Err(TCError::conflict(
                "a transaction decision must target an exact transactional resource",
            )),
            KernelTarget::State(state_target) => {
                self.inner
                    .dispatch_state(&txn, state_target, method, body)
                    .await
            }
            KernelTarget::AuthContext => {
                if method != Method::Get {
                    return Err(tc_error::TCError::method_not_allowed(
                        method.as_str(),
                        crate::uri::HOST_AUTH_CONTEXT,
                    ));
                }

                crate::host::auth_context(&txn).map(Some)
            }
            KernelTarget::Host => {
                if method != Method::Get {
                    return Err(TCError::method_not_allowed(method, crate::uri::HOST_ROOT));
                }
                let body = body.ok_or_else(|| TCError::bad_request("GET /host requires a body"))?;
                self.inner.bootstrap(&txn, body).await.map(Some)
            }
        }
    }

    pub(crate) fn deadline(&self) -> crate::Deadline {
        self.txn_server.resources().deadline()
    }

    #[cfg(feature = "http-server")]
    pub(crate) async fn admit_host_request(
        &self,
    ) -> TCResult<(
        crate::Deadline,
        crate::resources::CapacityPermit,
        tokio_util::task::task_tracker::TaskTrackerToken,
    )> {
        let deadline = self.deadline();
        let request = self.drain.admit()?;
        let permit = self.txn_server.resources().admit_request(deadline).await?;
        Ok((deadline, permit, request))
    }

    #[cfg(feature = "http-server")]
    pub(crate) async fn admit_connection(&self) -> TCResult<crate::resources::CapacityPermit> {
        let deadline = self.deadline();
        self.txn_server.resources().admit_connection(deadline).await
    }

    pub fn is_ready(&self) -> bool {
        !self.drain.is_stopping()
            && self.txn_server.is_ready()
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

        Ok(crate::State::from(Map::from_iter([
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

    /// Admit and bind a request before application body decoding.
    ///
    /// Verifies supplied credentials and establishes the transaction context.
    /// Resource-specific authorization remains enforced during concrete routing.
    pub async fn begin_request(
        &self,
        method: Method,
        raw_path: &str,
        body_is_none: bool,
        bearer: Option<String>,
    ) -> TCResult<Box<super::BoundRequest>> {
        let deadline = self.deadline();
        let request = self.drain.admit()?;
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

        Ok(Box::new(super::BoundRequest::new(
            self.clone(),
            method,
            target,
            txn,
            permit,
            request,
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
            txn.check_write()?;
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
            let Some(tc_state::State::Map(params)) = body else {
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
            txn.check_write()?;
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
    use safecast::TryCastFrom;

    Scalar::try_cast_from(body.unwrap_or_default(), |_| {
        TCError::bad_request("expected a scalar request")
    })
}

fn put_body(body: Option<crate::State>) -> TCResult<(Scalar, crate::State)> {
    let Some(tc_state::State::Tuple(mut values)) = body else {
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
