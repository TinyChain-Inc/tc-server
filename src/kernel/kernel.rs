use tc_error::{TCError, TCResult};
use tc_ir::{Handler, Scalar};

use super::Method;
use super::types::KernelTarget;
use crate::txn::TxnServer;

#[derive(Clone)]
pub struct Kernel {
    pub(crate) txn_server: TxnServer,
    pub(crate) inner: std::sync::Arc<super::KernelInner>,
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
    ) -> TCResult<(crate::TxnHandle, crate::replication::BootstrapSession)> {
        let request = self
            .begin_request(
                Method::Get,
                &resource.to_string(),
                true,
                None,
                self.resources().deadline(),
            )
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
        let actor = authority.actor.id().clone();
        let txn = txn.with_auth_context(crate::auth::AuthContext::new(actor.clone()).with_claim(
            authority.host.to_string(),
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
        coordinator: &pathlink::PathBuf,
        outcome: crate::txn::TransactionOutcome,
        require_mutation: bool,
    ) -> TCResult<()> {
        if !txn.lock_coordinator(coordinator, require_mutation)? {
            return Ok(());
        }
        let target: pathlink::Link = coordinator.to_string().parse().map_err(|error| {
            TCError::internal(format!("invalid coordinator resource path: {error}"))
        })?;
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
        if applied.is_some() {
            return Err(TCError::internal(
                "resource decision returned an ordinary response",
            ));
        }
        Ok(())
    }

    pub async fn new(
        services: super::HostServices,
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
            super::KernelInner::load(
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
        kernel.txn_server.start_expiry(
            &tokio::runtime::Handle::current(),
            std::sync::Arc::clone(&kernel.inner),
        );
        Ok(kernel)
    }

    pub(crate) async fn execute(
        &self,
        target: KernelTarget,
        txn: crate::TxnHandle,
        method: Method,
        body: Option<crate::State>,
    ) -> TCResult<Option<crate::State>> {
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
            KernelTarget::Health => self.health(method).map(Some),
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

    pub fn resources(&self) -> &crate::HostResources {
        self.txn_server.resources()
    }

    #[cfg(test)]
    pub(crate) async fn test_txn(&self) -> crate::TxnHandle {
        self.txn_server
            .bind(None, None, std::sync::Arc::clone(&self.inner))
            .await
            .expect("begin test transaction")
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
        deadline: crate::Deadline,
    ) -> TCResult<Box<super::KernelRequestGuard>> {
        let (path, txn_id) = crate::txn::wire::split_path_and_txn_id(raw_path)?;
        let link: pathlink::Link = path
            .parse()
            .map_err(|error| TCError::bad_request(format!("invalid request target: {error}")))?;
        let target = match link.path().first() {
            Some(root) if matches!(root.as_str(), "lib" | "class" | "service") => {
                KernelTarget::Application(link)
            }
            _ if path == crate::uri::HOST_HEALTH => KernelTarget::Health,
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
        let permit = self.resources().admit_request(deadline).await?;
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
        .with_deadline(deadline);
        if body_is_none && matches!(method, Method::Put | Method::Delete) && !txn.is_locked() {
            return Err(TCError::bad_request(
                "an ordinary PUT or DELETE request requires an explicit body",
            ));
        }
        Ok(Box::new(super::KernelRequestGuard {
            kernel: self.clone(),
            method,
            target,
            txn,
            _permit: permit,
        }))
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
