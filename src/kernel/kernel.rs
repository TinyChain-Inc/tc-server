use tc_error::{TCError, TCResult};
use tc_ir::{Handler, Scalar};

use super::Method;
use super::types::KernelTarget;
use crate::txn::TxnServer;

#[derive(Clone)]
pub struct Kernel {
    pub(crate) txn_server: TxnServer,
    pub(crate) runtime: std::sync::Arc<super::HostRuntime>,
}

impl Kernel {
    pub async fn new(
        services: super::HostServices,
        workspace: crate::Workspace,
        ttl: std::time::Duration,
    ) -> TCResult<Self> {
        let txn = crate::txn::TxnConfig::new(
            services.protocol,
            workspace,
            services.resources.clone(),
            ttl,
        );
        let runtime = std::sync::Arc::new(super::HostRuntime::new(
            services.applications,
            services.rpc,
            services.resources.clone(),
            services.public_keys,
        ));
        let txn_server = crate::txn::TxnServer::new(txn, services.verifier);
        let kernel = Self {
            txn_server,
            runtime,
        };
        kernel.txn_server.recover().await?;
        kernel.txn_server.start_expiry(
            &tokio::runtime::Handle::current(),
            std::sync::Arc::clone(&kernel.runtime),
        );
        Ok(kernel)
    }

    pub(crate) async fn execute(
        &self,
        target: KernelTarget,
        txn: crate::TxnHandle,
        method: Method,
        body: Option<crate::State>,
        expected_digest: Option<crate::application::Digest>,
    ) -> TCResult<Option<crate::State>> {
        match &target {
            KernelTarget::Application(application) => {
                self.runtime
                    .applications
                    .dispatch(&txn, application, expected_digest.as_ref(), method, body)
                    .await
            }
            _ if txn.is_locked() => Err(TCError::conflict(
                "a transaction decision must target an exact transactional resource",
            )),
            KernelTarget::State(state_target) => {
                self.runtime
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
            KernelTarget::External(external) => Err(TCError::not_found(external.to_string())),
        }
    }

    pub fn resources(&self) -> &crate::HostResources {
        self.txn_server.resources()
    }

    #[cfg(test)]
    pub(crate) fn txn_server(&self) -> &TxnServer {
        &self.txn_server
    }

    #[cfg(test)]
    pub(crate) async fn test_txn(&self) -> crate::TxnHandle {
        self.txn_server
            .bind(None, None, std::sync::Arc::clone(&self.runtime))
            .await
            .expect("begin test transaction")
    }

    pub fn is_ready(&self) -> bool {
        self.txn_server.is_ready()
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
                self.runtime.metrics(),
            ),
        ])))
    }

    pub fn metrics(&self, method: Method) -> TCResult<crate::State> {
        (method == Method::Get)
            .then(|| self.runtime.metrics())
            .ok_or_else(|| TCError::method_not_allowed(method, crate::uri::HOST_METRICS))
    }

    pub fn public_key(&self, method: Method, actor_id: &str) -> TCResult<crate::State> {
        if method != Method::Get {
            return Err(TCError::method_not_allowed(
                method,
                crate::uri::HOST_PUBLIC_KEY,
            ));
        }
        self.runtime.public_key(actor_id)
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
            _ if link
                .path()
                .first()
                .is_some_and(|root| root.as_str() == "state") =>
            {
                KernelTarget::State(link.path()[1..].into())
            }
            _ => KernelTarget::External(link),
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
        let txn = self
            .txn_server
            .bind(txn_id, token.as_ref(), std::sync::Arc::clone(&self.runtime))
            .await?
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
            expected_digest: None,
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
