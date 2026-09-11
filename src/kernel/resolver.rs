use super::Method;
use crate::State;

enum Target {
    Host,
    Application(pathlink::Link),
    Remote(pathlink::Link),
}

impl super::KernelInner {
    async fn prepare(
        &self,
        method: Method,
        target: &pathlink::Link,
        txn: &crate::TxnHandle,
    ) -> tc_error::TCResult<(Target, crate::TxnHandle)> {
        let scope = txn.application_scope().ok_or_else(|| {
            tc_error::TCError::unauthorized("application dependency call has no execution scope")
        })?;
        let path_string = target.path().to_string();
        let path = crate::uri::normalize_path(&path_string);
        if path == crate::uri::HOST_ROOT || path.starts_with(crate::uri::HOST_ROOT_PREFIX) {
            if target.host().is_some() {
                return Err(tc_error::TCError::unauthorized(
                    "cross-host /host access is not allowed from application routes",
                ));
            }
            return Ok((Target::Host, txn.clone()));
        }

        let identity = crate::uri::application_identity(target)?;
        if !scope.authorize(&identity, method) {
            return Err(tc_error::TCError::unauthorized(format!(
                "unauthorized dependency {identity}"
            )));
        }
        if target.host().is_none() {
            return Ok((Target::Application(target.clone()), txn.clone()));
        }
        let claim = crate::Claim::new(identity, umask::Mode::all());
        let txn = txn.grant_claim(claim).await?;
        Ok((Target::Remote(target.clone()), txn))
    }

    async fn application(
        &self,
        txn: &crate::TxnHandle,
        target: &pathlink::Link,
        method: Method,
        body: State,
    ) -> tc_error::TCResult<State> {
        self.dispatch(txn, target, method, Some(body))
            .await?
            .ok_or_else(|| {
                tc_error::TCError::internal(
                    "nested application dispatch returned a transaction decision",
                )
            })
    }

    pub(crate) async fn get(
        &self,
        target: pathlink::Link,
        txn: crate::TxnHandle,
        key: tc_ir::Scalar,
    ) -> tc_error::TCResult<State> {
        let (target, txn) = self.prepare(Method::Get, &target, &txn).await?;
        match target {
            Target::Host => crate::host::auth_context(&txn),
            Target::Application(application) => {
                self.application(&txn, &application, Method::Get, State::from_scalar(key))
                    .await
            }
            Target::Remote(target) => {
                let _permit = txn.resources().admit_outbound(txn.deadline()).await?;
                self.rpc.get(target, txn, key).await
            }
        }
    }

    pub(crate) async fn put(
        &self,
        target: pathlink::Link,
        txn: crate::TxnHandle,
        key: tc_ir::Scalar,
        value: State,
    ) -> tc_error::TCResult<()> {
        let (target, txn) = self.prepare(Method::Put, &target, &txn).await?;
        match target {
            Target::Host => Err(tc_error::TCError::method_not_allowed(
                Method::Put,
                crate::uri::HOST_ROOT,
            )),
            Target::Application(application) => self
                .application(
                    &txn,
                    &application,
                    Method::Put,
                    State::Tuple(vec![State::from_scalar(key), value]),
                )
                .await
                .map(|_| ()),
            Target::Remote(target) => {
                let _permit = txn.resources().admit_outbound(txn.deadline()).await?;
                self.rpc.put(target, txn, key, value).await
            }
        }
    }

    pub(crate) async fn post(
        &self,
        target: pathlink::Link,
        txn: crate::TxnHandle,
        params: tc_ir::Map<State>,
    ) -> tc_error::TCResult<State> {
        let (target, txn) = self.prepare(Method::Post, &target, &txn).await?;
        match target {
            Target::Host => Err(tc_error::TCError::method_not_allowed(
                Method::Post,
                crate::uri::HOST_ROOT,
            )),
            Target::Application(application) => {
                self.application(&txn, &application, Method::Post, State::Map(params))
                    .await
            }
            Target::Remote(target) => {
                let _permit = txn.resources().admit_outbound(txn.deadline()).await?;
                self.rpc.post(target, txn, params).await
            }
        }
    }

    pub(crate) async fn delete(
        &self,
        target: pathlink::Link,
        txn: crate::TxnHandle,
        key: tc_ir::Scalar,
    ) -> tc_error::TCResult<()> {
        let (target, txn) = self.prepare(Method::Delete, &target, &txn).await?;
        match target {
            Target::Host => Err(tc_error::TCError::method_not_allowed(
                Method::Delete,
                crate::uri::HOST_ROOT,
            )),
            Target::Application(application) => self
                .application(&txn, &application, Method::Delete, State::from_scalar(key))
                .await
                .map(|_| ()),
            Target::Remote(target) => {
                let _permit = txn.resources().admit_outbound(txn.deadline()).await?;
                self.rpc.delete(target, txn, key).await
            }
        }
    }
}
