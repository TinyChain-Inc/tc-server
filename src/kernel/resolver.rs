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
        txn.check_active()?;
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

        if matches!(method, Method::Put | Method::Delete) {
            txn.check_write()?;
        }

        if matches!(method, Method::Put | Method::Post | Method::Delete) {
            txn.record_dependency(identity.clone())?;
        }

        let txn = if method == Method::Get {
            txn.branch().read_only()
        } else {
            txn.branch()
        };

        // Delegation grants permission; only the destination establishes leadership.
        let mask = if method == Method::Get {
            umask::USER_READ
        } else {
            umask::USER_READ | umask::USER_WRITE
        };

        let claim = crate::Claim::new(identity, mask);
        let txn = txn.grant_claim(claim).await?;

        if target.host().is_none() {
            Ok((Target::Application(target.clone()), txn))
        } else {
            Ok((Target::Remote(target.clone()), txn))
        }
    }

    async fn application(
        &self,
        txn: &crate::TxnHandle,
        target: &pathlink::Link,
        method: Method,
        body: State,
    ) -> tc_error::TCResult<State> {
        Ok(self
            .dispatch(txn, target, method, Some(body))
            .await?
            .expect("nested application dispatch returned a transaction decision"))
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
                self.application(&txn, &application, Method::Get, State::from(key))
                    .await
            }
            Target::Remote(target) => {
                let _permit = txn.admit_outbound().await?;
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
                    State::from(vec![State::from(key), value]),
                )
                .await
                .map(|_| ()),
            Target::Remote(target) => {
                let _permit = txn.admit_outbound().await?;
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
                self.application(&txn, &application, Method::Post, State::from(params))
                    .await
            }
            Target::Remote(target) => {
                let _permit = txn.admit_outbound().await?;
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
                .application(&txn, &application, Method::Delete, State::from(key))
                .await
                .map(|_| ()),
            Target::Remote(target) => {
                let _permit = txn.admit_outbound().await?;
                self.rpc.delete(target, txn, key).await
            }
        }
    }
}
