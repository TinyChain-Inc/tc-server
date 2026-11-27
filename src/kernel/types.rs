use bytes::Bytes;
use destream::FromStream;
use futures::{FutureExt, Stream};
use tc_error::{TCError, TCResult};
use tc_ir::Method;

use crate::State;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BodyContract {
    Definition(usize),
    Wasm(usize),
    Native(usize),
}

struct JsonBody(State);

impl FromStream for JsonBody {
    type Context = Option<crate::TxnHandle>;

    async fn from_stream<D: destream::de::Decoder>(
        txn: Self::Context,
        decoder: &mut D,
    ) -> Result<Self, D::Error> {
        match txn {
            Some(txn) => State::from_stream(txn, decoder).await.map(Self),
            None => crate::Definition::from_stream((), decoder)
                .await
                .map(|definition| Self(definition.into())),
        }
    }
}

#[derive(Clone)]
pub(crate) enum KernelTarget {
    AuthContext,
    Host,
    State(Box<[pathlink::PathSegment]>),
    Application(pathlink::Link),
}

/// Owns an admitted, transaction-bound request through terminal projection.
///
/// Entry admission and verification of supplied credentials precede application
/// body decoding. Routing still enforces resource-specific authorization.
/// Execution returns this same owner with ordinary State so projection retains
/// its deadline, capacity, and shutdown tracking. Owning mutations commit before
/// projection. Dropping an unexecuted request or a projected result selects no
/// decision; cancelling active execution leaves rollback to the host-owned task.
pub struct BoundRequest {
    kernel: super::Kernel,
    method: Method,
    target: KernelTarget,
    txn: crate::TxnHandle,
    _permit: crate::resources::CapacityPermit,
    _request: tokio_util::task::task_tracker::TaskTrackerToken,
}

impl BoundRequest {
    pub(super) fn new(
        kernel: super::Kernel,
        method: Method,
        target: KernelTarget,
        txn: crate::TxnHandle,
        permit: crate::resources::CapacityPermit,
        request: tokio_util::task::task_tracker::TaskTrackerToken,
    ) -> Self {
        Self {
            kernel,
            method,
            target,
            txn,
            _permit: permit,
            _request: request,
        }
    }

    pub fn txn(&self) -> &crate::TxnHandle {
        &self.txn
    }

    /// Retain native input ownership through execution and any derived result.
    pub fn retain_source(&mut self, source: &crate::TxnHandle) -> tc_error::TCResult<()> {
        self.txn.retain_source(source)
    }

    pub fn deadline(&self) -> crate::Deadline {
        self.txn.deadline()
    }

    fn application_body_limit(&self) -> Option<usize> {
        match &self.target {
            KernelTarget::Application(target)
                if self.method == Method::Put
                    && crate::uri::is_application_root(target)
                    && !self.txn.is_locked() =>
            {
                Some(if target.path()[0].as_str() == "lib" {
                    self.txn.application_body_limit()
                } else {
                    crate::class::MAX_CLASS_BYTES
                })
            }
            _ => None,
        }
    }

    /// Select the wire representation and byte limit for an admitted request.
    pub fn body_contract(&self, content_type: Option<&str>) -> TCResult<BodyContract> {
        match (self.application_body_limit(), content_type) {
            (None, _) => Ok(BodyContract::Native(self.native_body_limit())),
            (Some(limit), None | Some("application/json")) => Ok(BodyContract::Definition(limit)),
            (Some(limit), Some("application/wasm")) => Ok(BodyContract::Wasm(limit)),
            (Some(_), Some(_)) => Err(TCError::bad_request("unsupported application content type")),
        }
    }

    fn json_context(&self) -> Option<crate::TxnHandle> {
        self.application_body_limit()
            .is_none()
            .then(|| self.txn.clone())
    }

    /// Decode one complete JSON body from a transport-bounded stream.
    pub async fn decode_json<S, E>(&self, source: S) -> TCResult<State>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Unpin,
        E: std::fmt::Display,
    {
        destream_json::try_decode::<_, _, JsonBody>(self.json_context(), source)
            .await
            .map(|body| body.0)
            .map_err(|error| TCError::bad_request(error.to_string()))
    }

    /// Decode one body from a bounded foreign-object decoder.
    pub async fn decode_object<D: destream::de::Decoder>(
        &self,
        decoder: &mut D,
    ) -> TCResult<State> {
        JsonBody::from_stream(self.json_context(), decoder)
            .await
            .map(|body| body.0)
            .map_err(|error| TCError::bad_request(error.to_string()))
    }

    pub(crate) fn application_admission(&self) -> crate::resources::ApplicationAdmission {
        self.txn.application_admission()
    }

    pub fn native_body_limit(&self) -> usize {
        self.txn.request_body_limit()
    }

    pub fn admit_application_memory(
        &self,
        bytes: usize,
    ) -> impl std::future::Future<Output = tc_error::TCResult<tokio::sync::OwnedSemaphorePermit>>
    + Send
    + 'static {
        let admission = self.application_admission();
        async move { admission.acquire(bytes).await }
    }

    async fn execute_result(
        self,
        body: Option<State>,
    ) -> tc_error::TCResult<(Option<State>, Self)> {
        self.txn.check_active()?;
        let deadline = self.deadline();
        let request = self;
        let kernel = request.kernel.clone();
        let (mut response, result) = tokio::sync::oneshot::channel();

        // The host retains admission through cleanup. Closing the receiver cancels
        // execution, but cannot cancel a decision which has already begun.
        tokio::spawn(async move {
            let work = async {
                let result = request.execute_owned(body, &mut response).await;
                let _ = response.send(result.map(|state| (state, request)));
            };

            if std::panic::AssertUnwindSafe(work)
                .catch_unwind()
                .await
                .is_err()
            {
                kernel.fatal_recovery("request decision worker panicked");
            }
        });

        // The caller keeps its original deadline even while host cleanup continues.
        deadline
            .run(async {
                result
                    .await
                    .map_err(|_| tc_error::TCError::internal("request worker stopped"))?
            })
            .await
    }

    async fn execute_owned(
        &self,
        body: Option<State>,
        response: &mut tokio::sync::oneshot::Sender<tc_error::TCResult<(Option<State>, Self)>>,
    ) -> tc_error::TCResult<Option<State>> {
        let locked = self.txn.is_locked();
        let ordinary_mutation =
            matches!(self.method, Method::Put | Method::Post | Method::Delete) && !locked;

        // Retain the measured native dispatch boxing boundary. Drop all local
        // execution work before rollback on errors or cancellation. Panics resume
        // into the fatal worker boundary in unwinding test harnesses.
        let result = {
            let execution = self.deadline().run(Box::pin(self.kernel.execute(
                self.target.clone(),
                self.txn.clone(),
                self.method,
                body,
            )));

            tokio::select! {
                biased;
                _ = response.closed(), if !locked => Err(tc_error::TCError::conflict("request execution cancelled")),
                result = execution => result,
            }
        };

        let result = result.map_err(|error| self.txn.storage_error(error));
        match result {
            Ok(state) if ordinary_mutation && !self.txn.is_locked() => {
                // Commit errors must never select rollback.
                self.deadline()
                    .run(Box::pin(self.kernel.coordinate(
                        &self.txn,
                        crate::txn::TransactionOutcome::Commit,
                        true,
                    )))
                    .await?;

                Ok(state)
            }
            Err(error) if ordinary_mutation && !self.txn.is_locked() => {
                let deadline = self.kernel.deadline();
                let txn = self.txn.for_cleanup(deadline);
                if let Err(rollback) = deadline
                    .run(Box::pin(self.kernel.coordinate(
                        &txn,
                        crate::txn::TransactionOutcome::Rollback,
                        true,
                    )))
                    .await
                {
                    self.kernel
                        .fatal_recovery(format!("rollback after {error} failed: {rollback}; execution causes: {:?}; rollback causes: {:?}", error.causes(), rollback.causes()));
                }

                Err(error)
            }
            result => result,
        }
    }

    /// Execute and decide an owning mutation before projecting its response.
    /// Ordinary values return this same owner for terminal projection, not for
    /// deferred authentication or commit. PUT/POST/DELETE execution errors,
    /// timeout, and cancellation may coordinate rollback through the host-owned task. Projection errors and dropping the owner cannot
    /// reverse a successful commit.
    pub async fn execute_bound(
        self,
        body: Option<State>,
    ) -> tc_error::TCResult<Option<(State, Self)>> {
        let application = self.application_body_limit().is_some();
        let (state, request) = self.execute_result(body).await?;
        let Some(state) = state else {
            // The selected resource already executed this transaction decision.
            return Ok(None);
        };

        if application {
            Ok(None)
        } else {
            Ok(Some((state, request)))
        }
    }
}
