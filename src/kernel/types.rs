use crate::State;
use tc_ir::Method;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BodyContract {
    Application { max_bytes: usize },
    Native,
}

#[derive(Clone)]
pub(crate) enum KernelTarget {
    AuthContext,
    Host,
    State(Box<[pathlink::PathSegment]>),
    Application(pathlink::Link),
}

pub struct KernelRequestGuard {
    kernel: super::Kernel,
    method: Method,
    target: KernelTarget,
    txn: crate::TxnHandle,
    _permit: crate::resources::CapacityPermit,
}

impl KernelRequestGuard {
    pub(super) fn new(
        kernel: super::Kernel,
        method: Method,
        target: KernelTarget,
        txn: crate::TxnHandle,
        permit: crate::resources::CapacityPermit,
    ) -> Self {
        Self {
            kernel,
            method,
            target,
            txn,
            _permit: permit,
        }
    }

    pub fn txn(&self) -> &crate::TxnHandle {
        &self.txn
    }

    pub fn deadline(&self) -> crate::Deadline {
        self.txn.deadline()
    }

    pub fn body_contract(&self) -> BodyContract {
        if self.txn.is_locked() {
            return BodyContract::Native;
        }
        match &self.target {
            KernelTarget::Application(target)
                if crate::uri::is_application_root(target) && self.method == Method::Put =>
            {
                BodyContract::Application {
                    max_bytes: if target.path()[0].as_str() == "lib" {
                        self.txn.application_body_limit()
                    } else {
                        crate::class::MAX_CLASS_BYTES
                    },
                }
            }
            KernelTarget::Application(_)
            | KernelTarget::Host
            | KernelTarget::AuthContext
            | KernelTarget::State(_) => BodyContract::Native,
        }
    }

    #[cfg(feature = "http-server")]
    pub(crate) fn application_admission(&self) -> crate::resources::ApplicationAdmission {
        self.txn.application_admission()
    }

    #[cfg(feature = "http-server")]
    pub(crate) fn native_body_limit(&self) -> usize {
        self.txn.request_body_limit()
    }

    pub fn admit_application_memory(
        &self,
        bytes: usize,
    ) -> impl std::future::Future<Output = tc_error::TCResult<tokio::sync::OwnedSemaphorePermit>>
    + Send
    + 'static {
        let txn = self.txn.clone();
        async move { txn.admit_application_memory(bytes).await }
    }

    pub async fn execute(&self, body: Option<State>) -> tc_error::TCResult<Option<State>> {
        self.deadline()
            .run(
                self.kernel
                    .execute(self.target.clone(), self.txn.clone(), self.method, body),
            )
            .await
            .map(|(state, _)| state)
    }

    /// Report that routing and terminal response projection both succeeded.
    ///
    /// Dropping a guard never selects an outcome. The first owning Cluster remains
    /// responsible for deciding whether this successful request mutated anything.
    pub async fn finish_success(self) -> tc_error::TCResult<()> {
        self.kernel
            .coordinate(&self.txn, crate::txn::TransactionOutcome::Commit, true)
            .await
    }

    pub async fn execute_bound(
        self,
        body: Option<State>,
    ) -> tc_error::TCResult<(Option<State>, bool, Self)> {
        let (state, raw_bytes) = self
            .deadline()
            .run(
                self.kernel
                    .execute(self.target.clone(), self.txn.clone(), self.method, body),
            )
            .await?;
        Ok((state, raw_bytes, self))
    }
}
