use crate::State;
use tc_ir::Method;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BodyContract {
    Application { max_bytes: usize },
    Native,
}

#[derive(Clone)]
pub(crate) enum KernelTarget {
    Health,
    AuthContext,
    Host,
    State(Box<[pathlink::PathSegment]>),
    Application(pathlink::Link),
}

pub struct KernelRequestGuard {
    pub(super) kernel: super::Kernel,
    pub(super) method: Method,
    pub(super) target: KernelTarget,
    pub(super) txn: crate::TxnHandle,
    pub(super) _permit: crate::resources::CapacityPermit,
}

impl KernelRequestGuard {
    #[cfg(feature = "http-server")]
    pub(crate) fn request(&self) -> (Method, &KernelTarget) {
        (self.method, &self.target)
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
                if target.path().len() == 1 && self.method == Method::Put =>
            {
                BodyContract::Application {
                    max_bytes: if target.path()[0].as_str() == "lib" {
                        self.txn.resources().limits().ingress.application_body_bytes
                    } else {
                        crate::class::MAX_CLASS_BYTES
                    },
                }
            }
            KernelTarget::Application(_)
            | KernelTarget::Health
            | KernelTarget::Host
            | KernelTarget::AuthContext
            | KernelTarget::State(_) => BodyContract::Native,
        }
    }

    pub fn admit_application_memory(
        &self,
        bytes: usize,
    ) -> impl std::future::Future<Output = tc_error::TCResult<tokio::sync::OwnedSemaphorePermit>>
    + Send
    + 'static {
        let resources = self.txn.resources().clone();
        let deadline = self.deadline();
        async move { resources.admit_application_bytes(bytes, deadline).await }
    }

    pub async fn execute(&self, body: Option<State>) -> tc_error::TCResult<Option<State>> {
        self.deadline()
            .run(
                self.kernel
                    .execute(self.target.clone(), self.txn.clone(), self.method, body),
            )
            .await
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
    ) -> tc_error::TCResult<(Option<State>, Self)> {
        let state = self.execute(body).await?;
        Ok((state, self))
    }
}
