use crate::State;
use tc_ir::Method;

#[cfg(feature = "http-server")]
pub(crate) struct AdmittedBody {
    bytes: std::sync::Arc<[u8]>,
    _permits: Vec<tokio::sync::OwnedSemaphorePermit>,
}

#[cfg(feature = "http-server")]
impl AdmittedBody {
    pub(crate) fn shared(&self) -> std::sync::Arc<[u8]> {
        std::sync::Arc::clone(&self.bytes)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BodyContract {
    Application { max_bytes: usize },
    Native,
}

#[derive(Clone)]
pub(crate) enum KernelTarget {
    Health,
    AuthContext,
    State(Box<[pathlink::PathSegment]>),
    Application(pathlink::Link),
    External(pathlink::Link),
}

pub struct KernelRequestGuard {
    pub(super) kernel: super::Kernel,
    pub(super) method: Method,
    pub(super) target: KernelTarget,
    pub(super) txn: crate::TxnHandle,
    pub(super) expected_digest: Option<crate::application::Digest>,
    pub(super) _permit: crate::resources::CapacityPermit,
}

impl KernelRequestGuard {
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
            | KernelTarget::AuthContext
            | KernelTarget::State(_)
            | KernelTarget::External(_) => BodyContract::Native,
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
        let state = self
            .kernel
            .execute(
                self.target.clone(),
                self.txn.clone(),
                self.method,
                body,
                self.expected_digest.clone(),
            )
            .await?;
        if state.is_some() {
            if let Some(coordinator) = self.txn.coordinator() {
                coordinator
                    .coordinate(&self.txn, crate::txn::TransactionOutcome::Commit, true)
                    .await?;
            }
        }
        Ok(state)
    }

    pub async fn execute_bound(
        self,
        body: Option<State>,
    ) -> tc_error::TCResult<(Option<State>, Self)> {
        match self.execute(body).await {
            Ok(state) => Ok((state, self)),
            Err(error) => Err(error),
        }
    }

    #[cfg(feature = "http-server")]
    pub(crate) async fn admit_body<S>(
        &self,
        input: S,
        max_bytes: usize,
    ) -> tc_error::TCResult<AdmittedBody>
    where
        S: futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + 'static,
    {
        use futures::TryStreamExt;

        let resources = self.txn.resources().clone();
        let deadline = self.deadline();
        let (bytes, permits) = input
            .try_fold(
                (Vec::new(), Vec::new()),
                move |(mut bytes, mut permits), chunk| {
                    let resources = resources.clone();
                    async move {
                        if bytes.len().saturating_add(chunk.len()) > max_bytes {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "application body exceeds its request bound",
                            ));
                        }
                        let permit = resources
                            .admit_application_bytes(chunk.len(), deadline)
                            .await
                            .map_err(|error| std::io::Error::other(error.to_string()))?;
                        bytes.extend_from_slice(&chunk);
                        permits.push(permit);
                        Ok((bytes, permits))
                    }
                },
            )
            .await
            .map_err(|error| tc_error::TCError::bad_request(error.to_string()))?;
        Ok(AdmittedBody {
            bytes: bytes.into(),
            _permits: permits,
        })
    }

    #[cfg(feature = "http-server")]
    pub(crate) fn set_expected_digest(
        &mut self,
        expected: crate::application::Digest,
    ) -> tc_error::TCResult<()> {
        let KernelTarget::Application(identity) = &self.target else {
            return Err(tc_error::TCError::bad_request(
                "expected a versioned application target",
            ));
        };
        let identity = crate::application::application_identity(identity)?;
        if !self.txn.has_protocol_claim(identity.path()) {
            return Err(tc_error::TCError::unauthorized(
                "expected digest requires authenticated resource-leader authority",
            ));
        }
        self.expected_digest = Some(expected);
        Ok(())
    }
}
