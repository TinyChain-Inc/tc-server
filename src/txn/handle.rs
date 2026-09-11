use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fmt, sync::Arc, time::SystemTime};

use pathlink::Link;
use tc_error::TCError;
use tc_ir::{Transaction, TxnId};
use umask::Mode;

use crate::Claim;
use crate::auth::{Actor, AuthContext, SignedToken, Token};

fn execution_unavailable(message: impl std::fmt::Display) -> TCError {
    TCError::resource_unavailable(
        message,
        tc_error::Pressure::new("/host/resource/graph", tc_error::PressureReason::Saturated),
    )
}

#[derive(Clone)]
pub(crate) enum ExecutionScope {
    Host,
    Application(Arc<crate::txn::DependencyScope>),
}

pub(super) struct ClaimContext {
    pub(super) signed: Option<Arc<SignedToken>>,
    pub(super) mutated: bool,
}

pub(super) struct ExecutionBudget {
    remaining: AtomicUsize,
}

impl ExecutionBudget {
    pub(super) fn new(invocations: usize) -> Self {
        Self {
            remaining: AtomicUsize::new(invocations),
        }
    }
}

#[derive(Clone)]
pub struct TxnHandle {
    pub(super) id: TxnId,
    pub(super) server: super::TxnServer,
    pub(crate) kernel: Arc<crate::kernel::KernelInner>,
    pub(super) scope: ExecutionScope,
    pub(super) auth_context: Option<AuthContext>,
    pub(super) protocol_claims: Arc<parking_lot::Mutex<ClaimContext>>,
    pub(super) autocommit: bool,
    pub(super) workspace_path: Vec<String>,
    pub(super) deadline: crate::Deadline,
    pub(super) graph_admitted: bool,
    pub(super) execution_budget: Arc<ExecutionBudget>,
    pub(super) execution_depth: usize,
}

impl TxnHandle {
    fn protocol_snapshot(&self) -> Option<super::token::ProtocolSnapshot> {
        self.protocol_claims
            .lock()
            .signed
            .as_deref()
            .and_then(|token| crate::txn::protocol_snapshot(self.id, token).ok())
    }

    pub(crate) fn is_locked(&self) -> bool {
        self.protocol_snapshot().is_some_and(|claims| claims.locked)
    }

    pub(crate) fn leader(&self, path: &pathlink::PathBuf) -> Option<(String, String)> {
        self.protocol_snapshot()
            .and_then(|claims| claims.leaders.get(path).cloned())
    }

    #[cfg(test)]
    pub(crate) fn claimed_paths(&self) -> std::collections::BTreeSet<pathlink::PathBuf> {
        self.protocol_snapshot()
            .map(|claims| claims.leaders.into_keys().collect())
            .unwrap_or_default()
    }

    pub(crate) fn mark_resource_mutated(&self, path: &pathlink::PathBuf) -> tc_error::TCResult<()> {
        let mut claims = self.protocol_claims.lock();
        let claimed = claims
            .signed
            .as_deref()
            .map(|token| crate::txn::protocol_snapshot(self.id, token))
            .transpose()?
            .is_some_and(|snapshot| snapshot.leaders.contains_key(path));
        if !claimed {
            return Err(TCError::conflict(format!(
                "resource {path} mutated before it was claimed"
            )));
        }
        claims.mutated = true;
        Ok(())
    }

    pub(crate) fn claim_cluster(
        &self,
        path: &pathlink::PathBuf,
        authority: &crate::ProtocolAuthority,
    ) -> tc_error::TCResult<()> {
        let mut claims = self.protocol_claims.lock();
        let snapshot = claims
            .signed
            .as_deref()
            .map(|token| crate::txn::protocol_snapshot(self.id, token))
            .transpose()?;
        if snapshot.as_ref().is_some_and(|snapshot| snapshot.locked) {
            return Err(TCError::conflict("a locked transaction cannot accept work"));
        }
        if snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.leaders.get(path))
            .is_none()
        {
            let mut grants = std::collections::BTreeMap::new();
            let ownerless = snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.owner.as_ref())
                .is_none();
            if ownerless {
                grants.insert(
                    crate::uri::transaction_path(self.id)
                        .parse()
                        .expect("transaction path"),
                    u32::from(umask::USER_EXEC),
                );
            }
            grants.insert(path.clone(), u32::from(umask::USER_EXEC));
            let now = SystemTime::now();
            let signed = if let Some(token) = &claims.signed {
                authority
                    .actor
                    .consume_and_sign((**token).clone(), authority.host.clone(), grants, now)
                    .map_err(|error| TCError::unauthorized(error.to_string()))?
            } else {
                authority
                    .actor
                    .sign_token(Token::new(
                        authority.host.clone(),
                        now,
                        self.server.ttl(),
                        authority.actor.id().clone(),
                        grants,
                    ))
                    .map_err(|error| TCError::unauthorized(error.to_string()))?
            };
            claims.signed = Some(Arc::new(signed));
        }
        Ok(())
    }

    pub(crate) fn lock_coordinator(
        &self,
        require_mutation: bool,
    ) -> tc_error::TCResult<Option<pathlink::PathBuf>> {
        let mut claims = self.protocol_claims.lock();
        if require_mutation && (!self.autocommit || !claims.mutated) {
            return Ok(None);
        }
        let snapshot = claims
            .signed
            .as_deref()
            .map(|token| crate::txn::protocol_snapshot(self.id, token))
            .transpose()?
            .ok_or_else(|| TCError::conflict("cannot decide an ownerless transaction"))?;
        let coordinator = snapshot
            .coordinator
            .ok_or_else(|| TCError::conflict("transaction has no resource coordinator"))?;
        let owner = snapshot
            .owner
            .ok_or_else(|| TCError::conflict("cannot decide an ownerless transaction"))?;
        if owner.1 != self.server.protocol_authority().actor.id().as_str() {
            return Err(TCError::unauthorized(
                "only the transaction-owning resource may coordinate a decision",
            ));
        }
        if !snapshot.locked {
            let authority = self.server.protocol_authority();
            let signed = authority
                .actor
                .consume_and_sign(
                    (**claims.signed.as_ref().expect("snapshot has a token")).clone(),
                    authority.host.clone(),
                    crate::auth::wire_claim(crate::Claim::new(
                        crate::uri::transaction_path(self.id)
                            .parse()
                            .expect("transaction path"),
                        umask::USER_WRITE,
                    )),
                    SystemTime::now(),
                )
                .map_err(|error| TCError::unauthorized(error.to_string()))?;
            claims.signed = Some(Arc::new(signed));
        }
        Ok(Some(coordinator))
    }

    pub fn subcontext(&self, name: impl Into<String>) -> Self {
        let mut txn = self.clone();
        txn.workspace_path.push(name.into());
        txn
    }

    pub fn subcontext_unique(&self) -> Self {
        self.subcontext("tmp")
            .subcontext(self.server.workspace().unique_name())
    }

    pub(crate) fn with_deadline(&self, deadline: crate::Deadline) -> Self {
        let mut txn = self.clone();
        txn.deadline = deadline;
        txn
    }

    pub(crate) fn resources(&self) -> &crate::HostResources {
        self.server.resources()
    }

    pub(crate) fn deadline(&self) -> crate::Deadline {
        self.deadline
    }

    pub(crate) fn graph_admitted(&self) -> bool {
        self.graph_admitted
    }

    pub(crate) fn with_graph_admission(&self) -> Self {
        let mut txn = self.clone();
        txn.graph_admitted = true;
        txn
    }

    async fn enter_execution(&self) -> tc_error::TCResult<Self> {
        if self.deadline.is_expired() {
            return Err(self.deadline.exceeded());
        }
        let limits = &self.resources().limits().execution;
        if self.execution_depth >= limits.max_execution_depth {
            return Err(execution_unavailable(format!(
                "operation nesting exceeds the {}-level limit",
                limits.max_execution_depth
            )));
        }
        self.execution_budget
            .remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .map_err(|_| execution_unavailable("operation invocation budget exhausted"))?;
        tokio::task::yield_now().await;
        if self.deadline.is_expired() {
            return Err(self.deadline.exceeded());
        }
        let mut txn = self.clone();
        txn.execution_depth += 1;
        Ok(txn)
    }

    pub(crate) fn for_application(&self, scope: Arc<crate::txn::DependencyScope>) -> Self {
        let mut txn = self.clone();
        txn.scope = ExecutionScope::Application(scope);
        txn
    }

    pub(crate) fn application_scope(&self) -> Option<&crate::txn::DependencyScope> {
        match &self.scope {
            ExecutionScope::Host => None,
            ExecutionScope::Application(scope) => Some(scope),
        }
    }

    async fn class(
        &self,
        identity: &Link,
    ) -> tc_error::TCResult<crate::cluster::Cluster<crate::class::Class>> {
        self.kernel
            .classes
            .clone()
            .lookup(self, &identity.path()[1..])
            .await?
            .exact_item()?
            .ok_or_else(|| TCError::not_found(identity.to_string()))
    }

    pub fn id(&self) -> TxnId {
        self.id
    }

    pub fn has_claim(&self, link: &Link, required: Mode) -> bool {
        let snapshot = self.protocol_snapshot();
        let protocol_allows = if link.path()
            == &crate::uri::transaction_path(self.id)
                .parse::<pathlink::PathBuf>()
                .expect("transaction path")
        {
            snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.owner.as_ref())
                .is_some()
                && required.has(umask::USER_EXEC)
                && (!required.has(umask::USER_WRITE)
                    || snapshot.as_ref().is_some_and(|snapshot| snapshot.locked))
        } else {
            snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.leaders.contains_key(link.path()))
                && required.has(umask::USER_EXEC)
        };
        protocol_allows
            || self.auth_context.as_ref().is_some_and(|auth| {
                auth.claims
                    .iter()
                    .any(|claim| claim.claim.allows(link, required))
            })
    }

    /// Forwarded work is authorized only by a verified claim for this exact resource.
    pub(crate) fn may_mutate(&self, identity: &Link, resource: &pathlink::PathBuf) -> bool {
        self.has_claim(identity, umask::USER_WRITE)
            || (!self.autocommit && self.leader(resource).is_some())
    }

    pub fn auth_context(&self) -> Option<&AuthContext> {
        self.auth_context.as_ref()
    }

    pub(crate) fn raw_token(&self) -> Option<String> {
        self.protocol_claims
            .lock()
            .signed
            .as_ref()
            .map(|token| (**token).clone().into_jwt())
    }

    #[cfg(feature = "http-client")]
    pub(crate) fn authorization_header(&self) -> Option<String> {
        self.raw_token().map(|token| format!("Bearer {token}"))
    }

    pub(crate) async fn grant_claim(&self, claim: Claim) -> tc_error::TCResult<Self> {
        let authority = self.server.protocol_authority();
        self.grant(
            authority.actor.as_ref(),
            authority.host.clone(),
            claim.link,
            claim.mask,
        )
    }

    pub async fn context(
        &self,
    ) -> tc_error::TCResult<freqfs::DirLock<tc_collection::PersistentFile>> {
        self.server
            .workspace()
            .transaction_child(self.id, &self.workspace_path)
            .await
    }

    #[cfg(test)]
    pub(crate) fn with_claims(&self, claims: Vec<Claim>) -> Self {
        let mut txn = self.clone();
        txn.auth_context = Some(AuthContext {
            principal: "test".into(),
            verified_at_nanos: 0,
            claims: claims
                .into_iter()
                .map(|claim| crate::auth::AuthClaimContext {
                    host: "test".into(),
                    actor_id: "test".into(),
                    claim,
                })
                .collect(),
            signed: None,
        });
        txn
    }

    pub(super) fn with_signed_token(&self, token: SignedToken) -> tc_error::TCResult<Self> {
        super::validate_signed_token(self.id, &token)?;
        let txn = self.clone();
        txn.protocol_claims.lock().signed = Some(Arc::new(token));
        Ok(txn)
    }

    #[cfg(test)]
    pub(crate) fn lock_for_test(&self) -> tc_error::TCResult<Self> {
        let authority = self.server.protocol_authority();
        let txn = self.grant(
            authority.actor.as_ref(),
            authority.host.clone(),
            crate::uri::transaction_path(self.id)
                .parse()
                .expect("transaction path"),
            umask::USER_WRITE,
        )?;
        Ok(txn)
    }

    #[cfg(any(feature = "http-client", test))]
    pub(crate) fn with_auth_context(&self, auth_context: AuthContext) -> Self {
        let mut txn = self.clone();
        txn.auth_context = Some(auth_context);
        txn
    }

    pub(super) fn grant(
        &self,
        actor: &Actor,
        host: Link,
        resource: Link,
        mode: Mode,
    ) -> tc_error::TCResult<Self> {
        let now = SystemTime::now();
        let claim = Claim::new(resource, mode);

        let current = self.protocol_claims.lock().signed.clone();
        let signed = match current {
            Some(token) => actor
                .consume_and_sign((*token).clone(), host, crate::auth::wire_claim(claim), now)
                .map_err(|err| TCError::unauthorized(err.to_string()))?,
            None => {
                let token = Token::new(
                    host,
                    now,
                    self.server.ttl(),
                    actor.id().clone(),
                    crate::auth::wire_claim(claim),
                );
                actor
                    .sign_token(token)
                    .map_err(|err| TCError::unauthorized(err.to_string()))?
            }
        };

        self.with_signed_token(signed)
    }
}

impl tc_collection::StorageContext for TxnHandle {
    type File = tc_collection::PersistentFile;

    fn context(
        &self,
    ) -> impl std::future::Future<Output = tc_error::TCResult<freqfs::DirLock<Self::File>>> + Send
    {
        TxnHandle::context(self)
    }

    fn subcontext(&self, name: impl Into<String>) -> Self {
        TxnHandle::subcontext(self, name)
    }

    fn subcontext_unique(&self) -> Self {
        TxnHandle::subcontext_unique(self)
    }

    fn materialized_tensor_bytes(&self) -> usize {
        self.resources().limits().device.materialized_tensor_bytes
    }
}

impl Transaction for TxnHandle {
    fn id(&self) -> TxnId {
        self.id
    }
}

impl tc_state::StateExecutor for TxnHandle {
    async fn resolve_class(&self, identity: &Link) -> tc_error::TCResult<tc_state::ClassDef> {
        self.class(identity)
            .await
            .map(|class| class.state().definition().clone())
    }

    async fn get(&self, target: Link, key: tc_ir::Scalar) -> tc_error::TCResult<crate::State> {
        self.kernel.get(target, self.clone(), key).await
    }

    async fn put(
        &self,
        target: Link,
        key: tc_ir::Scalar,
        value: crate::State,
    ) -> tc_error::TCResult<()> {
        self.kernel.put(target, self.clone(), key, value).await
    }

    async fn post(
        &self,
        target: Link,
        params: tc_ir::Map<crate::State>,
    ) -> tc_error::TCResult<crate::State> {
        self.kernel.post(target, self.clone(), params).await
    }

    async fn delete(&self, target: Link, key: tc_ir::Scalar) -> tc_error::TCResult<()> {
        self.kernel.delete(target, self.clone(), key).await
    }

    async fn execute_op(
        &self,
        definition: tc_ir::OpDef,
        args: crate::State,
        subject: Option<crate::State>,
        declared_by: Option<Link>,
    ) -> tc_error::TCResult<crate::State> {
        let entered = self.enter_execution().await?;
        let txn = if let Some(identity) = declared_by {
            let class = entered.class(&identity).await?;
            entered.for_application(class.state().scope())
        } else {
            match &subject {
                Some(crate::State::Object(object)) => match object.as_ref() {
                    tc_state::Object::Instance(instance) => {
                        let identity = instance.class().identity();
                        let class = entered.class(identity).await?;
                        entered.for_application(class.state().scope())
                    }
                    _ => entered.clone(),
                },
                _ => entered.clone(),
            }
        };
        crate::op_executor::execute(&txn, definition, args, subject).await
    }
}

impl fmt::Debug for TxnHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxnHandle").field("id", &self.id).finish()
    }
}
