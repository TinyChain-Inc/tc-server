use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::SystemTime;

use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use pathlink::Link;
use tc_error::TCError;
use tc_ir::{IntoView, Transaction, TxnId};
use tokio::sync::{mpsc, oneshot};
use umask::Mode;

use crate::Claim;
use crate::auth::{AuthContext, SignedToken, Token};

fn execution_unavailable(message: impl std::fmt::Display) -> TCError {
    TCError::resource_unavailable(
        message,
        tc_error::Pressure::new("/host/resource/graph", tc_error::PressureReason::Saturated),
    )
}

#[derive(Clone)]
enum ExecutionScope {
    Host,
    Application(Arc<crate::txn::DependencyScope>),
}

struct ClaimContext {
    signed: Option<Arc<SignedToken>>,
}

/// Transaction authority and retained storage lifetime. Clones delay expiry
/// materialization until released, but do not extend permission to execute.
/// Native callers must retain this capability while using transaction-backed
/// State, views, or streams, including after a request owner has been released.
#[derive(Clone)]
pub struct TxnHandle {
    id: TxnId,
    server: super::TxnServer,
    kernel: Arc<crate::kernel::KernelInner>,
    scope: ExecutionScope,
    auth_context: Option<AuthContext>,
    protocol_claims: Arc<parking_lot::Mutex<ClaimContext>>,
    autocommit: bool,
    pub(super) recovering: bool,
    read_only: bool,
    participant: Option<Arc<crate::cluster::Participation>>,
    workspace_path: Vec<String>,
    deadline: crate::Deadline,
    expires: tokio::time::Instant,
    activity: Arc<[tokio_util::task::task_tracker::TaskTrackerToken]>,
    graph_admitted: bool,
    execution_budget: Arc<AtomicUsize>,
    execution_depth: usize,
    execution: Option<mpsc::WeakSender<Invocation>>,
}

impl TxnHandle {
    pub(crate) fn fatal_recovery(&self, error: impl std::fmt::Display) -> ! {
        self.server
            .fatal_recovery(format_args!("transaction recovery required: {error}"))
    }

    /// Apply host policy only to locally classified storage failures.
    pub(crate) fn storage_error(&self, error: tc_error::TCError) -> tc_error::TCError {
        if error.requires_recovery() && !self.recovering {
            self.fatal_recovery(format!(
                "live storage integrity failure: {error}; causes: {:?}",
                error.causes()
            ));
        }

        error
    }

    /// Construct from authentication and its already-validated read-only scope.
    pub(super) fn new(
        id: TxnId,
        server: super::TxnServer,
        kernel: Arc<crate::kernel::KernelInner>,
        context: Option<(&AuthContext, bool)>,
        autocommit: bool,
        expires: tokio::time::Instant,
        activity: tokio_util::task::task_tracker::TaskTrackerToken,
    ) -> Self {
        let read_only = context.is_some_and(|(_, read_only)| read_only);
        let context = context.map(|(auth, _)| auth);
        let mut auth_context = context.cloned();
        if let Some(context) = auth_context.as_mut() {
            context.take_signed();
        }

        Self {
            id,
            server: server.clone(),
            kernel,
            scope: ExecutionScope::Host,
            auth_context,
            protocol_claims: Arc::new(parking_lot::Mutex::new(ClaimContext {
                signed: if autocommit {
                    None
                } else {
                    context.and_then(AuthContext::signed).cloned()
                },
            })),
            // Returning to the owning host does not turn a dependency into its caller.
            autocommit,
            recovering: false,
            read_only,
            participant: None,
            workspace_path: Vec::new(),
            deadline: crate::Deadline::at(server.resources().deadline().instant().min(expires)),
            expires,
            activity: Arc::from([activity]),
            graph_admitted: false,
            execution_budget: Arc::new(AtomicUsize::new(
                server.resources().limits().execution.max_op_invocations,
            )),
            execution_depth: 0,
            execution: None,
        }
    }

    /// A retained capability delays materialization, not its fixed expiry.
    pub fn check_active(&self) -> tc_error::TCResult<()> {
        if self.expires <= tokio::time::Instant::now() {
            Err(TCError::conflict("transaction has expired"))
        } else {
            Ok(())
        }
    }

    /// Acquire an owned view within the projection budget and fixed expiry.
    /// Adapters retain the same budget through encoding and foreign conversion.
    pub async fn view(
        &self,
        state: crate::State,
        deadline: crate::Deadline,
    ) -> tc_error::TCResult<tc_state::StateView> {
        self.check_active()?;
        deadline.check()?;
        let view = state.into_view(self.clone()).await?;
        self.check_active()?;
        deadline.check()?;
        Ok(view)
    }

    /// Latest deadline for reuse of a retained native result.
    pub fn expiry_deadline(&self) -> crate::Deadline {
        crate::Deadline::at(self.expires)
    }

    pub(crate) fn retain_source(&mut self, source: &Self) -> tc_error::TCResult<()> {
        self.check_active()?;
        source.check_active()?;
        let mut activity: Option<Vec<_>> = None;
        for token in source.activity.iter() {
            if !activity
                .as_deref()
                .unwrap_or(&self.activity)
                .iter()
                .any(|retained| {
                    tokio_util::task::TaskTracker::ptr_eq(
                        retained.task_tracker(),
                        token.task_tracker(),
                    )
                })
            {
                let activity = activity.get_or_insert_with(|| self.activity.to_vec());
                if activity.len() >= self.execution_limits().max_op_invocations {
                    return Err(execution_unavailable(
                        "retained transaction capability limit exceeded",
                    ));
                }

                activity.push(token.clone());
            }
        }

        // Retain only flat tokens, never another handle or its Kernel ownership.
        if let Some(activity) = activity {
            self.activity = activity.into();
        }

        self.expires = self.expires.min(source.expires);
        self.deadline = crate::Deadline::at(self.deadline.instant().min(self.expires));
        Ok(())
    }

    pub(crate) fn protocol_snapshot(&self) -> Option<super::token::ProtocolSnapshot> {
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

    #[cfg(feature = "http-client")]
    pub(crate) fn claim_owner(&self) -> tc_error::TCResult<()> {
        if self
            .protocol_snapshot()
            .and_then(|claims| claims.owner)
            .is_none()
        {
            self.grant(
                self.server.protocol_authority(),
                crate::uri::transaction_path(self.id)
                    .parse()
                    .expect("transaction path"),
                umask::USER_EXEC,
            )?;
        }

        Ok(())
    }

    pub(crate) async fn adopt_claims(&self, raw: &str) -> tc_error::TCResult<Self> {
        let auth = self.server.verifier().verify(raw.to_owned()).await?;
        let signed = auth
            .signed()
            .ok_or_else(|| TCError::unauthorized("missing signed transaction authority"))?;

        let claims = super::validate_signed_token(self.id, signed)?;
        if self
            .protocol_snapshot()
            .and_then(|claims| claims.owner)
            .is_some_and(|owner| Some(owner) != claims.owner)
        {
            return Err(TCError::unauthorized(
                "bootstrap changed transaction ownership",
            ));
        }

        let mut txn = self.clone();
        txn.read_only |= claims.read_only;
        txn.protocol_claims.lock().signed = Some(Arc::clone(signed));
        txn.auth_context = Some(auth);
        Ok(txn)
    }

    pub(crate) fn owns_invocation(&self) -> bool {
        self.autocommit
            && self.protocol_snapshot().is_some_and(|claims| {
                claims.owner
                    == Some((
                        self.server.protocol_authority().host().to_string(),
                        self.server.protocol_authority().actor_id().to_string(),
                    ))
            })
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
                    .extend((**token).clone(), grants, now)
                    .map_err(|error| TCError::unauthorized(error.to_string()))?
            } else {
                authority
                    .sign(
                        Token::new(
                            authority.host().clone(),
                            now,
                            self.server.ttl(),
                            authority.actor_id().to_string(),
                            grants,
                        )
                        .map_err(|error| TCError::unauthorized(error.to_string()))?,
                    )
                    .map_err(|error| TCError::unauthorized(error.to_string()))?
            };

            claims.signed = Some(Arc::new(signed));
        }

        Ok(())
    }

    pub(crate) fn lock_coordinator(
        &self,
        outcome: super::TransactionOutcome,
        automatic: bool,
    ) -> tc_error::TCResult<Option<pathlink::PathBuf>> {
        let mut claims = self.protocol_claims.lock();
        if automatic && !self.autocommit {
            return Ok(None);
        }

        let snapshot = claims
            .signed
            .as_deref()
            .map(|token| crate::txn::protocol_snapshot(self.id, token))
            .transpose()?;

        let authority = self.server.protocol_authority();
        let owns = snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.owner.as_ref())
            .is_some_and(|(host, actor)| {
                authority.host() == host.as_str() && actor == authority.actor_id()
            });

        if automatic {
            let Some(snapshot) = &snapshot else {
                return Ok(None);
            };

            if !owns || snapshot.coordinator.is_none() || (!outcome.commits() && snapshot.locked) {
                return Ok(None);
            }
        }

        let snapshot =
            snapshot.ok_or_else(|| TCError::conflict("cannot decide an ownerless transaction"))?;

        if snapshot
            .decision
            .is_some_and(|selected| selected != outcome)
        {
            return Err(TCError::conflict("inconsistent transaction decision"));
        }

        let coordinator = snapshot
            .coordinator
            .ok_or_else(|| TCError::conflict("transaction has no resource coordinator"))?;

        snapshot
            .owner
            .ok_or_else(|| TCError::conflict("cannot decide an ownerless transaction"))?;

        if !owns {
            return Err(TCError::unauthorized(
                "only the transaction-owning resource may coordinate a decision",
            ));
        }

        if !snapshot.locked {
            let signed = authority
                .extend(
                    (**claims.signed.as_ref().expect("snapshot has a token")).clone(),
                    crate::auth::wire_claim(crate::Claim::new(
                        crate::uri::transaction_path(self.id)
                            .parse()
                            .expect("transaction path"),
                        if outcome.commits() {
                            umask::USER_WRITE
                        } else {
                            umask::USER_READ | umask::USER_WRITE
                        },
                    )),
                    SystemTime::now(),
                )
                .map_err(|error| TCError::unauthorized(error.to_string()))?;

            claims.signed = Some(Arc::new(signed));
        }

        Ok(Some(coordinator))
    }

    pub(crate) fn read_only(&self) -> Self {
        let mut txn = self.clone();
        txn.read_only = true;
        txn
    }

    pub(crate) fn check_write(&self) -> tc_error::TCResult<()> {
        self.check_active()?;
        if self.read_only {
            Err(TCError::unauthorized(
                "GET execution cannot mutate resources",
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn for_resource(&self, participant: Arc<crate::cluster::Participation>) -> Self {
        let mut txn = self.clone();
        txn.participant = Some(participant);
        txn
    }

    pub(crate) fn register_peer_identity(
        &self,
        replica: &crate::replication::Replica,
    ) -> tc_error::TCResult<()> {
        self.kernel.register_peer_identity(replica)
    }

    pub(crate) fn record_dependency(&self, target: Link) -> tc_error::TCResult<()> {
        self.participant
            .as_ref()
            .ok_or_else(|| TCError::conflict("dependency has no resource owner"))?
            .enlist(target, self.execution_limits().max_graph_edges)
    }

    // A dependent call owns a branch, not the caller's mutable signing history.
    pub(crate) fn branch(&self) -> Self {
        let mut txn = self.clone();
        txn.protocol_claims = Arc::new(parking_lot::Mutex::new(ClaimContext {
            signed: self.protocol_claims.lock().signed.clone(),
        }));

        txn.autocommit = false;
        txn
    }

    pub(crate) fn decision_branch(
        &self,
        path: &pathlink::PathBuf,
        outcome: super::TransactionOutcome,
    ) -> tc_error::TCResult<Self> {
        let snapshot = self
            .protocol_snapshot()
            .ok_or_else(|| TCError::unauthorized("missing decision authority"))?;

        if !snapshot.locked
            || snapshot
                .decision
                .is_some_and(|previous| previous != outcome)
        {
            return Err(TCError::conflict("inconsistent transaction decision"));
        }

        if snapshot.visited.len() >= self.execution_limits().max_execution_depth {
            return Err(execution_unavailable(
                "transaction decision ancestry limit exceeded",
            ));
        }

        self.branch().grant(
            self.server.protocol_authority(),
            path.clone().into(),
            if outcome.commits() {
                umask::USER_WRITE
            } else {
                umask::USER_READ
            },
        )
    }

    pub(crate) async fn decide_local(
        &self,
        target: &Link,
        outcome: super::TransactionOutcome,
    ) -> tc_error::TCResult<()> {
        let result = Box::pin(self.kernel.dispatch(
            self,
            target,
            if outcome.commits() {
                tc_ir::Method::Put
            } else {
                tc_ir::Method::Delete
            },
            None,
        ))
        .await?;

        assert!(
            result.is_none(),
            "dependency decision returned an ordinary response"
        );

        Ok(())
    }

    fn subcontext(&self, name: impl Into<String>) -> Self {
        let mut txn = self.clone();
        txn.workspace_path.push(name.into());
        txn
    }

    fn subcontext_unique(&self) -> Self {
        self.subcontext("tmp")
            .subcontext(self.server.workspace().unique_name())
    }

    pub(crate) fn restrict_deadline(&self, deadline: crate::Deadline) -> Self {
        let mut txn = self.clone();
        if deadline.instant() < txn.deadline.instant() {
            txn.deadline = deadline;
        }
        txn
    }

    // Cleanup retains the original transaction and authority, but has its own
    // bounded time to deliver rollback after the execution deadline expires.
    pub(crate) fn for_cleanup(&self, deadline: crate::Deadline) -> Self {
        let mut txn = self.clone();
        txn.deadline = deadline;
        txn
    }

    pub(crate) fn request_body_limit(&self) -> usize {
        self.server.resources().limits().ingress.request_body_bytes
    }

    pub(crate) fn application_body_limit(&self) -> usize {
        self.server
            .resources()
            .limits()
            .ingress
            .application_body_bytes
    }

    pub(crate) fn application_admission(&self) -> crate::resources::ApplicationAdmission {
        self.server.resources().application_admission(self.deadline)
    }

    pub(crate) fn execution_limits(&self) -> crate::ExecutionLimits {
        self.server.resources().limits().execution.clone()
    }

    pub(crate) async fn admit_graph_op(
        &self,
    ) -> tc_error::TCResult<crate::resources::CapacityPermit> {
        self.server.resources().admit_graph_op(self.deadline).await
    }

    pub(crate) async fn admit_outbound(
        &self,
    ) -> tc_error::TCResult<crate::resources::CapacityPermit> {
        self.server.resources().admit_outbound(self.deadline).await
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
        self.check_active()?;
        self.deadline.check()?;
        let limits = &self.server.resources().limits().execution;
        if self.execution_depth >= limits.max_execution_depth {
            return Err(execution_unavailable(format!(
                "operation nesting exceeds the {}-level limit",
                limits.max_execution_depth
            )));
        }
        self.execution_budget
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .map_err(|_| execution_unavailable("operation invocation budget exhausted"))?;
        tokio::task::yield_now().await;
        self.deadline.check()?;
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
        self.kernel.resolve_class(self, identity).await
    }

    pub fn id(&self) -> TxnId {
        self.id
    }

    pub(crate) fn has_claim(&self, link: &Link, required: Mode) -> bool {
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
                && required == umask::USER_EXEC
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
        if self.has_claim(identity, umask::USER_WRITE) {
            return true;
        }

        !self.autocommit
            && self.auth_context.as_ref().is_some_and(|auth| {
                let leader = self.leader(resource);
                auth.claims.iter().any(|claim| {
                    claim.claim.mask == umask::USER_EXEC
                        && claim.claim.link.path() == resource
                        && leader.as_ref().is_some_and(|(host, actor)| {
                            host == &claim.host && actor == &claim.actor_id
                        })
                })
            })
    }

    pub(crate) fn auth_context(&self) -> Option<&AuthContext> {
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
        let permission = Claim::new(claim.link.path().clone().into(), claim.mask);
        let mut txn = self.branch().grant(authority, claim.link, claim.mask)?;
        let auth = txn
            .auth_context
            .take()
            .unwrap_or_else(|| AuthContext::new(authority.actor_id().to_string()));

        txn.auth_context = Some(auth.with_claim(
            authority.host().to_string(),
            authority.actor_id().to_string(),
            permission,
        ));

        if self.read_only
            && !txn
                .protocol_snapshot()
                .is_some_and(|snapshot| snapshot.read_only)
        {
            txn.grant(
                authority,
                crate::uri::transaction_path(self.id)
                    .parse()
                    .expect("transaction path"),
                umask::USER_READ,
            )
        } else {
            Ok(txn)
        }
    }

    async fn context(
        &self,
    ) -> tc_error::TCResult<freqfs::DirLock<crate::storage::ApplicationBlock>> {
        self.server
            .workspace()
            .transaction_child(self.id, &self.workspace_path)
            .await
            .map_err(|error| self.storage_error(error))
    }

    #[cfg(test)]
    pub(crate) fn with_claims(&self, claims: Vec<Claim>) -> Self {
        let mut txn = self.clone();
        txn.auth_context = Some(
            claims
                .into_iter()
                .fold(AuthContext::new("test"), |context, claim| {
                    context.with_claim("test".into(), "test".into(), claim)
                }),
        );
        txn
    }

    pub(super) fn with_signed_token(&self, token: SignedToken) -> tc_error::TCResult<Self> {
        let claims = super::validate_signed_token(self.id, &token)?;
        let mut txn = self.clone();
        txn.read_only |= claims.read_only;
        txn.protocol_claims.lock().signed = Some(Arc::new(token));
        Ok(txn)
    }

    #[cfg(test)]
    pub(crate) fn lock_for_test(&self) -> tc_error::TCResult<Self> {
        let authority = self.server.protocol_authority();
        let txn = self.grant(
            authority,
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
        authority: &crate::ProtocolAuthority,
        resource: Link,
        mode: Mode,
    ) -> tc_error::TCResult<Self> {
        let now = SystemTime::now();
        let claim = Claim::new(resource, mode);

        let current = self.protocol_claims.lock().signed.clone();
        let signed = match current {
            Some(token) => authority
                .extend((*token).clone(), crate::auth::wire_claim(claim), now)
                .map_err(|err| TCError::unauthorized(err.to_string()))?,
            None => {
                let token = Token::new(
                    authority.host().clone(),
                    now,
                    self.server.ttl(),
                    authority.actor_id().to_string(),
                    crate::auth::wire_claim(claim),
                )
                .map_err(|error| TCError::unauthorized(error.to_string()))?;

                authority
                    .sign(token)
                    .map_err(|err| TCError::unauthorized(err.to_string()))?
            }
        };

        self.with_signed_token(signed)
    }
}

impl tc_collection::StorageContext for TxnHandle {
    type File = crate::storage::ApplicationBlock;

    fn storage_error(&self, error: tc_error::TCError) -> tc_error::TCError {
        TxnHandle::storage_error(self, error)
    }

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
        self.server
            .resources()
            .limits()
            .execution
            .materialized_tensor_bytes
    }
}

impl Transaction for TxnHandle {
    fn id(&self) -> TxnId {
        self.id
    }
}

impl tc_state::Gateway for TxnHandle {
    fn check_write(&self) -> tc_error::TCResult<()> {
        TxnHandle::check_write(self)
    }

    async fn resolve_class(&self, identity: &Link) -> tc_error::TCResult<tc_state::ClassDef> {
        self.class(identity)
            .await
            .map(|class| class.state().definition().clone())
    }

    async fn get(&self, target: Link, key: tc_ir::Scalar) -> tc_error::TCResult<crate::State> {
        self.kernel
            .get(target, self.clone(), key)
            .await
            .map_err(|error| self.storage_error(error))
    }

    async fn put(
        &self,
        target: Link,
        key: tc_ir::Scalar,
        value: crate::State,
    ) -> tc_error::TCResult<()> {
        self.kernel
            .put(target, self.clone(), key, value)
            .await
            .map_err(|error| self.storage_error(error))
    }

    async fn post(
        &self,
        target: Link,
        params: tc_ir::Map<crate::State>,
    ) -> tc_error::TCResult<crate::State> {
        self.kernel
            .post(target, self.clone(), params)
            .await
            .map_err(|error| self.storage_error(error))
    }

    async fn delete(&self, target: Link, key: tc_ir::Scalar) -> tc_error::TCResult<()> {
        self.kernel
            .delete(target, self.clone(), key)
            .await
            .map_err(|error| self.storage_error(error))
    }
}

impl tc_state::OpExecutor for TxnHandle {
    async fn execute_op(
        &self,
        definition: tc_ir::OpDef,
        args: crate::State,
        subject: Option<crate::State>,
        declared_by: Option<Link>,
    ) -> tc_error::TCResult<crate::State> {
        let entered = self.enter_execution().await?;
        let entered = match &definition {
            tc_ir::OpDef::Get(_) => entered.read_only(),
            tc_ir::OpDef::Put(_) | tc_ir::OpDef::Delete(_) => {
                entered.check_write()?;
                entered
            }
            tc_ir::OpDef::Post(_) => entered,
        };

        let identity = declared_by.as_ref().or_else(|| match subject.as_ref() {
            Some(tc_state::State::Object(object)) => match object.as_ref() {
                tc_state::Object::Instance(instance) => Some(instance.class().identity()),
                _ => None,
            },
            _ => None,
        });

        let txn = if let Some(identity) = identity {
            let class = entered.class(identity).await?;
            entered.for_application(class.state().scope())
        } else {
            entered
        };

        if let Some(execution) = &txn.execution {
            let sender = execution.upgrade().ok_or_else(execution_cancelled)?;
            let (response, result) = oneshot::channel();
            sender
                .send(Invocation {
                    txn,
                    definition,
                    args,
                    subject,
                    response,
                })
                .await
                .map_err(|_| execution_cancelled())?;

            drop(sender);
            result.await.map_err(|_| execution_cancelled())?
        } else {
            drive_execution(txn, definition, args, subject).await
        }
    }
}

// The process owns suspended native invocations. Descendants retain only a weak
// queue handle, so cancellation cannot leave a driver/transaction ownership cycle.
struct Invocation {
    txn: TxnHandle,
    definition: tc_ir::OpDef,
    args: crate::State,
    subject: Option<crate::State>,
    response: oneshot::Sender<tc_error::TCResult<crate::State>>,
}

fn execution_cancelled() -> TCError {
    TCError::new(
        tc_error::ErrorKind::Unavailable,
        "native execution process cancelled",
    )
}

async fn run_invocation(request: Invocation) -> Option<tc_error::TCResult<crate::State>> {
    let Invocation {
        txn,
        definition,
        args,
        subject,
        mut response,
    } = request;

    tokio::select! {
        biased;
        _ = response.closed() => {},
        result = async {
            // Queueing retains the caller's absolute deadline.
            if txn.deadline.is_expired() { return Err(txn.deadline.exceeded()); }
            crate::op_executor::execute(&txn, definition, args, subject).await
        } => { let _ = response.send(result); }
    }

    None
}

async fn drive_execution(
    mut txn: TxnHandle,
    definition: tc_ir::OpDef,
    args: crate::State,
    subject: Option<crate::State>,
) -> tc_error::TCResult<crate::State> {
    let capacity = txn.execution_limits().parallel_graph_ops;
    let (sender, mut requests) = mpsc::channel::<Invocation>(capacity);
    txn.execution = Some(sender.downgrade());
    // This owner keeps the queue alive while every invocation is suspended.
    let _sender = sender;
    let mut active = FuturesUnordered::new();
    active.push(
        async move { Some(crate::op_executor::execute(&txn, definition, args, subject).await) }
            .boxed(),
    );

    loop {
        tokio::select! {
            Some(request) = requests.recv() => {
                if request.response.is_closed() { continue; }
                active.push(run_invocation(request).boxed());
            }
            Some(result) = active.next() => {
                if let Some(result) = result { return result; }
            }
        }
    }
}

impl fmt::Debug for TxnHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxnHandle").field("id", &self.id).finish()
    }
}

#[cfg(test)]
#[path = "../../tests/support/execution_driver.rs"]
mod execution_driver_tests;
