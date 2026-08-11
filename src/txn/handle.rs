use std::{
    collections::BTreeSet,
    fmt,
    sync::Arc,
    time::{Duration, SystemTime},
};

use pathlink::Link;
use tc_error::TCError;
use tc_ir::{Claim, NetworkTime, Transaction, TxnHeader, TxnId};
use umask::Mode;

use crate::auth::{Actor, SignedToken, Token};
use crate::gateway::RpcGateway;
use crate::workspace::Workspace;

#[derive(Clone, Debug)]
pub struct AuthClaimContext {
    pub host: String,
    pub actor_id: String,
    pub claim: Claim,
}

#[derive(Clone, Debug)]
pub struct AuthContext {
    pub principal: String,
    pub verified_at_nanos: u64,
    pub claims: Vec<AuthClaimContext>,
}

impl AuthContext {
    pub fn from_token_context(token: &crate::auth::TokenContext) -> Self {
        Self {
            principal: token.owner_id.clone(),
            verified_at_nanos: token.verified_at_nanos,
            claims: token
                .claims
                .iter()
                .map(|(host, actor_id, claim)| AuthClaimContext {
                    host: host.clone(),
                    actor_id: actor_id.clone(),
                    claim: claim.clone(),
                })
                .collect(),
        }
    }

    pub fn token_hosts(&self) -> Vec<String> {
        let mut hosts = BTreeSet::new();
        for claim in &self.claims {
            hosts.insert(claim.host.clone());
        }

        hosts.into_iter().collect()
    }
}

#[derive(Clone)]
pub struct TxnHandle {
    pub(super) id: TxnId,
    pub(super) claim: Claim,
    pub(super) claims: Vec<Claim>,
    pub(super) owner_id: Option<String>,
    pub(super) bearer_token: Option<String>,
    pub(super) resolver: Option<Arc<dyn RpcGateway>>,
    pub(super) ttl: Duration,
    pub(super) token: Option<Arc<SignedToken>>,
    pub(super) auth_context: Option<AuthContext>,
    pub(super) workspace: Option<Workspace>,
    pub(super) workspace_path: Vec<String>,
    pub(super) resources: crate::HostResources,
    pub(super) deadline: crate::Deadline,
}

impl TxnHandle {
    pub fn subcontext(&self, name: impl Into<String>) -> Self {
        let mut txn = self.clone();
        txn.workspace_path.push(name.into());
        txn
    }

    pub fn subcontext_unique(&self) -> Self {
        let workspace = self
            .workspace
            .as_ref()
            .expect("a local transaction workspace is required for collection storage");

        self.subcontext("tmp").subcontext(workspace.unique_name())
    }

    pub(crate) fn with_deadline(&self, deadline: crate::Deadline) -> Self {
        let mut txn = self.clone();
        txn.deadline = deadline;
        txn
    }

    pub(crate) fn resources(&self) -> &crate::HostResources {
        &self.resources
    }

    pub(crate) fn deadline(&self) -> crate::Deadline {
        self.deadline
    }
    pub fn with_resolver(&self, resolver: Arc<dyn RpcGateway>) -> Self {
        Self {
            id: self.id,
            claim: self.claim.clone(),
            claims: self.claims.clone(),
            owner_id: self.owner_id.clone(),
            bearer_token: self.bearer_token.clone(),
            resolver: Some(resolver),
            ttl: self.ttl,
            token: self.token.clone(),
            auth_context: self.auth_context.clone(),
            workspace: self.workspace.clone(),
            workspace_path: self.workspace_path.clone(),
            resources: self.resources.clone(),
            deadline: self.deadline,
        }
    }

    pub fn id(&self) -> TxnId {
        self.id
    }

    pub fn claim(&self) -> &Claim {
        &self.claim
    }

    pub fn claims(&self) -> &[Claim] {
        &self.claims
    }

    pub fn has_claim(&self, link: &Link, required: Mode) -> bool {
        self.claims.iter().any(|claim| claim.allows(link, required))
    }

    pub fn owner_id(&self) -> Option<&str> {
        self.owner_id.as_deref()
    }

    pub fn auth_context(&self) -> Option<&AuthContext> {
        self.auth_context.as_ref()
    }

    pub(crate) fn raw_token(&self) -> Option<&str> {
        self.bearer_token.as_deref()
    }

    pub(crate) fn has_signed_token(&self) -> bool {
        self.token.is_some()
    }

    pub(crate) fn authorization_header(&self) -> Option<String> {
        self.bearer_token
            .as_ref()
            .map(|token| format!("Bearer {token}"))
    }

    pub(crate) fn with_bearer_token(&self, bearer_token: String) -> Self {
        Self {
            id: self.id,
            claim: self.claim.clone(),
            claims: self.claims.clone(),
            owner_id: self.owner_id.clone(),
            bearer_token: Some(bearer_token),
            resolver: self.resolver.clone(),
            ttl: self.ttl,
            token: self.token.clone(),
            auth_context: self.auth_context.clone(),
            workspace: self.workspace.clone(),
            workspace_path: self.workspace_path.clone(),
            resources: self.resources.clone(),
            deadline: self.deadline,
        }
    }

    pub(crate) fn without_bearer_token(&self) -> Self {
        Self {
            id: self.id,
            claim: self.claim.clone(),
            claims: self.claims.clone(),
            owner_id: self.owner_id.clone(),
            bearer_token: None,
            resolver: self.resolver.clone(),
            ttl: self.ttl,
            token: self.token.clone(),
            auth_context: self.auth_context.clone(),
            workspace: self.workspace.clone(),
            workspace_path: self.workspace_path.clone(),
            resources: self.resources.clone(),
            deadline: self.deadline,
        }
    }

    pub fn header(&self) -> TxnHeader {
        TxnHeader::from_transaction(self)
    }

    pub async fn context(
        &self,
    ) -> tc_error::TCResult<freqfs::DirLock<tc_collection::PersistentFile>> {
        self.workspace
            .as_ref()
            .ok_or_else(|| {
                TCError::internal("transaction workspace is not configured at bootstrap")
            })?
            .transaction_child(self.id, &self.workspace_path)
            .await
    }

    pub(super) async fn remove_workspace(&self) -> tc_error::TCResult<()> {
        if let Some(workspace) = &self.workspace {
            workspace.remove_transaction(self.id).await?;
        }

        Ok(())
    }

    pub(crate) fn with_claims(&self, claims: Vec<Claim>) -> Self {
        Self {
            id: self.id,
            claim: self.claim.clone(),
            claims,
            owner_id: self.owner_id.clone(),
            bearer_token: self.bearer_token.clone(),
            resolver: self.resolver.clone(),
            ttl: self.ttl,
            token: self.token.clone(),
            auth_context: self.auth_context.clone(),
            workspace: self.workspace.clone(),
            workspace_path: self.workspace_path.clone(),
            resources: self.resources.clone(),
            deadline: self.deadline,
        }
    }

    pub(super) fn with_signed_token(&self, token: SignedToken) -> tc_error::TCResult<Self> {
        let canonical_claim = super::validate_signed_token(self.id, &token)?;
        let bearer_token = token.clone().into_jwt();
        let claims = token
            .claims()
            .iter()
            .map(|(_, _, claim)| claim.clone())
            .collect();
        Ok(Self {
            id: self.id,
            claim: canonical_claim,
            claims,
            owner_id: self.owner_id.clone(),
            bearer_token: Some(bearer_token),
            resolver: self.resolver.clone(),
            ttl: self.ttl,
            token: Some(Arc::new(token)),
            auth_context: self.auth_context.clone(),
            workspace: self.workspace.clone(),
            workspace_path: self.workspace_path.clone(),
            resources: self.resources.clone(),
            deadline: self.deadline,
        })
    }

    pub(crate) fn with_auth_context(&self, auth_context: AuthContext) -> Self {
        Self {
            id: self.id,
            claim: self.claim.clone(),
            claims: self.claims.clone(),
            owner_id: self.owner_id.clone(),
            bearer_token: self.bearer_token.clone(),
            resolver: self.resolver.clone(),
            ttl: self.ttl,
            token: self.token.clone(),
            auth_context: Some(auth_context),
            workspace: self.workspace.clone(),
            workspace_path: self.workspace_path.clone(),
            resources: self.resources.clone(),
            deadline: self.deadline,
        }
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

        let signed = match &self.token {
            Some(token) => actor
                .consume_and_sign((**token).clone(), host, claim, now)
                .map_err(|err| TCError::unauthorized(err.to_string()))?,
            None => {
                let token = Token::new(host, now, self.ttl, actor.id().clone(), claim);
                actor
                    .sign_token(token)
                    .map_err(|err| TCError::unauthorized(err.to_string()))?
            }
        };

        self.with_signed_token(signed)
    }

    pub fn has_permission(
        &self,
        actor: &Actor,
        resource: &Link,
        required: Mode,
    ) -> tc_error::TCResult<bool> {
        let Some(token) = &self.token else {
            return Ok(false);
        };

        for (_host, actor_id, claim) in token.claims() {
            if actor_id == actor.id() && claim.allows(resource, required) {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

impl tc_collection::StorageContext for TxnHandle {
    fn context(
        &self,
    ) -> impl std::future::Future<
        Output = tc_error::TCResult<freqfs::DirLock<tc_collection::PersistentFile>>,
    > + Send {
        TxnHandle::context(self)
    }

    fn subcontext(&self, name: impl Into<String>) -> Self {
        TxnHandle::subcontext(self, name)
    }

    fn subcontext_unique(&self) -> Self {
        TxnHandle::subcontext_unique(self)
    }

    fn materialized_tensor_bytes(&self) -> usize {
        self.resources.limits().device.materialized_tensor_bytes
    }
}

impl Transaction for TxnHandle {
    fn id(&self) -> TxnId {
        self.id
    }

    fn timestamp(&self) -> NetworkTime {
        self.id.timestamp()
    }

    fn claim(&self) -> &Claim {
        &self.claim
    }
}

impl fmt::Debug for TxnHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxnHandle")
            .field("id", &self.id)
            .field("claim", &self.claim)
            .field("owner_id", &self.owner_id)
            .finish()
    }
}
