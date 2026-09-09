use std::{collections::BTreeSet, fmt, sync::Arc, time::SystemTime};

use pathlink::Link;
use tc_error::TCError;
use tc_ir::{Claim, NetworkTime, Transaction, TxnId};
use umask::Mode;

use crate::auth::{Actor, SignedToken, Token};
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

#[derive(Clone)]
pub(crate) enum ExecutionScope {
    Host,
    Application(Arc<crate::application::ApplicationScope>),
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
        self.claims
            .iter()
            .map(|claim| claim.host.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

#[derive(Clone)]
pub struct TxnHandle {
    pub(super) id: TxnId,
    pub(super) claim: Claim,
    pub(super) server: super::TxnServer,
    pub(crate) runtime: Arc<crate::kernel::HostRuntime>,
    pub(super) scope: ExecutionScope,
    pub(super) auth_context: Option<AuthContext>,
    pub(crate) protocol_claims: Arc<parking_lot::Mutex<crate::cluster::ClaimState>>,
    pub(super) workspace_path: Vec<String>,
    pub(super) deadline: crate::Deadline,
    pub(super) graph_admitted: bool,
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

    pub(crate) fn register_resource(
        &self,
        resource: crate::cluster::HostResource,
    ) -> tc_error::TCResult<()> {
        let path = resource.path().clone();
        let mut claims = self.protocol_claims.lock();
        if claims.resources.get(&path).is_some_and(|(existing, _)| {
            std::mem::discriminant(existing) != std::mem::discriminant(&resource)
        }) {
            return Err(TCError::conflict(
                "resource type changed during a transaction",
            ));
        }
        claims.resources.entry(path).or_insert((resource, false));
        Ok(())
    }

    pub(crate) fn coordinator(&self) -> Option<crate::cluster::HostResource> {
        self.protocol_claims.lock().coordinator.clone()
    }

    pub(crate) fn mark_resource_mutated(&self, path: &pathlink::PathBuf) -> tc_error::TCResult<()> {
        let mut claims = self.protocol_claims.lock();
        let (_, mutated) = claims.resources.get_mut(path).ok_or_else(|| {
            TCError::conflict(format!("resource {path} mutated before it was claimed"))
        })?;
        *mutated = true;
        Ok(())
    }

    pub(crate) fn claim_cluster(
        &self,
        path: &pathlink::PathBuf,
        resource: crate::cluster::HostResource,
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
            if ownerless {
                claims.coordinator = Some(resource.clone());
            }
        }
        drop(claims);
        self.register_resource(resource)
    }

    pub(crate) fn lock_resources(
        &self,
        coordinator: &crate::cluster::HostResource,
        require_mutation: bool,
    ) -> tc_error::TCResult<Option<Vec<crate::cluster::HostResource>>> {
        let mut claims = self.protocol_claims.lock();
        if require_mutation
            && (!claims.autocommit || !claims.resources.values().any(|(_, mutated)| *mutated))
        {
            return Ok(None);
        }
        if !claims.coordinator.as_ref().is_some_and(|owned| {
            owned.path() == coordinator.path()
                && std::mem::discriminant(owned) == std::mem::discriminant(coordinator)
        }) {
            return Err(TCError::unauthorized(
                "only the first owning resource may coordinate a decision",
            ));
        }
        let snapshot = claims
            .signed
            .as_deref()
            .map(|token| crate::txn::protocol_snapshot(self.id, token))
            .transpose()?
            .ok_or_else(|| TCError::conflict("cannot decide an ownerless transaction"))?;
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
                    crate::auth::wire_claim(tc_ir::Claim::new(
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
        Ok(Some(
            claims
                .resources
                .values()
                .map(|(resource, _)| resource.clone())
                .collect(),
        ))
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

    pub(crate) fn for_application(&self, scope: Arc<crate::application::ApplicationScope>) -> Self {
        let mut txn = self.clone();
        txn.scope = ExecutionScope::Application(scope);
        txn
    }

    pub(crate) fn application_scope(&self) -> Option<&crate::application::ApplicationScope> {
        match &self.scope {
            ExecutionScope::Host => None,
            ExecutionScope::Application(scope) => Some(scope),
        }
    }

    pub fn id(&self) -> TxnId {
        self.id
    }

    pub fn claim(&self) -> &Claim {
        &self.claim
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
            || self.claim.allows(link, required)
            || self.auth_context.as_ref().is_some_and(|auth| {
                auth.claims
                    .iter()
                    .any(|claim| claim.claim.allows(link, required))
            })
    }

    pub fn auth_context(&self) -> Option<&AuthContext> {
        self.auth_context.as_ref()
    }

    #[cfg(feature = "http-server")]
    pub(crate) fn has_protocol_claim(&self, resource: &pathlink::PathBuf) -> bool {
        self.protocol_snapshot()
            .is_some_and(|snapshot| snapshot.leaders.contains_key(resource))
    }

    pub(crate) fn raw_token(&self) -> Option<String> {
        self.protocol_claims
            .lock()
            .signed
            .as_ref()
            .map(|token| (**token).clone().into_jwt())
    }

    pub(crate) fn has_signed_token(&self) -> bool {
        self.protocol_claims.lock().signed.is_some()
    }

    #[cfg(feature = "http-client")]
    pub(crate) fn authorization_header(&self) -> Option<String> {
        self.raw_token().map(|token| format!("Bearer {token}"))
    }

    pub(crate) async fn grant_claim(&self, claim: Claim) -> tc_error::TCResult<Self> {
        let authority = self.server.protocol_authority();
        self.grant(
            authority.actor.as_ref(),
            authority.host,
            claim.link,
            claim.mask,
        )
    }

    pub async fn context(
        &self,
    ) -> tc_error::TCResult<freqfs::DirLock<crate::storage::WorkspaceFile>> {
        self.server
            .workspace()
            .transaction_child(self.id, &self.workspace_path)
            .await
    }

    pub(crate) async fn resource_context(
        &self,
        resource: &pathlink::PathBuf,
    ) -> tc_error::TCResult<freqfs::DirLock<crate::storage::WorkspaceFile>> {
        let path = resource.iter().map(ToString::to_string).collect::<Vec<_>>();
        self.server
            .workspace()
            .transaction_child(self.id, &path)
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
                .map(|claim| AuthClaimContext {
                    host: "test".into(),
                    actor_id: "test".into(),
                    claim,
                })
                .collect(),
        });
        txn
    }

    pub(super) fn with_signed_token(&self, token: SignedToken) -> tc_error::TCResult<Self> {
        let canonical_claim = super::validate_signed_token(self.id, &token)?;
        let mut txn = self.clone();
        txn.claim = canonical_claim;
        txn.protocol_claims.lock().signed = Some(Arc::new(token));
        Ok(txn)
    }

    #[cfg(test)]
    pub(crate) fn lock_for_test(&self) -> tc_error::TCResult<Self> {
        let authority = self.server.protocol_authority();
        let txn = self.grant(
            authority.actor.as_ref(),
            authority.host,
            crate::uri::transaction_path(self.id)
                .parse()
                .expect("transaction path"),
            umask::USER_WRITE,
        )?;
        Ok(txn)
    }

    #[cfg(test)]
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
    type File = crate::storage::WorkspaceFile;

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

    fn timestamp(&self) -> NetworkTime {
        self.id.timestamp()
    }

    fn claim(&self) -> &Claim {
        &self.claim
    }
}

impl tc_state::StateExecutor for TxnHandle {
    async fn resolve_class(&self, identity: &Link) -> tc_error::TCResult<tc_state::ClassDef> {
        self.runtime
            .applications
            .classes
            .get(self, identity)
            .await?
            .ok_or_else(|| TCError::not_found(identity.to_string()))
    }

    async fn get(&self, target: Link, key: tc_ir::Scalar) -> tc_error::TCResult<crate::State> {
        self.runtime.get(target, self.clone(), key).await
    }

    async fn put(
        &self,
        target: Link,
        key: tc_ir::Scalar,
        value: crate::State,
    ) -> tc_error::TCResult<()> {
        self.runtime.put(target, self.clone(), key, value).await
    }

    async fn post(
        &self,
        target: Link,
        params: tc_ir::Map<crate::State>,
    ) -> tc_error::TCResult<crate::State> {
        self.runtime.post(target, self.clone(), params).await
    }

    async fn delete(&self, target: Link, key: tc_ir::Scalar) -> tc_error::TCResult<()> {
        self.runtime.delete(target, self.clone(), key).await
    }

    async fn execute_op(
        &self,
        definition: tc_ir::OpDef,
        args: crate::State,
        subject: Option<crate::State>,
    ) -> tc_error::TCResult<crate::State> {
        let txn = match &subject {
            Some(crate::State::Object(object)) => match object.as_ref() {
                tc_state::Object::Instance(instance) => {
                    let identity = instance.class().identity();
                    let class = self
                        .runtime
                        .applications
                        .classes
                        .clone()
                        .exact(self, &crate::application::identity_segments(identity)?)
                        .await?
                        .ok_or_else(|| TCError::not_found(identity.to_string()))?;
                    self.for_application(class.state().scope())
                }
                _ => self.clone(),
            },
            _ => self.clone(),
        };
        match definition {
            definition @ tc_ir::OpDef::Get(_) => {
                let key = crate::application::scalar_from_state(args)?;
                crate::op_executor::execute_get_with_self(&txn, definition, key, subject).await
            }
            definition @ tc_ir::OpDef::Put(_) => {
                let crate::State::Tuple(mut args) = args else {
                    return Err(TCError::bad_request("PUT OpDef expects [key, value]"));
                };
                if args.len() != 2 {
                    return Err(TCError::bad_request("PUT OpDef expects [key, value]"));
                }
                let value = args.pop().expect("PUT argument length checked");
                let key = crate::application::scalar_from_state(
                    args.pop().expect("PUT argument length checked"),
                )?;
                crate::op_executor::execute_put_with_self(&txn, definition, key, value, subject)
                    .await?;
                Ok(crate::State::default())
            }
            definition @ tc_ir::OpDef::Post(_) => {
                let crate::State::Map(params) = args else {
                    return Err(TCError::bad_request("POST OpDef expects a parameter map"));
                };
                crate::op_executor::execute_post_with_self(&txn, definition, params, subject).await
            }
            definition @ tc_ir::OpDef::Delete(_) => {
                let key = crate::application::scalar_from_state(args)?;
                crate::op_executor::execute_delete_with_self(&txn, definition, key, subject)
                    .await?;
                Ok(crate::State::default())
            }
        }
    }
}

impl fmt::Debug for TxnHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxnHandle")
            .field("id", &self.id)
            .field("claim", &self.claim)
            .finish()
    }
}
