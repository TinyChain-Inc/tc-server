use std::{
    collections::{HashMap, hash_map::Entry},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use futures::future::BoxFuture;
use parking_lot::Mutex;
use pathlink::Link;
use sha2::{Digest, Sha256};
use tc_error::{TCError, TCResult};
use tc_ir::{Claim, NetworkTime, TxnId};
use tokio::time::Instant;
use umask::Mode;

use crate::auth::{SignedToken, Token, TokenContext, TokenVerifier};
use crate::library::LibraryRegistry;

use super::{AuthContext, TxnConfig, TxnError, TxnHandle, owner_id_from_token};

pub(crate) type TxnFinalize =
    Arc<dyn Fn(TxnHandle, bool) -> BoxFuture<'static, TCResult<()>> + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TransactionOutcome {
    Succeeded,
    Failed,
    Expired,
    ExplicitCommit,
    ExplicitRollback,
}

impl TransactionOutcome {
    pub fn from_success(success: bool) -> Self {
        if success {
            Self::Succeeded
        } else {
            Self::Failed
        }
    }

    fn commits(self) -> bool {
        matches!(self, Self::Succeeded | Self::ExplicitCommit)
    }
}

#[derive(Clone)]
pub(crate) struct TxnServer {
    config: TxnConfig,
    inner: Arc<Mutex<Inner>>,
    notify: Arc<tokio::sync::Notify>,
    worker_started: Arc<AtomicBool>,
    library: Option<Arc<LibraryRegistry>>,
    finalize: Option<TxnFinalize>,
    verifier: Arc<dyn TokenVerifier>,
}

struct Inner {
    active: HashMap<TxnId, TxnRecord>,
    nonce: u16,
}

struct TxnRecord {
    txn: TxnHandle,
    owner: String,
    leaders: HashMap<String, String>,
    expires: Instant,
    phase: TxnPhase,
    revision: u64,
}

struct HandleSeed {
    id: TxnId,
    claim: Claim,
    claims: Vec<Claim>,
    owner: String,
    bearer: String,
    signed: Option<Arc<SignedToken>>,
    auth: Option<AuthContext>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TxnPhase {
    Active,
    Finalizing(TransactionOutcome),
}

impl TxnServer {
    pub(crate) fn new(
        config: TxnConfig,
        library: Option<Arc<LibraryRegistry>>,
        finalize: Option<TxnFinalize>,
        verifier: Arc<dyn TokenVerifier>,
    ) -> Self {
        Self {
            config,
            inner: Arc::new(Mutex::new(Inner {
                active: HashMap::new(),
                nonce: 0,
            })),
            notify: Arc::new(tokio::sync::Notify::new()),
            worker_started: Arc::new(AtomicBool::new(false)),
            library,
            finalize,
            verifier,
        }
    }

    pub(crate) fn protocol_host(&self) -> &Link {
        &self.config.protocol_host
    }

    pub(crate) fn protocol_actor(&self) -> &Arc<crate::auth::Actor> {
        &self.config.protocol_actor
    }

    /// Allocate or continue the one protocol transaction represented by this request.
    pub(crate) fn bind(
        &self,
        txn_id: Option<TxnId>,
        token: Option<&TokenContext>,
        component: Option<&str>,
    ) -> Result<TxnHandle, TxnError> {
        match txn_id {
            None => Ok(self.begin(token)),
            Some(txn_id) => {
                let token = token.ok_or(TxnError::Unauthorized)?;
                let owner = owner_id_from_token(txn_id, token)?;
                self.continue_txn(txn_id, owner, token, component)
            }
        }
    }

    pub(crate) async fn finish_authorized(
        &self,
        txn_id: TxnId,
        token: Option<&TokenContext>,
        component: Option<&str>,
        required: Mode,
        outcome: TransactionOutcome,
    ) -> TCResult<()> {
        let token = token.ok_or_else(|| TCError::unauthorized("missing transaction authority"))?;
        let owner = owner_id_from_token(txn_id, token)?;
        let txn_link = Link::from_str(&format!("/txn/{txn_id}"))
            .map_err(|_| TCError::unauthorized("invalid transaction authority"))?;
        if !token
            .claims
            .iter()
            .any(|(_, _, claim)| claim.allows(&txn_link, required))
        {
            return Err(TCError::unauthorized("insufficient transaction authority"));
        }

        let leader = component
            .map(|component| {
                component_leader(token, component, required)
                    .map(|leader| (component.to_string(), leader))
            })
            .transpose()?;
        let mut txn = self.continued_handle(txn_id, &owner, token)?;
        let txn = {
            let mut inner = self.inner.lock();
            let record = inner
                .active
                .get_mut(&txn_id)
                .ok_or_else(|| TCError::bad_request("unknown transaction id"))?;
            validate_record(record, &owner)?;
            validate_leader(record, leader.as_ref())?;
            txn.workspace_path = record.txn.workspace_path.clone();
            apply_leader(record, leader);
            record.txn = txn;
            record.phase = TxnPhase::Finalizing(outcome);
            record.revision += 1;
            record.txn.clone()
        };

        self.finish_claimed(txn_id, txn, outcome).await
    }

    fn begin(&self, token: Option<&TokenContext>) -> TxnHandle {
        let txn_id = {
            let mut inner = self.inner.lock();
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            let timestamp = NetworkTime::from_nanos(now);
            let nonce = inner.nonce;
            inner.nonce = inner.nonce.wrapping_add(1);
            let trace = compute_trace(&self.config.host_id, timestamp, nonce);
            TxnId::from_parts(timestamp, nonce).with_trace(trace)
        };

        let txn_claim = txn_claim(txn_id);
        let signed = self.sign_protocol_token(txn_claim.clone());
        let owner = format!(
            "{}::{}",
            self.config.protocol_host,
            self.config.protocol_actor.id()
        );
        let claims = token.map_or_else(
            || vec![txn_claim.clone()],
            |token| append_claims(token, txn_claim.clone()),
        );
        let txn = self.handle(HandleSeed {
            id: txn_id,
            claim: txn_claim,
            claims,
            owner: owner.clone(),
            bearer: signed.clone().into_jwt(),
            signed: Some(Arc::new(signed)),
            auth: token.map(AuthContext::from_token_context),
        });
        self.insert(txn.clone(), owner);
        txn
    }

    fn continue_txn(
        &self,
        txn_id: TxnId,
        owner: String,
        token: &TokenContext,
        component: Option<&str>,
    ) -> Result<TxnHandle, TxnError> {
        let leader = component
            .map(|component| {
                component_leader(token, component, umask::USER_EXEC)
                    .map(|leader| (component.to_string(), leader))
            })
            .transpose()?;
        let mut txn = self.continued_handle(txn_id, &owner, token)?;
        let mut inner = self.inner.lock();
        match inner.active.entry(txn_id) {
            Entry::Occupied(mut entry) => {
                let record = entry.get_mut();
                validate_record(record, &owner)?;
                validate_leader(record, leader.as_ref())?;
                txn.workspace_path = record.txn.workspace_path.clone();
                apply_leader(record, leader);
                record.txn = txn.clone();
                record.expires = Instant::now() + self.config.ttl;
                record.revision += 1;
            }
            Entry::Vacant(entry) => {
                let mut leaders = HashMap::new();
                if let Some((component, leader)) = leader {
                    leaders.insert(component, leader);
                }
                entry.insert(TxnRecord {
                    txn: txn.clone(),
                    owner,
                    leaders,
                    expires: Instant::now() + self.config.ttl,
                    phase: TxnPhase::Active,
                    revision: 0,
                });
            }
        }
        drop(inner);
        self.notify.notify_one();
        Ok(txn)
    }

    fn continued_handle(
        &self,
        txn_id: TxnId,
        owner: &str,
        token: &TokenContext,
    ) -> Result<TxnHandle, TxnError> {
        let claim = canonical_txn_claim(txn_id, token)?;
        let claims = token
            .claims
            .iter()
            .map(|(_, _, claim)| claim.clone())
            .collect();
        Ok(self.handle(HandleSeed {
            id: txn_id,
            claim,
            claims,
            owner: owner.to_string(),
            bearer: token.bearer_token.clone(),
            signed: None,
            auth: Some(AuthContext::from_token_context(token)),
        }))
    }

    pub(crate) async fn grant(&self, txn: &TxnHandle, claim: Claim) -> TCResult<String> {
        let component = claim.link.to_string();
        let leader = format!("{}::{}", self.protocol_host(), self.protocol_actor().id());
        let (current, revision) = {
            let inner = self.inner.lock();
            let record = inner
                .active
                .get(&txn.id())
                .ok_or_else(|| TCError::bad_request("unknown transaction id"))?;
            if record.phase != TxnPhase::Active
                || record.owner != txn.owner_id().unwrap_or_default()
            {
                return Err(TCError::unauthorized("unauthorized transaction owner"));
            }
            validate_leader(record, Some(&(component.clone(), leader.clone())))
                .map_err(|_| TCError::unauthorized("component leadership is already claimed"))?;
            (record.txn.clone(), record.revision)
        };

        let updated = if current.has_signed_token() {
            current.grant(
                self.protocol_actor(),
                self.protocol_host().clone(),
                claim.link,
                claim.mask,
            )?
        } else {
            let bearer = current
                .raw_token()
                .ok_or_else(|| TCError::unauthorized("missing bearer token"))?;
            let mut context = self
                .verifier
                .verify(bearer.to_string())
                .await
                .map_err(|_| TCError::unauthorized("invalid bearer token"))?;
            if owner_id_from_token(current.id(), &context).is_err() {
                context = self
                    .verifier
                    .grant(context, txn_claim(current.id()))
                    .await
                    .map_err(|_| TCError::unauthorized("invalid bearer token"))?;
            }
            let context = self
                .verifier
                .grant(context, claim)
                .await
                .map_err(|_| TCError::unauthorized("invalid bearer token"))?;
            let claims = context
                .claims
                .iter()
                .map(|(_, _, claim)| claim.clone())
                .collect();
            current
                .with_claims(claims)
                .with_bearer_token(context.bearer_token.clone())
                .with_auth_context(AuthContext::from_token_context(&context))
        };
        let bearer = updated
            .raw_token()
            .ok_or_else(|| TCError::unauthorized("invalid bearer token"))?
            .to_string();
        let mut inner = self.inner.lock();
        let record = inner
            .active
            .get_mut(&txn.id())
            .ok_or_else(|| TCError::bad_request("unknown transaction id"))?;
        if record.phase != TxnPhase::Active || record.owner != txn.owner_id().unwrap_or_default() {
            return Err(TCError::unauthorized("unauthorized transaction owner"));
        }
        if record.revision != revision {
            return Err(tc_error::unavailable!(
                "transaction authority changed while granting a claim"
            ));
        }
        validate_leader(record, Some(&(component.clone(), leader.clone())))
            .map_err(|_| TCError::unauthorized("component leadership is already claimed"))?;
        apply_leader(record, Some((component, leader)));
        record.txn = updated;
        record.expires = Instant::now() + self.config.ttl;
        record.revision += 1;
        drop(inner);
        self.notify.notify_one();
        Ok(bearer)
    }

    fn insert(&self, txn: TxnHandle, owner: String) {
        self.inner.lock().active.insert(
            txn.id(),
            TxnRecord {
                txn,
                owner,
                leaders: HashMap::new(),
                expires: Instant::now() + self.config.ttl,
                phase: TxnPhase::Active,
                revision: 0,
            },
        );
        self.notify.notify_one();
    }

    fn handle(&self, seed: HandleSeed) -> TxnHandle {
        TxnHandle {
            id: seed.id,
            claim: seed.claim,
            claims: seed.claims,
            owner_id: Some(seed.owner),
            bearer_token: Some(seed.bearer),
            resolver: None,
            ttl: self.config.ttl,
            token: seed.signed,
            auth_context: seed.auth,
            workspace: self.config.workspace.clone(),
            workspace_path: Vec::new(),
            resources: self.config.resources.clone(),
            deadline: self.config.resources.deadline(),
        }
    }

    fn sign_protocol_token(&self, claim: Claim) -> SignedToken {
        let token = Token::new(
            self.config.protocol_host.clone(),
            SystemTime::now(),
            self.config.ttl,
            self.config.protocol_actor.id().clone(),
            claim,
        );
        self.config
            .protocol_actor
            .sign_token(token)
            .expect("sign transaction protocol token")
    }

    pub fn start_expiry(&self, runtime: &tokio::runtime::Handle) {
        if self.worker_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let server = self.clone();
        runtime.spawn(async move { server.run_expiry().await });
    }

    async fn run_expiry(self) {
        loop {
            match self.next_expiry() {
                Some(deadline) => tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => self.expire_due(Instant::now()).await,
                    _ = self.notify.notified() => {}
                },
                None => self.notify.notified().await,
            }
        }
    }

    fn next_expiry(&self) -> Option<Instant> {
        self.inner
            .lock()
            .active
            .values()
            .filter(|record| record.phase == TxnPhase::Active)
            .map(|record| record.expires)
            .min()
    }

    async fn expire_due(&self, now: Instant) {
        let expired = {
            let mut inner = self.inner.lock();
            inner
                .active
                .iter_mut()
                .filter_map(|(txn_id, record)| {
                    if record.phase == TxnPhase::Active && record.expires <= now {
                        record.phase = TxnPhase::Finalizing(TransactionOutcome::Expired);
                        Some((*txn_id, record.txn.clone()))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };
        for (txn_id, txn) in expired {
            if let Err(err) = self
                .finish_claimed(txn_id, txn, TransactionOutcome::Expired)
                .await
            {
                log::error!("failed to roll back expired transaction {txn_id}: {err}");
            }
        }
    }

    pub(crate) async fn complete(
        &self,
        txn: TxnHandle,
        outcome: TransactionOutcome,
    ) -> TCResult<()> {
        let txn_id = txn.id();
        let txn = {
            let mut inner = self.inner.lock();
            let record = inner
                .active
                .get_mut(&txn_id)
                .ok_or_else(|| TCError::bad_request("unknown transaction id"))?;
            if record.owner != txn.owner_id().unwrap_or_default() {
                return Err(TCError::unauthorized("unauthorized transaction owner"));
            }
            if record.phase != TxnPhase::Active {
                return Err(tc_error::unavailable!(
                    "transaction finalization is already in progress"
                ));
            }
            record.phase = TxnPhase::Finalizing(outcome);
            record.txn.clone()
        };
        self.finish_claimed(txn_id, txn, outcome).await
    }

    async fn finish_claimed(
        &self,
        txn_id: TxnId,
        txn: TxnHandle,
        outcome: TransactionOutcome,
    ) -> TCResult<()> {
        let result = self.finish_resources(txn, outcome.commits()).await;
        let mut inner = self.inner.lock();
        if result.is_ok() {
            inner.active.remove(&txn_id);
        } else if let Some(record) = inner.active.get_mut(&txn_id) {
            record.phase = TxnPhase::Active;
            record.expires = Instant::now() + self.config.ttl;
        }
        drop(inner);
        self.notify.notify_one();
        result
    }

    async fn finish_resources(&self, txn: TxnHandle, commit: bool) -> TCResult<()> {
        if let Some(finalize) = &self.finalize {
            finalize(txn.clone(), commit).await?;
        }
        if let Some(library) = &self.library {
            library.finalize_txn(txn.id(), commit).await?;
        }
        txn.remove_workspace().await
    }

    #[cfg(test)]
    pub(crate) fn test(host_id: &str) -> Self {
        Self::test_with(host_id, std::time::Duration::from_secs(3), None)
    }

    #[cfg(test)]
    pub(crate) fn test_with(
        host_id: &str,
        ttl: std::time::Duration,
        workspace: Option<crate::Workspace>,
    ) -> Self {
        let mut config = TxnConfig::with_host_id(host_id);
        config.ttl = ttl;
        config.workspace = workspace;
        let verifier = test_verifier(&config);
        Self::new(config, None, None, verifier)
    }

    #[cfg(test)]
    pub(crate) fn test_txn(&self) -> TxnHandle {
        self.begin(None)
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, txn_id: &TxnId) -> bool {
        self.inner.lock().active.contains_key(txn_id)
    }
}

fn validate_record(record: &TxnRecord, owner: &str) -> Result<(), TxnError> {
    if record.owner == owner && record.phase == TxnPhase::Active {
        Ok(())
    } else {
        Err(TxnError::Unauthorized)
    }
}

fn validate_leader(record: &TxnRecord, leader: Option<&(String, String)>) -> Result<(), TxnError> {
    let Some((component, leader)) = leader else {
        return Ok(());
    };
    match record.leaders.get(component) {
        Some(existing) if existing != leader => Err(TxnError::Unauthorized),
        _ => Ok(()),
    }
}

fn apply_leader(record: &mut TxnRecord, leader: Option<(String, String)>) {
    if let Some((component, leader)) = leader {
        record.leaders.entry(component).or_insert(leader);
    }
}

fn txn_claim(txn_id: TxnId) -> Claim {
    Claim::new(
        Link::from_str(&format!("/txn/{txn_id}")).expect("transaction claim link"),
        umask::USER_EXEC | umask::USER_WRITE,
    )
}

fn canonical_txn_claim(txn_id: TxnId, token: &TokenContext) -> Result<Claim, TxnError> {
    let link = Link::from_str(&format!("/txn/{txn_id}")).map_err(|_| TxnError::Unauthorized)?;
    token
        .claims
        .iter()
        .find_map(|(_, _, claim)| (claim.link == link).then(|| claim.clone()))
        .ok_or(TxnError::Unauthorized)
}

fn component_leader(
    token: &TokenContext,
    component: &str,
    required: Mode,
) -> Result<String, TxnError> {
    let component = Link::from_str(component).map_err(|_| TxnError::Unauthorized)?;
    let mut leader = None;
    for (host, actor, claim) in &token.claims {
        if claim.allows(&component, required) {
            let principal = format!("{host}::{actor}");
            if leader
                .replace(principal.clone())
                .is_some_and(|old| old != principal)
            {
                return Err(TxnError::Unauthorized);
            }
        }
    }
    leader.ok_or(TxnError::Unauthorized)
}

fn append_claims(token: &TokenContext, txn_claim: Claim) -> Vec<Claim> {
    let mut claims = token
        .claims
        .iter()
        .map(|(_, _, claim)| claim.clone())
        .collect::<Vec<_>>();
    if !claims.contains(&txn_claim) {
        claims.push(txn_claim);
    }
    claims
}

fn compute_trace(host_id: &str, timestamp: NetworkTime, nonce: u16) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(host_id.as_bytes());
    hasher.update(timestamp.as_nanos().to_be_bytes());
    hasher.update(nonce.to_be_bytes());
    hasher.finalize().into()
}

#[cfg(test)]
fn test_verifier(config: &TxnConfig) -> Arc<dyn TokenVerifier> {
    let keyring = crate::auth::KeyringActorResolver::default().with_actor(
        config.protocol_host.clone(),
        config.protocol_actor.as_ref().clone(),
    );
    Arc::new(crate::auth::RjwtTokenVerifier::new(Arc::new(keyring)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn expiry_and_explicit_completion_share_one_record() {
        let server = TxnServer::test("txn-server-test");
        let txn = server.test_txn();
        let txn_id = txn.id();
        assert!(server.contains(&txn_id));
        server
            .complete(txn, TransactionOutcome::Failed)
            .await
            .expect("rollback");
        assert!(!server.contains(&txn_id));
    }

    #[test]
    fn exact_transaction_claim_is_required_for_continuation() {
        let server = TxnServer::test("txn-server-test");
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(7), 7).with_trace([1; 32]);
        let token = TokenContext::new("owner", "bearer");
        assert!(matches!(
            server.bind(Some(txn_id), Some(&token), Some("/lib/test/example/1.0.0"),),
            Err(TxnError::Unauthorized)
        ));
    }

    #[test]
    fn component_leadership_is_immutable() {
        let server = TxnServer::test("txn-server-test");
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(7), 8).with_trace([2; 32]);
        let component = "/lib/test/example/1.0.0";
        let token = authority_token(txn_id, component, "leader-a");
        server
            .bind(Some(txn_id), Some(&token), Some(component))
            .expect("initial participant claim");
        let bearer = server
            .inner
            .lock()
            .active
            .get(&txn_id)
            .and_then(|record| record.txn.raw_token().map(str::to_string))
            .expect("stored participant bearer");

        let changed = authority_token(txn_id, component, "leader-b");
        assert!(matches!(
            server.bind(Some(txn_id), Some(&changed), Some(component)),
            Err(TxnError::Unauthorized)
        ));
        let stored_bearer = server
            .inner
            .lock()
            .active
            .get(&txn_id)
            .and_then(|record| record.txn.raw_token().map(str::to_string));
        assert_eq!(
            stored_bearer.as_deref(),
            Some(bearer.as_str()),
            "a rejected leadership claim must not mutate the active record"
        );
    }

    #[tokio::test]
    async fn explicit_finalize_claims_the_phase_before_delegating() {
        let config = TxnConfig::with_host_id("txn-server-test");
        let verifier = test_verifier(&config);
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let finalize: TxnFinalize = {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            Arc::new(move |_txn, _commit| {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                Box::pin(async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(())
                })
            })
        };
        let server = TxnServer::new(config, None, Some(finalize), verifier);
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(7), 9).with_trace([3; 32]);
        let component = "/lib/test/example/1.0.0";
        let token = authority_token(txn_id, component, "leader-a");
        server
            .bind(Some(txn_id), Some(&token), Some(component))
            .expect("initial participant claim");

        let finishing_server = server.clone();
        let finishing_token = token.clone();
        let finishing = tokio::spawn(async move {
            finishing_server
                .finish_authorized(
                    txn_id,
                    Some(&finishing_token),
                    Some(component),
                    umask::USER_EXEC,
                    TransactionOutcome::ExplicitCommit,
                )
                .await
        });
        entered.notified().await;

        assert!(matches!(
            server.bind(Some(txn_id), Some(&token), Some(component)),
            Err(TxnError::Unauthorized)
        ));
        release.notify_one();
        finishing
            .await
            .expect("finalize task")
            .expect("explicit finalize");
        assert!(!server.contains(&txn_id));
    }

    #[tokio::test]
    async fn allocating_host_owns_and_delegates_authenticated_transactions() {
        let server = TxnServer::test("txn-server-test");
        let caller = TokenContext::new("caller-host::caller", "caller-token").with_claim(
            "caller-host".to_string(),
            "caller".to_string(),
            Claim::new(
                Link::from_str("/lib/test/example/1.0.0").expect("caller claim"),
                Mode::all(),
            ),
        );
        let txn = server.bind(None, Some(&caller), None).expect("allocate");
        assert_eq!(
            txn.owner_id(),
            Some(
                format!(
                    "{}::{}",
                    server.protocol_host(),
                    server.protocol_actor().id()
                )
                .as_str()
            )
        );

        let bearer = server
            .grant(
                &txn,
                Claim::new(
                    Link::from_str("/lib/test/example/1.0.0").expect("component claim"),
                    Mode::all(),
                ),
            )
            .await
            .expect("delegate component authority");
        let context = server
            .verifier
            .verify(bearer)
            .await
            .expect("verify delegation");
        assert_eq!(
            owner_id_from_token(txn.id(), &context).expect("transaction owner"),
            format!(
                "{}::{}",
                server.protocol_host(),
                server.protocol_actor().id()
            )
        );
    }

    fn authority_token(txn_id: TxnId, component: &str, leader: &str) -> TokenContext {
        TokenContext::new("host::owner", format!("bearer-{leader}"))
            .with_claim("host".to_string(), "owner".to_string(), txn_claim(txn_id))
            .with_claim(
                "host".to_string(),
                leader.to_string(),
                Claim::new(
                    Link::from_str(component).expect("component claim"),
                    Mode::all(),
                ),
            )
    }
}
