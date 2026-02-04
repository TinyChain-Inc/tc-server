use std::{
    collections::HashMap,
    fmt,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use futures::future::BoxFuture;
use getrandom::getrandom;
use parking_lot::Mutex;
use pathlink::Link;
use sha2::{Digest, Sha256};
use tc_error::TCError;
use tc_ir::{Claim, NetworkTime, Transaction, TxnHeader, TxnId};
use tc_state::State;
use tc_value::Value;
use umask::Mode;

use crate::auth::{Actor, SignedToken, Token};
use crate::gateway::RpcGateway;

#[derive(Clone)]
pub struct TxnHandle {
    id: TxnId,
    claim: Claim,
    claims: Vec<Claim>,
    owner_id: Option<String>,
    bearer_token: Option<String>,
    resolver: Option<Arc<dyn RpcGateway>>,
    ttl: Duration,
    token: Option<Arc<SignedToken>>,
}

impl TxnHandle {
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
        self.claims
            .iter()
            .any(|claim| claim.allows(link, required))
    }

    pub fn owner_id(&self) -> Option<&str> {
        self.owner_id.as_deref()
    }

    pub(crate) fn raw_token(&self) -> Option<&str> {
        self.bearer_token.as_deref()
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
        }
    }

    pub fn header(&self) -> TxnHeader {
        TxnHeader::new(self.id, self.id.timestamp(), self.claim.clone())
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
        }
    }
    pub fn with_signed_token(&self, token: SignedToken) -> tc_error::TCResult<Self> {
        let canonical_claim = self.validate_token(&token)?;
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
        })
    }

    pub fn grant(
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
                let token =
                    Token::new(host, now, self.ttl, actor.id().clone(), claim);
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

    fn validate_token(&self, token: &SignedToken) -> tc_error::TCResult<Claim> {
        let txn_link = Link::from_str(&format!("/txn/{}", self.id))
            .map_err(|err| TCError::bad_request(err.to_string()))?;

        let mut owner: Option<(&Link, &Value)> = None;
        let mut lock: Option<(&Link, &Value)> = None;
        let mut canonical_claim: Option<Claim> = None;

        for (index, (host, actor_id, claim)) in token.claims().iter().enumerate() {
            let claim_link = claim.link.to_string();
            if claim_link.starts_with("/txn/") {
                if claim.link != txn_link {
                    return Err(TCError::bad_request(format!(
                        "cannot initialize txn {} with token for {}",
                        self.id, claim.link
                    )));
                }

                if index <= 1 {
                    if canonical_claim.is_none() {
                        canonical_claim = Some(claim.clone());
                    }
                } else {
                    return Err(TCError::bad_request(
                        "invalid token: canonical claim must be first or second",
                    ));
                }

                if claim.mask.has(umask::USER_EXEC) {
                    if owner.is_some() {
                        return Err(TCError::bad_request(
                            "invalid token: multiple txn owners",
                        ));
                    }
                    owner = Some((host, actor_id));
                }

                if claim.mask.has(umask::USER_WRITE) {
                    if lock.is_some() {
                        return Err(TCError::bad_request(
                            "invalid token: multiple txn locks",
                        ));
                    }
                    lock = Some((host, actor_id));
                }
            }
        }

        let canonical_claim = canonical_claim.ok_or_else(|| {
            TCError::bad_request("invalid token: missing canonical txn claim")
        })?;

        if let Some(lock) = lock {
            let owner = owner.ok_or_else(|| {
                TCError::bad_request("invalid token: txn lock without owner")
            })?;

            if lock != owner {
                return Err(TCError::bad_request(
                    "invalid token: txn lock does not match owner",
                ));
            }
        }

        Ok(canonical_claim)
    }
}

#[derive(Clone)]
pub struct TxnManager {
    inner: Arc<Mutex<Inner>>,
    host_id: Arc<String>,
    ttl: Duration,
}

struct Inner {
    txns: HashMap<TxnId, TxnRecord>,
    nonce: u16,
}

#[derive(Clone)]
struct TxnRecord {
    claim: Claim,
    owner_id: Option<String>,
    bearer_token: Option<String>,
    claims: Vec<Claim>,
    token: Option<Arc<SignedToken>>,
}

#[derive(Debug)]
pub enum TxnError {
    NotFound,
    Unauthorized,
}

pub enum TxnFlow {
    Begin(TxnHandle),
    Use(TxnHandle),
}

impl Default for TxnManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TxnManager {
    pub fn new() -> Self {
        Self::with_host_id("tc-host-default")
    }

    pub fn with_host_id(host_id: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                txns: HashMap::new(),
                nonce: 0,
            })),
            host_id: Arc::new(host_id.into()),
            ttl: Duration::from_secs(30),
        }
    }

    pub fn with_host_id_and_ttl(host_id: impl Into<String>, ttl: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                txns: HashMap::new(),
                nonce: 0,
            })),
            host_id: Arc::new(host_id.into()),
            ttl,
        }
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    pub fn set_ttl(&mut self, ttl: Duration) {
        self.ttl = ttl;
    }

    pub fn begin(&self) -> TxnHandle {
        self.begin_with_owner(None, None)
    }

    pub fn begin_with_owner(
        &self,
        owner_id: Option<&str>,
        bearer_token: Option<&str>,
    ) -> TxnHandle {
        let mut inner = self.inner.lock();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let ts = NetworkTime::from_nanos(now);
        let nonce = inner.nonce;
        inner.nonce = inner.nonce.wrapping_add(1);

        let trace = compute_trace(&self.host_id, ts, nonce);
        let id = TxnId::from_parts(ts, nonce).with_trace(trace);
        let claim = default_claim();
        let txn_claim = Link::from_str(&format!("/txn/{id}"))
            .expect("txn claim link");
        let txn_claim = Claim::new(
            txn_claim,
            umask::USER_EXEC | umask::USER_WRITE,
        );
        let mut owner_id = owner_id.map(str::to_string);
        let mut bearer_token = bearer_token.map(str::to_string);
        let minted_token = owner_id.is_none() && bearer_token.is_none();
        if minted_token {
            let token = mint_bearer_token();
            owner_id = Some(token.clone());
            bearer_token = Some(token);
        }
        let handle_owner = owner_id.clone();
        let handle_bearer = bearer_token.clone();
        let claims = if minted_token || bearer_token.is_none() {
            vec![txn_claim.clone()]
        } else {
            Vec::new()
        };
        inner.txns.insert(
            id,
            TxnRecord {
                claim: claim.clone(),
                owner_id,
                bearer_token,
                claims: claims.clone(),
                token: None,
            },
        );
        TxnHandle {
            id,
            claim,
            claims,
            owner_id: handle_owner,
            bearer_token: handle_bearer,
            resolver: None,
            ttl: self.ttl,
            token: None,
        }
    }

    pub fn get(&self, id: &TxnId) -> Option<TxnHandle> {
        self.get_with_owner(id, None).ok()
    }

    pub fn get_with_owner(
        &self,
        id: &TxnId,
        owner_id: Option<&str>,
    ) -> Result<TxnHandle, TxnError> {
        let canonical = self.ensure_trace(*id);
        let inner = self.inner.lock();
        let record = inner
            .txns
            .get(&canonical)
            .cloned()
            .ok_or(TxnError::NotFound)?;
        if !owners_match(record.owner_id.as_deref(), owner_id) {
            return Err(TxnError::Unauthorized);
        }
        Ok(TxnHandle {
            id: canonical,
            claim: record.claim,
            claims: record.claims,
            owner_id: record.owner_id,
            bearer_token: record.bearer_token,
            resolver: None,
            ttl: self.ttl,
            token: record.token,
        })
    }

    pub fn commit(&self, id: TxnId) -> Result<(), TxnError> {
        let canonical = self.ensure_trace(id);
        let mut inner = self.inner.lock();
        match inner.txns.remove(&canonical) {
            Some(_) => Ok(()),
            None => Err(TxnError::NotFound),
        }
    }

    pub fn rollback(&self, id: TxnId) -> Result<(), TxnError> {
        let canonical = self.ensure_trace(id);
        let mut inner = self.inner.lock();
        match inner.txns.remove(&canonical) {
            Some(_) => Ok(()),
            None => Err(TxnError::NotFound),
        }
    }

    pub fn interpret_request(
        &self,
        txn_id: Option<TxnId>,
        owner_id: Option<&str>,
        bearer_token: Option<&str>,
    ) -> Result<TxnFlow, TxnError> {
        match txn_id {
            None => Ok(TxnFlow::Begin(
                self.begin_with_owner(owner_id, bearer_token),
            )),
            Some(id) => match self.get_with_owner(&id, owner_id) {
                Ok(handle) => Ok(TxnFlow::Use(handle)),
                Err(TxnError::NotFound) => {
                    let Some(owner_id) = owner_id else {
                        return Err(TxnError::NotFound);
                    };

                    Ok(TxnFlow::Begin(self.begin_with_id(
                        id,
                        Some(owner_id),
                        bearer_token,
                    )))
                }
                Err(err) => Err(err),
            },
        }
    }

    pub fn record_claim(&self, id: &TxnId, claim: Claim) -> Result<(), TxnError> {
        let canonical = self.ensure_trace(*id);
        let mut inner = self.inner.lock();
        let record = inner.txns.get_mut(&canonical).ok_or(TxnError::NotFound)?;
        if !record.claims.contains(&claim) {
            record.claims.push(claim);
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn pending_ids(&self) -> Vec<TxnId> {
        self.inner.lock().txns.keys().copied().collect()
    }

    fn ensure_trace(&self, id: TxnId) -> TxnId {
        if id.trace_bytes().iter().any(|byte| *byte != 0) {
            id
        } else {
            let trace = compute_trace(&self.host_id, id.timestamp(), id.nonce());
            TxnId::from_parts(id.timestamp(), id.nonce()).with_trace(trace)
        }
    }

    fn begin_with_id(
        &self,
        id: TxnId,
        owner_id: Option<&str>,
        bearer_token: Option<&str>,
    ) -> TxnHandle {
        let canonical = self.ensure_trace(id);
        let claim = default_claim();
        let txn_claim = Link::from_str(&format!("/txn/{canonical}"))
            .expect("txn claim link");
        let txn_claim = Claim::new(
            txn_claim,
            umask::USER_EXEC | umask::USER_WRITE,
        );
        let mut owner_id = owner_id.map(str::to_string);
        let mut bearer_token = bearer_token.map(str::to_string);
        let minted_token = owner_id.is_none() && bearer_token.is_none();
        if minted_token {
            let token = mint_bearer_token();
            owner_id = Some(token.clone());
            bearer_token = Some(token);
        }
        let claims = if minted_token || bearer_token.is_none() {
            vec![txn_claim.clone()]
        } else {
            Vec::new()
        };
        self.inner.lock().txns.insert(
            canonical,
            TxnRecord {
                claim: claim.clone(),
                owner_id: owner_id.clone(),
                bearer_token: bearer_token.clone(),
                claims: claims.clone(),
                token: None,
            },
        );
        TxnHandle {
            id: canonical,
            claim,
            claims,
            owner_id,
            bearer_token,
            resolver: None,
            ttl: self.ttl,
            token: None,
        }
    }
}

fn owners_match(existing: Option<&str>, request: Option<&str>) -> bool {
    match (existing, request) {
        (None, None) => true,
        (Some(existing), Some(request)) => existing == request,
        _ => false,
    }
}

fn default_claim() -> Claim {
    Claim::new(
        Link::from_str("/lib/default").expect("default claim link"),
        Mode::all(),
    )
}

pub(crate) fn owner_id_from_token(
    txn_id: TxnId,
    token: &crate::auth::TokenContext,
) -> Result<String, TxnError> {
    let txn_link = format!("/txn/{txn_id}");
    let txn_link =
        Link::from_str(&txn_link).map_err(|_| TxnError::Unauthorized)?;

    let mut owner: Option<(&str, &str)> = None;
    let mut lock: Option<(&str, &str)> = None;

    for (host, actor_id, claim) in &token.claims {
        if claim.link != txn_link {
            if claim.link.to_string().starts_with("/txn/") {
                return Err(TxnError::Unauthorized);
            }
            continue;
        }

        if claim.mask.has(umask::USER_EXEC) {
            if owner.is_some() {
                return Err(TxnError::Unauthorized);
            }
            owner = Some((host.as_str(), actor_id.as_str()));
        }

        if claim.mask.has(umask::USER_WRITE) {
            if lock.is_some() {
                return Err(TxnError::Unauthorized);
            }
            lock = Some((host.as_str(), actor_id.as_str()));
        }
    }

    if let Some(lock) = lock {
        if let Some(owner) = owner {
            if owner != lock {
                return Err(TxnError::Unauthorized);
            }
        } else {
            return Err(TxnError::Unauthorized);
        }
    }

    let Some((host, actor_id)) = owner else {
        return Err(TxnError::Unauthorized);
    };

    Ok(format!("{host}::{actor_id}"))
}

fn mint_bearer_token() -> String {
    let mut bytes = [0u8; 32];
    if let Err(err) = getrandom(&mut bytes) {
        panic!("failed to generate bearer token: {err}");
    }
    URL_SAFE_NO_PAD.encode(bytes)
}

fn compute_trace(host_id: &str, timestamp: NetworkTime, nonce: u16) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(host_id.as_bytes());
    hasher.update(timestamp.as_nanos().to_be_bytes());
    hasher.update(nonce.to_be_bytes());
    hasher.finalize().into()
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

impl RpcGateway for TxnHandle {
    fn get(
        &self,
        target: Link,
        txn: TxnHandle,
        key: Value,
    ) -> BoxFuture<'static, tc_error::TCResult<State>> {
        match &self.resolver {
            Some(resolver) => resolver.get(target, txn, key),
            None => Box::pin(async move {
                Err(TCError::bad_gateway("no txn resolver configured"))
            }),
        }
    }

    fn put(
        &self,
        target: Link,
        txn: TxnHandle,
        key: Value,
        value: State,
    ) -> BoxFuture<'static, tc_error::TCResult<()>> {
        match &self.resolver {
            Some(resolver) => resolver.put(target, txn, key, value),
            None => Box::pin(async move {
                Err(TCError::bad_gateway("no txn resolver configured"))
            }),
        }
    }

    fn post(
        &self,
        target: Link,
        txn: TxnHandle,
        params: tc_ir::Map<State>,
    ) -> BoxFuture<'static, tc_error::TCResult<State>> {
        match &self.resolver {
            Some(resolver) => resolver.post(target, txn, params),
            None => Box::pin(async move {
                Err(TCError::bad_gateway("no txn resolver configured"))
            }),
        }
    }

    fn delete(
        &self,
        target: Link,
        txn: TxnHandle,
        key: Value,
    ) -> BoxFuture<'static, tc_error::TCResult<()>> {
        match &self.resolver {
            Some(resolver) => resolver.delete(target, txn, key),
            None => Box::pin(async move {
                Err(TCError::bad_gateway("no txn resolver configured"))
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mints_bearer_token_for_anonymous_txn() {
        let manager = TxnManager::with_host_id("test-host");
        let handle = manager.begin();

        assert!(handle.raw_token().is_some());

        let txn_link =
            Link::from_str(&format!("/txn/{}", handle.id())).expect("txn link");
        assert!(handle.has_claim(&txn_link, umask::USER_EXEC));
        assert!(handle.has_claim(&txn_link, umask::USER_WRITE));
    }

    #[test]
    fn enforces_canonical_claim_position() {
        use std::time::{Duration, SystemTime};

        use rjwt::Actor;
        use tc_value::Value;

        let manager = TxnManager::with_host_id("test-host");
        let handle = manager.begin();
        let txn_id = handle.id();

        let txn_claim = Claim::new(
            Link::from_str(&format!("/txn/{txn_id}")).expect("txn claim"),
            umask::USER_EXEC,
        );
        let auth_claim = Claim::new(
            Link::from_str("/lib/auth").expect("auth link"),
            Mode::all(),
        );

        let host = Link::from_str("/host").expect("host link");
        let actor = Actor::new(Value::from("actor-a"));
        let now = SystemTime::now();
        let ttl = Duration::from_secs(30);

        let token = Token::new(
            host.clone(),
            now,
            ttl,
            actor.id().clone(),
            auth_claim.clone(),
        );
        let signed = actor.sign_token(token).expect("signed token");
        let signed = actor
            .consume_and_sign(signed, host.clone(), txn_claim.clone(), now)
            .expect("consume token");
        let updated = handle.with_signed_token(signed).expect("token accepted");
        assert_eq!(updated.claim(), &txn_claim);

        let other_claim = Claim::new(
            Link::from_str("/lib/other").expect("other link"),
            Mode::all(),
        );
        let final_claim = Claim::new(
            Link::from_str("/lib/final").expect("final link"),
            Mode::all(),
        );
        let token = Token::new(host.clone(), now, ttl, actor.id().clone(), txn_claim);
        let signed = actor.sign_token(token).expect("signed token");
        let signed = actor
            .consume_and_sign(signed, host.clone(), other_claim, now)
            .expect("consume token");
        let signed = actor
            .consume_and_sign(signed, host, final_claim, now)
            .expect("consume token");
        let err = handle.with_signed_token(signed).expect_err("should reject");
        assert!(err
            .message()
            .contains("canonical claim must be first or second"));
    }
}
