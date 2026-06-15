use std::{
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use parking_lot::Mutex;
use pathlink::Link;
use sha2::{Digest, Sha256};
use tc_ir::{Claim, NetworkTime, TxnId};
use umask::Mode;

use crate::auth::{Actor, SignedToken, Token};

use super::TxnHandle;

#[derive(Clone)]
pub struct TxnManager {
    inner: Arc<Mutex<Inner>>,
    host_id: Arc<String>,
    ttl: Duration,
    protocol_host: Link,
    protocol_actor: Arc<Actor>,
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
    token: Option<Arc<crate::auth::SignedToken>>,
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
        Self::with_host_id_and_ttl(host_id, Duration::from_secs(3))
    }

    pub fn with_host_id_and_ttl(host_id: impl Into<String>, ttl: Duration) -> Self {
        let host_id = host_id.into();
        let protocol_actor =
            Actor::new_falcon512(host_id.clone()).expect("generate Falcon-512 transaction actor");
        Self {
            inner: Arc::new(Mutex::new(Inner {
                txns: HashMap::new(),
                nonce: 0,
            })),
            host_id: Arc::new(host_id),
            ttl,
            protocol_host: Link::from_str(crate::uri::HOST_ROOT).expect("host root link"),
            protocol_actor: Arc::new(protocol_actor),
        }
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    pub fn set_ttl(&mut self, ttl: Duration) {
        self.ttl = ttl;
    }

    pub fn protocol_host(&self) -> &Link {
        &self.protocol_host
    }

    pub fn protocol_actor(&self) -> &Arc<Actor> {
        &self.protocol_actor
    }

    pub fn with_protocol_actor(mut self, host: Link, actor: Actor) -> Self {
        self.protocol_host = host;
        self.protocol_actor = Arc::new(actor);
        self
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
        let txn_claim = Link::from_str(&format!("/txn/{id}")).expect("txn claim link");
        let txn_claim = Claim::new(txn_claim, umask::USER_EXEC | umask::USER_WRITE);
        let owner_id = owner_id.map(str::to_string);
        let bearer_token = bearer_token.map(str::to_string);
        let signed_token = if bearer_token.is_none() {
            Some(self.sign_protocol_token(txn_claim.clone()))
        } else {
            None
        };
        let owner_id = owner_id.or_else(|| {
            signed_token.as_ref().map(|_| {
                format!(
                    "{}::{}",
                    self.protocol_host,
                    self.protocol_actor.id().clone()
                )
            })
        });
        let bearer_token =
            bearer_token.or_else(|| signed_token.as_ref().map(|token| token.clone().into_jwt()));
        let handle_owner = owner_id.clone();
        let handle_bearer = bearer_token.clone();
        let claims = vec![txn_claim.clone()];
        inner.txns.insert(
            id,
            TxnRecord {
                claim: claim.clone(),
                owner_id,
                bearer_token,
                claims: claims.clone(),
                token: signed_token.map(Arc::new),
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
            token: inner.txns.get(&id).and_then(|record| record.token.clone()),
            auth_context: None,
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
        let inner = self.inner.lock();
        let record = inner.txns.get(id).cloned().ok_or(TxnError::NotFound)?;
        if !owners_match(record.owner_id.as_deref(), owner_id) {
            return Err(TxnError::Unauthorized);
        }
        Ok(TxnHandle {
            id: *id,
            claim: record.claim,
            claims: record.claims,
            owner_id: record.owner_id,
            bearer_token: record.bearer_token,
            resolver: None,
            ttl: self.ttl,
            token: record.token,
            auth_context: None,
        })
    }

    pub fn commit(&self, id: TxnId) -> Result<(), TxnError> {
        let mut inner = self.inner.lock();
        match inner.txns.remove(&id) {
            Some(_) => Ok(()),
            None => Err(TxnError::NotFound),
        }
    }

    pub fn rollback(&self, id: TxnId) -> Result<(), TxnError> {
        let mut inner = self.inner.lock();
        match inner.txns.remove(&id) {
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
                    let (Some(owner_id), Some(bearer_token)) = (owner_id, bearer_token) else {
                        return Err(TxnError::NotFound);
                    };

                    if bearer_token.trim().is_empty() {
                        return Err(TxnError::Unauthorized);
                    }

                    Ok(TxnFlow::Begin(self.begin_with_id(
                        id,
                        Some(owner_id),
                        Some(bearer_token),
                    )))
                }
                Err(err) => Err(err),
            },
        }
    }

    pub fn record_claim(&self, id: &TxnId, claim: Claim) -> Result<(), TxnError> {
        let mut inner = self.inner.lock();
        let record = inner.txns.get_mut(id).ok_or(TxnError::NotFound)?;
        if !record.claims.contains(&claim) {
            record.claims.push(claim);
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn pending_ids(&self) -> Vec<TxnId> {
        self.inner.lock().txns.keys().copied().collect()
    }

    fn begin_with_id(
        &self,
        id: TxnId,
        owner_id: Option<&str>,
        bearer_token: Option<&str>,
    ) -> TxnHandle {
        let claim = default_claim();
        let txn_claim = Link::from_str(&format!("/txn/{id}")).expect("txn claim link");
        let txn_claim = Claim::new(txn_claim, umask::USER_EXEC | umask::USER_WRITE);
        let owner_id = owner_id.map(str::to_string);
        let bearer_token = bearer_token.map(str::to_string);
        let signed_token = if bearer_token.is_none() {
            Some(self.sign_protocol_token(txn_claim.clone()))
        } else {
            None
        };
        let owner_id = owner_id.or_else(|| {
            signed_token.as_ref().map(|_| {
                format!(
                    "{}::{}",
                    self.protocol_host,
                    self.protocol_actor.id().clone()
                )
            })
        });
        let bearer_token =
            bearer_token.or_else(|| signed_token.as_ref().map(|token| token.clone().into_jwt()));
        let claims = vec![txn_claim.clone()];
        self.inner.lock().txns.insert(
            id,
            TxnRecord {
                claim: claim.clone(),
                owner_id: owner_id.clone(),
                bearer_token: bearer_token.clone(),
                claims: claims.clone(),
                token: signed_token.clone().map(Arc::new),
            },
        );
        TxnHandle {
            id,
            claim,
            claims,
            owner_id,
            bearer_token,
            resolver: None,
            ttl: self.ttl,
            token: signed_token.map(Arc::new),
            auth_context: None,
        }
    }

    fn sign_protocol_token(&self, claim: Claim) -> SignedToken {
        let token = Token::new(
            self.protocol_host.clone(),
            SystemTime::now(),
            self.ttl,
            self.protocol_actor.id().clone(),
            claim,
        );
        self.protocol_actor
            .sign_token(token)
            .expect("sign transaction protocol token")
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

fn compute_trace(host_id: &str, timestamp: NetworkTime, nonce: u16) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(host_id.as_bytes());
    hasher.update(timestamp.as_nanos().to_be_bytes());
    hasher.update(nonce.to_be_bytes());
    hasher.finalize().into()
}
