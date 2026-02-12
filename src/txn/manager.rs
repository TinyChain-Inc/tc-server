use std::{
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use getrandom::getrandom;
use parking_lot::Mutex;
use pathlink::Link;
use sha2::{Digest, Sha256};
use tc_ir::{Claim, NetworkTime, TxnId};
use umask::Mode;

use super::TxnHandle;

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
        Self {
            inner: Arc::new(Mutex::new(Inner {
                txns: HashMap::new(),
                nonce: 0,
            })),
            host_id: Arc::new(host_id.into()),
            ttl: Duration::from_secs(3),
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
        let txn_claim = Link::from_str(&format!("/txn/{id}")).expect("txn claim link");
        let txn_claim = Claim::new(txn_claim, umask::USER_EXEC | umask::USER_WRITE);
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
        let txn_claim = Link::from_str(&format!("/txn/{canonical}")).expect("txn claim link");
        let txn_claim = Claim::new(txn_claim, umask::USER_EXEC | umask::USER_WRITE);
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
