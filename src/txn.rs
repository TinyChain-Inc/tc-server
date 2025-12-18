use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use parking_lot::Mutex;
use pathlink::Link;
use sha2::{Digest, Sha256};
use std::str::FromStr;
use tc_ir::{Claim, NetworkTime, Transaction, TxnHeader, TxnId};
use tc_state::StateContext;
use umask::Mode;

#[derive(Clone, Debug)]
pub struct TxnHandle {
    id: TxnId,
    claim: Claim,
}

impl TxnHandle {
    pub fn id(&self) -> TxnId {
        self.id
    }

    pub fn claim(&self) -> &Claim {
        &self.claim
    }

    pub fn header(&self) -> TxnHeader {
        TxnHeader::new(self.id, self.id.timestamp(), self.claim.clone())
    }
}

#[derive(Clone)]
pub struct TxnManager {
    inner: Arc<Mutex<Inner>>,
    host_id: Arc<String>,
}

struct Inner {
    txns: HashMap<TxnId, TxnRecord>,
    nonce: u16,
}

#[derive(Clone)]
struct TxnRecord {
    claim: Claim,
}

#[derive(Debug)]
pub enum TxnError {
    NotFound,
}

pub enum TxnFlow {
    Begin(TxnHandle),
    Use(Option<TxnHandle>),
    Commit(TxnHandle),
    Rollback(TxnHandle),
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
        }
    }

    pub fn begin(&self) -> TxnHandle {
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
        inner.txns.insert(
            id,
            TxnRecord {
                claim: claim.clone(),
            },
        );
        TxnHandle { id, claim }
    }

    pub fn get(&self, id: &TxnId) -> Option<TxnHandle> {
        let canonical = self.ensure_trace(*id);
        let inner = self.inner.lock();
        inner.txns.get(&canonical).cloned().map(|record| TxnHandle {
            id: canonical,
            claim: record.claim,
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
        method: Method,
        txn_id: Option<TxnId>,
        body_is_none: bool,
    ) -> Result<TxnFlow, TxnError> {
        match txn_id {
            None => Ok(TxnFlow::Begin(self.begin())),
            Some(id) => {
                let handle = self.get(&id).ok_or(TxnError::NotFound)?;
                if method == Method::Post && body_is_none {
                    Ok(TxnFlow::Commit(handle))
                } else if method == Method::Delete && body_is_none {
                    Ok(TxnFlow::Rollback(handle))
                } else {
                    Ok(TxnFlow::Use(Some(handle)))
                }
            }
        }
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
}
use crate::Method;

fn default_claim() -> Claim {
    Claim::new(
        Link::from_str("/library/default").expect("default claim link"),
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

impl From<TxnHandle> for StateContext {
    fn from(txn: TxnHandle) -> Self {
        StateContext::with_data(txn)
    }
}
