use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use tc_ir::TxnId;

#[derive(Clone)]
pub struct TxnServer {
    ttl: Duration,
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    expires: HashMap<TxnId, Instant>,
}

impl TxnServer {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            inner: Arc::new(Mutex::new(Inner {
                expires: HashMap::new(),
            })),
        }
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    pub fn touch(&self, txn_id: TxnId) {
        self.touch_at(txn_id, Instant::now());
    }

    pub fn touch_at(&self, txn_id: TxnId, now: Instant) {
        self.inner.lock().expires.insert(txn_id, now + self.ttl);
    }

    pub fn forget(&self, txn_id: &TxnId) {
        self.inner.lock().expires.remove(txn_id);
    }

    pub fn expire(&self) -> Vec<TxnId> {
        self.expire_at(Instant::now())
    }

    pub fn expire_at(&self, now: Instant) -> Vec<TxnId> {
        let mut inner = self.inner.lock();
        let expired = inner
            .expires
            .iter()
            .filter_map(
                |(txn_id, expires)| {
                    if *expires <= now { Some(*txn_id) } else { None }
                },
            )
            .collect::<Vec<_>>();

        for txn_id in &expired {
            inner.expires.remove(txn_id);
        }

        expired
    }

    #[cfg(test)]
    pub fn contains(&self, txn_id: &TxnId) -> bool {
        self.inner.lock().expires.contains_key(txn_id)
    }
}

impl Default for TxnServer {
    fn default() -> Self {
        Self::new(Duration::from_secs(3))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tc_ir::NetworkTime;

    #[test]
    fn expires_touched_transactions() {
        let server = TxnServer::new(Duration::from_secs(10));
        let now = Instant::now();
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(1), 1).with_trace([0; 32]);

        server.touch_at(txn_id, now);
        assert!(server.contains(&txn_id));

        let expired = server.expire_at(now + Duration::from_secs(11));
        assert_eq!(expired, vec![txn_id]);
        assert!(!server.contains(&txn_id));
    }

    #[test]
    fn forget_removes_transaction() {
        let server = TxnServer::new(Duration::from_secs(10));
        let now = Instant::now();
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(1), 1).with_trace([0; 32]);

        server.touch_at(txn_id, now);
        server.forget(&txn_id);
        assert!(!server.contains(&txn_id));
    }
}
