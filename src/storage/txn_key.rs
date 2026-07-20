use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
};

use freqfs::Name;
use tc_ir::{NetworkTime, TxnId};

static IMMEDIATE_TXN_NONCE: AtomicU64 = AtomicU64::new(1);

#[derive(Copy, Clone, Debug)]
pub(crate) struct StorageTxnKey(pub(crate) TxnId);

impl From<TxnId> for StorageTxnKey {
    fn from(txn_id: TxnId) -> Self {
        Self(txn_id)
    }
}

impl fmt::Display for StorageTxnKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl PartialEq for StorageTxnKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for StorageTxnKey {}

impl Hash for StorageTxnKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl PartialOrd for StorageTxnKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StorageTxnKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl FromStr for StorageTxnKey {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        TxnId::from_str(s).map(Self)
    }
}

impl Name for StorageTxnKey {
    fn partial_cmp(&self, key: &str) -> Option<Ordering> {
        let key: StorageTxnKey = key.parse().ok()?;
        PartialOrd::partial_cmp(self, &key)
    }
}

impl PartialEq<str> for StorageTxnKey {
    fn eq(&self, other: &str) -> bool {
        StorageTxnKey::from_str(other).is_ok_and(|other| self == &other)
    }
}

impl PartialOrd<str> for StorageTxnKey {
    fn partial_cmp(&self, other: &str) -> Option<Ordering> {
        let other: StorageTxnKey = other.parse().ok()?;
        PartialOrd::partial_cmp(self, &other)
    }
}

pub(super) fn immediate_txn_id() -> StorageTxnKey {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(2);
    let nonce = IMMEDIATE_TXN_NONCE.fetch_add(1, AtomicOrdering::Relaxed) as u16;
    StorageTxnKey(TxnId::from_parts(
        NetworkTime::from_nanos(nanos.max(2)),
        nonce,
    ))
}
