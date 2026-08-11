use std::{cmp::Ordering, fmt, str::FromStr};

use freqfs::Name;
use tc_ir::{NetworkTime, TxnId};

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum StorageTxnKey {
    Bootstrap,
    Protocol(TxnId),
    Maintenance(NetworkTime),
}

impl From<TxnId> for StorageTxnKey {
    fn from(txn_id: TxnId) -> Self {
        Self::Protocol(txn_id)
    }
}

impl fmt::Display for StorageTxnKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bootstrap => f.write_str("00000000000000000000-0"),
            Self::Protocol(txn_id) => write!(
                f,
                "{:020}-1-{:05}-{}",
                txn_id.timestamp().as_nanos(),
                txn_id.nonce(),
                HexTrace(txn_id.trace_bytes())
            ),
            Self::Maintenance(timestamp) => write!(f, "{:020}-2", timestamp.as_nanos()),
        }
    }
}

struct HexTrace<'a>(&'a [u8; 32]);

impl fmt::Display for HexTrace<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }

        Ok(())
    }
}

impl FromStr for StorageTxnKey {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "00000000000000000000-0" {
            return Ok(Self::Bootstrap);
        }

        let mut parts = s.split('-');
        let timestamp = parts.next().ok_or("missing storage version timestamp")?;
        let kind = parts.next().ok_or("missing storage version kind")?;

        match kind {
            "1" => {
                let nonce = parts.next().ok_or("missing protocol version nonce")?;
                let trace = parts.next().ok_or("missing protocol version trace")?;
                if parts.next().is_some() {
                    return Err("invalid protocol storage version");
                }

                format!(
                    "{timestamp}-{}-{trace}",
                    nonce
                        .parse::<u16>()
                        .map_err(|_| "invalid protocol version nonce")?
                )
                .parse()
                .map(Self::Protocol)
            }
            "2" => {
                if parts.next().is_some() {
                    return Err("invalid maintenance storage version");
                }

                timestamp
                    .parse()
                    .map(Self::Maintenance)
                    .map_err(|_| "invalid maintenance storage version")
            }
            _ => Err("invalid storage version kind"),
        }
    }
}

impl PartialOrd for StorageTxnKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StorageTxnKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Bootstrap, Self::Bootstrap) => Ordering::Equal,
            (Self::Bootstrap, _) => Ordering::Less,
            (_, Self::Bootstrap) => Ordering::Greater,
            (Self::Protocol(left), Self::Protocol(right)) => left.cmp(right),
            (Self::Maintenance(left), Self::Maintenance(right)) => left.cmp(right),
            (Self::Protocol(left), Self::Maintenance(right)) => {
                left.timestamp().cmp(right).then(Ordering::Less)
            }
            (Self::Maintenance(left), Self::Protocol(right)) => {
                left.cmp(&right.timestamp()).then(Ordering::Greater)
            }
        }
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
