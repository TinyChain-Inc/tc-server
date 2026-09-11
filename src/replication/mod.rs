#[cfg(feature = "http-client")]
pub(crate) mod client;
mod crypto;
mod gateway;
mod issuer;
mod membership;

use std::future::Future;

use futures::{StreamExt, stream};
use tc_error::{TCError, TCResult};

const FANOUT_CONCURRENCY: usize = 8;

#[cfg(feature = "http-client")]
pub(crate) use client::{bootstrap_seed, read_seed_state};
pub use gateway::{ClusterGateway, LocalClusterGateway};
pub use issuer::{ReplicationIssuer, parse_psk_keys};
pub use membership::Replica;

pub fn normalize_peer(peer: &str) -> TCResult<String> {
    let value = if peer.contains("://") {
        peer.to_string()
    } else {
        format!("http://{peer}")
    };
    let url = url::Url::parse(&value)
        .map_err(|error| TCError::bad_request(format!("invalid peer url: {error}")))?;
    let host = url
        .host_str()
        .ok_or_else(|| TCError::bad_request("peer URL missing host"))?;
    Ok(url.port().map_or_else(
        || format!("{}://{host}", url.scheme()),
        |port| format!("{}://{host}:{port}", url.scheme()),
    ))
}

pub fn normalize_replicated_prefix(prefix: &str) -> TCResult<String> {
    let trimmed = prefix.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(TCError::bad_request(
            "trusted installer prefix must not be empty",
        ));
    }

    let supported = trimmed.parse::<pathlink::Link>().ok().is_some_and(|link| {
        crate::uri::split_application_link(&link).is_ok_and(|(_, segments, _)| !segments.is_empty())
    });
    if !supported {
        return Err(TCError::bad_request(format!(
            "trusted installer prefix must start with /lib/, /class/, or /service/: {trimmed}"
        )));
    }

    Ok(trimmed.to_string())
}

pub(crate) async fn forward_put_to_peers(
    peers: &std::collections::BTreeSet<String>,
    txn: &crate::txn::TxnHandle,
    target: &pathlink::Link,
    key: tc_ir::Scalar,
    value: crate::State,
    gateway: &dyn ClusterGateway,
) -> TCResult<Fanout> {
    let token = txn
        .raw_token()
        .ok_or_else(|| tc_error::TCError::unauthorized("missing bearer token"))?
        .to_string();
    let txn_id = txn.id();

    fanout_attempt(peers, |peer| {
        let token = token.clone();
        let target = target.clone();
        let key = key.clone();
        let value = value.clone();
        async move {
            gateway
                .put(&peer, &token, txn_id, &target, key, value, txn.deadline())
                .await
        }
    })
    .await
}

pub(crate) async fn forward_delete_to_peers(
    peers: &std::collections::BTreeSet<String>,
    txn: &crate::txn::TxnHandle,
    target: &pathlink::Link,
    key: tc_ir::Scalar,
    gateway: &dyn ClusterGateway,
) -> TCResult<Fanout> {
    let token = txn
        .raw_token()
        .ok_or_else(|| tc_error::TCError::unauthorized("missing bearer token"))?
        .to_string();
    let txn_id = txn.id();
    fanout_attempt(peers, |peer| {
        let token = token.clone();
        let target = target.clone();
        let key = key.clone();
        async move {
            gateway
                .delete(&peer, &token, txn_id, &target, key, txn.deadline())
                .await
        }
    })
    .await
}

pub(crate) async fn forward_resource_decision(
    peers: &std::collections::BTreeSet<String>,
    txn: &crate::txn::TxnHandle,
    resource: &pathlink::PathBuf,
    commit: bool,
    gateway: &dyn ClusterGateway,
) -> TCResult<()> {
    let token = txn
        .raw_token()
        .ok_or_else(|| tc_error::TCError::unauthorized("missing bearer token"))?
        .to_string();
    let txn_id = txn.id();
    let result = fanout_attempt(peers, |peer| {
        let token = token.clone();
        let resource = resource.clone();
        async move {
            gateway
                .decide_resource(&peer, &token, txn_id, &resource, commit, txn.deadline())
                .await
        }
    })
    .await?;
    result.complete("failed to deliver transaction decision")
}

pub(crate) struct Fanout {
    outcomes: std::collections::BTreeMap<String, TCResult<()>>,
}

impl Fanout {
    pub(crate) fn complete(self, message: &'static str) -> TCResult<()> {
        let (_, failed, error) = self.partition();
        if failed.is_empty() {
            Ok(())
        } else {
            Err(error.unwrap_or_else(|| TCError::bad_gateway(message)))
        }
    }

    pub(crate) fn strict_majority(self) -> TCResult<ReplicaEvictions> {
        let replica_count = self.outcomes.len() + 1;
        let (delivered, failed, error) = self.partition();
        if error
            .as_ref()
            .is_some_and(|error| error.code() == tc_error::ErrorKind::Conflict)
        {
            return Err(error.expect("checked conflict"));
        }
        if delivered.len() + 1 <= replica_count / 2 {
            return Err(error.unwrap_or_else(|| {
                TCError::bad_gateway("replica write lost its strict majority")
            }));
        }

        Ok(ReplicaEvictions { delivered, failed })
    }

    fn partition(
        self,
    ) -> (
        std::collections::BTreeSet<String>,
        std::collections::BTreeSet<String>,
        Option<TCError>,
    ) {
        let mut delivered = std::collections::BTreeSet::new();
        let mut failed = std::collections::BTreeSet::new();
        let mut first_error = None;
        let mut first_conflict = None;
        for (peer, result) in self.outcomes {
            match result {
                Ok(()) => {
                    delivered.insert(peer);
                }
                Err(error) => {
                    failed.insert(peer);
                    if error.code() == tc_error::ErrorKind::Conflict && first_conflict.is_none() {
                        first_conflict = Some(error);
                    } else if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }

        (delivered, failed, first_conflict.or(first_error))
    }
}

pub(crate) struct ReplicaEvictions {
    delivered: std::collections::BTreeSet<String>,
    failed: std::collections::BTreeSet<String>,
}

impl ReplicaEvictions {
    pub(crate) fn delivered(&self) -> &std::collections::BTreeSet<String> {
        &self.delivered
    }

    pub(crate) fn failed(&self) -> &std::collections::BTreeSet<String> {
        &self.failed
    }

    pub(crate) fn into_failed(self) -> std::collections::BTreeSet<String> {
        self.failed
    }
}

async fn fanout_attempt<F, Fut>(
    peers: &std::collections::BTreeSet<String>,
    apply: F,
) -> TCResult<Fanout>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = TCResult<()>>,
{
    let results = stream::iter(peers.iter().cloned().map(|peer| {
        let fut = apply(peer.clone());
        async move { (peer, fut.await) }
    }))
    .buffer_unordered(FANOUT_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;
    Ok(Fanout {
        outcomes: results.into_iter().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fanout_interprets_failures_in_endpoint_order() {
        let peers = std::collections::BTreeSet::from(["a".to_string(), "b".to_string()]);
        let result = fanout_attempt(&peers, |peer| async move {
            if peer == "a" {
                tokio::task::yield_now().await;
            }
            Err(TCError::bad_gateway(peer))
        })
        .await
        .expect("fanout");

        assert_eq!(
            result
                .complete("fanout failed")
                .expect_err("failure")
                .message(),
            "a"
        );
    }

    #[test]
    fn fanout_derives_replica_evictions_only_from_a_strict_majority() {
        let majority = Fanout {
            outcomes: std::collections::BTreeMap::from([
                ("a".to_string(), Ok(())),
                ("b".to_string(), Err(TCError::bad_gateway("b"))),
            ]),
        }
        .strict_majority()
        .expect("local host and one peer form a strict majority of three");
        assert_eq!(
            majority.delivered(),
            &std::collections::BTreeSet::from(["a".to_string()])
        );
        assert_eq!(
            majority.failed(),
            &std::collections::BTreeSet::from(["b".to_string()])
        );

        let tie = Fanout {
            outcomes: std::collections::BTreeMap::from([
                ("a".to_string(), Ok(())),
                ("b".to_string(), Err(TCError::bad_gateway("b"))),
                ("c".to_string(), Err(TCError::bad_gateway("c"))),
            ]),
        };
        assert!(tie.strict_majority().is_err());
    }
}
