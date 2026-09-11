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
#[cfg(feature = "http-client")]
pub(crate) use issuer::BootstrapSession;
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
    if result.failed.is_empty() {
        Ok(())
    } else {
        Err(result
            .first_error
            .unwrap_or_else(|| TCError::bad_gateway("failed to deliver transaction decision")))
    }
}

pub(crate) struct Fanout {
    pub(crate) delivered: std::collections::BTreeSet<String>,
    pub(crate) failed: std::collections::BTreeSet<String>,
    pub(crate) first_error: Option<TCError>,
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
    let mut delivered = std::collections::BTreeSet::new();
    let mut failed = std::collections::BTreeSet::new();
    let mut first_error = None;
    for (peer, result) in results {
        match result {
            Ok(()) => {
                delivered.insert(peer);
            }
            Err(error) => {
                failed.insert(peer);
                first_error.get_or_insert(error);
            }
        }
    }
    Ok(Fanout {
        delivered,
        failed,
        first_error,
    })
}
