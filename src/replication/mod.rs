mod client;
mod crypto;
mod handler;
mod issuer;
mod membership;
mod peers;

#[cfg(test)]
mod tests;

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::library::LibraryRegistry;
use aes_gcm_siv::{Aes256GcmSiv, Key};
use futures::FutureExt;
use hyper::body::to_bytes;
use hyper::{Body, Request, Response, StatusCode};

pub const LIBRARY_EXPORT_PATH: &str = crate::uri::HOST_LIBRARY_EXPORT;
pub const PEERS_PATH: &str = "/host/peers";
pub const PEERS_JOIN_PATH: &str = "/host/peers/join";
pub const PEERS_LEAVE_PATH: &str = "/host/peers/leave";
pub const PEERS_HEARTBEAT_PATH: &str = "/host/peers/heartbeat";
pub const FORWARDED_HEADER: &str = "x-tc-replicated";
const TOKEN_PATH: &str = "/";
const REPLICATION_TTL: Duration = Duration::from_secs(30);

pub use client::{
    discover_library_paths, fetch_library_export, fetch_library_listing, fetch_library_schema,
    heartbeat_peer, leave_peer_cluster, list_peer_cluster, normalize_peer, register_with_peer,
    request_replication_token,
};
pub use handler::{export_handler, replication_token_handler};
pub use issuer::{ReplicationIssuer, parse_psk_keys, parse_psk_list};
pub use membership::PeerMembership;
pub use peers::peer_membership_handler;

pub async fn replicate_from_peers(
    registry: &Arc<LibraryRegistry>,
    peers: &[String],
    keys: &[Key<Aes256GcmSiv>],
) {
    for peer in peers {
        let library_paths = match discover_library_paths(peer).await {
            Ok(paths) => paths,
            Err(err) => {
                eprintln!("replication from {peer} failed: {err}");
                continue;
            }
        };

        for path in library_paths {
            let token = match request_replication_token(peer, &path, keys).await {
                Ok(token) => token,
                Err(err) => {
                    eprintln!("replication from {peer} failed: {err}");
                    continue;
                }
            };

            match fetch_library_export(peer, &token).await {
                Ok(Some(payload)) => {
                    if let Err(err) = registry.install_payload(payload).await {
                        eprintln!("replication from {peer} failed: {}", err.message());
                    } else {
                        eprintln!("replicated library {path} from {peer}");
                    }
                }
                Ok(None) => eprintln!("peer {peer} has no exportable library for {path}"),
                Err(err) => eprintln!("replication from {peer} failed: {err}"),
            }
        }
    }
}

pub async fn announce_self_to_cluster(
    membership: &PeerMembership,
    self_peer: &str,
    keys: &[Key<Aes256GcmSiv>],
) {
    let mut pending = membership.active_peers();
    let mut visited = std::collections::HashSet::new();
    while let Some(seed) = pending.pop() {
        if seed == self_peer || !visited.insert(seed.clone()) {
            continue;
        }

        match register_with_peer(&seed, self_peer, keys).await {
            Ok(discovered) => {
                membership.mark_success(&seed);
                for peer in discovered {
                    if peer == self_peer {
                        continue;
                    }
                    if membership.upsert_active(peer.clone()) {
                        pending.push(peer);
                    }
                }
            }
            Err(_) => membership.mark_failure(&seed),
        }
    }
}

pub fn live_replicating_install_put_handler(
    registry: Arc<LibraryRegistry>,
    membership: PeerMembership,
    keys: Vec<Key<Aes256GcmSiv>>,
) -> impl crate::KernelHandler {
    move |req: Request<Body>| {
        let registry = Arc::clone(&registry);
        let membership = membership.clone();
        let keys = keys.clone();
        async move {
            let forwarded = req
                .headers()
                .get(FORWARDED_HEADER)
                .and_then(|value| value.to_str().ok())
                == Some("1");

            let txn = match req.extensions().get::<crate::txn::TxnHandle>().cloned() {
                Some(txn) => txn,
                None => {
                    return Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body(Body::from("missing transaction context"))
                        .expect("unauthorized");
                }
            };

            let body = match to_bytes(req.into_body()).await {
                Ok(body) => body,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(err.to_string()))
                        .expect("bad request");
                }
            };

            let install_request = match crate::library::decode_install_request_bytes(&body) {
                Ok(request) => request,
                Err(err) => {
                    let status = if err.is_bad_request() {
                        StatusCode::BAD_REQUEST
                    } else {
                        StatusCode::INTERNAL_SERVER_ERROR
                    };
                    return Response::builder()
                        .status(status)
                        .body(Body::from(err.message().to_string()))
                        .expect("install error");
                }
            };

            let schema_path = match &install_request {
                crate::library::InstallRequest::SchemaOnly(schema) => schema.id().to_string(),
                crate::library::InstallRequest::WithArtifacts(payload) => {
                    payload.schema.id().to_string()
                }
            };

            let schema_link = match pathlink::Link::from_str(&schema_path) {
                Ok(link) => link,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(format!("invalid schema path: {err}")))
                        .expect("bad request");
                }
            };

            if !txn.has_claim(&schema_link, umask::USER_WRITE) {
                return Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body(Body::from("unauthorized library install"))
                    .expect("unauthorized");
            }

            let local_result = match install_request {
                crate::library::InstallRequest::SchemaOnly(schema) => {
                    registry.install_schema(schema).await
                }
                crate::library::InstallRequest::WithArtifacts(payload) => {
                    registry.install_payload(payload).await
                }
            };

            if let Err(err) = local_result {
                let status = if err.is_bad_request() {
                    StatusCode::BAD_REQUEST
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                };
                return Response::builder()
                    .status(status)
                    .body(Body::from(err.message().to_string()))
                    .expect("install error");
            }

            if !forwarded {
                let membership = membership.clone();
                let keys = keys.clone();
                let install_bytes = body.to_vec();
                tokio::spawn(async move {
                    forward_install_to_peers(&membership, &schema_path, install_bytes, &keys).await;
                });
            }

            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .expect("no content")
        }
        .boxed()
    }
}

pub async fn forward_install_to_peers(
    membership: &PeerMembership,
    schema_path: &str,
    install_payload: Vec<u8>,
    keys: &[Key<Aes256GcmSiv>],
) {
    let peers = membership.active_peers();
    for peer in peers {
        let token = match request_replication_token(&peer, schema_path, keys).await {
            Ok(token) => token,
            Err(_) => {
                eprintln!("live replication: failed to request token from {peer}");
                membership.mark_failure(&peer);
                continue;
            }
        };

        match client::push_install_payload(&peer, &token, install_payload.clone()).await {
            Ok(()) => membership.mark_success(&peer),
            Err(err) => {
                eprintln!("live replication: failed to push payload to {peer}: {err}");
                membership.mark_failure(&peer)
            }
        }
    }
}
