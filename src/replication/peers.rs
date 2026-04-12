use std::sync::Arc;

use futures::FutureExt;
use hyper::{Body, Request, StatusCode};
use serde::{Deserialize, Serialize};

use super::http_util::{bad_request, decode_encrypted_request, empty_response, json_ok};
use super::{PEERS_HEARTBEAT_PATH, PEERS_JOIN_PATH, PEERS_LEAVE_PATH, PEERS_PATH};
use super::{PeerIdentity, PeerMembership, ReplicationIssuer, normalize_peer};
use crate::KernelHandler;
use crate::Response;

#[derive(Serialize)]
struct PeerList {
    replicas: Vec<super::PeerDescriptor>,
}

#[derive(Deserialize)]
struct PeerAnnouncement {
    peer: String,
    actor_id: Option<String>,
    public_key_b64: Option<String>,
}

pub fn peer_membership_handler(
    membership: PeerMembership,
    issuer: Arc<ReplicationIssuer>,
) -> impl KernelHandler {
    move |req: Request<Body>| {
        let membership = membership.clone();
        let issuer = issuer.clone();
        async move {
            match (req.method().as_str(), req.uri().path()) {
                ("GET", PEERS_PATH) => {
                    let body = peer_list_body(&membership);
                    json_ok(body)
                }
                ("POST", PEERS_JOIN_PATH) => {
                    match decode_and_upsert_identity(req, &membership, &issuer).await {
                        Ok(()) => {}
                        Err(response) => return response,
                    }

                    let body = peer_list_body(&membership);
                    json_ok(body)
                }
                ("POST", PEERS_HEARTBEAT_PATH) => {
                    match decode_and_upsert_identity(req, &membership, &issuer).await {
                        Ok(()) => {}
                        Err(response) => return response,
                    }

                    empty_response(StatusCode::NO_CONTENT)
                }
                ("POST", PEERS_LEAVE_PATH) => {
                    let announcement = match decode_peer_announcement(req, &issuer).await {
                        Ok(announcement) => announcement,
                        Err(response) => return response,
                    };
                    let peer = match normalize_peer(&announcement.peer) {
                        Ok(peer) => peer,
                        Err(err) => return bad_request(err.to_string()),
                    };

                    membership.remove(&peer);

                    empty_response(StatusCode::NO_CONTENT)
                }
                _ => empty_response(StatusCode::NOT_FOUND),
            }
        }
        .boxed()
    }
}

fn peer_list_body(membership: &PeerMembership) -> Vec<u8> {
    serde_json::to_vec(&PeerList {
        replicas: membership.peer_descriptors(),
    })
    .expect("peer list")
}

fn parse_announcement(raw: &str) -> Result<PeerAnnouncement, String> {
    serde_json::from_str(raw).map_err(|err| format!("invalid peer announcement: {err}"))
}

async fn decode_peer_announcement(
    req: Request<Body>,
    issuer: &ReplicationIssuer,
) -> Result<PeerAnnouncement, Response> {
    let (payload, _) = decode_encrypted_request(req, issuer).await?;

    parse_announcement(&payload).map_err(bad_request)
}

async fn decode_identity_announcement(
    req: Request<Body>,
    issuer: &ReplicationIssuer,
) -> Result<PeerIdentity, Response> {
    let announcement = decode_peer_announcement(req, issuer).await?;
    let identity = normalize_identity(announcement).map_err(|err| bad_request(err.to_string()))?;
    issuer
        .register_peer_identity(&identity)
        .map_err(|err| bad_request(err.to_string()))?;

    Ok(identity)
}

async fn decode_and_upsert_identity(
    req: Request<Body>,
    membership: &PeerMembership,
    issuer: &ReplicationIssuer,
) -> Result<(), Response> {
    let identity = decode_identity_announcement(req, issuer).await?;
    membership.upsert_identity(identity);
    Ok(())
}

fn normalize_identity(announcement: PeerAnnouncement) -> tc_error::TCResult<PeerIdentity> {
    let peer = normalize_peer(&announcement.peer)?;
    let actor_id = announcement
        .actor_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| tc_error::TCError::bad_request("missing peer actor_id"))?;
    if !actor_id.starts_with("replication:") {
        return Err(tc_error::TCError::bad_request(
            "peer actor_id must start with replication:",
        ));
    }
    let public_key_b64 = announcement
        .public_key_b64
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| tc_error::TCError::bad_request("missing peer public_key_b64"))?;

    Ok(PeerIdentity {
        peer,
        actor_id,
        public_key_b64,
    })
}
