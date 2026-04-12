use std::sync::Arc;

use futures::FutureExt;
use hyper::body::to_bytes;
use hyper::{Body, Request, StatusCode};
use serde::{Deserialize, Serialize};

use super::crypto::decode_encrypted_payload;
use super::{PEERS_HEARTBEAT_PATH, PEERS_JOIN_PATH, PEERS_LEAVE_PATH, PEERS_PATH};
use super::{PeerIdentity, PeerMembership, ReplicationIssuer, normalize_peer};
use crate::KernelHandler;
use crate::Response;

#[derive(Serialize)]
struct PeerList {
    peers: Vec<String>,
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

                    hyper::Response::builder()
                        .status(StatusCode::OK)
                        .header(hyper::header::CONTENT_TYPE, "application/json")
                        .body(Body::from(body))
                        .expect("peer list response")
                }
                ("POST", PEERS_JOIN_PATH) => {
                    let body = match to_bytes(req.into_body()).await {
                        Ok(body) => body,
                        Err(err) => return bad_request(err.to_string()),
                    };

                    let (nonce, encrypted_peer) = match decode_encrypted_payload(body) {
                        Ok(payload) => payload,
                        Err(err) => return bad_request(err.to_string()),
                    };

                    let (peer, _) = match issuer.decrypt_path_with_key(&nonce, &encrypted_peer) {
                        Ok(peer) => peer,
                        Err(err) => return bad_request(err.to_string()),
                    };

                    let announcement = match parse_announcement(&peer) {
                        Ok(announcement) => announcement,
                        Err(err) => return bad_request(err),
                    };

                    let identity = match normalize_identity(announcement) {
                        Ok(identity) => identity,
                        Err(err) => return bad_request(err.to_string()),
                    };

                    if let Err(err) = issuer.register_peer_identity(&identity) {
                        return bad_request(err.to_string());
                    }
                    membership.upsert_identity(identity);

                    let body = peer_list_body(&membership);

                    hyper::Response::builder()
                        .status(StatusCode::OK)
                        .header(hyper::header::CONTENT_TYPE, "application/json")
                        .body(Body::from(body))
                        .expect("join response")
                }
                ("POST", PEERS_HEARTBEAT_PATH) => {
                    let body = match to_bytes(req.into_body()).await {
                        Ok(body) => body,
                        Err(err) => return bad_request(err.to_string()),
                    };

                    let (nonce, encrypted_peer) = match decode_encrypted_payload(body) {
                        Ok(payload) => payload,
                        Err(err) => return bad_request(err.to_string()),
                    };

                    let (peer, _) = match issuer.decrypt_path_with_key(&nonce, &encrypted_peer) {
                        Ok(peer) => peer,
                        Err(err) => return bad_request(err.to_string()),
                    };

                    let announcement = match parse_announcement(&peer) {
                        Ok(announcement) => announcement,
                        Err(err) => return bad_request(err),
                    };

                    let identity = match normalize_identity(announcement) {
                        Ok(identity) => identity,
                        Err(err) => return bad_request(err.to_string()),
                    };

                    if let Err(err) = issuer.register_peer_identity(&identity) {
                        return bad_request(err.to_string());
                    }
                    membership.upsert_identity(identity);

                    hyper::Response::builder()
                        .status(StatusCode::NO_CONTENT)
                        .body(Body::empty())
                        .expect("heartbeat response")
                }
                ("POST", PEERS_LEAVE_PATH) => {
                    let body = match to_bytes(req.into_body()).await {
                        Ok(body) => body,
                        Err(err) => return bad_request(err.to_string()),
                    };

                    let (nonce, encrypted_peer) = match decode_encrypted_payload(body) {
                        Ok(payload) => payload,
                        Err(err) => return bad_request(err.to_string()),
                    };

                    let (peer, _) = match issuer.decrypt_path_with_key(&nonce, &encrypted_peer) {
                        Ok(peer) => peer,
                        Err(err) => return bad_request(err.to_string()),
                    };

                    let announcement = match parse_announcement(&peer) {
                        Ok(announcement) => announcement,
                        Err(err) => return bad_request(err),
                    };
                    let peer = match normalize_peer(&announcement.peer) {
                        Ok(peer) => peer,
                        Err(err) => return bad_request(err.to_string()),
                    };

                    membership.remove(&peer);

                    hyper::Response::builder()
                        .status(StatusCode::NO_CONTENT)
                        .body(Body::empty())
                        .expect("leave response")
                }
                _ => hyper::Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("not found"),
            }
        }
        .boxed()
    }
}

fn bad_request(message: String) -> Response {
    hyper::Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(Body::from(message))
        .expect("bad request")
}

fn peer_list_body(membership: &PeerMembership) -> Vec<u8> {
    serde_json::to_vec(&PeerList {
        peers: membership.active_peers(),
        replicas: membership.peer_descriptors(),
    })
    .expect("peer list")
}

fn parse_announcement(raw: &str) -> Result<PeerAnnouncement, String> {
    if raw.trim_start().starts_with('{') {
        serde_json::from_str(raw).map_err(|err| format!("invalid peer announcement: {err}"))
    } else {
        Ok(PeerAnnouncement {
            peer: raw.to_string(),
            actor_id: None,
            public_key_b64: None,
        })
    }
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
