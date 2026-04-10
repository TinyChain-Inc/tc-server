use std::sync::Arc;

use futures::FutureExt;
use hyper::body::to_bytes;
use hyper::{Body, Request, StatusCode};
use serde::Serialize;

use super::crypto::decode_encrypted_payload;
use super::{PEERS_HEARTBEAT_PATH, PEERS_JOIN_PATH, PEERS_LEAVE_PATH, PEERS_PATH};
use super::{PeerMembership, ReplicationIssuer, normalize_peer};
use crate::KernelHandler;
use crate::Response;

#[derive(Serialize)]
struct PeerList {
    peers: Vec<String>,
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
                    let body = serde_json::to_vec(&PeerList {
                        peers: membership.active_peers(),
                    })
                    .expect("peer list");

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

                    let peer = match normalize_peer(&peer) {
                        Ok(peer) => peer,
                        Err(err) => return bad_request(err.to_string()),
                    };

                    membership.upsert_active(peer);

                    let body = serde_json::to_vec(&PeerList {
                        peers: membership.active_peers(),
                    })
                    .expect("peer list");

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

                    let peer = match normalize_peer(&peer) {
                        Ok(peer) => peer,
                        Err(err) => return bad_request(err.to_string()),
                    };

                    membership.upsert_active(peer);

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

                    let peer = match normalize_peer(&peer) {
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
