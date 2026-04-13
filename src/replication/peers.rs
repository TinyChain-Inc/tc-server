use std::sync::Arc;

use futures::FutureExt;
use hyper::{Body, Request, StatusCode};
use serde::{Deserialize, Serialize};

use super::http_util::{
    bad_request, decode_encrypted_request, empty_response, json_ok, method_not_allowed,
};
use super::{PeerIdentity, PeerMembership, PeerRoutes, ReplicationIssuer, normalize_peer};
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
    routes: PeerRoutes,
) -> impl KernelHandler {
    move |req: Request<Body>| {
        let membership = membership.clone();
        let issuer = issuer.clone();
        let routes = routes.clone();
        async move {
            match route(req.method().as_str(), req.uri().path(), &routes) {
                Some(Route::List) => {
                    match decode_and_touch_peer(req, &membership, &issuer).await {
                        Ok(()) => {}
                        Err(response) => return response,
                    }

                    let body = peer_list_body(&membership);
                    json_ok(body)
                }
                Some(Route::Join) => {
                    match decode_and_upsert_identity(req, &membership, &issuer).await {
                        Ok(()) => {}
                        Err(response) => return response,
                    }

                    let body = peer_list_body(&membership);
                    json_ok(body)
                }
                Some(Route::Heartbeat) => {
                    match decode_and_upsert_identity(req, &membership, &issuer).await {
                        Ok(()) => {}
                        Err(response) => return response,
                    }

                    empty_response(StatusCode::NO_CONTENT)
                }
                Some(Route::Leave) => {
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
                Some(Route::WrongMethod) => method_not_allowed("POST"),
                _ => empty_response(StatusCode::NOT_FOUND),
            }
        }
        .boxed()
    }
}

enum Route {
    List,
    Join,
    Heartbeat,
    Leave,
    WrongMethod,
}

fn route(method: &str, path: &str, routes: &PeerRoutes) -> Option<Route> {
    if is_list_path(path, routes) {
        return if method == "POST" {
            Some(Route::List)
        } else {
            Some(Route::WrongMethod)
        };
    }

    if is_join_path(path, routes) {
        return if method == "POST" {
            Some(Route::Join)
        } else {
            Some(Route::WrongMethod)
        };
    }

    if is_heartbeat_path(path, routes) {
        return if method == "POST" {
            Some(Route::Heartbeat)
        } else {
            Some(Route::WrongMethod)
        };
    }

    if is_leave_path(path, routes) {
        return if method == "POST" {
            Some(Route::Leave)
        } else {
            Some(Route::WrongMethod)
        };
    }

    None
}

fn is_list_path(path: &str, routes: &PeerRoutes) -> bool {
    path == routes.peers_path()
}

fn is_join_path(path: &str, routes: &PeerRoutes) -> bool {
    path == routes.join_path()
}

fn is_heartbeat_path(path: &str, routes: &PeerRoutes) -> bool {
    path == routes.heartbeat_path()
}

fn is_leave_path(path: &str, routes: &PeerRoutes) -> bool {
    path == routes.leave_path()
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

async fn decode_and_touch_peer(
    req: Request<Body>,
    membership: &PeerMembership,
    issuer: &ReplicationIssuer,
) -> Result<(), Response> {
    let announcement = decode_peer_announcement(req, issuer).await?;
    let peer = match normalize_peer(&announcement.peer) {
        Ok(peer) => peer,
        Err(err) => return Err(bad_request(err.to_string())),
    };

    if announcement
        .actor_id
        .as_ref()
        .is_some_and(|actor| !actor.trim().is_empty())
        || announcement
            .public_key_b64
            .as_ref()
            .is_some_and(|key| !key.trim().is_empty())
    {
        let identity =
            normalize_identity(announcement).map_err(|err| bad_request(err.to_string()))?;
        issuer
            .register_peer_identity(&identity)
            .map_err(|err| bad_request(err.to_string()))?;
        membership.upsert_identity(identity);
    } else {
        membership.upsert_active(peer);
    }

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
