use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use hyper::{Body, Request, StatusCode};
use tc_error::{TCError, TCResult};
use tc_ir::TxnId;
use url::Url;

use super::CanonicalBody;
use super::crypto::{encode_encrypted_payload, encrypt_path_with_key};
use super::gateway::ClusterGateway;
use super::{PeerClusterListing, PeerIdentity, PeerRoutes, normalize_peer};

type HttpClient = hyper::Client<hyper::client::HttpConnector, Body>;

#[derive(Clone)]
pub struct HttpClusterGateway {
    client: HttpClient,
    membership: super::PeerMembership,
}

impl HttpClusterGateway {
    pub fn new(membership: super::PeerMembership) -> Self {
        Self {
            client: hyper::Client::new(),
            membership,
        }
    }
}

#[async_trait::async_trait]
impl ClusterGateway for HttpClusterGateway {
    fn replicas(&self, _resource: &pathlink::PathBuf) -> BTreeSet<String> {
        self.membership
            .snapshot_active_peers()
            .into_iter()
            .collect()
    }

    async fn register_with_peer(
        &self,
        seed: &str,
        joiner: &PeerIdentity,
        routes: &PeerRoutes,
        keys: &[aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>],
    ) -> TCResult<PeerClusterListing> {
        register_with_peer(&self.client, seed, joiner, routes, keys).await
    }

    async fn put_application(
        &self,
        peer: &str,
        token: &str,
        txn_id: TxnId,
        application: CanonicalBody,
        deadline: crate::Deadline,
    ) -> TCResult<()> {
        put_application(&self.client, peer, token, txn_id, application, deadline).await
    }

    async fn delete_application(
        &self,
        peer: &str,
        token: &str,
        txn_id: TxnId,
        identity: &pathlink::Link,
        deadline: crate::Deadline,
    ) -> TCResult<()> {
        delete_application(&self.client, peer, token, txn_id, identity, deadline).await
    }

    async fn decide_resource(
        &self,
        peer: &str,
        token: &str,
        txn_id: TxnId,
        resource: &pathlink::PathBuf,
        commit: bool,
        deadline: crate::Deadline,
    ) -> TCResult<()> {
        decide_resource(
            &self.client,
            peer,
            token,
            txn_id,
            resource,
            commit,
            deadline,
        )
        .await
    }
}

async fn send_peer_request(
    client: &HttpClient,
    req: Request<Body>,
) -> TCResult<(StatusCode, Bytes)> {
    let deadline = crate::resources::Deadline::after(crate::outbound_http::DEFAULT_TIMEOUT);
    crate::outbound_http::send(client, req, deadline).await
}

async fn send_peer_request_at(
    client: &HttpClient,
    req: Request<Body>,
    deadline: crate::Deadline,
) -> TCResult<(StatusCode, Bytes)> {
    crate::outbound_http::send(client, req, deadline).await
}

async fn register_with_peer(
    client: &HttpClient,
    seed: &str,
    joiner: &PeerIdentity,
    routes: &PeerRoutes,
    keys: &[aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>],
) -> TCResult<PeerClusterListing> {
    let body = post_encrypted_peer_action(
        client,
        seed,
        &routes.join,
        PeerAnnouncement {
            peer: joiner.peer.clone(),
            actor_id: Some(joiner.actor_id.clone()),
            public_key_b64: Some(joiner.public_key_b64.clone()),
        },
        keys,
    )
    .await?;
    decode_peer_cluster_listing(&body)
}

pub(crate) async fn put_application(
    client: &HttpClient,
    peer: &str,
    token: &str,
    txn_id: TxnId,
    application: CanonicalBody,
    deadline: crate::Deadline,
) -> TCResult<()> {
    let mut url = peer_to_url(peer)?;
    let root = application
        .identity
        .path()
        .first()
        .ok_or_else(|| TCError::bad_request("application identity is empty"))?;
    url.set_path(&format!("/{root}"));
    let url = crate::uri::append_kernel_txn_id(&mut url, txn_id)?;
    let req = Request::builder()
        .method("PUT")
        .uri(url)
        .header(hyper::header::AUTHORIZATION, format!("Bearer {token}"))
        .header(hyper::header::CONTENT_TYPE, application.content_type)
        .body(Body::from(Bytes::from_owner(application.body)))
        .map_err(|err| TCError::bad_request(format!("invalid request: {err}")))?;
    let (status, body) = send_peer_request_at(client, req, deadline).await?;
    if status.is_success() {
        Ok(())
    } else {
        Err(TCError::bad_gateway(format!(
            "peer {peer} application work failed with status {status}: {}",
            String::from_utf8_lossy(&body)
        )))
    }
}

pub(crate) async fn delete_application(
    client: &HttpClient,
    peer: &str,
    token: &str,
    txn_id: TxnId,
    identity: &pathlink::Link,
    deadline: crate::Deadline,
) -> TCResult<()> {
    let mut url = peer_to_url(peer)?;
    url.set_path(&identity.to_string());
    let url = crate::uri::append_kernel_txn_id(&mut url, txn_id)?;
    let req = Request::builder()
        .method("DELETE")
        .uri(url)
        .header(hyper::header::AUTHORIZATION, format!("Bearer {token}"))
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Body::from("null"))
        .map_err(|err| TCError::bad_request(format!("invalid request: {err}")))?;
    let (status, body) = send_peer_request_at(client, req, deadline).await?;
    if status.is_success() {
        Ok(())
    } else {
        Err(TCError::bad_gateway(format!(
            "peer {peer} application deletion failed with status {status}: {}",
            String::from_utf8_lossy(&body)
        )))
    }
}

pub(crate) async fn decide_resource(
    client: &HttpClient,
    peer: &str,
    token: &str,
    txn_id: TxnId,
    resource: &pathlink::PathBuf,
    commit: bool,
    deadline: crate::Deadline,
) -> TCResult<()> {
    let mut url = peer_to_url(peer)?;
    url.set_path(&resource.to_string());
    let url = crate::uri::append_kernel_txn_id(&mut url, txn_id)?;

    let method = if commit { "PUT" } else { "DELETE" };
    let req = Request::builder()
        .method(method)
        .uri(url)
        .header(hyper::header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .map_err(|err| TCError::bad_request(format!("invalid request: {err}")))?;

    let (status, body) = send_peer_request_at(client, req, deadline).await?;
    if status.is_success() {
        Ok(())
    } else {
        Err(TCError::bad_gateway(format!(
            "peer {peer} transaction decision failed with status {status}: {}",
            String::from_utf8_lossy(&body)
        )))
    }
}

fn peer_to_url(peer: &str) -> TCResult<Url> {
    let value = if peer.contains("://") {
        peer.to_string()
    } else {
        format!("http://{peer}")
    };

    Url::parse(&value).map_err(|err| TCError::bad_request(format!("invalid peer url: {err}")))
}

async fn post_encrypted_peer_action(
    client: &HttpClient,
    seed: &str,
    path: &str,
    announcement: PeerAnnouncement,
    keys: &[aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>],
) -> TCResult<Bytes> {
    let peer = normalize_peer(&announcement.peer)?;
    let payload = serde_json::to_string(&PeerAnnouncement {
        peer,
        actor_id: announcement.actor_id,
        public_key_b64: announcement.public_key_b64,
    })
    .map_err(|err| TCError::bad_request(format!("invalid peer announcement: {err}")))?;
    let mut url = peer_to_url(seed)?;
    url.set_path(path);

    let mut last_error = String::new();
    for (idx, key) in keys.iter().enumerate() {
        let attempt = async {
            let (nonce, encrypted) = encrypt_path_with_key(&payload, key)?;
            let body = encode_encrypted_payload(&nonce, &encrypted)?;
            let request = Request::builder()
                .method(hyper::Method::POST)
                .uri(url.as_str())
                .body(Body::from(body))
                .map_err(|error| TCError::bad_request(format!("invalid request: {error}")))?;
            let (status, body) = send_peer_request(client, request).await?;
            if status.is_success() {
                Ok(body)
            } else {
                Err(TCError::bad_gateway(format!(
                    "status {status}: {}",
                    String::from_utf8_lossy(&body)
                )))
            }
        }
        .await;
        match attempt {
            Ok(body) => return Ok(body),
            Err(error) => last_error = format!("key[{idx}] {error}"),
        }
    }
    if last_error.is_empty() {
        Err(TCError::bad_gateway(
            "peer action has no configured PSK key",
        ))
    } else {
        Err(TCError::bad_gateway(format!(
            "peer action {path} failed: {last_error}"
        )))
    }
}

#[derive(Clone, serde::Deserialize, serde::Serialize)]
struct PeerAnnouncement {
    peer: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    actor_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    public_key_b64: Option<String>,
}

#[derive(serde::Deserialize)]
struct PeerListResponse {
    #[serde(default)]
    replicas: Vec<PeerIdentityRaw>,
    #[serde(default)]
    peers: Vec<String>,
}

#[derive(serde::Deserialize)]
struct PeerIdentityRaw {
    peer: String,
    actor_id: Option<String>,
    public_key_b64: Option<String>,
}

fn decode_peer_cluster_listing(body_bytes: &[u8]) -> TCResult<PeerClusterListing> {
    let listing: PeerListResponse = serde_json::from_slice(body_bytes)
        .map_err(|err| TCError::bad_gateway(format!("invalid peer list response: {err}")))?;

    let mut peers = BTreeSet::new();
    let mut identities = BTreeMap::new();
    for identity in listing.replicas {
        let peer = normalize_peer(&identity.peer)?;
        peers.insert(peer.clone());
        match (identity.actor_id, identity.public_key_b64) {
            (Some(actor_id), Some(public_key_b64))
                if !actor_id.trim().is_empty() && !public_key_b64.trim().is_empty() =>
            {
                identities.insert(
                    peer.clone(),
                    PeerIdentity {
                        peer,
                        actor_id,
                        public_key_b64,
                    },
                );
            }
            _ => {}
        }
    }

    for peer in listing.peers {
        peers.insert(normalize_peer(&peer)?);
    }

    Ok(PeerClusterListing {
        peers: peers.into_iter().collect(),
        identities: identities.into_values().collect(),
    })
}
