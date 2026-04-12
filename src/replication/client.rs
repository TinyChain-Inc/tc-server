use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::str::FromStr;
use std::time::Duration;

use hyper::body::to_bytes;
use hyper::{Body, Request, Response, StatusCode};
use number_general::Number;
use pathlink::Link;
use tc_error::{TCError, TCResult};
use tc_ir::{Id, TxnId};
use tc_state::{State, null_transaction};
use tc_value::Value;
use url::Url;

use crate::library::{InstallArtifacts, InstallRequest, decode_install_request_bytes};

use super::crypto::{
    decode_encrypted_payload, decrypt_token_with_key, encode_encrypted_payload,
    encrypt_path_with_key,
};
use super::{
    FORWARDED_HEADER, LIBRARY_EXPORT_PATH, PEERS_HEARTBEAT_PATH, PEERS_JOIN_PATH, PEERS_LEAVE_PATH,
    PEERS_PATH, PeerIdentity, TOKEN_PATH,
};

const PEER_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Debug, Default)]
pub struct PeerClusterListing {
    pub peers: Vec<String>,
    pub identities: Vec<PeerIdentity>,
}

async fn send_peer_request(req: Request<Body>) -> TCResult<Response<Body>> {
    let client = hyper::Client::new();
    let response = tokio::time::timeout(PEER_REQUEST_TIMEOUT, client.request(req))
        .await
        .map_err(|_| {
            TCError::bad_gateway(format!(
                "peer request timed out after {}s",
                PEER_REQUEST_TIMEOUT.as_secs()
            ))
        })?
        .map_err(|err| TCError::bad_gateway(err.to_string()))?;

    Ok(response)
}

pub async fn fetch_library_export(peer: &str, token: &str) -> TCResult<Option<InstallArtifacts>> {
    let mut url = peer_to_url(peer)?;
    url.set_path(LIBRARY_EXPORT_PATH);

    let req = build_export_request(url, token)?;
    let response = send_peer_request(req).await?;

    if response.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }

    let status = response.status();
    let body_bytes = to_bytes(response.into_body())
        .await
        .map_err(|err| TCError::bad_gateway(err.to_string()))?;

    if !status.is_success() {
        return Err(TCError::bad_gateway(format!(
            "peer {peer} export failed with status {status}"
        )));
    }

    let request = decode_install_request_bytes(&body_bytes)
        .map_err(|err| TCError::bad_gateway(err.message().to_string()))?;

    match request {
        InstallRequest::WithArtifacts(payload) => Ok(Some(payload)),
        InstallRequest::SchemaOnly(_) => Ok(None),
    }
}

pub async fn fetch_library_schema(peer: &str, path: &str) -> TCResult<tc_ir::LibrarySchema> {
    let mut url = peer_to_url(peer)?;
    url.set_path(path);

    let req = Request::builder()
        .method("GET")
        .uri(url.as_str())
        .body(Body::empty())
        .map_err(|err| TCError::bad_request(format!("invalid request: {err}")))?;

    let response = send_peer_request(req).await?;

    let status = response.status();
    let body_bytes = to_bytes(response.into_body())
        .await
        .map_err(|err| TCError::bad_gateway(err.to_string()))?;

    if !status.is_success() {
        return Err(TCError::bad_gateway(format!(
            "peer {peer} schema request failed with status {status}"
        )));
    }

    decode_schema_body(body_bytes).await
}

pub async fn fetch_library_listing(peer: &str, path: &str) -> TCResult<tc_ir::Map<bool>> {
    let mut url = peer_to_url(peer)?;
    url.set_path(path);

    let req = Request::builder()
        .method("GET")
        .uri(url.as_str())
        .body(Body::empty())
        .map_err(|err| TCError::bad_request(format!("invalid request: {err}")))?;

    let response = send_peer_request(req).await?;

    let status = response.status();
    let body_bytes = to_bytes(response.into_body())
        .await
        .map_err(|err| TCError::bad_gateway(err.to_string()))?;

    if !status.is_success() {
        return Err(TCError::bad_gateway(format!(
            "peer {peer} listing request failed with status {status}"
        )));
    }

    decode_listing_body(body_bytes).await
}

pub async fn discover_library_paths(peer: &str) -> TCResult<Vec<String>> {
    let mut pending = vec![crate::uri::LIB_ROOT.to_string()];
    let mut libraries = Vec::new();

    while let Some(path) = pending.pop() {
        let listing = fetch_library_listing(peer, &path).await?;
        for (name, is_dir) in listing {
            let next = if path == crate::uri::LIB_ROOT {
                format!("{}/{}", crate::uri::LIB_ROOT, name)
            } else {
                format!("{path}/{name}")
            };

            if is_dir {
                pending.push(next);
            } else {
                libraries.push(next);
            }
        }
    }

    Ok(libraries)
}

pub async fn request_replication_token(
    peer: &str,
    path: &str,
    keys: &[aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>],
) -> TCResult<String> {
    let mut url = peer_to_url(peer)?;
    url.set_path(TOKEN_PATH);
    with_psk_key_retries(
        keys,
        "no replication key succeeded: no keys configured",
        "no replication key succeeded",
        |idx, key| {
            let url = url.clone();
            async move {
                let (nonce, encrypted_path) =
                    encrypt_path_with_key(path, &key).map_err(|err| err.to_string())?;
                let body = encode_encrypted_payload(&nonce, &encrypted_path)
                    .map_err(|err| err.to_string())?;
                let req = Request::builder()
                    .method(hyper::Method::POST)
                    .uri(url.as_str())
                    .body(Body::from(body))
                    .map_err(|err| format!("key[{idx}] invalid request: {err}"))?;

                let response = send_peer_request(req)
                    .await
                    .map_err(|err| format!("key[{idx}] request failed: {err}"))?;

                let status = response.status();
                let body_bytes = to_bytes(response.into_body())
                    .await
                    .map_err(|err| format!("key[{idx}] response body read failed: {err}"))?;

                if !status.is_success() {
                    let message = String::from_utf8_lossy(&body_bytes);
                    return Err(format!("key[{idx}] status {status}: {message}"));
                }

                let (nonce, encrypted_token) = decode_encrypted_payload(body_bytes)
                    .map_err(|err| format!("key[{idx}] invalid encrypted payload: {err}"))?;

                if let Ok(token) = decrypt_token_with_key(&key, &nonce, &encrypted_token) {
                    return Ok(Some(token));
                }

                Err(format!("key[{idx}] could not decrypt token"))
            }
        },
    )
    .await
}

pub fn normalize_peer(peer: &str) -> TCResult<String> {
    let url = peer_to_url(peer)?;
    let host = url
        .host_str()
        .ok_or_else(|| TCError::bad_request("peer URL missing host"))?;
    let mut normalized = format!("{}://{}", url.scheme(), host);
    if let Some(port) = url.port() {
        normalized.push(':');
        normalized.push_str(&port.to_string());
    }
    Ok(normalized)
}

pub async fn register_with_peer(
    seed: &str,
    joiner: &PeerIdentity,
    keys: &[aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>],
) -> TCResult<PeerClusterListing> {
    post_encrypted_peer_action(
        seed,
        PEERS_JOIN_PATH,
        PeerAnnouncement {
            peer: joiner.peer.clone(),
            actor_id: Some(joiner.actor_id.clone()),
            public_key_b64: Some(joiner.public_key_b64.clone()),
        },
        keys,
    )
    .await?;
    list_peer_cluster(seed).await
}

pub async fn leave_peer_cluster(
    seed: &str,
    peer: &str,
    keys: &[aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>],
) -> TCResult<()> {
    post_encrypted_peer_action(
        seed,
        PEERS_LEAVE_PATH,
        PeerAnnouncement {
            peer: peer.to_string(),
            actor_id: None,
            public_key_b64: None,
        },
        keys,
    )
    .await
}

pub async fn heartbeat_peer(
    seed: &str,
    peer: &PeerIdentity,
    keys: &[aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>],
) -> TCResult<()> {
    post_encrypted_peer_action(
        seed,
        PEERS_HEARTBEAT_PATH,
        PeerAnnouncement {
            peer: peer.peer.clone(),
            actor_id: Some(peer.actor_id.clone()),
            public_key_b64: Some(peer.public_key_b64.clone()),
        },
        keys,
    )
    .await
}

#[allow(clippy::collapsible_if)]
pub async fn list_peer_cluster(seed: &str) -> TCResult<PeerClusterListing> {
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

    let mut url = peer_to_url(seed)?;
    url.set_path(PEERS_PATH);

    let req = Request::builder()
        .method("GET")
        .uri(url.as_str())
        .body(Body::empty())
        .map_err(|err| TCError::bad_request(format!("invalid request: {err}")))?;

    let response = send_peer_request(req).await?;

    let status = response.status();
    let body_bytes = to_bytes(response.into_body())
        .await
        .map_err(|err| TCError::bad_gateway(err.to_string()))?;

    if !status.is_success() {
        return Err(TCError::bad_gateway(format!(
            "peer {seed} list request failed with status {status}"
        )));
    }

    let listing: PeerListResponse = serde_json::from_slice(&body_bytes)
        .map_err(|err| TCError::bad_gateway(format!("invalid peer list response: {err}")))?;

    let mut peers = BTreeSet::new();
    let mut identities = BTreeMap::new();
    for identity in listing.replicas {
        let peer = normalize_peer(&identity.peer)?;
        peers.insert(peer.clone());
        if let (Some(actor_id), Some(public_key_b64)) = (identity.actor_id, identity.public_key_b64)
        {
            if !actor_id.trim().is_empty() && !public_key_b64.trim().is_empty() {
                identities.insert(
                    peer.clone(),
                    PeerIdentity {
                        peer,
                        actor_id,
                        public_key_b64,
                    },
                );
            }
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

pub(crate) async fn push_install_payload(
    peer: &str,
    token: &str,
    txn_id: TxnId,
    payload: Vec<u8>,
) -> TCResult<()> {
    let mut url = peer_to_url(peer)?;
    url.set_path(crate::uri::LIB_ROOT);
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("txn_id", &txn_id.to_string());
    }

    let req = Request::builder()
        .method("PUT")
        .uri(url.as_str())
        .header(hyper::header::AUTHORIZATION, format!("Bearer {token}"))
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .header(FORWARDED_HEADER, "1")
        .body(Body::from(payload))
        .map_err(|err| TCError::bad_request(format!("invalid request: {err}")))?;

    let response = send_peer_request(req).await?;

    if response.status().is_success() {
        Ok(())
    } else {
        Err(TCError::bad_gateway(format!(
            "peer {peer} install failed with status {}",
            response.status()
        )))
    }
}

pub(crate) async fn finalize_install_txn(
    peer: &str,
    token: &str,
    txn_id: TxnId,
    commit: bool,
) -> TCResult<()> {
    let mut url = peer_to_url(peer)?;
    url.set_path(crate::uri::LIB_ROOT);
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("txn_id", &txn_id.to_string());
    }

    let method = if commit { "POST" } else { "DELETE" };
    let req = Request::builder()
        .method(method)
        .uri(url.as_str())
        .header(hyper::header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .map_err(|err| TCError::bad_request(format!("invalid request: {err}")))?;

    let response = send_peer_request(req).await?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(TCError::bad_gateway(format!(
            "peer {peer} finalize failed with status {}",
            response.status()
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
    seed: &str,
    path: &str,
    announcement: PeerAnnouncement,
    keys: &[aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>],
) -> TCResult<()> {
    let peer = normalize_peer(&announcement.peer)?;
    let payload = serde_json::to_string(&PeerAnnouncement {
        peer,
        actor_id: announcement.actor_id,
        public_key_b64: announcement.public_key_b64,
    })
    .map_err(|err| TCError::bad_request(format!("invalid peer announcement: {err}")))?;
    let mut url = peer_to_url(seed)?;
    url.set_path(path);

    let failure_prefix = format!("peer action {path} failed");
    with_psk_key_retries(
        keys,
        "peer action failed for all PSK keys",
        &failure_prefix,
        |idx, key| {
            let payload = payload.clone();
            let url = url.clone();
            async move {
                let (nonce, encrypted_peer) =
                    encrypt_path_with_key(&payload, &key).map_err(|err| err.to_string())?;
                let body = encode_encrypted_payload(&nonce, &encrypted_peer)
                    .map_err(|err| err.to_string())?;
                let req = Request::builder()
                    .method(hyper::Method::POST)
                    .uri(url.as_str())
                    .body(Body::from(body))
                    .map_err(|err| format!("key[{idx}] invalid request: {err}"))?;

                let response = send_peer_request(req)
                    .await
                    .map_err(|err| format!("key[{idx}] request failed: {err}"))?;
                if response.status().is_success() {
                    return Ok(Some(()));
                }

                let status = response.status();
                let body = to_bytes(response.into_body())
                    .await
                    .map_err(|err| format!("key[{idx}] response body read failed: {err}"))?;
                let message = String::from_utf8_lossy(&body);
                Err(format!("key[{idx}] status {status}: {message}"))
            }
        },
    )
    .await
}

async fn with_psk_key_retries<T, F, Fut>(
    keys: &[aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>],
    no_keys_message: &str,
    failure_prefix: &str,
    mut attempt: F,
) -> TCResult<T>
where
    F: FnMut(usize, aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>) -> Fut,
    Fut: Future<Output = Result<Option<T>, String>>,
{
    let mut last_error = String::new();

    for (idx, key) in keys.iter().enumerate() {
        match attempt(idx, *key).await {
            Ok(Some(value)) => return Ok(value),
            Ok(None) => {}
            Err(err) => last_error = err,
        }
    }

    if last_error.is_empty() {
        Err(TCError::bad_gateway(no_keys_message))
    } else {
        Err(TCError::bad_gateway(format!(
            "{failure_prefix}: {last_error}"
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

fn build_export_request(url: Url, token: &str) -> TCResult<Request<Body>> {
    let builder = Request::builder()
        .method("GET")
        .uri(url.as_str())
        .header(hyper::header::AUTHORIZATION, format!("Bearer {token}"));

    builder
        .body(Body::empty())
        .map_err(|err| TCError::bad_request(format!("invalid request: {err}")))
}

async fn decode_schema_body(body: hyper::body::Bytes) -> TCResult<tc_ir::LibrarySchema> {
    let state = decode_state_body(body).await?;
    schema_from_state(state)
}

async fn decode_listing_body(body: hyper::body::Bytes) -> TCResult<tc_ir::Map<bool>> {
    let state = decode_state_body(body).await?;
    listing_from_state(state)
}

async fn decode_state_body(body: hyper::body::Bytes) -> TCResult<State> {
    use futures::stream;

    if body.is_empty() || body.iter().all(|b| b.is_ascii_whitespace()) {
        return Err(TCError::bad_request("empty response body"));
    }

    let stream = stream::iter(vec![Ok::<hyper::body::Bytes, std::io::Error>(body.clone())]);
    match destream_json::try_decode(null_transaction(), stream).await {
        Ok(state) => Ok(state),
        Err(_) => {
            let text = String::from_utf8(body.to_vec())
                .map_err(|err| TCError::bad_request(err.to_string()))?;
            let wrapped = format!(r#"{{"/state/scalar/map":{text}}}"#);
            let stream = stream::iter(vec![Ok::<hyper::body::Bytes, std::io::Error>(
                wrapped.into_bytes().into(),
            )]);
            destream_json::try_decode(null_transaction(), stream)
                .await
                .map_err(|err| TCError::bad_request(err.to_string()))
        }
    }
}

fn schema_from_state(state: State) -> TCResult<tc_ir::LibrarySchema> {
    let State::Map(map) = state else {
        return Err(TCError::bad_request("expected schema state map"));
    };

    let id_key: Id = "id".parse().expect("Id");
    let version_key: Id = "version".parse().expect("Id");
    let deps_key: Id = "dependencies".parse().expect("Id");

    let id = match map.get(&id_key) {
        Some(State::Scalar(tc_ir::Scalar::Value(Value::String(value)))) => value.clone(),
        _ => return Err(TCError::bad_request("missing schema id")),
    };
    let version = match map.get(&version_key) {
        Some(State::Scalar(tc_ir::Scalar::Value(Value::String(value)))) => value.clone(),
        _ => return Err(TCError::bad_request("missing schema version")),
    };
    let dependencies = match map.get(&deps_key) {
        Some(State::Map(deps)) => deps,
        _ => return Err(TCError::bad_request("missing schema dependencies")),
    };

    let mut items = dependencies
        .iter()
        .filter_map(|(key, value)| {
            let idx = key.as_str().parse::<usize>().ok()?;
            let State::Scalar(tc_ir::Scalar::Value(Value::String(link))) = value else {
                return None;
            };
            Some((idx, link.clone()))
        })
        .collect::<Vec<_>>();
    items.sort_by_key(|(idx, _)| *idx);

    let deps = items
        .into_iter()
        .map(|(_, link)| Link::from_str(&link).map_err(|err| TCError::bad_request(err.to_string())))
        .collect::<TCResult<Vec<_>>>()?;

    let schema_id = Link::from_str(&id).map_err(|err| TCError::bad_request(err.to_string()))?;
    Ok(tc_ir::LibrarySchema::new(schema_id, version, deps))
}

fn listing_from_state(state: State) -> TCResult<tc_ir::Map<bool>> {
    let map = unwrap_map_state(state)?;
    let listing_map = if map.len() == 1 {
        if let Some((name, value)) = map.iter().next() {
            if name.as_str() == "/state/scalar/map" {
                unwrap_map_state(value.clone())?
            } else {
                map
            }
        } else {
            map
        }
    } else {
        map
    };

    let mut listing = tc_ir::Map::new();
    for (name, value) in listing_map {
        if name.as_str() == "default" {
            continue;
        }

        let Some(is_dir) = parse_listing_bool(value) else {
            continue;
        };

        listing.insert(name, is_dir);
    }

    Ok(listing)
}

fn unwrap_map_state(state: State) -> TCResult<tc_ir::Map<State>> {
    let State::Map(map) = state else {
        return Err(TCError::bad_request("expected listing state map"));
    };
    Ok(map)
}

fn parse_listing_bool(value: State) -> Option<bool> {
    match value {
        State::Scalar(tc_ir::Scalar::Value(Value::Number(Number::Bool(value)))) => {
            Some(bool::from(value))
        }
        State::Scalar(tc_ir::Scalar::Value(Value::String(value))) => {
            Some(value.eq_ignore_ascii_case("true"))
        }
        State::Map(map) if map.len() == 1 => {
            let (_, wrapped) = map.into_iter().next().expect("single-item map");
            parse_listing_bool(wrapped)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{listing_from_state, parse_listing_bool};
    use futures::stream;
    use number_general::Number;
    use tc_state::State;
    use tc_state::null_transaction;

    fn decode_state(json: &str) -> State {
        futures::executor::block_on(async {
            let bytes = hyper::body::Bytes::from(json.as_bytes().to_vec());
            let stream = stream::iter(vec![Ok::<hyper::body::Bytes, std::io::Error>(bytes)]);
            destream_json::try_decode(null_transaction(), stream)
                .await
                .expect("decode")
        })
    }

    #[test]
    fn parse_wrapped_listing_bool() {
        let value = decode_state(r#"{"/state/scalar/value/number":true}"#);
        assert_eq!(parse_listing_bool(value), Some(true));
        assert_eq!(
            parse_listing_bool(State::from(Number::from(true))),
            Some(true)
        );
    }

    #[test]
    fn listing_ignores_typed_empty_map_default() {
        let listing_state = decode_state(
            r#"{
                "/state/scalar/map": {
                    "default": {
                        "/state/scalar/value/number": false
                    }
                }
            }"#,
        );

        let listing = listing_from_state(listing_state).expect("decode listing");
        assert!(
            listing.is_empty(),
            "empty typed listing should decode empty"
        );
    }
}
