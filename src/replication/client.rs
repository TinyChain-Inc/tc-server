use std::str::FromStr;

use hyper::body::to_bytes;
use hyper::{Body, Request, StatusCode};
use number_general::Number;
use pathlink::Link;
use tc_error::{TCError, TCResult};
use tc_ir::Id;
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
    PEERS_PATH, TOKEN_PATH,
};

pub async fn fetch_library_export(peer: &str, token: &str) -> TCResult<Option<InstallArtifacts>> {
    let mut url = peer_to_url(peer)?;
    url.set_path(LIBRARY_EXPORT_PATH);

    let req = build_export_request(url, token)?;
    let client = hyper::Client::new();

    let response = client
        .request(req)
        .await
        .map_err(|err| TCError::bad_gateway(err.to_string()))?;

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

    let client = hyper::Client::new();
    let response = client
        .request(req)
        .await
        .map_err(|err| TCError::bad_gateway(err.to_string()))?;

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

    let client = hyper::Client::new();
    let response = client
        .request(req)
        .await
        .map_err(|err| TCError::bad_gateway(err.to_string()))?;

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

    for key in keys {
        let (nonce, encrypted_path) = encrypt_path_with_key(path, key)?;
        let body = encode_encrypted_payload(&nonce, &encrypted_path)?;
        let req = Request::builder()
            .method("GET")
            .uri(url.as_str())
            .body(Body::from(body))
            .map_err(|err| TCError::bad_request(format!("invalid request: {err}")))?;

        let client = hyper::Client::new();
        let response = client
            .request(req)
            .await
            .map_err(|err| TCError::bad_gateway(err.to_string()))?;

        let status = response.status();
        let body_bytes = to_bytes(response.into_body())
            .await
            .map_err(|err| TCError::bad_gateway(err.to_string()))?;

        if !status.is_success() {
            continue;
        }

        let (nonce, encrypted_token) = decode_encrypted_payload(body_bytes)?;
        if let Ok(token) = decrypt_token_with_key(key, &nonce, &encrypted_token) {
            return Ok(token);
        }
    }

    Err(TCError::bad_gateway("no replication key succeeded"))
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
    joiner: &str,
    keys: &[aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>],
) -> TCResult<Vec<String>> {
    post_encrypted_peer_action(seed, PEERS_JOIN_PATH, joiner, keys).await?;
    list_peer_cluster(seed).await
}

pub async fn leave_peer_cluster(
    seed: &str,
    peer: &str,
    keys: &[aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>],
) -> TCResult<()> {
    post_encrypted_peer_action(seed, PEERS_LEAVE_PATH, peer, keys).await
}

pub async fn heartbeat_peer(
    seed: &str,
    peer: &str,
    keys: &[aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>],
) -> TCResult<()> {
    post_encrypted_peer_action(seed, PEERS_HEARTBEAT_PATH, peer, keys).await
}

pub async fn list_peer_cluster(seed: &str) -> TCResult<Vec<String>> {
    #[derive(serde::Deserialize)]
    struct PeerList {
        peers: Vec<String>,
    }

    let mut url = peer_to_url(seed)?;
    url.set_path(PEERS_PATH);

    let req = Request::builder()
        .method("GET")
        .uri(url.as_str())
        .body(Body::empty())
        .map_err(|err| TCError::bad_request(format!("invalid request: {err}")))?;

    let response = hyper::Client::new()
        .request(req)
        .await
        .map_err(|err| TCError::bad_gateway(err.to_string()))?;

    let status = response.status();
    let body_bytes = to_bytes(response.into_body())
        .await
        .map_err(|err| TCError::bad_gateway(err.to_string()))?;

    if !status.is_success() {
        return Err(TCError::bad_gateway(format!(
            "peer {seed} list request failed with status {status}"
        )));
    }

    let listing: PeerList = serde_json::from_slice(&body_bytes)
        .map_err(|err| TCError::bad_gateway(format!("invalid peer list response: {err}")))?;

    listing
        .peers
        .into_iter()
        .map(|peer| normalize_peer(&peer))
        .collect()
}

pub(crate) async fn push_install_payload(
    peer: &str,
    token: &str,
    payload: Vec<u8>,
) -> TCResult<()> {
    let mut url = peer_to_url(peer)?;
    url.set_path(crate::uri::LIB_ROOT);

    let req = Request::builder()
        .method("PUT")
        .uri(url.as_str())
        .header(hyper::header::AUTHORIZATION, format!("Bearer {token}"))
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .header(FORWARDED_HEADER, "1")
        .body(Body::from(payload))
        .map_err(|err| TCError::bad_request(format!("invalid request: {err}")))?;

    let response = hyper::Client::new()
        .request(req)
        .await
        .map_err(|err| TCError::bad_gateway(err.to_string()))?;

    if response.status().is_success() {
        Ok(())
    } else {
        Err(TCError::bad_gateway(format!(
            "peer {peer} install failed with status {}",
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
    peer: &str,
    keys: &[aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>],
) -> TCResult<()> {
    let peer = normalize_peer(peer)?;
    let mut url = peer_to_url(seed)?;
    url.set_path(path);

    for key in keys {
        let (nonce, encrypted_peer) = encrypt_path_with_key(&peer, key)?;
        let body = encode_encrypted_payload(&nonce, &encrypted_peer)?;
        let req = Request::builder()
            .method("POST")
            .uri(url.as_str())
            .body(Body::from(body))
            .map_err(|err| TCError::bad_request(format!("invalid request: {err}")))?;

        let response = hyper::Client::new()
            .request(req)
            .await
            .map_err(|err| TCError::bad_gateway(err.to_string()))?;

        if response.status().is_success() {
            return Ok(());
        }

        let status = response.status();
        let body = to_bytes(response.into_body())
            .await
            .map_err(|err| TCError::bad_gateway(err.to_string()))?;
        let message = String::from_utf8_lossy(&body);
        return Err(TCError::bad_gateway(format!(
            "peer action {path} failed with status {status}: {message}"
        )));
    }

    Err(TCError::bad_gateway("peer action failed for all PSK keys"))
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
    let State::Map(map) = state else {
        return Err(TCError::bad_request("expected listing state map"));
    };

    let mut listing = tc_ir::Map::new();
    for (name, value) in map {
        let is_dir = match value {
            State::Scalar(tc_ir::Scalar::Value(Value::Number(Number::Bool(value)))) => {
                bool::from(value)
            }
            State::Scalar(tc_ir::Scalar::Value(Value::String(value))) => {
                value.eq_ignore_ascii_case("true")
            }
            _ => false,
        };
        listing.insert(name, is_dir);
    }

    Ok(listing)
}
