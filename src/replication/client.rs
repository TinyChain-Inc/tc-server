use std::str::FromStr;

use hyper::body::to_bytes;
use hyper::{Body, Request, StatusCode};
use number_general::Number;
use pathlink::Link;
use tc_error::{TCError, TCResult};
use tc_ir::Id;
use tc_state::{null_transaction, State};
use tc_value::Value;
use url::Url;

use crate::library::{decode_install_request_bytes, InstallArtifacts, InstallRequest};

use super::crypto::{
    decode_encrypted_payload, decrypt_token_with_key, encode_encrypted_payload,
    encrypt_path_with_key,
};
use super::{LIBRARY_EXPORT_PATH, TOKEN_PATH};

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

fn peer_to_url(peer: &str) -> TCResult<Url> {
    let value = if peer.contains("://") {
        peer.to_string()
    } else {
        format!("http://{peer}")
    };

    Url::parse(&value).map_err(|err| TCError::bad_request(format!("invalid peer url: {err}")))
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
