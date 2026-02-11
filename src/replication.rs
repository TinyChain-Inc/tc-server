use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aes_gcm_siv::aead::rand_core::RngCore;
use aes_gcm_siv::aead::{Aead, OsRng};
use aes_gcm_siv::{Aes256GcmSiv, Key, KeyInit, Nonce};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures::FutureExt;
use hyper::body::to_bytes;
use hyper::{Body, Request, Response, StatusCode};
use number_general::Number;
use pathlink::Link;
use serde::{Deserialize, Serialize};
use tc_error::{TCError, TCResult};
use tc_ir::{Claim, Id};
use tc_state::{State, null_transaction};
use tc_value::Value;
use umask::Mode;
use url::Url;

use crate::KernelHandler;
use crate::auth::{Actor, KeyringActorResolver, PublicKeyStore, SignedToken, Token};
use crate::library::{
    InstallArtifacts, InstallRequest, LibraryRegistry, decode_install_request_bytes,
    encode_install_payload_bytes,
};
use crate::txn::TxnHandle;

pub const LIBRARY_EXPORT_PATH: &str = crate::uri::HOST_LIBRARY_EXPORT;
const TOKEN_PATH: &str = "/";
const REPLICATION_TTL: Duration = Duration::from_secs(30);

#[derive(Deserialize, Serialize)]
struct EncryptedPayload {
    nonce: String,
    data: String,
}

pub fn parse_psk_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| item.to_string())
        .collect()
}

pub fn parse_psk_keys(values: &[String]) -> TCResult<Vec<Key<Aes256GcmSiv>>> {
    values
        .iter()
        .map(|value| {
            let raw = hex::decode(value.trim()).map_err(|_| {
                TCError::bad_request("invalid PSK: expected hex-encoded 32-byte key")
            })?;
            if raw.len() != 32 {
                return Err(TCError::bad_request("invalid PSK: expected 32-byte key"));
            }
            Ok(Key::<Aes256GcmSiv>::from_slice(&raw).to_owned())
        })
        .collect()
}

pub struct ReplicationIssuer {
    host: pathlink::Link,
    keys: Vec<Key<Aes256GcmSiv>>,
    actors: Arc<Mutex<HashMap<String, Actor>>>,
    keyring: KeyringActorResolver,
    public_keys: PublicKeyStore,
}

impl ReplicationIssuer {
    pub fn new(
        host: pathlink::Link,
        keys: Vec<Key<Aes256GcmSiv>>,
        keyring: KeyringActorResolver,
        public_keys: PublicKeyStore,
    ) -> Self {
        Self {
            host,
            keys,
            actors: Arc::new(Mutex::new(HashMap::new())),
            keyring,
            public_keys,
        }
    }

    pub fn keyring(&self) -> KeyringActorResolver {
        self.keyring.clone()
    }

    pub fn public_keys(&self) -> PublicKeyStore {
        self.public_keys.clone()
    }

    pub fn keys(&self) -> &[Key<Aes256GcmSiv>] {
        &self.keys
    }

    fn actor_for_path(&self, path: &str) -> Actor {
        let mut actors = self.actors.lock().expect("replication actor map");
        if let Some(actor) = actors.get(path) {
            return actor.clone();
        }

        let actor = Actor::new(Value::String(path.to_string()));
        self.public_keys.insert_actor(&actor);
        let _ = self
            .keyring
            .clone()
            .with_actor(self.host.clone(), actor.clone());
        actors.insert(path.to_string(), actor.clone());
        actor
    }

    pub fn issue_token(&self, path: &str) -> TCResult<SignedToken> {
        let claim = Claim::new(
            pathlink::Link::from_str(path)
                .map_err(|err| TCError::bad_request(format!("invalid claim path: {err}")))?,
            Mode::all(),
        );

        let actor = self.actor_for_path(path);
        let token = Token::new(
            self.host.clone(),
            std::time::SystemTime::now(),
            REPLICATION_TTL,
            actor.id().clone(),
            claim,
        );

        actor
            .sign_token(token)
            .map_err(|err| TCError::internal(err.to_string()))
    }

    pub fn decrypt_path_with_key(
        &self,
        nonce: &[u8],
        ciphertext: &[u8],
    ) -> TCResult<(String, Key<Aes256GcmSiv>)> {
        for key in &self.keys {
            let cipher = Aes256GcmSiv::new(key);
            if let Ok(path) = decrypt_path(&cipher, nonce, ciphertext) {
                return Ok((path, *key));
            }
        }

        Err(TCError::bad_request("unable to decrypt replication path"))
    }
}

pub fn replication_token_handler(issuer: Arc<ReplicationIssuer>) -> impl KernelHandler {
    move |req: Request<Body>| {
        let issuer = issuer.clone();
        async move {
            if req.uri().path() != TOKEN_PATH {
                return Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("not found");
            }

            let body_bytes = match to_bytes(req.into_body()).await {
                Ok(bytes) => bytes,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(err.to_string()))
                        .expect("bad request");
                }
            };

            let (nonce, path_encrypted) = match decode_encrypted_payload(body_bytes) {
                Ok(tuple) => tuple,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(err.to_string()))
                        .expect("bad request");
                }
            };

            let (path, key) = match issuer.decrypt_path_with_key(&nonce, &path_encrypted) {
                Ok(result) => result,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(err.to_string()))
                        .expect("bad request");
                }
            };

            let signed = match issuer.issue_token(&path) {
                Ok(token) => token,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(err.to_string()))
                        .expect("internal error");
                }
            };

            let token = signed.into_jwt();

            let mut nonce = [0u8; 12];
            OsRng.fill_bytes(&mut nonce);
            let cipher = Aes256GcmSiv::new(&key);
            let encrypted = match encrypt_token(&cipher, &nonce, token) {
                Ok(encrypted) => encrypted,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(err.to_string()))
                        .expect("internal error");
                }
            };

            let bytes = match encode_encrypted_payload(&nonce, &encrypted) {
                Ok(bytes) => bytes,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(err.to_string()))
                        .expect("internal error");
                }
            };

            Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(Body::from(bytes))
                .expect("token response")
        }
        .boxed()
    }
}

pub fn export_handler(registry: Arc<LibraryRegistry>) -> impl KernelHandler {
    move |req: Request<Body>| {
        let registry = Arc::clone(&registry);
        async move {
            if req.uri().path() != LIBRARY_EXPORT_PATH {
                return Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("not found");
            }

            let Some(txn) = req.extensions().get::<TxnHandle>() else {
                return Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body(Body::from("missing transaction context"))
                    .expect("unauthorized");
            };

            let payload = match registry.export_payload_for_claims(txn).await {
                Ok(Some(payload)) => payload,
                Ok(None) => {
                    return Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Body::empty())
                        .expect("not found");
                }
                Err(err) if err.code() == tc_error::ErrorKind::Unauthorized => {
                    return Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body(Body::from("unauthorized"))
                        .expect("unauthorized");
                }
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(err.to_string()))
                        .expect("internal error");
                }
            };

            let bytes = match encode_install_payload_bytes(&payload) {
                Ok(bytes) => bytes,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(err))
                        .expect("internal error");
                }
            };

            Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(Body::from(bytes))
                .expect("export response")
        }
        .boxed()
    }
}

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
            "peer {peer} schema fetch failed with status {status}"
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

    if response.status() == StatusCode::NOT_FOUND {
        return Ok(tc_ir::Map::new());
    }

    let status = response.status();
    let body_bytes = to_bytes(response.into_body())
        .await
        .map_err(|err| TCError::bad_gateway(err.to_string()))?;

    if !status.is_success() {
        return Err(TCError::bad_gateway(format!(
            "peer {peer} listing fetch failed with status {status}"
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
    keys: &[Key<Aes256GcmSiv>],
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

fn decode_encrypted_payload(body: hyper::body::Bytes) -> TCResult<(Vec<u8>, Vec<u8>)> {
    if body.is_empty() || body.iter().all(|b| b.is_ascii_whitespace()) {
        return Err(TCError::bad_request("empty replication payload"));
    }

    let payload: EncryptedPayload = serde_json::from_slice(&body)
        .map_err(|err| TCError::bad_request(format!("invalid replication payload: {err}")))?;

    let nonce = BASE64
        .decode(payload.nonce.as_bytes())
        .map_err(|err| TCError::bad_request(format!("invalid nonce base64: {err}")))?;
    let data = BASE64
        .decode(payload.data.as_bytes())
        .map_err(|err| TCError::bad_request(format!("invalid data base64: {err}")))?;

    Ok((nonce, data))
}

fn encode_encrypted_payload(nonce: &[u8], data: &[u8]) -> TCResult<Vec<u8>> {
    let payload = EncryptedPayload {
        nonce: BASE64.encode(nonce),
        data: BASE64.encode(data),
    };

    serde_json::to_vec(&payload)
        .map_err(|err| TCError::internal(format!("encode replication payload failed: {err}")))
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

fn encrypt_path_with_key(path: &str, key: &Key<Aes256GcmSiv>) -> TCResult<(Vec<u8>, Vec<u8>)> {
    let cipher = Aes256GcmSiv::new(key);
    let mut nonce = [0u8; 12];
    OsRng.fill_bytes(&mut nonce);
    let encrypted = encrypt_path(&cipher, &nonce, path)?;
    Ok((nonce.to_vec(), encrypted))
}

fn decrypt_token_with_key(
    key: &Key<Aes256GcmSiv>,
    nonce: &[u8],
    token_encrypted: &[u8],
) -> TCResult<String> {
    let cipher = Aes256GcmSiv::new(key);
    decrypt_token(&cipher, nonce, token_encrypted)
}

fn decrypt_path(cipher: &Aes256GcmSiv, nonce: &[u8], path_encrypted: &[u8]) -> TCResult<String> {
    let nonce: [u8; 12] = nonce
        .try_into()
        .map_err(|_| TCError::bad_request("invalid nonce length"))?;
    let nonce = Nonce::from_slice(&nonce);

    match cipher.decrypt(nonce, path_encrypted) {
        Ok(path_decrypted) => String::from_utf8(path_decrypted)
            .map_err(|cause| TCError::bad_request(format!("invalid UTF8: {cause}"))),
        Err(_cause) => Err(TCError::bad_request("unable to decrypt path")),
    }
}

fn decrypt_token(cipher: &Aes256GcmSiv, nonce: &[u8], token_encrypted: &[u8]) -> TCResult<String> {
    let nonce: [u8; 12] = nonce
        .try_into()
        .map_err(|_| TCError::bad_request("invalid nonce length"))?;
    let nonce = Nonce::from_slice(&nonce);

    match cipher.decrypt(nonce, token_encrypted) {
        Ok(token_decrypted) => String::from_utf8(token_decrypted)
            .map_err(|cause| TCError::bad_request(format!("invalid UTF8: {cause}"))),
        Err(_cause) => Err(TCError::bad_request("unable to decrypt token")),
    }
}

fn encrypt_path(cipher: &Aes256GcmSiv, nonce: &[u8], path: &str) -> TCResult<Vec<u8>> {
    let nonce: [u8; 12] = nonce
        .try_into()
        .map_err(|_| TCError::bad_request("invalid nonce length"))?;
    let nonce = Nonce::from_slice(&nonce);
    cipher
        .encrypt(nonce, path.as_bytes())
        .map_err(|_| TCError::internal("unable to encrypt path"))
}

fn encrypt_token(cipher: &Aes256GcmSiv, nonce: &[u8], token: String) -> TCResult<Vec<u8>> {
    let nonce: [u8; 12] = nonce
        .try_into()
        .map_err(|_| TCError::bad_request("invalid nonce length"))?;
    let nonce = Nonce::from_slice(&nonce);
    cipher
        .encrypt(nonce, token.as_bytes())
        .map_err(|_| TCError::internal("unable to encrypt token"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::library::http::build_http_library_module;
    use crate::storage::{LibraryStore, load_library_root};
    use pathlink::Link;
    use tc_ir::LibrarySchema;

    #[tokio::test]
    async fn issues_replication_token() {
        let dir = std::env::temp_dir().join(format!(
            "tc-psk-rotation-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create dir");

        let schema = LibrarySchema::new(
            Link::from_str("/lib/example-devco/hello/0.1.0").expect("link"),
            "0.1.0",
            vec![],
        );
        let root = load_library_root(dir).await.expect("store root");
        let store = LibraryStore::from_root(root);
        let store = store.for_schema(&schema).await.expect("store");
        store
            .persist_artifact(
                &schema,
                &crate::storage::Artifact {
                    path: schema.id().to_string(),
                    content_type: crate::ir::WASM_ARTIFACT_CONTENT_TYPE.to_string(),
                    bytes: b"fake-wasm".to_vec(),
                },
            )
            .await
            .expect("persist wasm");

        let _module = build_http_library_module(schema.clone(), None)
            .await
            .expect("module");

        let host = pathlink::Link::from_str("/host").expect("host");
        let keyring = KeyringActorResolver::default();
        let public_keys = PublicKeyStore::default();
        let keys = parse_psk_keys(&vec![
            "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
            "0000000000000000000000000000000000000000000000000000000000000002".to_string(),
        ])
        .expect("keys");

        let issuer = Arc::new(ReplicationIssuer::new(
            host,
            keys.clone(),
            keyring,
            public_keys,
        ));
        let token_handler = replication_token_handler(issuer.clone());

        let token = {
            let (nonce, encrypted) =
                encrypt_path_with_key(schema.id().to_string().as_str(), &keys[0]).expect("encrypt");
            let body = encode_encrypted_payload(&nonce, &encrypted).expect("encode body");
            let req = Request::builder()
                .uri(TOKEN_PATH)
                .body(Body::from(body))
                .expect("request");
            let response = futures::executor::block_on(token_handler.call(req));
            assert_eq!(response.status(), StatusCode::OK);
            let body = futures::executor::block_on(to_bytes(response.into_body())).expect("body");
            let (nonce, encrypted) = decode_encrypted_payload(body).expect("nonce");
            decrypt_token_with_key(&keys[0], &nonce, &encrypted).expect("decrypt")
        };
        assert!(!token.is_empty());
    }
}
