use aes_gcm_siv::{Aes256GcmSiv, Key, KeyInit};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use tc_error::TCError;
use tc_ir::TxnId;

use crate::Claim;
use umask::USER_EXEC;

use crate::auth::{Actor, KeyringActorResolver};

use super::Replica;
use super::crypto::{
    decode_encrypted_payload, decrypt_path, encode_encrypted_payload, encrypt_path_with_key,
};

#[derive(serde::Deserialize, serde::Serialize)]
pub(crate) struct BootstrapRequest {
    pub(crate) resource: String,
    txn_id: String,
    identity: Replica,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct BootstrapResponse {
    token: String,
    host: String,
    actor_id: String,
    algorithm: rjwt::AlgKind,
    public_key_b64: String,
    state_hash: String,
}

#[cfg(feature = "http-client")]
pub(crate) struct BootstrapSession {
    pub(crate) token: String,
    pub(crate) replica: crate::replication::Replica,
    pub(crate) state_hash: String,
}

pub fn parse_psk_keys(values: &[String]) -> tc_error::TCResult<Vec<Key<Aes256GcmSiv>>> {
    values
        .iter()
        .map(|value| {
            let raw = hex::decode(value.trim()).map_err(|_| {
                TCError::bad_request("invalid PSK: expected hex-encoded 32-byte key")
            })?;
            let raw: [u8; 32] = raw
                .try_into()
                .map_err(|_| TCError::bad_request("invalid PSK: expected 32-byte key"))?;
            Ok(raw.into())
        })
        .collect()
}

pub struct ReplicationIssuer {
    authority: std::sync::Arc<crate::ProtocolAuthority>,
    keys: Vec<Key<Aes256GcmSiv>>,
    keyring: KeyringActorResolver,
}

impl ReplicationIssuer {
    pub fn local(
        authority: &crate::ProtocolAuthority,
        keyring: KeyringActorResolver,
    ) -> tc_error::TCResult<Self> {
        Self::new(std::sync::Arc::new(authority.clone()), Vec::new(), keyring)
    }

    pub fn new(
        authority: std::sync::Arc<crate::ProtocolAuthority>,
        keys: Vec<Key<Aes256GcmSiv>>,
        keyring: KeyringActorResolver,
    ) -> tc_error::TCResult<Self> {
        let signer_public = authority.actor.verifying_key();
        let signer_id = authority.actor.id().clone();
        keyring.insert(
            authority.host.clone(),
            Actor::with_verifying_key(signer_id, signer_public),
        )?;

        Ok(Self {
            authority,
            keys,
            keyring,
        })
    }

    pub fn self_identity(&self, endpoint: String) -> tc_error::TCResult<Replica> {
        let public_key_b64 = BASE64.encode(self.authority.actor.verifying_key().to_bytes());
        Ok(Replica {
            endpoint,
            host: self.authority.host.to_string(),
            actor_id: self.authority.actor.id().clone(),
            algorithm: self.authority.actor.verifying_key().alg(),
            public_key_b64,
        })
    }

    pub fn register_peer_identity(&self, identity: &Replica) -> tc_error::TCResult<()> {
        let actor_id = identity.actor_id.trim();
        if actor_id.is_empty() {
            return Err(TCError::bad_request("peer actor_id must not be empty"));
        }

        let key_bytes = BASE64
            .decode(identity.public_key_b64.trim())
            .map_err(|err| TCError::bad_request(format!("invalid peer public_key_b64: {err}")))?;
        let verifying_key =
            crate::auth::verifying_key_from_bytes(identity.algorithm, key_bytes.as_slice())
                .map_err(|err| {
                    TCError::bad_request(format!("invalid peer public key bytes: {err}"))
                })?;

        let host = identity.host.parse().map_err(|error| {
            TCError::bad_request(format!("invalid peer host identity: {error}"))
        })?;
        let actor = Actor::with_verifying_key(actor_id.to_string(), verifying_key);
        self.keyring.insert(host, actor)
    }

    pub fn decrypt_path_with_key(
        &self,
        nonce: &[u8],
        ciphertext: &[u8],
    ) -> tc_error::TCResult<(String, Key<Aes256GcmSiv>)> {
        for key in &self.keys {
            let cipher = Aes256GcmSiv::new(key);
            if let Ok(path) = decrypt_path(&cipher, nonce, ciphertext) {
                return Ok((path, *key));
            }
        }

        Err(TCError::bad_request("unable to decrypt replication path"))
    }

    pub(crate) fn open_request(
        &self,
        txn_id: TxnId,
        encrypted: &[u8],
    ) -> tc_error::TCResult<(BootstrapRequest, Key<Aes256GcmSiv>)> {
        let (nonce, ciphertext) = decode_encrypted_payload(encrypted)?;
        let (payload, key) = self.decrypt_path_with_key(&nonce, &ciphertext)?;
        let request: BootstrapRequest = serde_json::from_str(&payload)
            .map_err(|error| TCError::bad_request(format!("invalid bootstrap request: {error}")))?;
        if request.txn_id != txn_id.to_string() {
            return Err(TCError::conflict(
                "bootstrap transaction identity changed in transit",
            ));
        }
        let resource: pathlink::Link = request.resource.parse().map_err(|error| {
            TCError::bad_request(format!("invalid bootstrap resource: {error}"))
        })?;
        if !matches!(
            resource.path().first().map(pathlink::PathSegment::as_str),
            Some("lib" | "class" | "service")
        ) {
            return Err(TCError::bad_request(
                "bootstrap must name an application Cluster",
            ));
        }
        self.register_peer_identity(&request.identity)?;

        Ok((request, key))
    }

    pub(crate) fn seal_response(
        &self,
        txn_id: TxnId,
        resource: pathlink::Link,
        state_hash: [u8; 32],
        key: &Key<Aes256GcmSiv>,
    ) -> tc_error::TCResult<Vec<u8>> {
        let grants = crate::auth::wire_claim(Claim::new(
            crate::uri::transaction_path(txn_id)
                .parse()
                .expect("transaction path"),
            USER_EXEC,
        ));
        let signed = self
            .authority
            .actor
            .sign_token(crate::auth::Token::new(
                self.authority.host.clone(),
                std::time::SystemTime::now(),
                std::time::Duration::from_secs(30),
                self.authority.actor.id().clone(),
                grants,
            ))
            .map_err(|error| TCError::unauthorized(error.to_string()))?;
        let signed = self
            .authority
            .actor
            .consume_and_sign(
                signed,
                self.authority.host.clone(),
                crate::auth::wire_claim(Claim::new(resource, USER_EXEC)),
                std::time::SystemTime::now(),
            )
            .map_err(|error| TCError::unauthorized(error.to_string()))?;
        let response = serde_json::to_string(&BootstrapResponse {
            token: signed.into_jwt(),
            host: self.authority.host.to_string(),
            actor_id: self.authority.actor.id().clone(),
            algorithm: self.authority.actor.verifying_key().alg(),
            public_key_b64: BASE64.encode(self.authority.actor.verifying_key().to_bytes()),
            state_hash: hex::encode(state_hash),
        })
        .map_err(|error| TCError::internal(format!("encode bootstrap response: {error}")))?;
        let (nonce, ciphertext) = encrypt_path_with_key(&response, key)?;
        encode_encrypted_payload(&nonce, &ciphertext)
    }

    #[cfg(feature = "http-client")]
    pub(crate) fn bootstrap_requests(
        &self,
        txn_id: TxnId,
        resource: &pathlink::Link,
        identity: &Replica,
    ) -> tc_error::TCResult<Vec<Vec<u8>>> {
        let request = serde_json::to_string(&BootstrapRequest {
            resource: resource.to_string(),
            txn_id: txn_id.to_string(),
            identity: identity.clone(),
        })
        .map_err(|error| TCError::internal(format!("encode bootstrap request: {error}")))?;
        self.keys
            .iter()
            .map(|key| {
                let (nonce, ciphertext) = encrypt_path_with_key(&request, key)?;
                encode_encrypted_payload(&nonce, &ciphertext)
            })
            .collect()
    }

    #[cfg(feature = "http-client")]
    pub(crate) fn open_response(
        &self,
        endpoint: String,
        encrypted: &[u8],
    ) -> tc_error::TCResult<BootstrapSession> {
        let (nonce, ciphertext) = decode_encrypted_payload(encrypted)?;
        let (response, _) = self.decrypt_path_with_key(&nonce, &ciphertext)?;
        let response: BootstrapResponse = serde_json::from_str(&response).map_err(|error| {
            TCError::bad_gateway(format!("invalid bootstrap response: {error}"))
        })?;
        let replica = Replica {
            endpoint,
            host: response.host,
            actor_id: response.actor_id,
            algorithm: response.algorithm,
            public_key_b64: response.public_key_b64,
        };
        self.register_peer_identity(&replica)?;
        Ok(BootstrapSession {
            token: response.token,
            replica,
            state_hash: response.state_hash,
        })
    }
}

#[cfg(all(test, feature = "http-client"))]
mod tests {
    use super::*;

    fn issuer(label: &str, keys: Vec<Key<Aes256GcmSiv>>) -> ReplicationIssuer {
        let actor = Actor::new_falcon512(label.to_string()).expect("test actor");
        let authority = std::sync::Arc::new(crate::ProtocolAuthority::new(
            format!("https://{label}.example").parse().expect("host"),
            actor,
        ));
        ReplicationIssuer::new(authority, keys, KeyringActorResolver::default())
            .expect("replication issuer")
    }

    #[test]
    fn psk_rotation_accepts_overlap_and_rejects_a_retired_key() {
        let old = Key::<Aes256GcmSiv>::from([7; 32]);
        let new = Key::<Aes256GcmSiv>::from([8; 32]);
        let seed = issuer("seed", vec![old, new]);
        let old_client = issuer("old-client", vec![old]);
        let new_client = issuer("new-client", vec![new]);
        let txn_id = TxnId::from_parts(tc_ir::NetworkTime::from_nanos(1), 0);
        let resource: pathlink::Link = "/lib".parse().expect("resource");

        for client in [&old_client, &new_client] {
            let identity = client
                .self_identity("http://127.0.0.1:8702".to_string())
                .expect("identity");
            let encrypted = client
                .bootstrap_requests(txn_id, &resource, &identity)
                .expect("bootstrap request")
                .remove(0);
            seed.open_request(txn_id, &encrypted)
                .expect("overlapping PSK accepted");
        }

        let rotated = issuer("rotated-seed", vec![new]);
        let identity = old_client
            .self_identity("http://127.0.0.1:8703".to_string())
            .expect("identity");
        let encrypted = old_client
            .bootstrap_requests(txn_id, &resource, &identity)
            .expect("bootstrap request")
            .remove(0);
        assert!(rotated.open_request(txn_id, &encrypted).is_err());
    }
}
