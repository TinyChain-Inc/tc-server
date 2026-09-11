use aes_gcm_siv::{Aes256GcmSiv, Key, KeyInit};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use destream::{de, en};
use futures::TryStreamExt;
use tc_error::TCError;
use tc_ir::TxnId;

use crate::Claim;
use umask::USER_EXEC;

use crate::auth::{Actor, KeyringActorResolver};

use super::Replica;
use super::crypto::{decrypt, encrypt_with_key};

const MAX_BOOTSTRAP_MESSAGE_BYTES: usize = 1024 * 1024;

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

    pub fn decrypt_with_key(
        &self,
        nonce: &[u8],
        ciphertext: &[u8],
    ) -> tc_error::TCResult<(Vec<u8>, Key<Aes256GcmSiv>)> {
        for key in &self.keys {
            let cipher = Aes256GcmSiv::new(key);
            if let Ok(plaintext) = decrypt(&cipher, nonce, ciphertext) {
                return Ok((plaintext, *key));
            }
        }

        Err(TCError::bad_request("unable to decrypt replication path"))
    }

    pub(crate) async fn open_request(
        &self,
        txn_id: TxnId,
        nonce: &[u8],
        ciphertext: &[u8],
    ) -> tc_error::TCResult<(pathlink::Link, Key<Aes256GcmSiv>)> {
        let (payload, key) = self.decrypt_with_key(nonce, ciphertext)?;
        let (resource, received_txn_id, endpoint, host, actor_id, algorithm, public_key_b64): (
            String,
            String,
            String,
            String,
            String,
            String,
            String,
        ) = decode_message(payload, "bootstrap request").await?;
        let received_txn_id: TxnId = received_txn_id.parse().map_err(|error| {
            TCError::bad_request(format!("invalid bootstrap transaction: {error}"))
        })?;
        if received_txn_id != txn_id {
            return Err(TCError::conflict(
                "bootstrap transaction identity changed in transit",
            ));
        }
        let resource: pathlink::Link = resource.parse().map_err(|error| {
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
        self.register_peer_identity(&Replica {
            endpoint,
            host,
            actor_id,
            algorithm: algorithm
                .parse()
                .map_err(|error: rjwt::Error| TCError::bad_request(error.to_string()))?,
            public_key_b64,
        })?;

        Ok((resource, key))
    }

    pub(crate) async fn seal_response(
        &self,
        txn_id: TxnId,
        resource: pathlink::Link,
        state_hash: [u8; 32],
        key: &Key<Aes256GcmSiv>,
    ) -> tc_error::TCResult<(Vec<u8>, Vec<u8>)> {
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
        let response = (
            signed.into_jwt(),
            self.authority.host.to_string(),
            self.authority.actor.id().clone(),
            self.authority
                .actor
                .verifying_key()
                .alg()
                .name()
                .to_string(),
            BASE64.encode(self.authority.actor.verifying_key().to_bytes()),
            hex::encode(state_hash),
        );
        let response = encode_message(&response).await?;
        encrypt_with_key(&response, key)
    }

    #[cfg(feature = "http-client")]
    pub(crate) async fn bootstrap_requests(
        &self,
        txn_id: TxnId,
        resource: &pathlink::Link,
        identity: &Replica,
    ) -> tc_error::TCResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let request = (
            resource.to_string(),
            txn_id.to_string(),
            identity.endpoint.clone(),
            identity.host.clone(),
            identity.actor_id.clone(),
            identity.algorithm.name().to_string(),
            identity.public_key_b64.clone(),
        );
        let request = encode_message(&request).await?;
        self.keys
            .iter()
            .map(|key| encrypt_with_key(&request, key))
            .collect()
    }

    #[cfg(feature = "http-client")]
    pub(crate) async fn open_response(
        &self,
        endpoint: String,
        nonce: &[u8],
        ciphertext: &[u8],
    ) -> tc_error::TCResult<BootstrapSession> {
        let (response, _) = self.decrypt_with_key(nonce, ciphertext)?;
        let (token, host, actor_id, algorithm, public_key_b64, state_hash): (
            String,
            String,
            String,
            String,
            String,
            String,
        ) = decode_message(response, "bootstrap response").await?;
        let replica = Replica {
            endpoint,
            host,
            actor_id,
            algorithm: algorithm
                .parse()
                .map_err(|error: rjwt::Error| TCError::bad_gateway(error.to_string()))?,
            public_key_b64,
        };
        self.register_peer_identity(&replica)?;
        Ok(BootstrapSession {
            token,
            replica,
            state_hash,
        })
    }
}

async fn encode_message<T>(value: &T) -> tc_error::TCResult<Vec<u8>>
where
    T: for<'en> en::ToStream<'en>,
{
    destream_json::encode(value)
        .map_err(|error| TCError::internal(format!("encode bootstrap message: {error}")))?
        .map_err(|error| TCError::internal(format!("encode bootstrap message: {error}")))
        .try_fold(Vec::new(), |mut bytes, chunk| async move {
            if bytes.len().saturating_add(chunk.len()) > MAX_BOOTSTRAP_MESSAGE_BYTES {
                return Err(TCError::bad_request("bootstrap message exceeds its bound"));
            }
            bytes.extend_from_slice(&chunk);
            Ok(bytes)
        })
        .await
}

async fn decode_message<T>(bytes: Vec<u8>, label: &str) -> tc_error::TCResult<T>
where
    T: de::FromStream<Context = ()>,
{
    if bytes.len() > MAX_BOOTSTRAP_MESSAGE_BYTES {
        return Err(TCError::bad_request(format!("{label} exceeds its bound")));
    }
    destream_json::try_decode(
        (),
        futures::stream::iter([Ok::<_, std::io::Error>(bytes.into())]),
    )
    .await
    .map_err(|error| TCError::bad_request(format!("invalid {label}: {error}")))
}

#[cfg(all(test, feature = "http-client"))]
#[path = "../../tests/support/replication_issuer.rs"]
mod tests;
