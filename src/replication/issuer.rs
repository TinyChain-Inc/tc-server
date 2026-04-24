use std::str::FromStr;

use aes_gcm_siv::{Aes256GcmSiv, Key, KeyInit};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use tc_error::TCError;
use tc_ir::Claim;
use tc_value::Value;
use umask::USER_READ;

use crate::auth::{Actor, KeyringActorResolver, PublicKeyStore, SignedToken, Token};

use super::PeerIdentity;
use super::REPLICATION_TTL;
use super::crypto::decrypt_path;

pub fn parse_psk_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| item.to_string())
        .collect()
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
    host: pathlink::Link,
    keys: Vec<Key<Aes256GcmSiv>>,
    signer: Actor,
    keyring: KeyringActorResolver,
    public_keys: PublicKeyStore,
}

impl ReplicationIssuer {
    pub fn new(
        host: pathlink::Link,
        keys: Vec<Key<Aes256GcmSiv>>,
        signer: Actor,
        keyring: KeyringActorResolver,
        public_keys: PublicKeyStore,
    ) -> Self {
        let signer_public = signer.public_key();
        let signer_id = signer.id().clone();
        public_keys.insert_actor(&signer);
        let keyring = keyring.with_actor(
            host.clone(),
            Actor::with_public_key(signer_id, signer_public),
        );

        Self {
            host,
            keys,
            signer,
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

    pub fn self_identity(&self, peer: String) -> tc_error::TCResult<PeerIdentity> {
        let actor_id = match self.signer.id() {
            Value::String(id) => id.clone(),
            other => {
                return Err(TCError::internal(format!(
                    "replication signer actor_id must be a string, got {other:?}"
                )));
            }
        };

        let public_key_b64 = BASE64.encode(self.signer.public_key().to_bytes());
        Ok(PeerIdentity {
            peer,
            actor_id,
            public_key_b64,
        })
    }

    pub fn register_peer_identity(&self, identity: &PeerIdentity) -> tc_error::TCResult<()> {
        let actor_id = identity.actor_id.trim();
        if actor_id.is_empty() {
            return Err(TCError::bad_request("peer actor_id must not be empty"));
        }

        let key_bytes = BASE64
            .decode(identity.public_key_b64.trim())
            .map_err(|err| TCError::bad_request(format!("invalid peer public_key_b64: {err}")))?;
        let verifying_key = rjwt::VerifyingKey::try_from(key_bytes.as_slice())
            .map_err(|err| TCError::bad_request(format!("invalid peer public key bytes: {err}")))?;

        let actor = Actor::with_public_key(Value::String(actor_id.to_string()), verifying_key);
        self.public_keys.insert_actor(&actor);
        let _ = self.keyring.clone().with_actor(self.host.clone(), actor);
        Ok(())
    }

    pub fn issue_token(&self, path: &str) -> tc_error::TCResult<SignedToken> {
        let claim = Claim::new(
            pathlink::Link::from_str(path)
                .map_err(|err| TCError::bad_request(format!("invalid claim path: {err}")))?,
            USER_READ,
        );

        let token = Token::new(
            self.host.clone(),
            std::time::SystemTime::now(),
            REPLICATION_TTL,
            self.signer.id().clone(),
            claim,
        );

        self.signer
            .sign_token(token)
            .map_err(|err| TCError::internal(err.to_string()))
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
}
