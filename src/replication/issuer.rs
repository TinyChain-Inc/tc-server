use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use aes_gcm_siv::{Aes256GcmSiv, Key, KeyInit};
use tc_error::TCError;
use tc_ir::Claim;
use tc_value::Value;
use umask::Mode;

use crate::auth::{Actor, KeyringActorResolver, PublicKeyStore, SignedToken, Token};

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

    pub fn issue_token(&self, path: &str) -> tc_error::TCResult<SignedToken> {
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
