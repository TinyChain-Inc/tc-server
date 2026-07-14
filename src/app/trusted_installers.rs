use std::collections::HashMap;
use std::fs;
use std::str::FromStr;

use base64::Engine as _;
use futures::FutureExt;
use pathlink::Link;
use serde::Deserialize;
use tc_error::{TCError, TCResult};
use tinychain::auth::{
    KeyringActorResolver, PublicKeyStore, RjwtTokenVerifier, TokenContext, TokenVerifier,
};
use tinychain::replication::{
    PeerMembership, is_supported_replicated_path, normalize_replicated_prefix,
};

use super::config::Config;

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct TrustedInstaller {
    pub(crate) host: String,
    pub(crate) actor_id: String,
    pub(crate) public_key_b64: String,
    #[serde(default)]
    #[serde(alias = "allowed_prefixes")]
    pub(crate) allowed_lib_prefixes: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct TrustedInstallerPolicy {
    by_actor: HashMap<(String, String), Vec<String>>,
    replication_root: String,
}

impl TrustedInstallerPolicy {
    pub(crate) fn from_installers(
        installers: &[TrustedInstaller],
        cluster_root: &str,
    ) -> TCResult<Self> {
        let mut policy = Self {
            by_actor: HashMap::new(),
            replication_root: normalize_replicated_prefix(cluster_root)?,
        };

        for installer in installers {
            let host = Link::from_str(&installer.host).map_err(|err| {
                TCError::bad_request(format!("invalid trusted installer host: {err}"))
            })?;
            let actor_id = installer.actor_id.trim();
            if actor_id.is_empty() {
                return Err(TCError::bad_request(
                    "trusted installer actor_id must not be empty",
                ));
            }

            let mut prefixes = Vec::new();
            for prefix in &installer.allowed_lib_prefixes {
                prefixes.push(normalize_replicated_prefix(prefix)?);
            }

            if prefixes.is_empty() {
                return Err(TCError::bad_request(format!(
                    "trusted installer {actor_id} must define at least one allowed_lib_prefix"
                )));
            }

            policy
                .by_actor
                .insert((host.to_string(), actor_id.to_string()), prefixes);
        }

        Ok(policy)
    }

    pub(crate) fn replication_root(&self) -> &str {
        &self.replication_root
    }

    fn validate_external_context(
        &self,
        ctx: &TokenContext,
    ) -> Result<(), tinychain::txn::TxnError> {
        for (host, actor_id, claim) in &ctx.claims {
            let path = claim.link.to_string();

            if path.starts_with("/txn/") {
                continue;
            }

            if host == "/host" {
                continue;
            }

            let Some(prefixes) = self.by_actor.get(&(host.clone(), actor_id.clone())) else {
                return Err(tinychain::txn::TxnError::Unauthorized);
            };

            if !is_supported_replicated_path(&path) {
                return Err(tinychain::txn::TxnError::Unauthorized);
            }

            if !prefixes
                .iter()
                .any(|prefix| path_matches_prefix(&path, prefix))
            {
                return Err(tinychain::txn::TxnError::Unauthorized);
            }
        }

        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct TrustedInstallerTokenVerifier {
    inner: RjwtTokenVerifier,
    policy: TrustedInstallerPolicy,
    replication_membership: PeerMembership,
    local_replication_actor_id: String,
}

impl TrustedInstallerTokenVerifier {
    pub(crate) fn new(
        inner: RjwtTokenVerifier,
        policy: TrustedInstallerPolicy,
        replication_membership: PeerMembership,
        local_replication_actor_id: String,
    ) -> Self {
        Self {
            inner,
            policy,
            replication_membership,
            local_replication_actor_id,
        }
    }
}

impl TokenVerifier for TrustedInstallerTokenVerifier {
    fn verify(
        &self,
        bearer_token: String,
    ) -> futures::future::BoxFuture<'static, Result<TokenContext, tinychain::txn::TxnError>> {
        let inner = self.inner.clone();
        let policy = self.policy.clone();
        let replication_membership = self.replication_membership.clone();
        let local_replication_actor_id = self.local_replication_actor_id.clone();
        async move {
            let ctx = inner.verify(bearer_token).await?;
            policy.validate_external_context(&ctx)?;

            for (host, actor_id, claim) in &ctx.claims {
                let path = claim.link.to_string();
                if path.starts_with("/txn/") {
                    continue;
                }

                if host == "/host" {
                    let from_known_replica = actor_id == &local_replication_actor_id
                        || replication_membership
                            .peer_descriptors()
                            .iter()
                            .any(|peer| peer.actor_id.as_deref() == Some(actor_id.as_str()));

                    if !from_known_replica {
                        return Err(tinychain::txn::TxnError::Unauthorized);
                    }

                    if path != tinychain::uri::HOST_LIBRARY_EXPORT
                        && !path_matches_prefix(&path, policy.replication_root())
                    {
                        return Err(tinychain::txn::TxnError::Unauthorized);
                    }
                }
            }

            Ok(ctx)
        }
        .boxed()
    }

    fn grant(
        &self,
        token: TokenContext,
        claim: tc_ir::Claim,
    ) -> futures::future::BoxFuture<'static, Result<TokenContext, tinychain::txn::TxnError>> {
        self.inner.grant(token, claim)
    }
}

pub(crate) fn load_trusted_installers(config: &Config) -> TCResult<Vec<TrustedInstaller>> {
    let raw = match (
        config.trusted_installers_json.as_ref(),
        config.trusted_installers_json_path.as_ref(),
    ) {
        (Some(json), None) => Some(json.clone()),
        (None, Some(path)) => Some(fs::read_to_string(path).map_err(|err| {
            TCError::bad_request(format!("failed to read trusted installers file: {err}"))
        })?),
        (None, None) => None,
        (Some(_), Some(_)) => {
            return Err(TCError::bad_request(
                "set only one of TC_TRUSTED_INSTALLERS_JSON or TC_TRUSTED_INSTALLERS_JSON_PATH",
            ));
        }
    };

    let Some(raw) = raw else {
        return Ok(Vec::new());
    };

    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    serde_json::from_str(trimmed).map_err(|err| {
        TCError::bad_request(format!(
            "invalid trusted installers JSON (expected array of installer entries): {err}"
        ))
    })
}

pub(crate) fn bootstrap_trusted_installers(
    mut keyring: KeyringActorResolver,
    public_keys: &PublicKeyStore,
    installers: &[TrustedInstaller],
) -> TCResult<KeyringActorResolver> {
    for installer in installers {
        let host = Link::from_str(&installer.host).map_err(|err| {
            TCError::bad_request(format!("invalid trusted installer host: {err}"))
        })?;

        let actor_id = installer.actor_id.trim();
        if actor_id.is_empty() {
            return Err(TCError::bad_request(
                "trusted installer actor_id must not be empty",
            ));
        }

        let key_bytes = base64::engine::general_purpose::STANDARD
            .decode(installer.public_key_b64.trim())
            .map_err(|err| {
                TCError::bad_request(format!("invalid installer public_key_b64: {err}"))
            })?;

        let verifying_key = tinychain::auth::verifying_key_from_bytes(key_bytes.as_slice())
            .map_err(|err| {
                TCError::bad_request(format!("invalid installer public key bytes: {err}"))
            })?;

        let actor =
            tinychain::auth::Actor::with_verifying_key(actor_id.to_string(), verifying_key.clone());

        keyring = keyring.with_actor(host, actor);
        public_keys.insert(actor_id.to_string(), verifying_key);
    }

    Ok(keyring)
}

fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    if path == prefix {
        return true;
    }

    path.strip_prefix(prefix)
        .is_some_and(|rest| rest.starts_with('/'))
}
