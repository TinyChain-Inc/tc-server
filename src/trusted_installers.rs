use std::collections::HashMap;
use std::io::Read;
use std::str::FromStr;

use base64::Engine as _;
use futures::FutureExt;
use pathlink::Link;
use serde::Deserialize;
use tc_error::{TCError, TCResult};
use tinychain::auth::{AuthContext, KeyringActorResolver, RjwtTokenVerifier, TokenVerifier};
use tinychain::replication::normalize_replicated_prefix;

use super::config::Config;

const MAX_TRUSTED_INSTALLERS_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct TrustedInstaller {
    pub(crate) host: String,
    pub(crate) actor_id: String,
    pub(crate) algorithm: rjwt::AlgKind,
    pub(crate) public_key_b64: String,
    pub(crate) allowed_prefixes: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct TrustedInstallerPolicy {
    by_actor: HashMap<(String, String), Vec<String>>,
}

impl TrustedInstallerPolicy {
    pub(crate) fn from_installers(installers: &[TrustedInstaller]) -> TCResult<Self> {
        let mut policy = Self {
            by_actor: HashMap::new(),
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
            for prefix in &installer.allowed_prefixes {
                prefixes.push(normalize_replicated_prefix(prefix)?);
            }

            if prefixes.is_empty() {
                return Err(TCError::bad_request(format!(
                    "trusted installer {actor_id} must define at least one allowed_prefix"
                )));
            }

            policy
                .by_actor
                .insert((host.to_string(), actor_id.to_string()), prefixes);
        }

        Ok(policy)
    }

    fn validate_external_context(&self, ctx: &AuthContext) -> TCResult<()> {
        for claim in &ctx.claims {
            let path = claim.claim.link.to_string();

            if path.starts_with(tinychain::uri::HOST_TXN_PREFIX) {
                continue;
            }

            if claim.host == "/host" {
                continue;
            }

            let Some(prefixes) = self
                .by_actor
                .get(&(claim.host.clone(), claim.actor_id.clone()))
            else {
                return Err(TCError::unauthorized("untrusted installer actor"));
            };

            if normalize_replicated_prefix(&path).is_err() {
                return Err(TCError::unauthorized("invalid installer claim"));
            }

            if !prefixes
                .iter()
                .any(|prefix| path_matches_prefix(&path, prefix))
            {
                return Err(TCError::unauthorized(
                    "installer claim is outside its policy",
                ));
            }
        }

        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct TrustedInstallerTokenVerifier {
    inner: RjwtTokenVerifier,
    policy: TrustedInstallerPolicy,
}

impl TrustedInstallerTokenVerifier {
    pub(crate) fn new(inner: RjwtTokenVerifier, policy: TrustedInstallerPolicy) -> Self {
        Self { inner, policy }
    }
}

impl TokenVerifier for TrustedInstallerTokenVerifier {
    fn verify(
        &self,
        bearer_token: String,
    ) -> futures::future::BoxFuture<'static, TCResult<AuthContext>> {
        let inner = self.inner.clone();
        let policy = self.policy.clone();
        async move {
            let ctx = inner.verify(bearer_token).await?;
            policy.validate_external_context(&ctx)?;
            Ok(ctx)
        }
        .boxed()
    }

    fn grant(
        &self,
        token: AuthContext,
        claim: tinychain::Claim,
    ) -> futures::future::BoxFuture<'static, TCResult<AuthContext>> {
        self.inner.grant(token, claim)
    }
}

pub(crate) fn load_trusted_installers(config: &Config) -> TCResult<Vec<TrustedInstaller>> {
    let raw = match (
        config.trusted_installers_json.as_ref(),
        config.trusted_installers_json_path.as_ref(),
    ) {
        (Some(json), None) => Some(json.clone()),
        (None, Some(path)) => Some(read_trusted_installers(path)?),
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

fn read_trusted_installers(path: &std::path::Path) -> TCResult<String> {
    // DIRECT_FS_BOOTSTRAP: this bounded, one-shot configuration read happens
    // before HostStorage publishes any freqfs cache and is outside its roots.
    let file = std::fs::File::open(path).map_err(|err| {
        TCError::bad_request(format!("failed to open trusted installers file: {err}"))
    })?;
    let mut raw = String::new();
    file.take(MAX_TRUSTED_INSTALLERS_BYTES + 1)
        .read_to_string(&mut raw)
        .map_err(|err| {
            TCError::bad_request(format!("failed to read trusted installers file: {err}"))
        })?;
    if raw.len() as u64 > MAX_TRUSTED_INSTALLERS_BYTES {
        return Err(TCError::bad_request(
            "trusted installers file exceeds its 1 MiB limit",
        ));
    }
    Ok(raw)
}

pub(crate) fn bootstrap_trusted_installers(
    keyring: KeyringActorResolver,
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

        let verifying_key =
            tinychain::auth::verifying_key_from_bytes(installer.algorithm, key_bytes.as_slice())
                .map_err(|err| {
                    TCError::bad_request(format!("invalid installer public key bytes: {err}"))
                })?;

        let actor =
            tinychain::auth::Actor::with_verifying_key(actor_id.to_string(), verifying_key.clone());

        keyring.insert(host, actor)?;
    }

    Ok(keyring)
}

fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}
