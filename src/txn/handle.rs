use std::{
    fmt,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime},
};

use pathlink::Link;
use tc_error::TCError;
use tc_ir::{Claim, NetworkTime, Transaction, TxnHeader, TxnId};
use tc_value::Value;
use umask::Mode;

use crate::auth::{Actor, SignedToken, Token};
use crate::gateway::RpcGateway;

#[derive(Clone)]
pub struct TxnHandle {
    pub(super) id: TxnId,
    pub(super) claim: Claim,
    pub(super) claims: Vec<Claim>,
    pub(super) owner_id: Option<String>,
    pub(super) bearer_token: Option<String>,
    pub(super) resolver: Option<Arc<dyn RpcGateway>>,
    pub(super) ttl: Duration,
    pub(super) token: Option<Arc<SignedToken>>,
}

impl TxnHandle {
    pub fn with_resolver(&self, resolver: Arc<dyn RpcGateway>) -> Self {
        Self {
            id: self.id,
            claim: self.claim.clone(),
            claims: self.claims.clone(),
            owner_id: self.owner_id.clone(),
            bearer_token: self.bearer_token.clone(),
            resolver: Some(resolver),
            ttl: self.ttl,
            token: self.token.clone(),
        }
    }

    pub fn id(&self) -> TxnId {
        self.id
    }

    pub fn claim(&self) -> &Claim {
        &self.claim
    }

    pub fn claims(&self) -> &[Claim] {
        &self.claims
    }

    pub fn has_claim(&self, link: &Link, required: Mode) -> bool {
        self.claims.iter().any(|claim| claim.allows(link, required))
    }

    pub fn owner_id(&self) -> Option<&str> {
        self.owner_id.as_deref()
    }

    pub(crate) fn raw_token(&self) -> Option<&str> {
        self.bearer_token.as_deref()
    }

    pub(crate) fn authorization_header(&self) -> Option<String> {
        self.bearer_token
            .as_ref()
            .map(|token| format!("Bearer {token}"))
    }

    pub(crate) fn with_bearer_token(&self, bearer_token: String) -> Self {
        Self {
            id: self.id,
            claim: self.claim.clone(),
            claims: self.claims.clone(),
            owner_id: self.owner_id.clone(),
            bearer_token: Some(bearer_token),
            resolver: self.resolver.clone(),
            ttl: self.ttl,
            token: self.token.clone(),
        }
    }

    pub fn header(&self) -> TxnHeader {
        TxnHeader::new(self.id, self.id.timestamp(), self.claim.clone())
    }

    pub(crate) fn with_claims(&self, claims: Vec<Claim>) -> Self {
        Self {
            id: self.id,
            claim: self.claim.clone(),
            claims,
            owner_id: self.owner_id.clone(),
            bearer_token: self.bearer_token.clone(),
            resolver: self.resolver.clone(),
            ttl: self.ttl,
            token: self.token.clone(),
        }
    }

    pub fn with_signed_token(&self, token: SignedToken) -> tc_error::TCResult<Self> {
        let canonical_claim = self.validate_token(&token)?;
        let bearer_token = token.clone().into_jwt();
        let claims = token
            .claims()
            .iter()
            .map(|(_, _, claim)| claim.clone())
            .collect();
        Ok(Self {
            id: self.id,
            claim: canonical_claim,
            claims,
            owner_id: self.owner_id.clone(),
            bearer_token: Some(bearer_token),
            resolver: self.resolver.clone(),
            ttl: self.ttl,
            token: Some(Arc::new(token)),
        })
    }

    pub fn grant(
        &self,
        actor: &Actor,
        host: Link,
        resource: Link,
        mode: Mode,
    ) -> tc_error::TCResult<Self> {
        let now = SystemTime::now();
        let claim = Claim::new(resource, mode);

        let signed = match &self.token {
            Some(token) => actor
                .consume_and_sign((**token).clone(), host, claim, now)
                .map_err(|err| TCError::unauthorized(err.to_string()))?,
            None => {
                let token = Token::new(host, now, self.ttl, actor.id().clone(), claim);
                actor
                    .sign_token(token)
                    .map_err(|err| TCError::unauthorized(err.to_string()))?
            }
        };

        self.with_signed_token(signed)
    }

    pub fn has_permission(
        &self,
        actor: &Actor,
        resource: &Link,
        required: Mode,
    ) -> tc_error::TCResult<bool> {
        let Some(token) = &self.token else {
            return Ok(false);
        };

        for (_host, actor_id, claim) in token.claims() {
            if actor_id == actor.id() && claim.allows(resource, required) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn validate_token(&self, token: &SignedToken) -> tc_error::TCResult<Claim> {
        let txn_link = Link::from_str(&format!("/txn/{}", self.id))
            .map_err(|err| TCError::bad_request(err.to_string()))?;

        let mut owner: Option<(&Link, &Value)> = None;
        let mut lock: Option<(&Link, &Value)> = None;
        let mut canonical_claim: Option<Claim> = None;

        for (index, (host, actor_id, claim)) in token.claims().iter().enumerate() {
            let claim_link = claim.link.to_string();
            if claim_link.starts_with("/txn/") {
                if claim.link != txn_link {
                    return Err(TCError::bad_request(format!(
                        "cannot initialize txn {} with token for {}",
                        self.id, claim.link
                    )));
                }

                if index <= 1 {
                    if canonical_claim.is_none() {
                        canonical_claim = Some(claim.clone());
                    }
                } else {
                    return Err(TCError::bad_request(
                        "invalid token: canonical claim must be first or second",
                    ));
                }

                if claim.mask.has(umask::USER_EXEC) {
                    if owner.is_some() {
                        return Err(TCError::bad_request("invalid token: multiple txn owners"));
                    }
                    owner = Some((host, actor_id));
                }

                if claim.mask.has(umask::USER_WRITE) {
                    if lock.is_some() {
                        return Err(TCError::bad_request("invalid token: multiple txn locks"));
                    }
                    lock = Some((host, actor_id));
                }
            }
        }

        let canonical_claim = canonical_claim
            .ok_or_else(|| TCError::bad_request("invalid token: missing canonical txn claim"))?;

        if let Some(lock) = lock {
            let owner = owner
                .ok_or_else(|| TCError::bad_request("invalid token: txn lock without owner"))?;

            if lock != owner {
                return Err(TCError::bad_request(
                    "invalid token: txn lock does not match owner",
                ));
            }
        }

        Ok(canonical_claim)
    }
}

impl Transaction for TxnHandle {
    fn id(&self) -> TxnId {
        self.id
    }

    fn timestamp(&self) -> NetworkTime {
        self.id.timestamp()
    }

    fn claim(&self) -> &Claim {
        &self.claim
    }
}

impl fmt::Debug for TxnHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxnHandle")
            .field("id", &self.id)
            .field("claim", &self.claim)
            .field("owner_id", &self.owner_id)
            .finish()
    }
}
