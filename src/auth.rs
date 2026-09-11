use pathlink::Link;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Claim {
    pub link: Link,
    pub mask: umask::Mode,
}

impl Claim {
    pub fn new(link: Link, mask: umask::Mode) -> Self {
        Self { link, mask }
    }

    pub fn allows(&self, link: &Link, required: umask::Mode) -> bool {
        self.link == *link && u32::from(self.mask) & u32::from(required) == u32::from(required)
    }
}

use futures::future::{BoxFuture, FutureExt};

pub fn bearer_token(header: &str) -> Option<&str> {
    let (scheme, token) = header.split_once(' ')?;
    let token = token.trim();
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty()).then_some(token)
}

/// A kernel-owned verifier which maps an `Authorization: Bearer ...` token to a stable owner
/// identity used to pin transaction ownership.
///
/// The trait keeps transaction semantics in the kernel so protocol adapters can remain thin.
pub trait TokenVerifier: Send + Sync + 'static {
    fn verify(&self, bearer_token: String) -> BoxFuture<'static, tc_error::TCResult<AuthContext>>;

    fn grant(
        &self,
        _token: AuthContext,
        _claim: Claim,
    ) -> BoxFuture<'static, tc_error::TCResult<AuthContext>> {
        futures::future::ready(Err(tc_error::TCError::unauthorized(
            "claim escalation is not supported",
        )))
        .boxed()
    }
}

#[derive(Clone, Debug)]
pub struct AuthClaimContext {
    pub host: String,
    pub actor_id: String,
    pub claim: Claim,
}

#[derive(Clone, Debug)]
pub struct AuthContext {
    pub principal: String,
    pub claims: Vec<AuthClaimContext>,
    pub verified_at_nanos: u64,
    pub(crate) signed: Option<Arc<SignedToken>>,
}

impl AuthContext {
    pub fn new(principal: impl Into<String>) -> Self {
        let verified_at_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        Self {
            principal: principal.into(),
            claims: Vec::new(),
            verified_at_nanos,
            signed: None,
        }
    }

    pub fn with_claim(mut self, host: String, actor_id: String, claim: Claim) -> Self {
        self.claims.push(AuthClaimContext {
            host,
            actor_id,
            claim,
        });
        self
    }

    pub fn token_hosts(&self) -> Vec<String> {
        self.claims
            .iter()
            .map(|claim| claim.host.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

mod rjwt_token {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::FutureExt;
    use parking_lot::RwLock;
    use pathlink::{Link, PathBuf};
    use rjwt::{
        Actor as RjwtActor, Error as RjwtError, Resolve, SignedToken as RjwtSignedToken,
        Token as RjwtToken, VerifyingKey,
    };

    use crate::auth::{AuthContext, Claim, TokenVerifier};

    pub type WireClaims = BTreeMap<PathBuf, u32>;

    pub fn claims_from_wire(claims: WireClaims) -> Vec<Claim> {
        claims
            .into_iter()
            .map(|(path, mask)| Claim::new(Link::from(path), mask.into()))
            .collect()
    }

    pub fn wire_claim(claim: Claim) -> WireClaims {
        BTreeMap::from([(claim.link.path().clone(), claim.mask.into())])
    }

    pub type SignedToken = RjwtSignedToken<Link, String, WireClaims>;
    pub type Token = RjwtToken<Link, String, WireClaims>;
    pub type Actor = RjwtActor<String>;

    #[derive(Default)]
    struct ActorDirectory {
        actors: HashMap<(Link, String), Actor>,
        keys: HashMap<String, VerifyingKey>,
    }

    #[derive(Clone, Default)]
    pub struct KeyringActorResolver(Arc<RwLock<ActorDirectory>>);

    impl KeyringActorResolver {
        pub fn insert(&self, host: Link, actor: Actor) -> tc_error::TCResult<()> {
            let actor_id = actor.id().clone();
            let key = actor.verifying_key();
            let mut directory = self.0.write();
            if directory
                .keys
                .get(&actor_id)
                .is_some_and(|known| known.to_bytes() != key.to_bytes())
            {
                return Err(tc_error::TCError::conflict(format!(
                    "actor {actor_id} has conflicting public keys"
                )));
            }
            directory.keys.insert(actor_id.clone(), key);
            directory.actors.insert((host, actor_id), actor);
            Ok(())
        }

        pub fn public_key(&self, actor_id: &str) -> Option<VerifyingKey> {
            self.0.read().keys.get(actor_id).cloned()
        }
    }

    pub fn verifying_key_from_bytes(
        algorithm: rjwt::AlgKind,
        bytes: &[u8],
    ) -> Result<VerifyingKey, RjwtError> {
        VerifyingKey::from_bytes(algorithm, bytes)
    }

    #[async_trait]
    pub trait ActorResolver: Send + Sync + 'static {
        async fn resolve_actor(&self, host: &Link, actor_id: &str) -> tc_error::TCResult<Actor>;
    }

    #[async_trait]
    impl ActorResolver for KeyringActorResolver {
        async fn resolve_actor(&self, host: &Link, actor_id: &str) -> tc_error::TCResult<Actor> {
            self.0
                .read()
                .actors
                .get(&(host.clone(), actor_id.to_string()))
                .cloned()
                .ok_or_else(|| tc_error::TCError::unauthorized("unknown token actor"))
        }
    }

    #[derive(Clone)]
    pub struct RjwtTokenVerifier {
        resolver: Arc<dyn ActorResolver>,
    }

    impl RjwtTokenVerifier {
        pub fn new(resolver: Arc<dyn ActorResolver>) -> Self {
            Self { resolver }
        }
    }

    impl Resolve for RjwtTokenVerifier {
        type HostId = Link;
        type ActorId = String;
        type Claims = WireClaims;

        fn resolve(
            &self,
            host: &Self::HostId,
            actor_id: &Self::ActorId,
        ) -> impl std::future::Future<Output = Result<RjwtActor<Self::ActorId>, RjwtError>> + Send
        {
            let resolver = self.resolver.clone();
            let host = host.clone();
            let actor_id = actor_id.clone();
            async move {
                resolver
                    .resolve_actor(&host, actor_id.as_str())
                    .await
                    .map_err(RjwtError::fetch)
            }
        }
    }

    impl TokenVerifier for RjwtTokenVerifier {
        fn verify(
            &self,
            bearer_token: String,
        ) -> futures::future::BoxFuture<'static, tc_error::TCResult<AuthContext>> {
            let this = self.clone();
            async move {
                let signed =
                    Resolve::verify(&this, bearer_token.clone(), std::time::SystemTime::now())
                        .await
                        .map_err(|_| tc_error::TCError::unauthorized("invalid bearer token"))?;

                let mut claims = signed.claims().iter();
                let (owner_host, owner_actor_id, owner_claims) = claims
                    .next()
                    .ok_or_else(|| tc_error::TCError::unauthorized("bearer token has no owner"))?;

                let owner_id = format!("{owner_host}::{}", owner_actor_id.clone());
                let mut ctx = AuthContext::new(owner_id);
                ctx.signed = Some(Arc::new(signed.clone()));

                for claim in claims_from_wire(owner_claims.clone()) {
                    ctx = ctx.with_claim(owner_host.to_string(), owner_actor_id.clone(), claim);
                }

                for (host, actor_id, claims) in claims {
                    for claim in claims_from_wire(claims.clone()) {
                        ctx = ctx.with_claim(host.to_string(), actor_id.clone(), claim);
                    }
                }

                Ok(ctx)
            }
            .boxed()
        }

        fn grant(
            &self,
            token: AuthContext,
            claim: Claim,
        ) -> futures::future::BoxFuture<'static, tc_error::TCResult<AuthContext>> {
            let required_link = claim.link.clone();
            let required_mode = claim.mask;
            let allowed = token
                .claims
                .iter()
                .any(|existing| existing.claim.allows(&required_link, required_mode));

            futures::future::ready(if allowed {
                Ok(token)
            } else {
                Err(tc_error::TCError::unauthorized("claim is not authorized"))
            })
            .boxed()
        }
    }
}

pub use rjwt_token::{
    Actor, ActorResolver as RjwtActorResolver, KeyringActorResolver, RjwtTokenVerifier,
    SignedToken, Token, claims_from_wire, verifying_key_from_bytes, wire_claim,
};
