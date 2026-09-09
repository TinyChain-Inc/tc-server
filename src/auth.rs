use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tc_ir::Claim;

use crate::txn::TxnError;
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
    fn verify(&self, bearer_token: String) -> BoxFuture<'static, Result<TokenContext, TxnError>>;

    fn grant(
        &self,
        _token: TokenContext,
        _claim: Claim,
    ) -> BoxFuture<'static, Result<TokenContext, TxnError>> {
        futures::future::ready(Err(TxnError::Unauthorized)).boxed()
    }
}

#[derive(Clone, Debug)]
pub struct TokenContext {
    pub owner_id: String,
    pub bearer_token: String,
    pub claims: Vec<(String, String, Claim)>,
    pub verified_at_nanos: u64,
    pub(crate) signed: Option<Arc<SignedToken>>,
}

impl TokenContext {
    pub fn new(owner_id: impl Into<String>, bearer_token: impl Into<String>) -> Self {
        let verified_at_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        Self {
            owner_id: owner_id.into(),
            bearer_token: bearer_token.into(),
            claims: Vec::new(),
            verified_at_nanos,
            signed: None,
        }
    }

    pub fn with_claim(mut self, host: String, actor_id: String, claim: Claim) -> Self {
        self.claims.push((host, actor_id, claim));
        self
    }
}

impl<T> TokenVerifier for Arc<T>
where
    T: TokenVerifier + ?Sized,
{
    fn verify(&self, bearer_token: String) -> BoxFuture<'static, Result<TokenContext, TxnError>> {
        (**self).verify(bearer_token)
    }

    fn grant(
        &self,
        token: TokenContext,
        claim: Claim,
    ) -> BoxFuture<'static, Result<TokenContext, TxnError>> {
        (**self).grant(token, claim)
    }
}

mod rjwt_token {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::FutureExt;
    use parking_lot::RwLock;
    use pathlink::{Link, PathBuf};
    use rjwt::{Actor, AlgKind, Error as RjwtError, Resolve, SignedToken, Token, VerifyingKey};

    use crate::auth::{TokenContext, TokenVerifier};
    use crate::txn::TxnError;
    use tc_ir::Claim;

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

    pub type SignedTokenV1 = SignedToken<Link, String, WireClaims>;
    pub type TokenV1 = Token<Link, String, WireClaims>;
    pub type ActorV1 = Actor<String>;

    #[derive(Clone, Default)]
    pub struct KeyringActorResolver {
        actors: Arc<RwLock<HashMap<(Link, String), ActorV1>>>,
    }

    impl KeyringActorResolver {
        pub fn with_actor(self, host: Link, actor: ActorV1) -> Self {
            self.actors
                .write()
                .insert((host, actor.id().clone()), actor);
            self
        }
    }

    #[derive(Clone, Default)]
    pub struct PublicKeyStore {
        keys: Arc<RwLock<HashMap<String, VerifyingKey>>>,
    }

    impl PublicKeyStore {
        pub fn insert(&self, actor_id: impl Into<String>, key: VerifyingKey) {
            self.keys.write().insert(actor_id.into(), key);
        }

        pub fn insert_actor(&self, actor: &ActorV1) {
            self.insert(actor.id().clone(), actor.verifying_key());
        }

        pub fn public_key(&self, actor_id: &str) -> Option<VerifyingKey> {
            self.keys.read().get(actor_id).cloned()
        }
    }

    pub fn verifying_key_from_bytes(bytes: &[u8]) -> Result<VerifyingKey, RjwtError> {
        VerifyingKey::from_bytes(AlgKind::Falcon512, bytes)
            .or_else(|_| VerifyingKey::from_bytes(AlgKind::Ed25519, bytes))
    }

    #[async_trait]
    pub trait ActorResolver: Send + Sync + 'static {
        async fn resolve_actor(&self, host: &Link, actor_id: &str) -> Result<ActorV1, TxnError>;
    }

    #[async_trait]
    impl ActorResolver for KeyringActorResolver {
        async fn resolve_actor(&self, host: &Link, actor_id: &str) -> Result<ActorV1, TxnError> {
            self.actors
                .read()
                .get(&(host.clone(), actor_id.to_string()))
                .cloned()
                .ok_or(TxnError::Unauthorized)
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
        ) -> impl std::future::Future<Output = Result<Actor<Self::ActorId>, RjwtError>> + Send
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
        ) -> futures::future::BoxFuture<'static, Result<TokenContext, TxnError>> {
            let this = self.clone();
            async move {
                let signed =
                    Resolve::verify(&this, bearer_token.clone(), std::time::SystemTime::now())
                        .await
                        .map_err(|_| TxnError::Unauthorized)?;

                let mut claims = signed.claims().iter();
                let (owner_host, owner_actor_id, owner_claims) =
                    claims.next().ok_or(TxnError::Unauthorized)?;

                let owner_id = format!("{owner_host}::{}", owner_actor_id.clone());
                let mut ctx = TokenContext::new(owner_id, bearer_token);
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
            token: TokenContext,
            claim: Claim,
        ) -> futures::future::BoxFuture<'static, Result<TokenContext, TxnError>> {
            let required_link = claim.link.clone();
            let required_mode = claim.mask;
            let allowed = token
                .claims
                .iter()
                .any(|(_, _, existing)| existing.allows(&required_link, required_mode));

            futures::future::ready(if allowed {
                Ok(token)
            } else {
                Err(TxnError::Unauthorized)
            })
            .boxed()
        }
    }
}

pub use rjwt_token::{
    ActorResolver as RjwtActorResolver, ActorV1 as Actor, KeyringActorResolver, PublicKeyStore,
    RjwtTokenVerifier, SignedTokenV1 as SignedToken, TokenV1 as Token, claims_from_wire,
    verifying_key_from_bytes, wire_claim,
};
