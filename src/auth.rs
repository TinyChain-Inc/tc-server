use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tc_ir::Claim;

use crate::txn::TxnError;
use futures::future::{BoxFuture, FutureExt};

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
    use std::str::FromStr;
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::FutureExt;
    use parking_lot::RwLock;
    use pathlink::Link;
    use rjwt::{Actor, AlgKind, Error as RjwtError, Resolve, SignedToken, Token, VerifyingKey};
    use serde::{Deserialize, Serialize};
    use tc_value::Value;

    use crate::auth::{TokenContext, TokenVerifier};
    use crate::txn::TxnError;
    use tc_ir::Claim;

    pub type SignedTokenV1 = SignedToken<Link, String, Claim>;
    pub type TokenV1 = Token<Link, String, Claim>;
    pub type ActorV1 = Actor<String>;

    #[derive(Clone, Debug, Deserialize, Serialize)]
    #[serde(untagged)]
    pub enum RjwtClaims {
        Legacy(Claim),
        Paths(BTreeMap<String, u32>),
    }

    impl RjwtClaims {
        fn flatten(&self) -> Result<Vec<Claim>, TxnError> {
            match self {
                Self::Legacy(claim) => Ok(vec![claim.clone()]),
                Self::Paths(paths) => paths
                    .iter()
                    .map(|(path, mode)| {
                        let link = Link::from_str(path).map_err(|_| TxnError::Unauthorized)?;
                        Ok(Claim::new(link, (*mode).into()))
                    })
                    .collect(),
            }
        }
    }

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

    #[cfg(feature = "http-client")]
    #[derive(Clone)]
    pub struct RpcActorResolver {
        gateway: Arc<dyn crate::gateway::RpcGateway>,
        txn: crate::txn::TxnHandle,
    }

    #[cfg(feature = "http-client")]
    impl RpcActorResolver {
        pub fn new(
            gateway: Arc<dyn crate::gateway::RpcGateway>,
            txn: crate::txn::TxnHandle,
        ) -> Self {
            Self { gateway, txn }
        }
    }

    #[cfg(feature = "http-client")]
    #[async_trait]
    impl ActorResolver for RpcActorResolver {
        async fn resolve_actor(&self, host: &Link, actor_id: &str) -> Result<ActorV1, TxnError> {
            use base64::Engine as _;

            let target = {
                let host_segment =
                    pathlink::PathSegment::from_str("host").map_err(|_| TxnError::Unauthorized)?;
                let key_segment = pathlink::PathSegment::from_str("public_key")
                    .map_err(|_| TxnError::Unauthorized)?;
                host.clone().append(host_segment).append(key_segment)
            };

            let response = self
                .gateway
                .get(
                    target,
                    // Public-key discovery bootstraps bearer verification. Do not attach the
                    // unverified bearer token to the lookup request; application dependency calls
                    // still propagate verified transaction tokens through the kernel gateway.
                    self.txn.without_bearer_token(),
                    Value::String(actor_id.to_string()),
                )
                .await
                .map_err(|_| TxnError::Unauthorized)?;

            let encoded = match response {
                tc_state::State::Scalar(tc_ir::Scalar::Value(Value::String(s))) => s,
                _ => return Err(TxnError::Unauthorized),
            };

            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| TxnError::Unauthorized)?;

            let verifying_key =
                verifying_key_from_bytes(bytes.as_slice()).map_err(|_| TxnError::Unauthorized)?;

            Ok(Actor::with_verifying_key(
                actor_id.to_string(),
                verifying_key,
            ))
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
        type Claims = RjwtClaims;

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

                for claim in owner_claims.flatten()? {
                    ctx = ctx.with_claim(owner_host.to_string(), owner_actor_id.clone(), claim);
                }

                for (host, actor_id, claims) in claims {
                    for claim in claims.flatten()? {
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
    RjwtTokenVerifier, SignedTokenV1 as SignedToken, TokenV1 as Token, verifying_key_from_bytes,
};

#[cfg(feature = "http-client")]
pub use rjwt_token::RpcActorResolver;
