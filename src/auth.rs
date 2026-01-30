use std::sync::Arc;

use tc_ir::Claim;

use crate::txn::TxnError;
use futures::future::{BoxFuture, FutureExt};

/// A kernel-owned verifier which maps an `Authorization: Bearer ...` token to a stable owner
/// identity used to pin transaction ownership.
///
/// Today this verifier treats bearer tokens as opaque, so the owner identity is the token itself.
/// The trait exists so the kernel can evolve to verify and decode signed tokens without pushing
/// transaction semantics into protocol adapters.
pub trait TokenVerifier: Send + Sync + 'static {
    fn verify(&self, bearer_token: String) -> BoxFuture<'static, Result<TokenContext, TxnError>>;

    fn grant(
        &self,
        token: TokenContext,
        _claim: Claim,
    ) -> BoxFuture<'static, Result<TokenContext, TxnError>> {
        futures::future::ready(Ok(token)).boxed()
    }
}

#[derive(Clone, Debug)]
pub struct TokenContext {
    pub owner_id: String,
    pub bearer_token: String,
    pub claims: Vec<(String, String, Claim)>,
}

impl TokenContext {
    pub fn new(owner_id: impl Into<String>, bearer_token: impl Into<String>) -> Self {
        Self {
            owner_id: owner_id.into(),
            bearer_token: bearer_token.into(),
            claims: Vec::new(),
        }
    }

    pub fn with_claim(mut self, host: String, actor_id: String, claim: Claim) -> Self {
        self.claims.push((host, actor_id, claim));
        self
    }
}

#[derive(Clone, Default)]
pub struct OpaqueBearerTokenVerifier;

impl TokenVerifier for OpaqueBearerTokenVerifier {
    fn verify(&self, bearer_token: String) -> BoxFuture<'static, Result<TokenContext, TxnError>> {
        futures::future::ready({
            let token = bearer_token.trim();
            if token.is_empty() {
                Err(TxnError::Unauthorized)
            } else {
                Ok(TokenContext::new(token, token))
            }
        })
        .boxed()
    }
}

pub(crate) fn default_token_verifier() -> Arc<dyn TokenVerifier> {
    Arc::new(OpaqueBearerTokenVerifier)
}

#[cfg(feature = "rjwt-token")]
mod rjwt_token {
    use std::collections::HashMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::FutureExt;
    use parking_lot::RwLock;
    use pathlink::Link;
    use rjwt::{Actor, Error as RjwtError, Resolve, VerifyingKey};
    use tc_value::Value;

    use crate::auth::{TokenContext, TokenVerifier};
    use crate::txn::TxnError;
    use tc_ir::Claim;

    #[derive(Clone, Default)]
    pub struct KeyringActorResolver {
        actors: Arc<RwLock<HashMap<(Link, String), Actor<Value>>>>,
    }

    impl KeyringActorResolver {
        pub fn with_actor(self, host: Link, actor: Actor<Value>) -> Self {
            self.actors
                .write()
                .insert((host, actor_identity(actor.id())), actor);
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

        pub fn insert_actor(&self, actor: &Actor<Value>) {
            self.insert(actor_identity(actor.id()), actor.public_key());
        }

        pub fn public_key(&self, actor_id: &str) -> Option<VerifyingKey> {
            self.keys.read().get(actor_id).copied()
        }
    }

    #[async_trait]
    pub trait ActorResolver: Send + Sync + 'static {
        async fn resolve_actor(
            &self,
            host: &Link,
            actor_id: &Value,
        ) -> Result<Actor<Value>, TxnError>;
    }

    #[async_trait]
    impl ActorResolver for KeyringActorResolver {
        async fn resolve_actor(
            &self,
            host: &Link,
            actor_id: &Value,
        ) -> Result<Actor<Value>, TxnError> {
            self.actors
                .read()
                .get(&(host.clone(), actor_identity(actor_id)))
                .cloned()
                .ok_or(TxnError::Unauthorized)
        }
    }

    #[cfg(feature = "http-client")]
    #[derive(Clone)]
    pub struct RpcActorResolver {
        gateway: Arc<dyn crate::gateway::RpcGateway>,
        txn_id: tc_ir::TxnId,
    }

    #[cfg(feature = "http-client")]
    impl RpcActorResolver {
        pub fn new(gateway: Arc<dyn crate::gateway::RpcGateway>, txn_id: tc_ir::TxnId) -> Self {
            Self { gateway, txn_id }
        }
    }

    #[cfg(feature = "http-client")]
    #[async_trait]
    impl ActorResolver for RpcActorResolver {
        async fn resolve_actor(
            &self,
            host: &Link,
            actor_id: &Value,
        ) -> Result<Actor<Value>, TxnError> {
            use base64::Engine as _;

            let actor_id = actor_identity(actor_id);
            let mut url = url::Url::parse(&host.to_string()).map_err(|_| TxnError::Unauthorized)?;
            url.set_path("/host/public_key");
            url.set_query(Some(&format!(
                "key={}",
                url::form_urlencoded::byte_serialize(actor_id.as_bytes()).collect::<String>()
            )));

            let response = self
                .gateway
                .request(
                    crate::Method::Get,
                    url.to_string(),
                    self.txn_id,
                    None,
                    Vec::new(),
                )
                .await
                .map_err(|_| TxnError::Unauthorized)?;

            if response.status != 200 {
                return Err(TxnError::Unauthorized);
            }

            let body = String::from_utf8(response.body).map_err(|_| TxnError::Unauthorized)?;
            let encoded: String =
                serde_json::from_str(&body).map_err(|_| TxnError::Unauthorized)?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| TxnError::Unauthorized)?;

            let verifying_key = rjwt::VerifyingKey::try_from(bytes.as_slice())
                .map_err(|_| TxnError::Unauthorized)?;

            Ok(Actor::with_public_key(Value::from(actor_id), verifying_key))
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

    #[async_trait]
    impl Resolve for RjwtTokenVerifier {
        type HostId = Link;
        type ActorId = Value;
        type Claims = Claim;

        async fn resolve(
            &self,
            host: &Self::HostId,
            actor_id: &Self::ActorId,
        ) -> Result<Actor<Self::ActorId>, RjwtError> {
            self.resolver
                .resolve_actor(host, actor_id)
                .await
                .map_err(RjwtError::fetch)
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
                let (owner_host, owner_actor_id, owner_claim) =
                    claims.next().ok_or(TxnError::Unauthorized)?;

                let owner_id = format!("{owner_host}::{}", actor_identity(owner_actor_id));
                let mut ctx = TokenContext::new(owner_id, bearer_token);

                ctx = ctx.with_claim(
                    owner_host.to_string(),
                    actor_identity(owner_actor_id),
                    owner_claim.clone(),
                );

                for (host, actor_id, claim) in claims {
                    ctx = ctx.with_claim(host.to_string(), actor_identity(actor_id), claim.clone());
                }

                Ok(ctx)
            }
            .boxed()
        }
    }

    fn actor_identity(actor_id: &Value) -> String {
        match actor_id {
            Value::None => "none".to_string(),
            Value::Number(num) => num.to_string(),
            Value::String(value) => value.clone(),
        }
    }
}

#[cfg(feature = "rjwt-token")]
pub use rjwt_token::{
    ActorResolver as RjwtActorResolver, KeyringActorResolver, PublicKeyStore, RjwtTokenVerifier,
};

#[cfg(all(feature = "rjwt-token", feature = "http-client"))]
pub use rjwt_token::RpcActorResolver;
