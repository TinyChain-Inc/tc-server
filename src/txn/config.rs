use std::{sync::Arc, time::Duration};

use pathlink::Link;

use crate::auth::Actor;
use crate::workspace::Workspace;

#[derive(Clone)]
pub struct ProtocolAuthority {
    host: Link,
    actor: Arc<Actor>,
}

impl ProtocolAuthority {
    pub fn new(host: Link, actor: Actor) -> Self {
        Self {
            host,
            actor: Arc::new(actor),
        }
    }

    pub(crate) fn host(&self) -> &Link {
        &self.host
    }

    pub(crate) fn actor_id(&self) -> &str {
        self.actor.id()
    }

    pub(crate) fn verifying_key(&self) -> rjwt::VerifyingKey {
        self.actor.verifying_key()
    }

    pub(crate) fn sign(
        &self,
        token: crate::auth::Token,
    ) -> Result<crate::auth::SignedToken, rjwt::Error> {
        self.actor.sign_token(token)
    }

    pub(crate) fn extend(
        &self,
        token: crate::auth::SignedToken,
        grants: crate::auth::WireClaims,
        now: std::time::SystemTime,
    ) -> Result<crate::auth::SignedToken, rjwt::Error> {
        self.actor
            .consume_and_sign(token, self.host.clone(), grants, now)
    }
}

#[derive(Clone)]
pub(crate) struct TxnConfig {
    ttl: Duration,
    grace: Duration,
    protocol: ProtocolAuthority,
    workspace: Workspace,
    resources: crate::HostResources,
}

impl TxnConfig {
    pub(crate) fn new(
        authority: ProtocolAuthority,
        workspace: Workspace,
        resources: crate::HostResources,
        ttl: Duration,
    ) -> Self {
        Self {
            ttl,
            grace: Duration::from_nanos(super::wire::MAX_INBOUND_TXN_CLOCK_SKEW_NANOS),
            protocol: authority,
            workspace,
            resources,
        }
    }

    pub(super) fn ttl(&self) -> Duration {
        self.ttl
    }

    pub(super) fn grace(&self) -> Duration {
        self.grace
    }

    pub(super) fn protocol(&self) -> &ProtocolAuthority {
        &self.protocol
    }

    pub(super) fn workspace(&self) -> &Workspace {
        &self.workspace
    }

    pub(super) fn resources(&self) -> &crate::HostResources {
        &self.resources
    }
}
