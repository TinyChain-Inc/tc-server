use std::{sync::Arc, time::Duration};

use pathlink::Link;

use crate::auth::Actor;
use crate::workspace::Workspace;

#[derive(Clone)]
pub struct ProtocolAuthority {
    pub(crate) host: Link,
    pub(crate) actor: Arc<Actor>,
}

impl ProtocolAuthority {
    pub fn new(host: Link, actor: Actor) -> Self {
        Self {
            host,
            actor: Arc::new(actor),
        }
    }
}

#[derive(Clone)]
pub(crate) struct TxnConfig {
    pub(crate) ttl: Duration,
    pub(crate) grace: Duration,
    pub(crate) protocol: ProtocolAuthority,
    pub(crate) workspace: Workspace,
    pub(crate) resources: crate::HostResources,
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
}
