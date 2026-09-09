use std::{sync::Arc, time::Duration};

use pathlink::Link;

use crate::auth::Actor;
use crate::workspace::Workspace;

#[derive(Debug)]
pub enum TxnError {
    NotFound,
    Unauthorized,
}

impl From<TxnError> for tc_error::TCError {
    fn from(err: TxnError) -> Self {
        match err {
            TxnError::NotFound => Self::bad_request("unknown transaction id"),
            TxnError::Unauthorized => Self::unauthorized("unauthorized transaction owner"),
        }
    }
}

#[derive(Clone)]
pub struct ProtocolAuthority {
    pub(crate) host_id: Arc<String>,
    pub(crate) host: Link,
    pub(crate) actor: Arc<Actor>,
}

impl ProtocolAuthority {
    pub fn new(host_id: impl Into<String>, host: Link, actor: Actor) -> Self {
        Self {
            host_id: Arc::new(host_id.into()),
            host,
            actor: Arc::new(actor),
        }
    }
}

#[derive(Clone)]
pub(crate) struct TxnConfig {
    pub(crate) host_id: Arc<String>,
    pub(crate) ttl: Duration,
    pub(crate) grace: Duration,
    pub(crate) protocol_host: Link,
    pub(crate) protocol_actor: Arc<Actor>,
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
            host_id: authority.host_id,
            ttl,
            grace: Duration::from_nanos(super::wire::MAX_INBOUND_TXN_CLOCK_SKEW_NANOS),
            protocol_host: authority.host,
            protocol_actor: authority.actor,
            workspace,
            resources,
        }
    }
}
