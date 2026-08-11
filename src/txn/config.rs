use std::{str::FromStr, sync::Arc, time::Duration};

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
pub(crate) struct TxnConfig {
    pub(crate) host_id: Arc<String>,
    pub(crate) ttl: Duration,
    pub(crate) protocol_host: Link,
    pub(crate) protocol_actor: Arc<Actor>,
    pub(crate) workspace: Option<Workspace>,
    pub(crate) resources: crate::HostResources,
}

impl Default for TxnConfig {
    fn default() -> Self {
        Self::with_host_id("tc-host-default")
    }
}

impl TxnConfig {
    pub(crate) fn with_host_id(host_id: impl Into<String>) -> Self {
        let host_id = host_id.into();
        let protocol_actor =
            Actor::new_falcon512(host_id.clone()).expect("generate Falcon-512 transaction actor");
        Self {
            host_id: Arc::new(host_id),
            ttl: Duration::from_secs(3),
            protocol_host: Link::from_str(crate::uri::HOST_ROOT).expect("host root link"),
            protocol_actor: Arc::new(protocol_actor),
            workspace: None,
            resources: crate::HostResources::default(),
        }
    }
}
