use std::sync::Arc;

use tc_error::{TCError, TCResult};
use tc_ir::TxnId;

#[derive(Clone)]
pub struct HostServices {
    pub applications: Arc<crate::ApplicationOwners>,
    pub rpc: Arc<dyn crate::gateway::RpcGateway>,
    pub resources: crate::HostResources,
    pub protocol: crate::ProtocolAuthority,
    pub verifier: Arc<dyn crate::auth::TokenVerifier>,
    pub public_keys: crate::auth::PublicKeyStore,
}

pub(crate) struct HostRuntime {
    pub(crate) applications: Arc<crate::ApplicationOwners>,
    pub(crate) rpc: Arc<dyn crate::gateway::RpcGateway>,
    pub(crate) state: tc_state::Static<crate::TxnHandle>,
    resources: crate::HostResources,
    public_keys: crate::auth::PublicKeyStore,
}

impl HostRuntime {
    pub(crate) fn new(
        applications: Arc<crate::ApplicationOwners>,
        rpc: Arc<dyn crate::gateway::RpcGateway>,
        resources: crate::HostResources,
        public_keys: crate::auth::PublicKeyStore,
    ) -> Self {
        Self {
            applications,
            rpc,
            state: tc_state::Static::default(),
            resources,
            public_keys,
        }
    }

    pub(crate) fn metrics(&self) -> crate::State {
        crate::State::Tuple(
            self.resources
                .snapshots()
                .into_iter()
                .map(capacity_state)
                .collect(),
        )
    }

    pub(crate) fn public_key(&self, actor_id: &str) -> TCResult<crate::State> {
        use base64::Engine as _;

        let key = self
            .public_keys
            .public_key(actor_id)
            .ok_or_else(|| TCError::not_found(actor_id))?;
        Ok(crate::State::from(tc_value::Value::from(
            base64::engine::general_purpose::STANDARD.encode(key.to_bytes()),
        )))
    }

    pub(crate) async fn dispatch_state(
        &self,
        txn: &crate::TxnHandle,
        target: &[pathlink::PathSegment],
        method: tc_ir::Method,
        body: Option<crate::State>,
    ) -> TCResult<Option<crate::State>> {
        let handler =
            tc_ir::Route::route(&self.state, target).ok_or_else(|| TCError::not_found("/state"))?;
        super::invoke_handler(handler, txn, method, body)
            .await
            .map(Some)
    }

    pub(crate) async fn finalize(&self, cutoff: &TxnId) -> TCResult<()> {
        self.applications.finalize(cutoff).await
    }
}

fn capacity_state(snapshot: crate::CapacitySnapshot) -> crate::State {
    use number_general::Number;
    use tc_ir::{Id, Map};
    use tc_value::Value;

    let entry = |name: &str, value| -> (Id, crate::State) {
        (name.parse().expect("capacity field"), value)
    };
    crate::State::Map(Map::from_iter([
        entry(
            "resource",
            crate::State::from(Value::from(snapshot.resource)),
        ),
        entry(
            "limit",
            crate::State::from(Value::from(Number::from(snapshot.limit as u64))),
        ),
        entry(
            "in_flight",
            crate::State::from(Value::from(Number::from(snapshot.in_flight as u64))),
        ),
        entry(
            "wait_count",
            crate::State::from(Value::from(Number::from(snapshot.wait_count))),
        ),
        entry(
            "wait_time_ms",
            crate::State::from(Value::from(Number::from(snapshot.wait_time_ms))),
        ),
        entry(
            "rejection_count",
            crate::State::from(Value::from(Number::from(snapshot.rejection_count))),
        ),
        entry(
            "best_effort_drop_count",
            crate::State::from(Value::from(Number::from(snapshot.best_effort_drop_count))),
        ),
    ]))
}
