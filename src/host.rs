use number_general::Number;
use tc_error::{TCError, TCResult};
use tc_ir::{Id, Map};
use tc_value::Value;

use crate::{State, TxnHandle};

fn entry(name: &str, value: State) -> (Id, State) {
    (name.parse().expect("static host field name"), value)
}

/// Return the authenticated host context as native state.
pub fn auth_context(txn: &TxnHandle) -> TCResult<State> {
    let auth = txn
        .auth_context()
        .ok_or_else(|| TCError::unauthorized("missing authenticated request context"))?;

    let claims = auth
        .claims
        .iter()
        .map(|claim| {
            State::Map(Map::from_iter([
                entry("host", State::from(Value::from(claim.host.clone()))),
                entry("actor_id", State::from(Value::from(claim.actor_id.clone()))),
                entry(
                    "link",
                    State::from(Value::from(claim.claim.link.to_string())),
                ),
                entry(
                    "mode",
                    State::from(Value::from(Number::from(u32::from(claim.claim.mask)))),
                ),
            ]))
        })
        .collect();
    let token_hosts = auth
        .token_hosts()
        .into_iter()
        .map(|host| State::from(Value::from(host)))
        .collect();

    Ok(State::Map(Map::from_iter([
        entry(
            "principal",
            State::from(Value::from(auth.principal.clone())),
        ),
        entry(
            "txn_timestamp_nanos",
            State::from(Value::from(Number::from(txn.id().timestamp().as_nanos()))),
        ),
        entry(
            "token_verified_at_nanos",
            State::from(Value::from(Number::from(auth.verified_at_nanos))),
        ),
        entry("token_hosts", State::Tuple(token_hosts)),
        entry("claims", State::Tuple(claims)),
    ])))
}
