use super::*;

pub(super) fn param_id(params: &Map<Scalar>, name: &str) -> TCResult<Id> {
    let id: Id = name
        .parse()
        .map_err(|err| TCError::internal(format!("invalid {name} id: {err}")))?;
    let Some(value) = params.get(&id) else {
        return Err(TCError::bad_request(format!("missing {name} parameter")));
    };
    match value {
        Scalar::Value(Value::String(raw)) => raw
            .parse()
            .map_err(|err| TCError::bad_request(format!("invalid {name} value: {err}"))),
        other => Err(TCError::bad_request(format!(
            "expected {name} to be a string but found {other:?}"
        ))),
    }
}

pub(super) async fn param_opdef(
    params: &Map<Scalar>,
    name: &str,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<OpDef> {
    let id: Id = name
        .parse()
        .map_err(|err| TCError::internal(format!("invalid {name} id: {err}")))?;
    let Some(value) = params.get(&id) else {
        return Err(TCError::bad_request(format!("missing {name} parameter")));
    };
    let state = resolve_scalar(value.clone(), values, txn, self_link).await?;
    state_to_opdef(state)
}

pub(super) async fn param_state(
    params: &Map<Scalar>,
    name: &str,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<State> {
    let id: Id = name
        .parse()
        .map_err(|err| TCError::internal(format!("invalid {name} id: {err}")))?;
    let Some(value) = params.get(&id) else {
        return Err(TCError::bad_request(format!("missing {name} parameter")));
    };
    resolve_scalar(value.clone(), values, txn, self_link).await
}

pub(super) fn resolve_params(
    params: Map<Scalar>,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> BoxFuture<'static, TCResult<Map<State>>> {
    let values = Arc::clone(values);
    let txn = txn.clone();
    let self_link = self_link.cloned();
    Box::pin(async move {
        let mut resolved = Map::new();
        for (key, value) in params {
            let value = resolve_scalar(value, &values, &txn, self_link.as_ref()).await?;
            resolved.insert(key, value);
        }
        Ok(resolved)
    })
}
