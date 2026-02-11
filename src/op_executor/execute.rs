use pathlink::Link;
use tc_error::{TCError, TCResult};
use tc_ir::{Map, OpDef};
use tc_state::State;
use tc_value::Value;

use super::Executor;

pub async fn execute_get(txn: &crate::txn::TxnHandle, op: OpDef, key: Value) -> TCResult<State> {
    execute_get_with_self(txn, op, key, None).await
}

pub async fn execute_get_with_self(
    txn: &crate::txn::TxnHandle,
    op: OpDef,
    key: Value,
    self_link: Option<Link>,
) -> TCResult<State> {
    let OpDef::Get((key_name, form)) = op else {
        return Err(TCError::bad_request(
            "expected GET op definition".to_string(),
        ));
    };

    let capture = form
        .last()
        .map(|(id, _)| id.clone())
        .unwrap_or_else(|| "_result".parse().expect("Id"));

    let data = [(key_name, State::from(key))];
    Executor::new_with_self(txn, data, form, self_link)?
        .capture(capture)
        .await
}

pub async fn execute_put(
    txn: &crate::txn::TxnHandle,
    op: OpDef,
    key: Value,
    value: State,
) -> TCResult<()> {
    execute_put_with_self(txn, op, key, value, None).await
}

pub async fn execute_put_with_self(
    txn: &crate::txn::TxnHandle,
    op: OpDef,
    key: Value,
    value: State,
    self_link: Option<Link>,
) -> TCResult<()> {
    let OpDef::Put((key_name, value_name, form)) = op else {
        return Err(TCError::bad_request(
            "expected PUT op definition".to_string(),
        ));
    };

    let capture = match form.last() {
        Some((id, _)) => id.clone(),
        None => return Ok(()),
    };

    let data = [(key_name, State::from(key)), (value_name, value)];
    Executor::new_with_self(txn, data, form, self_link)?
        .capture(capture)
        .await?;
    Ok(())
}

pub async fn execute_post(
    txn: &crate::txn::TxnHandle,
    op: OpDef,
    params: Map<State>,
) -> TCResult<State> {
    execute_post_with_self(txn, op, params, None).await
}

pub async fn execute_post_with_self(
    txn: &crate::txn::TxnHandle,
    op: OpDef,
    params: Map<State>,
    self_link: Option<Link>,
) -> TCResult<State> {
    let OpDef::Post(form) = op else {
        return Err(TCError::bad_request(
            "expected POST op definition".to_string(),
        ));
    };

    let capture = form
        .last()
        .map(|(id, _)| id.clone())
        .unwrap_or_else(|| "_result".parse().expect("Id"));

    let mut data = Vec::with_capacity(params.len());
    for (key, value) in params {
        data.push((key, value));
    }
    Executor::new_with_self(txn, data, form, self_link)?
        .capture(capture)
        .await
}

pub async fn execute_delete(txn: &crate::txn::TxnHandle, op: OpDef, key: Value) -> TCResult<()> {
    execute_delete_with_self(txn, op, key, None).await
}

pub async fn execute_delete_with_self(
    txn: &crate::txn::TxnHandle,
    op: OpDef,
    key: Value,
    self_link: Option<Link>,
) -> TCResult<()> {
    let OpDef::Delete((key_name, form)) = op else {
        return Err(TCError::bad_request(
            "expected DELETE op definition".to_string(),
        ));
    };

    let capture = match form.last() {
        Some((id, _)) => id.clone(),
        None => return Ok(()),
    };

    let data = [(key_name, State::from(key))];
    Executor::new_with_self(txn, data, form, self_link)?
        .capture(capture)
        .await?;
    Ok(())
}
