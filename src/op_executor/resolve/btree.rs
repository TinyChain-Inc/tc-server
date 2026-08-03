use super::*;
use safecast::TryCastFrom;

pub(super) async fn resolve_btree_post(
    id_ref: &tc_ir::IdRef,
    segments: &[PathSegment],
    params: &Map<Scalar>,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<Option<State>> {
    if segments.len() != 1 {
        return Ok(None);
    }

    let state = values
        .get(id_ref.as_str())
        .cloned()
        .ok_or_else(|| TCError::not_found(format!("unknown id ${}", id_ref.as_str())))?;

    let State::Collection(Collection::BTree(collection)) = state else {
        return Ok(None);
    };

    let op = segments[0].as_str();
    let state = match op {
        "insert" => {
            let row = param_state(params, "row", values, txn, self_link).await?;
            let key = Vec::<Value>::try_cast_from(row, |row| {
                TCError::bad_request(format!("expected BTree row values but found {row:?}"))
            })?;
            collection
                .btree
                .insert_row(txn.id(), key)
                .await
                .map_err(|err| TCError::bad_request(err.to_string()))?;
            State::None
        }
        "delete" => {
            let row = param_state(params, "row", values, txn, self_link).await?;
            let key = Vec::<Value>::try_cast_from(row, |row| {
                TCError::bad_request(format!("expected BTree row values but found {row:?}"))
            })?;
            collection
                .btree
                .delete_row(txn.id(), key)
                .await
                .map_err(|err| TCError::bad_request(err.to_string()))?;
            State::None
        }
        _ => return Ok(None),
    };

    Ok(Some(state))
}

pub(super) async fn resolve_btree_get(
    id_ref: &tc_ir::IdRef,
    segments: &[PathSegment],
    key: Scalar,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<Option<State>> {
    if segments.len() != 1 {
        return Ok(None);
    }

    let state = values
        .get(id_ref.as_str())
        .cloned()
        .ok_or_else(|| TCError::not_found(format!("unknown id ${}", id_ref.as_str())))?;

    let State::Collection(Collection::BTree(collection)) = state else {
        return Ok(None);
    };

    let op = segments[0].as_str();
    let key_state = scalar_to_state(key, values, txn, self_link).await?;
    let state = match op {
        "contains" => {
            let row = Vec::<Value>::try_cast_from(key_state, |row| {
                TCError::bad_request(format!("expected BTree row values but found {row:?}"))
            })?;
            let contains = collection.btree.contains_row(txn.id(), &row).await;
            State::from(Value::from(contains))
        }
        "count" => {
            if matches!(
                key_state,
                State::None | State::Scalar(Scalar::Value(Value::None))
            ) {
                let count = collection
                    .btree
                    .slice(collection.bounds.clone(), collection.reverse)
                    .count(txn.id())
                    .await;
                State::from(Value::from(count))
            } else {
                let (start, end, _) = btree_slice_bounds_from_state(key_state)?;
                let count = collection
                    .btree
                    .slice((start, end), false)
                    .count(txn.id())
                    .await;
                State::from(Value::from(count))
            }
        }
        "is_empty" => {
            if matches!(
                key_state,
                State::None | State::Scalar(Scalar::Value(Value::None))
            ) {
                let is_empty = collection
                    .btree
                    .slice(collection.bounds.clone(), collection.reverse)
                    .is_empty(txn.id())
                    .await;
                State::from(Value::from(is_empty))
            } else {
                let (start, end, reverse) = btree_slice_bounds_from_state(key_state)?;
                let is_empty = collection
                    .btree
                    .slice((start, end), reverse)
                    .is_empty(txn.id())
                    .await;
                State::from(Value::from(is_empty))
            }
        }
        "slice" => {
            let (start, end, reverse) = btree_slice_bounds_from_state(key_state)?;
            State::Collection(Collection::from(collection.slice((start, end), reverse)))
        }
        _ => return Ok(None),
    };

    Ok(Some(state))
}

pub(super) async fn resolve_btree_delete(
    id_ref: &tc_ir::IdRef,
    segments: &[PathSegment],
    key: Scalar,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<Option<State>> {
    if !segments.is_empty() {
        return Ok(None);
    }

    let state = values
        .get(id_ref.as_str())
        .cloned()
        .ok_or_else(|| TCError::not_found(format!("unknown id ${}", id_ref.as_str())))?;

    let State::Collection(Collection::BTree(collection)) = state else {
        return Ok(None);
    };

    let row_state = scalar_to_state(key, values, txn, self_link).await?;
    let row = Vec::<Value>::try_cast_from(row_state, |row| {
        TCError::bad_request(format!("expected BTree row values but found {row:?}"))
    })?;
    collection
        .btree
        .delete_row(txn.id(), row)
        .await
        .map_err(|err| TCError::bad_request(err.to_string()))?;

    Ok(Some(State::None))
}

fn btree_slice_bounds_from_state(
    key_state: State,
) -> TCResult<(std::ops::Bound<Value>, std::ops::Bound<Value>, bool)> {
    let map = match key_state {
        State::Map(map) => map,
        State::Scalar(Scalar::Map(map)) => map
            .into_iter()
            .map(|(id, scalar)| (id, State::Scalar(scalar)))
            .collect(),
        other => {
            return Err(TCError::bad_request(format!(
                "expected BTree slice key map but found {other:?}"
            )));
        }
    };

    let start_id: Id = "start".parse().expect("start id");
    let end_id: Id = "end".parse().expect("end id");
    let reverse_id: Id = "reverse".parse().expect("reverse id");

    let start = match map.get(&start_id).cloned() {
        Some(State::None) | None => std::ops::Bound::Unbounded,
        Some(state) => {
            let value = Value::try_cast_from(state, |state| {
                TCError::bad_request(format!("expected BTree bound value but found {state:?}"))
            })?;
            std::ops::Bound::Included(value)
        }
    };

    let end = match map.get(&end_id).cloned() {
        Some(State::None) | None => std::ops::Bound::Unbounded,
        Some(state) => {
            let value = Value::try_cast_from(state, |state| {
                TCError::bad_request(format!("expected BTree bound value but found {state:?}"))
            })?;
            std::ops::Bound::Excluded(value)
        }
    };

    let reverse = match map.get(&reverse_id).cloned() {
        Some(state) => bool_from_state(state, "BTree slice reverse")?,
        None => false,
    };

    Ok((start, end, reverse))
}
