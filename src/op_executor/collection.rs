use crate::State;
use pathlink::{Link, PathSegment};
use tc_collection::CollectionState;
use tc_error::TCResult;
use tc_ir::{Map, Public};
use tc_state::Collection;

pub(super) async fn get(
    state: &State,
    path: &[PathSegment],
    key: State,
    txn: &crate::txn::TxnHandle,
) -> TCResult<Option<State>> {
    let Some(collection) = collection(state) else {
        return Ok(None);
    };
    optional(Public::get(collection, txn, path, key.into_scalar()?).await)
}

pub(super) async fn post(
    state: &State,
    path: &[PathSegment],
    params: Map<State>,
    txn: &crate::txn::TxnHandle,
) -> TCResult<Option<State>> {
    let Some(collection) = collection(state) else {
        return Ok(None);
    };
    optional(Public::post(collection, txn, path, params).await)
}

pub(super) async fn put(
    state: &State,
    path: &[PathSegment],
    key: State,
    value: State,
    txn: &crate::txn::TxnHandle,
) -> TCResult<Option<State>> {
    let Some(collection) = collection(state) else {
        return Ok(None);
    };
    optional(Public::put(collection, txn, path, key.into_scalar()?, value).await)
        .map(|result| result.map(|()| State::None))
}

pub(super) async fn delete(
    state: &State,
    path: &[PathSegment],
    key: State,
    txn: &crate::txn::TxnHandle,
) -> TCResult<Option<State>> {
    let Some(collection) = collection(state) else {
        return Ok(None);
    };
    optional(Public::<State>::delete(collection, txn, path, key.into_scalar()?).await)
        .map(|result| result.map(|()| State::None))
}

fn collection(state: &State) -> Option<&Collection<crate::TxnHandle>> {
    let State::Collection(collection) = state else {
        return None;
    };
    Some(collection)
}

fn optional<T>(result: TCResult<T>) -> TCResult<Option<T>> {
    match result {
        Ok(result) => Ok(Some(result)),
        Err(err) if err.code() == tc_error::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

pub(super) fn from_put(link: &Link, key: State, value: State) -> TCResult<Option<State>> {
    Collection::from_put(link, key, value)
}
