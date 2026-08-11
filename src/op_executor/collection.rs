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
    match state {
        State::Collection(collection) => {
            let Some(routes) = collection.routes::<State>() else {
                return Ok(None);
            };
            match Public::get(&routes, txn, path, key.into_scalar()?).await {
                Ok(state) => Ok(Some(state)),
                Err(err) if err.code() == tc_error::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(err),
            }
        }
        _ => Ok(None),
    }
}

pub(super) async fn post(
    state: &State,
    path: &[PathSegment],
    params: Map<State>,
    txn: &crate::txn::TxnHandle,
) -> TCResult<Option<State>> {
    match state {
        State::Collection(collection) => {
            let Some(routes) = collection.routes::<State>() else {
                return Ok(None);
            };
            match Public::post(&routes, txn, path, params).await {
                Ok(state) => Ok(Some(state)),
                Err(err) if err.code() == tc_error::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(err),
            }
        }
        _ => Ok(None),
    }
}

pub(super) async fn delete(
    state: &State,
    path: &[PathSegment],
    key: State,
    txn: &crate::txn::TxnHandle,
) -> TCResult<Option<State>> {
    match state {
        State::Collection(collection) => {
            let Some(routes) = collection.routes::<State>() else {
                return Ok(None);
            };
            match Public::delete(&routes, txn, path, key.into_scalar()?).await {
                Ok(()) => Ok(Some(State::None)),
                Err(err) if err.code() == tc_error::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(err),
            }
        }
        _ => Ok(None),
    }
}

pub(super) fn from_put(link: &Link, key: State, value: State) -> TCResult<Option<State>> {
    Collection::tensor_literal(link, key, value)
}
