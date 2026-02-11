use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use number_general::Number;
use pathlink::Link;
use safecast::CastInto;
use tc_error::{TCError, TCResult};
use tc_ir::{Id, IfRef, Map, OpDef, OpRef, Scalar, Subject, TCRef, While};
use tc_state::State;
use tc_value::Value;

use crate::gateway::RpcGateway;

use super::execute::execute_post_with_self;
use super::reflect::reflect_link;

pub(super) fn resolve_scalar<'a>(
    scalar: Scalar,
    values: &'a Arc<HashMap<Id, State>>,
    txn: &'a crate::txn::TxnHandle,
    self_link: Option<&'a Link>,
) -> BoxFuture<'a, TCResult<State>> {
    Box::pin(async move {
        match scalar {
            Scalar::Value(value) => Ok(State::from(value)),
            Scalar::Op(op) => Ok(State::Scalar(Scalar::Op(op))),
            Scalar::Map(map) => {
                let mut out = Map::new();
                for (key, value) in map {
                    let state = resolve_scalar(value, values, txn, self_link).await?;
                    out.insert(key, state_to_scalar(state)?);
                }
                Ok(State::Scalar(Scalar::Map(out)))
            }
            Scalar::Tuple(items) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    let state = resolve_scalar(item, values, txn, self_link).await?;
                    out.push(state_to_scalar(state)?);
                }
                Ok(State::Scalar(Scalar::Tuple(out)))
            }
            Scalar::Ref(r) => match *r {
                TCRef::Id(id_ref) => values
                    .get(id_ref.as_str())
                    .cloned()
                    .ok_or_else(|| TCError::not_found(format!("unknown id ${}", id_ref.as_str()))),
                TCRef::Op(op) => resolve_opref(op, values, txn, self_link).await,
                TCRef::If(if_ref) => resolve_if(*if_ref, values, txn, self_link).await,
                TCRef::While(while_ref) => resolve_while(*while_ref, values, txn, self_link).await,
            },
        }
    })
}

fn resolve_opref(
    op: OpRef,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> BoxFuture<'static, TCResult<State>> {
    let values = Arc::clone(values);
    let txn = txn.clone();
    let self_link = self_link.cloned();
    Box::pin(async move {
        match op {
            OpRef::Get((subject, key)) => match subject {
                Subject::Link(_) | Subject::Ref(_, _) => {
                    let link = resolve_subject(subject, self_link.as_ref())?;
                    let key = scalar_to_value(key, &values, &txn, self_link.as_ref()).await?;
                    txn.get(link, txn.clone(), key).await
                }
            },
            OpRef::Put((subject, key, value)) => match subject {
                Subject::Link(_) | Subject::Ref(_, _) => {
                    let link = resolve_subject(subject, self_link.as_ref())?;
                    let key = scalar_to_value(key, &values, &txn, self_link.as_ref()).await?;
                    let value = scalar_to_state(value, &values, &txn, self_link.as_ref()).await?;
                    txn.put(link, txn.clone(), key, value)
                        .await
                        .map(|()| State::default())
                }
            },
            OpRef::Post((subject, params)) => match subject {
                Subject::Link(_) => {
                    let link = resolve_subject(subject, self_link.as_ref())?;
                    if let Some(state) = reflect_link(&link, &params, &values)? {
                        return Ok(state);
                    }
                    let params = resolve_params(params, &values, &txn, self_link.as_ref()).await?;
                    txn.post(link, txn.clone(), params).await
                }
                Subject::Ref(id_ref, suffix) => {
                    if id_ref.as_str() == "self" {
                        let link =
                            resolve_subject(Subject::Ref(id_ref, suffix), self_link.as_ref())?;
                        let params =
                            resolve_params(params, &values, &txn, self_link.as_ref()).await?;
                        return txn.post(link, txn.clone(), params).await;
                    }
                    let segments = suffix.as_ref();
                    if segments.len() == 1 && segments[0].as_str() == "add" {
                        let left = values.get(id_ref.as_str()).cloned().ok_or_else(|| {
                            TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                        })?;

                        let State::Scalar(Scalar::Value(Value::Number(left))) = left else {
                            return Err(TCError::bad_request(
                                "expected add subject to be a number".to_string(),
                            ));
                        };

                        let r_id: Id = "r".parse().expect("Id");
                        let Some(r_value) = params.get(&r_id) else {
                            return Err(TCError::bad_request(
                                "missing add parameter r".to_string(),
                            ));
                        };
                        let r_value =
                            scalar_to_value(r_value.clone(), &values, &txn, self_link.as_ref())
                                .await?;
                        let Value::Number(right) = r_value else {
                            return Err(TCError::bad_request(
                                "expected add parameter r to be a number".to_string(),
                            ));
                        };

                        Ok(State::from(Value::Number(left + right)))
                    } else if segments.len() == 1 && segments[0].as_str() == "eq" {
                        let left = values.get(id_ref.as_str()).cloned().ok_or_else(|| {
                            TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                        })?;

                        let State::Scalar(Scalar::Value(left)) = left else {
                            return Err(TCError::bad_request(
                                "expected eq subject to be a scalar value".to_string(),
                            ));
                        };

                        let r_id: Id = "r".parse().expect("Id");
                        let Some(r_value) = params.get(&r_id) else {
                            return Err(TCError::bad_request("missing eq parameter r".to_string()));
                        };
                        let right =
                            scalar_to_value(r_value.clone(), &values, &txn, self_link.as_ref())
                                .await?;

                        let is_equal = match (&left, &right) {
                            (Value::Link(l), Value::String(r)) => l.to_string() == *r,
                            (Value::String(l), Value::Link(r)) => *l == r.to_string(),
                            _ => left == right,
                        };

                        Ok(State::from(Value::Number(Number::from(is_equal))))
                    } else if segments.len() == 1 && segments[0].as_str() == "gt" {
                        let left = values.get(id_ref.as_str()).cloned().ok_or_else(|| {
                            TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                        })?;

                        let State::Scalar(Scalar::Value(Value::Number(left))) = left else {
                            return Err(TCError::bad_request(
                                "expected gt subject to be a number".to_string(),
                            ));
                        };

                        let r_id: Id = "r".parse().expect("Id");
                        let Some(r_value) = params.get(&r_id) else {
                            return Err(TCError::bad_request("missing gt parameter r".to_string()));
                        };
                        let r_value =
                            scalar_to_value(r_value.clone(), &values, &txn, self_link.as_ref())
                                .await?;
                        let Value::Number(right) = r_value else {
                            return Err(TCError::bad_request(
                                "expected gt parameter r to be a number".to_string(),
                            ));
                        };

                        Ok(State::from(Value::Number(Number::from(left > right))))
                    } else if segments.len() == 1 && segments[0].as_str() == "len" {
                        let tuple_state =
                            values.get(id_ref.as_str()).cloned().ok_or_else(|| {
                                TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                            })?;
                        let items = tuple_state_to_items(tuple_state)?;
                        Ok(State::from(Value::Number(Number::from(items.len() as u64))))
                    } else if segments.len() == 1 && segments[0].as_str() == "head" {
                        let tuple_state =
                            values.get(id_ref.as_str()).cloned().ok_or_else(|| {
                                TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                            })?;
                        let mut items = tuple_state_to_items(tuple_state)?;
                        let head = items.drain(..1).next().ok_or_else(|| {
                            TCError::bad_request("cannot take head of empty tuple".to_string())
                        })?;
                        let head = state_to_scalar(head)?;
                        Ok(State::Scalar(head))
                    } else if segments.len() == 1 && segments[0].as_str() == "tail" {
                        let tuple_state =
                            values.get(id_ref.as_str()).cloned().ok_or_else(|| {
                                TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                            })?;
                        let items = tuple_state_to_items(tuple_state)?;
                        let tail = items
                            .into_iter()
                            .skip(1)
                            .map(state_to_scalar)
                            .collect::<TCResult<Vec<_>>>()?;
                        Ok(State::Scalar(Scalar::Tuple(tail)))
                    } else if segments.len() == 1 && segments[0].as_str() == "concat" {
                        let left_state = values.get(id_ref.as_str()).cloned().ok_or_else(|| {
                            TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                        })?;
                        let mut left = tuple_state_to_items(left_state)?
                            .into_iter()
                            .map(state_to_scalar)
                            .collect::<TCResult<Vec<_>>>()?;

                        let r_id: Id = "r".parse().expect("Id");
                        let Some(r_value) = params.get(&r_id) else {
                            return Err(TCError::bad_request(
                                "missing concat parameter r".to_string(),
                            ));
                        };
                        let right_state =
                            resolve_scalar(r_value.clone(), &values, &txn, self_link.as_ref())
                                .await?;
                        let mut right = tuple_state_to_items(right_state)?
                            .into_iter()
                            .map(state_to_scalar)
                            .collect::<TCResult<Vec<_>>>()?;
                        left.append(&mut right);
                        Ok(State::Scalar(Scalar::Tuple(left)))
                    } else if segments.len() == 1 && segments[0].as_str() == "get" {
                        let tuple_state =
                            values.get(id_ref.as_str()).cloned().ok_or_else(|| {
                                TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                            })?;
                        let items = tuple_state_to_items(tuple_state)?;

                        let i_id: Id = "i".parse().expect("Id");
                        let Some(i_value) = params.get(&i_id) else {
                            return Err(TCError::bad_request(
                                "missing get parameter i".to_string(),
                            ));
                        };
                        let idx_value =
                            scalar_to_value(i_value.clone(), &values, &txn, self_link.as_ref())
                                .await?;
                        let Value::Number(idx_num) = idx_value else {
                            return Err(TCError::bad_request(
                                "expected get parameter i to be a number".to_string(),
                            ));
                        };
                        let idx: usize = idx_num.cast_into();
                        let item = items.get(idx).cloned().ok_or_else(|| {
                            TCError::bad_request("tuple index out of bounds".to_string())
                        })?;
                        let item = state_to_scalar(item)?;
                        Ok(State::Scalar(item))
                    } else if segments.len() == 1
                        && (segments[0].as_str() == "reduce" || segments[0].as_str() == "fold")
                    {
                        let tuple_state =
                            values.get(id_ref.as_str()).cloned().ok_or_else(|| {
                                TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                            })?;
                        let items = tuple_state_to_items(tuple_state)?;

                        let item_name = param_id(&params, "item_name")?;
                        let op_def =
                            param_opdef(&params, "op", &values, &txn, self_link.as_ref()).await?;
                        let mut state =
                            param_state(&params, "value", &values, &txn, self_link.as_ref())
                                .await?;

                        for item in items {
                            let mut call_params = state_to_params(state)?;
                            call_params.insert(item_name.clone(), item);
                            state = execute_post_with_self(
                                &txn,
                                op_def.clone(),
                                call_params,
                                self_link.clone(),
                            )
                            .await?;
                        }

                        Ok(state)
                    } else {
                        Err(TCError::bad_request(format!(
                            "unsupported opref subject ${}{}",
                            id_ref.as_str(),
                            suffix
                        )))
                    }
                }
            },
            OpRef::Delete((subject, key)) => match subject {
                Subject::Link(_) | Subject::Ref(_, _) => {
                    let link = resolve_subject(subject, self_link.as_ref())?;
                    let key = scalar_to_value(key, &values, &txn, self_link.as_ref()).await?;
                    txn.delete(link, txn.clone(), key)
                        .await
                        .map(|()| State::default())
                }
            },
        }
    })
}

fn resolve_subject(subject: Subject, self_link: Option<&Link>) -> TCResult<Link> {
    match subject {
        Subject::Link(link) => Ok(link),
        Subject::Ref(id_ref, suffix) => {
            if id_ref.as_str() != "self" {
                return Err(TCError::bad_request(format!(
                    "unsupported opref subject ${}",
                    id_ref.as_str()
                )));
            }

            let base = self_link
                .ok_or_else(|| TCError::bad_request("OpDef has $self but no scope"))?
                .clone();

            let mut link = base;
            for segment in suffix.as_ref() {
                link = link.append(segment.clone());
            }
            Ok(link)
        }
    }
}

fn scalar_to_value(
    scalar: Scalar,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> BoxFuture<'static, TCResult<Value>> {
    let values = Arc::clone(values);
    let txn = txn.clone();
    let self_link = self_link.cloned();
    Box::pin(async move {
        match scalar {
            Scalar::Value(value) => Ok(value),
            Scalar::Op(_) => Err(TCError::bad_request(
                "expected scalar value; found Op definition".to_string(),
            )),
            Scalar::Map(_) | Scalar::Tuple(_) => Err(TCError::bad_request(
                "expected scalar value; found a scalar container".to_string(),
            )),
            Scalar::Ref(r) => {
                let response = match *r {
                    TCRef::Id(id_ref) => values.get(id_ref.as_str()).cloned().ok_or_else(|| {
                        TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                    })?,
                    TCRef::Op(op) => resolve_opref(op, &values, &txn, self_link.as_ref()).await?,
                    TCRef::If(if_ref) => {
                        resolve_if(*if_ref, &values, &txn, self_link.as_ref()).await?
                    }
                    TCRef::While(while_ref) => {
                        resolve_while(*while_ref, &values, &txn, self_link.as_ref()).await?
                    }
                };

                match response {
                    State::None => Ok(Value::None),
                    State::Scalar(Scalar::Value(value)) => Ok(value),
                    State::Scalar(Scalar::Map(_) | Scalar::Tuple(_)) => Err(TCError::bad_request(
                        "resolved ref returned scalar container; expected value".to_string(),
                    )),
                    State::Scalar(Scalar::Ref(_)) => Err(TCError::bad_request(
                        "resolved ref returned scalar ref; expected value".to_string(),
                    )),
                    _ => Err(TCError::bad_request(
                        "resolved ref returned non-scalar state".to_string(),
                    )),
                }
            }
        }
    })
}

fn scalar_to_state(
    scalar: Scalar,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> BoxFuture<'static, TCResult<State>> {
    let values = Arc::clone(values);
    let txn = txn.clone();
    let self_link = self_link.cloned();
    Box::pin(async move {
        match scalar {
            Scalar::Value(value) => Ok(State::from(value)),
            Scalar::Op(_) => Ok(State::Scalar(scalar)),
            Scalar::Map(_) | Scalar::Tuple(_) => Ok(State::Scalar(scalar)),
            Scalar::Ref(r) => match *r {
                TCRef::Id(id_ref) => values
                    .get(id_ref.as_str())
                    .cloned()
                    .ok_or_else(|| TCError::not_found(format!("unknown id ${}", id_ref.as_str()))),
                TCRef::Op(op) => resolve_opref(op, &values, &txn, self_link.as_ref()).await,
                TCRef::If(if_ref) => resolve_if(*if_ref, &values, &txn, self_link.as_ref()).await,
                TCRef::While(while_ref) => {
                    resolve_while(*while_ref, &values, &txn, self_link.as_ref()).await
                }
            },
        }
    })
}

async fn resolve_if(
    if_ref: IfRef,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<State> {
    let IfRef {
        cond,
        then,
        or_else,
    } = if_ref;
    let cond_state = resolve_scalar(Scalar::from(cond), values, txn, self_link).await?;
    let cond = resolve_bool_state(cond_state, values, txn, self_link).await?;
    if cond {
        resolve_scalar(then, values, txn, self_link).await
    } else {
        resolve_scalar(or_else, values, txn, self_link).await
    }
}

async fn resolve_while(
    while_ref: While,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<State> {
    let While {
        cond,
        closure,
        state,
    } = while_ref;

    let cond_def = resolve_scalar(cond, values, txn, self_link)
        .await
        .and_then(state_to_opdef)?;
    let closure_def = resolve_scalar(closure, values, txn, self_link)
        .await
        .and_then(state_to_opdef)?;
    let mut state = resolve_scalar(state, values, txn, self_link).await?;

    loop {
        let cond_state = execute_post_with_self(
            txn,
            cond_def.clone(),
            while_params(state.clone())?,
            self_link.cloned(),
        )
        .await?;

        let should_continue = resolve_bool_state(cond_state, values, txn, self_link).await?;

        if !should_continue {
            return Ok(state);
        }

        state = execute_post_with_self(
            txn,
            closure_def.clone(),
            while_params(state)?,
            self_link.cloned(),
        )
        .await?;
    }
}

fn while_params(state: State) -> TCResult<Map<State>> {
    let mut params = Map::new();
    let state_id: Id = "state"
        .parse()
        .map_err(|err| TCError::internal(format!("invalid while state id: {err}")))?;
    params.insert(state_id, state);
    Ok(params)
}

fn state_to_opdef(state: State) -> TCResult<OpDef> {
    match state {
        State::Scalar(Scalar::Op(op)) => Ok(op),
        State::Scalar(other) => Err(TCError::bad_request(format!(
            "expected OpDef for While but found scalar {other:?}"
        ))),
        other => Err(TCError::bad_request(format!(
            "expected OpDef for While but found {other:?}"
        ))),
    }
}

fn tuple_state_to_items(state: State) -> TCResult<Vec<State>> {
    match state {
        State::Tuple(items) => Ok(items),
        State::Scalar(Scalar::Tuple(items)) => Ok(items.into_iter().map(State::Scalar).collect()),
        other => Err(TCError::bad_request(format!(
            "expected tuple for reduce but found {other:?}"
        ))),
    }
}

fn state_to_params(state: State) -> TCResult<Map<State>> {
    match state {
        State::Map(map) => Ok(map),
        State::Scalar(Scalar::Map(map)) => Ok(map
            .into_iter()
            .map(|(id, scalar)| (id, State::Scalar(scalar)))
            .collect()),
        State::Scalar(scalar) => {
            let id: Id = "state"
                .parse()
                .map_err(|err| TCError::internal(format!("invalid state id: {err}")))?;
            let mut map = Map::new();
            map.insert(id, State::Scalar(scalar));
            Ok(map)
        }
        other => Err(TCError::bad_request(format!(
            "expected map for reduce state but found {other:?}"
        ))),
    }
}

fn param_id(params: &Map<Scalar>, name: &str) -> TCResult<Id> {
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

async fn param_opdef(
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
    let state = scalar_to_state(value.clone(), values, txn, self_link).await?;
    state_to_opdef(state)
}

async fn param_state(
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
    scalar_to_state(value.clone(), values, txn, self_link).await
}
async fn resolve_bool_state(
    mut state: State,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<bool> {
    loop {
        match state {
            State::Scalar(Scalar::Ref(r)) => {
                state = resolve_scalar(Scalar::Ref(r), values, txn, self_link).await?;
            }
            State::Scalar(Scalar::Value(Value::Number(number))) => {
                return Ok(number.cast_into());
            }
            State::Scalar(Scalar::Value(Value::None)) | State::None => {
                return Err(TCError::bad_request(
                    "expected condition to be a boolean".to_string(),
                ));
            }
            State::Scalar(other) => {
                return Err(TCError::bad_request(format!(
                    "expected condition to be a scalar boolean; found {other:?}"
                )));
            }
            other => {
                return Err(TCError::bad_request(format!(
                    "expected condition to be a scalar boolean; found {other:?}"
                )));
            }
        }
    }
}

fn resolve_params(
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
            let value = scalar_to_state(value, &values, &txn, self_link.as_ref()).await?;
            resolved.insert(key, value);
        }
        Ok(resolved)
    })
}

pub(super) fn state_to_scalar(state: State) -> TCResult<Scalar> {
    match state {
        State::None => Ok(Scalar::Value(Value::None)),
        State::Scalar(scalar) => Ok(scalar),
        _ => Err(TCError::bad_request(
            "expected scalar state while resolving op".to_string(),
        )),
    }
}
