use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use number_general::Number;
use pathlink::{Link, PathSegment};
use safecast::CastInto;
use tc_error::{TCError, TCResult};
use tc_ir::{Cond, ForEach, Id, Map, NativeClass, OpDef, OpRef, Scalar, Subject, TCRef, While};
use tc_state::{AxisRange, Collection, Range, State, Tensor, TensorReduceResult, TensorType};
use tc_value::Value;

use super::broadcast_reduce::broadcast_reduce_sum;
use super::execute::execute_post_with_self;
use super::reflect::reflect_link;
use super::tensor_add::broadcast_add;
use super::tensor_dtype::tensor_op_result;
use super::tensor_matmul::batched_matmul;
use crate::gateway::RpcGateway;
use crate::op_plan::opdef_free_ids;

pub(crate) fn resolve_scalar<'a>(
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
                TCRef::Cond(cond) => resolve_cond(*cond, values, txn, self_link).await,
                TCRef::While(while_ref) => resolve_while(*while_ref, values, txn, self_link).await,
                TCRef::ForEach(for_each) => {
                    resolve_for_each(*for_each, values, txn, self_link).await
                }
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
                Subject::Ref(id_ref, suffix) => {
                    if let Some(state) = resolve_tensor_get(
                        &id_ref,
                        suffix.as_ref(),
                        key.clone(),
                        &values,
                        &txn,
                        self_link.as_ref(),
                    )
                    .await?
                    {
                        return Ok(state);
                    }

                    let link = resolve_subject(
                        Subject::Ref(id_ref, suffix),
                        values.as_ref(),
                        self_link.as_ref(),
                    )?;
                    let key = scalar_to_value(key, &values, &txn, self_link.as_ref()).await?;
                    txn.get(link, txn.clone(), key).await
                }
                Subject::Link(_) => {
                    let link = resolve_subject(subject, values.as_ref(), self_link.as_ref())?;
                    let key = scalar_to_value(key, &values, &txn, self_link.as_ref()).await?;
                    txn.get(link, txn.clone(), key).await
                }
            },
            OpRef::Put((subject, key, value)) => match subject {
                Subject::Link(_) | Subject::Ref(_, _) => {
                    let link = resolve_subject(subject, values.as_ref(), self_link.as_ref())?;
                    if matches!(key, Scalar::Tuple(_)) && link == TensorType.path().to_string() {
                        let tensor = parse_tensor_literal_put(key, value)?;
                        return Ok(State::Collection(Collection::Tensor(tensor)));
                    }
                    let key = scalar_to_value(key, &values, &txn, self_link.as_ref()).await?;
                    let value = scalar_to_state(value, &values, &txn, self_link.as_ref()).await?;
                    txn.put(link, txn.clone(), key, value)
                        .await
                        .map(|()| State::default())
                }
            },
            OpRef::Post((subject, params)) => match subject {
                Subject::Link(_) => {
                    let link = resolve_subject(subject, values.as_ref(), self_link.as_ref())?;
                    if let Some(state) = reflect_link(&link, &params, &values)? {
                        return Ok(state);
                    }
                    let params = resolve_params(params, &values, &txn, self_link.as_ref()).await?;
                    txn.post(link, txn.clone(), params).await
                }
                Subject::Ref(id_ref, suffix) => {
                    if id_ref.as_str() == "self" {
                        let link = resolve_subject(
                            Subject::Ref(id_ref, suffix),
                            values.as_ref(),
                            self_link.as_ref(),
                        )?;
                        let params =
                            resolve_params(params, &values, &txn, self_link.as_ref()).await?;
                        return txn.post(link, txn.clone(), params).await;
                    }
                    if let Some(state) = resolve_tensor_post(
                        &id_ref,
                        suffix.as_ref(),
                        &params,
                        &values,
                        &txn,
                        self_link.as_ref(),
                    )
                    .await?
                    {
                        return Ok(state);
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
                            (Value::Link(l), Value::String(r)) => l == r,
                            (Value::String(l), Value::Link(r)) => r == l,
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
                    } else if segments.len() == 1
                        && matches!(segments[0].as_str(), "and" | "or" | "xor")
                    {
                        let left = values.get(id_ref.as_str()).cloned().ok_or_else(|| {
                            TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                        })?;

                        let State::Scalar(Scalar::Value(Value::Number(left))) = left else {
                            return Err(TCError::bad_request(
                                "expected boolean subject to be a number".to_string(),
                            ));
                        };

                        let r_id: Id = "r".parse().expect("Id");
                        let Some(r_value) = params.get(&r_id) else {
                            return Err(TCError::bad_request(
                                "missing boolean parameter r".to_string(),
                            ));
                        };
                        let r_value =
                            scalar_to_value(r_value.clone(), &values, &txn, self_link.as_ref())
                                .await?;
                        let Value::Number(right) = r_value else {
                            return Err(TCError::bad_request(
                                "expected boolean parameter r to be a number".to_string(),
                            ));
                        };

                        let left_bool = left != Number::from(0);
                        let right_bool = right != Number::from(0);
                        let result = match segments[0].as_str() {
                            "and" => left_bool && right_bool,
                            "or" => left_bool || right_bool,
                            "xor" => left_bool ^ right_bool,
                            _ => unreachable!("boolean op checked above"),
                        };
                        Ok(State::from(Value::Number(Number::from(result))))
                    } else if segments.len() == 1 && segments[0].as_str() == "not" {
                        let left = values.get(id_ref.as_str()).cloned().ok_or_else(|| {
                            TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                        })?;

                        let State::Scalar(Scalar::Value(Value::Number(left))) = left else {
                            return Err(TCError::bad_request(
                                "expected boolean subject to be a number".to_string(),
                            ));
                        };

                        let left_bool = left != Number::from(0);
                        Ok(State::from(Value::Number(Number::from(!left_bool))))
                    } else if segments.len() == 1 && segments[0].as_str() == "len" {
                        let tuple_state =
                            values.get(id_ref.as_str()).cloned().ok_or_else(|| {
                                TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                            })?;
                        let items = tuple_state_to_items(tuple_state, "slice")?;
                        Ok(State::from(Value::Number(Number::from(items.len() as u64))))
                    } else if segments.len() == 1 && segments[0].as_str() == "slice" {
                        let state = values.get(id_ref.as_str()).cloned().ok_or_else(|| {
                            TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                        })?;
                        if let State::Scalar(Scalar::Value(value)) = &state {
                            return Err(TCError::bad_request(format!(
                                "slice subject ${} resolved to scalar value {value:?}",
                                id_ref.as_str()
                            )));
                        }
                        let items = tuple_state_to_items(state, "slice")?;

                        let start_id: Id = "start".parse().expect("Id");
                        let stop_id: Id = "stop".parse().expect("Id");
                        let Some(start_value) = params.get(&start_id) else {
                            return Err(TCError::bad_request(
                                "missing slice parameter start".to_string(),
                            ));
                        };
                        let Some(stop_value) = params.get(&stop_id) else {
                            return Err(TCError::bad_request(
                                "missing slice parameter stop".to_string(),
                            ));
                        };

                        let start_value =
                            scalar_to_value(start_value.clone(), &values, &txn, self_link.as_ref())
                                .await?;
                        let stop_value =
                            scalar_to_value(stop_value.clone(), &values, &txn, self_link.as_ref())
                                .await?;

                        let Value::Number(start_num) = start_value else {
                            return Err(TCError::bad_request(
                                "expected slice parameter start to be a number".to_string(),
                            ));
                        };
                        let Value::Number(stop_num) = stop_value else {
                            return Err(TCError::bad_request(
                                "expected slice parameter stop to be a number".to_string(),
                            ));
                        };

                        let len = items.len() as i64;
                        let mut start: i64 = start_num.cast_into();
                        let mut stop: i64 = stop_num.cast_into();
                        if start < 0 {
                            start += len;
                        }
                        if stop < 0 {
                            stop += len;
                        }
                        if start < 0 {
                            start = 0;
                        }
                        if stop < 0 {
                            stop = 0;
                        }
                        if start > len {
                            start = len;
                        }
                        if stop > len {
                            stop = len;
                        }
                        if stop < start {
                            stop = start;
                        }

                        let sliced = items
                            .into_iter()
                            .skip(start as usize)
                            .take((stop - start) as usize)
                            .map(state_to_scalar)
                            .collect::<TCResult<Vec<_>>>()?;
                        Ok(State::Scalar(Scalar::Tuple(sliced)))
                    } else if segments.len() == 1 && segments[0].as_str() == "head" {
                        let tuple_state =
                            values.get(id_ref.as_str()).cloned().ok_or_else(|| {
                                TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                            })?;
                        let mut items = tuple_state_to_items(tuple_state, "head")?;
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
                        let items = tuple_state_to_items(tuple_state, "tail")?;
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

                        let r_id: Id = "r".parse().expect("Id");
                        let Some(r_value) = params.get(&r_id) else {
                            return Err(TCError::bad_request(
                                "missing concat parameter r".to_string(),
                            ));
                        };
                        let right_state =
                            resolve_scalar(r_value.clone(), &values, &txn, self_link.as_ref())
                                .await?;

                        if let Some(left) = string_state_value(&left_state) {
                            let Some(right) = string_state_value(&right_state) else {
                                return Err(TCError::bad_request(format!(
                                    "expected string concat parameter r but found {right_state:?}"
                                )));
                            };
                            let mut out = left.to_string();
                            out.push_str(right);
                            return Ok(State::Scalar(Scalar::Value(Value::String(out))));
                        }

                        let mut left = tuple_state_to_items(left_state, "concat")?
                            .into_iter()
                            .map(state_to_scalar)
                            .collect::<TCResult<Vec<_>>>()?;
                        let mut right = tuple_state_to_items(right_state, "concat")?
                            .into_iter()
                            .map(state_to_scalar)
                            .collect::<TCResult<Vec<_>>>()?;
                        left.append(&mut right);
                        Ok(State::Scalar(Scalar::Tuple(left)))
                    } else if segments.len() == 1 && segments[0].as_str() == "render" {
                        let template = values.get(id_ref.as_str()).cloned().ok_or_else(|| {
                            TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                        })?;
                        let Some(template) = string_state_value(&template) else {
                            return Err(TCError::bad_request(
                                "expected render subject to be a string".to_string(),
                            ));
                        };

                        let mut rendered = template.to_string();
                        for (key, value) in params {
                            let value =
                                scalar_to_value(value, &values, &txn, self_link.as_ref()).await?;
                            let replacement = value_to_render_string(value)?;
                            rendered = rendered.replace(
                                &format!("{{{{{}}}}}", key.as_str()),
                                replacement.as_str(),
                            );
                        }

                        Ok(State::Scalar(Scalar::Value(Value::String(rendered))))
                    } else if segments.len() == 1 && segments[0].as_str() == "get" {
                        let state = values.get(id_ref.as_str()).cloned().ok_or_else(|| {
                            TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                        })?;

                        let i_id: Id = "i".parse().expect("Id");
                        let Some(i_value) = params.get(&i_id) else {
                            return Err(TCError::bad_request(
                                "missing get parameter i".to_string(),
                            ));
                        };
                        let idx_value =
                            scalar_to_value(i_value.clone(), &values, &txn, self_link.as_ref())
                                .await?;

                        match state {
                            State::Tuple(items) => {
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
                            }
                            State::Scalar(Scalar::Tuple(items)) => {
                                let Value::Number(idx_num) = idx_value else {
                                    return Err(TCError::bad_request(
                                        "expected get parameter i to be a number".to_string(),
                                    ));
                                };
                                let idx: usize = idx_num.cast_into();
                                let item = items.get(idx).cloned().ok_or_else(|| {
                                    TCError::bad_request("tuple index out of bounds".to_string())
                                })?;
                                Ok(State::Scalar(item))
                            }
                            State::Map(map) => {
                                let Value::String(key) = idx_value else {
                                    return Err(TCError::bad_request(
                                        "expected get parameter i to be a string".to_string(),
                                    ));
                                };
                                let id: Id = key.parse().map_err(|err| {
                                    TCError::bad_request(format!("invalid map key: {err}"))
                                })?;
                                let value = map.get(&id).cloned().ok_or_else(|| {
                                    TCError::bad_request("map key not found".to_string())
                                })?;
                                Ok(value)
                            }
                            State::Scalar(Scalar::Map(map)) => {
                                let Value::String(key) = idx_value else {
                                    return Err(TCError::bad_request(
                                        "expected get parameter i to be a string".to_string(),
                                    ));
                                };
                                let id: Id = key.parse().map_err(|err| {
                                    TCError::bad_request(format!("invalid map key: {err}"))
                                })?;
                                let value = map.get(&id).cloned().ok_or_else(|| {
                                    TCError::bad_request("map key not found".to_string())
                                })?;
                                Ok(State::Scalar(value))
                            }
                            other => Err(TCError::bad_request(format!(
                                "expected tuple or map for get on ${} but found {other:?}",
                                id_ref.as_str()
                            ))),
                        }
                    } else if segments.len() == 1
                        && (segments[0].as_str() == "reduce" || segments[0].as_str() == "fold")
                    {
                        let tuple_state =
                            values.get(id_ref.as_str()).cloned().ok_or_else(|| {
                                TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                            })?;
                        let items = tuple_state_to_items(tuple_state, "slice")?;

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
                        let link = resolve_subject(
                            Subject::Ref(id_ref, suffix),
                            values.as_ref(),
                            self_link.as_ref(),
                        )?;
                        let params =
                            resolve_params(params, &values, &txn, self_link.as_ref()).await?;
                        txn.post(link, txn.clone(), params).await
                    }
                }
            },
            OpRef::Delete((subject, key)) => match subject {
                Subject::Link(_) | Subject::Ref(_, _) => {
                    let link = resolve_subject(subject, values.as_ref(), self_link.as_ref())?;
                    let key = scalar_to_value(key, &values, &txn, self_link.as_ref()).await?;
                    txn.delete(link, txn.clone(), key)
                        .await
                        .map(|()| State::default())
                }
            },
        }
    })
}

fn resolve_subject(
    subject: Subject,
    values: &HashMap<Id, State>,
    self_link: Option<&Link>,
) -> TCResult<Link> {
    match subject {
        Subject::Link(link) => Ok(link),
        Subject::Ref(id_ref, suffix) => {
            let base = if id_ref.as_str() == "self" {
                self_link
                    .ok_or_else(|| TCError::bad_request("OpDef has $self but no scope"))?
                    .clone()
            } else {
                let state = values.get(id_ref.as_str()).ok_or_else(|| {
                    TCError::not_found(format!("unknown id ${}", id_ref.as_str()))
                })?;

                match state {
                    State::Scalar(Scalar::Value(Value::Link(link))) => link.clone(),
                    State::Scalar(Scalar::Value(Value::String(link))) => {
                        link.parse().map_err(|err| {
                            TCError::bad_request(format!(
                                "expected id ${} to resolve to a valid link, found invalid string: {err}",
                                id_ref.as_str()
                            ))
                        })?
                    }
                    other => {
                        return Err(TCError::bad_request(format!(
                            "expected id ${} to resolve to a link, found {other:?}",
                            id_ref.as_str()
                        )))
                    }
                }
            };

            let mut link = base;
            for segment in suffix.as_ref() {
                link = link.append(segment.clone());
            }
            Ok(link)
        }
    }
}

async fn resolve_tensor_get(
    id_ref: &tc_ir::IdRef,
    segments: &[PathSegment],
    key: Scalar,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<Option<State>> {
    let Some(tensor) = tensor_for_id(id_ref, values, txn, self_link).await? else {
        return Ok(None);
    };

    if segments.is_empty() {
        let bounds = scalar_to_state(key, values, txn, self_link).await?;
        let range = tensor_range_from_state(bounds, tensor.shape())?;
        let sliced = tensor.slice(range).map_err(TCError::bad_request)?;
        return Ok(Some(State::Collection(Collection::Tensor(sliced))));
    }

    if segments.len() != 1 {
        return Ok(None);
    }

    let state = match segments[0].as_str() {
        "broadcast" => {
            let shape = scalar_to_state(key, values, txn, self_link).await?;
            let shape = shape_from_state(shape)?;
            tensor.broadcast(shape).map_err(TCError::bad_request)?
        }
        "cast" => {
            let dtype_state = scalar_to_state(key, values, txn, self_link).await?;
            let dtype = tensor_dtype_from_state(dtype_state)?;
            tensor.cast(&dtype).map_err(TCError::bad_request)?
        }
        "expand_dims" => {
            let axes = scalar_to_state(key, values, txn, self_link).await?;
            if let Some(axes) = optional_shape_from_state(axes)? {
                tensor
                    .expand_dims(Some(axes))
                    .map_err(TCError::bad_request)?
            } else {
                tensor.expand_dims(None).map_err(TCError::bad_request)?
            }
        }
        "reshape" => {
            let shape = scalar_to_state(key, values, txn, self_link).await?;
            let shape = shape_from_state(shape)?;
            tensor.reshape(shape).map_err(TCError::bad_request)?
        }
        "transpose" => {
            let permutation = scalar_to_state(key, values, txn, self_link).await?;
            let permutation = optional_shape_from_state(permutation)?;

            tensor
                .transpose(permutation)
                .map_err(TCError::bad_request)?
        }
        _ => return Ok(None),
    };

    Ok(Some(State::Collection(Collection::Tensor(state))))
}

async fn resolve_tensor_post(
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

    let Some(tensor) = tensor_for_id(id_ref, values, txn, self_link).await? else {
        return Ok(None);
    };

    let state = match segments[0].as_str() {
        "dtype" => State::Scalar(Scalar::Value(Value::String(tensor.dtype_tag().to_string()))),
        "ndim" => State::from(Value::Number(Number::from(tensor.shape().len() as u64))),
        "shape" => State::Scalar(Scalar::Tuple(
            tensor
                .shape()
                .iter()
                .map(|dim| Scalar::Value(Value::Number(Number::from(*dim as u64))))
                .collect(),
        )),
        "size" => State::from(Value::Number(Number::from(tensor.size() as u64))),
        "all" => tensor_truthy_state(&tensor, true)?,
        "any" => tensor_truthy_state(&tensor, false)?,
        "cond" => {
            let then_tensor = tensor_param(params, "then", values, txn, self_link).await?;
            let else_tensor = tensor_param(params, "or_else", values, txn, self_link).await?;
            State::Collection(Collection::Tensor(
                Tensor::cond(&tensor, &then_tensor, &else_tensor).map_err(TCError::bad_request)?,
            ))
        }
        "max" | "min" | "mean" | "norm" | "product" | "std" | "sum" => {
            let axes = tensor_optional_axes_param(params, values, txn, self_link).await?;
            let keepdims = tensor_keepdims_param(params, values, txn, self_link).await?;

            match tensor
                .reduce_axes(segments[0].as_str(), axes, keepdims)
                .map_err(TCError::bad_request)?
            {
                TensorReduceResult::Scalar(number) => State::from(Value::Number(number)),
                TensorReduceResult::Tensor(tensor) => State::Collection(Collection::Tensor(tensor)),
            }
        }
        "broadcast_reduce" => {
            let target_shape =
                tensor_shape_param(params, "target_shape", values, txn, self_link).await?;
            State::Collection(Collection::Tensor(tensor_op_result(broadcast_reduce_sum(
                &tensor,
                &target_shape,
            ))?))
        }
        "matmul" => {
            let right = tensor_param(params, "r", values, txn, self_link).await?;
            State::Collection(Collection::Tensor(tensor_op_result(batched_matmul(
                &tensor, &right,
            ))?))
        }
        "add" => {
            let right = tensor_param(params, "r", values, txn, self_link).await?;
            State::Collection(Collection::Tensor(tensor_op_result(broadcast_add(
                &tensor, &right,
            ))?))
        }
        "sub" | "mul" | "div" | "and" | "or" | "xor" => {
            let right = tensor_param(params, "r", values, txn, self_link).await?;
            State::Collection(Collection::Tensor(
                tensor
                    .binary_op(&right, segments[0].as_str())
                    .map_err(TCError::bad_request)?,
            ))
        }
        "not" => State::Collection(Collection::Tensor(
            tensor.unary_not().map_err(TCError::bad_request)?,
        )),
        _ => unreachable!("unsupported tensor post op segment"),
    };

    Ok(Some(state))
}

async fn tensor_for_id(
    id_ref: &tc_ir::IdRef,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<Option<Tensor>> {
    let Some(state) = values.get(id_ref.as_str()) else {
        return Err(TCError::not_found(format!(
            "unknown id ${}",
            id_ref.as_str()
        )));
    };

    match state {
        State::Collection(Collection::Tensor(tensor)) => Ok(Some(tensor.clone())),
        State::Scalar(scalar) => {
            if let Some(tensor) = tensor_from_scalar_literal(scalar)? {
                return Ok(Some(tensor));
            }

            let resolved = resolve_scalar(scalar.clone(), values, txn, self_link).await?;
            match resolved {
                State::Collection(Collection::Tensor(tensor)) => Ok(Some(tensor)),
                _ => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

async fn tensor_param(
    params: &Map<Scalar>,
    name: &str,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<Tensor> {
    let value = required_scalar_param(params, name, "tensor parameter")?;

    if let Some(tensor) = tensor_from_scalar_literal(value)? {
        return Ok(tensor);
    }

    match scalar_to_state(value.clone(), values, txn, self_link).await? {
        State::Collection(Collection::Tensor(tensor)) => Ok(tensor),
        State::Scalar(scalar) => {
            if let Some(tensor) = tensor_from_scalar_literal(&scalar)? {
                Ok(tensor)
            } else {
                Err(TCError::bad_request(format!(
                    "expected tensor parameter {name} but found scalar {scalar:?}"
                )))
            }
        }
        other => Err(TCError::bad_request(format!(
            "expected tensor parameter {name} but found {other:?}"
        ))),
    }
}

async fn tensor_optional_axes_param(
    params: &Map<Scalar>,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<Option<Vec<usize>>> {
    let value = if let Some(value) = scalar_param(params, "axes", "tensor axes")? {
        value
    } else if let Some(value) = scalar_param(params, "axis", "tensor axis")? {
        value
    } else {
        return Ok(None);
    };

    let state = scalar_to_state(value.clone(), values, txn, self_link).await?;
    optional_axes_from_state(state)
}

async fn tensor_keepdims_param(
    params: &Map<Scalar>,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<bool> {
    let Some(value) = scalar_param(params, "keepdims", "tensor keepdims")? else {
        return Ok(false);
    };

    let state = scalar_to_state(value.clone(), values, txn, self_link).await?;
    bool_from_state(state, "tensor keepdims")
}

async fn tensor_shape_param(
    params: &Map<Scalar>,
    name: &str,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<Vec<usize>> {
    let value = required_scalar_param(params, name, "tensor shape")?;
    let state = scalar_to_state(value.clone(), values, txn, self_link).await?;
    shape_from_state(state)
}

fn scalar_param<'a>(
    params: &'a Map<Scalar>,
    name: &str,
    context: &str,
) -> TCResult<Option<&'a Scalar>> {
    let id: Id = name
        .parse()
        .map_err(|err| TCError::internal(format!("invalid {context} id: {err}")))?;

    Ok(params.get(&id))
}

fn required_scalar_param<'a>(
    params: &'a Map<Scalar>,
    name: &str,
    context: &str,
) -> TCResult<&'a Scalar> {
    let Some(value) = scalar_param(params, name, context)? else {
        return Err(TCError::bad_request(format!("missing {context} {name}")));
    };

    Ok(value)
}

fn tensor_from_scalar_literal(scalar: &Scalar) -> TCResult<Option<Tensor>> {
    let Scalar::Ref(r) = scalar else {
        return Ok(None);
    };

    let TCRef::Op(OpRef::Put((subject, key, value))) = r.as_ref() else {
        return Ok(None);
    };

    let Subject::Link(link) = subject else {
        return Ok(None);
    };

    let subject_path = link.to_string();
    let tensor_path = TensorType.path().to_string();
    if subject_path != tensor_path {
        return Ok(None);
    }

    parse_tensor_literal_put(key.clone(), value.clone()).map(Some)
}

fn parse_tensor_literal_put(key: Scalar, value: Scalar) -> TCResult<Tensor> {
    let Scalar::Tuple(key_parts) = key else {
        return Err(TCError::bad_request(
            "tensor literal key must be a tuple [dtype, shape]".to_string(),
        ));
    };

    if key_parts.len() != 2 {
        return Err(TCError::bad_request(
            "tensor literal key must have exactly two entries [dtype, shape]".to_string(),
        ));
    }

    let dtype = scalar_to_string(key_parts[0].clone(), "tensor literal dtype")?;
    let shape = scalar_to_shape(key_parts[1].clone())?;
    let values = scalar_to_numbers(value)?;

    match dtype.as_str() {
        "f32" => Tensor::dense_f32(shape, values.into_iter().map(|n| n.cast_into()).collect())
            .map_err(TCError::bad_request),
        "f64" => Tensor::dense_f64(shape, values.into_iter().map(|n| n.cast_into()).collect())
            .map_err(TCError::bad_request),
        "u64" => Tensor::dense_u64(shape, values.into_iter().map(|n| n.cast_into()).collect())
            .map_err(TCError::bad_request),
        other => Err(TCError::bad_request(format!(
            "unsupported tensor literal dtype {other}"
        ))),
    }
}

fn scalar_to_string(scalar: Scalar, context: &str) -> TCResult<String> {
    match scalar {
        Scalar::Value(Value::String(s)) => Ok(s),
        other => Err(TCError::bad_request(format!(
            "expected {context} to be a string but found {other:?}"
        ))),
    }
}

fn scalar_to_shape(scalar: Scalar) -> TCResult<Vec<usize>> {
    scalar_number_tuple(scalar, "tensor literal shape")?
        .into_iter()
        .map(|number| number_to_usize(number, "tensor shape dimension"))
        .collect()
}

fn scalar_to_numbers(scalar: Scalar) -> TCResult<Vec<Number>> {
    scalar_number_tuple(scalar, "tensor literal values")
}

fn shape_from_state(state: State) -> TCResult<Vec<usize>> {
    let items = tuple_state_to_items(state, "tensor shape")?;
    items
        .into_iter()
        .map(|item| match item {
            State::Scalar(Scalar::Value(Value::Number(number))) => {
                number_to_usize(number, "tensor shape dimension")
            }
            other => Err(TCError::bad_request(format!(
                "expected tensor shape dimension to be a number but found {other:?}"
            ))),
        })
        .collect()
}

fn scalar_tuple_items(scalar: Scalar, context: &str) -> TCResult<Vec<Scalar>> {
    match scalar {
        Scalar::Tuple(items) => Ok(items),
        other => Err(TCError::bad_request(format!(
            "expected {context} to be a tuple but found {other:?}"
        ))),
    }
}

fn scalar_number_tuple(scalar: Scalar, context: &str) -> TCResult<Vec<Number>> {
    let items = scalar_tuple_items(scalar, context)?;
    items
        .into_iter()
        .map(|item| match item {
            Scalar::Value(Value::Number(number)) => Ok(number),
            other => Err(TCError::bad_request(format!(
                "expected {context} element to be a number but found {other:?}"
            ))),
        })
        .collect()
}

fn optional_shape_from_state(state: State) -> TCResult<Option<Vec<usize>>> {
    if matches!(
        state,
        State::None | State::Scalar(Scalar::Value(Value::None))
    ) {
        Ok(None)
    } else {
        shape_from_state(state).map(Some)
    }
}

fn optional_axes_from_state(state: State) -> TCResult<Option<Vec<usize>>> {
    match state {
        State::None | State::Scalar(Scalar::Value(Value::None)) => Ok(None),
        State::Scalar(Scalar::Value(Value::Number(number))) => Ok(Some(vec![number_to_usize(
            number,
            "tensor reduction axis",
        )?])),
        other => shape_from_state(other).map(Some),
    }
}

fn tensor_dtype_from_state(state: State) -> TCResult<String> {
    match state {
        State::Scalar(Scalar::Value(Value::String(dtype))) => Ok(dtype),
        State::Scalar(Scalar::Value(Value::Link(link))) => Ok(link.to_string()),
        other => Err(TCError::bad_request(format!(
            "expected tensor cast dtype to be a string or link but found {other:?}"
        ))),
    }
}

fn tensor_range_from_state(bounds_state: State, shape: &[usize]) -> TCResult<Range> {
    let bounds = tuple_state_to_items(bounds_state, "tensor slice")?;
    if bounds.len() != shape.len() {
        return Err(TCError::bad_request(format!(
            "tensor slice bounds rank {} does not match tensor rank {}",
            bounds.len(),
            shape.len()
        )));
    }

    let mut range = Range::with_capacity(bounds.len());
    for (axis, (bound, axis_dim)) in bounds.into_iter().zip(shape.iter().copied()).enumerate() {
        range.push(tensor_axis_range_from_state(bound, axis, axis_dim)?);
    }

    Ok(range)
}

fn tensor_axis_range_from_state(bound: State, axis: usize, axis_dim: usize) -> TCResult<AxisRange> {
    match bound {
        State::Scalar(Scalar::Value(Value::Number(number))) => {
            let index = number_to_usize(number, &format!("tensor slice index at axis {axis}"))?;
            if index >= axis_dim {
                return Err(TCError::bad_request(format!(
                    "tensor slice index {index} is out of bounds for axis {axis} with dim {axis_dim}"
                )));
            }

            Ok(AxisRange::At(index))
        }
        State::Tuple(parts) => tensor_axis_range_from_parts(parts, axis, axis_dim),
        State::Scalar(Scalar::Tuple(parts)) => {
            let states = parts.into_iter().map(State::Scalar).collect();
            tensor_axis_range_from_parts(states, axis, axis_dim)
        }
        other => Err(TCError::bad_request(format!(
            "expected tensor slice bound at axis {axis} to be a number or tuple range but found {other:?}"
        ))),
    }
}

fn tensor_axis_range_from_parts(
    parts: Vec<State>,
    axis: usize,
    axis_dim: usize,
) -> TCResult<AxisRange> {
    if parts.is_empty() || parts.len() > 3 {
        return Err(TCError::bad_request(format!(
            "tensor slice range at axis {axis} must have 1 to 3 components"
        )));
    }

    let start = state_to_usize(
        parts[0].clone(),
        &format!("tensor slice start at axis {axis}"),
    )?;
    let stop = if parts.len() >= 2 {
        state_to_usize(
            parts[1].clone(),
            &format!("tensor slice stop at axis {axis}"),
        )?
    } else {
        axis_dim
    };
    let step = if parts.len() >= 3 {
        state_to_usize(
            parts[2].clone(),
            &format!("tensor slice step at axis {axis}"),
        )?
    } else {
        1
    };

    if step == 0 {
        return Err(TCError::bad_request(format!(
            "tensor slice step at axis {axis} must be positive"
        )));
    }
    if start > stop || stop > axis_dim {
        return Err(TCError::bad_request(format!(
            "tensor slice range [{start}, {stop}) is out of bounds for axis {axis} with dim {axis_dim}"
        )));
    }

    Ok(AxisRange::In(start, stop, step))
}

fn state_to_usize(state: State, context: &str) -> TCResult<usize> {
    match state {
        State::Scalar(Scalar::Value(Value::Number(number))) => number_to_usize(number, context),
        other => Err(TCError::bad_request(format!(
            "expected {context} to be a number but found {other:?}"
        ))),
    }
}

fn bool_from_state(state: State, context: &str) -> TCResult<bool> {
    match state {
        State::Scalar(Scalar::Value(Value::Number(number))) => Ok(number.cast_into()),
        other => Err(TCError::bad_request(format!(
            "expected {context} to be a boolean but found {other:?}"
        ))),
    }
}

fn number_to_usize(number: Number, context: &str) -> TCResult<usize> {
    let signed: i64 = number.cast_into();
    if signed < 0 {
        return Err(TCError::bad_request(format!(
            "expected {context} to be non-negative"
        )));
    }

    Ok(signed as usize)
}

fn tensor_truthy_state(tensor: &Tensor, require_all: bool) -> TCResult<State> {
    let values = tensor.values_f64().map_err(TCError::bad_request)?;
    let truthy = if require_all {
        values.iter().all(|v| *v != 0.0)
    } else {
        values.iter().any(|v| *v != 0.0)
    };

    Ok(State::from(Value::Number(Number::Bool(truthy.into()))))
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
                    TCRef::Cond(cond) => {
                        resolve_cond(*cond, &values, &txn, self_link.as_ref()).await?
                    }
                    TCRef::While(while_ref) => {
                        resolve_while(*while_ref, &values, &txn, self_link.as_ref()).await?
                    }
                    TCRef::ForEach(for_each) => {
                        resolve_for_each(*for_each, &values, &txn, self_link.as_ref()).await?
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
                TCRef::Cond(cond) => resolve_cond(*cond, &values, &txn, self_link.as_ref()).await,
                TCRef::While(while_ref) => {
                    resolve_while(*while_ref, &values, &txn, self_link.as_ref()).await
                }
                TCRef::ForEach(for_each) => {
                    resolve_for_each(*for_each, &values, &txn, self_link.as_ref()).await
                }
            },
        }
    })
}

async fn resolve_cond(
    cond_ref: Cond,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<State> {
    let Cond {
        cond,
        then,
        or_else,
    } = cond_ref;

    let cond_state = resolve_scalar(Scalar::from(cond), values, txn, self_link).await?;
    let cond_value = resolve_bool_state(cond_state, values, txn, self_link).await?;
    let branch = if cond_value { then } else { or_else };

    match branch {
        Scalar::Op(op_def) => {
            let params = values_to_params_for_opdef(values, &op_def);
            execute_post_with_self(txn, op_def, params, self_link.cloned()).await
        }
        scalar => resolve_scalar(scalar, values, txn, self_link).await,
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

async fn resolve_for_each(
    for_each: ForEach,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<State> {
    let ForEach {
        items,
        op,
        item_name,
    } = for_each;

    let items = resolve_scalar(items, values, txn, self_link).await?;
    let items = tuple_state_to_items(items, "for_each")?;
    let op_def = resolve_scalar(op, values, txn, self_link)
        .await
        .and_then(state_to_opdef)?;

    let mut last_state: Option<State> = None;
    for item in items {
        let mut params = Map::new();
        params.insert(item_name.clone(), item);
        last_state =
            Some(execute_post_with_self(txn, op_def.clone(), params, self_link.cloned()).await?);
    }

    Ok(last_state.unwrap_or_default())
}

fn while_params(state: State) -> TCResult<Map<State>> {
    let mut params = Map::new();
    let state_id: Id = "state"
        .parse()
        .map_err(|err| TCError::internal(format!("invalid while state id: {err}")))?;
    params.insert(state_id, state);
    Ok(params)
}

fn values_to_params_for_opdef(values: &Arc<HashMap<Id, State>>, opdef: &OpDef) -> Map<State> {
    let mut params = Map::new();
    for id in opdef_free_ids(opdef) {
        if let Some(value) = values.get(&id) {
            params.insert(id, value.clone());
        }
    }
    params
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

fn tuple_state_to_items(state: State, context: &str) -> TCResult<Vec<State>> {
    match state {
        State::Tuple(items) => Ok(items),
        State::Scalar(Scalar::Tuple(items)) => Ok(items.into_iter().map(State::Scalar).collect()),
        State::Map(map) => Ok(map
            .into_iter()
            .map(|(id, _value)| State::Scalar(Scalar::Value(Value::String(id.to_string()))))
            .collect()),
        State::Scalar(Scalar::Map(map)) => Ok(map
            .into_iter()
            .map(|(id, _value)| State::Scalar(Scalar::Value(Value::String(id.to_string()))))
            .collect()),
        other => Err(TCError::bad_request(format!(
            "expected tuple or map for {context} but found {other:?}"
        ))),
    }
}

fn string_state_value(state: &State) -> Option<&str> {
    match state {
        State::Scalar(Scalar::Value(Value::String(value))) => Some(value.as_str()),
        _ => None,
    }
}

fn value_to_render_string(value: Value) -> TCResult<String> {
    match value {
        Value::String(value) => Ok(value),
        Value::Number(value) => Ok(value.to_string()),
        Value::Link(value) => Ok(value.to_string()),
        Value::Map(_) => Err(TCError::bad_request(
            "cannot render map as a string parameter".to_string(),
        )),
        Value::Tuple(_) => Err(TCError::bad_request(
            "cannot render tuple as a string parameter".to_string(),
        )),
        Value::None => Err(TCError::bad_request(
            "cannot render None as a string parameter".to_string(),
        )),
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
