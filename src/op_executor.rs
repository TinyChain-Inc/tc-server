use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use number_general::Number;
use pathlink::Link;
use safecast::CastInto;
use tc_error::{TCError, TCResult};
use tc_ir::{Id, IfRef, Map, NativeClass, OpDef, OpRef, Scalar, Subject, TCRef, While};
use tc_state::State;
use tc_value::Value;

use crate::gateway::RpcGateway;
use crate::op_plan::OpPlan;
struct Queue {
    order: VecDeque<Id>,
    set: HashSet<Id>,
}

impl Queue {
    fn with_capacity(size: usize) -> Self {
        Self {
            order: VecDeque::with_capacity(size),
            set: HashSet::with_capacity(size),
        }
    }

    fn into_iter(self) -> impl Iterator<Item = Id> {
        self.order.into_iter()
    }

    fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    fn len(&self) -> usize {
        self.set.len()
    }

    fn push_back(&mut self, id: Id) {
        if !self.set.contains(&id) {
            self.set.insert(id.clone());
            self.order.push_back(id)
        }
    }

}

pub struct Executor<'a> {
    txn: &'a crate::txn::TxnHandle,
    values: HashMap<Id, State>,
    providers: HashMap<Id, Scalar>,
    plan: OpPlan,
    self_link: Option<Link>,
}

impl<'a> Executor<'a> {
    pub fn new<I, P>(
        txn: &'a crate::txn::TxnHandle,
        data: I,
        providers: P,
    ) -> TCResult<Self>
    where
        I: IntoIterator<Item = (Id, State)>,
        P: IntoIterator<Item = (Id, Scalar)>,
    {
        Self::new_with_self(txn, data, providers, None)
    }

    pub fn new_with_self<I, P>(
        txn: &'a crate::txn::TxnHandle,
        data: I,
        providers: P,
        self_link: Option<Link>,
    ) -> TCResult<Self>
    where
        I: IntoIterator<Item = (Id, State)>,
        P: IntoIterator<Item = (Id, Scalar)>,
    {
        let values: HashMap<Id, State> = data.into_iter().collect();
        let providers: HashMap<Id, Scalar> = providers.into_iter().collect();
        let plan = OpPlan::compile(&providers)?;

        Ok(Self {
            txn,
            values,
            providers,
            plan,
            self_link,
        })
    }

    pub async fn capture(mut self, capture: Id) -> TCResult<State> {
        if self.values.contains_key(&capture) {
            return self
                .values
                .remove(&capture)
                .ok_or_else(|| TCError::not_found(format!("capture {capture}")));
        }

        if !self.providers.contains_key(&capture) {
            return Err(TCError::not_found(format!(
                "missing provider for id {capture}",
            )));
        }

        let required = self.plan.required_set(&capture, &self.providers);

        for level in &self.plan.levels {
            let mut pending = Queue::with_capacity(level.len());

            for id in level {
                if !required.contains(id) || self.values.contains_key(id) {
                    continue;
                }

                let deps = self.plan.deps.get(id).ok_or_else(|| {
                    TCError::not_found(format!("missing dependency info for id {id}"))
                })?;

                for dep in deps {
                    if !self.providers.contains_key(dep) && !self.values.contains_key(dep) {
                        return Err(TCError::not_found(format!(
                            "missing input value for id {dep}",
                        )));
                    }
                }

                pending.push_back(id.clone());
            }

            if pending.is_empty() {
                if self.values.contains_key(&capture) {
                    break;
                }
                continue;
            }

            let mut resolved = HashMap::with_capacity(pending.len());
            {
                let values = Arc::new(self.values.clone());
                let mut futures = FuturesUnordered::new();
                for id in pending.into_iter() {
                    let provider = self
                        .providers
                        .get(&id)
                        .cloned()
                        .ok_or_else(|| {
                            TCError::not_found(format!("missing provider for id {id}"))
                        })?;

                    let values = Arc::clone(&values);
                    let txn = self.txn.clone();
                    let self_link = self.self_link.clone();
                    futures.push(async move {
                        let state = resolve_scalar(provider, &values, &txn, self_link.as_ref()).await;
                        (id, state)
                    });
                }

                while let Some((id, result)) = futures.next().await {
                    match result {
                        Ok(state) => {
                            resolved.insert(id, state);
                        }
                        Err(err) => return Err(err),
                    }
                }
            }

            self.values.extend(resolved);

            if self.values.contains_key(&capture) {
                break;
            }
        }

        if !self.values.contains_key(&capture) {
            return Err(TCError::bad_request(format!(
                "cannot resolve all dependencies of {capture}",
            )));
        }

        self.values
            .remove(&capture)
            .ok_or_else(|| TCError::not_found(format!("capture {capture}")))
    }
}

fn resolve_scalar<'a>(
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
                TCRef::While(while_ref) => {
                    resolve_while(*while_ref, values, txn, self_link).await
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
            OpRef::Get((subject, key)) => {
                match subject {
                    Subject::Link(_) | Subject::Ref(_, _) => {
                        let link = resolve_subject(subject, self_link.as_ref())?;
                        let key = scalar_to_value(key, &values, &txn, self_link.as_ref()).await?;
                        txn.get(link, txn.clone(), key).await
                    }
                }
            }
            OpRef::Put((subject, key, value)) => {
                match subject {
                    Subject::Link(_) | Subject::Ref(_, _) => {
                        let link = resolve_subject(subject, self_link.as_ref())?;
                        let key = scalar_to_value(key, &values, &txn, self_link.as_ref()).await?;
                        let value = scalar_to_state(value, &values, &txn, self_link.as_ref()).await?;
                        txn.put(link, txn.clone(), key, value)
                            .await
                            .map(|()| State::default())
                    }
                }
            }
            OpRef::Post((subject, params)) => {
                match subject {
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
                            let link = resolve_subject(Subject::Ref(id_ref, suffix), self_link.as_ref())?;
                            let params =
                                resolve_params(params, &values, &txn, self_link.as_ref()).await?;
                            return txn.post(link, txn.clone(), params).await;
                        }
                        let segments = suffix.as_ref();
                        if segments.len() == 1 && segments[0].as_str() == "add" {
                            let left = values
                                .get(id_ref.as_str())
                                .cloned()
                                .ok_or_else(|| TCError::not_found(format!("unknown id ${}", id_ref.as_str())))?;

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
                            let r_value = scalar_to_value(r_value.clone(), &values, &txn, self_link.as_ref()).await?;
                            let Value::Number(right) = r_value else {
                                return Err(TCError::bad_request(
                                    "expected add parameter r to be a number".to_string(),
                                ));
                            };

                            Ok(State::from(Value::Number(left + right)))
                        } else if segments.len() == 1 && segments[0].as_str() == "eq" {
                            let left = values
                                .get(id_ref.as_str())
                                .cloned()
                                .ok_or_else(|| TCError::not_found(format!("unknown id ${}", id_ref.as_str())))?;

                            let State::Scalar(Scalar::Value(left)) = left else {
                                return Err(TCError::bad_request(
                                    "expected eq subject to be a scalar value".to_string(),
                                ));
                            };

                            let r_id: Id = "r".parse().expect("Id");
                            let Some(r_value) = params.get(&r_id) else {
                                return Err(TCError::bad_request(
                                    "missing eq parameter r".to_string(),
                                ));
                            };
                            let right = scalar_to_value(
                                r_value.clone(),
                                &values,
                                &txn,
                                self_link.as_ref(),
                            )
                            .await?;

                            let is_equal = match (&left, &right) {
                                (Value::Link(l), Value::String(r)) => l.to_string() == *r,
                                (Value::String(l), Value::Link(r)) => *l == r.to_string(),
                                _ => left == right,
                            };

                            Ok(State::from(Value::Number(Number::from(is_equal))))
                        } else if segments.len() == 1 && segments[0].as_str() == "gt" {
                            let left = values
                                .get(id_ref.as_str())
                                .cloned()
                                .ok_or_else(|| TCError::not_found(format!("unknown id ${}", id_ref.as_str())))?;

                            let State::Scalar(Scalar::Value(Value::Number(left))) = left else {
                                return Err(TCError::bad_request(
                                    "expected gt subject to be a number".to_string(),
                                ));
                            };

                            let r_id: Id = "r".parse().expect("Id");
                            let Some(r_value) = params.get(&r_id) else {
                                return Err(TCError::bad_request(
                                    "missing gt parameter r".to_string(),
                                ));
                            };
                            let r_value = scalar_to_value(r_value.clone(), &values, &txn, self_link.as_ref()).await?;
                            let Value::Number(right) = r_value else {
                                return Err(TCError::bad_request(
                                    "expected gt parameter r to be a number".to_string(),
                                ));
                            };

                            Ok(State::from(Value::Number(Number::from(left > right))))
                        } else if segments.len() == 1 && segments[0].as_str() == "len" {
                            let tuple_state = values
                                .get(id_ref.as_str())
                                .cloned()
                                .ok_or_else(|| TCError::not_found(format!("unknown id ${}", id_ref.as_str())))?;
                            let items = tuple_state_to_items(tuple_state)?;
                            Ok(State::from(Value::Number(Number::from(items.len() as u64))))
                        } else if segments.len() == 1 && segments[0].as_str() == "head" {
                            let tuple_state = values
                                .get(id_ref.as_str())
                                .cloned()
                                .ok_or_else(|| TCError::not_found(format!("unknown id ${}", id_ref.as_str())))?;
                            let mut items = tuple_state_to_items(tuple_state)?;
                            let head = items
                                .drain(..1)
                                .next()
                                .ok_or_else(|| TCError::bad_request("cannot take head of empty tuple".to_string()))?;
                            let head = state_to_scalar(head)?;
                            Ok(State::Scalar(head))
                        } else if segments.len() == 1 && segments[0].as_str() == "tail" {
                            let tuple_state = values
                                .get(id_ref.as_str())
                                .cloned()
                                .ok_or_else(|| TCError::not_found(format!("unknown id ${}", id_ref.as_str())))?;
                            let items = tuple_state_to_items(tuple_state)?;
                            let tail = items
                                .into_iter()
                                .skip(1)
                                .map(state_to_scalar)
                                .collect::<TCResult<Vec<_>>>()?;
                            Ok(State::Scalar(Scalar::Tuple(tail)))
                        } else if segments.len() == 1 && segments[0].as_str() == "concat" {
                            let left_state = values
                                .get(id_ref.as_str())
                                .cloned()
                                .ok_or_else(|| TCError::not_found(format!("unknown id ${}", id_ref.as_str())))?;
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
                            let tuple_state = values
                                .get(id_ref.as_str())
                                .cloned()
                                .ok_or_else(|| TCError::not_found(format!("unknown id ${}", id_ref.as_str())))?;
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
                            let tuple_state = values
                                .get(id_ref.as_str())
                                .cloned()
                                .ok_or_else(|| TCError::not_found(format!("unknown id ${}", id_ref.as_str())))?;
                            let items = tuple_state_to_items(tuple_state)?;

                            let item_name = param_id(&params, "item_name")?;
                            let op_def = param_opdef(
                                &params,
                                "op",
                                &values,
                                &txn,
                                self_link.as_ref(),
                            )
                            .await?;
                            let mut state = param_state(
                                &params,
                                "value",
                                &values,
                                &txn,
                                self_link.as_ref(),
                            )
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
                }
            }
            OpRef::Delete((subject, key)) => {
                match subject {
                    Subject::Link(_) | Subject::Ref(_, _) => {
                        let link = resolve_subject(subject, self_link.as_ref())?;
                        let key = scalar_to_value(key, &values, &txn, self_link.as_ref()).await?;
                        txn.delete(link, txn.clone(), key)
                            .await
                            .map(|()| State::default())
                    }
                }
            }
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
                    TCRef::Id(id_ref) => values
                        .get(id_ref.as_str())
                        .cloned()
                        .ok_or_else(|| {
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
                TCRef::If(if_ref) => {
                    resolve_if(*if_ref, &values, &txn, self_link.as_ref()).await
                }
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
    let IfRef { cond, then, or_else } = if_ref;
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
    let While { cond, closure, state } = while_ref;

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
    let state_id: Id = "state".parse().map_err(|err| {
        TCError::internal(format!("invalid while state id: {err}"))
    })?;
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
        State::Scalar(Scalar::Tuple(items)) => Ok(items
            .into_iter()
            .map(State::Scalar)
            .collect()),
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
            let id: Id = "state".parse().map_err(|err| {
                TCError::internal(format!("invalid state id: {err}"))
            })?;
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
    let id: Id = name.parse().map_err(|err| {
        TCError::internal(format!("invalid {name} id: {err}"))
    })?;
    let Some(value) = params.get(&id) else {
        return Err(TCError::bad_request(format!(
            "missing {name} parameter"
        )));
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
    let id: Id = name.parse().map_err(|err| {
        TCError::internal(format!("invalid {name} id: {err}"))
    })?;
    let Some(value) = params.get(&id) else {
        return Err(TCError::bad_request(format!(
            "missing {name} parameter"
        )));
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
    let id: Id = name.parse().map_err(|err| {
        TCError::internal(format!("invalid {name} id: {err}"))
    })?;
    let Some(value) = params.get(&id) else {
        return Err(TCError::bad_request(format!(
            "missing {name} parameter"
        )));
    };
    scalar_to_state(value.clone(), values, txn, self_link).await
}

enum ReflectKind {
    ScalarClass,
    ScalarIfParts,
    OpDefForm,
    OpDefLastId,
    OpDefScalars,
}

fn reflect_kind(link: &Link) -> Option<ReflectKind> {
    let normalized = link.path().to_string();
    match normalized.trim_start_matches('/') {
        "state/scalar/reflect/class" => Some(ReflectKind::ScalarClass),
        "state/scalar/reflect/if_parts" => Some(ReflectKind::ScalarIfParts),
        "state/scalar/op/reflect/form" => Some(ReflectKind::OpDefForm),
        "state/scalar/op/reflect/last_id" => Some(ReflectKind::OpDefLastId),
        "state/scalar/op/reflect/scalars" => Some(ReflectKind::OpDefScalars),
        _ => None,
    }
}

fn extract_scalar_param(params: &Map<Scalar>) -> TCResult<Scalar> {
    let scalar_id: Id = "scalar".parse().map_err(|err| {
        TCError::internal(format!("invalid scalar id: {err}"))
    })?;
    let op_id: Id = "op".parse().map_err(|err| {
        TCError::internal(format!("invalid op id: {err}"))
    })?;
    params
        .get(&scalar_id)
        .or_else(|| params.get(&op_id))
        .cloned()
        .ok_or_else(|| TCError::bad_request("missing scalar parameter".to_string()))
}

fn opdef_from_scalar_param(scalar: &Scalar) -> TCResult<OpDef> {
    match scalar {
        Scalar::Op(opdef) => Ok(opdef.clone()),
        _ => Err(TCError::bad_request(
            "expected OpDef scalar parameter".to_string(),
        )),
    }
}

fn class_from_value(value: &Value) -> Link {
    let path = value.class().path().to_string();
    Link::from_str(&path).expect("value class link")
}

fn class_from_opdef(opdef: &OpDef) -> Link {
    let path = match opdef {
        OpDef::Get(_) => pathlink::PathBuf::from(tc_ir::OPDEF_GET).to_string(),
        OpDef::Put(_) => pathlink::PathBuf::from(tc_ir::OPDEF_PUT).to_string(),
        OpDef::Post(_) => pathlink::PathBuf::from(tc_ir::OPDEF_POST).to_string(),
        OpDef::Delete(_) => pathlink::PathBuf::from(tc_ir::OPDEF_DELETE).to_string(),
    };
    Link::from_str(&path).expect("opdef class link")
}

fn class_from_tcref(tc_ref: &TCRef) -> Link {
    let path = match tc_ref {
        TCRef::If(_) => pathlink::PathBuf::from(tc_ir::TCREF_IF).to_string(),
        TCRef::While(_) => pathlink::PathBuf::from(tc_ir::TCREF_WHILE).to_string(),
        TCRef::Id(_) => pathlink::PathBuf::from(tc_ir::SCALAR_REF_PREFIX).to_string(),
        TCRef::Op(opref) => match opref {
            OpRef::Get(_) => pathlink::PathBuf::from(tc_ir::OPREF_GET).to_string(),
            OpRef::Put(_) => pathlink::PathBuf::from(tc_ir::OPREF_PUT).to_string(),
            OpRef::Post(_) => pathlink::PathBuf::from(tc_ir::OPREF_POST).to_string(),
            OpRef::Delete(_) => pathlink::PathBuf::from(tc_ir::OPREF_DELETE).to_string(),
        },
    };
    Link::from_str(&path).expect("tcref class link")
}

fn reflect_link(
    link: &Link,
    params: &Map<Scalar>,
    values: &HashMap<Id, State>,
) -> TCResult<Option<State>> {
    let Some(kind) = reflect_kind(link) else {
        return Ok(None);
    };

    let scalar = extract_scalar_param(params)?;
    let scalar = resolve_reflect_param(scalar, values)?;

    let state = match kind {
        ReflectKind::ScalarClass => {
            let class = match &scalar {
                Scalar::Value(value) => class_from_value(value),
                Scalar::Op(opdef) => class_from_opdef(opdef),
                Scalar::Ref(r) => class_from_tcref(r.as_ref()),
                Scalar::Map(_) => {
                    Link::from_str(&pathlink::PathBuf::from(tc_ir::SCALAR_MAP).to_string())
                        .expect("scalar map class")
                }
                Scalar::Tuple(_) => {
                    Link::from_str(&pathlink::PathBuf::from(tc_ir::SCALAR_TUPLE).to_string())
                        .expect("scalar tuple class")
                }
            };
            State::from(Value::Link(class))
        }
        ReflectKind::ScalarIfParts => {
            let Scalar::Ref(r) = &scalar else {
                return Ok(Some(State::Scalar(Scalar::Tuple(vec![]))));
            };
            let TCRef::If(if_ref) = r.as_ref() else {
                return Ok(Some(State::Scalar(Scalar::Tuple(vec![]))));
            };
            let parts = Scalar::Tuple(vec![
                Scalar::from(if_ref.cond.clone()),
                if_ref.then.clone(),
                if_ref.or_else.clone(),
            ]);
            State::Scalar(parts)
        }
        ReflectKind::OpDefForm => {
            let opdef = opdef_from_scalar_param(&scalar)?;
            let form = opdef
                .form()
                .iter()
                .map(|(id, scalar)| {
                    Scalar::Tuple(vec![
                        Scalar::Value(Value::String(id.to_string())),
                        scalar.clone(),
                    ])
                })
                .collect();
            State::Scalar(Scalar::Tuple(form))
        }
        ReflectKind::OpDefLastId => {
            let opdef = opdef_from_scalar_param(&scalar)?;
            let value = match opdef.last_id() {
                Some(id) => Value::String(id.to_string()),
                None => Value::None,
            };
            State::Scalar(Scalar::Value(value))
        }
        ReflectKind::OpDefScalars => {
            let opdef = opdef_from_scalar_param(&scalar)?;
            let scalars = opdef.walk_scalars().cloned().collect();
            State::Scalar(Scalar::Tuple(scalars))
        }
    };

    Ok(Some(state))
}

fn resolve_reflect_param(scalar: Scalar, values: &HashMap<Id, State>) -> TCResult<Scalar> {
    match &scalar {
        Scalar::Ref(r) => match r.as_ref() {
            TCRef::Id(id_ref) => values
                .get(id_ref.as_str())
                .cloned()
                .ok_or_else(|| TCError::not_found(format!("unknown id ${}", id_ref.as_str())))
                .and_then(state_to_scalar_for_reflect),
            _ => Ok(scalar),
        },
        _ => Ok(scalar),
    }
}

fn state_to_scalar_for_reflect(state: State) -> TCResult<Scalar> {
    match state {
        State::None => Ok(Scalar::Value(Value::None)),
        State::Scalar(scalar) => Ok(scalar),
        State::Map(map) => {
            let mut out = Map::new();
            for (id, value) in map {
                out.insert(id, state_to_scalar_for_reflect(value)?);
            }
            Ok(Scalar::Map(out))
        }
        State::Tuple(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(state_to_scalar_for_reflect(item)?);
            }
            Ok(Scalar::Tuple(out))
        }
        other => Err(TCError::bad_request(format!(
            "expected scalar state while resolving op; found {other:?}"
        ))),
    }
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
                ))
            }
            State::Scalar(other) => {
                return Err(TCError::bad_request(format!(
                    "expected condition to be a scalar boolean; found {other:?}"
                )))
            }
            other => {
                return Err(TCError::bad_request(format!(
                    "expected condition to be a scalar boolean; found {other:?}"
                )))
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

fn state_to_scalar(state: State) -> TCResult<Scalar> {
    match state {
        State::None => Ok(Scalar::Value(Value::None)),
        State::Scalar(scalar) => Ok(scalar),
        _ => Err(TCError::bad_request(
            "expected scalar state while resolving op".to_string(),
        )),
    }
}

pub async fn execute_get(
    txn: &crate::txn::TxnHandle,
    op: OpDef,
    key: Value,
) -> TCResult<State> {
    execute_get_with_self(txn, op, key, None).await
}

pub async fn execute_get_with_self(
    txn: &crate::txn::TxnHandle,
    op: OpDef,
    key: Value,
    self_link: Option<Link>,
) -> TCResult<State> {
    let OpDef::Get((key_name, form)) = op else {
        return Err(TCError::bad_request("expected GET op definition".to_string()));
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
        return Err(TCError::bad_request("expected PUT op definition".to_string()));
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
        return Err(TCError::bad_request("expected POST op definition".to_string()));
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

pub async fn execute_delete(
    txn: &crate::txn::TxnHandle,
    op: OpDef,
    key: Value,
) -> TCResult<()> {
    execute_delete_with_self(txn, op, key, None).await
}

pub async fn execute_delete_with_self(
    txn: &crate::txn::TxnHandle,
    op: OpDef,
    key: Value,
    self_link: Option<Link>,
) -> TCResult<()> {
    let OpDef::Delete((key_name, form)) = op else {
        return Err(TCError::bad_request("expected DELETE op definition".to_string()));
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

#[cfg(test)]
mod tests {
    use super::*;
    use number_general::Number;
    use tc_ir::TCRef;

    #[tokio::test]
    async fn executes_post_opdef_with_id_ref() {
        let form = vec![
            ("a".parse().expect("Id"), Scalar::from(Value::from(1_u64))),
            (
                "b".parse().expect("Id"),
                Scalar::Ref(Box::new(TCRef::Id("$a".parse().expect("IdRef")))),
            ),
        ];
        let op = OpDef::Post(form);

        let txn = crate::txn::TxnManager::with_host_id("test-host").begin();
        let result = execute_post(&txn, op, Map::new()).await.expect("exec");

        match result {
            State::Scalar(Scalar::Value(Value::Number(n))) => {
                assert_eq!(n, Number::from(1_u64));
            }
            other => panic!("unexpected result {other:?}"),
        }
    }
}
