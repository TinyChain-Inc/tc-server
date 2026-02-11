use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use futures::stream::{FuturesUnordered, StreamExt};
use pathlink::Link;
use tc_error::{TCError, TCResult};
use tc_ir::{Id, Scalar};
use tc_state::State;

use crate::op_plan::OpPlan;

use super::resolve::resolve_scalar;

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
    pub fn new<I, P>(txn: &'a crate::txn::TxnHandle, data: I, providers: P) -> TCResult<Self>
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
                    let provider = self.providers.get(&id).cloned().ok_or_else(|| {
                        TCError::not_found(format!("missing provider for id {id}"))
                    })?;

                    let values = Arc::clone(&values);
                    let txn = self.txn.clone();
                    let self_link = self.self_link.clone();
                    futures.push(async move {
                        let state =
                            resolve_scalar(provider, &values, &txn, self_link.as_ref()).await;
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
