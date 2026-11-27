use std::collections::{BTreeMap, BTreeSet, HashMap};

use futures::stream::{self, StreamExt};
use tc_error::{TCError, TCResult};
use tc_ir::{Id, Scalar};
use tc_state::resolve;

use crate::State;

pub(super) async fn resolve_with_admission(
    provider: Scalar,
    values: &HashMap<Id, State>,
    txn: &crate::txn::TxnHandle,
    self_state: Option<&State>,
) -> TCResult<State> {
    if txn.graph_admitted() {
        return resolve(provider, values, txn, self_state).await;
    }

    let _permit = txn.admit_graph_op().await?;
    let txn = txn.with_graph_admission();
    resolve(provider, values, &txn, self_state).await
}

pub(crate) struct Executor<'a> {
    txn: &'a crate::txn::TxnHandle,
    resolved: HashMap<Id, State>,
    bindings: BTreeMap<Id, Scalar>,
    self_state: Option<State>,
}

#[derive(Debug)]
struct Schedule {
    dependents: BTreeMap<Id, BTreeSet<Id>>,
    indegree: BTreeMap<Id, usize>,
    ready: BTreeSet<Id>,
}

impl<'a> Executor<'a> {
    pub(crate) fn new_with_self<I, P>(
        txn: &'a crate::txn::TxnHandle,
        data: I,
        providers: P,
        self_state: Option<State>,
    ) -> TCResult<Self>
    where
        I: IntoIterator<Item = (Id, State)>,
        P: IntoIterator<Item = (Id, Scalar)>,
    {
        let mut resolved = HashMap::new();
        for (id, value) in data {
            if resolved.insert(id.clone(), value).is_some() {
                return Err(TCError::bad_request(format!(
                    "duplicate input binding ${id}"
                )));
            }
        }

        let mut bindings = BTreeMap::new();
        for (id, provider) in providers {
            if resolved.contains_key(&id) || bindings.insert(id.clone(), provider).is_some() {
                return Err(TCError::bad_request(format!(
                    "duplicate graph binding ${id}"
                )));
            }
        }

        Ok(Self {
            txn,
            resolved,
            bindings,
            self_state,
        })
    }

    pub(crate) async fn capture(mut self, capture: Id) -> TCResult<State> {
        if self.resolved.contains_key(&capture) {
            return self
                .resolved
                .remove(&capture)
                .ok_or_else(|| TCError::not_found(format!("capture {capture}")));
        }

        if !self.bindings.contains_key(&capture) {
            return Err(TCError::bad_request(format!(
                "missing provider for id {capture}"
            )));
        }

        let limits = self.txn.execution_limits();
        let mut schedule = Schedule::build(
            &capture,
            &self.bindings,
            &self.resolved,
            limits.max_graph_nodes,
            limits.max_graph_edges,
        )?;

        while !self.resolved.contains_key(&capture) {
            let pending = std::mem::take(&mut schedule.ready);

            if pending.is_empty() {
                return Err(TCError::bad_request(format!(
                    "cannot resolve cyclic dependencies of {capture}"
                )));
            }

            let mut resolved = HashMap::with_capacity(pending.len());
            {
                let values = &self.resolved;
                let txn = self.txn;
                let self_state = self.self_state.as_ref();
                let pending = pending
                    .into_iter()
                    .map(|id| {
                        let provider = self
                            .bindings
                            .remove(&id)
                            .expect("scheduled provider exists");

                        (id, provider)
                    })
                    .collect::<Vec<_>>();

                let limit = if self.txn.graph_admitted() {
                    1
                } else {
                    limits.parallel_graph_ops
                };
                let mut futures = stream::iter(pending)
                    .map(|(id, provider)| async move {
                        let state = resolve_with_admission(provider, values, txn, self_state).await;
                        (id, state)
                    })
                    .buffered(limit);

                while let Some((id, result)) = futures.next().await {
                    match result {
                        Ok(state) => {
                            resolved.insert(id, state);
                        }
                        Err(err) => return Err(err),
                    }
                }
            }

            for id in resolved.keys() {
                schedule.complete(id);
            }

            self.resolved.extend(resolved);
        }

        self.resolved
            .remove(&capture)
            .ok_or_else(|| TCError::not_found(format!("capture {capture}")))
    }
}

impl Schedule {
    fn build(
        capture: &Id,
        bindings: &BTreeMap<Id, Scalar>,
        resolved: &HashMap<Id, State>,
        max_nodes: usize,
        max_edges: usize,
    ) -> TCResult<Self> {
        let mut required = BTreeSet::new();
        let mut pending = vec![capture.clone()];
        let mut dependents = BTreeMap::<Id, BTreeSet<Id>>::new();
        let mut indegree = BTreeMap::new();
        let mut ready = BTreeSet::new();
        let mut edge_count = 0usize;

        while let Some(id) = pending.pop() {
            if resolved.contains_key(&id) || !required.insert(id.clone()) {
                continue;
            }
            if required.len() > max_nodes {
                return Err(TCError::bad_request(format!(
                    "operation graph exceeds the {max_nodes}-provider limit"
                )));
            }
            let provider = bindings
                .get(&id)
                .ok_or_else(|| TCError::bad_request(format!("missing input value for id {id}")))?;
            let mut provider_dependencies = BTreeSet::new();
            provider.requires(&mut provider_dependencies);
            let mut unresolved = 0usize;
            for dependency in provider_dependencies.into_iter().rev() {
                edge_count = edge_count
                    .checked_add(1)
                    .ok_or_else(|| TCError::bad_request("operation graph edge count overflow"))?;
                if edge_count > max_edges {
                    return Err(TCError::bad_request(format!(
                        "operation graph exceeds the {max_edges}-edge limit"
                    )));
                }
                if resolved.contains_key(&dependency) {
                    continue;
                }
                unresolved += 1;
                dependents
                    .entry(dependency.clone())
                    .or_default()
                    .insert(id.clone());
                pending.push(dependency);
            }
            indegree.insert(id.clone(), unresolved);
            if unresolved == 0 {
                ready.insert(id);
            }
        }

        Ok(Self {
            dependents,
            indegree,
            ready,
        })
    }

    fn complete(&mut self, id: &Id) {
        if let Some(dependents) = self.dependents.get(id) {
            for dependent in dependents {
                let remaining = self
                    .indegree
                    .get_mut(dependent)
                    .expect("reachable dependent has an indegree");
                *remaining -= 1;
                if *remaining == 0 {
                    self.ready.insert(dependent.clone());
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/support/op_executor_schedule.rs"]
mod tests;
