use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use crate::State;
use futures::stream::{self, StreamExt};
use tc_error::{TCError, TCResult};
use tc_ir::{Id, Scalar};
use tc_state::resolve;

pub(super) async fn resolve_with_admission(
    provider: Scalar,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_state: Option<&State>,
) -> TCResult<State> {
    if txn.graph_admitted() {
        return resolve(provider, values, txn, self_state).await;
    }

    let _permit = txn.resources().admit_graph_op(txn.deadline()).await?;
    let txn = txn.with_graph_admission();
    resolve(provider, values, &txn, self_state).await
}

pub(crate) struct Executor<'a> {
    txn: &'a crate::txn::TxnHandle,
    resolved: Arc<HashMap<Id, State>>,
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
            resolved: Arc::new(resolved),
            bindings,
            self_state,
        })
    }

    pub(crate) async fn capture(mut self, capture: Id) -> TCResult<State> {
        if self.resolved.contains_key(&capture) {
            return self
                .resolved_mut()
                .remove(&capture)
                .ok_or_else(|| TCError::not_found(format!("capture {capture}")));
        }

        if !self.bindings.contains_key(&capture) {
            return Err(TCError::bad_request(format!(
                "missing provider for id {capture}"
            )));
        }

        let limits = &self.txn.resources().limits().execution;
        let mut schedule = Schedule::build(
            &capture,
            &self.bindings,
            &self.resolved,
            limits.max_graph_nodes,
            limits.max_graph_edges,
        )?;

        while !self.resolved.contains_key(&capture) {
            let pending = std::mem::take(&mut schedule.ready)
                .into_iter()
                .collect::<Vec<_>>();

            if pending.is_empty() {
                return Err(TCError::bad_request(format!(
                    "cannot resolve cyclic dependencies of {capture}"
                )));
            }

            let mut resolved = HashMap::with_capacity(pending.len());
            {
                let values = Arc::clone(&self.resolved);
                let pending = pending
                    .into_iter()
                    .map(|id| {
                        let provider = self.bindings[&id].clone();
                        (id, provider)
                    })
                    .collect::<Vec<_>>();
                let limit = if self.txn.graph_admitted() {
                    1
                } else {
                    self.txn.resources().limits().execution.parallel_graph_ops
                };
                let mut futures = stream::iter(pending)
                    .map(|(id, provider)| {
                        let values = Arc::clone(&values);
                        let txn = self.txn.clone();
                        let self_state = self.self_state.clone();
                        async move {
                            let state = resolve_with_admission(
                                provider,
                                &values,
                                &txn,
                                self_state.as_ref(),
                            )
                            .await;
                            (id, state)
                        }
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
            self.resolved_mut().extend(resolved);
        }

        self.resolved_mut()
            .remove(&capture)
            .ok_or_else(|| TCError::not_found(format!("capture {capture}")))
    }

    fn resolved_mut(&mut self) -> &mut HashMap<Id, State> {
        Arc::make_mut(&mut self.resolved)
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
mod tests {
    use super::*;
    use tc_ir::{IdRef, TCRef};

    fn dependency(id: &str) -> Scalar {
        Scalar::from(TCRef::Id(id.parse::<IdRef>().expect("IdRef")))
    }

    #[test]
    fn provider_discovery_is_transitive_and_ordered() {
        let bindings = BTreeMap::from([
            ("a".parse().unwrap(), dependency("$b")),
            ("b".parse().unwrap(), Scalar::from(1_u64)),
        ]);
        let schedule =
            Schedule::build(&"a".parse().unwrap(), &bindings, &HashMap::new(), 10, 10).unwrap();
        assert_eq!(
            schedule
                .indegree
                .into_keys()
                .map(|id| id.to_string())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
    }

    #[test]
    fn provider_discovery_rejects_missing_inputs_and_cycles() {
        let missing = BTreeMap::from([("a".parse().unwrap(), dependency("$missing"))]);
        let error =
            Schedule::build(&"a".parse().unwrap(), &missing, &HashMap::new(), 10, 10).unwrap_err();
        assert!(error.to_string().contains("missing input value"));

        let cyclic = BTreeMap::from([
            ("a".parse().unwrap(), dependency("$b")),
            ("b".parse().unwrap(), dependency("$a")),
        ]);
        let schedule =
            Schedule::build(&"a".parse().unwrap(), &cyclic, &HashMap::new(), 10, 10).unwrap();
        assert!(schedule.ready.is_empty());
    }

    #[test]
    fn scheduler_bounds_edges_and_handles_a_deep_chain_iteratively() {
        let fan_in = BTreeMap::from([
            (
                "result".parse().unwrap(),
                Scalar::Tuple(vec![dependency("$left"), dependency("$right")]),
            ),
            ("left".parse().unwrap(), Scalar::from(1_u64)),
            ("right".parse().unwrap(), Scalar::from(2_u64)),
        ]);
        let error = Schedule::build(&"result".parse().unwrap(), &fan_in, &HashMap::new(), 10, 1)
            .unwrap_err();
        assert!(error.to_string().contains("1-edge limit"));

        let mut chain = BTreeMap::new();
        chain.insert("n0000".parse().unwrap(), Scalar::from(0_u64));
        for index in 1..4_096 {
            let id: Id = format!("n{index:04}").parse().unwrap();
            chain.insert(id, dependency(&format!("$n{:04}", index - 1)));
        }
        let schedule = Schedule::build(
            &"n4095".parse().unwrap(),
            &chain,
            &HashMap::new(),
            4_096,
            4_095,
        )
        .expect("iterative chain planning");
        assert_eq!(schedule.indegree.len(), 4_096);
        assert_eq!(schedule.ready.len(), 1);
    }
}
