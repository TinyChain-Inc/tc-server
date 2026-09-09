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

pub struct Executor<'a> {
    txn: &'a crate::txn::TxnHandle,
    resolved: HashMap<Id, State>,
    bindings: BTreeMap<Id, Scalar>,
    self_state: Option<State>,
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

    pub async fn capture(mut self, capture: Id) -> TCResult<State> {
        if self.resolved.contains_key(&capture) {
            return self
                .resolved
                .remove(&capture)
                .ok_or_else(|| TCError::not_found(format!("capture {capture}")));
        }

        if !self.bindings.contains_key(&capture) {
            return Err(TCError::not_found(format!(
                "missing provider for id {capture}",
            )));
        }

        let required = required_bindings(&capture, &self.bindings, &self.resolved)?;

        while !self.resolved.contains_key(&capture) {
            let pending = required
                .iter()
                .filter(|id| !self.resolved.contains_key(*id))
                .filter(|id| {
                    let mut required = BTreeSet::new();
                    self.bindings[*id].requires(&mut required);
                    required.iter().all(|dep| self.resolved.contains_key(dep))
                })
                .cloned()
                .collect::<Vec<_>>();

            if pending.is_empty() {
                return Err(TCError::bad_request(format!(
                    "cannot resolve cyclic dependencies of {capture}",
                )));
            }

            let mut resolved = HashMap::with_capacity(pending.len());
            {
                let values = Arc::new(self.resolved.clone());
                let pending = pending
                    .into_iter()
                    .map(|id| {
                        let provider = self.bindings.get(&id).cloned().ok_or_else(|| {
                            TCError::not_found(format!("missing provider for id {id}"))
                        })?;
                        Ok((id, provider))
                    })
                    .collect::<TCResult<Vec<_>>>()?;
                let limit = self.txn.resources().limits().execution.parallel_graph_ops;
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
                    .buffer_unordered(limit);

                while let Some((id, result)) = futures.next().await {
                    match result {
                        Ok(state) => {
                            resolved.insert(id, state);
                        }
                        Err(err) => return Err(err),
                    }
                }
            }

            self.resolved.extend(resolved);
        }

        if !self.resolved.contains_key(&capture) {
            return Err(TCError::bad_request(format!(
                "cannot resolve all dependencies of {capture}",
            )));
        }

        self.resolved
            .remove(&capture)
            .ok_or_else(|| TCError::not_found(format!("capture {capture}")))
    }
}

fn required_bindings(
    capture: &Id,
    bindings: &BTreeMap<Id, Scalar>,
    resolved: &HashMap<Id, State>,
) -> TCResult<BTreeSet<Id>> {
    fn visit(
        id: &Id,
        bindings: &BTreeMap<Id, Scalar>,
        resolved: &HashMap<Id, State>,
        visiting: &mut BTreeSet<Id>,
        required: &mut BTreeSet<Id>,
    ) -> TCResult<()> {
        if resolved.contains_key(id) || required.contains(id) {
            return Ok(());
        }
        let provider = bindings
            .get(id)
            .ok_or_else(|| TCError::not_found(format!("missing input value for id {id}")))?;
        if !visiting.insert(id.clone()) {
            return Err(TCError::bad_request(format!(
                "cyclic provider dependency at {id}"
            )));
        }
        let mut dependencies = BTreeSet::new();
        provider.requires(&mut dependencies);
        for dependency in dependencies {
            visit(&dependency, bindings, resolved, visiting, required)?;
        }
        visiting.remove(id);
        required.insert(id.clone());
        Ok(())
    }

    let mut required = BTreeSet::new();
    visit(
        capture,
        bindings,
        resolved,
        &mut BTreeSet::new(),
        &mut required,
    )?;
    Ok(required)
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
        let required =
            required_bindings(&"a".parse().unwrap(), &bindings, &HashMap::new()).unwrap();
        assert_eq!(
            required
                .into_iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
    }

    #[test]
    fn provider_discovery_rejects_missing_inputs_and_cycles() {
        let missing = BTreeMap::from([("a".parse().unwrap(), dependency("$missing"))]);
        let error =
            required_bindings(&"a".parse().unwrap(), &missing, &HashMap::new()).unwrap_err();
        assert!(error.to_string().contains("missing input value"));

        let cyclic = BTreeMap::from([
            ("a".parse().unwrap(), dependency("$b")),
            ("b".parse().unwrap(), dependency("$a")),
        ]);
        let error = required_bindings(&"a".parse().unwrap(), &cyclic, &HashMap::new()).unwrap_err();
        assert!(error.to_string().contains("cyclic provider dependency"));
    }
}
