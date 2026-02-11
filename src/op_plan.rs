use std::collections::{HashMap, HashSet};

use tc_error::{TCError, TCResult};
use tc_ir::{Id, OpRef, Scalar, Subject, TCRef};

#[derive(Clone)]
pub struct OpPlan {
    pub levels: Vec<Vec<Id>>,
    pub deps: HashMap<Id, HashSet<Id>>,
}

impl OpPlan {
    pub fn compile(providers: &HashMap<Id, Scalar>) -> TCResult<Self> {
        let mut deps = HashMap::new();
        for (id, provider) in providers {
            let mut node_deps = HashSet::new();
            scalar_requires(provider, &mut node_deps);
            deps.insert(id.clone(), node_deps);
        }

        let mut indegree: HashMap<Id, usize> = HashMap::with_capacity(providers.len());
        let mut reverse: HashMap<Id, Vec<Id>> = HashMap::new();

        for id in providers.keys() {
            indegree.entry(id.clone()).or_insert(0);
        }

        for (id, node_deps) in &deps {
            for dep in node_deps {
                if providers.contains_key(dep) {
                    *indegree.entry(id.clone()).or_insert(0) += 1;
                    reverse.entry(dep.clone()).or_default().push(id.clone());
                }
            }
        }

        let mut ready: Vec<Id> = indegree
            .iter()
            .filter_map(|(id, &deg)| (deg == 0).then_some(id.clone()))
            .collect();

        let mut levels = Vec::new();
        let mut processed = 0usize;

        while !ready.is_empty() {
            let mut next_ready = Vec::new();
            let mut level = Vec::new();

            for id in ready {
                processed += 1;
                level.push(id.clone());

                if let Some(children) = reverse.get(&id) {
                    for child in children {
                        if let Some(deg) = indegree.get_mut(child) {
                            *deg = deg.saturating_sub(1);
                            if *deg == 0 {
                                next_ready.push(child.clone());
                            }
                        }
                    }
                }
            }

            levels.push(level);
            ready = next_ready;
        }

        if processed != providers.len() {
            return Err(TCError::bad_request(
                "op definition has circular dependencies".to_string(),
            ));
        }

        Ok(Self { levels, deps })
    }

    pub fn required_set(
        &self,
        capture: &Id,
        providers: &HashMap<Id, Scalar>,
    ) -> HashSet<Id> {
        let mut required = HashSet::new();
        let mut stack = vec![capture.clone()];

        while let Some(id) = stack.pop() {
            if !required.insert(id.clone()) {
                continue;
            }

            let Some(node_deps) = self.deps.get(&id) else {
                continue;
            };

            for dep in node_deps {
                if providers.contains_key(dep) {
                    stack.push(dep.clone());
                }
            }
        }

        required
    }
}

fn scalar_requires(scalar: &Scalar, deps: &mut HashSet<Id>) {
    match scalar {
        Scalar::Ref(r) => match r.as_ref() {
            TCRef::Id(id) => {
                deps.insert(id.id().clone());
            }
            TCRef::Op(op) => {
                opref_requires(op, deps);
            }
            TCRef::If(if_ref) => {
                tcref_requires(&if_ref.cond, deps);
                scalar_requires(&if_ref.then, deps);
                scalar_requires(&if_ref.or_else, deps);
            }
            TCRef::While(while_ref) => {
                scalar_requires(&while_ref.cond, deps);
                scalar_requires(&while_ref.closure, deps);
                scalar_requires(&while_ref.state, deps);
            }
        },
        Scalar::Map(map) => {
            for value in map.values() {
                scalar_requires(value, deps);
            }
        }
        Scalar::Tuple(items) => {
            for value in items {
                scalar_requires(value, deps);
            }
        }
        _ => {}
    }
}

fn tcref_requires(r: &TCRef, deps: &mut HashSet<Id>) {
    match r {
        TCRef::Id(id) => {
            deps.insert(id.id().clone());
        }
        TCRef::Op(op) => {
            opref_requires(op, deps);
        }
        TCRef::If(if_ref) => {
            tcref_requires(&if_ref.cond, deps);
            scalar_requires(&if_ref.then, deps);
            scalar_requires(&if_ref.or_else, deps);
        }
        TCRef::While(while_ref) => {
            scalar_requires(&while_ref.cond, deps);
            scalar_requires(&while_ref.closure, deps);
            scalar_requires(&while_ref.state, deps);
        }
    }
}

fn opref_requires(op: &OpRef, deps: &mut HashSet<Id>) {
    match op {
        OpRef::Get((subject, key)) => {
            subject_requires(subject, deps);
            scalar_requires(key, deps);
        }
        OpRef::Put((subject, key, value)) => {
            subject_requires(subject, deps);
            scalar_requires(key, deps);
            scalar_requires(value, deps);
        }
        OpRef::Post((subject, params)) => {
            subject_requires(subject, deps);
            for value in params.values() {
                scalar_requires(value, deps);
            }
        }
        OpRef::Delete((subject, key)) => {
            subject_requires(subject, deps);
            scalar_requires(key, deps);
        }
    }
}

fn subject_requires(subject: &Subject, deps: &mut HashSet<Id>) {
    if let Subject::Ref(id_ref, _) = subject {
        deps.insert(id_ref.id().clone());
    }
}
