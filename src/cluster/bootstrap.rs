use std::collections::{BTreeSet, VecDeque};

use tc_error::{TCError, TCResult};
use tc_ir::Scalar;

use super::{Cluster, Dir, DirItem, replica_put_state};

pub(crate) struct BootstrapSession {
    token: String,
    replica: crate::replication::Replica,
    state_hash: String,
}

impl BootstrapSession {
    pub(crate) fn new(
        token: String,
        replica: crate::replication::Replica,
        state_hash: String,
    ) -> Self {
        Self {
            token,
            replica,
            state_hash,
        }
    }

    fn token(&self) -> &str {
        &self.token
    }

    fn replica(&self) -> &crate::replication::Replica {
        &self.replica
    }

    fn state_hash(&self) -> &str {
        &self.state_hash
    }
}

impl<T: DirItem> Cluster<Dir<T>> {
    pub(crate) async fn bootstrap(
        &self,
        kernel: &crate::Kernel,
        seed: &str,
        identity: &crate::replication::Replica,
    ) -> TCResult<BTreeSet<String>> {
        let root = pathlink::Link::from(self.path().clone());
        let (txn, session) = kernel.bootstrap_resource(seed, &root, identity).await?;
        let resources = seed_tree(kernel, seed, identity, session.token(), &txn, &root).await?;
        kernel
            .coordinate(&txn, crate::txn::TransactionOutcome::Commit, true)
            .await?;

        let mut peers = BTreeSet::new();
        for resource in resources {
            peers.extend(self.join(kernel, seed, &resource, identity).await?);
        }
        Ok(peers)
    }

    async fn join(
        &self,
        kernel: &crate::Kernel,
        seed: &str,
        resource: &pathlink::Link,
        identity: &crate::replication::Replica,
    ) -> TCResult<BTreeSet<String>> {
        let (txn, session) = kernel.bootstrap_resource(seed, resource, identity).await?;
        let replicas = pathlink::Link::from(
            resource.path().clone().append(
                "replicas"
                    .parse::<pathlink::PathSegment>()
                    .expect("replicas segment"),
            ),
        );
        let local_value = crate::State::Tuple(vec![
            crate::State::from(tc_value::Value::String(session.replica().endpoint.clone())),
            replica_put_state(session.replica(), session.state_hash().to_string()),
        ]);
        let peers =
            seed_replica_endpoints(seed, session.token(), txn.id(), &replicas, txn.deadline())
                .await?;
        kernel
            .update_bootstrap_membership(&txn, &replicas, local_value)
            .await?;
        self.gateway
            .put(
                &session.replica().endpoint,
                session.token(),
                txn.id(),
                &replicas,
                Scalar::Value(tc_value::Value::String(identity.endpoint.clone())),
                replica_put_state(identity, session.state_hash().to_string()),
                txn.deadline(),
            )
            .await?;
        kernel
            .coordinate(&txn, crate::txn::TransactionOutcome::Commit, true)
            .await?;
        Ok(peers)
    }
}

async fn seed_tree(
    kernel: &crate::Kernel,
    seed: &str,
    identity: &crate::replication::Replica,
    root_token: &str,
    txn: &crate::TxnHandle,
    root: &pathlink::Link,
) -> TCResult<Vec<pathlink::Link>> {
    let mut directories = VecDeque::from([(root.clone(), root_token.to_owned())]);
    let mut resources = Vec::new();
    let mut items = Vec::new();
    while let Some((directory, token)) = directories.pop_front() {
        resources.push(directory.clone());
        let state = crate::replication::read_seed_state(
            seed,
            &token,
            txn.id(),
            &directory,
            txn.deadline(),
            txn.request_body_limit(),
        )
        .await?;
        let crate::State::Map(entries) = state else {
            return Err(TCError::bad_gateway(format!(
                "seed directory {directory} did not return a membership map"
            )));
        };
        for (name, is_directory) in entries.into_iter().rev() {
            let child: pathlink::Link = directory.path().clone().append(name).into();
            let session = kernel.bootstrap_child(seed, txn, &child, identity).await?;
            if state_bool(is_directory)? {
                directories.push_back((child, session.token().to_string()));
            } else {
                let state = crate::replication::read_seed_state(
                    seed,
                    session.token(),
                    txn.id(),
                    &child,
                    txn.deadline(),
                    txn.application_body_limit(),
                )
                .await?;
                kernel.install_seed_item(txn, child.clone(), state).await?;
                items.push(child);
            }
        }
    }
    resources.extend(items);
    Ok(resources)
}

fn state_bool(state: crate::State) -> TCResult<bool> {
    match state {
        crate::State::Scalar(Scalar::Value(tc_value::Value::Number(
            number_general::Number::Bool(value),
        ))) => Ok(value.into()),
        _ => Err(TCError::bad_gateway(
            "seed directory membership marker is not a boolean",
        )),
    }
}

async fn seed_replica_endpoints(
    seed: &str,
    token: &str,
    txn_id: tc_ir::TxnId,
    target: &pathlink::Link,
    deadline: crate::Deadline,
) -> TCResult<BTreeSet<String>> {
    let state = crate::replication::read_seed_state(
        seed,
        token,
        txn_id,
        target,
        deadline,
        crate::literal::MAX_DEFINITION_BYTES,
    )
    .await?;
    let crate::State::Tuple(replicas) = state else {
        return Err(TCError::bad_gateway("seed replicas were not a tuple"));
    };
    replicas
        .into_iter()
        .map(|replica| {
            let crate::State::Tuple(fields) = replica else {
                return Err(TCError::bad_gateway("seed replica was not a tuple"));
            };
            let Some(crate::State::Scalar(Scalar::Value(tc_value::Value::String(endpoint)))) =
                fields.into_iter().next()
            else {
                return Err(TCError::bad_gateway("seed replica has no endpoint"));
            };
            crate::replication::normalize_peer(&endpoint)
        })
        .collect()
}
