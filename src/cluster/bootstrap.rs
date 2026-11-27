use std::collections::{BTreeSet, VecDeque};

use safecast::TryCastFrom;
use tc_error::{TCError, TCResult};
use tc_ir::Scalar;

use super::dir::{DirEntry, Resolved};
use super::replicas::replicas_link;
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

    pub(crate) fn token(&self) -> &str {
        &self.token
    }

    pub(crate) fn replica(&self) -> &crate::replication::Replica {
        &self.replica
    }

    pub(crate) fn state_hash(&self) -> &str {
        &self.state_hash
    }
}

impl<T: DirItem> DirEntry<T> {
    pub(crate) async fn synchronize(
        &self,
        txn: &crate::TxnHandle,
        seed: &str,
        session: &BootstrapSession,
    ) -> TCResult<()> {
        match self {
            Self::Item(cluster) => T::synchronize(cluster, txn, seed, session).await,
            Self::Dir(_) => Ok(()),
        }
    }
}

impl<T: Clone> Cluster<T> {
    async fn adopt_seed(
        &self,
        txn: &crate::TxnHandle,
        seed: &crate::replication::Replica,
    ) -> TCResult<crate::TxnHandle> {
        self.stage_seed(txn, seed).await?;
        let (txn, _call) = self.enter(txn).await?;
        Ok(txn)
    }

    async fn stage_seed(
        &self,
        txn: &crate::TxnHandle,
        seed: &crate::replication::Replica,
    ) -> TCResult<()> {
        if txn.is_locked()
            || txn.leader(self.path()) != Some((seed.host.clone(), seed.actor_id.clone()))
        {
            return Err(TCError::unauthorized(
                "bootstrap resource leader does not match its seed",
            ));
        }

        // The encrypted handshake authenticates this seed before restoration.
        // Ordinary membership updates still verify the completed resource hash.
        match self.replicas.entry(txn.id(), seed.endpoint.clone()).await? {
            txn_lock::map::Entry::Vacant(entry) => {
                entry.insert(seed.clone())?;
            }
            txn_lock::map::Entry::Occupied(entry) if *entry.get() == *seed => {}
            txn_lock::map::Entry::Occupied(_) => {
                return Err(TCError::conflict("replica endpoint has another identity"));
            }
        }

        Ok(())
    }
}

impl<T: DirItem> Cluster<Dir<T>> {
    pub(crate) async fn bootstrap_resource(
        &self,
        kernel: &crate::Kernel,
        seed: &str,
        resource: &pathlink::Link,
        identity: &crate::replication::Replica,
    ) -> TCResult<(crate::TxnHandle, BootstrapSession, DirEntry<T>)> {
        // Introduce the joining owner's key to every existing peer before a
        // later transaction can deliver that owner's signed decision to it.
        let (_probe, probe_txn, expected) =
            kernel.bootstrap_probe(seed, resource, identity).await?;

        let replicas = replicas_link(resource.path());
        let peers = seed_replicas(seed, expected.token(), &probe_txn, &replicas).await?;

        for peer in &peers {
            if peer.host == identity.host && peer.actor_id == identity.actor_id {
                continue;
            }

            let (_request, _txn, observed) = kernel
                .bootstrap_probe(&peer.endpoint, resource, identity)
                .await?;

            if observed.replica() != peer || observed.state_hash() != expected.state_hash() {
                return Err(TCError::conflict(
                    "bootstrap peers have inconsistent identities or state",
                ));
            }
        }

        let (_request, txn, session) = kernel.bootstrap_probe(seed, resource, identity).await?;
        let current = seed_replicas(seed, session.token(), &txn, &replicas).await?;
        if current != peers
            || session.replica() != expected.replica()
            || session.state_hash() != expected.state_hash()
        {
            return Err(TCError::conflict(
                "replica membership or state changed during bootstrap",
            ));
        }

        let suffix = resource
            .path()
            .strip_prefix(self.path().as_ref())
            .ok_or_else(|| TCError::not_found("bootstrap resource"))?;

        let entry = self.clone().lookup(&txn, suffix).await?.into_exact()?;
        let txn = match &entry {
            DirEntry::Dir(cluster) => cluster.adopt_seed(&txn, session.replica()).await?,
            DirEntry::Item(cluster) => cluster.adopt_seed(&txn, session.replica()).await?,
        };

        Ok((txn, session, entry))
    }

    pub(crate) async fn bootstrap(
        &self,
        kernel: &crate::Kernel,
        seed: &str,
        identity: &crate::replication::Replica,
    ) -> TCResult<BTreeSet<String>> {
        let root = pathlink::Link::from(self.path().clone());
        let (txn, session, _) = self
            .bootstrap_resource(kernel, seed, &root, identity)
            .await?;

        let resources = seed_tree(self, kernel, seed, identity, session.token(), &txn).await?;
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
        let (txn, session, entry) = self
            .bootstrap_resource(kernel, seed, resource, identity)
            .await?;
        // Keep native restoration out of the recursive bootstrap polling frame.
        Box::pin(entry.synchronize(&txn, seed, &session)).await?;
        let replicas = replicas_link(resource.path());
        let mut learned = seed_replicas(seed, session.token(), &txn, &replicas).await?;
        learned.push(session.replica().clone());
        let membership = txn
            .grant_claim(crate::Claim::new(replicas.clone(), umask::USER_WRITE))
            .await?;

        let mut peers = BTreeSet::new();
        for replica in learned {
            if replica.host == identity.host && replica.actor_id == identity.actor_id {
                continue;
            }

            peers.insert(replica.endpoint.clone());
            kernel
                .update_bootstrap_membership(&membership, &replicas, &replica, session.state_hash())
                .await?;
        }

        self.gateway
            .put(
                &session.replica().endpoint,
                session.token(),
                &txn,
                &replicas,
                Scalar::from(tc_value::Value::from(identity.endpoint.clone())),
                replica_put_state(identity, session.state_hash().to_string()),
            )
            .await?;

        let known = match entry {
            DirEntry::Dir(cluster) => cluster.replica_snapshot(txn.id()).await?,
            DirEntry::Item(cluster) => cluster.replica_snapshot(txn.id()).await?,
        };

        kernel
            .coordinate(&txn, crate::txn::TransactionOutcome::Commit, true)
            .await?;

        // Every peer now recognizes the joining host. A fresh ordinary transaction
        // can lead membership convergence across its previous and discovered peers.
        kernel
            .reconcile_bootstrap_membership(&replicas, &known, session.state_hash())
            .await?;

        Ok(peers)
    }
}

async fn seed_tree<T: DirItem>(
    root: &Cluster<Dir<T>>,
    kernel: &crate::Kernel,
    seed: &str,
    identity: &crate::replication::Replica,
    root_token: &str,
    txn: &crate::TxnHandle,
) -> TCResult<Vec<pathlink::Link>> {
    let mut directories = VecDeque::from([(
        pathlink::Link::from(root.path().clone()),
        root_token.to_owned(),
    )]);

    let mut resources = Vec::new();
    let mut items = Vec::new();
    while let Some((directory, token)) = directories.pop_front() {
        resources.push(directory.clone());
        let state = crate::replication::read_seed_state(
            seed,
            &token,
            txn,
            &directory,
            txn.request_body_limit(),
        )
        .await?;

        let tc_state::State::Map(entries) = state else {
            return Err(TCError::bad_gateway(format!(
                "seed directory {directory} did not return a membership map"
            )));
        };

        match root
            .clone()
            .lookup(txn, &directory.path()[root.path().len()..])
            .await?
        {
            Resolved::Dir { cluster, unmatched } if unmatched.is_empty() => {
                for (name, is_directory) in cluster.state().entries(txn.id()).await? {
                    if entries.get(&name).map(state_bool).transpose()? != Some(is_directory) {
                        return Err(TCError::conflict(
                            "seed directory does not contain the local resource tree",
                        ));
                    }
                }
            }
            Resolved::Dir { .. } => {}
            Resolved::Item { .. } => {
                return Err(TCError::conflict(
                    "seed directory conflicts with a local application",
                ));
            }
        }

        for (name, is_directory) in entries.into_iter().rev() {
            let is_directory = state_bool(&is_directory)?;
            let child: pathlink::Link = directory.path().clone().append(name).into();
            let session = kernel.bootstrap_child(seed, txn, &child, identity).await?;

            // Existing descendants remain under the namespace decision. Stage only
            // source-listed identities, without claiming them as separate owners.
            let branch = txn.branch().adopt_claims(session.token()).await?;
            match root
                .clone()
                .lookup(txn, &child.path()[root.path().len()..])
                .await?
            {
                Resolved::Dir { cluster, unmatched } if unmatched.is_empty() && is_directory => {
                    cluster.reserve_structure(txn.id())?;
                    cluster.stage_seed(&branch, session.replica()).await?;
                }
                Resolved::Item {
                    cluster, suffix, ..
                } if suffix.is_empty() && !is_directory => {
                    cluster.reserve_structure(txn.id())?;
                    cluster.stage_seed(&branch, session.replica()).await?;
                }
                Resolved::Dir { unmatched, .. } if !unmatched.is_empty() => {}
                _ => {
                    return Err(TCError::conflict(
                        "seed resource conflicts with the local namespace",
                    ));
                }
            }

            if is_directory {
                directories.push_back((child, session.token().to_string()));
            } else {
                let state = crate::replication::read_seed_state(
                    seed,
                    session.token(),
                    txn,
                    &child,
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

fn state_bool(state: &crate::State) -> TCResult<bool> {
    match state {
        tc_state::State::Scalar(Scalar::Value(tc_value::Value::Number(
            number_general::Number::Bool(value),
        ))) => Ok((*value).into()),
        _ => Err(TCError::bad_gateway(
            "seed directory membership marker is not a boolean",
        )),
    }
}

pub(crate) async fn seed_replicas(
    seed: &str,
    token: &str,
    txn: &crate::TxnHandle,
    target: &pathlink::Link,
) -> TCResult<Vec<crate::replication::Replica>> {
    let state = crate::replication::read_seed_state(
        seed,
        token,
        txn,
        target,
        crate::literal::MAX_DEFINITION_BYTES,
    )
    .await?;

    let tc_state::State::Tuple(replicas) = state else {
        return Err(TCError::bad_gateway("seed replicas were not a tuple"));
    };

    let mut replicas = replicas
        .into_iter()
        .map(|replica| {
            let tc_state::State::Tuple(fields) = replica else {
                return Err(TCError::bad_gateway("seed replica was not a tuple"));
            };

            let [endpoint, host, actor_id, algorithm, public_key_b64]: [crate::State; 5] = fields
                .try_into()
                .map_err(|_| TCError::bad_gateway("seed replica must have five identity fields"))?;

            let string = |state| match tc_value::Value::opt_cast_from(state) {
                Some(tc_value::Value::String(value)) => Ok(value),
                _ => Err(TCError::bad_gateway("seed replica fields must be strings")),
            };

            Ok(crate::replication::Replica {
                endpoint: crate::replication::normalize_peer(&string(endpoint)?)?,
                host: string(host)?,
                actor_id: string(actor_id)?,
                algorithm: string(algorithm)?
                    .parse()
                    .map_err(|error: rjwt::Error| TCError::bad_gateway(error.to_string()))?,
                public_key_b64: string(public_key_b64)?,
            })
        })
        .collect::<TCResult<Vec<_>>>()?;

    replicas.sort_unstable_by(|left, right| left.endpoint.cmp(&right.endpoint));
    if replicas
        .windows(2)
        .any(|pair| pair[0].endpoint == pair[1].endpoint)
    {
        return Err(TCError::bad_gateway(
            "seed returned a duplicate replica endpoint",
        ));
    }

    Ok(replicas)
}
