use std::collections::BTreeSet;
use std::sync::Arc;

use base64::Engine as _;
use pathlink::PathSegment;
use safecast::TryCastFrom;
use tc_error::{TCError, TCResult};
use tc_ir::{DeleteHandler, GetHandler, Handler, PutHandler, Route, Scalar, Transact, TxnId};

use super::{AsyncHash, Cluster, Participation};

pub(super) struct ReplicaHandler<'a, T> {
    pub(super) cluster: &'a Cluster<T>,
    pub(super) endpoint: Option<String>,
}

impl<T> Cluster<T> {
    fn leads(&self, txn: &crate::TxnHandle) -> bool {
        txn.leader(&self.path)
            == Some((
                self.protocol.host().to_string(),
                self.protocol.actor_id().to_string(),
            ))
    }

    pub(super) async fn replica_snapshot(
        &self,
        txn_id: TxnId,
    ) -> TCResult<Vec<crate::replication::Replica>> {
        self.replicas
            .iter(txn_id)
            .await
            .map_err(TCError::from)
            .map(|entries| entries.map(|(_, replica)| replica.clone()).collect())
    }

    async fn replica_endpoints(&self, txn_id: TxnId) -> TCResult<BTreeSet<String>> {
        Ok(self
            .replica_snapshot(txn_id)
            .await?
            .into_iter()
            .map(|replica| replica.endpoint)
            .collect())
    }

    pub(super) async fn replica_identities(
        &self,
        txn_id: TxnId,
    ) -> TCResult<BTreeSet<(String, String, String, Vec<u8>)>> {
        let key = self.protocol.verifying_key();
        let mut identities = BTreeSet::from([(
            self.protocol.host().to_string(),
            self.protocol.actor_id().to_string(),
            key.alg().name().to_string(),
            key.to_bytes().to_vec(),
        )]);

        for replica in self.replica_snapshot(txn_id).await? {
            let host = replica
                .host
                .parse::<pathlink::Link>()
                .map_err(|error| TCError::internal(format!("invalid replica host: {error}")))?;

            let key = base64::engine::general_purpose::STANDARD
                .decode(replica.public_key_b64)
                .map_err(|error| TCError::internal(format!("invalid replica key: {error}")))?;

            identities.insert((
                host.to_string(),
                replica.actor_id,
                replica.algorithm.name().to_string(),
                key,
            ));
        }

        Ok(identities)
    }

    pub(crate) async fn replication_peers(
        &self,
        txn: &crate::TxnHandle,
    ) -> TCResult<BTreeSet<String>> {
        if self.leads(txn) {
            self.replica_endpoints(txn.id()).await
        } else {
            Ok(BTreeSet::new())
        }
    }

    pub(crate) async fn replicate(
        &self,
        txn: &crate::TxnHandle,
        peers: &BTreeSet<String>,
        target: &pathlink::Link,
        key: Scalar,
        value: Option<crate::State>,
    ) -> TCResult<()> {
        if peers.is_empty() {
            return Ok(());
        }

        let result = self.forward(txn, peers, target, key, value).await?;
        self.accept_write_fanout(txn, result).await
    }

    async fn forward(
        &self,
        txn: &crate::TxnHandle,
        peers: &BTreeSet<String>,
        target: &pathlink::Link,
        key: Scalar,
        value: Option<crate::State>,
    ) -> TCResult<crate::replication::Fanout> {
        match value {
            Some(value) => {
                crate::replication::forward_put_to_peers(
                    peers,
                    txn,
                    target,
                    key,
                    value,
                    self.gateway.as_ref(),
                )
                .await
            }
            None => {
                crate::replication::forward_delete_to_peers(
                    peers,
                    txn,
                    target,
                    key,
                    self.gateway.as_ref(),
                )
                .await
            }
        }
    }

    async fn accept_write_fanout(
        &self,
        txn: &crate::TxnHandle,
        result: crate::replication::Fanout,
    ) -> TCResult<()> {
        let evictions = result.strict_majority()?;

        for endpoint in evictions.failed() {
            self.replicas
                .remove(txn.id(), endpoint)
                .await
                .map_err(TCError::from)?;
        }

        if !evictions.delivered().is_empty() {
            let target = replicas_link(&self.path);
            let delivered = evictions.delivered().clone();
            for endpoint in evictions.into_failed() {
                let removal = crate::replication::forward_delete_to_peers(
                    &delivered,
                    txn,
                    &target,
                    Scalar::from(tc_value::Value::from(endpoint)),
                    self.gateway.as_ref(),
                )
                .await?;

                removal.complete("failed to propagate replica eviction")?;
            }
        }

        Ok(())
    }

    async fn replicate_membership(
        &self,
        txn: &crate::TxnHandle,
        mut peers: BTreeSet<String>,
        endpoint: &str,
        value: Option<crate::State>,
    ) -> TCResult<()> {
        // A peer does not retain its own identity as a remote replica.
        peers.remove(endpoint);

        if peers.is_empty() {
            return Ok(());
        }

        let target = replicas_link(&self.path);
        let key = Scalar::from(tc_value::Value::from(endpoint.to_string()));
        let result = self.forward(txn, &peers, &target, key, value).await?;
        self.accept_write_fanout(txn, result).await
    }

    /// Resolve decision authority without admitting another application call.
    async fn decision_participant(&self, txn: &crate::TxnHandle) -> TCResult<Arc<Participation>>
    where
        T: Clone,
    {
        let mut claims = txn
            .protocol_snapshot()
            .ok_or_else(|| TCError::unauthorized("missing transaction authority"))?;

        if !claims.locked {
            return Err(TCError::conflict("inconsistent transaction decision"));
        }

        let owner = claims
            .owner
            .as_ref()
            .ok_or_else(|| TCError::unauthorized("missing transaction owner"))?;

        let leader = claims.leaders.get(&self.path);
        let existing = self.participation.lock().get(&txn.id()).cloned();
        let participant = match existing {
            Some(Some(existing)) => existing,
            Some(None) => return Err(TCError::conflict("namespace structure is reserved")),
            None => {
                // An untouched replica may enroll for a decision, never new work.
                let leader = leader.ok_or_else(|| TCError::conflict("resource was not claimed"))?;
                self.verify_leader(txn, leader).await?;
                let participant = Arc::new(Participation::new(owner.clone(), leader.clone()));
                participant.seal()?;
                // Membership access awaited; refresh authority before validating
                // the winning entry. Existing participation needs no reparse.
                claims = txn
                    .protocol_snapshot()
                    .ok_or_else(|| TCError::unauthorized("missing transaction authority"))?;

                self.participation
                    .lock()
                    .entry(txn.id())
                    .or_insert(Some(participant))
                    .as_ref()
                    .cloned()
                    .ok_or_else(|| TCError::conflict("namespace structure is reserved"))?
            }
        };

        if claims.owner.as_ref() != Some(&participant.owner)
            || claims
                .leaders
                .get(&self.path)
                .is_some_and(|leader| leader != &participant.leader)
        {
            return Err(TCError::unauthorized(
                "decision does not match resource ownership",
            ));
        }

        Ok(participant)
    }

    pub(crate) async fn decide(
        &self,
        txn: &crate::TxnHandle,
        outcome: crate::txn::TransactionOutcome,
    ) -> TCResult<()>
    where
        T: Clone + Transact,
    {
        use futures::{StreamExt, TryStreamExt};

        let participant = self.decision_participant(txn).await?;

        let claims = txn
            .protocol_snapshot()
            .ok_or_else(|| TCError::unauthorized("missing decision authority"))?;

        if !claims.locked || claims.decision.is_some_and(|previous| previous != outcome) {
            return Err(TCError::conflict("inconsistent transaction decision"));
        }

        let dependencies = participant.seal()?;
        let leads = participant.leader
            == (
                self.protocol.host().to_string(),
                self.protocol.actor_id().to_string(),
            );

        if !leads && txn.owns_invocation() {
            // Bootstrap may own the invocation on a joining replica while the
            // established resource leader remains on the seed host.
            let leader = self
                .replica_snapshot(txn.id())
                .await?
                .into_iter()
                .find(|replica| {
                    replica.host == participant.leader.0 && replica.actor_id == participant.leader.1
                })
                .ok_or_else(|| TCError::conflict("resource leader has no replica endpoint"))?;

            let token = txn.raw_token().expect("locked owner decision");
            self.gateway
                .decide_resource(&leader.endpoint, &token, txn, &self.path, outcome.commits())
                .await?;
        }

        if leads {
            // Only the destination knows its retained leader. A valid signature
            // for another actor or resource cannot suppress delivery here.
            if claims.visited.contains(&(
                participant.leader.0.clone(),
                participant.leader.1.clone(),
                self.path.clone(),
            )) {
                return Ok(());
            }

            let branch = txn.decision_branch(&self.path, outcome)?;
            let token = branch.raw_token().expect("signed decision branch");
            let mut deliveries = futures::stream::iter(dependencies)
                .map(|target| {
                    let branch = &branch;
                    let token = &token;
                    async move {
                        if let Some(host) = target.host() {
                            let peer = crate::replication::normalize_peer(&host.to_string())?;
                            self.gateway
                                .decide_resource(
                                    &peer,
                                    token,
                                    branch,
                                    target.path(),
                                    outcome.commits(),
                                )
                                .await
                        } else {
                            branch.decide_local(&target, outcome).await
                        }
                    }
                })
                .buffered(8);

            while deliveries.try_next().await?.is_some() {}

            crate::replication::forward_resource_decision(
                &self.replica_endpoints(txn.id()).await?,
                &branch,
                &self.path,
                outcome.commits(),
                self.gateway.as_ref(),
            )
            .await?;
        }

        let mut application = LocalDecision { txn, armed: true };
        let result = if outcome.commits() {
            self.commit(txn.id()).await
        } else {
            self.rollback(&txn.id()).await
        };

        if let Err(error) = result {
            txn.fatal_recovery(format!(
                "local transaction application failed: {error}; causes: {:?}",
                error.causes()
            ));
        }

        application.armed = false;
        Ok(())
    }

    pub(super) async fn decide_request(
        &self,
        txn: &crate::TxnHandle,
        suffix: &[PathSegment],
        method: tc_ir::Method,
        has_body: bool,
    ) -> TCResult<bool>
    where
        T: Clone + Transact,
    {
        if !txn.is_locked() {
            return Ok(false);
        }
        if !suffix.is_empty() || has_body {
            return Err(TCError::conflict(format!(
                "transaction decision for {} has suffix {suffix:?} or body={has_body}",
                self.path,
            )));
        }
        let outcome = match method {
            tc_ir::Method::Put => crate::txn::TransactionOutcome::Commit,
            tc_ir::Method::Delete => crate::txn::TransactionOutcome::Rollback,
            _ => {
                return Err(TCError::conflict(
                    "a locked transaction accepts only bodyless PUT or DELETE",
                ));
            }
        };
        self.decide(txn, outcome).await?;
        Ok(true)
    }
}

impl<T> Route<crate::State> for Cluster<T>
where
    T: Route<crate::State> + AsyncHash,
{
    fn route<'a>(
        &'a self,
        path: &[PathSegment],
    ) -> Option<Box<dyn Handler<'a, crate::State> + 'a>> {
        if path
            .first()
            .is_some_and(|segment| segment.as_str() == super::REPLICAS)
        {
            return (path.len() <= 2).then(|| {
                Box::new(ReplicaHandler {
                    cluster: self,
                    endpoint: path.get(1).map(|segment| segment.as_str().to_string()),
                }) as Box<dyn Handler<'a, crate::State>>
            });
        }
        let leaf = self.state.route(path)?;
        // Executable Service members mutate this exact resource. Definition-only
        // applications and directory membership retain their existing owners.
        if !path.is_empty()
            && self
                .path
                .first()
                .is_some_and(|kind| kind.as_str() == "service")
        {
            let mut target = pathlink::Link::from(self.path.clone());
            for segment in path {
                target = target.append(segment.clone());
            }
            Some(Box::new(ServiceMutation {
                cluster: self,
                target,
                leaf,
            }))
        } else {
            Some(leaf)
        }
    }
}

/// V1's replication boundary, retaining the selected native verb instead of
/// routing it again. A participant's `replicate` call deliberately does not forward.
struct ServiceMutation<'a, T> {
    cluster: &'a Cluster<T>,
    target: pathlink::Link,
    leaf: Box<dyn Handler<'a, crate::State> + 'a>,
}

impl<'a, T: AsyncHash> Handler<'a, crate::State> for ServiceMutation<'a, T> {
    fn get<'txn>(self: Box<Self>) -> Option<GetHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        self.leaf.get()
    }

    fn post<'txn>(self: Box<Self>) -> Option<tc_ir::PostHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        self.leaf.post()
    }

    fn put<'txn>(self: Box<Self>) -> Option<PutHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        let Self {
            cluster,
            target,
            leaf,
        } = *self;
        let put = leaf.put()?;
        Some(Box::new(move |txn, key, value| {
            Box::pin(async move {
                if !txn.may_mutate(&cluster.path.clone().into(), cluster.path()) {
                    return Err(TCError::unauthorized("unauthorized Service mutation"));
                }

                let peers = cluster.replication_peers(txn).await?;
                put(txn, key.clone(), value.clone()).await?;

                cluster
                    .replicate(txn, &peers, &target, key, Some(value))
                    .await
            })
        }))
    }

    fn delete<'txn>(self: Box<Self>) -> Option<DeleteHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        let Self {
            cluster,
            target,
            leaf,
        } = *self;
        let delete = leaf.delete()?;
        Some(Box::new(move |txn, key| {
            Box::pin(async move {
                if !txn.may_mutate(&cluster.path.clone().into(), cluster.path()) {
                    return Err(TCError::unauthorized("unauthorized Service mutation"));
                }

                let peers = cluster.replication_peers(txn).await?;
                delete(txn, key.clone()).await?;

                cluster.replicate(txn, &peers, &target, key, None).await
            })
        }))
    }
}

impl<T> Transact for Cluster<T>
where
    T: Transact,
{
    async fn commit(&self, txn_id: TxnId) -> TCResult<()> {
        self.state.commit(txn_id).await?;
        self.replicas.commit(txn_id)?;
        Ok(())
    }

    async fn rollback(&self, txn_id: &TxnId) -> TCResult<()> {
        self.state.rollback(txn_id).await?;
        self.replicas.rollback(txn_id)?;
        Ok(())
    }

    async fn finalize(&self, cutoff: &TxnId) -> TCResult<()> {
        self.state.finalize(cutoff).await?;
        self.replicas.finalize(*cutoff)?;
        self.participation
            .lock()
            .retain(|txn_id, _| txn_id > cutoff);

        Ok(())
    }
}

impl<'a, T> Handler<'a, crate::State> for ReplicaHandler<'a, T>
where
    T: AsyncHash,
{
    fn get<'txn>(self: Box<Self>) -> Option<GetHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, key| {
            Box::pin(async move {
                let endpoint = replica_key(self.endpoint.as_deref(), key)?;
                let replicas = self.cluster.replica_snapshot(txn.id()).await?;
                match endpoint {
                    Some(endpoint) => replicas
                        .into_iter()
                        .find(|replica| replica.endpoint == endpoint)
                        .map(replica_state)
                        .ok_or_else(|| TCError::not_found(endpoint)),
                    None => Ok(crate::State::Tuple(
                        replicas.into_iter().map(replica_state).collect(),
                    )),
                }
            })
        }))
    }

    fn put<'txn>(self: Box<Self>) -> Option<PutHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, key, value| {
            Box::pin(async move {
                if !txn.may_mutate(&replicas_link(self.cluster.path()), self.cluster.path()) {
                    return Err(TCError::unauthorized(
                        "unauthorized replica membership mutation",
                    ));
                }

                if self.endpoint.is_some() {
                    return Err(TCError::bad_request(
                        "replica PUT must target the replicas collection",
                    ));
                }

                let endpoint = replica_key(None, key)?
                    .ok_or_else(|| TCError::bad_request("replica PUT requires an endpoint key"))?;
                let (host, actor_id, algorithm, public_key_b64, expected_hash) =
                    replica_value(value)?;
                let actual_hash = self.cluster.state.hash(txn).await?;
                if hex::encode(actual_hash) != expected_hash {
                    return Err(TCError::conflict(
                        "replica state hash does not match this resource",
                    ));
                }

                use txn_lock::map::Entry;

                let replica = crate::replication::Replica {
                    endpoint: endpoint.clone(),
                    host,
                    actor_id,
                    algorithm,
                    public_key_b64,
                };

                let peers = self.cluster.replication_peers(txn).await?;
                let value = replica_put_state(&replica, expected_hash);
                {
                    let entry = self
                        .cluster
                        .replicas
                        .entry(txn.id(), endpoint.clone())
                        .await
                        .map_err(TCError::from)?;

                    if matches!(&entry, Entry::Occupied(entry) if entry.get() != &replica) {
                        return Err(TCError::conflict("replica endpoint has another identity"));
                    }

                    // Validate identity before registering its key. Registration is
                    // synchronous; release the membership entry before any fanout.
                    txn.register_peer_identity(&replica)?;
                    if let Entry::Vacant(entry) = entry {
                        entry.insert(replica)?;
                    }
                }

                self.cluster
                    .replicate_membership(txn, peers, &endpoint, Some(value))
                    .await
            })
        }))
    }

    fn delete<'txn>(self: Box<Self>) -> Option<DeleteHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, key| {
            Box::pin(async move {
                if !txn.may_mutate(&replicas_link(self.cluster.path()), self.cluster.path()) {
                    return Err(TCError::unauthorized(
                        "unauthorized replica membership mutation",
                    ));
                }

                let endpoint = replica_key(self.endpoint.as_deref(), key)?
                    .ok_or_else(|| TCError::bad_request("replica DELETE requires an endpoint"))?;
                if self
                    .cluster
                    .replicas
                    .get(txn.id(), &endpoint)
                    .await
                    .map_err(TCError::from)?
                    .is_none()
                {
                    return Ok(());
                }

                let peers = self.cluster.replication_peers(txn).await?;
                self.cluster
                    .replicas
                    .remove(txn.id(), &endpoint)
                    .await
                    .map_err(TCError::from)?;

                self.cluster
                    .replicate_membership(txn, peers, &endpoint, None)
                    .await
            })
        }))
    }
}

fn replica_key(path: Option<&str>, key: Scalar) -> TCResult<Option<String>> {
    let from_path = path.map(str::to_string);
    let from_key = match tc_value::Value::opt_cast_from(key) {
        Some(tc_value::Value::None) => None,
        Some(tc_value::Value::String(endpoint)) => Some(endpoint),
        _ => {
            return Err(TCError::bad_request(
                "replica key must be an endpoint string",
            ));
        }
    };

    match (from_path, from_key) {
        (Some(path), Some(key)) if path != key => {
            Err(TCError::bad_request("conflicting replica keys"))
        }
        (path, key) => Ok(path.or(key)),
    }
}

pub(super) fn replicas_link(path: &pathlink::PathBuf) -> pathlink::Link {
    pathlink::Link::from(
        path.clone().append(
            super::REPLICAS
                .parse::<pathlink::PathSegment>()
                .expect("reserved replica segment"),
        ),
    )
}

fn replica_value(value: crate::State) -> TCResult<(String, String, rjwt::AlgKind, String, String)> {
    let tc_state::State::Tuple(values) = value else {
        return Err(TCError::bad_request(
            "replica value must be [host, actor, algorithm, public_key, state_hash]",
        ));
    };

    let [host, actor, algorithm, key, hash]: [crate::State; 5] =
        values.try_into().map_err(|_| {
            TCError::bad_request(
                "replica value must be [host, actor, algorithm, public_key, state_hash]",
            )
        })?;
    Ok((
        state_string(host)?,
        state_string(actor)?,
        state_string(algorithm)?
            .parse()
            .map_err(|error: rjwt::Error| TCError::bad_request(error.to_string()))?,
        state_string(key)?,
        state_string(hash)?,
    ))
}

fn state_string(state: crate::State) -> TCResult<String> {
    match tc_value::Value::opt_cast_from(state) {
        Some(tc_value::Value::String(value)) => Ok(value),
        _ => Err(TCError::bad_request(
            "replica record fields must be strings",
        )),
    }
}

fn replica_state(replica: crate::replication::Replica) -> crate::State {
    crate::State::Tuple(
        [
            replica.endpoint,
            replica.host,
            replica.actor_id,
            replica.algorithm.name().to_string(),
            replica.public_key_b64,
        ]
        .into_iter()
        .map(|value| crate::State::from(tc_value::Value::from(value)))
        .collect(),
    )
}

pub(crate) fn replica_put_state(
    replica: &crate::replication::Replica,
    state_hash: String,
) -> crate::State {
    crate::State::Tuple(
        [
            replica.host.clone(),
            replica.actor_id.clone(),
            replica.algorithm.name().to_string(),
            replica.public_key_b64.clone(),
            state_hash,
        ]
        .into_iter()
        .map(|value| crate::State::from(tc_value::Value::from(value)))
        .collect(),
    )
}

#[cfg(test)]
#[path = "../../tests/support/integrity.rs"]
mod integrity_tests;

/// Once local lifecycle application starts, cancellation cannot leave the host reusable.
struct LocalDecision<'a> {
    txn: &'a crate::TxnHandle,
    armed: bool,
}

impl Drop for LocalDecision<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.txn
                .fatal_recovery("local transaction application interrupted");
        }
    }
}
