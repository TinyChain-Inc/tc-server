use std::collections::BTreeSet;

use pathlink::PathSegment;
use tc_error::{TCError, TCResult};
use tc_ir::{DeleteHandler, GetHandler, Handler, PutHandler, Route, Scalar, Transact, TxnId};

use super::{Cluster, ResourceHash};

pub(super) struct ReplicaHandler<'a, T> {
    pub(super) cluster: &'a Cluster<T>,
    pub(super) endpoint: Option<String>,
}

impl<T> Cluster<T> {
    fn leads(&self, txn: &crate::TxnHandle) -> bool {
        txn.leader(&self.path)
            == Some((
                self.protocol.host.to_string(),
                self.protocol.actor.id().to_string(),
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

    pub(crate) async fn replicate(
        &self,
        txn: &crate::TxnHandle,
        target: &pathlink::Link,
        method: tc_ir::Method,
        key: Scalar,
        value: Option<crate::State>,
    ) -> TCResult<()> {
        if !self.leads(txn) {
            return Ok(());
        }
        let peers = self.replica_endpoints(txn.id()).await?;
        if peers.is_empty() {
            return Ok(());
        }
        let result = match method {
            tc_ir::Method::Put if value.is_some() => {
                self.forward(txn, &peers, target, key, value).await?
            }
            tc_ir::Method::Delete if value.is_none() => {
                self.forward(txn, &peers, target, key, value).await?
            }
            _ => return Err(TCError::internal("invalid replicated resource mutation")),
        };
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
        if result.failed.is_empty() {
            return Ok(());
        }
        if result
            .first_error
            .as_ref()
            .is_some_and(|error| error.code() == tc_error::ErrorKind::Conflict)
        {
            return Err(result.first_error.expect("checked conflict"));
        }
        let replica_count = result.delivered.len() + result.failed.len() + 1;
        if result.delivered.len() + 1 <= replica_count / 2 {
            return Err(result.first_error.unwrap_or_else(|| {
                TCError::bad_gateway("replica write lost its strict majority")
            }));
        }

        for endpoint in &result.failed {
            self.replicas
                .remove(txn.id(), endpoint)
                .await
                .map_err(TCError::from)?;
        }
        txn.mark_resource_mutated(self.path())?;

        if !result.delivered.is_empty() {
            let target: pathlink::Link = format!("{}/replicas", self.path)
                .parse()
                .map_err(|error| TCError::internal(format!("invalid replica route: {error}")))?;
            for endpoint in result.failed {
                let removal = crate::replication::forward_delete_to_peers(
                    &result.delivered,
                    txn,
                    &target,
                    Scalar::Value(tc_value::Value::String(endpoint)),
                    self.gateway.as_ref(),
                )
                .await?;
                if !removal.failed.is_empty() {
                    return Err(removal.first_error.unwrap_or_else(|| {
                        TCError::bad_gateway("failed to propagate replica eviction")
                    }));
                }
            }
        }
        Ok(())
    }

    async fn replicate_membership(
        &self,
        txn: &crate::TxnHandle,
        endpoint: &str,
        value: Option<crate::State>,
    ) -> TCResult<()> {
        if !self.leads(txn) {
            return Ok(());
        }

        let mut peers = self.replica_endpoints(txn.id()).await?;
        if value.is_none() {
            peers.remove(endpoint);
        }
        if peers.is_empty() {
            return Ok(());
        }

        let target: pathlink::Link = format!("{}/replicas", self.path)
            .parse()
            .map_err(|error| TCError::internal(format!("invalid replica route: {error}")))?;
        let key = Scalar::Value(tc_value::Value::String(endpoint.to_string()));
        let result = self.forward(txn, &peers, &target, key, value).await?;
        self.accept_write_fanout(txn, result).await
    }

    pub(crate) async fn decide(
        &self,
        txn: &crate::TxnHandle,
        outcome: crate::txn::TransactionOutcome,
    ) -> TCResult<()>
    where
        T: Transact,
    {
        if !self.leads(txn) {
            return if outcome.commits() {
                self.commit(txn.id()).await
            } else {
                self.rollback(&txn.id()).await
            };
        }
        crate::replication::forward_resource_decision(
            &self.replica_endpoints(txn.id()).await?,
            txn,
            &self.path,
            outcome.commits(),
            self.gateway.as_ref(),
        )
        .await?;
        if outcome.commits() {
            self.commit(txn.id()).await
        } else {
            self.rollback(&txn.id()).await
        }
    }

    pub(super) async fn decide_request(
        &self,
        txn: &crate::TxnHandle,
        suffix: &[PathSegment],
        method: tc_ir::Method,
        has_body: bool,
    ) -> TCResult<bool>
    where
        T: Transact,
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
    T: Route<crate::State> + ResourceHash,
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
        self.state.route(path)
    }
}

impl<T> Transact for Cluster<T>
where
    T: Transact,
{
    async fn commit(&self, txn_id: TxnId) -> TCResult<()> {
        self.state.commit(txn_id).await?;
        self.replicas.commit(txn_id);
        Ok(())
    }

    async fn rollback(&self, txn_id: &TxnId) -> TCResult<()> {
        self.state.rollback(txn_id).await?;
        self.replicas.rollback(txn_id);
        Ok(())
    }

    async fn finalize(&self, cutoff: &TxnId) -> TCResult<()> {
        self.state.finalize(cutoff).await?;
        self.replicas.finalize(*cutoff);
        Ok(())
    }
}

impl<'a, T> Handler<'a, crate::State> for ReplicaHandler<'a, T>
where
    T: ResourceHash,
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
                if self.endpoint.is_some() {
                    return Err(TCError::bad_request(
                        "replica PUT must target the replicas collection",
                    ));
                }
                let endpoint = replica_key(None, key)?
                    .ok_or_else(|| TCError::bad_request("replica PUT requires an endpoint key"))?;
                let (host, actor_id, algorithm, public_key_b64, expected_hash) =
                    replica_value(value)?;
                let actual_hash = self.cluster.state.resource_hash(txn.id()).await?;
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
                let existing = self
                    .cluster
                    .replicas
                    .get(txn.id(), &endpoint)
                    .await
                    .map_err(TCError::from)?
                    .map(|entry| entry.clone());
                match existing {
                    Some(existing) if existing == replica => return Ok(()),
                    Some(_) => {
                        return Err(TCError::conflict("replica endpoint has another identity"));
                    }
                    None => {}
                }

                let value = replica_put_state(&replica, expected_hash);
                self.cluster
                    .replicate_membership(txn, &replica.endpoint, Some(value))
                    .await?;
                match self
                    .cluster
                    .replicas
                    .entry(txn.id(), endpoint)
                    .await
                    .map_err(TCError::from)?
                {
                    Entry::Vacant(entry) => {
                        entry.insert(replica);
                        txn.mark_resource_mutated(self.cluster.path())?;
                    }
                    Entry::Occupied(entry) if entry.get() == &replica => return Ok(()),
                    Entry::Occupied(_) => {
                        return Err(TCError::conflict("replica endpoint has another identity"));
                    }
                }
                Ok(())
            })
        }))
    }

    fn delete<'txn>(self: Box<Self>) -> Option<DeleteHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, key| {
            Box::pin(async move {
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
                self.cluster
                    .replicate_membership(txn, &endpoint, None)
                    .await?;
                self.cluster
                    .replicas
                    .remove(txn.id(), &endpoint)
                    .await
                    .map_err(TCError::from)?;
                txn.mark_resource_mutated(self.cluster.path())?;
                Ok(())
            })
        }))
    }
}

fn replica_key(path: Option<&str>, key: Scalar) -> TCResult<Option<String>> {
    let from_path = path.map(str::to_string);
    let from_key = match key {
        Scalar::Value(tc_value::Value::None) => None,
        Scalar::Value(tc_value::Value::String(endpoint)) => Some(endpoint),
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

fn replica_value(value: crate::State) -> TCResult<(String, String, rjwt::AlgKind, String, String)> {
    let crate::State::Tuple(values) = value else {
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
    match state {
        crate::State::Scalar(Scalar::Value(tc_value::Value::String(value))) => Ok(value),
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
        .map(|value| crate::State::from(tc_value::Value::String(value)))
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
        .map(|value| crate::State::from(tc_value::Value::String(value)))
        .collect(),
    )
}
