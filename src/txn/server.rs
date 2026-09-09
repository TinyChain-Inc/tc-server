use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use parking_lot::Mutex;
#[cfg(test)]
use pathlink::Link;
use sha2::{Digest, Sha256};
use tc_error::{TCError, TCResult};
use tc_ir::{Claim, NetworkTime, TxnId};
use tokio::time::Instant;

use super::{AuthContext, TxnConfig, TxnError, TxnHandle, validate_signed_token};
use crate::auth::{TokenContext, TokenVerifier};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TransactionOutcome {
    Commit,
    Rollback,
}

impl TransactionOutcome {
    pub(crate) fn commits(self) -> bool {
        self == Self::Commit
    }
}

#[derive(Clone)]
pub(crate) struct TxnServer {
    state: Arc<TxnState>,
}

struct TxnState {
    config: TxnConfig,
    allocation: tokio::sync::Mutex<()>,
    inner: Mutex<Inner>,
    notify: tokio::sync::Notify,
    worker_started: AtomicBool,
    ready: AtomicBool,
    verifier: Arc<dyn TokenVerifier>,
}

struct Inner {
    active: BTreeMap<TxnId, Active>,
    latest_finalized: Option<TxnId>,
    last_allocated: Option<TxnId>,
}

#[derive(Clone, Copy)]
struct Active {
    expires: Instant,
    retry: Option<Instant>,
}

impl TxnServer {
    pub(crate) fn resources(&self) -> &crate::HostResources {
        &self.state.config.resources
    }

    pub(super) fn workspace(&self) -> &crate::Workspace {
        &self.state.config.workspace
    }

    pub(super) fn ttl(&self) -> Duration {
        self.state.config.ttl
    }

    pub(crate) fn protocol_authority(&self) -> crate::ProtocolAuthority {
        crate::ProtocolAuthority {
            host_id: Arc::clone(&self.state.config.host_id),
            host: self.state.config.protocol_host.clone(),
            actor: Arc::clone(&self.state.config.protocol_actor),
        }
    }

    pub(crate) fn verifier(&self) -> &Arc<dyn TokenVerifier> {
        &self.state.verifier
    }

    pub(crate) fn new(config: TxnConfig, verifier: Arc<dyn TokenVerifier>) -> Self {
        Self {
            state: Arc::new(TxnState {
                config,
                allocation: tokio::sync::Mutex::new(()),
                inner: Mutex::new(Inner {
                    active: BTreeMap::new(),
                    latest_finalized: None,
                    last_allocated: None,
                }),
                notify: tokio::sync::Notify::new(),
                worker_started: AtomicBool::new(false),
                ready: AtomicBool::new(false),
                verifier,
            }),
        }
    }

    pub(crate) async fn bind(
        &self,
        txn_id: Option<TxnId>,
        token: Option<&TokenContext>,
        runtime: Arc<crate::kernel::HostRuntime>,
    ) -> TCResult<TxnHandle> {
        if !self.is_ready() {
            return Err(TCError::new(
                tc_error::ErrorKind::Unavailable,
                "transaction storage is not ready",
            ));
        }
        match txn_id {
            None => self.begin(token, runtime).await,
            Some(txn_id) => {
                self.reject_finalized(txn_id)?;
                self.reject_expired(txn_id)?;
                let token = token.ok_or(TxnError::Unauthorized)?;
                let signed = token.signed.as_deref().ok_or(TxnError::Unauthorized)?;
                validate_signed_token(txn_id, signed).map_err(|_| TxnError::Unauthorized)?;
                self.observe(txn_id);
                self.handle(txn_id, false, Some(token), runtime)
            }
        }
    }

    async fn begin(
        &self,
        auth: Option<&TokenContext>,
        runtime: Arc<crate::kernel::HostRuntime>,
    ) -> TCResult<TxnHandle> {
        let txn_id = self.allocate_id().await?;
        self.observe(txn_id);
        self.handle(txn_id, true, auth, runtime)
    }

    fn handle(
        &self,
        id: TxnId,
        autocommit: bool,
        context: Option<&TokenContext>,
        runtime: Arc<crate::kernel::HostRuntime>,
    ) -> TCResult<TxnHandle> {
        let mut protocol = crate::cluster::ClaimState {
            signed: None,
            coordinator: None,
            resources: BTreeMap::new(),
            autocommit,
        };
        if let Some(context) = context {
            protocol.signed = context.signed.clone();
        }
        Ok(TxnHandle {
            id,
            claim: Claim::new(
                crate::uri::transaction_path(id)
                    .parse()
                    .expect("transaction path"),
                umask::Mode::new(),
            ),
            server: self.clone(),
            runtime,
            scope: super::handle::ExecutionScope::Host,
            auth_context: context.map(AuthContext::from_token_context),
            protocol_claims: Arc::new(parking_lot::Mutex::new(protocol)),
            workspace_path: Vec::new(),
            deadline: self.state.config.resources.deadline(),
            graph_admitted: false,
        })
    }

    fn observe(&self, txn_id: TxnId) {
        let expires = expiry(txn_id, self.state.config.ttl, self.state.config.grace);
        self.state
            .inner
            .lock()
            .active
            .entry(txn_id)
            .or_insert(Active {
                expires,
                retry: None,
            });
        self.state.notify.notify_one();
    }

    async fn allocate_id(&self) -> TCResult<TxnId> {
        let _allocation = self.state.allocation.lock().await;
        let txn_id = {
            let inner = self.state.inner.lock();
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .min(u128::from(u64::MAX)) as u64;
            let floor = inner
                .last_allocated
                .into_iter()
                .chain(inner.latest_finalized)
                .max();
            let (timestamp, nonce) = match floor {
                Some(floor) if now <= floor.timestamp().as_nanos() => {
                    if floor.nonce() == u16::MAX {
                        (floor.timestamp().as_nanos().saturating_add(1), 0)
                    } else {
                        (floor.timestamp().as_nanos(), floor.nonce() + 1)
                    }
                }
                _ => (now, 0),
            };
            let timestamp = NetworkTime::from_nanos(timestamp);
            TxnId::from_parts(timestamp, nonce).with_trace(compute_trace(
                &self.state.config.host_id,
                timestamp,
                nonce,
            ))
        };
        if let Err(error) = self
            .state
            .config
            .workspace
            .update_host_frontier(None, Some(txn_id))
            .await
        {
            self.state.ready.store(false, Ordering::Release);
            if let Ok(Some(host)) = self.state.config.workspace.host_record().await {
                let mut inner = self.state.inner.lock();
                inner.latest_finalized = host.latest_finalized;
                inner.last_allocated = host.last_allocated;
                self.state.ready.store(true, Ordering::Release);
            }
            return Err(error);
        }
        self.state.inner.lock().last_allocated = Some(txn_id);
        Ok(txn_id)
    }

    pub fn start_expiry(
        &self,
        runtime: &tokio::runtime::Handle,
        host: Arc<crate::kernel::HostRuntime>,
    ) {
        if self.state.worker_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let server = self.clone();
        runtime.spawn(async move { server.run_expiry(host).await });
    }

    async fn run_expiry(self, host: Arc<crate::kernel::HostRuntime>) {
        loop {
            match self.next_expiry() {
                Some(deadline) => tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => self.expire_due(Instant::now(), &host).await,
                    _ = self.state.notify.notified() => {}
                },
                None => self.state.notify.notified().await,
            }
        }
    }

    fn next_expiry(&self) -> Option<Instant> {
        self.state
            .inner
            .lock()
            .active
            .first_key_value()
            .map(|(_, active)| active.retry.unwrap_or(active.expires))
    }

    async fn expire_due(&self, now: Instant, host: &crate::kernel::HostRuntime) {
        let due = self
            .state
            .inner
            .lock()
            .active
            .first_key_value()
            .and_then(|(id, active)| {
                (active.retry.unwrap_or(active.expires) <= now).then_some(*id)
            });
        let Some(cutoff) = due else { return };
        if let Err(error) = host.finalize(&cutoff).await {
            self.defer(cutoff, "finalize resources", error);
            return;
        }
        if let Err(error) = self
            .state
            .config
            .workspace
            .update_host_frontier(Some(cutoff), None)
            .await
        {
            self.defer(cutoff, "persist finalized cutoff", error);
            return;
        }
        if let Err(error) = self.state.config.workspace.remove_through(cutoff).await {
            self.defer(cutoff, "clean transaction workspaces", error);
            return;
        }
        let mut inner = self.state.inner.lock();
        inner.latest_finalized = Some(cutoff);
        inner.active.retain(|id, _| *id > cutoff);
        self.state.ready.store(true, Ordering::Release);
        drop(inner);
        self.state.notify.notify_one();
    }

    fn defer(&self, cutoff: TxnId, operation: &str, error: impl std::fmt::Display) {
        log::error!("failed to {operation} at transaction cutoff {cutoff}: {error}");
        self.state.ready.store(false, Ordering::Release);
        if let Some(active) = self.state.inner.lock().active.get_mut(&cutoff) {
            active.retry = Some(Instant::now() + Duration::from_secs(1));
        }
        self.state.notify.notify_one();
    }

    pub(crate) async fn recover(&self) -> TCResult<()> {
        let host = self
            .state
            .config
            .workspace
            .host_record()
            .await?
            .ok_or_else(|| TCError::internal("missing workspace protocol authority"))?;
        let workspaces = self.state.config.workspace.transaction_ids().await?;
        {
            let mut inner = self.state.inner.lock();
            inner.latest_finalized = host.latest_finalized;
            inner.last_allocated = host.last_allocated;
            for txn_id in workspaces.iter().copied() {
                if host.latest_finalized.is_none_or(|cutoff| txn_id > cutoff) {
                    inner.active.insert(
                        txn_id,
                        Active {
                            expires: expiry(txn_id, self.state.config.ttl, self.state.config.grace),
                            retry: None,
                        },
                    );
                }
            }
        }
        if let Some(cutoff) = host.latest_finalized {
            self.state.config.workspace.remove_through(cutoff).await?;
        }
        self.state.ready.store(true, Ordering::Release);
        self.state.notify.notify_one();
        Ok(())
    }

    fn reject_finalized(&self, txn_id: TxnId) -> TCResult<()> {
        if self
            .state
            .inner
            .lock()
            .latest_finalized
            .is_some_and(|cutoff| txn_id <= cutoff)
        {
            Err(TCError::conflict(format!(
                "transaction {txn_id} is at or below the finalized frontier"
            )))
        } else {
            Ok(())
        }
    }

    fn reject_expired(&self, txn_id: TxnId) -> TCResult<()> {
        if expiry(txn_id, self.state.config.ttl, self.state.config.grace) <= Instant::now() {
            Err(TCError::conflict(format!(
                "transaction {txn_id} has expired"
            )))
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    pub(crate) fn protocol_host(&self) -> &Link {
        &self.state.config.protocol_host
    }

    #[cfg(test)]
    pub(crate) fn protocol_actor(&self) -> &Arc<crate::auth::Actor> {
        &self.state.config.protocol_actor
    }

    #[cfg(test)]
    pub(crate) fn test_decision_bearer(&self, txn: &TxnHandle) -> String {
        txn.lock_for_test()
            .expect("lock test transaction")
            .raw_token()
            .expect("decision bearer")
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, txn_id: &TxnId) -> bool {
        self.state.inner.lock().active.contains_key(txn_id)
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.state.ready.load(Ordering::Acquire)
    }
}

fn expiry(txn_id: TxnId, ttl: Duration, grace: Duration) -> Instant {
    let nanos = txn_id
        .timestamp()
        .as_nanos()
        .saturating_add(ttl.as_nanos().min(u128::from(u64::MAX)) as u64)
        .saturating_add(grace.as_nanos().min(u128::from(u64::MAX)) as u64);
    let remaining = UNIX_EPOCH
        .checked_add(Duration::from_nanos(nanos))
        .and_then(|expiry| expiry.duration_since(SystemTime::now()).ok())
        .unwrap_or_default();
    Instant::now() + remaining
}

fn compute_trace(host_id: &str, timestamp: NetworkTime, nonce: u16) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(host_id.as_bytes());
    hasher.update(timestamp.as_nanos().to_be_bytes());
    hasher.update(nonce.to_be_bytes());
    hasher.finalize().into()
}

#[cfg(test)]
pub(super) fn test_verifier(config: &TxnConfig) -> Arc<dyn TokenVerifier> {
    let actor = rjwt::Actor::with_verifying_key(
        config.protocol_actor.id().clone(),
        config.protocol_actor.verifying_key(),
    );
    let keyring = crate::auth::KeyringActorResolver::default()
        .with_actor(config.protocol_host.clone(), actor);
    Arc::new(crate::auth::RjwtTokenVerifier::new(Arc::new(keyring)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn activity_does_not_extend_the_identity_derived_expiry() {
        let kernel = super::super::test_kernel_with(
            "fixed-expiry",
            Duration::from_millis(40),
            super::super::test_workspace("fixed-expiry"),
        )
        .await;
        let server = kernel.txn_server();
        let txn = kernel.test_txn().await;
        let first = server.state.inner.lock().active[&txn.id()].expires;
        tokio::time::sleep(Duration::from_millis(5)).await;
        server.observe(txn.id());
        assert_eq!(server.state.inner.lock().active[&txn.id()].expires, first);
    }

    #[tokio::test]
    async fn concurrent_allocation_persists_the_latest_identity() {
        let kernel = super::super::test_kernel("concurrent-allocation").await;
        let (first, second) = tokio::join!(kernel.test_txn(), kernel.test_txn());
        let latest = std::cmp::max(first.id(), second.id());
        let record = kernel
            .txn_server()
            .state
            .config
            .workspace
            .host_record()
            .await
            .expect("host record")
            .expect("durable host frontier");
        assert_eq!(record.last_allocated, Some(latest));
    }
}
