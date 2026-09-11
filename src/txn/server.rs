use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use tc_error::{TCError, TCResult};
use tc_ir::{NetworkTime, TxnId};

use tokio::time::Instant;

use super::{TxnConfig, TxnHandle, validate_signed_token};
use crate::auth::{AuthContext, TokenVerifier};

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
        self.state.config.resources()
    }

    pub(super) fn workspace(&self) -> &crate::Workspace {
        self.state.config.workspace()
    }

    pub(super) fn ttl(&self) -> Duration {
        self.state.config.ttl()
    }

    pub(crate) fn protocol_authority(&self) -> &crate::ProtocolAuthority {
        self.state.config.protocol()
    }

    pub(crate) fn verifier(&self) -> &Arc<dyn TokenVerifier> {
        &self.state.verifier
    }

    pub(crate) async fn load(
        config: TxnConfig,
        verifier: Arc<dyn TokenVerifier>,
    ) -> TCResult<Self> {
        let server = Self {
            state: Arc::new(TxnState {
                config,
                allocation: tokio::sync::Mutex::new(()),
                inner: Mutex::new(Inner {
                    active: BTreeMap::new(),
                    latest_finalized: None,
                    last_allocated: None,
                }),
                notify: tokio::sync::Notify::new(),
                ready: AtomicBool::new(false),
                verifier,
            }),
        };
        server.restore_frontier().await?;
        Ok(server)
    }

    pub(crate) async fn bind(
        &self,
        txn_id: Option<TxnId>,
        token: Option<&AuthContext>,
        kernel: Arc<crate::kernel::KernelInner>,
    ) -> TCResult<TxnHandle> {
        if !self.is_ready() {
            return Err(TCError::new(
                tc_error::ErrorKind::Unavailable,
                "transaction storage is not ready",
            ));
        }
        match txn_id {
            None => {
                let txn_id = self.allocate().await?;
                self.handle(txn_id, true, token, kernel)
            }
            Some(txn_id) => {
                self.reject_finalized(txn_id)?;
                self.reject_expired(txn_id)?;
                let token =
                    token.ok_or_else(|| TCError::unauthorized("missing transaction authority"))?;
                let signed = token
                    .signed()
                    .map(Arc::as_ref)
                    .ok_or_else(|| TCError::unauthorized("missing signed transaction authority"))?;
                validate_signed_token(txn_id, signed)
                    .map_err(|_| TCError::unauthorized("invalid transaction authority"))?;
                self.observe(txn_id);
                self.handle(txn_id, false, Some(token), kernel)
            }
        }
    }

    pub(crate) fn bind_seed(
        &self,
        txn_id: TxnId,
        kernel: Arc<crate::kernel::KernelInner>,
    ) -> TCResult<TxnHandle> {
        self.reject_finalized(txn_id)?;
        self.reject_expired(txn_id)?;
        self.observe(txn_id);
        self.handle(txn_id, false, None, kernel)
    }

    fn handle(
        &self,
        id: TxnId,
        autocommit: bool,
        context: Option<&AuthContext>,
        kernel: Arc<crate::kernel::KernelInner>,
    ) -> TCResult<TxnHandle> {
        TxnHandle::new(id, self.clone(), kernel, context, autocommit)
    }

    fn observe(&self, txn_id: TxnId) {
        let expires = expiry(txn_id, self.state.config.ttl(), self.state.config.grace());
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

    pub(crate) async fn allocate(&self) -> TCResult<TxnId> {
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
                self.state.config.protocol().actor_id(),
                timestamp,
                nonce,
            ))
        };
        if let Err(error) = self
            .state
            .config
            .workspace()
            .write_last_allocated(txn_id)
            .await
        {
            self.state.ready.store(false, Ordering::Release);
            if let Ok((latest_finalized, last_allocated)) =
                self.state.config.workspace().frontiers().await
            {
                let mut inner = self.state.inner.lock();
                inner.latest_finalized = latest_finalized;
                inner.last_allocated = last_allocated;
                self.state.ready.store(true, Ordering::Release);
            }
            return Err(error);
        }
        self.state.inner.lock().last_allocated = Some(txn_id);
        self.observe(txn_id);
        Ok(txn_id)
    }

    pub(crate) fn start_expiry<F, Fut>(&self, runtime: &tokio::runtime::Handle, finalize: F)
    where
        F: Fn(TxnId) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = TCResult<()>> + Send + 'static,
    {
        let server = self.clone();
        runtime.spawn(async move { server.run_expiry(finalize).await });
    }

    async fn run_expiry<F, Fut>(self, finalize: F)
    where
        F: Fn(TxnId) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = TCResult<()>> + Send,
    {
        loop {
            match self.next_expiry() {
                Some(deadline) => tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => self.expire_due(Instant::now(), &finalize).await,
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

    async fn expire_due<F, Fut>(&self, now: Instant, finalize: &F)
    where
        F: Fn(TxnId) -> Fut,
        Fut: std::future::Future<Output = TCResult<()>>,
    {
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
        if let Err(error) = finalize(cutoff).await {
            self.defer(cutoff, "finalize resources", error);
            return;
        }
        if let Err(error) = self
            .state
            .config
            .workspace()
            .write_latest_finalized(cutoff)
            .await
        {
            self.defer(cutoff, "persist finalized cutoff", error);
            return;
        }
        if let Err(error) = self.state.config.workspace().remove_through(cutoff).await {
            self.defer(cutoff, "clean transaction workspaces", error);
            return;
        }
        {
            let mut inner = self.state.inner.lock();
            inner.latest_finalized = Some(cutoff);
            inner.active.retain(|id, _| *id > cutoff);
            self.state.ready.store(true, Ordering::Release);
        }
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

    async fn restore_frontier(&self) -> TCResult<()> {
        let (latest_finalized, last_allocated) = self.state.config.workspace().frontiers().await?;
        let workspaces = self.state.config.workspace().transaction_ids().await?;
        {
            let mut inner = self.state.inner.lock();
            inner.latest_finalized = latest_finalized;
            inner.last_allocated = last_allocated;
            for txn_id in workspaces.iter().copied() {
                if latest_finalized.is_none_or(|cutoff| txn_id > cutoff) {
                    inner.active.insert(
                        txn_id,
                        Active {
                            expires: expiry(
                                txn_id,
                                self.state.config.ttl(),
                                self.state.config.grace(),
                            ),
                            retry: None,
                        },
                    );
                }
            }
        }
        if let Some(cutoff) = latest_finalized {
            self.state.config.workspace().remove_through(cutoff).await?;
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
        if expiry(txn_id, self.state.config.ttl(), self.state.config.grace()) <= Instant::now() {
            Err(TCError::conflict(format!(
                "transaction {txn_id} has expired"
            )))
        } else {
            Ok(())
        }
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
        config.protocol().actor_id().to_string(),
        config.protocol().verifying_key(),
    );
    let keyring = crate::auth::KeyringActorResolver::default();
    keyring
        .insert(config.protocol().host().clone(), actor)
        .expect("test protocol actor is unique");
    Arc::new(crate::auth::RjwtTokenVerifier::new(Arc::new(keyring)))
}

#[cfg(test)]
#[path = "../../tests/support/txn_server.rs"]
mod tests;
