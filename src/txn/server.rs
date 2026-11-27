use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::FutureExt;
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use tc_error::{TCError, TCResult};
use tc_ir::{NetworkTime, TxnId};
use tokio::time::Instant;

use crate::auth::{AuthContext, TokenVerifier};
use super::{TxnConfig, TxnHandle, validate_signed_token};

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
    stop: tokio_util::sync::CancellationToken,
    worker: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

struct Inner {
    active: BTreeMap<TxnId, Active>,
    latest_finalized: Option<TxnId>,
    last_allocated: Option<TxnId>,
}

struct Active {
    calls: tokio_util::task::TaskTracker,
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
                stop: tokio_util::sync::CancellationToken::new(),
                worker: tokio::sync::Mutex::new(None),
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
                let context = token
                    .map(|auth| {
                        let claims = auth
                            .signed()
                            .map(|signed| super::protocol_snapshot(txn_id, signed))
                            .transpose()?;

                        Ok::<_, TCError>((auth, claims.is_some_and(|claims| claims.read_only)))
                    })
                    .transpose()?;

                self.handle(txn_id, true, context, kernel, false)
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

                let claims = validate_signed_token(txn_id, signed)
                    .map_err(|_| TCError::unauthorized("invalid transaction authority"))?;

                let context = Some((token, claims.read_only));
                self.handle(txn_id, false, context, kernel, false)
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
        self.handle(txn_id, false, None, kernel, false)
    }

    /// Only startup recovery of validated Chain records may bind expired IDs.
    /// This capability never crosses the request authentication boundary.
    pub(crate) fn bind_recovery(
        &self,
        txn_id: TxnId,
        kernel: Arc<crate::kernel::KernelInner>,
    ) -> TCResult<TxnHandle> {
        use tc_collection::StorageContext;

        if self.is_ready() {
            return Err(TCError::conflict("recovery requires an unpublished host"));
        }
        self.reject_finalized(txn_id)?;
        let mut txn = self.handle(txn_id, false, None, kernel, true)?;
        // Startup replay is an unpublished inspection/recovery capability, not
        // ordinary host execution. Its errors must prevent publication.
        txn.recovering = true;
        Ok(txn.subcontext_unique())
    }

    pub(crate) async fn finish_recovery(&self) -> TCResult<()> {
        let cutoff = self.state.inner.lock().latest_finalized;
        if let Some(cutoff) = cutoff {
            self.state.config.workspace().remove_through(cutoff).await?;
        }
        self.state.ready.store(true, Ordering::Release);
        self.state.notify.notify_one();
        Ok(())
    }

    fn handle(
        &self,
        id: TxnId,
        autocommit: bool,
        context: Option<(&AuthContext, bool)>,
        kernel: Arc<crate::kernel::KernelInner>,
        recovering: bool,
    ) -> TCResult<TxnHandle> {
        let (expires, activity) = {
            let mut inner = self.state.inner.lock();
            let active = self.admit(&mut inner, id)?;
            if active.calls.is_closed() || (!recovering && active.expires <= Instant::now()) {
                return Err(TCError::conflict("transaction has expired"));
            }

            let expires = if recovering {
                self.resources().deadline().instant()
            } else {
                active.expires
            };

            (expires, active.calls.token())
        };

        self.state.notify.notify_one();
        Ok(TxnHandle::new(
            id,
            self.clone(),
            kernel,
            context,
            autocommit,
            expires,
            activity,
        ))
    }

    fn admit<'a>(&self, inner: &'a mut Inner, id: TxnId) -> TCResult<&'a mut Active> {
        if inner.latest_finalized.is_some_and(|cutoff| id <= cutoff) {
            return Err(TCError::conflict("transaction is already finalized"));
        }

        Ok(inner.active.entry(id).or_insert_with(|| Active {
            calls: tokio_util::task::TaskTracker::new(),
            expires: expiry(id, self.state.config.ttl(), self.state.config.grace()),
            retry: None,
        }))
    }

    pub(crate) async fn allocate(&self) -> TCResult<TxnId> {
        let txn_id = self.allocate_id().await?;
        self.admit(&mut self.state.inner.lock(), txn_id)?;
        self.state.notify.notify_one();
        Ok(txn_id)
    }

    /// Allocate the ordinary read transaction used by unpublished kernel construction.
    pub(crate) async fn allocate_bootstrap(&self) -> TCResult<TxnId> {
        if self.is_ready() {
            return Err(TCError::conflict("bootstrap requires an unpublished host"));
        }

        let txn_id = self.allocate_id().await?;
        self.admit(&mut self.state.inner.lock(), txn_id)?;
        self.state.notify.notify_one();
        Ok(txn_id)
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
            if error.requires_recovery() && self.is_ready() {
                self.fatal_recovery(format!(
                    "transaction frontier publication failed: {error}; causes: {:?}",
                    error.causes()
                ));
            }

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
        Ok(txn_id)
    }

    pub(crate) fn start_expiry<F, Fut>(&self, runtime: &tokio::runtime::Handle, finalize: F)
    where
        F: Fn(TxnId) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = TCResult<()>> + Send + 'static,
    {
        let mut worker = self
            .state
            .worker
            .try_lock()
            .expect("expiry starts before shutdown");

        assert!(worker.is_none(), "expiry starts once");
        let server = self.clone();
        *worker = Some(runtime.spawn(async move {
            if std::panic::AssertUnwindSafe(server.run_expiry(finalize))
                .catch_unwind()
                .await
                .is_err()
            {
                server.fatal_recovery("canonical recovery required: finalizer worker panicked");
            }
        }));
    }

    pub(crate) async fn stop_expiry(&self) {
        self.state.stop.cancel();
        let mut worker = self.state.worker.lock().await;
        if let Some(task) = worker.as_mut() {
            // Borrow the handle until completion: cancelling this waiter must not
            // detach a worker which another shutdown caller still needs to join.
            if let Err(error) = task.await {
                self.fatal_recovery(format!(
                    "canonical recovery required: finalizer worker failed: {error}"
                ));
            }

            *worker = None;
        }
    }

    async fn run_expiry<F, Fut>(&self, finalize: F)
    where
        F: Fn(TxnId) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = TCResult<()>> + Send,
    {
        loop {
            match self.next_expiry() {
                Some(deadline) => tokio::select! {
                    biased;
                    _ = self.state.stop.cancelled() => return,
                    _ = tokio::time::sleep_until(deadline) => self.expire_due(Instant::now(), &finalize).await,
                    _ = self.state.notify.notified() => {}
                },
                None => tokio::select! {
                    biased;
                    _ = self.state.stop.cancelled() => return,
                    _ = self.state.notify.notified() => {}
                },
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
        let due = {
            let inner = self.state.inner.lock();
            inner.active.first_key_value().and_then(|(id, active)| {
                if active.retry.unwrap_or(active.expires) > now {
                    return None;
                }

                // Binding and closure share this mutex. Existing capability clones
                // retain tokens; no later binding can enter this transaction.
                active.calls.close();
                Some((*id, active.calls.clone()))
            })
        };

        let Some((cutoff, calls)) = due else { return };

        // Shutdown may stop this ownership wait, but never native materialization.
        tokio::select! {
            biased;
            _ = self.state.stop.cancelled() => return,
            _ = calls.wait() => {}
        }

        // Coalesce only the expired, quiescent prefix. Binding and closure share
        // this mutex; an active or not-yet-due identity remains a hard barrier.
        let cutoff = {
            let inner = self.state.inner.lock();
            let mut last = cutoff;
            for (&id, active) in inner.active.range(cutoff..) {
                if active.retry.unwrap_or(active.expires) > now || !active.calls.is_empty() {
                    break;
                }

                active.calls.close();
                last = id;
            }

            last
        };

        if let Err(error) = finalize(cutoff).await {
            self.fatal_recovery(format!(
                "canonical recovery required: failed to finalize resources at {cutoff}: {error}; causes: {:?}",
                error.causes()
            ));
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

    // Failed cleanup or materialization requires recovery. A task panic alone
    // would leave the host serving requests against uncertain state.
    pub(crate) fn fatal_recovery(&self, error: impl std::fmt::Display) -> ! {
        use std::io::Write;

        self.state.ready.store(false, Ordering::Release);

        // Fatal diagnostics must survive an uninitialized logging backend.
        let _ = writeln!(std::io::stderr().lock(), "{error}");
        std::process::abort()
    }

    fn defer(&self, cutoff: TxnId, operation: &str, error: impl std::fmt::Display) {
        log::error!("failed to {operation} at transaction cutoff {cutoff}: {error}");
        self.state.ready.store(false, Ordering::Release);
        if let Some((_, active)) = self.state.inner.lock().active.range_mut(..=cutoff).next() {
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
                    self.admit(&mut inner, txn_id)?;
                }
            }
        }

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
pub(super) use tests::test_verifier;

#[cfg(test)]
#[path = "../../tests/support/txn_server.rs"]
pub(crate) mod tests;
