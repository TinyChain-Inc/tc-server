use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use tc_error::{Pressure, PressureReason, TCError, TCResult};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

const MIB: usize = 1024 * 1024;
const GIB: u64 = 1024 * 1024 * 1024;

/// Finite host capacity configured once at bootstrap.
#[derive(Clone, Debug)]
pub struct HostLimits {
    pub transaction_ttl: Duration,
    pub ingress: IngressLimits,
    pub execution: ExecutionLimits,
    pub storage: StorageLimits,
    pub device: DeviceLimits,
}

#[derive(Clone, Debug)]
pub struct IngressLimits {
    pub request_body_bytes: usize,
    pub artifact_body_bytes: usize,
    pub in_flight_requests: usize,
    pub active_connections: usize,
}

#[derive(Clone, Debug)]
pub struct ExecutionLimits {
    pub request_deadline: Duration,
    pub parallel_graph_ops: usize,
    pub outbound_requests: usize,
}

#[derive(Clone, Debug)]
pub struct StorageLimits {
    pub collection_cache_bytes: usize,
    pub library_cache_bytes: usize,
    pub collection_file_handles: usize,
    pub library_file_handles: usize,
    pub minimum_free_disk_bytes: u64,
    pub cache_wait: Duration,
}

#[derive(Clone, Debug)]
pub struct DeviceLimits {
    pub materialized_tensor_bytes: usize,
    pub operations_per_device: usize,
}

impl Default for HostLimits {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1);
        Self {
            transaction_ttl: Duration::from_secs(3),
            ingress: IngressLimits {
                request_body_bytes: MIB,
                artifact_body_bytes: 64 * MIB,
                in_flight_requests: 8.max(4 * cpus),
                active_connections: 1024,
            },
            execution: ExecutionLimits {
                request_deadline: Duration::from_secs(3),
                parallel_graph_ops: cpus,
                outbound_requests: 4.max(2 * cpus),
            },
            storage: StorageLimits {
                collection_cache_bytes: 64 * MIB,
                library_cache_bytes: 64 * MIB,
                collection_file_handles: 1024,
                library_file_handles: 1024,
                minimum_free_disk_bytes: GIB,
                cache_wait: Duration::from_secs(3),
            },
            device: DeviceLimits {
                materialized_tensor_bytes: 256 * MIB,
                operations_per_device: 1,
            },
        }
    }
}

/// The one absolute deadline inherited by all work in a request.
#[derive(Clone, Copy, Debug)]
pub struct Deadline(Instant);

impl Deadline {
    pub fn after(duration: Duration) -> Self {
        Self(Instant::now() + duration)
    }

    pub fn instant(self) -> Instant {
        self.0
    }

    pub fn is_expired(self) -> bool {
        Instant::now() >= self.0
    }

    pub fn remaining(self) -> Duration {
        self.0.saturating_duration_since(Instant::now())
    }

    pub async fn wait<F: Future>(self, future: F) -> TCResult<F::Output> {
        tokio::time::timeout_at(self.0, future)
            .await
            .map_err(|_| self.exceeded())
    }

    pub async fn run<F, T>(self, future: F) -> TCResult<T>
    where
        F: Future<Output = TCResult<T>>,
    {
        self.wait(future).await?
    }

    pub fn exceeded(self) -> TCError {
        TCError::resource_unavailable(
            "request deadline exceeded",
            Pressure::new("/host/resource/request", PressureReason::Saturated)
                .with_retry_after_ms(1000),
        )
    }
}

/// A point-in-time view produced by the resource which owns the capacity.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct CapacitySnapshot {
    pub resource: String,
    pub limit: usize,
    pub in_flight: usize,
    pub wait_count: u64,
    pub wait_time_ms: u64,
    pub rejection_count: u64,
    pub best_effort_drop_count: u64,
}

#[derive(Clone)]
pub struct HostResources {
    inner: Arc<HostResourcesInner>,
}

struct HostResourcesInner {
    limits: HostLimits,
    requests: Arc<Capacity>,
    connections: Arc<Capacity>,
    graph: Arc<Capacity>,
    outbound: Arc<Capacity>,
    devices: Mutex<BTreeMap<String, Arc<Capacity>>>,
}

impl HostResources {
    pub fn new(limits: HostLimits) -> Self {
        let requests = Capacity::new("/host/resource/request", limits.ingress.in_flight_requests);
        let connections = Capacity::new(
            "/host/resource/connection",
            limits.ingress.active_connections,
        );
        let graph = Capacity::new("/host/resource/graph", limits.execution.parallel_graph_ops);
        let outbound = Capacity::new("/host/resource/rpc", limits.execution.outbound_requests);
        Self {
            inner: Arc::new(HostResourcesInner {
                limits,
                requests,
                connections,
                graph,
                outbound,
                devices: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    pub fn limits(&self) -> &HostLimits {
        &self.inner.limits
    }

    pub fn deadline(&self) -> Deadline {
        Deadline::after(self.inner.limits.execution.request_deadline)
    }

    pub async fn admit_request(&self, deadline: Deadline) -> TCResult<CapacityPermit> {
        self.inner.requests.clone().acquire(deadline).await
    }

    pub async fn admit_connection(&self, deadline: Deadline) -> TCResult<CapacityPermit> {
        self.inner.connections.clone().acquire(deadline).await
    }

    pub async fn admit_graph_op(&self, deadline: Deadline) -> TCResult<CapacityPermit> {
        self.inner.graph.clone().acquire(deadline).await
    }

    pub async fn admit_outbound(&self, deadline: Deadline) -> TCResult<CapacityPermit> {
        self.inner.outbound.clone().acquire(deadline).await
    }

    pub async fn admit_device(
        &self,
        device: impl Into<String>,
        deadline: Deadline,
    ) -> TCResult<CapacityPermit> {
        let resource = format!("/host/resource/device/{}", device.into());
        let capacity = self
            .inner
            .devices
            .lock()
            .entry(resource.clone())
            .or_insert_with(|| {
                Capacity::new(resource, self.inner.limits.device.operations_per_device)
            })
            .clone();
        capacity.acquire(deadline).await
    }

    pub fn snapshots(&self) -> Vec<CapacitySnapshot> {
        let mut snapshots = vec![
            self.inner.requests.snapshot(),
            self.inner.connections.snapshot(),
            self.inner.graph.snapshot(),
            self.inner.outbound.snapshot(),
        ];
        snapshots.extend(
            self.inner
                .devices
                .lock()
                .values()
                .map(|item| item.snapshot()),
        );
        snapshots
    }
}

impl Default for HostResources {
    fn default() -> Self {
        Self::new(HostLimits::default())
    }
}

struct Capacity {
    resource: String,
    limit: usize,
    semaphore: Arc<Semaphore>,
    in_flight: AtomicUsize,
    wait_count: AtomicU64,
    wait_nanos: AtomicU64,
    rejection_count: AtomicU64,
    best_effort_drop_count: AtomicU64,
}

impl Capacity {
    fn new(resource: impl Into<String>, limit: usize) -> Arc<Self> {
        assert!(limit > 0, "resource capacity must be nonzero");
        Arc::new(Self {
            resource: resource.into(),
            limit,
            semaphore: Arc::new(Semaphore::new(limit)),
            in_flight: AtomicUsize::new(0),
            wait_count: AtomicU64::new(0),
            wait_nanos: AtomicU64::new(0),
            rejection_count: AtomicU64::new(0),
            best_effort_drop_count: AtomicU64::new(0),
        })
    }

    async fn acquire(self: Arc<Self>, deadline: Deadline) -> TCResult<CapacityPermit> {
        let permit = match Arc::clone(&self.semaphore).try_acquire_owned() {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                self.wait_count.fetch_add(1, Ordering::Relaxed);
                let started = Instant::now();
                let result = deadline
                    .wait(Arc::clone(&self.semaphore).acquire_owned())
                    .await;
                let nanos = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                self.wait_nanos.fetch_add(nanos, Ordering::Relaxed);
                match result {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(_)) | Err(_) => return Err(self.rejected()),
                }
            }
            Err(tokio::sync::TryAcquireError::Closed) => return Err(self.rejected()),
        };
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        Ok(CapacityPermit {
            owner: self,
            _permit: permit,
        })
    }

    fn rejected(&self) -> TCError {
        self.rejection_count.fetch_add(1, Ordering::Relaxed);
        TCError::resource_unavailable(
            format!("{} is at capacity", self.resource),
            Pressure::new(&self.resource, PressureReason::Saturated).with_retry_after_ms(1000),
        )
    }

    fn snapshot(&self) -> CapacitySnapshot {
        CapacitySnapshot {
            resource: self.resource.clone(),
            limit: self.limit,
            in_flight: self.in_flight.load(Ordering::Relaxed),
            wait_count: self.wait_count.load(Ordering::Relaxed),
            wait_time_ms: self.wait_nanos.load(Ordering::Relaxed) / 1_000_000,
            rejection_count: self.rejection_count.load(Ordering::Relaxed),
            best_effort_drop_count: self.best_effort_drop_count.load(Ordering::Relaxed),
        }
    }
}

pub struct CapacityPermit {
    owner: Arc<Capacity>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for CapacityPermit {
    fn drop(&mut self) {
        self.owner.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn admission_waits_and_recovers_after_release() {
        let capacity = Capacity::new("/host/resource/test", 1);
        let first = capacity
            .clone()
            .acquire(Deadline::after(Duration::from_secs(1)))
            .await
            .unwrap();
        let waiting = capacity
            .clone()
            .acquire(Deadline::after(Duration::from_secs(1)));
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
        drop(first);
        let second = waiting.await.unwrap();
        assert_eq!(capacity.snapshot().in_flight, 1);
        drop(second);
        assert_eq!(capacity.snapshot().in_flight, 0);
    }

    #[tokio::test]
    async fn admission_rejects_at_deadline() {
        let capacity = Capacity::new("/host/resource/test", 1);
        let _first = capacity
            .clone()
            .acquire(Deadline::after(Duration::from_secs(1)))
            .await
            .unwrap();
        let result = capacity
            .clone()
            .acquire(Deadline::after(Duration::from_millis(1)))
            .await;
        let Err(err) = result else {
            panic!("admission unexpectedly succeeded");
        };
        assert_eq!(err.code(), tc_error::ErrorKind::Unavailable);
        assert_eq!(err.pressure().unwrap().resource(), "/host/resource/test");
        assert_eq!(capacity.snapshot().rejection_count, 1);
    }

    #[test]
    fn deadline_is_one_absolute_budget() {
        let deadline = Deadline::after(Duration::from_secs(1));
        assert_eq!(deadline.instant(), deadline.instant());
        assert!(!deadline.is_expired());
        assert!(deadline.remaining() <= Duration::from_secs(1));
    }

    #[tokio::test]
    async fn deadline_run_flattens_results_and_preserves_timeout_errors() {
        let value = Deadline::after(Duration::from_secs(1))
            .run(async { Ok::<_, TCError>(42) })
            .await
            .expect("completed operation");
        assert_eq!(value, 42);

        let err = Deadline::after(Duration::from_millis(1))
            .run(async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok::<_, TCError>(())
            })
            .await
            .expect_err("deadline");
        assert_eq!(err.code(), tc_error::ErrorKind::Unavailable);
        assert_eq!(
            err.pressure().expect("pressure").resource(),
            "/host/resource/request"
        );
    }
}
