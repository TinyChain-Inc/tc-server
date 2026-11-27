use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

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
}

#[derive(Clone, Debug)]
pub struct IngressLimits {
    pub request_body_bytes: usize,
    pub application_body_bytes: usize,
    pub in_flight_requests: usize,
    pub active_connections: usize,
    pub application_in_flight_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct ExecutionLimits {
    pub request_deadline: Duration,
    pub parallel_graph_ops: usize,
    pub outbound_requests: usize,
    pub max_graph_nodes: usize,
    pub max_graph_edges: usize,
    pub max_execution_depth: usize,
    pub max_op_invocations: usize,
    pub materialized_tensor_bytes: usize,
    pub wasm_fuel_per_call: u64,
    pub wasm_memory_bytes: usize,
    pub wasm_result_bytes: usize,
    pub wasm_compilations: usize,
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

impl Default for HostLimits {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1);
        Self {
            transaction_ttl: Duration::from_secs(3),
            ingress: IngressLimits {
                request_body_bytes: MIB,
                application_body_bytes: 64 * MIB,
                in_flight_requests: 8.max(4 * cpus),
                active_connections: 1024,
                application_in_flight_bytes: 256 * MIB,
            },
            execution: ExecutionLimits {
                request_deadline: Duration::from_secs(3),
                parallel_graph_ops: cpus,
                outbound_requests: 4.max(2 * cpus),
                max_graph_nodes: 4_096,
                max_graph_edges: 16_384,
                max_execution_depth: 64,
                max_op_invocations: 10_000,
                materialized_tensor_bytes: 256 * MIB,
                wasm_fuel_per_call: 10_000_000,
                wasm_memory_bytes: 64 * MIB,
                wasm_result_bytes: MIB,
                wasm_compilations: 1.max(cpus / 2),
            },
            storage: StorageLimits {
                collection_cache_bytes: 64 * MIB,
                library_cache_bytes: 64 * MIB,
                collection_file_handles: 1024,
                library_file_handles: 1024,
                minimum_free_disk_bytes: GIB,
                cache_wait: Duration::from_secs(3),
            },
        }
    }
}

/// The one absolute deadline inherited by all work in a request.
#[derive(Clone, Copy, Debug)]
pub struct Deadline(Instant);

impl Deadline {
    pub(crate) fn at(instant: Instant) -> Self {
        Self(instant)
    }

    /// Construct a native deadline. `duration` must fit the monotonic clock;
    /// host configuration validates this before admitting requests.
    pub fn after(duration: Duration) -> Self {
        Self(Instant::now() + duration)
    }

    pub fn instant(self) -> Instant {
        self.0
    }

    pub fn is_expired(self) -> bool {
        Instant::now() >= self.0
    }

    /// Reject synchronous work which has already exhausted its deadline.
    pub fn check(self) -> TCResult<()> {
        if self.is_expired() {
            Err(self.exceeded())
        } else {
            Ok(())
        }
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
#[derive(Clone, Debug, Eq, PartialEq)]
struct CapacitySnapshot {
    resource: String,
    limit: usize,
    in_flight: usize,
    wait_count: u64,
    wait_time_ms: u64,
    rejection_count: u64,
    best_effort_drop_count: u64,
}

#[derive(Clone)]
pub struct HostResources {
    inner: Arc<HostResourcesInner>,
}

#[derive(Clone)]
pub(crate) struct ApplicationAdmission {
    resources: HostResources,
    deadline: Deadline,
}

impl ApplicationAdmission {
    pub(crate) async fn acquire(
        &self,
        bytes: usize,
    ) -> TCResult<tokio::sync::OwnedSemaphorePermit> {
        self.resources
            .admit_application_bytes(bytes, self.deadline)
            .await
    }
}

struct HostResourcesInner {
    limits: HostLimits,
    requests: Arc<Capacity>,
    connections: Arc<Capacity>,
    graph: Arc<Capacity>,
    outbound: Arc<Capacity>,
    application_in_flight: Arc<Semaphore>,
}

impl HostResources {
    pub(crate) fn application_admission(&self, deadline: Deadline) -> ApplicationAdmission {
        ApplicationAdmission {
            resources: self.clone(),
            deadline,
        }
    }

    pub fn new(limits: HostLimits) -> TCResult<Self> {
        validate_execution_limits(&limits.execution)?;
        for (name, capacity) in [
            ("in-flight requests", limits.ingress.in_flight_requests),
            ("active connections", limits.ingress.active_connections),
            (
                "application admission bytes",
                limits.ingress.application_in_flight_bytes,
            ),
            (
                "parallel graph operations",
                limits.execution.parallel_graph_ops,
            ),
            ("outbound requests", limits.execution.outbound_requests),
            ("WASM compilations", limits.execution.wasm_compilations),
        ] {
            if capacity == 0 || capacity > Semaphore::MAX_PERMITS {
                return Err(TCError::bad_request(format!(
                    "{name} exceeds supported capacity"
                )));
            }
        }

        let requests = Capacity::new("/host/resource/request", limits.ingress.in_flight_requests);
        let connections = Capacity::new(
            "/host/resource/connection",
            limits.ingress.active_connections,
        );
        let graph = Capacity::new("/host/resource/graph", limits.execution.parallel_graph_ops);
        let outbound = Capacity::new("/host/resource/rpc", limits.execution.outbound_requests);
        let application_in_flight =
            Arc::new(Semaphore::new(limits.ingress.application_in_flight_bytes));
        Ok(Self {
            inner: Arc::new(HostResourcesInner {
                limits,
                requests,
                connections,
                graph,
                outbound,
                application_in_flight,
            }),
        })
    }

    pub(crate) fn limits(&self) -> &HostLimits {
        &self.inner.limits
    }

    pub(crate) fn deadline(&self) -> Deadline {
        Deadline::after(self.inner.limits.execution.request_deadline)
    }

    pub(crate) async fn admit_request(&self, deadline: Deadline) -> TCResult<CapacityPermit> {
        self.inner.requests.clone().acquire(deadline).await
    }

    #[cfg(feature = "http-server")]
    pub(crate) async fn admit_connection(&self, deadline: Deadline) -> TCResult<CapacityPermit> {
        self.inner.connections.clone().acquire(deadline).await
    }

    pub(crate) async fn admit_graph_op(&self, deadline: Deadline) -> TCResult<CapacityPermit> {
        self.inner.graph.clone().acquire(deadline).await
    }

    pub(crate) async fn admit_outbound(&self, deadline: Deadline) -> TCResult<CapacityPermit> {
        self.inner.outbound.clone().acquire(deadline).await
    }

    pub(crate) async fn admit_application_bytes(
        &self,
        bytes: usize,
        deadline: Deadline,
    ) -> TCResult<OwnedSemaphorePermit> {
        let bytes = u32::try_from(bytes)
            .map_err(|_| TCError::bad_request("application body exceeds admission range"))?;
        deadline
            .wait(Arc::clone(&self.inner.application_in_flight).acquire_many_owned(bytes))
            .await?
            .map_err(|_| {
                TCError::resource_unavailable(
                    "application admission is unavailable",
                    Pressure::new(
                        "/host/resource/application-in-flight",
                        PressureReason::Saturated,
                    ),
                )
            })
    }

    pub(crate) fn state(&self) -> crate::State {
        crate::State::Tuple(
            [
                &self.inner.requests,
                &self.inner.connections,
                &self.inner.graph,
                &self.inner.outbound,
            ]
            .into_iter()
            .map(|capacity| capacity_state(capacity.snapshot()))
            .collect(),
        )
    }
}

fn validate_execution_limits(limits: &ExecutionLimits) -> TCResult<()> {
    if limits.request_deadline.is_zero() {
        return Err(TCError::bad_request("request deadline must be nonzero"));
    }

    if Instant::now()
        .checked_add(limits.request_deadline)
        .is_none()
    {
        return Err(TCError::bad_request(
            "request deadline exceeds the monotonic clock",
        ));
    }

    let capacities = [
        ("parallel graph operations", limits.parallel_graph_ops),
        ("outbound requests", limits.outbound_requests),
        ("graph nodes", limits.max_graph_nodes),
        ("graph edges", limits.max_graph_edges),
        ("execution depth", limits.max_execution_depth),
        ("operation invocations", limits.max_op_invocations),
        ("WASM memory bytes", limits.wasm_memory_bytes),
        ("WASM result bytes", limits.wasm_result_bytes),
        ("WASM compilations", limits.wasm_compilations),
    ];
    if let Some((name, _)) = capacities.into_iter().find(|(_, value)| *value == 0) {
        return Err(TCError::bad_request(format!("{name} must be nonzero")));
    }
    if limits.wasm_fuel_per_call == 0 {
        return Err(TCError::bad_request("WASM fuel must be nonzero"));
    }
    if limits.wasm_result_bytes > limits.wasm_memory_bytes {
        return Err(TCError::bad_request(
            "WASM result limit cannot exceed its memory limit",
        ));
    }
    Ok(())
}

fn capacity_state(snapshot: CapacitySnapshot) -> crate::State {
    use tc_ir::{Id, Map};
    use tc_value::Value;

    let entry = |name: &str, value| -> (Id, crate::State) {
        (name.parse().expect("capacity field"), value)
    };

    crate::State::from(Map::from_iter([
        entry(
            "resource",
            crate::State::from(Value::from(snapshot.resource)),
        ),
        entry("limit", crate::State::from(snapshot.limit as u64)),
        entry("in_flight", crate::State::from(snapshot.in_flight as u64)),
        entry("wait_count", crate::State::from(snapshot.wait_count)),
        entry("wait_time_ms", crate::State::from(snapshot.wait_time_ms)),
        entry(
            "rejection_count",
            crate::State::from(snapshot.rejection_count),
        ),
        entry(
            "best_effort_drop_count",
            crate::State::from(snapshot.best_effort_drop_count),
        ),
    ]))
}

impl Default for HostResources {
    fn default() -> Self {
        Self::new(HostLimits::default()).expect("default host limits are valid")
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

pub(crate) struct CapacityPermit {
    owner: Arc<Capacity>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for CapacityPermit {
    fn drop(&mut self) {
        self.owner.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
#[path = "../tests/support/resources.rs"]
mod tests;
