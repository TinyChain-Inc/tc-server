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

#[tokio::test]
async fn application_byte_admission_is_shared_and_released() {
    let mut limits = HostLimits::default();
    limits.ingress.application_in_flight_bytes = 4;
    let resources = HostResources::new(limits).unwrap();
    let first = resources
        .admit_application_bytes(4, Deadline::after(Duration::from_secs(1)))
        .await
        .expect("first body");

    assert!(
        resources
            .admit_application_bytes(1, Deadline::after(Duration::from_millis(1)))
            .await
            .is_err()
    );

    drop(first);
    let _released = resources
        .admit_application_bytes(4, Deadline::after(Duration::from_secs(1)))
        .await
        .expect("released body");
}

#[test]
fn deadline_is_one_absolute_budget() {
    let deadline = Deadline::after(Duration::from_secs(1));
    assert!(!deadline.is_expired());
    assert!(deadline.check().is_ok());
    assert!(deadline.remaining() <= Duration::from_secs(1));
    let expired = Deadline::at(Instant::now());
    let error = expired.check().unwrap_err();
    assert_eq!(error.code(), tc_error::ErrorKind::Unavailable);
    assert_eq!(
        error.pressure().unwrap().reason(),
        tc_error::PressureReason::Saturated
    );
}

#[test]
fn execution_limits_must_be_nonzero_and_consistent() {
    let mut limits = HostLimits::default();
    limits.execution.wasm_result_bytes = limits.execution.wasm_memory_bytes + 1;
    assert!(HostResources::new(limits).is_err());
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

#[test]
fn invalid_admission_capacity_and_deadlines_return_errors() {
    let fields: [fn(&mut HostLimits) -> &mut usize; 6] = [
        |limits| &mut limits.ingress.in_flight_requests,
        |limits| &mut limits.ingress.active_connections,
        |limits| &mut limits.ingress.application_in_flight_bytes,
        |limits| &mut limits.execution.parallel_graph_ops,
        |limits| &mut limits.execution.outbound_requests,
        |limits| &mut limits.execution.wasm_compilations,
    ];

    for capacity in [0, usize::MAX] {
        for field in fields {
            let mut limits = HostLimits::default();
            *field(&mut limits) = capacity;
            assert!(HostResources::new(limits).is_err());
        }
    }

    let mut limits = HostLimits::default();
    limits.execution.request_deadline = Duration::MAX;
    assert!(HostResources::new(limits).is_err());
    assert!(HostResources::new(HostLimits::default()).is_ok());
}
