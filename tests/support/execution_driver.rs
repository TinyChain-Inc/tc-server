use tc_ir::{Cond, Map, OpDef, Scalar, TCRef};
use tc_state::OpExecutor;
use tc_value::Value;

use crate::State;
use super::*;

async fn txn(name: &str) -> TxnHandle {
    let mut limits = crate::HostLimits {
        transaction_ttl: std::time::Duration::from_secs(60),
        ..Default::default()
    };

    limits.execution.parallel_graph_ops = 1;
    crate::txn::test_kernel_with_limits(
        name,
        crate::txn::test_workspace(name),
        limits,
    )
    .await
    .test_txn()
    .await
}

fn op() -> OpDef {
    OpDef::Post(vec![("result".parse().unwrap(), Scalar::from(1_u64))])
}

fn nested(depth: usize) -> OpDef {
    let mut nested = op();
    for level in 1..depth {
        nested = OpDef::Post(vec![(
            format!("provider_{level}").parse().unwrap(),
            Scalar::from(TCRef::Cond(Box::new(Cond::new(
                TCRef::Id("$condition".parse().unwrap()),
                Scalar::Op(nested),
                Scalar::default(),
            )))),
        )]);
    }

    nested
}

fn args(marker: Arc<[u8]>) -> State {
    State::Map(Map::from_iter([
        (
            "keepalive".parse().unwrap(),
            State::from(Value::Bytes(marker)),
        ),
        ("condition".parse().unwrap(), State::from(Value::from(true))),
    ]))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_nested_execution_admits_sixty_four_and_rejects_sixty_five() {
    tokio::spawn(async {
        for depth in [64, 65] {
            let txn = txn(&format!("driver-depth-{depth}")).await;
            let marker: Arc<[u8]> = vec![1; 16].into();
            let result = txn
                .execute_op(nested(depth), args(Arc::clone(&marker)), None, None)
                .await;

            if depth == 64 {
                let state = result.unwrap();
                assert!(matches!(&state, State::Scalar(value)
                    if matches!(&value, Scalar::Value(value)
                        if matches!(&value, Value::Number(number) if *number == 1_u64.into()))));
            } else {
                let error = result.unwrap_err();
                assert_eq!(error.code(), tc_error::ErrorKind::Unavailable);
                assert!(error.to_string().contains("nesting"));
            }

            assert_eq!(Arc::strong_count(&marker), 1);
            drop(txn.admit_graph_op().await.unwrap());
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_releases_waiting_queued_and_active_invocations() {
    tokio::spawn(async {
        let mut txn = txn("driver-cancellation").await;
        let permit = txn.admit_graph_op().await.unwrap();
        for stage in 0..3 {
            let (sender, mut requests) = mpsc::channel(1);
            txn.execution = Some(sender.downgrade());
            if stage == 0 {
                let (response, _result) = oneshot::channel();
                sender
                    .send(Invocation {
                        txn: txn.clone(),
                        definition: op(),
                        args: State::Map(Map::new()),
                        subject: None,
                        response,
                    })
                    .await
                    .unwrap();
            }

            let marker: Arc<[u8]> = vec![7; 16].into();
            let mut caller = Box::pin(txn.execute_op(op(), args(Arc::clone(&marker)), None, None));
            for _ in 0..4 {
                assert!(futures::poll!(&mut caller).is_pending());
            }

            assert_eq!(requests.len(), 1);
            if stage == 2 {
                let request = requests.recv().await.unwrap();
                let mut active = Box::pin(run_invocation(request));
                assert!(futures::poll!(&mut active).is_pending());
                drop(caller);
                assert!(matches!(
                    futures::poll!(&mut active),
                    std::task::Poll::Ready(None)
                ));

                drop(active);
            } else {
                drop(caller);
                let request = requests.recv().await.unwrap();
                assert!(request.response.is_closed());
                let mut queued = Box::pin(run_invocation(request));
                assert!(matches!(
                    futures::poll!(&mut queued),
                    std::task::Poll::Ready(None)
                ));

                drop(queued);
            }

            assert_eq!(Arc::strong_count(&marker), 1);
            drop(sender);
            drop(requests);
        }

        let mut expired = txn.clone();
        expired.deadline = crate::Deadline::after(std::time::Duration::ZERO);
        let (response, result) = oneshot::channel();
        run_invocation(Invocation {
            txn: expired,
            definition: op(),
            args: State::Map(Map::new()),
            subject: None,
            response,
        })
        .await;

        let error = result.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("deadline"));
        drop(permit);
        drop(txn.admit_graph_op().await.unwrap());
        let error = txn
            .execute_op(op(), State::Map(Map::new()), None, None)
            .await
            .unwrap_err();

        assert_eq!(error.code(), tc_error::ErrorKind::Unavailable);
        assert!(error.to_string().contains("cancelled"));
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_process_releases_nested_frames_and_graph_admission() {
    tokio::spawn(async {
        let txn = txn("driver-process-drop").await;
        let marker: Arc<[u8]> = vec![9; 16].into();
        let initial = txn.execution_budget.load(Ordering::Acquire);
        let mut process =
            Box::pin(txn.execute_op(nested(64), args(Arc::clone(&marker)), None, None));

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            futures::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(process.as_mut(), cx).is_pending());
                if initial - txn.execution_budget.load(Ordering::Acquire) >= 16 {
                    std::task::Poll::Ready(())
                } else {
                    std::task::Poll::Pending
                }
            }),
        )
        .await
        .unwrap();

        assert!(initial - txn.execution_budget.load(Ordering::Acquire) >= 16);
        drop(process);
        assert_eq!(Arc::strong_count(&marker), 1);
        drop(txn.admit_graph_op().await.unwrap());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn retaining_known_capabilities_reuses_token_storage() {
    let source = txn("retained-token-storage").await;
    let mut target = source.clone();
    let activity = target.activity.clone();
    let expiry = target.expires;
    target.retain_source(&source).unwrap();
    assert!(Arc::ptr_eq(&activity, &target.activity));
    assert_eq!(target.expires, expiry);
}

#[tokio::test]
async fn unpublished_recovery_returns_integrity_errors_without_terminating() {
    let active = txn("recovery-error-policy").await;
    let config = crate::txn::TxnConfig::new(
        active.server.protocol_authority().clone(),
        active.server.workspace().clone(),
        active.server.resources().clone(),
    );

    let server = crate::txn::TxnServer::load(config, active.server.verifier().clone())
        .await
        .unwrap();

    let recovery = server
        .bind_recovery(active.id(), active.kernel.clone())
        .unwrap();

    assert!(recovery.recovering);
    let error = recovery.storage_error(TCError::recovery_required("unpublished stored corruption"));
    assert!(error.requires_recovery());
    assert!(!server.is_ready());
    assert!(
        active
            .server
            .bind_recovery(active.id(), active.kernel.clone())
            .is_err()
    );
}

#[tokio::test]
async fn projection_preserves_fixed_expiry_for_retained_results() {
    let mut txn = txn("retained-projection").await;
    txn.deadline = crate::Deadline::at(tokio::time::Instant::now());
    assert!(txn.deadline.check().is_err());
    let error = txn
        .view(State::default(), txn.deadline)
        .await
        .err()
        .unwrap();

    assert_eq!(error.code(), tc_error::ErrorKind::Unavailable);
    let view = txn
        .view(State::from(Value::from(7_u64)), txn.expiry_deadline())
        .await
        .unwrap();

    assert!(matches!(
        view,
        tc_state::StateView::Scalar(Scalar::Value(Value::Number(_)))
    ));

    txn.expires = tokio::time::Instant::now();
    let error = txn
        .view(State::default(), txn.expiry_deadline())
        .await
        .err()
        .unwrap();

    assert_eq!(error.code(), tc_error::ErrorKind::Conflict);
}

#[tokio::test]
async fn projection_rechecks_expiry_after_storage_acquisition() {
    use tc_collection::btree::{BTree, BTreeColumnSchema, BTreeSchema};
    use tc_ir::Transact;

    let kernel = crate::txn::test_kernel("blocked-projection").await;
    let writer = kernel.test_txn().await;
    let mut txn = kernel.test_txn().await;
    let dir = txn.subcontext_unique().context().await.unwrap();
    let dtype = tc_value::ValueType::Number(tc_value::NumberType::Number);
    let schema = BTreeSchema::try_from(vec![BTreeColumnSchema {
        name: "id".into(),
        dtype,
        max_size: None,
    }])
    .unwrap();

    let tree = BTree::<TxnHandle>::create(dir, schema).await.unwrap();
    tree.insert_row(&writer, vec![Value::from(1_u64)])
        .await
        .unwrap();

    let state = State::from(tc_collection::Collection::BTree(Box::new(tree.clone())));
    txn.expires = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    let error = {
        let mut projection = std::pin::pin!(txn.view(state, txn.deadline));
        assert!(futures::poll!(&mut projection).is_pending());
        // Cross the fixed expiry while the earlier write excludes observation.
        tokio::time::sleep_until(txn.expires).await;
        tree.rollback(&writer.id()).await.unwrap();
        projection.await.err().unwrap()
    };

    assert_eq!(error.code(), tc_error::ErrorKind::Conflict);
    let retry = kernel.test_txn().await;
    retry
        .deadline()
        .run(tree.insert_row(&retry, vec![Value::from(1_u64)]))
        .await
        .expect("failed projection must release its read reservation");
}
