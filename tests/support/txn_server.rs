use super::*;

impl TxnServer {
    fn observe(&self, id: TxnId) -> TCResult<()> {
        let mut inner = self.state.inner.lock();
        if inner.latest_finalized.is_none_or(|cutoff| id > cutoff) {
            self.admit(&mut inner, id)?;
        }

        Ok(())
    }

    pub(crate) fn test_decision_bearer(&self, txn: &TxnHandle) -> String {
        txn.lock_for_test()
            .expect("lock test transaction")
            .raw_token()
            .expect("decision bearer")
    }

    pub(crate) fn contains(&self, txn_id: &TxnId) -> bool {
        self.state.inner.lock().active.contains_key(txn_id)
    }
}

pub(crate) fn test_verifier(config: &TxnConfig) -> Arc<dyn TokenVerifier> {
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

#[tokio::test]
async fn activity_does_not_extend_the_identity_derived_expiry() {
    let kernel = super::super::test_kernel_with(
        "fixed-expiry",
        Duration::from_millis(40),
        super::super::test_workspace("fixed-expiry"),
    )
    .await;
    let server = kernel.test_txn_server();
    let txn = kernel.test_txn().await;
    let first = server.state.inner.lock().active[&txn.id()].expires;
    tokio::time::sleep(Duration::from_millis(5)).await;
    server.observe(txn.id()).unwrap();
    assert_eq!(server.state.inner.lock().active[&txn.id()].expires, first);
    let cloned = txn.clone();
    assert!(txn.deadline().instant() <= first);
    tokio::time::sleep_until(first).await;
    assert!(txn.check_active().is_err());
    assert!(cloned.check_active().is_err());
    assert!(
        server.contains(&txn.id()),
        "live expired ownership remains tracked"
    );

    drop(txn);
    drop(cloned);
    kernel.shutdown().await;
}

#[tokio::test]
async fn concurrent_allocation_persists_the_latest_identity() {
    let kernel = super::super::test_kernel("concurrent-allocation").await;
    let (first, second) = tokio::join!(kernel.test_txn(), kernel.test_txn());
    let latest = std::cmp::max(first.id(), second.id());
    let server = kernel.test_txn_server();
    let config = server.state.config.clone();
    let workspace = config.workspace().clone();
    let (_, last_allocated) = workspace.frontiers().await.expect("durable host frontier");
    assert_eq!(last_allocated, Some(latest));
    assert!(workspace.transaction_ids().await.unwrap().is_empty());
    let restarted = TxnServer::load(config.clone(), test_verifier(&config))
        .await
        .unwrap();
    let next = restarted.allocate().await.unwrap();
    assert!(next > latest);
    assert_eq!(workspace.frontiers().await.unwrap().1, Some(next));
}

async fn isolated_server(kernel: &crate::Kernel, workspace: crate::Workspace) -> TxnServer {
    let config = TxnConfig::new(
        kernel.test_txn_server().protocol_authority().clone(),
        workspace,
        crate::HostResources::new(crate::HostLimits {
            transaction_ttl: Duration::from_secs(60),
            ..Default::default()
        })
        .unwrap(),
    );

    TxnServer::load(config.clone(), test_verifier(&config))
        .await
        .unwrap()
}

fn owner_context(server: &TxnServer, id: TxnId) -> AuthContext {
    let authority = server.protocol_authority();
    let token = crate::auth::Token::new(
        authority.host().clone(),
        SystemTime::now(),
        Duration::from_secs(60),
        authority.actor_id().to_owned(),
        crate::auth::wire_claim(crate::Claim::new(
            crate::uri::transaction_path(id).parse().unwrap(),
            umask::USER_EXEC,
        )),
    )
    .unwrap();

    AuthContext::new(authority.actor_id()).with_signed(authority.sign(token).unwrap())
}

#[tokio::test]
async fn decisions_retain_identities_until_cutoff_without_blocking_admission() {
    let kernel = super::super::test_kernel("retained-identities").await;
    let server =
        isolated_server(&kernel, super::super::test_workspace("retained-identities")).await;

    let isolated = kernel.with_test_txn_server(server.clone());
    server.finish_recovery().await.unwrap();
    let committed = isolated.test_bind(None, None).await.unwrap();
    let first = committed.id();
    kernel.test_libraries().claim(&committed).await.unwrap();
    kernel
        .coordinate(&committed, TransactionOutcome::Commit, false)
        .await
        .unwrap();

    drop(committed);

    let (one, two) = tokio::join!(server.allocate(), server.allocate());
    let second = one.unwrap();
    let third = two.unwrap();
    assert_ne!(second, third);
    let rolled_back = isolated.test_seed_txn(second).unwrap();
    kernel.test_libraries().claim(&rolled_back).await.unwrap();
    kernel
        .coordinate(&rolled_back, TransactionOutcome::Rollback, false)
        .await
        .unwrap();

    drop(rolled_back);
    assert_eq!(server.state.inner.lock().active.len(), 3);
    assert!(server.contains(&first));
    assert!(server.contains(&second));

    let context = owner_context(&server, first);
    let duplicate = isolated
        .test_bind(Some(first), Some(&context))
        .await
        .unwrap();

    assert_eq!(duplicate.id(), first);
    drop(duplicate);
    assert_eq!(server.state.inner.lock().active.len(), 3);
    let unseen = kernel.test_txn().await.id();
    let missing_auth = isolated.test_bind(Some(unseen), None).await.err().unwrap();
    assert_eq!(missing_auth.code(), tc_error::ErrorKind::Unauthorized);
    assert!(!server.contains(&unseen));
    let context = owner_context(&server, unseen);
    let participant = isolated
        .test_bind(Some(unseen), Some(&context))
        .await
        .unwrap();

    assert_eq!(participant.id(), unseen);
    drop(participant);
    drop(isolated.test_seed_txn(unseen).unwrap());
    assert_eq!(server.state.inner.lock().active.len(), 4);

    // The same deferral path used after frontier/workspace cleanup errors keeps
    // every retained identity and withdraws readiness until cleanup can finish.
    server.defer(
        first,
        "injected cleanup failure",
        TCError::internal("fixture"),
    );

    assert_eq!(server.state.inner.lock().active.len(), 4);
    assert!(isolated.test_bind(None, None).await.is_err());

    server
        .expire_due(Instant::now() + Duration::from_secs(120), &|_| {
            std::future::ready(Ok(()))
        })
        .await;

    assert!(!server.contains(&first));
    assert!(!server.contains(&second));
    assert!(server.state.inner.lock().active.is_empty());
    assert!(isolated.test_bind(None, None).await.is_ok());
    kernel.shutdown().await;
}

#[tokio::test]
async fn startup_restores_transactions_and_live_admission_resumes_before_cleanup() {
    let kernel = super::super::test_kernel("recovery-admission").await;
    let workspace = super::super::test_workspace("recovery-admission");
    let original = isolated_server(&kernel, workspace.clone()).await;
    original.finish_recovery().await.unwrap();
    let first = original.allocate().await.unwrap();
    let second = original.allocate().await.unwrap();
    for id in [first, second] {
        workspace
            .transaction_child(id, &[])
            .await
            .unwrap()
            .sync_all()
            .await
            .unwrap();
    }

    drop(original);

    let server = isolated_server(&kernel, workspace).await;
    let isolated = kernel.with_test_txn_server(server.clone());
    assert_eq!(server.state.inner.lock().active.len(), 2);
    let bootstrap = server.allocate_bootstrap().await.unwrap();
    let recovered = isolated.test_recovery_txn(second).unwrap();
    assert_eq!(recovered.id(), second);
    drop(recovered);

    // A WAL-only identity need not have left an old transaction workspace.
    let wal_only = kernel.test_txn().await.id();
    let recovered = isolated.test_recovery_txn(wal_only).unwrap();
    drop(recovered);
    assert_eq!(server.state.inner.lock().active.len(), 4);
    assert!(isolated.test_bind(None, None).await.is_err());
    assert!(server.state.worker.lock().await.is_none());

    server.finish_recovery().await.unwrap();
    assert!(server.allocate_bootstrap().await.is_err());
    drop(isolated.test_bind(None, None).await.unwrap());
    assert!(server.contains(&first));
    assert!(server.contains(&second));

    for _ in 0..4 {
        server
            .expire_due(Instant::now() + Duration::from_secs(120), &|_| {
                std::future::ready(Ok(()))
            })
            .await;
    }

    assert!(!server.contains(&bootstrap));
    assert!(server.state.inner.lock().active.is_empty());
    assert!(server.allocate().await.is_ok());
    kernel.shutdown().await;
}

#[test]
fn finalizer_failure_terminates_process() {
    const CHILD: &str = "TC_FINALIZER_FATAL_CASE";
    if let Ok(case) = std::env::var(CHILD) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async {
            let kernel = super::super::test_kernel("fatal-finalizer").await;
            let txn = kernel.test_txn().await;
            let config = kernel.test_txn_server().state.config.clone();
            // Use an independent server so the kernel's normal expiry worker
            // cannot consume the injected worker's active transactions.
            let server = TxnServer::load(config.clone(), test_verifier(&config))
                .await
                .unwrap();

            server.finish_recovery().await.unwrap();
            server.observe(txn.id()).unwrap();
            // Bootstrap may leave an earlier active identity; the worker always
            // finalizes the oldest one first.
            for active in server.state.inner.lock().active.values_mut() {
                active.expires = Instant::now();
            }

            server.start_expiry(&tokio::runtime::Handle::current(), move |_| {
                let panic = case == "panic";
                async move {
                    if panic {
                        panic!("injected finalizer panic");
                    }

                    Err(TCError::internal("injected materialization failure"))
                }
            });

            tokio::time::sleep(Duration::from_secs(2)).await;
            panic!("finalizer did not terminate the host");
        });

        return;
    }

    for case in ["error", "panic"] {
        // Aborting is the behavior under test; do not leave a potentially large core dump.
        #[cfg(unix)]
        let mut child = {
            let mut command = std::process::Command::new("sh");
            command
                .args(["-c", "ulimit -c 0; exec \"$@\"", "finalizer-probe"])
                .arg(std::env::current_exe().unwrap());

            command
        };

        #[cfg(not(unix))]
        let mut child = std::process::Command::new(std::env::current_exe().unwrap());

        let output = child
            .args([
                "--exact",
                "txn::server::tests::finalizer_failure_terminates_process",
                "--nocapture",
            ])
            .env(CHILD, case)
            .output()
            .unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;

            assert_eq!(output.status.signal(), Some(6), "{case}: {output:?}");
        }

        #[cfg(not(unix))]
        assert!(!output.status.success(), "{case}: {output:?}");
    }
}

#[tokio::test]
async fn participant_admission_during_allocation_preserves_both_identities() {
    let kernel = super::super::test_kernel("allocation-admission-race").await;
    let workspace = super::super::test_workspace("allocation-admission-race");
    let server = isolated_server(&kernel, workspace.clone()).await;
    let isolated = kernel.with_test_txn_server(server.clone());
    server.finish_recovery().await.unwrap();
    let participant = kernel.test_txn().await.id();

    let allocation_lock = server.state.allocation.lock().await;
    let allocation = server.allocate();
    tokio::pin!(allocation);
    assert!(futures::poll!(&mut allocation).is_pending());
    drop(isolated.test_seed_txn(participant).unwrap());
    drop(allocation_lock);
    let allocated = allocation.await.unwrap();
    assert!(allocated > participant);

    assert_eq!(
        server
            .state
            .inner
            .lock()
            .active
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![participant, allocated]
    );

    assert_eq!(workspace.frontiers().await.unwrap().1, Some(allocated));

    server
        .expire_due(Instant::now() + Duration::from_secs(120), &|_| {
            std::future::ready(Ok(()))
        })
        .await;

    assert!(server.allocate().await.unwrap() > allocated);
    kernel.shutdown().await;
}

// Drive the real expiry/fatal boundary with the selected Kernel's Service tree.
pub(crate) async fn expire_services(kernel: &crate::Kernel) {
    use tc_ir::Transact;

    let server = kernel.test_txn_server();
    for active in server.state.inner.lock().active.values_mut() {
        active.expires = Instant::now();
    }

    let attempts = server.state.inner.lock().active.len();
    for _ in 0..attempts {
        let services = kernel.test_services();
        server
            .expire_due(Instant::now(), &move |cutoff| {
                let services = services.clone();
                async move { services.finalize(&cutoff).await }
            })
            .await;
    }

    panic!("expiry completed without the injected materialization failure");
}

#[tokio::test]
async fn shutdown_joins_active_finalization_after_cancelled_waiter() {
    let kernel = super::super::test_kernel("drain-finalizer").await;
    let config = kernel.test_txn_server().state.config.clone();
    let server = TxnServer::load(config.clone(), test_verifier(&config))
        .await
        .unwrap();

    server.finish_recovery().await.unwrap();
    let txn = server.allocate().await.unwrap();
    for active in server.state.inner.lock().active.values_mut() {
        active.expires = Instant::now();
    }

    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let finished = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let subject = kernel.test_services().clone();
    server.start_expiry(&tokio::runtime::Handle::current(), {
        let entered = entered.clone();
        let release = release.clone();
        let finished = finished.clone();
        let calls = calls.clone();
        move |cutoff| {
            let entered = entered.clone();
            let release = release.clone();
            let finished = finished.clone();
            let calls = calls.clone();
            let subject = subject.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                entered.notify_one();
                release.notified().await;
                use tc_ir::Transact;

                subject.finalize(&cutoff).await?;
                finished.store(true, Ordering::SeqCst);
                Ok(())
            }
        }
    });

    entered.notified().await;
    {
        let shutdown = server.stop_expiry();
        tokio::pin!(shutdown);
        assert!(futures::poll!(&mut shutdown).is_pending());
        assert!(!finished.load(Ordering::SeqCst));
    }

    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(server.stop_expiry(), server.stop_expiry());
    })
    .await
    .unwrap();

    assert!(finished.load(Ordering::SeqCst));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        server
            .state
            .inner
            .lock()
            .latest_finalized
            .is_some_and(|id| id <= txn)
    );

    kernel.shutdown().await;
}

#[tokio::test]
async fn expiry_waits_for_flat_capabilities_and_closes_binding() {
    let kernel = super::super::test_kernel("retained-expiry").await;
    let server = kernel.test_txn_server();
    let first = kernel.test_txn().await;
    let first_id = first.id();
    let first_clone = first.clone();
    let mut derived = kernel.test_txn().await;
    let second_id = derived.id();
    derived.retain_source(&first).unwrap();
    derived.retain_source(&first_clone).unwrap();
    let tracker = server.state.inner.lock().active[&first_id].calls.clone();
    // One shared original token and one flat derived token, despite repeated input.
    assert_eq!(tracker.len(), 2);
    assert!(derived.deadline().instant() <= first.expiry_deadline().instant());
    // Remove only tokenless bootstrap entries so this probe starts at its subject.
    server.state.inner.lock().active.retain(|id, active| {
        assert!(*id >= first_id || active.calls.is_empty());
        *id >= first_id
    });

    let applied = std::sync::Mutex::new(Vec::new());
    let finalize = |id| {
        applied.lock().unwrap().push(id);
        std::future::ready(Ok(()))
    };

    {
        // Advance the expiry observation deterministically, without changing the clock.
        let expiry = server.expire_due(Instant::now() + Duration::from_secs(60), &finalize);
        tokio::pin!(expiry);
        assert!(futures::poll!(&mut expiry).is_pending());
        assert!(tracker.is_closed());
        assert!(applied.lock().unwrap().is_empty());
        let target = format!("{}?txn_id={first_id}", crate::uri::HOST_ROOT);
        let error = kernel
            .begin_request(tc_ir::Method::Get, &target, true, None)
            .await
            .err()
            .expect("closed transaction rejects binding");

        assert_eq!(error.code(), tc_error::ErrorKind::Conflict);
        drop(first);
        assert!(futures::poll!(&mut expiry).is_pending());
        drop(first_clone);
        assert!(futures::poll!(&mut expiry).is_pending());
        drop(derived);
        tokio::time::timeout(Duration::from_secs(5), expiry)
            .await
            .unwrap();
    }

    assert!(tracker.is_empty());
    server.observe(first_id).unwrap();
    assert!(
        !server.contains(&first_id),
        "late observation cannot reopen a finalized identity"
    );

    assert_eq!(*applied.lock().unwrap(), vec![second_id]);
    server
        .expire_due(Instant::now() + Duration::from_secs(60), &finalize)
        .await;

    assert_eq!(*applied.lock().unwrap(), vec![second_id]);
    kernel.shutdown().await;
}

#[tokio::test]
async fn expiry_batches_quiescent_prefix_and_respects_active_and_unexpired_barriers() {
    let kernel = super::super::test_kernel("batched-expiry").await;
    let server = isolated_server(&kernel, super::super::test_workspace("batched-expiry")).await;
    server.finish_recovery().await.unwrap();
    let first = server.allocate().await.unwrap();
    let second = server.allocate().await.unwrap();
    let middle = server.allocate().await.unwrap();
    let last = server.allocate().await.unwrap();
    let isolated = kernel.with_test_txn_server(server.clone());
    let active = isolated.test_seed_txn(middle).unwrap();
    let now = Instant::now() + Duration::from_secs(120);
    let applied = std::sync::Mutex::new(Vec::new());
    let finalize = |cutoff| {
        for (_, entry) in server.state.inner.lock().active.range(..=cutoff) {
            assert!(entry.calls.is_closed());
            assert!(entry.calls.is_empty());
        }

        applied.lock().unwrap().push(cutoff);
        std::future::ready(Ok(()))
    };

    server.expire_due(now, &finalize).await;
    assert_eq!(*applied.lock().unwrap(), vec![second]);
    assert!(!server.contains(&first));
    assert!(!server.contains(&second));
    assert!(server.contains(&middle));
    assert!(server.contains(&last));
    assert!(isolated.test_seed_txn(first).is_err());
    drop(active);

    server
        .state
        .inner
        .lock()
        .active
        .get_mut(&last)
        .unwrap()
        .expires = now + Duration::from_secs(60);

    server.expire_due(now, &finalize).await;
    assert_eq!(*applied.lock().unwrap(), vec![second, middle]);
    assert!(server.contains(&last));
    server.expire_due(now, &finalize).await;
    assert_eq!(*applied.lock().unwrap(), vec![second, middle]);
    server
        .expire_due(now + Duration::from_secs(60), &finalize)
        .await;

    assert_eq!(*applied.lock().unwrap(), vec![second, middle, last]);
    assert!(server.state.inner.lock().active.is_empty());
    kernel.shutdown().await;
}

#[tokio::test]
async fn batched_cleanup_backoff_is_owned_by_the_oldest_retained_identity() {
    let kernel = super::super::test_kernel("batched-backoff").await;
    let server = isolated_server(&kernel, super::super::test_workspace("batched-backoff")).await;
    let isolated = kernel.with_test_txn_server(server.clone());
    server.finish_recovery().await.unwrap();
    let first = server.allocate().await.unwrap();
    let last = server.allocate().await.unwrap();
    let now = Instant::now();
    for active in server.state.inner.lock().active.values_mut() {
        active.expires = now;
        active.calls.close();
    }

    server.defer(last, "injected batched cleanup failure", "fixture");
    assert!(server.state.inner.lock().active[&first].retry.is_some());
    assert!(server.state.inner.lock().active[&last].retry.is_none());
    let applied = std::sync::Mutex::new(Vec::new());
    let finalize = |cutoff| {
        applied.lock().unwrap().push(cutoff);
        std::future::ready(Ok(()))
    };

    server.expire_due(now, &finalize).await;
    assert!(applied.lock().unwrap().is_empty());
    assert!(!server.is_ready());
    assert!(isolated.test_bind(None, None).await.is_err());
    server
        .expire_due(now + Duration::from_secs(2), &finalize)
        .await;

    assert_eq!(*applied.lock().unwrap(), vec![last]);
    assert!(server.is_ready());
    assert!(server.state.inner.lock().active.is_empty());
    kernel.shutdown().await;
}

#[tokio::test]
async fn shutdown_interrupts_retained_capability_wait() {
    let kernel = super::super::test_kernel("retained-shutdown").await;
    let txn = kernel.test_txn().await;
    let server = kernel.test_txn_server();
    let tracker = server.state.inner.lock().active[&txn.id()].calls.clone();
    server.state.inner.lock().active.retain(|id, active| {
        assert!(*id >= txn.id() || active.calls.is_empty());
        *id >= txn.id()
    });

    let finalize = |_| async { panic!("retained capability must prevent materialization") };
    let expiry = server.expire_due(Instant::now() + Duration::from_secs(60), &finalize);
    tokio::pin!(expiry);
    assert!(futures::poll!(&mut expiry).is_pending());
    tokio::time::timeout(Duration::from_secs(5), kernel.shutdown())
        .await
        .unwrap();

    expiry.await;
    assert!(!tracker.is_empty());
    assert!(server.contains(&txn.id()));
    drop(txn);
    assert!(tracker.is_empty());
}
