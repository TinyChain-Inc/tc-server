use super::*;
use crate::KernelRequestGuard;
use tc_ir::Public;
async fn stage_service(
    kernel: &Kernel,
    txn: &crate::TxnHandle,
    identity: pathlink::Link,
    definition: tc_ir::Scalar,
) -> KernelRequestGuard {
    let permit = kernel
        .txn_server
        .resources()
        .admit_request(kernel.deadline())
        .await
        .expect("request admission");
    let guard = KernelRequestGuard::new(
        kernel.clone(),
        Method::Put,
        KernelTarget::Application("/service".parse().expect("application target")),
        txn.clone(),
        permit,
    );
    guard
        .execute(Some(crate::State::Tuple(vec![
            crate::State::from(tc_value::Value::Link(identity)),
            crate::State::from_scalar(definition),
        ])))
        .await
        .expect("stage Service");
    guard
}
async fn kernel(name: &str) -> Kernel {
    setup_with_ttl(name, std::time::Duration::from_secs(3)).await
}
async fn execute(
    kernel: &Kernel,
    method: Method,
    path: &str,
    body: Option<crate::State>,
    txn: crate::TxnHandle,
) -> tc_error::TCResult<crate::State> {
    let target = KernelTarget::Application(path.parse().expect("application target"));
    kernel
        .execute(target, txn, method, body)
        .await?
        .0
        .ok_or_else(|| tc_error::TCError::internal("ordinary request returned no response"))
}
async fn bind(kernel: &Kernel) -> crate::TxnHandle {
    kernel.test_txn().await
}

async fn service_txn(kernel: &Kernel, identity: &pathlink::Link) -> crate::TxnHandle {
    bind(kernel).await.with_claims(vec![crate::Claim::new(
        identity.clone(),
        umask::Mode::all(),
    )])
}

async fn complete(kernel: &Kernel, txn: crate::TxnHandle, outcome: crate::txn::TransactionOutcome) {
    kernel
        .coordinate(&txn, outcome, false)
        .await
        .expect("complete transaction");
}

#[path = "service.rs"]
mod service_fixture;

use service_fixture::definition as executable_service;

#[cfg(all(feature = "http-client", feature = "http-server"))]
#[test]
fn service_snapshot_is_transaction_bound_and_failed_sync_publishes_no_membership() {
    crate::test_runtime::run(|| async {
        use crate::cluster::AsyncHash;
        use futures::FutureExt;

        let source = setup_with_ttl("snapshot-source", std::time::Duration::from_secs(60)).await;
        let target = setup_with_ttl("snapshot-target", std::time::Duration::from_secs(60)).await;
        let identity: pathlink::Link = "/service/test/native/1.0.0".parse().unwrap();
        for kernel in [&source, &target] {
            let txn = service_txn(kernel, &identity).await;
            stage_service(kernel, &txn, identity.clone(), executable_service(true))
                .await
                .finish_success()
                .await
                .unwrap();
        }
        let body = |n: tc_value::Value| {
            crate::State::Tuple(vec![
                tc_value::Value::Tuple(vec![n.clone()]).into(),
                tc_value::Value::Tuple(vec![n]).into(),
            ])
        };
        let write = service_txn(&source, &identity).await;
        execute(
            &source,
            Method::Put,
            &format!("{identity}/insert"),
            Some(body(1_u64.into())),
            write.clone(),
        )
        .await
        .unwrap();
        complete(&source, write, crate::txn::TransactionOutcome::Commit).await;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let http = crate::http::HttpServer::new(source.clone());
        let task = tokio::spawn(async move {
            http.serve_listener_with_shutdown(listener, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
        });
        let replica = target
            .inner
            .bootstrap
            .self_identity("http://127.0.0.1:12345".into())
            .unwrap();

        let (txn, session) = target
            .bootstrap_resource(&endpoint, &identity, &replica)
            .await
            .unwrap();
        let source_identity = source
            .inner
            .bootstrap
            .self_identity(endpoint.clone())
            .unwrap();
        let mismatched = crate::cluster::BootstrapSession::new(
            session.token().into(),
            source_identity,
            "00".repeat(32),
        );
        let error = target
            .synchronize_service(&txn, &identity, &endpoint, &mismatched)
            .await
            .unwrap_err();
        assert_eq!(error.code(), tc_error::ErrorKind::Conflict);
        target.inner.services.rollback(&txn.id()).await.unwrap();

        // Cancellation while the network snapshot is pending selects no decision.
        let (txn, session) = target
            .bootstrap_resource(&endpoint, &identity, &replica)
            .await
            .unwrap();
        let stalled = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let stalled_endpoint = format!("http://{}", stalled.local_addr().unwrap());
        {
            let pending = target.synchronize_service(&txn, &identity, &stalled_endpoint, &session);
            futures::pin_mut!(pending);
            assert!(pending.as_mut().now_or_never().is_none());
        }
        target.inner.services.rollback(&txn.id()).await.unwrap();
        let read = bind(&target).await;
        let local = target
            .inner
            .services
            .state()
            .items(read.id())
            .await
            .unwrap()
            .pop()
            .unwrap();
        let initial_hash = local.hash(&read).await.unwrap();

        let (txn, session) = target
            .bootstrap_resource(&endpoint, &identity, &replica)
            .await
            .unwrap();
        let write = service_txn(&source, &identity).await;
        // A later transaction may change the source, but the older snapshot and
        // hash still describe the same source transaction.
        execute(
            &source,
            Method::Put,
            &format!("{identity}/insert"),
            Some(body(2_u64.into())),
            write.clone(),
        )
        .await
        .unwrap();
        complete(&source, write, crate::txn::TransactionOutcome::Commit).await;
        target
            .synchronize_service(&txn, &identity, &endpoint, &session)
            .await
            .unwrap();
        assert_ne!(local.hash(&txn).await.unwrap(), initial_hash);
        assert_eq!(
            hex::encode(local.hash(&txn).await.unwrap()),
            session.state_hash()
        );
        let value = execute(
            &target,
            Method::Get,
            &format!("{identity}/count"),
            None,
            txn.clone(),
        )
        .await
        .unwrap();
        assert!(
            matches!(value, crate::State::Scalar(Scalar::Value(tc_value::Value::Number(n))) if n.to_string() == "1")
        );
        target.inner.services.rollback(&txn.id()).await.unwrap();
        let response = execute(
            &target,
            Method::Get,
            &format!("{identity}/replicas"),
            None,
            bind(&target).await,
        )
        .await
        .unwrap();
        assert!(matches!(response, crate::State::Tuple(replicas) if replicas.is_empty()));
        let _ = shutdown.send(());
        task.await.unwrap();
    });
}

#[test]
fn executable_services_keep_direct_and_composed_writes_in_the_chain() {
    crate::test_runtime::run(|| async {
        for table in [false, true] {
            let path = crate::txn::test_path(if table {
                "executable-table"
            } else {
                "executable-btree"
            });
            let kernel = open_at(path.clone(), std::time::Duration::from_secs(60), false)
                .await
                .unwrap();
            let identity: pathlink::Link = "/service/test/native/1.0.0".parse().unwrap();
            let txn = service_txn(&kernel, &identity).await;
            stage_service(&kernel, &txn, identity.clone(), executable_service(table))
                .await
                .finish_success()
                .await
                .unwrap();
            let key = |n| {
                if table {
                    tc_value::Value::Tuple(vec![tc_value::Value::from(n)])
                } else {
                    tc_value::Value::None
                }
            };
            let body = |n| {
                crate::State::Tuple(vec![
                    key(n).into(),
                    tc_value::Value::Tuple(vec![tc_value::Value::from(n)]).into(),
                ])
            };
            let txn = service_txn(&kernel, &identity).await;
            execute(
                &kernel,
                Method::Put,
                &format!("{identity}/data/insert"),
                Some(body(1_u64)),
                txn.clone(),
            )
            .await
            .unwrap();
            let pending = execute(
                &kernel,
                Method::Get,
                &format!("{identity}/count"),
                None,
                txn.clone(),
            )
            .await
            .unwrap();
            assert!(
                matches!(pending, crate::State::Scalar(Scalar::Value(tc_value::Value::Number(n))) if n.to_string() == "1")
            );
            complete(&kernel, txn, crate::txn::TransactionOutcome::Commit).await;

            for (method, suffix, value) in [
                (Method::Put, "insert", body(2)),
                (
                    Method::Post,
                    "append",
                    crate::State::Map(
                        [
                            ("key".parse().unwrap(), key(3).into()),
                            (
                                "value".parse().unwrap(),
                                tc_value::Value::Tuple(vec![3_u64.into()]).into(),
                            ),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                ),
            ] {
                let txn = service_txn(&kernel, &identity).await;
                execute(
                    &kernel,
                    method,
                    &format!("{identity}/{suffix}"),
                    Some(value),
                    txn.clone(),
                )
                .await
                .unwrap();
                complete(&kernel, txn, crate::txn::TransactionOutcome::Commit).await;
            }
            let bytes = std::fs::read(
                path.join("data/service/test/native/1.0.0/.native/data/wal/committed.chain_block"),
            )
            .unwrap();
            let wal: serde_json::Value = serde_json::from_slice(&bytes[32..]).unwrap();
            let requests = wal[2][1].as_object().unwrap();
            assert_eq!(requests.len(), 3);
            assert!(
                requests
                    .values()
                    .all(|batch| batch.as_array().unwrap().len() == 1)
            );
            let txn = service_txn(&kernel, &identity).await;
            execute(
                &kernel,
                Method::Put,
                &format!("{identity}/insert"),
                Some(body(4)),
                txn.clone(),
            )
            .await
            .unwrap();
            complete(
                &kernel,
                txn.clone(),
                crate::txn::TransactionOutcome::Rollback,
            )
            .await;
            kernel.test_services().finalize(&txn.id()).await.unwrap();
            let read = bind(&kernel).await;
            let count = execute(
                &kernel,
                Method::Get,
                &format!("{identity}/count"),
                None,
                read.clone(),
            )
            .await
            .unwrap();
            assert!(
                matches!(count, crate::State::Scalar(Scalar::Value(tc_value::Value::Number(n))) if n.to_string() == "3")
            );
            let label = execute(
                &kernel,
                Method::Get,
                &format!("{identity}/label"),
                None,
                read,
            )
            .await
            .unwrap();
            assert!(
                matches!(label, crate::State::Scalar(Scalar::Value(tc_value::Value::String(s))) if s == "native")
            );
        }
    });
}

#[test]
fn service_recovery_preserves_expired_original_ids_and_rejects_materialization_intent() {
    use sha2::Digest;
    let path = crate::txn::test_path("service-recovery");
    let first = path.clone();
    let id = crate::test_runtime::run(move || async move {
        let kernel = open_at(first, std::time::Duration::from_secs(60), false)
            .await
            .unwrap();
        let identity: pathlink::Link = "/service/test/native/1.0.0".parse().unwrap();
        let txn = service_txn(&kernel, &identity).await;
        stage_service(&kernel, &txn, identity.clone(), executable_service(true))
            .await
            .finish_success()
            .await
            .unwrap();
        let txn = service_txn(&kernel, &identity).await;
        execute(
            &kernel,
            Method::Put,
            &format!("{identity}/insert"),
            Some(crate::State::Tuple(vec![
                tc_value::Value::Tuple(vec![1_u64.into()]).into(),
                tc_value::Value::Tuple(vec![7_u64.into()]).into(),
            ])),
            txn.clone(),
        )
        .await
        .unwrap();
        complete(&kernel, txn.clone(), crate::txn::TransactionOutcome::Commit).await;
        txn.id()
    });
    let wal_path =
        path.join("data/service/test/native/1.0.0/.native/data/wal/committed.chain_block");
    let committed = std::fs::read(&wal_path).unwrap();
    assert!(
        serde_json::from_slice::<serde_json::Value>(&committed[32..]).unwrap()[2][1]
            .get(id.to_string())
            .is_some()
    );
    std::fs::remove_dir_all(path.join("workspace/txn")).unwrap();
    // The old runtime (including expiry) is gone. Reopening after TTL + clock
    // skew exercises the private recovery capability, not request authentication.
    std::thread::sleep(std::time::Duration::from_millis(4100));
    let second = path.clone();
    let expected = committed.clone();
    crate::test_runtime::run(move || async move {
        let kernel = open_at(second.clone(), std::time::Duration::from_millis(50), false)
            .await
            .unwrap();
        // Before yielding to the newly started expiry task, recovery retains the WAL.
        assert_eq!(
            std::fs::read(
                second
                    .join("data/service/test/native/1.0.0/.native/data/wal/committed.chain_block")
            )
            .unwrap(),
            expected
        );
        assert!(kernel.is_ready());
        let txn = bind(&kernel).await;
        let count = execute(
            &kernel,
            Method::Get,
            "/service/test/native/1.0.0/count",
            None,
            txn,
        )
        .await
        .unwrap();
        assert!(
            matches!(count, crate::State::Scalar(Scalar::Value(tc_value::Value::Number(n))) if n.to_string() == "1")
        );
    });

    // An explicit recovery-state fixture: any surviving materialization intent
    // rejects opening, even when the native subject itself is structurally valid.
    let bytes = std::fs::read(&wal_path).unwrap();
    let mut wal: serde_json::Value = serde_json::from_slice(&bytes[32..]).unwrap();
    wal[1] = serde_json::Value::String(id.to_string());
    let encoded = serde_json::to_vec(&wal).unwrap();
    let mut marked = sha2::Sha256::digest(&encoded).to_vec();
    marked.extend(encoded);
    std::fs::write(&wal_path, &marked).unwrap();
    let last = path.clone();
    crate::test_runtime::run(move || async move {
        let error = open_at(last, std::time::Duration::from_secs(60), false)
            .await
            .err()
            .expect("must remain unavailable");
        assert!(error.to_string().contains("recovery required"));
    });
    assert_eq!(std::fs::read(&wal_path).unwrap(), marked);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn collection_type_routes_do_not_host_named_resources() {
    let kernel = kernel("collection-types-only").await;
    let txn = bind(&kernel).await;
    let target = KernelTarget::State(
        ["collection", "btree", "orders"]
            .into_iter()
            .map(|segment| segment.parse().expect("path segment"))
            .collect(),
    );
    let error = kernel
        .execute(target, txn, Method::Get, None)
        .await
        .expect_err("a deeper collection URI must not resolve as a hosted resource");
    assert_eq!(error.code(), tc_error::ErrorKind::NotFound);
}
async fn setup(name: &str) -> Kernel {
    setup_with_ttl(name, std::time::Duration::from_secs(3)).await
}
async fn setup_with_ttl(name: &str, ttl: std::time::Duration) -> Kernel {
    setup_with_bootstrap(name, ttl, false).await
}
async fn setup_with_bootstrap(
    name: &str,
    ttl: std::time::Duration,
    bootstrap_required: bool,
) -> Kernel {
    open_at(crate::txn::test_path(name), ttl, bootstrap_required)
        .await
        .expect("construct kernel")
}

async fn open_at(
    path: std::path::PathBuf,
    ttl: std::time::Duration,
    bootstrap_required: bool,
) -> tc_error::TCResult<Kernel> {
    let storage = crate::HostStorage::new(&crate::HostLimits::default().storage);
    let workspace = storage.workspace(path.join("workspace"))?;
    let host: pathlink::Link = crate::uri::HOST_ROOT.parse().expect("host link");
    let (protocol, actor) = workspace
        .load_or_create_protocol_authority(
            &path.file_name().unwrap().to_str().unwrap().parse().unwrap(),
            host.clone(),
        )
        .await
        .expect("protocol authority");
    let application_roots = storage
        .application_roots(path.join("data"))
        .await
        .expect("test application roots");
    let actors = crate::auth::KeyringActorResolver::default();
    actors
        .insert(
            host.clone(),
            crate::auth::Actor::with_verifying_key(actor.id().clone(), actor.verifying_key()),
        )
        .expect("unique test actor");
    let verifier = crate::auth::RjwtTokenVerifier::new(std::sync::Arc::new(actors.clone()));
    let bootstrap = std::sync::Arc::new(
        crate::replication::ReplicationIssuer::new(
            std::sync::Arc::new(protocol.clone()),
            vec![[7; 32].into()],
            actors.clone(),
        )
        .expect("test replication issuer"),
    );
    Kernel::new(
        crate::HostServices {
            application_roots,
            replication: std::sync::Arc::new(crate::replication::LocalClusterGateway),
            rpc: std::sync::Arc::new(crate::gateway::LocalRpcGateway),
            resources: crate::HostResources::default(),
            protocol,
            verifier: std::sync::Arc::new(verifier),
            actors,
            bootstrap,
            bootstrap_required,
        },
        workspace,
        ttl,
    )
    .await
}

#[tokio::test]
async fn configured_seed_bootstrap_gates_readiness() {
    let kernel = setup_with_bootstrap(
        "bootstrap-readiness",
        std::time::Duration::from_secs(3),
        true,
    )
    .await;
    assert!(!kernel.is_ready());
}
#[tokio::test]
async fn service_discovery_and_delete_use_the_native_owner_path() {
    let kernel = setup("service-native").await;
    let identity: pathlink::Link = "/service/example-devco/catalog/1.0.0"
        .parse()
        .expect("identity");
    let manifest = tc_ir::Scalar::Map(tc_ir::Map::new());
    let txn = bind(&kernel).await;
    let txn = txn.with_claims(vec![crate::Claim::new(
        identity.clone(),
        umask::Mode::all(),
    )]);
    let guard = stage_service(&kernel, &txn, identity.clone(), manifest).await;
    assert!(
        !txn.is_locked(),
        "routing must not commit before projection"
    );
    guard.finish_success().await.expect("terminal success");
    assert!(txn.is_locked(), "terminal success must trigger autocommit");
    let txn = bind(&kernel).await;
    let state = execute(
        &kernel,
        Method::Get,
        &identity.to_string(),
        None,
        txn.clone(),
    )
    .await
    .expect("discover Service");
    assert!(matches!(state, crate::State::Map(_)));
    let execution = execute(
        &kernel,
        Method::Get,
        &format!("{identity}/2.0.0/run"),
        None,
        txn.clone(),
    )
    .await
    .expect_err("Service execution suffixes are not routed");
    assert_eq!(execution.code(), tc_error::ErrorKind::NotFound);
    complete(&kernel, txn, crate::txn::TransactionOutcome::Commit).await;
    let txn = bind(&kernel).await;
    let txn = txn.with_claims(vec![crate::Claim::new(
        identity.clone(),
        umask::Mode::all(),
    )]);
    let error = execute(
        &kernel,
        Method::Delete,
        &identity.to_string(),
        Some(crate::State::from(number_general::Number::from(true))),
        txn.clone(),
    )
    .await
    .expect_err("a non-null deletion body must be rejected");
    assert_eq!(error.code(), tc_error::ErrorKind::BadRequest);
    execute(
        &kernel,
        Method::Delete,
        &identity.to_string(),
        Some(crate::State::None),
        txn.clone(),
    )
    .await
    .expect("delete Service");
    complete(&kernel, txn, crate::txn::TransactionOutcome::Commit).await;
    let error = execute(
        &kernel,
        Method::Get,
        &identity.to_string(),
        None,
        bind(&kernel).await,
    )
    .await
    .expect_err("deleted Service must not resolve");
    assert_eq!(error.code(), tc_error::ErrorKind::NotFound);
}
#[tokio::test]
async fn class_instance_routes_bound_methods_with_concrete_self() {
    let kernel = setup("class-self").await;
    let identity: pathlink::Link = "/class/example-devco/named/1.0.0"
        .parse()
        .expect("identity");
    let get_name = tc_ir::Scalar::Ref(Box::new(tc_ir::TCRef::Op(tc_ir::OpRef::Get((
        tc_ir::Subject::Ref(
            "$self".parse().expect("self ref"),
            "name".parse().expect("member path"),
        ),
        tc_ir::Scalar::default(),
    )))));
    let prototype = tc_ir::Map::from_iter([(
        "label".parse().expect("member"),
        tc_ir::Scalar::Op(tc_ir::OpDef::Post(vec![(
            "result".parse().expect("result"),
            get_name,
        )])),
    )]);
    let definition = tc_state::ClassBody::new(
        tc_state::ClassParent::Native(tc_state::StateType::Tuple),
        prototype,
    );
    let txn = bind(&kernel).await;
    let txn = txn.with_claims(vec![crate::Claim::new(
        identity.clone(),
        umask::Mode::all(),
    )]);
    let definition = definition.definition();
    execute(
        &kernel,
        Method::Put,
        "/class",
        Some(crate::State::Tuple(vec![
            crate::State::from(tc_value::Value::Link(identity.clone())),
            crate::State::from_scalar(definition),
        ])),
        txn.clone(),
    )
    .await
    .expect("install Class");
    complete(&kernel, txn, crate::txn::TransactionOutcome::Commit).await;
    let txn = bind(&kernel).await;
    let members = tc_ir::Map::from_iter([(
        "name".parse().expect("member"),
        crate::State::from(tc_value::Value::from("Ada")),
    )]);
    let instance = execute(
        &kernel,
        Method::Post,
        &identity.to_string(),
        Some(crate::State::Map(members)),
        txn.clone(),
    )
    .await
    .expect("construct instance");
    let label = instance
        .post(
            &txn,
            &["label".parse().expect("method path")],
            tc_ir::Map::new(),
        )
        .await
        .expect("invoke bound method");
    assert!(matches!(
        label,
        crate::State::Scalar(tc_ir::Scalar::Value(tc_value::Value::String(value)))
            if value == "Ada"
    ));
}
#[tokio::test]
async fn binds_an_ownerless_transaction_for_native_execution() {
    let kernel = kernel("ownerless").await;
    let deadline = kernel.deadline();
    let guard = kernel
        .begin_request(Method::Get, "/state/scalar/value/number/add", false, None)
        .await
        .expect("bind transaction");
    let upper_bound = kernel.deadline();
    assert!(guard.txn().deadline().instant() >= deadline.instant());
    assert!(guard.txn().deadline().instant() <= upper_bound.instant());
}
#[tokio::test]
async fn ttl_worker_finalizes_an_abandoned_transaction() {
    let kernel = setup_with_ttl("ttl", std::time::Duration::from_millis(10)).await;
    let txn = kernel.test_txn().await;
    let txn_id = txn.id();
    tokio::time::timeout(std::time::Duration::from_secs(4), async {
        while kernel.test_txn_server().contains(&txn_id) {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("TTL finalization");
}
#[tokio::test]
async fn decisions_delegate_repeatedly_until_time_based_finalization() {
    let kernel = kernel("finalize").await;
    let txn = kernel.test_txn().await;
    let txn_id = txn.id();
    kernel
        .test_libraries()
        .claim(&txn)
        .await
        .expect("claim resource");
    kernel
        .coordinate(&txn, crate::txn::TransactionOutcome::Rollback, false)
        .await
        .expect("first decision");
    kernel
        .coordinate(&txn, crate::txn::TransactionOutcome::Rollback, false)
        .await
        .expect("duplicate decision");
    kernel
        .coordinate(&txn, crate::txn::TransactionOutcome::Commit, false)
        .await
        .expect("opposite decision delegates to the idempotent resource lifecycle");
    let error = kernel
        .test_classes()
        .claim(&txn)
        .await
        .expect_err("decided transactions cannot accept more work");
    assert_eq!(error.code(), tc_error::ErrorKind::Conflict);
    drop(txn);
    assert!(kernel.test_txn_server().contains(&txn_id));
}
#[tokio::test]
async fn a_locked_empty_put_decides_only_its_exact_resource() {
    let kernel = setup("exact-resource-decision").await;
    let identity: pathlink::Link = "/service/example-devco/nested/catalog/1.0.0"
        .parse()
        .expect("identity");
    let txn = bind(&kernel).await;
    let txn = txn.with_claims(vec![crate::Claim::new(
        identity.clone(),
        umask::Mode::all(),
    )]);
    let definition = tc_ir::Scalar::Map(tc_ir::Map::new());
    let _guard = stage_service(&kernel, &txn, identity.clone(), definition).await;
    let active_bearer = txn.raw_token().expect("protocol token").to_string();
    let error = match kernel
        .begin_request(
            Method::Put,
            &format!("{identity}?txn_id={}", txn.id()),
            true,
            Some(active_bearer),
        )
        .await
    {
        Ok(_) => panic!("active work authority is not a decision lock"),
        Err(error) => error,
    };
    assert_eq!(error.code(), tc_error::ErrorKind::BadRequest);
    let bearer = kernel.test_txn_server().test_decision_bearer(&txn);
    let resources = txn.claimed_paths();
    let guard = kernel
        .begin_request(
            Method::Put,
            &format!("{identity}/suffix?txn_id={}", txn.id()),
            true,
            Some(bearer.clone()),
        )
        .await
        .expect("bind suffix decision");
    let error = guard
        .execute(None)
        .await
        .expect_err("a suffix is not a resource decision target");
    assert_eq!(error.code(), tc_error::ErrorKind::Conflict);
    let guard = kernel
        .begin_request(
            Method::Put,
            &format!("/service/example-devco/other/1.0.0?txn_id={}", txn.id()),
            true,
            Some(bearer.clone()),
        )
        .await
        .expect("bind missing decision");
    let error = guard
        .execute(None)
        .await
        .expect_err("an unenlisted resource cannot be decided");
    assert_eq!(error.code(), tc_error::ErrorKind::Conflict);
    let guard = kernel
        .begin_request(
            Method::Put,
            &format!("{identity}?txn_id={}", txn.id()),
            true,
            Some(bearer.clone()),
        )
        .await
        .expect("bind resource decision");
    assert!(
        guard
            .execute(None)
            .await
            .expect("resource decision")
            .is_none()
    );
    let duplicate = kernel
        .begin_request(
            Method::Put,
            &format!("{identity}?txn_id={}", txn.id()),
            true,
            Some(bearer.clone()),
        )
        .await
        .expect("bind duplicate resource decision");
    assert!(
        duplicate
            .execute(None)
            .await
            .expect("duplicate decision")
            .is_none()
    );
    for resource in resources.iter().filter(|path| *path != identity.path()) {
        let guard = kernel
            .begin_request(
                Method::Put,
                &format!("{resource}?txn_id={}", txn.id()),
                true,
                Some(bearer.clone()),
            )
            .await
            .expect("bind resource decision");
        assert!(
            guard
                .execute(None)
                .await
                .expect("resource decision")
                .is_none()
        );
    }
    execute(
        &kernel,
        Method::Get,
        &identity.to_string(),
        None,
        bind(&kernel).await,
    )
    .await
    .expect("committed Service must resolve");
}
#[tokio::test]
async fn an_unlocked_empty_mutation_is_ordinary_invalid_input() {
    let kernel = kernel("unlocked-empty-mutation").await;
    for method in [Method::Put, Method::Delete] {
        let error = match kernel
            .begin_request(method, "/service/example-devco/catalog/1.0.0", true, None)
            .await
        {
            Ok(_) => panic!("unlocked empty mutation must be rejected"),
            Err(error) => error,
        };
        assert_eq!(error.code(), tc_error::ErrorKind::BadRequest);
    }
    kernel
        .begin_request(
            Method::Delete,
            "/service/example-devco/catalog/1.0.0",
            false,
            None,
        )
        .await
        .expect("explicit null body binds ordinary deletion");
}
#[tokio::test]
async fn a_locked_decision_excludes_new_work() {
    let kernel = setup("decision-work-race").await;
    let identity: pathlink::Link = "/service/example-devco/race/1.0.0"
        .parse()
        .expect("identity");
    let txn = bind(&kernel).await;
    let txn = txn.with_claims(vec![crate::Claim::new(
        identity.clone(),
        umask::Mode::all(),
    )]);
    let definition = tc_ir::Scalar::Map(tc_ir::Map::new());
    stage_service(&kernel, &txn, identity.clone(), definition).await;
    let bearer = kernel.test_txn_server().test_decision_bearer(&txn);
    let guard = kernel
        .begin_request(
            Method::Put,
            &format!("{identity}?txn_id={}", txn.id()),
            true,
            Some(bearer),
        )
        .await
        .expect("bind decision");
    assert!(guard.execute(None).await.expect("decision").is_none());
    let error = kernel
        .test_classes()
        .claim(&txn)
        .await
        .expect_err("locked work must fail");
    assert_eq!(error.code(), tc_error::ErrorKind::Conflict);
}
