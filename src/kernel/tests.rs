use super::*;
use tc_ir::Public;
async fn stage_service(
    kernel: &Kernel,
    txn: &crate::TxnHandle,
    identity: pathlink::Link,
    definition: tc_ir::Scalar,
) -> KernelRequestGuard {
    let deadline = txn.deadline();
    let guard = KernelRequestGuard {
        kernel: kernel.clone(),
        method: Method::Put,
        target: KernelTarget::Application("/service".parse().expect("application target")),
        txn: txn.clone(),
        _permit: kernel
            .resources()
            .admit_request(deadline)
            .await
            .expect("request admission"),
    };
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
        .ok_or_else(|| tc_error::TCError::internal("ordinary request returned no response"))
}
async fn bind(kernel: &Kernel) -> crate::TxnHandle {
    kernel
        .txn_server
        .bind(None, None, std::sync::Arc::clone(&kernel.inner))
        .await
        .expect("bind transaction")
        .with_deadline(kernel.resources().deadline())
}
async fn complete(kernel: &Kernel, txn: crate::TxnHandle, outcome: crate::txn::TransactionOutcome) {
    kernel
        .coordinate(&txn, outcome, false)
        .await
        .expect("complete transaction");
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
    let workspace = crate::txn::test_workspace(name);
    let host: pathlink::Link = crate::uri::HOST_ROOT.parse().expect("host link");
    let (protocol, actor) = workspace
        .load_or_create_protocol_authority(&"test-host".parse().unwrap(), host.clone())
        .await
        .expect("protocol authority");
    let storage = crate::HostStorage::new(&crate::HostLimits::default().storage);
    let application_roots = storage
        .application_roots(crate::txn::test_path(&format!("apps-{name}")))
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
        crate::replication::ReplicationIssuer::local(&protocol, actors.clone())
            .expect("test replication issuer"),
    );
    let kernel = Kernel::new(
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
    .expect("construct kernel");
    kernel
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
    let deadline = kernel.resources().deadline();
    let guard = kernel
        .begin_request(
            Method::Get,
            "/state/scalar/value/number/add",
            false,
            None,
            deadline,
        )
        .await
        .expect("bind transaction");
    assert_eq!(guard.txn().deadline().instant(), deadline.instant());
}
#[tokio::test]
async fn ttl_worker_finalizes_an_abandoned_transaction() {
    let kernel = setup_with_ttl("ttl", std::time::Duration::from_millis(10)).await;
    let txn = kernel
        .txn_server
        .bind(None, None, std::sync::Arc::clone(&kernel.inner))
        .await
        .expect("bind transaction")
        .with_deadline(kernel.resources().deadline());
    let txn_id = txn.id();
    tokio::time::timeout(std::time::Duration::from_secs(4), async {
        while kernel.txn_server.contains(&txn_id) {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("TTL finalization");
}
#[tokio::test]
async fn decisions_delegate_repeatedly_until_time_based_finalization() {
    let kernel = kernel("finalize").await;
    let txn = kernel
        .txn_server
        .bind(None, None, std::sync::Arc::clone(&kernel.inner))
        .await
        .expect("bind transaction")
        .with_deadline(kernel.resources().deadline());
    let txn_id = txn.id();
    kernel
        .inner
        .libraries
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
        .inner
        .classes
        .claim(&txn)
        .await
        .expect_err("decided transactions cannot accept more work");
    assert_eq!(error.code(), tc_error::ErrorKind::Conflict);
    drop(txn);
    assert!(kernel.txn_server.contains(&txn_id));
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
            kernel.resources().deadline(),
        )
        .await
    {
        Ok(_) => panic!("active work authority is not a decision lock"),
        Err(error) => error,
    };
    assert_eq!(error.code(), tc_error::ErrorKind::BadRequest);
    let bearer = kernel.txn_server.test_decision_bearer(&txn);
    let resources = txn.claimed_paths();
    let guard = kernel
        .begin_request(
            Method::Put,
            &format!("{identity}/suffix?txn_id={}", txn.id()),
            true,
            Some(bearer.clone()),
            kernel.resources().deadline(),
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
            kernel.resources().deadline(),
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
            kernel.resources().deadline(),
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
            kernel.resources().deadline(),
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
                kernel.resources().deadline(),
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
            .begin_request(
                method,
                "/service/example-devco/catalog/1.0.0",
                true,
                None,
                kernel.resources().deadline(),
            )
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
            kernel.resources().deadline(),
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
    let bearer = kernel.txn_server.test_decision_bearer(&txn);
    let guard = kernel
        .begin_request(
            Method::Put,
            &format!("{identity}?txn_id={}", txn.id()),
            true,
            Some(bearer),
            crate::Deadline::after(std::time::Duration::from_secs(1)),
        )
        .await
        .expect("bind decision");
    assert!(guard.execute(None).await.expect("decision").is_none());
    let error = kernel
        .inner
        .classes
        .claim(&txn)
        .await
        .expect_err("locked work must fail");
    assert_eq!(error.code(), tc_error::ErrorKind::Conflict);
}
