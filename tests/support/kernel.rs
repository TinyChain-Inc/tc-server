use tc_ir::Public;

use crate::BoundRequest;
use super::*;

const SERVICE_ROOT: pathlink::PathLabel = pathlink::path_label(&["service"]);

impl Kernel {
    pub(crate) async fn test_txn(&self) -> crate::TxnHandle {
        self.test_bind(None, None)
            .await
            .expect("begin test transaction")
    }

    pub(crate) fn test_txn_server(&self) -> &TxnServer {
        &self.txn_server
    }

    pub(crate) fn test_libraries(&self) -> Root<crate::library::Library> {
        self.inner.libraries.clone()
    }

    pub(crate) fn test_classes(&self) -> Root<crate::class::Class> {
        self.inner.classes.clone()
    }

    pub(crate) fn test_services(&self) -> Root<crate::service::Service> {
        self.inner.services.clone()
    }

    pub(crate) async fn test_bind(
        &self,
        txn_id: Option<TxnId>,
        context: Option<&crate::auth::AuthContext>,
    ) -> TCResult<crate::TxnHandle> {
        self.txn_server
            .bind(txn_id, context, std::sync::Arc::clone(&self.inner))
            .await
    }

    pub(crate) fn with_test_txn_server(&self, txn_server: TxnServer) -> Self {
        Self {
            txn_server,
            ..self.clone()
        }
    }

    pub(crate) fn test_seed_txn(&self, id: TxnId) -> TCResult<crate::TxnHandle> {
        self.txn_server
            .bind_seed(id, std::sync::Arc::clone(&self.inner))
    }

    pub(crate) fn test_recovery_txn(&self, id: TxnId) -> TCResult<crate::TxnHandle> {
        self.txn_server
            .bind_recovery(id, std::sync::Arc::clone(&self.inner))
    }
}

async fn stage_service(
    kernel: &Kernel,
    txn: &crate::TxnHandle,
    identity: pathlink::Link,
    definition: tc_ir::Scalar,
) -> BoundRequest {
    let permit = kernel
        .txn_server
        .resources()
        .admit_request(kernel.deadline())
        .await
        .expect("request admission");

    let request = BoundRequest::new(
        kernel.clone(),
        Method::Put,
        KernelTarget::Application(SERVICE_ROOT.into()),
        txn.clone(),
        permit,
        kernel.drain.admit().unwrap(),
    );

    // These native lifecycle fixtures deliberately retain undecided work.
    kernel
        .execute(
            KernelTarget::Application(SERVICE_ROOT.into()),
            txn.clone(),
            Method::Put,
            Some(crate::State::Tuple(vec![
                crate::State::from(tc_value::Value::Link(identity)),
                crate::State::from(definition),
            ])),
        )
        .await
        .expect("stage Service");

    request
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
        use futures::FutureExt;

        use crate::cluster::AsyncHash;

        let source = setup_with_ttl("snapshot-source", std::time::Duration::from_secs(60)).await;
        let target = setup_with_ttl("snapshot-target", std::time::Duration::from_secs(60)).await;
        let identity: pathlink::Link = "/service/test/native/1.0.0".parse().unwrap();
        for kernel in [&source, &target] {
            let txn = service_txn(kernel, &identity).await;
            stage_service(kernel, &txn, identity.clone(), executable_service(true)).await;
            complete(kernel, txn, crate::txn::TransactionOutcome::Commit).await;
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

        let (txn, session, entry) = target
            .inner
            .services
            .bootstrap_resource(&target, &endpoint, &identity, &replica)
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

        let error = entry
            .synchronize(&txn, &endpoint, &mismatched)
            .await
            .unwrap_err();

        assert_eq!(error.code(), tc_error::ErrorKind::Conflict);
        target.inner.services.rollback(&txn.id()).await.unwrap();

        // Cancellation while the network snapshot is pending selects no decision.
        let (txn, session, entry) = target
            .inner
            .services
            .bootstrap_resource(&target, &endpoint, &identity, &replica)
            .await
            .unwrap();

        let stalled = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let stalled_endpoint = format!("http://{}", stalled.local_addr().unwrap());
        {
            let pending = entry.synchronize(&txn, &stalled_endpoint, &session);
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

        let (txn, session, entry) = target
            .inner
            .services
            .bootstrap_resource(&target, &endpoint, &identity, &replica)
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
        entry.synchronize(&txn, &endpoint, &session).await.unwrap();
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
            matches!(&value, crate::State::Scalar(scalar) if matches!(&scalar, tc_ir::Scalar::Value(value) if matches!(&value, tc_value::Value::Number(n) if n.to_string() == "1")))
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

        assert!(matches!(&response, crate::State::Tuple(replicas) if replicas.is_empty()));
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
            stage_service(&kernel, &txn, identity.clone(), executable_service(table)).await;
            complete(&kernel, txn, crate::txn::TransactionOutcome::Commit).await;
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
                matches!(&pending, crate::State::Scalar(scalar) if matches!(&scalar, tc_ir::Scalar::Value(value) if matches!(&value, tc_value::Value::Number(n) if n.to_string() == "1")))
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

            let requests: Vec<_> =
                std::fs::read_dir(path.join("data/service/test/native/1.0.0/.native/data/wal"))
                    .unwrap()
                    .map(Result::unwrap)
                    .filter(|entry| entry.file_name() != "committed.chain_block")
                    .collect();

            assert_eq!(requests.len(), 3);
            for entry in requests {
                let bytes = std::fs::read(entry.path()).unwrap();
                let wal: serde_json::Value = serde_json::from_slice(&bytes[32..]).unwrap();
                assert_eq!(wal[0], "transaction");
                assert_eq!(wal[2].as_array().unwrap().len(), 1);
            }

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
                matches!(&count, crate::State::Scalar(scalar) if matches!(&scalar, tc_ir::Scalar::Value(value) if matches!(&value, tc_value::Value::Number(n) if n.to_string() == "3")))
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
                matches!(&label, crate::State::Scalar(scalar) if matches!(&scalar, tc_ir::Scalar::Value(value) if matches!(&value, tc_value::Value::String(s) if s == "native")))
            );

            kernel.shutdown().await;
        }
    });
}

fn failing_mutations() -> tc_ir::Scalar {
    use pathlink::{PathLabel, path_label};

    use tc_ir::{After, IdRef, OpDef, OpRef, Scalar, Subject, TCRef};
    use tc_value::Value;

    const INSERT: PathLabel = path_label(&["data", "insert"]);
    const DELETE: PathLabel = path_label(&["data", "delete"]);

    let Scalar::Map(mut members) = executable_service(false) else {
        unreachable!("Service definition is a map")
    };

    let subject = |path| Subject::Ref(IdRef::new("self".parse().unwrap()), path);
    let parameter = |name: &str| Scalar::from(TCRef::Id(IdRef::new(name.parse().unwrap())));
    let insert = |key| {
        Scalar::from(TCRef::Op(OpRef::Put((
            subject(INSERT.into()),
            key,
            parameter("value"),
        ))))
    };

    let delete = |key| Scalar::from(TCRef::Op(OpRef::Delete((subject(DELETE.into()), key))));

    // The first native Chain call changes the pending collection. The second
    // fails validation before the outer Service handler returns success.
    members.insert(
        "fail_post".parse().unwrap(),
        Scalar::Op(OpDef::Post(vec![(
            "result".parse().unwrap(),
            TCRef::After(Box::new(After::new(
                insert(Scalar::default()),
                insert(Value::from(1_u64).into()),
            )))
            .into(),
        )])),
    );

    members.insert(
        "fail_put".parse().unwrap(),
        Scalar::Op(OpDef::Put((
            "key".parse().unwrap(),
            "value".parse().unwrap(),
            vec![(
                "result".parse().unwrap(),
                TCRef::After(Box::new(After::new(
                    insert(Scalar::default()),
                    insert(Value::from(1_u64).into()),
                )))
                .into(),
            )],
        ))),
    );

    members.insert(
        "fail_delete".parse().unwrap(),
        Scalar::Op(OpDef::Delete((
            "key".parse().unwrap(),
            vec![(
                "result".parse().unwrap(),
                TCRef::After(Box::new(After::new(
                    delete(parameter("key")),
                    delete(Value::from(false).into()),
                )))
                .into(),
            )],
        ))),
    );

    Scalar::Map(members)
}

#[test]
fn returned_write_errors_roll_back_partial_service_mutations() {
    crate::test_runtime::run(|| async {
        use pathlink::{PathLabel, label, path_label};

        const IDENTITY: PathLabel = path_label(&["service", "test", "failed-write", "1.0.0"]);

        for (method, member, expected, initial_count) in [
            (
                Method::Put,
                "fail_put",
                "BTree insert expects a null key",
                0,
            ),
            (Method::Delete, "fail_delete", "expected BTree row tuple", 1),
            (
                Method::Post,
                "fail_post",
                "BTree insert expects a null key",
                0,
            ),
        ] {
            let kernel = setup_with_ttl(member, std::time::Duration::from_secs(60)).await;
            let identity = pathlink::Link::from(IDENTITY);
            let install = service_txn(&kernel, &identity).await;
            stage_service(&kernel, &install, identity.clone(), failing_mutations()).await;
            complete(&kernel, install, crate::txn::TransactionOutcome::Commit).await;

            let row = tc_value::Value::Tuple(vec![7_u64.into()]);
            let insertion =
                || crate::State::Tuple(vec![tc_value::Value::None.into(), row.clone().into()]);

            let insert_path = identity.path().clone().append(label("insert"));
            if initial_count != 0 {
                let seed = service_txn(&kernel, &identity).await;
                execute(
                    &kernel,
                    Method::Put,
                    &insert_path.to_string(),
                    Some(insertion()),
                    seed.clone(),
                )
                .await
                .unwrap();

                complete(&kernel, seed, crate::txn::TransactionOutcome::Commit).await;
            }

            let txn = service_txn(&kernel, &identity).await;
            let permit = kernel
                .txn_server
                .resources()
                .admit_request(txn.deadline())
                .await
                .unwrap();

            let request = BoundRequest::new(
                kernel.clone(),
                method,
                KernelTarget::Application(
                    identity
                        .path()
                        .clone()
                        .append(member.parse::<pathlink::PathSegment>().unwrap())
                        .into(),
                ),
                txn.clone(),
                permit,
                kernel.drain.admit().unwrap(),
            );

            let body = match method {
                Method::Put => insertion(),
                Method::Post => crate::State::Map(
                    [("value".parse().unwrap(), row.clone().into())]
                        .into_iter()
                        .collect(),
                ),
                Method::Delete => row.clone().into(),
                Method::Get => unreachable!("mutation cases"),
            };

            let error = request
                .execute_bound(Some(body))
                .await
                .map(|_| ())
                .expect_err("malformed mutation");

            assert_eq!(error.code(), tc_error::ErrorKind::BadRequest);
            assert!(error.message().contains(expected), "{error:?}");
            assert!(
                txn.is_locked(),
                "returned write errors must select rollback"
            );

            let read = service_txn(&kernel, &identity).await;
            let count_path = identity.path().clone().append(label("count"));
            let count = tokio::time::timeout(
                std::time::Duration::from_secs(3),
                execute(
                    &kernel,
                    Method::Get,
                    &count_path.to_string(),
                    None,
                    read.clone(),
                ),
            )
            .await
            .expect("rollback must release the earlier transaction's reservations")
            .unwrap();

            assert!(matches!(
                &count,
                crate::State::Scalar(tc_ir::Scalar::Value(tc_value::Value::Number(number)))
                    if number.to_string() == initial_count.to_string()
            ));

            complete(&kernel, read, crate::txn::TransactionOutcome::Rollback).await;

            let error = kernel
                .coordinate(&txn, crate::txn::TransactionOutcome::Commit, false)
                .await
                .expect_err("Chain must reject commit after rollback");

            assert_eq!(error.code(), tc_error::ErrorKind::Conflict);

            let retry = service_txn(&kernel, &identity).await;
            tokio::time::timeout(
                std::time::Duration::from_secs(3),
                execute(
                    &kernel,
                    Method::Put,
                    &insert_path.to_string(),
                    Some(insertion()),
                    retry.clone(),
                ),
            )
            .await
            .expect("rollback must release native write permits")
            .unwrap();

            complete(&kernel, retry, crate::txn::TransactionOutcome::Commit).await;
            kernel.shutdown().await;
        }
    });
}

#[tokio::test]
async fn ownerless_write_errors_do_not_select_a_decision() {
    use tc_value::class::NativeClass;

    let kernel = kernel("ownerless-write-error").await;
    let target = tc_value::ValueType::Number(number_general::NumberType::Number)
        .path()
        .append(pathlink::label("add"));

    for method in [Method::Put, Method::Post, Method::Delete] {
        let request = kernel
            .begin_request(method, &target.to_string(), false, None)
            .await
            .unwrap();

        let txn = request.txn().clone();
        let body = match method {
            Method::Put => crate::State::Tuple(vec![crate::State::None, crate::State::None]),
            Method::Post => crate::State::Map(tc_ir::Map::new()),
            Method::Delete => crate::State::None,
            Method::Get => unreachable!("mutation cases"),
        };

        let error = match request.execute_bound(Some(body)).await {
            Err(error) => error,
            Ok(_) => panic!("static numeric addition has no mutation handler"),
        };

        assert_eq!(
            error.code(),
            if method == Method::Post {
                tc_error::ErrorKind::BadRequest
            } else {
                tc_error::ErrorKind::MethodNotAllowed
            }
        );

        assert!(txn.raw_token().is_none());
        assert!(!txn.is_locked());
    }

    kernel.shutdown().await;
}

#[test]
fn service_recovery_preserves_expired_original_ids_and_rejects_invalid_frontiers() {
    use sha2::Digest;

    let path = crate::txn::test_path("service-recovery");
    let first = path.clone();
    let id = crate::test_runtime::run(move || async move {
        let kernel = open_at(first, std::time::Duration::from_secs(60), false)
            .await
            .unwrap();
        let identity: pathlink::Link = "/service/test/native/1.0.0".parse().unwrap();
        let txn = service_txn(&kernel, &identity).await;
        stage_service(&kernel, &txn, identity.clone(), executable_service(true)).await;
        complete(&kernel, txn, crate::txn::TransactionOutcome::Commit).await;
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
        kernel.shutdown().await;
        txn.id()
    });

    let wal_path = path.join(format!(
        "data/service/test/native/1.0.0/.native/data/wal/{id}.chain_block"
    ));

    let committed = std::fs::read(&wal_path).unwrap();
    assert!(
        serde_json::from_slice::<serde_json::Value>(&committed[32..]).unwrap()[1] == id.to_string()
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
            std::fs::read(second.join(format!(
                "data/service/test/native/1.0.0/.native/data/wal/{id}.chain_block"
            )))
            .unwrap(),
            expected
        );

        assert!(kernel.is_ready());
        let txn = bind(&kernel).await;
        let read_id = txn.id();
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
            matches!(&count, crate::State::Scalar(scalar) if matches!(&scalar, tc_ir::Scalar::Value(value) if matches!(&value, tc_value::Value::Number(n) if n.to_string() == "1")))
        );

        // Finish native finalization before shutting down its I/O runtime.
        // A cancelled materialization requires recovery, even in a restart fixture.
        tokio::time::timeout(std::time::Duration::from_secs(4), async {
            while kernel.test_txn_server().contains(&read_id) {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("recovered transaction finalization");

        kernel.shutdown().await;
    });

    // A valid checksum cannot make an unsupported frontier layout acceptable.
    let wal_path =
        path.join("data/service/test/native/1.0.0/.native/data/wal/committed.chain_block");

    let bytes = std::fs::read(&wal_path).unwrap();
    let mut wal: serde_json::Value = serde_json::from_slice(&bytes[32..]).unwrap();
    wal[1] = serde_json::Value::Null;
    wal.as_array_mut()
        .unwrap()
        .push(serde_json::Value::String(id.to_string()));

    let encoded = serde_json::to_vec(&wal).unwrap();
    let mut marked = sha2::Sha256::digest(&encoded).to_vec();
    marked.extend(encoded);
    std::fs::write(&wal_path, &marked).unwrap();
    let last = path.clone();
    crate::test_runtime::run(move || async move {
        assert!(
            open_at(last, std::time::Duration::from_secs(60), false)
                .await
                .is_err()
        );
    });

    assert_eq!(std::fs::read(&wal_path).unwrap(), marked);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn late_service_recovery_failure_never_publishes_or_reclaims_history() {
    let path = crate::txn::test_path("late-service-recovery-failure");
    let first = path.clone();
    let ids = crate::test_runtime::run(move || async move {
        let kernel = open_at(first, std::time::Duration::from_secs(60), false)
            .await
            .unwrap();

        let identity: pathlink::Link = "/service/test/native/1.0.0".parse().unwrap();
        let install = service_txn(&kernel, &identity).await;
        stage_service(
            &kernel,
            &install,
            identity.clone(),
            executable_service(true),
        )
        .await;

        complete(&kernel, install, crate::txn::TransactionOutcome::Commit).await;
        let mut ids = Vec::new();
        for n in [1_u64, 2] {
            let txn = service_txn(&kernel, &identity).await;
            execute(
                &kernel,
                Method::Put,
                "/service/test/native/1.0.0/insert",
                Some(crate::State::Tuple(vec![
                    tc_value::Value::Tuple(vec![n.into()]).into(),
                    tc_value::Value::Tuple(vec![n.into()]).into(),
                ])),
                txn.clone(),
            )
            .await
            .unwrap();

            ids.push(txn.id());
            complete(&kernel, txn, crate::txn::TransactionOutcome::Commit).await;
        }

        kernel.shutdown().await;
        ids
    });

    let wal = path.join("data/service/test/native/1.0.0/.native/data/wal");
    let first_file = wal.join(format!("{}.chain_block", ids[0]));
    let last_file = wal.join(format!("{}.chain_block", ids[1]));
    let earlier = std::fs::read(&first_file).unwrap();
    let frontier = std::fs::read(wal.join("committed.chain_block")).unwrap();
    let original = std::fs::read(&last_file).unwrap();
    let mut corrupt = original.clone();
    corrupt[0] ^= 1;
    std::fs::write(&last_file, &corrupt).unwrap();
    std::fs::remove_dir_all(path.join("workspace/txn")).unwrap();
    let second = path.clone();
    crate::test_runtime::run(move || async move {
        assert!(
            open_at(second, std::time::Duration::from_secs(60), false)
                .await
                .is_err()
        );
    });

    // Durable native decisions require no replay capability or fresh workspace.
    // Failed construction publishes no kernel and starts no expiry or WAL cleanup.
    assert!(!path.join("workspace/txn").join(ids[0].to_string()).exists());
    assert_eq!(std::fs::read(&first_file).unwrap(), earlier);
    assert_eq!(std::fs::read(&last_file).unwrap(), corrupt);
    assert_eq!(
        std::fs::read(wal.join("committed.chain_block")).unwrap(),
        frontier
    );

    std::fs::write(&last_file, original).unwrap();
    let third = path.clone();
    crate::test_runtime::run(move || async move {
        let kernel = open_at(third, std::time::Duration::from_secs(60), false)
            .await
            .unwrap();

        let count = execute(
            &kernel,
            Method::Get,
            "/service/test/native/1.0.0/count",
            None,
            bind(&kernel).await,
        )
        .await
        .unwrap();

        assert!(
            matches!(count, crate::State::Scalar(tc_ir::Scalar::Value(value)) if value == tc_value::Value::from(2_u64))
        );

        kernel.shutdown().await;
    });

    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "http-client")]
mod rollback_propagation {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use pathlink::{Link, PathBuf, PathLabel, label, path_label};
    use tc_error::{TCError, TCResult};
    use tc_ir::{Method, Scalar};
    use tokio::sync::{mpsc, oneshot};

    use crate::cluster::AsyncHash;
    use crate::kernel::types::KernelTarget;
    use crate::{BoundRequest, State, TxnHandle};
    use super::{
        complete, executable_service, execute, open_at_with_gateway, service_txn, stage_service,
    };

    const IDENTITY: PathLabel = path_label(&["service", "test", "rollback-propagation", "1.0.0"]);

    type Reply = oneshot::Sender<TCResult<()>>;

    struct Gateway {
        decisions: mpsc::Sender<(PathBuf, Reply)>,
        mutations: std::sync::Mutex<Option<mpsc::Sender<Reply>>>,
    }

    #[async_trait]
    impl crate::replication::ClusterGateway for Gateway {
        async fn put(
            &self,
            _peer: &str,
            _token: &str,
            _txn: &TxnHandle,
            _target: &Link,
            _key: Scalar,
            _value: State,
        ) -> TCResult<()> {
            let gate = self.mutations.lock().unwrap().clone();
            if let Some(gate) = gate {
                let (reply, result) = oneshot::channel();
                gate.send(reply).await.unwrap();
                result
                    .await
                    .expect("fixture controls the mutation response")
            } else {
                Ok(())
            }
        }

        async fn delete(
            &self,
            _peer: &str,
            _token: &str,
            _txn: &TxnHandle,
            _target: &Link,
            _key: Scalar,
        ) -> TCResult<()> {
            Ok(())
        }

        async fn decide_resource(
            &self,
            _peer: &str,
            _token: &str,
            _txn: &TxnHandle,
            resource: &PathBuf,
            commit: bool,
        ) -> TCResult<()> {
            if commit {
                return Ok(());
            }

            let (reply, result) = oneshot::channel();
            self.decisions
                .send((resource.clone(), reply))
                .await
                .unwrap();

            result
                .await
                .expect("fixture must answer or cancel the rollback")
        }
    }

    #[test]
    fn failed_or_interrupted_rollback_never_applies_the_local_decision() {
        const CHILD: &str = "TC_ROLLBACK_FATAL_CASE";
        if std::env::var_os(CHILD).is_none() {
            for case in ["error", "deadline", "execution_panic"] {
                #[cfg(unix)]
                let mut child = {
                    let mut command = std::process::Command::new("sh");
                    command
                        .args(["-c", "ulimit -c 0; exec \"$@\"", "rollback-probe"])
                        .arg(std::env::current_exe().unwrap());

                    command
                };

                #[cfg(not(unix))]
                let mut child = std::process::Command::new(std::env::current_exe().unwrap());

                let output = child.args([
                    "--exact",
                    "kernel::kernel::tests::rollback_propagation::failed_or_interrupted_rollback_never_applies_the_local_decision",
                    "--nocapture",
                ]).env(CHILD, case).output().unwrap();

                #[cfg(unix)]
                {
                    use std::os::unix::process::ExitStatusExt;

                    assert_eq!(output.status.signal(), Some(6), "{case}: {output:?}");
                }

                #[cfg(not(unix))]
                assert!(!output.status.success(), "{case}: {output:?}");

                assert!(
                    String::from_utf8_lossy(&output.stderr)
                        .contains("transaction recovery required")
                );

                if case == "execution_panic" {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    assert!(stderr.contains("fixture controls the mutation response"));
                    assert!(stderr.contains("request decision worker panicked"));
                }
            }
        }

        let cases = match std::env::var(CHILD) {
            Ok(case) => vec![case],
            Err(_) => vec![
                "cancellation".into(),
                "execution_cancellation".into(),
                "execution_deadline".into(),
            ],
        };

        crate::test_runtime::run(move || async move {
            for case in cases {
                let case = case.as_str();
                let (sender, mut decisions) = mpsc::channel(1);
                let gateway = Arc::new(Gateway {
                    decisions: sender,
                    mutations: std::sync::Mutex::new(None),
                });

                let kernel = open_at_with_gateway(
                    crate::txn::test_path(case),
                    Duration::from_secs(60),
                    false,
                    gateway.clone(),
                )
                .await
                .unwrap();

                // Namespace installation and native member mutation have different
                // owners. Install first, then retain undecided work in the Service.
                let identity = Link::from(IDENTITY);
                let install = service_txn(&kernel, &identity).await;
                stage_service(
                    &kernel,
                    &install,
                    identity.clone(),
                    executable_service(false),
                )
                .await;

                complete(&kernel, install, crate::txn::TransactionOutcome::Commit).await;

                let replicas = PathBuf::from(IDENTITY).append(label("replicas"));
                let join = service_txn(&kernel, &identity).await.with_claims(vec![
                    crate::Claim::new(identity.clone(), umask::Mode::all()),
                    crate::Claim::new(replicas.clone().into(), umask::USER_WRITE),
                ]);

                let service = kernel
                    .inner
                    .services
                    .clone()
                    .lookup(&join, &identity.path()[1..])
                    .await
                    .unwrap()
                    .exact_item()
                    .unwrap()
                    .unwrap();

                service.claim(&join).await.unwrap();
                let replica = kernel
                    .inner
                    .bootstrap
                    .self_identity("http://replica.invalid".into())
                    .unwrap();

                let hash = service.state().hash(&join).await.unwrap();
                execute(
                    &kernel,
                    Method::Put,
                    &replicas.to_string(),
                    Some(State::Tuple(vec![
                        tc_value::Value::from(replica.endpoint.clone()).into(),
                        crate::cluster::replica_put_state(&replica, hex::encode(hash)),
                    ])),
                    join.clone(),
                )
                .await
                .unwrap();

                complete(&kernel, join, crate::txn::TransactionOutcome::Commit).await;

                let txn = service_txn(&kernel, &identity).await;
                execute(
                    &kernel,
                    Method::Put,
                    &identity.clone().append(label("insert")).to_string(),
                    Some(State::Tuple(vec![
                        tc_value::Value::None.into(),
                        tc_value::Value::Tuple(vec![7_u64.into()]).into(),
                    ])),
                    txn.clone(),
                )
                .await
                .unwrap();

                let State::Collection(tc_collection::Collection::BTree(pending)) = execute(
                    &kernel,
                    Method::Get,
                    &identity.clone().append(label("data")).to_string(),
                    None,
                    txn.clone(),
                )
                .await
                .unwrap() else {
                    panic!("expected a native BTree member");
                };

                let txn = if case == "deadline" {
                    txn.restrict_deadline(crate::Deadline::after(Duration::from_secs(5)))
                } else if case == "execution_deadline" {
                    txn.restrict_deadline(crate::Deadline::after(Duration::from_secs(1)))
                } else {
                    txn
                };

                let permit = kernel
                    .txn_server
                    .resources()
                    .admit_request(txn.deadline())
                    .await
                    .unwrap();

                let (mutations, mut pending_mutations) = mpsc::channel(1);
                let executing = matches!(
                    case,
                    "execution_cancellation" | "execution_deadline" | "execution_panic"
                );

                if executing {
                    *gateway.mutations.lock().unwrap() = Some(mutations);
                }

                let request = BoundRequest::new(
                    kernel.clone(),
                    if executing {
                        Method::Put
                    } else {
                        Method::Delete
                    },
                    KernelTarget::Application(if executing {
                        identity.append(label("insert"))
                    } else {
                        identity
                    }),
                    txn.clone(),
                    permit,
                    kernel.drain.admit().unwrap(),
                );

                let body = if executing {
                    State::Tuple(vec![
                        tc_value::Value::None.into(),
                        tc_value::Value::Tuple(vec![8_u64.into()]).into(),
                    ])
                } else {
                    tc_value::Value::from(true).into()
                };

                let mut failed = Box::pin(request.execute_bound(Some(body)));
                if executing {
                    let mutation = tokio::select! {
                        mutation = pending_mutations.recv() => mutation.unwrap(),
                        result = &mut failed => panic!("execution did not block: {:?}", result.err()),
                    };

                    assert_eq!(pending.count(txn.id()).await.unwrap(), 2);
                    if case == "execution_panic" {
                        // The gateway's receive must panic during real request execution.
                        // Successful rollback would incorrectly turn this into an error.
                        drop(mutation);
                        tokio::select! {
                            decision = decisions.recv() => panic!("panic selected rollback: {decision:?}"),
                            result = &mut failed => panic!("panic returned a request result: {:?}", result.err()),
                            _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("execution panic did not terminate the host"),
                        }
                    }

                    // Retain the receiver only for the timeout case; dropping the
                    // other future must independently signal execution cancellation.
                    let mut waiting = (case == "execution_deadline").then_some(failed);
                    let (_, reply) = tokio::time::timeout(Duration::from_secs(5), decisions.recv())
                        .await
                        .unwrap()
                        .unwrap();

                    assert!(
                        mutation.is_closed(),
                        "execution must be released before rollback"
                    );

                    reply.send(Ok(())).unwrap();
                    if let Some(failed) = waiting.take() {
                        let error = failed.await.err().expect("execution deadline must fail");
                        assert_eq!(error.code(), tc_error::ErrorKind::Unavailable);
                    }

                    kernel.shutdown().await;
                    assert_eq!(pending.count(txn.id()).await.unwrap(), 0);
                    continue;
                }

                let (resource, reply) = tokio::select! {
                    decision = decisions.recv() => decision.unwrap(),
                    result = &mut failed => panic!("rollback was not propagated: {:?}", result.err()),
                };

                assert_eq!(resource, PathBuf::from(IDENTITY));
                assert!(txn.is_locked());
                assert_eq!(pending.count(txn.id()).await.unwrap(), 1);

                match case {
                    "error" => {
                        reply
                            .send(Err(TCError::bad_gateway("injected rollback failure")))
                            .unwrap();

                        let _ = failed.await;
                        panic!("rollback failure must terminate the host");
                    }
                    "deadline" => {
                        let _reply = reply;
                        let _ = failed.await;
                        panic!("cleanup deadline must terminate the host");
                    }
                    "cancellation" => {
                        drop(failed);
                        assert!(
                            !reply.is_closed(),
                            "caller cancellation must retain rollback work"
                        );

                        let mut shutdown = Box::pin(kernel.shutdown());
                        assert!(futures::poll!(&mut shutdown).is_pending());
                        reply.send(Ok(())).unwrap();
                        tokio::time::timeout(Duration::from_secs(10), &mut shutdown)
                            .await
                            .unwrap();
                    }
                    _ => unreachable!(),
                }

                assert!(
                    decisions.try_recv().is_err(),
                    "rollback must not be retried"
                );

                let read = txn.id();
                assert_eq!(pending.count(read).await.unwrap(), 0);
                kernel.shutdown().await;
            }
        });
    }
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
    open_at_with_gateway(
        path,
        ttl,
        bootstrap_required,
        std::sync::Arc::new(crate::replication::LocalClusterGateway),
    )
    .await
}

async fn open_at_with_gateway(
    path: std::path::PathBuf,
    ttl: std::time::Duration,
    bootstrap_required: bool,
    replication: std::sync::Arc<dyn crate::replication::ClusterGateway>,
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
            replication,
            rpc: std::sync::Arc::new(crate::gateway::LocalRpcGateway),
            resources: crate::HostResources::new(crate::HostLimits {
                transaction_ttl: ttl,
                ..Default::default()
            })?,
            protocol,
            verifier: std::sync::Arc::new(verifier),
            actors,
            bootstrap,
            bootstrap_required,
        },
        workspace,
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

    let permit = kernel
        .txn_server
        .resources()
        .admit_request(txn.deadline())
        .await
        .unwrap();

    let request = BoundRequest::new(
        kernel.clone(),
        Method::Put,
        KernelTarget::Application(SERVICE_ROOT.into()),
        txn.clone(),
        permit,
        kernel.drain.admit().unwrap(),
    );

    let state = request
        .execute_bound(Some(crate::State::Tuple(vec![
            tc_value::Value::Link(identity.clone()).into(),
            manifest.into(),
        ])))
        .await
        .expect("install Service");

    assert!(txn.is_locked(), "execution must commit before projection");
    assert!(state.is_none());
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

    assert!(matches!(&state, crate::State::Map(_)));
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
    let error = execute(
        &kernel,
        Method::Delete,
        &identity.to_string(),
        Some(crate::State::None),
        txn.clone(),
    )
    .await
    .expect_err("an admitted item cannot become its namespace's structural mutation");

    assert_eq!(error.code(), tc_error::ErrorKind::Conflict);
    complete(&kernel, txn, crate::txn::TransactionOutcome::Rollback).await;

    let txn = service_txn(&kernel, &identity).await;
    execute(
        &kernel,
        Method::Delete,
        &identity.to_string(),
        Some(crate::State::None),
        txn.clone(),
    )
    .await
    .expect("delete Service in a fresh namespace transaction");

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
    use tc_state::OpExecutor;

    const CHILD: pathlink::PathLabel =
        pathlink::path_label(&["class", "example-devco", "child", "1.0.0"]);
    const MISSING: pathlink::PathLabel =
        pathlink::path_label(&["class", "example-devco", "missing", "1.0.0"]);
    const CLASS_ROOT: pathlink::PathLabel = pathlink::path_label(&["class"]);

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

    let read = |target| {
        tc_ir::OpDef::Post(vec![(
            "result".parse().unwrap(),
            tc_ir::Scalar::from(tc_ir::TCRef::Op(tc_ir::OpRef::Get((
                tc_ir::Subject::Link(target),
                tc_ir::Scalar::default(),
            )))),
        )])
    };

    let mut prototype = tc_ir::Map::from_iter([(
        "label".parse().expect("member"),
        tc_ir::Scalar::Op(tc_ir::OpDef::Post(vec![(
            "result".parse().expect("result"),
            get_name,
        )])),
    )]);

    prototype.insert(
        "definition".parse().unwrap(),
        tc_ir::Scalar::Op(read(identity.clone())),
    );

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
            crate::State::from(definition),
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
        &label,
        crate::State::Scalar(scalar)
            if matches!(&scalar, tc_ir::Scalar::Value(value)
                if matches!(&value, tc_value::Value::String(value) if value == "Ada"))
    ));

    let child = pathlink::Link::from(CHILD);
    let install = bind(&kernel)
        .await
        .with_claims(vec![crate::Claim::new(child.clone(), umask::Mode::all())]);

    let body = tc_state::ClassBody::new(
        tc_state::ClassParent::Class(identity.clone()),
        tc_ir::Map::new(),
    );

    execute(
        &kernel,
        Method::Put,
        &CLASS_ROOT.to_string(),
        Some(crate::State::Tuple(vec![
            tc_value::Value::Link(child.clone()).into(),
            crate::State::from(body.definition()),
        ])),
        install.clone(),
    )
    .await
    .unwrap();

    complete(&kernel, install, crate::txn::TransactionOutcome::Commit).await;

    let txn = bind(&kernel).await;
    // Establish ordinary resource ownership before issuing dependency calls.
    execute(
        &kernel,
        Method::Get,
        &identity.to_string(),
        None,
        txn.clone(),
    )
    .await
    .unwrap();

    let args = || crate::State::Map(tc_ir::Map::new());

    // The instance's Class supplies the scope only when no declaring Class exists.
    txn.execute_op(read(identity.clone()), args(), Some(instance.clone()), None)
        .await
        .unwrap();

    let error = txn
        .execute_op(read(child.clone()), args(), Some(instance.clone()), None)
        .await
        .unwrap_err();

    assert_eq!(error.code(), tc_error::ErrorKind::Unauthorized);
    txn.execute_op(
        read(child.clone()),
        args(),
        Some(instance.clone()),
        Some(child.clone()),
    )
    .await
    .unwrap();

    let error = txn
        .execute_op(
            read(identity.clone()),
            args(),
            Some(instance),
            Some(MISSING.into()),
        )
        .await
        .unwrap_err();

    assert_eq!(error.code(), tc_error::ErrorKind::NotFound);

    // With neither a declaring Class nor an instance, retain the caller's scope.
    let scoped = txn.for_application(std::sync::Arc::new(crate::txn::DependencyScope::new(
        identity.clone(),
        Default::default(),
    )));

    scoped
        .execute_op(read(identity.clone()), args(), None, None)
        .await
        .unwrap();

    let error = scoped
        .execute_op(read(child.clone()), args(), None, None)
        .await
        .unwrap_err();

    assert_eq!(error.code(), tc_error::ErrorKind::Unauthorized);

    let inherited = execute(
        &kernel,
        Method::Post,
        &child.to_string(),
        Some(crate::State::Map(tc_ir::Map::from_iter([(
            "name".parse().unwrap(),
            crate::State::from(tc_value::Value::from("Ada")),
        )]))),
        txn.clone(),
    )
    .await
    .unwrap();

    let inherited_label = inherited
        .post(&txn, &["label".parse().unwrap()], tc_ir::Map::new())
        .await
        .unwrap();

    assert!(matches!(
        &inherited_label,
        crate::State::Scalar(scalar)
            if matches!(&scalar, tc_ir::Scalar::Value(value)
                if matches!(&value, tc_value::Value::String(value) if value == "Ada"))
    ));

    // The inherited method uses its declaring parent's permissions, not the child's.
    inherited
        .post(&txn, &["definition".parse().unwrap()], tc_ir::Map::new())
        .await
        .unwrap();

    let error = txn
        .execute_op(read(identity), args(), Some(inherited), None)
        .await
        .unwrap_err();

    assert_eq!(error.code(), tc_error::ErrorKind::Unauthorized);
}

#[tokio::test]
async fn binds_an_ownerless_transaction_for_native_execution() {
    let kernel = kernel("ownerless").await;
    let deadline = kernel.deadline();
    let request = kernel
        .begin_request(Method::Get, "/state/scalar/value/number/add", false, None)
        .await
        .expect("bind transaction");

    let upper_bound = kernel.deadline();
    assert!(request.txn().deadline().instant() >= deadline.instant());
    assert!(request.txn().deadline().instant() <= upper_bound.instant());
}

#[tokio::test]
async fn ttl_worker_finalizes_an_abandoned_transaction() {
    let kernel = setup_with_ttl("ttl", std::time::Duration::from_millis(10)).await;
    let txn = kernel.test_txn().await;
    let txn_id = txn.id();
    drop(txn);
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

    let error = kernel
        .coordinate(&txn, crate::txn::TransactionOutcome::Commit, false)
        .await
        .expect_err("the signed decision cannot change its outcome");

    assert_eq!(error.code(), tc_error::ErrorKind::Conflict);
    let error = kernel
        .test_classes()
        .claim(&txn)
        .await
        .map(|_| ())
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

    let install = service_txn(&kernel, &identity).await;
    stage_service(
        &kernel,
        &install,
        identity.clone(),
        executable_service(false),
    )
    .await;

    complete(&kernel, install, crate::txn::TransactionOutcome::Commit).await;

    let txn = service_txn(&kernel, &identity).await;
    execute(
        &kernel,
        Method::Put,
        &identity
            .clone()
            .append(pathlink::label("insert"))
            .to_string(),
        Some(crate::State::Tuple(vec![
            tc_value::Value::None.into(),
            tc_value::Value::Tuple(vec![7_u64.into()]).into(),
        ])),
        txn.clone(),
    )
    .await
    .unwrap();

    let crate::State::Collection(tc_collection::Collection::BTree(pending)) = execute(
        &kernel,
        Method::Get,
        &identity.clone().append(pathlink::label("data")).to_string(),
        None,
        txn.clone(),
    )
    .await
    .unwrap() else {
        panic!("expected a native BTree member");
    };

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
    let request = kernel
        .begin_request(
            Method::Put,
            &format!("{identity}/suffix?txn_id={}", txn.id()),
            true,
            Some(bearer.clone()),
        )
        .await
        .expect("bind suffix decision");

    let error = request
        .execute_bound(None)
        .await
        .map(|_| ())
        .expect_err("a suffix is not a resource decision target");

    assert_eq!(error.code(), tc_error::ErrorKind::Conflict);
    assert_eq!(
        kernel
            .inner
            .services
            .state()
            .items(txn.id())
            .await
            .unwrap()
            .len(),
        1,
        "a failed locked decision must retain the Service"
    );

    assert_eq!(pending.count(txn.id()).await.unwrap(), 1);
    let request = kernel
        .begin_request(
            Method::Put,
            &format!("/service/example-devco/other/1.0.0?txn_id={}", txn.id()),
            true,
            Some(bearer.clone()),
        )
        .await
        .expect("bind missing decision");

    let error = request
        .execute_bound(None)
        .await
        .map(|_| ())
        .expect_err("an unenlisted resource cannot be decided");

    assert_eq!(error.code(), tc_error::ErrorKind::Conflict);
    assert_eq!(
        kernel
            .inner
            .services
            .state()
            .items(txn.id())
            .await
            .unwrap()
            .len(),
        1,
        "an invalid decision target must leave the Service intact"
    );

    assert_eq!(pending.count(txn.id()).await.unwrap(), 1);
    let request = kernel
        .begin_request(
            Method::Put,
            &format!("{identity}?txn_id={}", txn.id()),
            true,
            Some(bearer.clone()),
        )
        .await
        .expect("bind resource decision");

    assert!(
        request
            .execute_bound(None)
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
            .execute_bound(None)
            .await
            .expect("duplicate decision")
            .is_none()
    );

    for resource in resources.iter().filter(|path| *path != identity.path()) {
        let request = kernel
            .begin_request(
                Method::Put,
                &format!("{resource}?txn_id={}", txn.id()),
                true,
                Some(bearer.clone()),
            )
            .await
            .expect("bind resource decision");

        assert!(
            request
                .execute_bound(None)
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

    assert_eq!(pending.count(bind(&kernel).await.id()).await.unwrap(), 1);
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

    let install = service_txn(&kernel, &identity).await;
    let definition = tc_ir::Scalar::Map(tc_ir::Map::new());
    stage_service(&kernel, &install, identity.clone(), definition).await;
    complete(&kernel, install, crate::txn::TransactionOutcome::Commit).await;

    let txn = service_txn(&kernel, &identity).await;
    kernel
        .test_services()
        .lookup(&txn, &identity.path()[1..])
        .await
        .unwrap()
        .exact_item()
        .unwrap()
        .unwrap()
        .claim(&txn)
        .await
        .unwrap();

    let bearer = kernel.test_txn_server().test_decision_bearer(&txn);
    let request = kernel
        .begin_request(
            Method::Put,
            &format!("{identity}?txn_id={}", txn.id()),
            true,
            Some(bearer),
        )
        .await
        .expect("bind decision");

    assert!(
        request
            .execute_bound(None)
            .await
            .expect("decision")
            .is_none()
    );

    let error = kernel
        .test_classes()
        .claim(&txn)
        .await
        .map(|_| ())
        .expect_err("locked work must fail");

    assert_eq!(error.code(), tc_error::ErrorKind::Conflict);
}

#[path = "request_protocol.rs"]
mod request_protocol;
#[path = "tensor_service.rs"]
mod tensor_service;

#[tokio::test]
async fn shutdown_waits_for_request_ownership_and_is_repeatable() {
    let kernel = kernel("request-drain").await;
    let request = kernel
        .begin_request(Method::Get, "/state/scalar/value/number", true, None)
        .await
        .unwrap();

    assert!(kernel.is_ready());
    kernel.begin_shutdown();
    assert!(!kernel.is_ready());
    assert!(
        matches!(kernel.begin_request(Method::Get, "/state", true, None).await,
        Err(error) if error.code() == tc_error::ErrorKind::Unavailable)
    );

    {
        let first = kernel.shutdown();
        tokio::pin!(first);
        assert!(futures::poll!(&mut first).is_pending());
    }

    drop(request);
    tokio::join!(kernel.shutdown(), kernel.shutdown());
    assert!(!kernel.is_ready());
}
