use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use pathlink::{Link, PathBuf, PathLabel, label, path_label};
use safecast::TryCastFrom;
use tc_error::{ErrorKind, TCError, TCResult};
use tc_ir::{IdRef, Map, OpDef, OpRef, Scalar, Subject, TCRef};
use tc_value::Value;
use tokio::sync::{mpsc, oneshot};

use crate::cluster::{AsyncHash, Cluster};
use crate::kernel::types::KernelTarget;
use crate::txn::TransactionOutcome;
use crate::{BoundRequest, Kernel, Method, State, TxnHandle};
use super::{
    bind, complete, executable_service, execute, open_at_with_gateway, service_txn, stage_service,
};

const A: PathLabel = path_label(&["service", "test", "a", "1.0.0"]);

const B: PathLabel = path_label(&["service", "test", "b", "1.0.0"]);

const C: PathLabel = path_label(&["service", "test", "c", "1.0.0"]);

const D: PathLabel = path_label(&["service", "test", "d", "1.0.0"]);

const APPEND: PathLabel = path_label(&["append"]);

const PURE: PathLabel = path_label(&["pure_post"]);

type PausedWrite = (TxnHandle, oneshot::Sender<()>);

#[derive(Default)]
struct Gateway {
    writes: Mutex<Vec<PathBuf>>,
    decisions: Mutex<Vec<PathBuf>>,
    pause: Mutex<Option<mpsc::Sender<PausedWrite>>>,
    fail: Mutex<Option<PathBuf>>,
}

#[async_trait]
impl crate::replication::ClusterGateway for Gateway {
    async fn put(
        &self,
        _peer: &str,
        _token: &str,
        txn: &TxnHandle,
        target: &Link,
        _key: Scalar,
        _value: State,
    ) -> TCResult<()> {
        self.writes.lock().unwrap().push(target.path().clone());
        let pause = self.pause.lock().unwrap().clone();
        if let Some(pause) = pause {
            let (release, resumed) = oneshot::channel();
            pause.send((txn.clone(), release)).await.unwrap();
            resumed.await.unwrap();
        }

        Ok(())
    }

    async fn delete(
        &self,
        _peer: &str,
        _token: &str,
        _txn: &TxnHandle,
        target: &Link,
        _key: Scalar,
    ) -> TCResult<()> {
        self.writes.lock().unwrap().push(target.path().clone());
        Ok(())
    }

    async fn decide_resource(
        &self,
        _peer: &str,
        _token: &str,
        _txn: &TxnHandle,
        resource: &PathBuf,
        _commit: bool,
    ) -> TCResult<()> {
        self.decisions.lock().unwrap().push(resource.clone());
        let mut fail = self.fail.lock().unwrap();
        if fail.as_ref() == Some(resource) {
            fail.take();
            Err(TCError::bad_gateway("injected decision failure"))
        } else {
            Ok(())
        }
    }
}

async fn install(
    kernel: &Kernel,
    identity: Link,
    definition: Scalar,
) -> Cluster<crate::service::Service> {
    let txn = service_txn(kernel, &identity).await;
    stage_service(kernel, &txn, identity.clone(), definition).await;
    complete(kernel, txn, TransactionOutcome::Commit).await;

    let join = service_txn(kernel, &identity).await.with_claims(vec![
        crate::Claim::new(identity.clone(), umask::Mode::all()),
        crate::Claim::new(
            identity.clone().append(label("replicas")),
            umask::USER_WRITE,
        ),
    ]);

    let cluster = kernel
        .test_services()
        .lookup(&join, &identity.path()[1..])
        .await
        .unwrap()
        .exact_item()
        .unwrap()
        .unwrap();

    let replica = kernel
        .inner
        .bootstrap
        .self_identity("http://replica.invalid".into())
        .unwrap();

    let hash = cluster.state().hash(&join).await.unwrap();
    execute(
        kernel,
        Method::Put,
        &identity
            .path()
            .clone()
            .append(label("replicas"))
            .to_string(),
        Some(State::Tuple(vec![
            Value::from(replica.endpoint.clone()).into(),
            crate::cluster::replica_put_state(&replica, hex::encode(hash)),
        ])),
        join.clone(),
    )
    .await
    .unwrap();

    complete(kernel, join, TransactionOutcome::Commit).await;

    cluster
}

async fn request(kernel: &Kernel, txn: &TxnHandle, method: Method, target: Link) -> BoundRequest {
    let permit = kernel
        .txn_server
        .resources()
        .admit_request(txn.deadline())
        .await
        .unwrap();

    BoundRequest::new(
        kernel.clone(),
        method,
        KernelTarget::Application(target),
        txn.clone(),
        permit,
        kernel.drain.admit().unwrap(),
    )
}

#[test]
fn local_mutation_precedes_replica_work_and_invalid_work_is_not_forwarded() {
    crate::test_runtime::run(|| async {
        let gateway = Arc::new(Gateway::default());
        let kernel = open_at_with_gateway(
            crate::txn::test_path("local-first"),
            Duration::from_secs(60),
            false,
            gateway.clone(),
        )
        .await
        .unwrap();

        let identity = Link::from(A);
        install(&kernel, identity.clone(), executable_service(false)).await;
        gateway.writes.lock().unwrap().clear();
        gateway.decisions.lock().unwrap().clear();

        let invalid = service_txn(&kernel, &identity).await;
        let target = identity.clone().append(label("insert"));
        let bound = request(&kernel, &invalid, Method::Put, target.clone()).await;
        let error = bound
            .execute_bound(Some(State::Tuple(vec![
                Value::from(1_u64).into(),
                Value::Tuple(vec![7_u64.into()]).into(),
            ])))
            .await
            .map(|_| ())
            .expect_err("malformed mutation");

        assert_eq!(error.code(), ErrorKind::BadRequest);
        assert!(gateway.writes.lock().unwrap().is_empty());

        let txn = service_txn(&kernel, &identity).await;
        let bound = request(&kernel, &txn, Method::Put, target).await;
        let (pause, mut paused) = mpsc::channel(1);
        *gateway.pause.lock().unwrap() = Some(pause);
        {
            let write = bound.execute_bound(Some(State::Tuple(vec![
                Value::None.into(),
                Value::Tuple(vec![7_u64.into()]).into(),
            ])));

            let mut write = std::pin::pin!(write);
            let (forwarded, release) = tokio::time::timeout(Duration::from_secs(3), async {
                tokio::select! {
                    paused = paused.recv() => paused.unwrap(),
                    result = &mut write => panic!("write returned before its fanout: {:?}", result.map(|response| response.map(|(state, _)| state))),
                }
            })
            .await
            .expect("write must reach replica fanout");

            let count = execute(
                &kernel,
                Method::Get,
                &identity.path().clone().append(label("count")).to_string(),
                None,
                forwarded,
            )
            .await
            .unwrap();

            assert_eq!(Value::opt_cast_from(count).unwrap(), Value::from(1_u64));
            assert!(!txn.is_locked());
            release.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(3), write)
                .await
                .expect("write completion")
                .unwrap();
        }

        assert!(txn.is_locked(), "commit precedes result projection");
        assert_eq!(gateway.writes.lock().unwrap().len(), 1);
        kernel.shutdown().await;
    });
}

#[test]
fn cyclic_and_shared_dependencies_keep_complete_retry_obligations() {
    crate::test_runtime::run(|| async {
        let gateway = Arc::new(Gateway::default());
        let kernel = open_at_with_gateway(
            crate::txn::test_path("decision-topology"),
            Duration::from_secs(60),
            false,
            gateway.clone(),
        )
        .await
        .unwrap();

        let mut clusters = BTreeMap::new();
        for identity in [A, B, C, D] {
            let identity = Link::from(identity);
            let cluster = install(&kernel, identity.clone(), Scalar::Map(Map::new())).await;
            clusters.insert(identity, cluster);
        }

        for (edges, expected) in [
            (vec![(A, B), (B, A)], vec![(A, 1), (B, 1)]),
            (
                vec![(A, B), (A, C), (B, D), (C, D)],
                vec![(A, 1), (B, 1), (C, 1), (D, 2)],
            ),
        ] {
            let txn = bind(&kernel).await;
            // Only the outer call claims the coordinator. Dependent resources
            // establish their own leadership on independent signed branches.
            let (_, call) = clusters[&Link::from(A)].enter(&txn).await.unwrap();
            drop(call);
            for identity in [B, C, D] {
                let (_, call) = clusters[&Link::from(identity)]
                    .enter(&txn.branch())
                    .await
                    .unwrap();

                drop(call);
            }

            for (source, target) in &edges {
                let (branch, call) = clusters[&Link::from(*source)]
                    .enter(&txn.branch())
                    .await
                    .unwrap();

                branch.record_dependency(Link::from(*target)).unwrap();
                drop(call);
            }

            txn.lock_coordinator(TransactionOutcome::Commit, false)
                .unwrap();

            let root = &clusters[&Link::from(A)];
            gateway.decisions.lock().unwrap().clear();
            *gateway.fail.lock().unwrap() = Some(PathBuf::from(B));
            let error = tokio::time::timeout(
                Duration::from_secs(3),
                root.decide(&txn, TransactionOutcome::Commit),
            )
            .await
            .expect("cyclic decisions must terminate")
            .unwrap_err();

            assert_eq!(error.code(), ErrorKind::BadGateway);
            eprintln!(
                "decision deliveries before injected failure: {:?}",
                gateway.decisions.lock().unwrap()
            );

            gateway.decisions.lock().unwrap().clear();
            tokio::time::timeout(
                Duration::from_secs(3),
                root.decide(&txn, TransactionOutcome::Commit),
            )
            .await
            .expect("retry must terminate")
            .unwrap();

            let mut actual = BTreeMap::new();
            for resource in gateway.decisions.lock().unwrap().iter() {
                *actual.entry(resource.clone()).or_insert(0) += 1;
            }

            let expected = expected
                .into_iter()
                .map(|(path, count)| (PathBuf::from(path), count))
                .collect();

            eprintln!("decision deliveries after retry: {actual:?}");
            assert_eq!(actual, expected, "retry must retain every dependency edge");
        }

        kernel.shutdown().await;
    });
}

#[test]
fn decisions_reject_active_resource_work_before_delivery() {
    crate::test_runtime::run(|| async {
        let gateway = Arc::new(Gateway::default());
        let kernel = open_at_with_gateway(
            crate::txn::test_path("active-resource-decision"),
            Duration::from_secs(60),
            false,
            gateway.clone(),
        )
        .await
        .unwrap();

        let cluster = install(&kernel, A.into(), Scalar::Map(Map::new())).await;
        gateway.decisions.lock().unwrap().clear();

        let txn = bind(&kernel).await;
        let (_, call) = cluster.enter(&txn).await.unwrap();
        txn.lock_coordinator(TransactionOutcome::Commit, false)
            .unwrap();

        let error = cluster
            .decide(&txn, TransactionOutcome::Commit)
            .await
            .unwrap_err();

        assert_eq!(error.code(), ErrorKind::Conflict);
        assert!(gateway.decisions.lock().unwrap().is_empty());

        drop(call);
        cluster
            .decide(&txn, TransactionOutcome::Commit)
            .await
            .unwrap();

        assert_eq!(*gateway.decisions.lock().unwrap(), [PathBuf::from(A)]);
        kernel.shutdown().await;
    });
}

#[test]
fn newly_claimed_leadership_does_not_authorize_a_participant_install() {
    crate::test_runtime::run(|| async {
        let gateway = Arc::new(Gateway::default());
        let kernel = open_at_with_gateway(
            crate::txn::test_path("participant-install-authority"),
            Duration::from_secs(60),
            false,
            gateway.clone(),
        )
        .await
        .unwrap();

        let owner = bind(&kernel).await;
        kernel.test_services().claim(&owner).await.unwrap();
        let participant = owner.branch();
        let bound = request(
            &kernel,
            &participant,
            Method::Put,
            super::SERVICE_ROOT.into(),
        )
        .await;

        let error = bound
            .execute_bound(Some(State::Tuple(vec![
                Value::Link(A.into()).into(),
                Scalar::Map(Map::new()).into(),
            ])))
            .await
            .map(|_| ())
            .expect_err("participant cannot install");

        assert_eq!(error.code(), ErrorKind::Unauthorized);
        assert!(!owner.is_locked(), "participants do not select decisions");
        assert!(
            kernel
                .test_services()
                .state()
                .items(owner.id())
                .await
                .unwrap()
                .is_empty()
        );

        assert!(gateway.writes.lock().unwrap().is_empty());
        assert!(gateway.decisions.lock().unwrap().is_empty());

        complete(&kernel, owner, TransactionOutcome::Rollback).await;
        kernel.shutdown().await;
    });
}

#[test]
fn forged_dependency_ancestry_cannot_skip_its_owning_resource() {
    crate::test_runtime::run(|| async {
        let gateway = Arc::new(Gateway::default());
        let kernel = open_at_with_gateway(
            crate::txn::test_path("forged-decision-ancestor"),
            Duration::from_secs(60),
            false,
            gateway.clone(),
        )
        .await
        .unwrap();

        let a = install(&kernel, A.into(), Scalar::Map(Map::new())).await;
        let b = install(&kernel, B.into(), Scalar::Map(Map::new())).await;
        gateway.decisions.lock().unwrap().clear();

        let txn = bind(&kernel).await;
        let (source, call) = a.enter(&txn).await.unwrap();
        source.record_dependency(B.into()).unwrap();
        drop(call);
        let (_, call) = b.enter(&txn.branch()).await.unwrap();
        drop(call);
        txn.lock_coordinator(TransactionOutcome::Commit, false)
            .unwrap();

        // B's leadership was established on its own branch. The root token does
        // not carry it, so an unrelated signer can claim B only as an ancestor;
        // B must authenticate that claim against its retained resource owner.
        let authority = kernel.txn_server.protocol_authority();
        let impostor = crate::ProtocolAuthority::new(
            authority.host().clone(),
            crate::auth::Actor::new("unrelated-ancestor".into(), rjwt::AlgKind::Falcon512).unwrap(),
        );

        let context = kernel
            .txn_server
            .verifier()
            .verify(txn.raw_token().unwrap())
            .await
            .unwrap();

        let signed = impostor
            .extend(
                (**context.signed().unwrap()).clone(),
                BTreeMap::from([(PathBuf::from(B), u32::from(umask::USER_WRITE))]),
                std::time::SystemTime::now(),
            )
            .unwrap();

        let context = context.with_signed(signed);
        let forged = kernel
            .txn_server
            .bind(Some(txn.id()), Some(&context), kernel.inner.clone())
            .await
            .unwrap();

        let error = a
            .decide(&forged, TransactionOutcome::Commit)
            .await
            .unwrap_err();

        assert_eq!(error.code(), ErrorKind::Unauthorized);
        assert!(gateway.decisions.lock().unwrap().is_empty());

        // Rejection neither applies the parent nor erases its obligation to B.
        a.decide(&txn, TransactionOutcome::Commit).await.unwrap();
        assert_eq!(
            *gateway.decisions.lock().unwrap(),
            [PathBuf::from(B), PathBuf::from(A)]
        );

        kernel.shutdown().await;
    });
}

#[test]
fn get_allows_pure_post_but_rejects_its_nested_write() {
    crate::test_runtime::run(|| async {
        let gateway = Arc::new(Gateway::default());
        let kernel = open_at_with_gateway(
            crate::txn::test_path("get-post-effects"),
            Duration::from_secs(60),
            false,
            gateway.clone(),
        )
        .await
        .unwrap();

        let identity = Link::from(A);
        let Scalar::Map(mut definition) = executable_service(false) else {
            unreachable!()
        };

        definition.insert(
            "pure_post".parse().unwrap(),
            Scalar::Op(OpDef::Post(vec![(
                "result".parse().unwrap(),
                Value::from(7_u64).into(),
            )])),
        );

        for (method, path, args) in [
            ("read_pure", PURE, Map::new()),
            (
                "read_write",
                APPEND,
                Map::from_iter([
                    ("key".parse().unwrap(), Value::None.into()),
                    (
                        "value".parse().unwrap(),
                        Value::Tuple(vec![7_u64.into()]).into(),
                    ),
                ]),
            ),
        ] {
            definition.insert(
                method.parse().unwrap(),
                Scalar::Op(OpDef::Get((
                    "key".parse().unwrap(),
                    vec![(
                        "result".parse().unwrap(),
                        TCRef::Op(OpRef::Post((
                            Subject::Ref(IdRef::new("self".parse().unwrap()), path.into()),
                            args,
                        )))
                        .into(),
                    )],
                ))),
            );
        }

        install(&kernel, identity.clone(), Scalar::Map(definition)).await;
        gateway.writes.lock().unwrap().clear();
        gateway.decisions.lock().unwrap().clear();
        for (member, writes) in [("read_pure", false), ("read_write", true)] {
            let txn = service_txn(&kernel, &identity).await;
            let bound = request(
                &kernel,
                &txn,
                Method::Get,
                identity.clone().append(label(member)),
            )
            .await;

            let result = bound.execute_bound(None).await;
            if writes {
                let error = result.map(|_| ()).expect_err("GET cannot mutate");
                assert_eq!(error.code(), ErrorKind::Unauthorized);
                assert!(error.message().contains("GET execution cannot mutate"));
            } else {
                assert_eq!(
                    Value::opt_cast_from(result.unwrap().unwrap().0).unwrap(),
                    Value::from(7_u64)
                );
            }

            assert!(
                !txn.is_locked(),
                "GET must not select an automatic decision"
            );

            assert!(gateway.writes.lock().unwrap().is_empty());
            complete(&kernel, txn, TransactionOutcome::Rollback).await;
        }

        // Pure POST still selects its owner's decision before its value is projected.
        let txn = service_txn(&kernel, &identity).await;
        let bound = request(
            &kernel,
            &txn,
            Method::Post,
            identity.append(label("pure_post")),
        )
        .await;

        let result = bound
            .execute_bound(Some(State::Map(Map::new())))
            .await
            .unwrap()
            .unwrap();

        assert!(txn.is_locked());
        assert_eq!(Value::opt_cast_from(result.0).unwrap(), Value::from(7_u64));
        drop(result.1);
        kernel.shutdown().await;
    });
}
