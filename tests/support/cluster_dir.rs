use std::sync::atomic::{AtomicUsize, Ordering};

use base64::Engine as _;
use tc_ir::{Scalar, Transact, TxnId};
use txn_lock::map::Entry;

use crate::replication::Replica;
use super::{Cluster, Resolved};

#[tokio::test]
async fn root_lifecycle_delegates_to_nested_cluster_replica_state() {
    let kernel = crate::txn::test_kernel("nested-cluster-lifecycle").await;
    let identity: pathlink::Link = "/service/example/nested/resource/1.0.0"
        .parse()
        .expect("Service identity");

    let segments = crate::uri::validate_identity(&identity, "service").expect("identity");
    let path = identity.path()[1..].to_vec();
    let definition = Scalar::Map(tc_ir::Map::new());
    let create_txn = kernel.test_txn().await;
    let create_txn_id = create_txn.id();
    kernel
        .test_services()
        .create_item(&create_txn, &segments, move |storage| {
            crate::service::Service::create(create_txn_id, storage, identity, definition, 8)
        })
        .await
        .expect("create nested Service");

    kernel
        .test_services()
        .commit(create_txn_id)
        .await
        .expect("commit nested tree");

    let txn = kernel.test_txn().await;
    let Resolved::Item { ancestors, .. } = kernel
        .test_services()
        .lookup(&txn, &path)
        .await
        .expect("resolve nested Service")
    else {
        panic!("expected nested Service item");
    };

    let parent = &ancestors.last().expect("parent directory").0;
    let replica = Replica {
        endpoint: "http://replica.example".into(),
        host: "/host".into(),
        actor_id: "replica".into(),
        algorithm: rjwt::AlgKind::Falcon512,
        public_key_b64: "key".into(),
    };

    insert_replica(parent, txn.id(), replica.clone()).await;
    kernel
        .test_services()
        .commit(txn.id())
        .await
        .expect("recursive commit");

    assert!(has_replica(parent, txn.id(), &replica.endpoint).await);
    let rollback_txn = kernel.test_txn().await;
    parent
        .replicas
        .remove(rollback_txn.id(), &replica.endpoint)
        .await
        .expect("stage replica removal");

    kernel
        .test_services()
        .rollback(&rollback_txn.id())
        .await
        .expect("recursive rollback");

    assert!(has_replica(parent, rollback_txn.id(), &replica.endpoint).await);
    let pending_txn = kernel.test_txn().await;
    let pending = Replica {
        endpoint: "http://pending.example".into(),
        ..replica
    };

    insert_replica(parent, pending_txn.id(), pending.clone()).await;
    kernel
        .test_services()
        .finalize(&pending_txn.id())
        .await
        .expect("recursive finalization");

    assert!(!has_replica(parent, pending_txn.id(), &pending.endpoint).await);
}

async fn has_replica<T>(cluster: &Cluster<T>, txn_id: TxnId, endpoint: &str) -> bool {
    cluster
        .replica_snapshot(txn_id)
        .await
        .unwrap()
        .iter()
        .any(|known| known.endpoint == endpoint)
}

async fn insert_replica<T>(cluster: &Cluster<T>, txn_id: TxnId, replica: Replica) {
    let entry = cluster
        .replicas
        .entry(txn_id, replica.endpoint.clone())
        .await
        .unwrap();

    let Entry::Vacant(entry) = entry else {
        panic!("new replica already exists")
    };

    entry.insert(replica).unwrap();
}

async fn service_fixture(label: &str) -> (crate::Kernel, pathlink::Link) {
    let kernel = crate::txn::test_kernel(label).await;
    let root = kernel.test_services();
    let mut path = root.path().clone();
    for segment in ["example", "nested", "resource", "1.0.0"] {
        path = path.append(
            segment
                .parse::<pathlink::PathSegment>()
                .expect("identity segment"),
        );
    }

    let identity = pathlink::Link::from(path);
    let segments = crate::uri::validate_identity(&identity, "service").expect("identity");
    let txn = kernel.test_txn().await;
    let txn_id = txn.id();
    let created_identity = identity.clone();
    root.clone()
        .create_item(&txn, &segments, move |storage| {
            crate::service::Service::create(
                txn_id,
                storage,
                created_identity,
                Scalar::Map(tc_ir::Map::new()),
                8,
            )
        })
        .await
        .expect("create Service");

    root.commit(txn_id).await.expect("commit fixture");
    (kernel, identity)
}

#[tokio::test]
async fn immutable_install_skips_preparation_and_failed_creation_is_not_published() {
    let (kernel, identity) = service_fixture("immutable-install-preparation").await;
    let root = kernel.test_services();
    let segments = crate::uri::validate_identity(&identity, "service").unwrap();
    let preparations = AtomicUsize::new(0);
    for conflict in [false, true] {
        let txn = kernel.test_txn().await;
        let result = root
            .clone()
            .create_item_if_absent(
                &txn,
                &identity.path()[1..],
                &segments,
                conflict,
                |_, conflict| {
                    if *conflict {
                        Err(tc_error::TCError::conflict("fixture conflict"))
                    } else {
                        Ok(())
                    }
                },
                |_| {
                    preparations.fetch_add(1, Ordering::Relaxed);
                    ((Scalar::default(), crate::State::None), |_| {
                        std::future::ready(Err::<crate::service::Service, _>(
                            tc_error::TCError::internal("unexpected creation"),
                        ))
                    })
                },
            )
            .await;

        if conflict {
            assert_eq!(result.unwrap_err().code(), tc_error::ErrorKind::Conflict);
        } else {
            result.unwrap();
        }

        assert_eq!(preparations.load(Ordering::Relaxed), 0);
        root.rollback(&txn.id()).await.unwrap();
    }

    let txn = kernel.test_txn().await;
    let key = root.protocol.verifying_key();
    let replica = Replica {
        endpoint: "http://replica.invalid".into(),
        host: root.protocol.host().to_string(),
        actor_id: root.protocol.actor_id().to_string(),
        algorithm: key.alg(),
        public_key_b64: base64::engine::general_purpose::STANDARD.encode(key.to_bytes()),
    };

    insert_replica(&root, txn.id(), replica).await;
    root.claim(&txn).await.unwrap();
    assert_eq!(root.replication_peers(&txn).await.unwrap().len(), 1);
    let failed = pathlink::Link::from(pathlink::path_label(&[
        "service", "other", "failed", "1.0.0",
    ]));

    let segments = crate::uri::validate_identity(&failed, "service").unwrap();
    let error = root
        .clone()
        .create_item_if_absent(
            &txn,
            &failed.path()[1..],
            &segments,
            (),
            |_, _| Ok(()),
            |_| {
                ((Scalar::default(), crate::State::None), |_| {
                    std::future::ready(Err::<crate::service::Service, _>(
                        tc_error::TCError::bad_request("fixture creation failure"),
                    ))
                })
            },
        )
        .await
        .unwrap_err();

    assert_eq!(error.code(), tc_error::ErrorKind::BadRequest);
    assert!(
        matches!(root.lookup(&txn, &failed.path()[1..]).await.unwrap(),
        Resolved::Dir { unmatched, .. } if !unmatched.is_empty())
    );

    // No child owner was constructed to delegate rollback to. Discard this
    // unpublished owner after inspecting the failed construction.
    drop(txn);
    kernel.shutdown().await;
}

#[tokio::test]
async fn ordinary_lookup_claims_only_the_selected_cluster() {
    use pathlink::{PathBuf, path_label};

    let (kernel, identity) = service_fixture("selected-cluster-ownership").await;
    let root = kernel.test_services();
    let txn = kernel.test_txn().await;

    for (case, path, consumed) in [
        ("root", path_label(&[]), 0),
        ("directory", path_label(&["example", "nested"]), 2),
        (
            "missing tail",
            path_label(&["example", "nested", "missing", "child"]),
            2,
        ),
        (
            "root replicas",
            path_label(&[crate::cluster::REPLICAS, "peer"]),
            0,
        ),
        (
            "directory replicas",
            path_label(&["example", "nested", crate::cluster::REPLICAS, "peer"]),
            2,
        ),
    ] {
        let path = PathBuf::from(path);
        let Resolved::Dir { cluster, unmatched } = root
            .clone()
            .lookup(&txn, &path)
            .await
            .expect("resolve directory")
        else {
            panic!("{case}: expected a directory");
        };

        assert_eq!(
            &cluster.path()[..],
            &identity.path()[..consumed + 1],
            "{case}: wrong directory",
        );

        assert_eq!(&*unmatched, &path[consumed..], "{case}: wrong suffix");
    }

    let Resolved::Item {
        cluster,
        suffix,
        ancestors,
    } = root
        .clone()
        .lookup(&txn, &identity.path()[1..])
        .await
        .expect("resolve Service")
    else {
        panic!("expected Service");
    };

    assert_eq!(cluster.path(), identity.path());
    assert!(suffix.is_empty());
    assert_eq!(ancestors.len(), identity.path().len() - 1);
    for (index, (parent, name)) in ancestors.iter().enumerate() {
        assert_eq!(&parent.path()[..], &identity.path()[..index + 1]);
        assert_eq!(name, &identity.path()[index + 1]);
    }

    let target = identity
        .clone()
        .append(pathlink::label(crate::cluster::REPLICAS))
        .append(pathlink::label("peer"));

    let Resolved::Item {
        cluster: selected,
        suffix,
        ancestors: selected_ancestors,
    } = root
        .lookup(&txn, &target.path()[1..])
        .await
        .expect("resolve Service suffix")
    else {
        panic!("expected Service with a suffix");
    };

    assert_eq!(selected.path(), identity.path());
    assert_eq!(&*suffix, &target.path()[identity.path().len()..]);
    assert_eq!(selected_ancestors.len(), ancestors.len());
    for ((parent, name), (expected_parent, expected_name)) in
        selected_ancestors.iter().zip(&ancestors)
    {
        assert_eq!(parent.path(), expected_parent.path());
        assert_eq!(name, expected_name);
    }

    assert!(
        txn.claimed_paths().is_empty(),
        "lookup must not claim directories"
    );

    let (first, second) = tokio::join!(cluster.claim(&txn), cluster.claim(&txn));
    let first = first.unwrap();
    let second = second.unwrap();
    assert!(std::sync::Arc::ptr_eq(&first, &second));
    assert!(std::sync::Arc::ptr_eq(
        &first,
        &cluster.claim(&txn).await.unwrap()
    ));

    let (selected, call) = cluster.enter(&txn).await.expect("enter selected Service");
    assert_eq!(
        selected.claimed_paths(),
        std::collections::BTreeSet::from([identity.path().clone()])
    );

    assert!(
        ancestors
            .iter()
            .all(|(directory, _)| { selected.leader(directory.path()).is_none() })
    );

    drop(call);
    kernel.shutdown().await;
}

#[tokio::test]
async fn namespace_structure_rejects_enlisted_descendants() {
    let (kernel, identity) = service_fixture("structural-owner-exclusion").await;
    let root = kernel.test_services();
    let txn = kernel.test_txn().await;
    let cluster = root
        .clone()
        .lookup(&txn, &identity.path()[1..])
        .await
        .expect("resolve Service")
        .exact_item()
        .expect("item")
        .expect("Service exists");

    let (_, call) = cluster.enter(&txn).await.expect("enlist Service");
    let error = root
        .check_structure(txn.id(), &identity.path()[1..])
        .await
        .expect_err("a structural decision cannot cover a separately enlisted owner");

    assert!(error.to_string().contains("enlisted"));
    drop(call);

    let txn = kernel.test_txn().await;
    root.check_structure(txn.id(), &identity.path()[1..])
        .await
        .expect("reserve structural ownership");

    assert!(
        cluster.enter(&txn).await.is_err(),
        "enrollment cannot race past the reservation"
    );

    assert!(
        root.clone()
            .lookup(&txn, &identity.path()[1..])
            .await
            .expect("lookup after preflight")
            .exact_item()
            .expect("item")
            .is_some()
    );

    kernel.shutdown().await;
}

#[tokio::test]
async fn namespace_structure_compares_complete_replica_identities() {
    let (kernel, identity) = service_fixture("structural-replica-preflight").await;
    let root = kernel.test_services();
    let txn = kernel.test_txn().await;
    let cluster = root
        .clone()
        .lookup(&txn, &identity.path()[1..])
        .await
        .expect("resolve Service")
        .exact_item()
        .expect("item")
        .expect("Service exists");

    let key = root.protocol.verifying_key();
    let duplicate_self = Replica {
        endpoint: "http://alias.example".into(),
        host: root.protocol.host().to_string(),
        actor_id: root.protocol.actor_id().to_string(),
        algorithm: key.alg(),
        public_key_b64: base64::engine::general_purpose::STANDARD.encode(key.to_bytes()),
    };

    insert_replica(&cluster, txn.id(), duplicate_self).await;
    assert_eq!(
        root.replica_identities(txn.id()).await.unwrap(),
        cluster.replica_identities(txn.id()).await.unwrap()
    );

    root.check_structure(txn.id(), &identity.path()[1..])
        .await
        .expect("endpoint aliases do not change replica identity");

    root.rollback(&txn.id()).await.expect("discard alias");

    let txn = kernel.test_txn().await;
    insert_replica(
        &cluster,
        txn.id(),
        Replica {
            endpoint: "http://other.example".into(),
            host: root.protocol.host().to_string(),
            actor_id: "another-replica".into(),
            algorithm: key.alg(),
            public_key_b64: base64::engine::general_purpose::STANDARD.encode(key.to_bytes()),
        },
    )
    .await;

    let error = root
        .check_structure(txn.id(), &identity.path()[1..])
        .await
        .expect_err("different replica membership must reject before deletion");

    assert!(
        error
            .to_string()
            .contains("matching namespace replica sets")
    );

    assert!(
        root.clone()
            .lookup(&txn, &identity.path()[1..])
            .await
            .expect("lookup after rejection")
            .exact_item()
            .expect("item")
            .is_some()
    );

    root.rollback(&txn.id())
        .await
        .expect("discard unmatched replica");

    kernel.shutdown().await;
}

#[tokio::test]
async fn whole_deletion_retains_the_namespace_decision_owner() {
    let (kernel, identity) = service_fixture("namespace-deletion-owner").await;
    let root = kernel.test_services();
    let txn = kernel
        .test_txn()
        .await
        .with_claims(vec![crate::Claim::new(identity.clone(), umask::USER_WRITE)]);

    root.clone()
        .dispatch(
            &txn,
            &identity,
            tc_ir::Method::Delete,
            Some(crate::State::None),
            crate::service::Root(&root),
        )
        .await
        .expect("delete Service through namespace owner");

    assert_eq!(
        txn.claimed_paths(),
        std::collections::BTreeSet::from([root.path().clone()])
    );

    assert!(
        root.clone()
            .lookup(&txn, &identity.path()[1..])
            .await
            .expect("resolve deleted path")
            .exact_item()
            .expect("item lookup")
            .is_none()
    );

    let Resolved::Dir { unmatched, .. } = root
        .clone()
        .lookup(&txn, &[])
        .await
        .expect("resolve stable coordinator")
    else {
        panic!("namespace coordinator must remain addressable");
    };

    assert!(unmatched.is_empty());
    assert!(
        root.state()
            .entries(txn.id())
            .await
            .expect("read namespace after deletion")
            .is_empty(),
        "deleting the only item must remove its empty ancestor directories"
    );

    root.rollback(&txn.id())
        .await
        .expect("rollback namespace deletion");

    drop(txn);
    let read = kernel.test_txn().await;
    assert!(
        root.clone()
            .lookup(&read, &identity.path()[1..])
            .await
            .expect("resolve restored Service")
            .exact_item()
            .expect("item lookup")
            .is_some()
    );

    kernel.shutdown().await;
}

#[tokio::test]
async fn claiming_a_cluster_does_not_authorize_membership_mutation() {
    use tc_ir::Route;

    let kernel = crate::txn::test_kernel("membership-authorization").await;
    let root = kernel.test_services();
    let original = kernel.test_txn().await;
    let (txn, call) = root.enter(&original).await.expect("claim local cluster");
    let path = [crate::cluster::REPLICAS
        .parse::<pathlink::PathSegment>()
        .expect("replicas segment")];

    let put = root
        .route(&path)
        .expect("replica route")
        .put()
        .expect("replica PUT");

    let error = put(&txn, Scalar::default(), crate::State::None)
        .await
        .expect_err("new leadership cannot grant user write permission");

    assert_eq!(error.code(), tc_error::ErrorKind::Unauthorized);

    let delete = root
        .route(&path)
        .expect("replica route")
        .delete()
        .expect("replica DELETE");

    let error = delete(&txn, Scalar::from(tc_value::Value::from("peer")))
        .await
        .expect_err("membership deletion also requires authorization");

    assert_eq!(error.code(), tc_error::ErrorKind::Unauthorized);
    assert!(root.replica_snapshot(txn.id()).await.unwrap().is_empty());
    drop(call);
    kernel.shutdown().await;
}

#[tokio::test]
async fn untouched_replica_decisions_require_exact_leadership_and_seal_admission() {
    use std::collections::BTreeMap;
    use std::time::{Duration, SystemTime};

    use crate::auth::{Actor, AuthContext, Token};
    use crate::txn::TransactionOutcome;

    let (kernel, identity) = service_fixture("untouched-replica-decision").await;
    let setup = kernel.test_txn().await;
    let cluster = kernel
        .test_services()
        .lookup(&setup, &identity.path()[1..])
        .await
        .unwrap()
        .exact_item()
        .unwrap()
        .unwrap();

    let leader = Actor::new("replica-leader".into(), rjwt::AlgKind::Falcon512).unwrap();
    let host = pathlink::Link::from(pathlink::path_label(&["peer"]));
    let key = leader.verifying_key();
    insert_replica(
        &cluster,
        setup.id(),
        Replica {
            endpoint: "http://peer.example".into(),
            host: host.to_string(),
            actor_id: leader.id().clone(),
            algorithm: key.alg(),
            public_key_b64: base64::engine::general_purpose::STANDARD.encode(key.to_bytes()),
        },
    )
    .await;

    cluster.commit(setup.id()).await.unwrap();

    let sign = |actor: &Actor, id: TxnId, resource: pathlink::PathBuf| {
        let txn = crate::uri::transaction_path(id)
            .parse::<pathlink::PathBuf>()
            .unwrap();

        let now = SystemTime::now();
        let unlocked = actor
            .sign_token(
                Token::new(
                    host.clone(),
                    now,
                    Duration::from_secs(30),
                    actor.id().clone(),
                    BTreeMap::from([
                        (txn.clone(), u32::from(umask::USER_EXEC)),
                        (resource, u32::from(umask::USER_EXEC)),
                    ]),
                )
                .expect("valid token time"),
            )
            .unwrap();

        let locked = actor
            .consume_and_sign(
                unlocked.clone(),
                host.clone(),
                BTreeMap::from([(txn, u32::from(umask::USER_WRITE))]),
                now,
            )
            .unwrap();

        (unlocked, locked)
    };

    let id = kernel.test_txn().await.id();
    let (unlocked, locked) = sign(&leader, id, identity.path().clone());
    let decision = kernel
        .test_bind(
            Some(id),
            Some(&AuthContext::new(leader.id().clone()).with_signed(locked)),
        )
        .await
        .unwrap();

    assert!(!cluster.participation.lock().contains_key(&id));
    cluster
        .decide(&decision, TransactionOutcome::Commit)
        .await
        .unwrap();

    let participant = cluster
        .participation
        .lock()
        .get(&id)
        .unwrap()
        .as_ref()
        .unwrap()
        .clone();

    assert_eq!(participant.owner, (host.to_string(), leader.id().clone()));
    assert_eq!(participant.leader, (host.to_string(), leader.id().clone()));

    let late = kernel
        .test_bind(
            Some(id),
            Some(&AuthContext::new(leader.id().clone()).with_signed(unlocked)),
        )
        .await
        .unwrap();

    assert_eq!(
        cluster.enter(&late).await.unwrap_err().code(),
        tc_error::ErrorKind::Conflict
    );

    assert_eq!(
        participant.enlist(identity.clone(), 1).unwrap_err().code(),
        tc_error::ErrorKind::Conflict
    );

    cluster
        .decide(&decision, TransactionOutcome::Commit)
        .await
        .unwrap();

    assert!(std::sync::Arc::ptr_eq(
        &participant,
        cluster
            .participation
            .lock()
            .get(&id)
            .unwrap()
            .as_ref()
            .unwrap(),
    ));

    let forged = Actor::new("forged-leader".into(), rjwt::AlgKind::Falcon512).unwrap();
    let other = pathlink::PathBuf::from(pathlink::path_label(&[
        "service", "other", "resource", "1.0.0",
    ]));

    for (actor, resource, code) in [
        (
            &forged,
            identity.path().clone(),
            tc_error::ErrorKind::Unauthorized,
        ),
        (&leader, other, tc_error::ErrorKind::Conflict),
    ] {
        let id = kernel.test_txn().await.id();
        let (_, locked) = sign(actor, id, resource);
        let decision = kernel
            .test_bind(
                Some(id),
                Some(&AuthContext::new(actor.id().clone()).with_signed(locked)),
            )
            .await
            .unwrap();

        // Envelope errors precede authority checks and never enroll a replica.
        for (method, has_body) in [(tc_ir::Method::Get, false), (tc_ir::Method::Put, true)] {
            assert_eq!(
                cluster
                    .decide_request(&decision, &[], method, has_body)
                    .await
                    .unwrap_err()
                    .code(),
                tc_error::ErrorKind::Conflict
            );

            assert!(!cluster.participation.lock().contains_key(&id));
        }

        assert_eq!(
            cluster
                .decide(&decision, TransactionOutcome::Commit)
                .await
                .unwrap_err()
                .code(),
            code
        );

        assert!(
            !cluster.participation.lock().contains_key(&id),
            "rejected authority must not create participation"
        );
    }

    kernel.shutdown().await;
}
