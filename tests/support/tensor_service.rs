use tc_ir::Transact;

use crate::{Kernel, Method};
use super::{complete, execute, open_at, service_fixture, service_txn, stage_service};

const IDENTITY: &str = "/service/test/tensor/1.0.0";

fn coordinate(index: u64) -> crate::State {
    tc_value::Value::Tuple(vec![index.into()]).into()
}

fn selection(start: u64, stop: u64) -> crate::State {
    tc_value::Value::Tuple(vec![tc_value::Value::Tuple(vec![
        start.into(),
        stop.into(),
        1u64.into(),
    ])])
    .into()
}

async fn write(kernel: &Kernel, txn: &crate::TxnHandle, member: &str, index: u64, value: u64) {
    execute(
        kernel,
        Method::Put,
        &format!("{IDENTITY}/write_{member}"),
        Some(crate::State::Tuple(vec![
            coordinate(index),
            tc_value::Value::from(value).into(),
        ])),
        txn.clone(),
    )
    .await
    .unwrap();
}

async fn read(kernel: &Kernel, txn: &crate::TxnHandle, member: &str, index: u64) -> u64 {
    let value = execute(
        kernel,
        Method::Get,
        &format!("{IDENTITY}/{member}"),
        Some(coordinate(index)),
        txn.clone(),
    )
    .await
    .unwrap();

    let crate::State::Scalar(scalar) = &value else {
        panic!("expected scalar")
    };

    let tc_ir::Scalar::Value(value) = &scalar else {
        panic!("expected value")
    };

    let tc_value::Value::Number(number) = &value else {
        panic!("expected number")
    };

    number.to_string().parse().unwrap()
}

fn read_history(
    path: std::path::PathBuf,
    expected: tc_ir::TxnId,
) -> std::sync::Arc<Vec<tc_chain::MutationRecord>> {
    crate::test_runtime::run(move || async move {
        use freqfs::FileLoad;

        let file = tokio::fs::File::open(&path).await.unwrap();
        let metadata = file.metadata().await.unwrap();
        let tc_chain::ChainFile::Transaction { id, records } =
            tc_chain::ChainFile::load(&path, file, metadata)
                .await
                .unwrap()
        else {
            panic!("expected transaction history")
        };

        assert_eq!(id, expected);
        records
    })
}

#[test]
fn tensor_service_visibility_rollback_and_recovery() {
    let path = crate::txn::test_path("tensor-service-recovery");
    let initial = path.clone();
    let committed = crate::test_runtime::run(move || async move {
        let kernel = open_at(initial, std::time::Duration::from_secs(600), false)
            .await
            .unwrap();

        let identity = IDENTITY.parse().unwrap();
        let install = service_txn(&kernel, &identity).await;
        stage_service(
            &kernel,
            &install,
            identity.clone(),
            service_fixture::tensor_definition(),
        )
        .await;

        complete(&kernel, install, crate::txn::TransactionOutcome::Commit).await;

        let older = service_txn(&kernel, &identity).await;
        let pending = service_txn(&kernel, &identity).await;
        for member in ["dense", "sparse"] {
            // Read the older snapshot before the later writer registers its range.
            assert_eq!(read(&kernel, &older, member, 0).await, 0);
            write(&kernel, &pending, member, 4_096, 9).await;
            assert_eq!(read(&kernel, &pending, member, 4_096).await, 9);
        }

        complete(
            &kernel,
            pending.clone(),
            crate::txn::TransactionOutcome::Rollback,
        )
        .await;

        complete(&kernel, pending, crate::txn::TransactionOutcome::Rollback).await;
        complete(&kernel, older, crate::txn::TransactionOutcome::Commit).await;

        let accepted = service_txn(&kernel, &identity).await;
        for member in ["dense", "sparse"] {
            assert_eq!(read(&kernel, &accepted, member, 4_096).await, 0);
            write(&kernel, &accepted, member, 0, 7).await;
            write(&kernel, &accepted, member, 4_096, 8).await;
            // A selected write targets the Chain-owned member, spanning the
            // native block boundary rather than mutating a returned slice.
            execute(
                &kernel,
                Method::Put,
                &format!("{IDENTITY}/write_{member}"),
                Some(crate::State::Tuple(vec![
                    selection(4_095, 4_097),
                    tc_value::Value::from(8u64).into(),
                ])),
                accepted.clone(),
            )
            .await
            .unwrap();

            let source = execute(
                &kernel,
                Method::Get,
                &format!("{IDENTITY}/{member}"),
                None,
                accepted.clone(),
            )
            .await
            .unwrap();

            let crate::State::Collection(tc_collection::Collection::Tensor(tensor)) = &source
            else {
                panic!("expected Tensor")
            };

            for view in [
                tensor.clone(),
                tensor.clone().reshape(vec![4_097].into()).unwrap(),
                tensor.clone().transpose(None).unwrap(),
                tensor
                    .clone()
                    .slice(vec![tc_collection::tensor::AxisRange::In(0, 4_097, 1)].into())
                    .unwrap(),
            ] {
                assert!(!view.is_persistent());
                assert_eq!(
                    view.write_value(&accepted, &[0], 99u64.into())
                        .await
                        .unwrap_err()
                        .code(),
                    tc_error::ErrorKind::BadRequest
                );

                assert!(view.fill(&accepted, 99u64.into()).await.is_err());
                assert!(view.write_tensor(&accepted, tensor.clone()).await.is_err());
                assert!(view.commit(accepted.id()).await.is_err());
                assert!(view.rollback(&accepted.id()).await.is_err());
                assert!(view.finalize(&accepted.id()).await.is_err());
            }

            assert_eq!(read(&kernel, &accepted, member, 0).await, 7);
            assert_eq!(read(&kernel, &accepted, member, 4_095).await, 8);
            execute(
                &kernel,
                Method::Put,
                &format!("{IDENTITY}/write_{member}"),
                Some(crate::State::Tuple(vec![
                    tc_value::Value::default().into(),
                    source,
                ])),
                accepted.clone(),
            )
            .await
            .unwrap();
        }

        complete(
            &kernel,
            accepted.clone(),
            crate::txn::TransactionOutcome::Commit,
        )
        .await;

        complete(
            &kernel,
            accepted.clone(),
            crate::txn::TransactionOutcome::Commit,
        )
        .await;

        let zero = service_txn(&kernel, &identity).await;
        write(&kernel, &zero, "sparse", 0, 0).await;
        execute(
            &kernel,
            Method::Put,
            &format!("{IDENTITY}/write_sparse"),
            Some(crate::State::Tuple(vec![
                selection(4_095, 4_097),
                tc_value::Value::from(0u64).into(),
            ])),
            zero.clone(),
        )
        .await
        .unwrap();

        write(&kernel, &zero, "sparse", 4_096, 8).await;
        complete(
            &kernel,
            zero.clone(),
            crate::txn::TransactionOutcome::Commit,
        )
        .await;

        kernel.shutdown().await;
        (accepted.id(), zero.id())
    });

    // Restart with the expiry task stopped, retaining the committed Chain records.
    for member in ["dense", "sparse"] {
        let wal = path.join(format!(
            "data/service/test/tensor/1.0.0/.native/{member}/wal/{}.chain_block",
            committed.0
        ));

        let accepted = read_history(wal, committed.0);
        assert_eq!(accepted.len(), 4, "only owner-bound writes enter history");
        let crate::State::Scalar(expected_key) = selection(4_095, 4_097) else {
            panic!("expected scalar selection")
        };

        assert!(accepted.iter().any(|record| {
            matches!(record, tc_chain::MutationRecord::Put(_, key, value)
                if key == &expected_key && matches!(value.as_ref(), tc_chain::StoredValue::Scalar(value)
                    if value == &tc_ir::Scalar::from(tc_value::Value::from(8u64))))
        }));

        if member == "sparse" {
            let zero = read_history(
                path.join(format!(
                    "data/service/test/tensor/1.0.0/.native/{member}/wal/{}.chain_block",
                    committed.1
                )),
                committed.1,
            );

            assert_eq!(zero.len(), 3);
            assert!(zero.iter().any(|record| {
                matches!(record, tc_chain::MutationRecord::Put(_, key, value)
                    if key == &expected_key && matches!(value.as_ref(), tc_chain::StoredValue::Scalar(value)
                        if value == &tc_ir::Scalar::from(tc_value::Value::from(0u64))))
            }));
        }
    }

    for finalize in [true, false] {
        let reopened = path.clone();
        crate::test_runtime::run(move || async move {
            let kernel = open_at(reopened, std::time::Duration::from_secs(600), false)
                .await
                .unwrap();

            let txn = service_txn(&kernel, &IDENTITY.parse().unwrap()).await;
            assert_eq!(read(&kernel, &txn, "dense", 0).await, 7);
            assert_eq!(read(&kernel, &txn, "sparse", 0).await, 0);
            assert_eq!(read(&kernel, &txn, "dense", 4_095).await, 8);
            assert_eq!(read(&kernel, &txn, "sparse", 4_095).await, 0);
            for member in ["dense", "sparse"] {
                assert_eq!(read(&kernel, &txn, member, 4_094).await, 0);
                assert_eq!(read(&kernel, &txn, member, 4_096).await, 8);
            }

            complete(&kernel, txn.clone(), crate::txn::TransactionOutcome::Commit).await;
            if finalize {
                kernel.test_services().finalize(&txn.id()).await.unwrap();
            }

            kernel.shutdown().await;
        });

        if finalize {
            std::fs::remove_dir_all(path.join("workspace/txn")).unwrap();
        }
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn tensor_service_committed_replay_survives_process_abort() {
    const CHILD: &str = "TC_TENSOR_SERVICE_CRASH_ROOT";
    if let Some(path) = std::env::var_os(CHILD) {
        crate::test_runtime::run(move || async move {
            let path = std::path::PathBuf::from(path);
            let kernel = open_at(path.clone(), std::time::Duration::from_secs(600), false)
                .await
                .unwrap();

            let identity = IDENTITY.parse().unwrap();
            let install = service_txn(&kernel, &identity).await;
            stage_service(
                &kernel,
                &install,
                identity.clone(),
                service_fixture::tensor_definition(),
            )
            .await;

            complete(
                &kernel,
                install.clone(),
                crate::txn::TransactionOutcome::Commit,
            )
            .await;

            // Publish initialization before arming the save observer, so only
            // committed canonical mutation is counted at the failure boundary.
            kernel
                .test_services()
                .finalize(&install.id())
                .await
                .unwrap();

            let mode = std::env::var("TC_TENSOR_FAILURE_MODE").unwrap();
            let members = if mode == "finalize-dense" {
                &["dense"][..]
            } else if mode == "finalize-sparse" {
                &["sparse"][..]
            } else {
                &["dense", "sparse"][..]
            };

            let accepted = service_txn(&kernel, &identity).await;
            for member in members {
                write(&kernel, &accepted, member, 0, 10).await;
                write(&kernel, &accepted, member, 4_096, 11).await;
            }

            complete(
                &kernel,
                accepted.clone(),
                crate::txn::TransactionOutcome::Commit,
            )
            .await;

            let newer = service_txn(&kernel, &identity).await;
            for member in members {
                write(&kernel, &newer, member, 0, 12).await;
                write(&kernel, &newer, member, 4_096, 11).await;
            }

            complete(&kernel, newer, crate::txn::TransactionOutcome::Commit).await;

            if mode != "abort" {
                std::fs::write(path.join("abort-ready"), accepted.id().to_string()).unwrap();
                drop(accepted);
                drop(install);
                use crate::storage::tensor_fault::{EVIDENCE, FAIL_NATIVE, SAVED_PAYLOADS};

                FAIL_NATIVE
                    .scope(
                        mode == "finalize-dense",
                        EVIDENCE.scope(
                            path.join("saved-before-failure"),
                            SAVED_PAYLOADS.scope(
                                std::cell::Cell::new(0),
                                crate::txn::server::tests::expire_services(&kernel),
                            ),
                        ),
                    )
                    .await;

                panic!("materialization failure did not terminate the host");
            }

            let uncommitted = service_txn(&kernel, &identity).await;
            for member in ["dense", "sparse"] {
                write(&kernel, &uncommitted, member, 4_096, 99).await;
            }

            // Parent requires this evidence; an earlier panic cannot pass the test.
            std::fs::write(path.join("abort-ready"), accepted.id().to_string()).unwrap();
            std::process::abort();
        });

        unreachable!();
    }

    for mode in ["abort", "finalize-dense", "finalize-sparse"] {
        let path = crate::txn::test_path(&format!("tensor-service-{mode}"));
        #[cfg(unix)]
        let mut command = {
            let mut command = std::process::Command::new("sh");
            command
                .args(["-c", "ulimit -c 0; exec \"$@\"", "tensor-abort"])
                .arg(std::env::current_exe().unwrap());

            command
        };

        #[cfg(not(unix))]
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());

        let mut child = command
            .args([
                "--exact",
                &format!(
                    "{}::tensor_service_committed_replay_survives_process_abort",
                    module_path!().split_once("::").unwrap().1
                ),
                "--nocapture",
            ])
            .env(CHILD, &path)
            .env("TC_TENSOR_FAILURE_MODE", mode)
            .spawn()
            .unwrap();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }

            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("Tensor crash subprocess timed out");
            }

            std::thread::sleep(std::time::Duration::from_millis(10));
        };

        assert!(!status.success());
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;

            assert_eq!(status.signal(), Some(6));
        }

        let id = std::fs::read_to_string(path.join("abort-ready"))
            .expect("committed and pending work reached abort boundary");

        let members = if mode == "finalize-dense" {
            &["dense"][..]
        } else if mode == "finalize-sparse" {
            &["sparse"][..]
        } else {
            &["dense", "sparse"][..]
        };

        for member in members {
            let bytes = std::fs::read(path.join(format!(
                "data/service/test/tensor/1.0.0/.native/{member}/wal/{id}.chain_block"
            )))
            .unwrap();

            let wal: serde_json::Value = serde_json::from_slice(&bytes[32..]).unwrap();
            assert_eq!(wal[0], "transaction");
            assert_eq!(wal[1], id);
        }

        if mode != "abort" {
            let saved: usize = std::fs::read_to_string(path.join("saved-before-failure"))
                .unwrap()
                .parse()
                .unwrap();

            assert!(saved > 0, "canonical payload publication preceded failure");
        }

        let reopened = path.clone();
        crate::test_runtime::run(move || async move {
            let kernel = open_at(reopened, std::time::Duration::from_secs(600), false)
                .await
                .unwrap();

            let txn = service_txn(&kernel, &IDENTITY.parse().unwrap()).await;
            for member in members {
                assert_eq!(read(&kernel, &txn, member, 4_096).await, 11);
                assert_eq!(read(&kernel, &txn, member, 0).await, 12);
            }

            complete(&kernel, txn.clone(), crate::txn::TransactionOutcome::Commit).await;
            kernel.test_services().finalize(&txn.id()).await.unwrap();
            kernel.shutdown().await;
        });

        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn tensor_service_concurrent_writes_and_cancelled_stream_release() {
    let path = crate::test_runtime::run(|| async {
        use futures::{FutureExt, TryStreamExt};

        let path = crate::txn::test_path("tensor-service-concurrency");
        let kernel = open_at(path.clone(), std::time::Duration::from_secs(600), false)
            .await
            .unwrap();

        let identity = IDENTITY.parse().unwrap();
        let install = service_txn(&kernel, &identity).await;
        stage_service(
            &kernel,
            &install,
            identity.clone(),
            service_fixture::tensor_definition(),
        )
        .await;

        complete(&kernel, install, crate::txn::TransactionOutcome::Commit).await;
        for member in ["dense", "sparse"] {
            let txn = service_txn(&kernel, &identity).await;
            let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
            let attempt = |index, value| {
                let (barrier, kernel, txn) = (&barrier, &kernel, &txn);
                async move {
                    barrier.wait().await;
                    execute(
                        kernel,
                        Method::Put,
                        &format!("{IDENTITY}/write_{member}"),
                        Some(crate::State::Tuple(vec![
                            coordinate(index),
                            tc_value::Value::from(value).into(),
                        ])),
                        txn.clone(),
                    )
                    .await
                }
            };

            let (left, right) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                tokio::join!(attempt(0, 3u64), attempt(4096, 4u64))
            })
            .await
            .expect("concurrent requests complete or conflict");

            assert!(left.is_ok() || right.is_ok());
            // A request owns the transaction operation permit. Admission conflicts
            // precede mutation and can be retried after the admitted request completes.
            for (result, index, value) in [(left, 0, 3), (right, 4096, 4)] {
                if let Err(error) = result {
                    assert!(error.code().is_conflict(), "{error}");
                    write(&kernel, &txn, member, index, value).await;
                }
            }

            assert_eq!(read(&kernel, &txn, member, 0).await, 3);
            assert_eq!(read(&kernel, &txn, member, 4096).await, 4);
            let value = execute(
                &kernel,
                Method::Get,
                &format!("{IDENTITY}/{member}"),
                None,
                txn.clone(),
            )
            .await
            .unwrap();

            let crate::State::Collection(tc_collection::Collection::Tensor(tensor)) = value else {
                panic!("expected Tensor")
            };

            let mut stream = tensor.elements(txn.id()).await.unwrap();
            assert_eq!(stream.try_next().await.unwrap().unwrap(), 3u64.into());
            {
                let pending = write(&kernel, &txn, member, 0, 5);
                let mut pending = std::pin::pin!(pending);
                assert!(
                    pending.as_mut().now_or_never().is_none(),
                    "retained stream excludes overlapping mutation"
                );

                // Dropping the pending request must not leak its reservation.
            }

            drop(stream);
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                write(&kernel, &txn, member, 0, 6),
            )
            .await
            .expect("cancelled request and stream release permits");

            assert_eq!(read(&kernel, &txn, member, 0).await, 6);
            complete(&kernel, txn, crate::txn::TransactionOutcome::Commit).await;
        }

        kernel.shutdown().await;
        drop(kernel);
        path
    });

    std::fs::remove_dir_all(path).unwrap();
}
