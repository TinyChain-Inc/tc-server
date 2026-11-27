use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use tc_collection::StorageContext;
use tc_collection::tensor::{Tensor, TensorSchema};
use tokio::sync::Notify;

use crate::txn::TransactionOutcome;
use super::*;

#[derive(Clone)]
struct Application {
    block: bool,
    entered: Arc<Notify>,
}

impl Application {
    async fn apply(&self) -> TCResult<()> {
        self.entered.notify_one();
        if self.block {
            std::future::pending().await
        } else {
            Err(TCError::internal("injected local application failure")
                .consume("injected retained cause"))
        }
    }
}

impl Transact for Application {
    async fn commit(&self, _: TxnId) -> TCResult<()> {
        self.apply().await
    }

    async fn rollback(&self, _: &TxnId) -> TCResult<()> {
        self.apply().await
    }

    async fn finalize(&self, _: &TxnId) -> TCResult<()> {
        Ok(())
    }
}

#[test]
fn local_integrity_failures_terminate_before_reuse() {
    const CHILD: &str = "TC_LOCAL_INTEGRITY_CASE";
    if let Ok(case) = std::env::var(CHILD) {
        crate::test_runtime::run(move || async move {
            let kernel = crate::txn::test_kernel("local-integrity").await;
            let txn = kernel.test_txn().await;
            if case.starts_with("storage") {
                let dir = txn.subcontext_unique().context().await.unwrap();
                let schema = TensorSchema::new(
                    tc_value::NumberType::Float(number_general::FloatType::F64),
                    vec![4].into(),
                    None,
                )
                .unwrap();

                let tensor = Tensor::<crate::TxnHandle>::create(dir.clone(), schema, 2)
                    .await
                    .unwrap();

                let base = dir.read().await.get_dir("base").cloned().unwrap();
                let blocks = base.read().await.get_dir("blocks").cloned().unwrap();
                let block = blocks.read().await.get_file("0").cloned().unwrap();
                // A real persisted payload, not a request validation error.
                *block.write::<Vec<f64>>().await.unwrap() = vec![0.0];
                block.sync_all().await.unwrap();
                #[cfg(feature = "http-server")]
                if case == "storage-http" {
                    use tc_value::class::NativeClass;

                    let target = tc_value::ValueType::None.path().to_string();
                    let request = kernel
                        .begin_request(tc_ir::Method::Get, &target, true, None)
                        .await
                        .unwrap();

                    let state = crate::State::from(tc_collection::Collection::Tensor(tensor));
                    let response = crate::http::native_state_response(state, *request)
                        .await
                        .unwrap();

                    hyper::body::to_bytes(response.into_body()).await.unwrap();
                    panic!("HTTP corruption was returned without host termination");
                }

                if case == "storage-read" {
                    let error = tensor.read_value(txn.id(), &[0]).await.unwrap_err();
                    assert!(error.requires_recovery());
                    txn.storage_error(error);
                } else {
                    let state = crate::State::from(tc_collection::Collection::Tensor(tensor));
                    let view = txn.view(state, txn.deadline()).await.unwrap();
                    let mut encoded = destream_json::encode(view).unwrap().boxed();
                    while encoded.try_next().await.unwrap().is_some() {}
                }

                panic!("corruption was returned without host termination");
            }

            let original = kernel.test_libraries();
            let entered = Arc::new(Notify::new());
            let cluster = Cluster::new(
                original.path.clone(),
                Application {
                    block: case.ends_with("cancel"),
                    entered: entered.clone(),
                },
                original.protocol.clone(),
                original.gateway.clone(),
            );

            let outcome = if case.starts_with("commit") {
                TransactionOutcome::Commit
            } else {
                TransactionOutcome::Rollback
            };

            cluster.claim(&txn).await.unwrap();
            txn.lock_coordinator(outcome, false).unwrap();
            let mut decision = Box::pin(cluster.decide(&txn, outcome));
            if case.ends_with("cancel") {
                tokio::select! {
                    _ = entered.notified() => {},
                    result = &mut decision => panic!("application did not block: {result:?}"),
                }

                drop(decision);
            } else {
                let _ = decision.await;
            }

            panic!("uncertain application did not terminate");
        });

        return;
    }

    for case in [
        "commit-error",
        "rollback-error",
        "commit-cancel",
        "rollback-cancel",
        "storage-read",
        "storage-lazy",
        #[cfg(feature = "http-server")]
        "storage-http",
    ] {
        #[cfg(unix)]
        let mut command = {
            let mut command = std::process::Command::new("sh");
            command.args(["-c", "ulimit -c 0; exec \"$@\"", "integrity-probe"]);
            command.arg(std::env::current_exe().unwrap());
            command
        };

        #[cfg(not(unix))]
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());

        let mut child = command.args(["--exact",
            "cluster::replicas::integrity_tests::local_integrity_failures_terminate_before_reuse",
            "--nocapture"]).env(CHILD, case)
            .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while child.try_wait().unwrap().is_none() {
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                let output = child.wait_with_output().unwrap();
                panic!("integrity child timed out: {output:?}");
            }

            // Poll process completion; the timeout is a failure bound, not coordination.
            std::thread::sleep(Duration::from_millis(10));
        }

        let output = child.wait_with_output().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;

            assert_eq!(output.status.signal(), Some(6), "{case}: {output:?}");
        }

        #[cfg(not(unix))]
        assert!(!output.status.success(), "{case}: {output:?}");

        let diagnostic = String::from_utf8_lossy(&output.stderr);
        assert!(
            diagnostic.contains("transaction recovery required"),
            "{case}: {diagnostic}"
        );

        assert!(
            diagnostic.contains(if case.starts_with("storage") {
                "live storage integrity failure"
            } else if case.ends_with("cancel") {
                "local transaction application interrupted"
            } else {
                "injected local application failure"
            }),
            "{case}: {diagnostic}"
        );

        if case.ends_with("error") {
            assert!(
                diagnostic.contains("injected retained cause"),
                "{case}: {diagnostic}"
            );
        }
    }
}

#[tokio::test]
async fn ordinary_and_remote_errors_do_not_require_host_recovery() {
    let kernel = crate::txn::test_kernel("ordinary-integrity-errors").await;
    let txn = kernel.test_txn().await;
    for error in [
        TCError::bad_request("invalid input"),
        TCError::conflict("active calls"),
        TCError::from(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "malformed request",
        )),
    ] {
        assert!(!txn.storage_error(error).requires_recovery());
    }

    let encoded = destream_json::encode(TCError::recovery_required("remote corruption")).unwrap();
    let remote: TCError = destream_json::try_decode((), encoded).await.unwrap();
    assert!(!txn.storage_error(remote).requires_recovery());
    assert!(txn.check_active().is_ok());
    drop(txn);
    kernel.shutdown().await;
}
