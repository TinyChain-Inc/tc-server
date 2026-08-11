use std::sync::Arc;

use super::*;

#[tokio::test]
async fn binds_an_implicit_transaction_for_native_execution() {
    let kernel = Kernel::builder().with_host_id("test-host").finish();
    let deadline = kernel.resources().deadline();
    let binding = kernel
        .bind_transaction(
            Method::Get,
            "/state/scalar/value/number/add",
            false,
            None,
            None,
            deadline,
        )
        .await
        .expect("bind transaction");

    let Some(BoundTransaction {
        txn,
        implicit: true,
    }) = binding
    else {
        panic!("expected implicit transaction");
    };
    assert_eq!(txn.deadline().instant(), deadline.instant());
}

#[tokio::test]
async fn ttl_worker_rolls_back_abandoned_transaction() {
    let kernel = Kernel::builder()
        .with_host_id("test-host")
        .with_txn_ttl(std::time::Duration::from_millis(10))
        .finish();
    kernel.start_transaction_expiry(&tokio::runtime::Handle::current());
    let Some(BoundTransaction { txn, .. }) = kernel
        .bind_transaction(
            Method::Get,
            "/state/scalar/value/number/add",
            false,
            None,
            None,
            kernel.resources().deadline(),
        )
        .await
        .expect("bind transaction")
    else {
        panic!("expected executable transaction");
    };
    let txn_id = txn.id();

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while kernel.txn_server().contains(&txn_id) {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("TTL rollback");
}

#[tokio::test]
async fn finalization_is_exactly_once() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hook_calls = Arc::clone(&calls);
    let kernel = Kernel::builder()
        .with_host_id("test-host")
        .with_txn_finalize_hook(move |_txn, _commit| {
            let calls = Arc::clone(&hook_calls);
            async move {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        })
        .finish();
    let Some(BoundTransaction { txn, .. }) = kernel
        .bind_transaction(
            Method::Get,
            "/state/scalar/value/number/add",
            false,
            None,
            None,
            kernel.resources().deadline(),
        )
        .await
        .expect("bind transaction")
    else {
        panic!("expected executable transaction");
    };

    kernel
        .complete_transaction(txn.clone(), crate::txn::TransactionOutcome::Failed)
        .await
        .expect("first finalize");
    assert!(
        kernel
            .complete_transaction(txn, crate::txn::TransactionOutcome::Failed)
            .await
            .is_err()
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}
