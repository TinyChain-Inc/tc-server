use super::*;

#[tokio::test]
async fn activity_does_not_extend_the_identity_derived_expiry() {
    let kernel = super::super::test_kernel_with(
        "fixed-expiry",
        Duration::from_millis(40),
        super::super::test_workspace("fixed-expiry"),
    )
    .await;
    let server = &kernel.txn_server;
    let txn = kernel.test_txn().await;
    let first = server.state.inner.lock().active[&txn.id()].expires;
    tokio::time::sleep(Duration::from_millis(5)).await;
    server.observe(txn.id());
    assert_eq!(server.state.inner.lock().active[&txn.id()].expires, first);
}

#[tokio::test]
async fn concurrent_allocation_persists_the_latest_identity() {
    let kernel = super::super::test_kernel("concurrent-allocation").await;
    let (first, second) = tokio::join!(kernel.test_txn(), kernel.test_txn());
    let latest = std::cmp::max(first.id(), second.id());
    let server = &kernel.txn_server;
    let config = server.state.config.clone();
    let workspace = config.workspace.clone();
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
