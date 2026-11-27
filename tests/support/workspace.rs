impl super::Workspace {
    pub(crate) async fn has_transaction(&self, txn_id: tc_ir::TxnId) -> tc_error::TCResult<bool> {
        let txns = self.transactions.read().await;
        Ok(txns.get_dir(&txn_id.to_string()).is_some())
    }
}

fn txn_id() -> tc_ir::TxnId {
    tc_ir::TxnId::from_parts(tc_ir::NetworkTime::from_nanos(1), 0).with_trace([1; 32])
}

fn workspace(name: &str) -> super::Workspace {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_nanos();

    crate::HostStorage::new(&crate::HostLimits::default().storage)
        .workspace(std::env::temp_dir().join(format!("tc-workspace-{name}-{unique}")))
        .expect("workspace")
}

#[tokio::test]
async fn transaction_workspace_ids_are_recovered_without_records() {
    let workspace = workspace("transaction-id");
    workspace
        .transaction_child(txn_id(), &[])
        .await
        .expect("transaction dir");

    assert_eq!(workspace.transaction_ids().await.expect("IDs"), [txn_id()]);
}

#[tokio::test]
async fn cleanup_removes_workspaces_through_the_frontier() {
    let workspace = workspace("cleanup-through");
    let newer = tc_ir::TxnId::from_parts(tc_ir::NetworkTime::from_nanos(2), 0).with_trace([2; 32]);
    workspace
        .transaction_child(txn_id(), &[])
        .await
        .expect("old transaction");

    workspace
        .transaction_child(newer, &[])
        .await
        .expect("new transaction");

    workspace.remove_through(txn_id()).await.expect("cleanup");
    assert_eq!(workspace.transaction_ids().await.expect("IDs"), [newer]);
}
