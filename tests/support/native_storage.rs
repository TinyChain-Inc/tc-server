use freqfs::Cache;
use tc_collection::{
    Collection, StorageContext, btree::BTreeColumnSchema, collection::CollectionSchema,
};
use tc_ir::{Public, Scalar, Transact};
use tc_value::Value;

use super::ApplicationBlock;

#[test]
fn application_membership_stops_at_native_chain_storage() {
    crate::test_runtime::run(|| async {
        let txn = crate::txn::test_txn("native-storage").await;
        let path = crate::txn::test_path("native-application");
        std::fs::create_dir_all(&path).unwrap();
        let cache = || {
            Cache::<ApplicationBlock>::new(1 << 24, Some(64), 0, std::time::Duration::from_secs(3))
        };
        let root = cache().load(path.clone()).unwrap();
        let directory = txfs::Dir::load(root.clone()).await.unwrap();
        directory
            .create_file(
                txn.id(),
                "manifest.json".parse().unwrap(),
                ApplicationBlock::Manifest(
                    "/service/test/state/1.0.0".parse().unwrap(),
                    Scalar::Map(Default::default()),
                ),
            )
            .await
            .unwrap();
        let native = directory.create_native().await.unwrap();
        let (canonical, wal, values) = {
            let mut native = native.write().await;
            (
                native.create_dir("subject".into()).unwrap(),
                native.create_dir("wal".into()).unwrap(),
                native.create_dir("values".into()).unwrap(),
            )
        };
        let schema = CollectionSchema::BTree(vec![BTreeColumnSchema {
            name: "key".into(),
            dtype: Value::from(0_u64).class(),
            max_size: None,
        }]);
        let subject =
            Collection::<crate::TxnHandle>::create(canonical.clone(), schema.clone()).unwrap();
        let chain = tc_chain::SyncChain::create(
            subject,
            wal.clone(),
            values,
            tc_chain::TxnTaskQueue::new(8),
        )
        .await
        .unwrap();
        let state = crate::State::from(chain.clone());
        state
            .put(
                &txn,
                &["insert".parse().unwrap()],
                Value::None.into(),
                crate::State::from(Value::Tuple(vec![1_u64.into()])),
            )
            .await
            .unwrap();
        chain.commit(txn.id()).await.unwrap();

        // These guards would deadlock any accidental recursive native writeback.
        {
            let _canonical = canonical.write().await;
            let _wal = wal.write().await;
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                directory.commit(txn.id(), true).await.unwrap();
                directory.finalize(txn.id()).await.unwrap();
            })
            .await
            .unwrap();
        }
        let before = std::fs::read(path.join(".native/wal/committed.chain_block")).unwrap();
        let reopened =
            txfs::Dir::<tc_ir::TxnId, ApplicationBlock>::load(cache().load(path.clone()).unwrap())
                .await
                .unwrap();
        let native = reopened.native().await.unwrap().unwrap();
        let (canonical, wal, values) = {
            let mut native = native.write().await;
            (
                native.get_dir("subject").unwrap().clone(),
                native.get_dir("wal").unwrap().clone(),
                native.get_or_create_dir("values".into()).unwrap(),
            )
        };
        assert!(!canonical.read().await.contains(txfs::VERSIONS));
        assert!(!wal.read().await.contains(txfs::VERSIONS));
        let chain = tc_chain::SyncChain::open(
            || Collection::<crate::TxnHandle>::load(canonical, schema),
            wal,
            values,
            tc_chain::TxnTaskQueue::new(8),
        )
        .await
        .unwrap();
        Box::pin(chain.recover::<crate::State, _, _>(|id| {
            assert_eq!(id, txn.id());
            let txn = txn.subcontext_unique();
            async move { Ok(txn) }
        }))
        .await
        .unwrap();
        assert_eq!(
            std::fs::read(path.join(".native/wal/committed.chain_block")).unwrap(),
            before
        );
        let count: crate::State = chain
            .get(&txn, &["count".parse().unwrap()], Value::None.into())
            .await
            .unwrap();
        assert!(
            matches!(count, crate::State::Scalar(Scalar::Value(Value::Number(n))) if n.to_string() == "1")
        );
    });
}
