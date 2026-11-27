use freqfs::Cache;
use tc_collection::btree::BTreeColumnSchema;
use tc_collection::collection::CollectionSchema;
use tc_collection::{Collection, StorageContext};
use tc_ir::{Public, Scalar, Transact};
use tc_value::{NumberType, Value, ValueType};

use super::ApplicationBlock;

#[test]
fn native_history_can_exceed_the_application_body_limit_and_reopen() {
    let path = crate::txn::test_path("native-large-wal-file");
    let cleanup = path.clone();
    crate::test_runtime::run(move || async move {
        use freqfs::FileSave;
        use get_size::GetSize;
        use tokio::io::AsyncWriteExt;

        let txn_id = tc_ir::TxnId::from_parts(tc_ir::NetworkTime::from_nanos(1), 1);
        std::fs::create_dir_all(&path).unwrap();
        let name = format!("{txn_id}.chain_block");
        let value = ApplicationBlock::Chain(tc_chain::ChainFile::Transaction {
            id: txn_id,
            records: vec![tc_chain::MutationRecord::Put(
                "/insert".parse().unwrap(),
                Scalar::default(),
                Box::new(tc_chain::StoredValue::Scalar(Scalar::from(Value::String(
                    "\u{0001}".repeat(11 * 1024 * 1024),
                )))),
            )]
            .into(),
        });

        let limit = 64 * 1024 * 1024;
        assert!(value.get_size() < limit);
        // The file composition owns this unpublished fixture, before cache load.
        let mut file = tokio::fs::File::create(path.join(&name)).await.unwrap();
        let written = value.save(&mut file).await.unwrap();
        assert!(written > limit as u64);
        file.flush().await.unwrap();
        drop(file);
        let root =
            Cache::<ApplicationBlock>::new(limit, None, 0, std::time::Duration::from_secs(3))
                .load(path.clone())
                .unwrap();

        let entry = root.read().await.get_file(&name).unwrap().clone();
        let decoded = entry.read::<ApplicationBlock>().await.unwrap();
        assert!(decoded.get_size() <= value.get_size());
        let ApplicationBlock::Chain(actual) = &*decoded else {
            panic!("native history")
        };

        assert!(
            matches!(actual, tc_chain::ChainFile::Transaction { id, records } if *id == txn_id && records.len() == 1)
        );
    });

    // The test runtime and every cache handle are gone before fixture cleanup.
    std::fs::remove_dir_all(cleanup).unwrap();
}

#[test]
fn application_formats_enforce_their_limits_during_save() {
    let path = crate::txn::test_path("application-save-limits");
    let cleanup = path.clone();
    crate::test_runtime::run(move || async move {
        use freqfs::{FileLoad, FileSave};
        use tokio::io::AsyncWriteExt;

        std::fs::create_dir_all(&path).unwrap();
        let module = ApplicationBlock::Module(vec![0; 64 * 1024 * 1024 + 1].into());
        let mut file = tokio::fs::File::create(path.join("module.wasm"))
            .await
            .unwrap();

        assert_eq!(
            module.save(&mut file).await.unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );

        file.flush().await.unwrap();
        assert_eq!(file.metadata().await.unwrap().len(), 0);
        file.set_len(64 * 1024 * 1024 + 1).await.unwrap();
        let metadata = file.metadata().await.unwrap();
        assert_eq!(
            ApplicationBlock::load(&path.join("module.wasm"), file, metadata)
                .await
                .err()
                .expect("oversized module")
                .kind(),
            std::io::ErrorKind::InvalidData
        );

        drop(module);
        let manifest = ApplicationBlock::Manifest(
            "/service/test/limit/1.0.0".parse().unwrap(),
            Box::new(Scalar::from(Value::String(
                "\u{0001}".repeat(11 * 1024 * 1024),
            ))),
        );

        let mut file = tokio::fs::File::create(path.join("manifest.json"))
            .await
            .unwrap();

        assert_eq!(
            manifest.save(&mut file).await.unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );

        file.flush().await.unwrap();
        assert!(file.metadata().await.unwrap().len() <= 64 * 1024 * 1024);
    });

    std::fs::remove_dir_all(cleanup).unwrap();
}

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
                    Box::new(Scalar::Map(Default::default())),
                ),
            )
            .await
            .unwrap();

        let native = directory
            .create_dir(txn.id(), ".native".parse().unwrap())
            .await
            .unwrap();

        let (canonical, wal, values) = {
            let mut native = native.write().await;
            (
                native.create_dir("subject".into()).unwrap(),
                native.create_dir("wal".into()).unwrap(),
                native.create_dir("values".into()).unwrap(),
            )
        };

        let schema = CollectionSchema::BTree(
            vec![BTreeColumnSchema {
                name: "key".into(),
                dtype: ValueType::Number(NumberType::Number),
                max_size: None,
            }]
            .try_into()
            .unwrap(),
        );

        let subject = Collection::<crate::TxnHandle>::create(canonical.clone(), schema.clone())
            .await
            .unwrap();

        let chain = tc_chain::SyncChain::create(
            subject,
            wal.clone(),
            values,
            8,
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
                directory.commit(txn.id()).await.unwrap();
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

        let native = reopened
            .get_dir(txn.id(), &".native".parse().unwrap())
            .await
            .unwrap()
            .unwrap()
            .clone();
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
            8,
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
            matches!(&count, crate::State::Scalar(scalar) if matches!(&scalar, tc_ir::Scalar::Value(value) if matches!(&value, tc_value::Value::Number(n) if n.to_string() == "1")))
        );
    });
}

#[test]
fn application_tensor_payloads_and_sparse_index_reopen_with_recorded_types() {
    crate::test_runtime::run(|| async {
        use tc_collection::tensor::{Tensor, TensorSchema};

        let txn = crate::txn::test_txn("native-tensor-storage").await;
        let path = crate::txn::test_path("native-tensor-application");
        std::fs::create_dir_all(&path).unwrap();
        let cache =
            || Cache::<ApplicationBlock>::new(1 << 20, None, 0, std::time::Duration::from_secs(3));

        let schema = TensorSchema::new(
            tc_value::NumberType::Float(number_general::FloatType::F64),
            vec![128].into(),
            Some(0),
        )
        .unwrap();

        let tensor = Tensor::<crate::TxnHandle>::create(
            cache().load(path.clone()).unwrap(),
            schema.clone(),
            2,
        )
        .await
        .unwrap();

        for key in 0..64u64 {
            tensor
                .write_value(&txn, &[key * 2], (key + 1).into())
                .await
                .unwrap();
        }

        tensor.commit(txn.id()).await.unwrap();
        tensor.finalize(&txn.id()).await.unwrap();
        drop(tensor);
        let tensor = Tensor::<crate::TxnHandle>::load(cache().load(path.clone()).unwrap(), schema)
            .await
            .unwrap();

        assert_eq!(
            tensor
                .read_value(
                    crate::txn::test_txn("native-tensor-reopen").await.id(),
                    &[126]
                )
                .await
                .unwrap(),
            number_general::Number::from(64.0)
        );

        let wrong = TensorSchema::new(
            tc_value::NumberType::UInt(number_general::UIntType::U64),
            vec![128].into(),
            Some(0),
        )
        .unwrap();

        assert!(
            Tensor::<crate::TxnHandle>::load(cache().load(path).unwrap(), wrong)
                .await
                .is_err()
        );
    });
}

#[test]
fn cold_reads_admit_long_values_and_short_modules() {
    crate::test_runtime::run(|| async {
        use tc_collection::PersistentFile;

        let mut cases = vec![
            (
                "manifest.json",
                ApplicationBlock::Manifest(
                    "/lib/test/allocation/1.0.0".parse().unwrap(),
                    Box::new(Value::from("x".repeat(128 * 1024)).into()),
                ),
                64 * 1024 * 1024,
            ),
            (
                "node",
                ApplicationBlock::Collection(PersistentFile::Node(
                    tc_collection::CollectionNode::Leaf(vec![vec![Value::from("x".repeat(4096))]]),
                )),
                64 * 1024,
            ),
        ];

        for len in [0, 8, 16] {
            let mut bytes = vec![0; len];
            if len >= 8 {
                bytes[..8].copy_from_slice(b"\0asm\x01\0\0\0");
            }

            cases.push((
                "module.wasm",
                ApplicationBlock::Module(bytes.into()),
                64 * 1024,
            ));
        }

        for (i, (name, expected, budget)) in cases.into_iter().enumerate() {
            let path = crate::txn::test_path(&format!("cold-admission-{i}"));
            std::fs::create_dir_all(&path).unwrap();
            let cache = || {
                Cache::<ApplicationBlock>::new(budget, None, 0, std::time::Duration::from_secs(3))
            };

            {
                let root = cache().load(path.clone()).unwrap();
                let file = root
                    .write()
                    .await
                    .create_file(name.into(), expected.clone())
                    .await
                    .unwrap();

                file.sync_all().await.unwrap();
            }

            {
                let root = cache().load(path.clone()).unwrap();
                let file = root.read().await.get_file(name).unwrap().clone();
                let actual = file.read::<ApplicationBlock>().await.unwrap();
                match (&expected, &*actual) {
                    (ApplicationBlock::Manifest(a, b), ApplicationBlock::Manifest(c, d)) => {
                        assert_eq!((a, b), (c, d))
                    }
                    (ApplicationBlock::Module(a), ApplicationBlock::Module(b)) => assert_eq!(a, b),
                    (
                        ApplicationBlock::Collection(PersistentFile::Node(a)),
                        ApplicationBlock::Collection(PersistentFile::Node(b)),
                    ) => match (a, b) {
                        (
                            tc_collection::CollectionNode::Leaf(a),
                            tc_collection::CollectionNode::Leaf(b),
                        ) => assert_eq!(a, b),
                        _ => panic!("wrong node kind"),
                    },
                    _ => panic!("wrong reopened payload"),
                }
            }

            std::fs::remove_dir_all(path).unwrap();
        }
    });
}
