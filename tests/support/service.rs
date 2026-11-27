pub(crate) fn definition(table: bool) -> tc_ir::Scalar {
    use tc_collection::btree::BTreeColumnSchema;
    use tc_collection::collection::CollectionSchema;
    use tc_ir::{IdRef, OpDef, OpRef, Scalar, Subject, TCRef};
    use tc_value::{NumberType, Value, ValueType};

    // These lifecycle fixtures accept native and JSON-decoded numbers alike.
    let dtype = ValueType::Number(NumberType::Number);
    let schema = if table {
        use tc_collection::btree::StorageConfig;
        use tc_collection::table::{Column, TableSchema};

        CollectionSchema::Table(
            TableSchema::new(
                vec![Column {
                    name: "key".parse().unwrap(),
                    dtype: dtype.clone(),
                }],
                vec![Column {
                    name: "value".parse().unwrap(),
                    dtype,
                }],
                vec![],
                StorageConfig::default(),
            )
            .unwrap(),
        )
    } else {
        CollectionSchema::BTree(
            vec![BTreeColumnSchema {
                name: "key".into(),
                dtype,
                max_size: None,
            }]
            .try_into()
            .unwrap(),
        )
    };

    let (class, schema): (pathlink::PathBuf, Value) = schema.into();
    let collection = Scalar::from(TCRef::Op(OpRef::Get((
        Subject::Link(class.into()),
        schema.into(),
    ))));
    let chain = Scalar::from(TCRef::Op(OpRef::Get((
        Subject::Link(pathlink::PathBuf::from(tc_chain::SYNC_CHAIN).into()),
        collection,
    ))));
    let parameter = |name: &str| Scalar::from(TCRef::Id(IdRef::new(name.parse().unwrap())));
    let insert = Scalar::from(TCRef::Op(OpRef::Put((
        Subject::Ref(
            IdRef::new("self".parse().unwrap()),
            "/data/insert".parse().unwrap(),
        ),
        parameter("key"),
        parameter("value"),
    ))));
    Scalar::Map(
        [
            ("data".parse().unwrap(), chain),
            ("label".parse().unwrap(), Value::from("native").into()),
            (
                "insert".parse().unwrap(),
                Scalar::Op(OpDef::Put((
                    "key".parse().unwrap(),
                    "value".parse().unwrap(),
                    vec![("result".parse().unwrap(), insert.clone())],
                ))),
            ),
            (
                "append".parse().unwrap(),
                Scalar::Op(OpDef::Post(vec![("result".parse().unwrap(), insert)])),
            ),
            (
                "count".parse().unwrap(),
                Scalar::Op(OpDef::Get((
                    "key".parse().unwrap(),
                    vec![(
                        "result".parse().unwrap(),
                        Scalar::from(TCRef::Op(OpRef::Get((
                            Subject::Ref(
                                IdRef::new("self".parse().unwrap()),
                                "/data/count".parse().unwrap(),
                            ),
                            Scalar::default(),
                        )))),
                    )],
                ))),
            ),
        ]
        .into_iter()
        .collect(),
    )
}

// Adapted from v1's TensorChainTests: one Service owns independent dense and
// sparse SyncChains, and method calls mutate those same durable subjects.
pub(crate) fn tensor_definition() -> tc_ir::Scalar {
    use tc_collection::collection::CollectionSchema;
    use tc_collection::tensor::TensorSchema;
    use tc_ir::{IdRef, OpDef, OpRef, Scalar, Subject, TCRef};
    use tc_value::Value;

    let mut members = tc_ir::Map::new();
    for (name, sparse) in [("dense", false), ("sparse", true)] {
        let schema = CollectionSchema::Tensor(
            TensorSchema::new(
                number_general::NumberType::UInt(number_general::UIntType::U64),
                vec![4_097].into(),
                sparse.then_some(0),
            )
            .unwrap(),
        );

        let (class, schema): (pathlink::PathBuf, Value) = schema.into();
        let collection = Scalar::from(TCRef::Op(OpRef::Get((
            Subject::Link(class.into()),
            schema.into(),
        ))));

        let chain = Scalar::from(TCRef::Op(OpRef::Get((
            Subject::Link(pathlink::PathBuf::from(tc_chain::SYNC_CHAIN).into()),
            collection,
        ))));

        members.insert(name.parse().unwrap(), chain);
        let parameter = |name: &str| Scalar::from(TCRef::Id(IdRef::new(name.parse().unwrap())));
        let write = Scalar::from(TCRef::Op(OpRef::Put((
            Subject::Ref(
                IdRef::new("self".parse().unwrap()),
                format!("/{name}").parse().unwrap(),
            ),
            parameter("key"),
            parameter("value"),
        ))));

        members.insert(
            format!("write_{name}").parse().unwrap(),
            Scalar::Op(OpDef::Put((
                "key".parse().unwrap(),
                "value".parse().unwrap(),
                vec![("result".parse().unwrap(), write)],
            ))),
        );

        // Read the other member so the graph's retained source does not hold a
        // read lease on the assignment target. Replicated arguments stay Scalar IR.
        let source_member = if sparse { "dense" } else { "sparse" };
        let source = Scalar::from(TCRef::Op(OpRef::Post((
            Subject::Ref(
                IdRef::new("self".parse().unwrap()),
                format!("/{source_member}/mul").parse().unwrap(),
            ),
            [("r".parse().unwrap(), Value::from(0u64).into())]
                .into_iter()
                .collect(),
        ))));

        let assign = Scalar::from(TCRef::Op(OpRef::Put((
            Subject::Ref(
                IdRef::new("self".parse().unwrap()),
                format!("/{name}").parse().unwrap(),
            ),
            Value::default().into(),
            parameter("source"),
        ))));

        members.insert(
            format!("assign_{name}").parse().unwrap(),
            Scalar::Op(OpDef::Put((
                "key".parse().unwrap(),
                "value".parse().unwrap(),
                vec![
                    ("source".parse().unwrap(), source),
                    ("result".parse().unwrap(), assign),
                ],
            ))),
        );
    }

    Scalar::Map(members)
}
