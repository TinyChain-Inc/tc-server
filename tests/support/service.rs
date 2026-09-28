pub(crate) fn definition(table: bool) -> tc_ir::Scalar {
    use tc_collection::{btree::BTreeColumnSchema, collection::CollectionSchema};
    use tc_ir::{IdRef, OpDef, OpRef, Scalar, Subject, TCRef};
    use tc_value::Value;

    let dtype = Value::from(0_u64).class();
    let schema = if table {
        use tc_collection::{
            btree::StorageConfig,
            table::{Column, TableSchema},
        };
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
        CollectionSchema::BTree(vec![BTreeColumnSchema {
            name: "key".into(),
            dtype,
            max_size: None,
        }])
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
