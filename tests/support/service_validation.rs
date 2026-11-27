use tc_ir::{Scalar, TCRef};

use super::*;

#[test]
fn table_layout_validation_is_delegated_before_service_allocation() {
    use safecast::CastFrom;

    let column = || {
        Value::Tuple(vec![
            "id".into(),
            Value::cast_from(tc_value::ValueType::String),
        ])
    };

    let schema = Value::Tuple(vec![
        Value::Tuple(vec![
            Value::Tuple(vec![column()]),
            Value::Tuple(vec![column()]),
        ]),
        Value::None,
    ]);

    let collection = Scalar::from(TCRef::Op(OpRef::Get((
        Subject::Link("/state/collection/table".parse().unwrap()),
        schema.into(),
    ))));

    let chain = Scalar::from(TCRef::Op(OpRef::Get((
        Subject::Link("/state/chain/sync".parse().unwrap()),
        collection,
    ))));

    let definition = Scalar::Map([("data".parse().unwrap(), chain)].into_iter().collect());
    let error = validate(&"/service/test/native/1.0.0".parse().unwrap(), &definition)
        .err()
        .expect("duplicate Table columns must reject during preparation");

    assert_eq!(error.code(), tc_error::ErrorKind::BadRequest);
}

#[test]
fn declarations_and_replicated_methods_fail_before_storage_allocation() {
    let identity = "/service/test/native/1.0.0".parse().unwrap();
    let definition = |value| Scalar::Map([("data".parse().unwrap(), value)].into_iter().collect());
    let get = |path: &str, key| {
        Scalar::from(TCRef::Op(OpRef::Get((
            Subject::Link(path.parse().unwrap()),
            key,
        ))))
    };

    for value in [
        get("/state/chain/block", Scalar::default()),
        get(
            "/state/chain/sync",
            get("/state/collection/tensor", Scalar::default()),
        ),
        get(
            "/state/chain/sync",
            get(
                "/state/collection/btree",
                Value::from("invalid schema").into(),
            ),
        ),
        Scalar::Tuple(vec![get("/service/test/other/1.0.0", Scalar::default())]),
    ] {
        assert!(validate(&identity, &definition(value)).is_err());
    }

    let external = Scalar::from(OpDef::Put((
        "key".parse().unwrap(),
        "value".parse().unwrap(),
        vec![(
            "write".parse().unwrap(),
            Scalar::from(TCRef::Op(OpRef::Put((
                Subject::Link("/service/test/other/1.0.0/data".parse().unwrap()),
                Scalar::default(),
                Scalar::default(),
            )))),
        )],
    )));

    assert!(validate(&identity, &definition(external)).is_err());
    assert!(validate(&identity, &definition(Value::from("literal").into())).is_ok());
}
