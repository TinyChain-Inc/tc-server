use super::*;
use number_general::Number;
use tc_ir::{Cond, Map, OpDef, OpRef, Scalar, Subject, TCRef};
use tc_state::{Collection, State, Tensor};
use tc_value::Value;

#[tokio::test]
async fn executes_post_opdef_with_id_ref() {
    let form = vec![
        ("a".parse().expect("Id"), Scalar::from(Value::from(1_u64))),
        (
            "b".parse().expect("Id"),
            Scalar::Ref(Box::new(TCRef::Id("$a".parse().expect("IdRef")))),
        ),
    ];
    let op = OpDef::Post(form);

    let txn = crate::txn::TxnManager::with_host_id("test-host").begin();
    let result = execute_post(&txn, op, Map::new()).await.expect("exec");

    match result {
        State::Scalar(Scalar::Value(Value::Number(n))) => {
            assert_eq!(n, Number::from(1_u64));
        }
        other => panic!("unexpected result {other:?}"),
    }
}

#[tokio::test]
async fn nested_cond_opdef_uses_lexical_inputs_not_parent_temps() {
    let branch = OpDef::Post(vec![
        (
            "_tmp1".parse().expect("Id"),
            Scalar::Tuple(vec![
                Scalar::from(Value::from(10_u64)),
                Scalar::from(Value::from(20_u64)),
            ]),
        ),
        (
            "result".parse().expect("Id"),
            Scalar::Ref(Box::new(TCRef::Op(OpRef::Post((
                Subject::Ref(
                    "$_tmp1".parse().expect("IdRef"),
                    "get".parse().expect("Path"),
                ),
                {
                    let mut params = Map::new();
                    params.insert("i".parse().expect("Id"), Scalar::from(Value::from(1_u64)));
                    params
                },
            ))))),
        ),
    ]);

    let form = vec![
        ("cond".parse().expect("Id"), Scalar::from(Value::from(true))),
        (
            "_tmp1".parse().expect("Id"),
            Scalar::from(Value::from(1_u64)),
        ),
        (
            "result".parse().expect("Id"),
            Scalar::Ref(Box::new(TCRef::Cond(Box::new(Cond::new(
                TCRef::Id("$cond".parse().expect("IdRef")),
                Scalar::Op(branch),
                Scalar::from(Value::from(0_u64)),
            ))))),
        ),
    ];
    let op = OpDef::Post(form);

    let txn = crate::txn::TxnManager::with_host_id("test-host").begin();
    let result = execute_post(&txn, op, Map::new()).await.expect("exec");

    match result {
        State::Scalar(Scalar::Value(Value::Number(n))) => {
            assert_eq!(n, Number::from(20_u64));
        }
        other => panic!("unexpected result {other:?}"),
    }
}

#[tokio::test]
async fn executes_tensor_metadata_and_matmul_refs() {
    let form = vec![
        (
            "shape".parse().expect("Id"),
            Scalar::Ref(Box::new(TCRef::Op(OpRef::Post((
                Subject::Ref("$x".parse().expect("IdRef"), "shape".parse().expect("Path")),
                Map::new(),
            ))))),
        ),
        (
            "product".parse().expect("Id"),
            Scalar::Ref(Box::new(TCRef::Op(OpRef::Post((
                Subject::Ref(
                    "$x".parse().expect("IdRef"),
                    "matmul".parse().expect("Path"),
                ),
                {
                    let mut params = Map::new();
                    params.insert(
                        "r".parse().expect("Id"),
                        Scalar::Ref(Box::new(TCRef::Id("$y".parse().expect("IdRef")))),
                    );
                    params
                },
            ))))),
        ),
    ];
    let op = OpDef::Post(form);

    let mut params = Map::new();
    params.insert(
        "x".parse().expect("Id"),
        State::Collection(Collection::Tensor(
            Tensor::dense_u64(vec![2, 2], vec![1, 2, 3, 4]).expect("left tensor"),
        )),
    );
    params.insert(
        "y".parse().expect("Id"),
        State::Collection(Collection::Tensor(
            Tensor::dense_u64(vec![2, 2], vec![5, 6, 7, 8]).expect("right tensor"),
        )),
    );

    let txn = crate::txn::TxnManager::with_host_id("test-host").begin();
    let result = execute_post(&txn, op, params).await.expect("exec");

    match result {
        State::Collection(Collection::Tensor(tensor)) => {
            assert_eq!(tensor.shape(), &[2, 2]);
            assert_eq!(
                tensor.flattened_u64().expect("u64 values"),
                vec![19, 22, 43, 50]
            );
        }
        other => panic!("unexpected result {other:?}"),
    }
}

#[tokio::test]
async fn executes_tensor_view_and_reduction_refs() {
    let form = vec![
        (
            "reshaped".parse().expect("Id"),
            Scalar::Ref(Box::new(TCRef::Op(OpRef::Get((
                Subject::Ref(
                    "$x".parse().expect("IdRef"),
                    "reshape".parse().expect("Path"),
                ),
                Scalar::Tuple(vec![
                    Scalar::Value(Value::Number(Number::from(4_u64))),
                    Scalar::Value(Value::Number(Number::from(1_u64))),
                ]),
            ))))),
        ),
        (
            "result".parse().expect("Id"),
            Scalar::Ref(Box::new(TCRef::Op(OpRef::Post((
                Subject::Ref(
                    "$reshaped".parse().expect("IdRef"),
                    "sum".parse().expect("Path"),
                ),
                Map::new(),
            ))))),
        ),
    ];
    let op = OpDef::Post(form);

    let mut params = Map::new();
    params.insert(
        "x".parse().expect("Id"),
        State::Collection(Collection::Tensor(
            Tensor::dense_u64(vec![2, 2], vec![1, 2, 3, 4]).expect("tensor"),
        )),
    );

    let txn = crate::txn::TxnManager::with_host_id("test-host").begin();
    let result = execute_post(&txn, op, params).await.expect("exec");

    match result {
        State::Scalar(Scalar::Value(Value::Number(n))) => {
            assert_eq!(n, Number::from(10_u64));
        }
        other => panic!("unexpected result {other:?}"),
    }
}
