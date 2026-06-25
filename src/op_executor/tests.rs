use super::*;
use number_general::Number;
use tc_ir::{Cond, Map, OpDef, OpRef, Scalar, Subject, TCRef};
use tc_state::{Collection, NativeClass, State, Tensor, TensorType};
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

#[tokio::test]
async fn executes_tensor_slice_ref() {
    let form = vec![(
        "sliced".parse().expect("Id"),
        Scalar::Ref(Box::new(TCRef::Op(OpRef::Get((
            Subject::Ref("$x".parse().expect("IdRef"), Default::default()),
            Scalar::Tuple(vec![Scalar::Tuple(vec![
                Scalar::Value(Value::Number(Number::from(1_u64))),
                Scalar::Value(Value::Number(Number::from(4_u64))),
            ])]),
        ))))),
    )];
    let op = OpDef::Post(form);

    let mut params = Map::new();
    params.insert(
        "x".parse().expect("Id"),
        State::Collection(Collection::Tensor(
            Tensor::dense_u64(vec![5], vec![10, 20, 30, 40, 50]).expect("tensor"),
        )),
    );

    let txn = crate::txn::TxnManager::with_host_id("test-host").begin();
    let result = execute_post(&txn, op, params).await.expect("exec");

    match result {
        State::Collection(Collection::Tensor(tensor)) => {
            assert_eq!(tensor.shape(), &[3]);
            assert_eq!(tensor.flattened_u64().expect("values"), vec![20, 30, 40]);
        }
        other => panic!("unexpected result {other:?}"),
    }
}

#[tokio::test]
async fn executes_tensor_cast_ref() {
    let form = vec![(
        "casted".parse().expect("Id"),
        Scalar::Ref(Box::new(TCRef::Op(OpRef::Get((
            Subject::Ref("$x".parse().expect("IdRef"), "cast".parse().expect("Path")),
            Scalar::Value(Value::String("u64".to_string())),
        ))))),
    )];
    let op = OpDef::Post(form);

    let mut params = Map::new();
    params.insert(
        "x".parse().expect("Id"),
        State::Collection(Collection::Tensor(
            Tensor::dense_f64(vec![3], vec![1.0, 2.0, 3.0]).expect("tensor"),
        )),
    );

    let txn = crate::txn::TxnManager::with_host_id("test-host").begin();
    let result = execute_post(&txn, op, params).await.expect("exec");

    match result {
        State::Collection(Collection::Tensor(tensor)) => {
            assert_eq!(tensor.dtype_tag(), "u64");
            assert_eq!(tensor.flattened_u64().expect("values"), vec![1, 2, 3]);
        }
        other => panic!("unexpected result {other:?}"),
    }
}

#[tokio::test]
async fn executes_tensor_reduction_axes_keepdims_ref() {
    let form = vec![(
        "reduced".parse().expect("Id"),
        Scalar::Ref(Box::new(TCRef::Op(OpRef::Post((
            Subject::Ref("$x".parse().expect("IdRef"), "sum".parse().expect("Path")),
            {
                let mut params = Map::new();
                params.insert(
                    "axes".parse().expect("Id"),
                    Scalar::Tuple(vec![Scalar::Value(Value::Number(Number::from(1_u64)))]),
                );
                params.insert(
                    "keepdims".parse().expect("Id"),
                    Scalar::Value(Value::Number(Number::from(true))),
                );
                params
            },
        ))))),
    )];
    let op = OpDef::Post(form);

    let mut params = Map::new();
    params.insert(
        "x".parse().expect("Id"),
        State::Collection(Collection::Tensor(
            Tensor::dense_f64(vec![2, 2], vec![1.0, 2.0, 3.0, 4.0]).expect("tensor"),
        )),
    );

    let txn = crate::txn::TxnManager::with_host_id("test-host").begin();
    let result = execute_post(&txn, op, params).await.expect("exec");

    match result {
        State::Collection(Collection::Tensor(tensor)) => {
            assert_eq!(tensor.shape(), &[2, 1]);
            assert_eq!(tensor.flattened_f64().expect("values"), vec![3.0, 7.0]);
        }
        other => panic!("unexpected result {other:?}"),
    }
}

#[test]
fn tensor_dtype_guard_rejects_non_float() {
    let tensor = Tensor::dense_u64(vec![2], vec![1, 2]).expect("tensor");
    let err = tensor_dtype::TensorDtypeGuard::validate(&tensor).expect_err("dtype rejected");
    assert!(err.to_string().contains("TensorOpError::DtypeNotSupported"));
}

#[tokio::test]
async fn decodes_f32_and_f64_tensor_literals_from_wire_form() {
    async fn decode_literal(dtype: &str) -> Tensor {
        let tensor_link: pathlink::Link = TensorType.path().into();
        let form = vec![
            (
                "x".parse().expect("Id"),
                Scalar::Ref(Box::new(TCRef::Op(OpRef::Put((
                    Subject::Link(tensor_link),
                    Scalar::Tuple(vec![
                        Scalar::Value(Value::String(dtype.to_string())),
                        Scalar::Tuple(vec![Scalar::Value(Value::Number(Number::from(2_u64)))]),
                    ]),
                    Scalar::Tuple(vec![
                        Scalar::Value(Value::Number(Number::from(1.5_f64))),
                        Scalar::Value(Value::Number(Number::from(2.5_f64))),
                    ]),
                ))))),
            ),
            (
                "result".parse().expect("Id"),
                Scalar::Ref(Box::new(TCRef::Op(OpRef::Post((
                    Subject::Ref("$x".parse().expect("IdRef"), "add".parse().expect("Path")),
                    {
                        let mut params = Map::new();
                        params.insert(
                            "r".parse().expect("Id"),
                            Scalar::Ref(Box::new(TCRef::Id("$x".parse().expect("IdRef")))),
                        );
                        params
                    },
                ))))),
            ),
        ];

        let txn = crate::txn::TxnManager::with_host_id("test-host").begin();
        let result = execute_post(&txn, OpDef::Post(form), Map::new())
            .await
            .expect("exec");

        match result {
            State::Collection(Collection::Tensor(tensor)) => tensor,
            other => panic!("unexpected result {other:?}"),
        }
    }

    let f32_tensor = decode_literal("f32").await;
    assert_eq!(f32_tensor.dtype_tag(), "f32");
    assert_eq!(f32_tensor.flattened_f32().expect("values"), vec![3.0, 5.0]);

    let f64_tensor = decode_literal("f64").await;
    assert_eq!(f64_tensor.dtype_tag(), "f64");
    assert_eq!(f64_tensor.flattened_f64().expect("values"), vec![3.0, 5.0]);
}

#[test]
fn exact_shape_tensor_add_supports_f32_and_f64() {
    let left = Tensor::dense_f32(vec![2], vec![1.0, 2.0]).expect("left");
    let right = Tensor::dense_f32(vec![2], vec![3.0, 4.0]).expect("right");
    let result = tensor_add::exact_shape_add(&left, &right).expect("add");
    assert_eq!(result.flattened_f32().expect("values"), vec![4.0, 6.0]);

    let left = Tensor::dense_f64(vec![2], vec![1.0, 2.0]).expect("left");
    let right = Tensor::dense_f64(vec![2], vec![3.0, 4.0]).expect("right");
    let result = tensor_add::exact_shape_add(&left, &right).expect("add");
    assert_eq!(result.flattened_f64().expect("values"), vec![4.0, 6.0]);
}

#[test]
fn broadcast_tensor_add_supports_right_aligned_and_leading_dims() {
    let left = Tensor::dense_f64(vec![2, 3], vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]).expect("left");
    let right = Tensor::dense_f64(vec![3], vec![10.0, 20.0, 30.0]).expect("right");
    let result = tensor_add::broadcast_add(&left, &right).expect("add");
    assert_eq!(result.shape(), &[2, 3]);
    assert_eq!(
        result.flattened_f64().expect("values"),
        vec![11.0, 22.0, 33.0, 14.0, 25.0, 36.0]
    );

    let left = Tensor::dense_f64(vec![1, 3], vec![1.0, 2.0, 3.0]).expect("left");
    let right = Tensor::dense_f64(vec![2, 1], vec![10.0, 20.0]).expect("right");
    let result = tensor_add::broadcast_add(&left, &right).expect("add");
    assert_eq!(result.shape(), &[2, 3]);
    assert_eq!(
        result.flattened_f64().expect("values"),
        vec![11.0, 12.0, 13.0, 21.0, 22.0, 23.0]
    );
}

#[test]
fn broadcast_tensor_add_rejects_incompatible_shapes() {
    let left = Tensor::dense_f64(vec![2], vec![1.0, 2.0]).expect("left");
    let right = Tensor::dense_f64(vec![3], vec![1.0, 2.0, 3.0]).expect("right");
    let err = tensor_add::broadcast_add(&left, &right).expect_err("shape error");
    assert!(
        err.to_string()
            .contains("TensorOpError::BroadcastIncompatible")
    );
}

#[test]
fn broadcast_reduce_sums_to_target_shape() {
    let input = Tensor::dense_f64(vec![2, 3], vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]).expect("input");
    let result = broadcast_reduce::broadcast_reduce_sum(&input, &[2, 3]).expect("reduce");
    assert_eq!(result.shape(), &[2, 3]);
    assert_eq!(
        result.flattened_f64().expect("values"),
        vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    );
}

#[test]
fn broadcast_reduce_sums_leading_and_singleton_axes() {
    let input = Tensor::dense_f64(vec![2, 3], vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]).expect("input");
    let result = broadcast_reduce::broadcast_reduce_sum(&input, &[3]).expect("reduce");
    assert_eq!(result.shape(), &[3]);
    assert_eq!(result.flattened_f64().expect("values"), vec![5.0, 7.0, 9.0]);

    let input = Tensor::dense_f64(vec![2, 3], vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]).expect("input");
    let result = broadcast_reduce::broadcast_reduce_sum(&input, &[2, 1]).expect("reduce");
    assert_eq!(result.shape(), &[2, 1]);
    assert_eq!(result.flattened_f64().expect("values"), vec![6.0, 15.0]);
}

#[tokio::test]
async fn opdef_post_broadcast_reduce_via_form() {
    let form = vec![(
        "result".parse().expect("Id"),
        Scalar::Ref(Box::new(TCRef::Op(OpRef::Post((
            Subject::Ref(
                "$x".parse().expect("IdRef"),
                "broadcast_reduce".parse().expect("Path"),
            ),
            {
                let mut params = Map::new();
                params.insert(
                    "target_shape".parse().expect("Id"),
                    Scalar::Tuple(vec![
                        Scalar::Value(Value::Number(Number::from(1_u64))),
                        Scalar::Value(Value::Number(Number::from(3_u64))),
                    ]),
                );
                params
            },
        ))))),
    )];
    let op = OpDef::Post(form);

    let mut params = Map::new();
    params.insert(
        "x".parse().expect("Id"),
        State::Collection(Collection::Tensor(
            Tensor::dense_f32(vec![2, 3], vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]).expect("x"),
        )),
    );

    let txn = crate::txn::TxnManager::with_host_id("test-host").begin();
    let result = execute_post(&txn, op, params).await.expect("exec");

    match result {
        State::Collection(Collection::Tensor(tensor)) => {
            assert_eq!(tensor.shape(), &[1, 3]);
            assert_eq!(tensor.flattened_f32().expect("values"), vec![5.0, 7.0, 9.0]);
        }
        other => panic!("unexpected result {other:?}"),
    }
}

#[tokio::test]
async fn opdef_post_add_via_form() {
    let form = vec![(
        "result".parse().expect("Id"),
        Scalar::Ref(Box::new(TCRef::Op(OpRef::Post((
            Subject::Ref("$x".parse().expect("IdRef"), "add".parse().expect("Path")),
            {
                let mut params = Map::new();
                params.insert(
                    "r".parse().expect("Id"),
                    Scalar::Ref(Box::new(TCRef::Id("$y".parse().expect("IdRef")))),
                );
                params
            },
        ))))),
    )];
    let op = OpDef::Post(form);

    let mut params = Map::new();
    params.insert(
        "x".parse().expect("Id"),
        State::Collection(Collection::Tensor(
            Tensor::dense_f32(vec![2], vec![1.0, 2.0]).expect("x"),
        )),
    );
    params.insert(
        "y".parse().expect("Id"),
        State::Collection(Collection::Tensor(
            Tensor::dense_f32(vec![2], vec![3.0, 4.0]).expect("y"),
        )),
    );

    let txn = crate::txn::TxnManager::with_host_id("test-host").begin();
    let result = execute_post(&txn, op, params).await.expect("exec");

    match result {
        State::Collection(Collection::Tensor(tensor)) => {
            assert_eq!(tensor.shape(), &[2]);
            assert_eq!(tensor.flattened_f32().expect("values"), vec![4.0, 6.0]);
        }
        other => panic!("unexpected result {other:?}"),
    }
}
