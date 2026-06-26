use super::tensor_matmul::batched_matmul;
use super::tensor_transpose::{tensor_transpose, transpose_output_shape};
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
            Tensor::dense_f64(vec![2, 2], vec![1.0, 2.0, 3.0, 4.0]).expect("left tensor"),
        )),
    );
    params.insert(
        "y".parse().expect("Id"),
        State::Collection(Collection::Tensor(
            Tensor::dense_f64(vec![2, 2], vec![5.0, 6.0, 7.0, 8.0]).expect("right tensor"),
        )),
    );

    let txn = crate::txn::TxnManager::with_host_id("test-host").begin();
    let result = execute_post(&txn, op, params).await.expect("exec");

    match result {
        State::Collection(Collection::Tensor(tensor)) => {
            assert_eq!(tensor.shape(), &[2, 2]);
            assert_eq!(
                tensor.flattened_f64().expect("f64 values"),
                vec![19.0, 22.0, 43.0, 50.0]
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

#[test]
fn batched_matmul_rank2_square() {
    let left = Tensor::dense_f64(vec![2, 2], vec![1.0, 2.0, 3.0, 4.0]).expect("left");
    let right = Tensor::dense_f64(vec![2, 2], vec![5.0, 6.0, 7.0, 8.0]).expect("right");
    let result = batched_matmul(&left, &right).expect("matmul");
    assert_eq!(result.shape(), &[2, 2]);
    assert_eq!(
        result.flattened_f64().expect("values"),
        vec![19.0, 22.0, 43.0, 50.0]
    );
}

#[test]
fn batched_matmul_rank2_non_square() {
    // A: 2×3, B: 3×2 → C: 2×2
    let left = Tensor::dense_f64(vec![2, 3], vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]).expect("left");
    let right =
        Tensor::dense_f64(vec![3, 2], vec![7.0, 8.0, 9.0, 10.0, 11.0, 12.0]).expect("right");
    let result = batched_matmul(&left, &right).expect("matmul");
    assert_eq!(result.shape(), &[2, 2]);
    // C[0,0]=1*7+2*9+3*11=58, C[0,1]=1*8+2*10+3*12=64
    // C[1,0]=4*7+5*9+6*11=139, C[1,1]=4*8+5*10+6*12=154
    assert_eq!(
        result.flattened_f64().expect("values"),
        vec![58.0, 64.0, 139.0, 154.0]
    );
}

#[test]
fn batched_matmul_rank4_batched() {
    // A: [2, 1, 2, 2], B: [2, 1, 2, 2]
    let mat = vec![1.0_f64, 2.0, 3.0, 4.0];
    let left = Tensor::dense_f64(
        vec![2, 1, 2, 2],
        mat.iter().chain(mat.iter()).copied().collect(),
    )
    .expect("left");
    let right = Tensor::dense_f64(
        vec![2, 1, 2, 2],
        mat.iter().chain(mat.iter()).copied().collect(),
    )
    .expect("right");
    let result = batched_matmul(&left, &right).expect("matmul");
    assert_eq!(result.shape(), &[2, 1, 2, 2]);
    // [1,2;3,4] * [1,2;3,4] = [7,10;15,22] for both batches
    let expected = vec![7.0, 10.0, 15.0, 22.0, 7.0, 10.0, 15.0, 22.0];
    assert_eq!(result.flattened_f64().expect("values"), expected);
}

#[test]
fn batched_matmul_batch_broadcast() {
    // A: [2, 1, 2, 2] — 2 outer batches, 1 inner (broadcast)
    // B: [1, 3, 2, 2] — 1 outer (broadcast), 3 inner batches
    // result: [2, 3, 2, 2]
    // A batches: [[1,0;0,1], [2,0;0,2]] (identity and 2×identity)
    let a_vals = vec![
        1.0_f64, 0.0, 0.0, 1.0, // batch (0,0): identity
        2.0, 0.0, 0.0, 2.0, // batch (1,0): 2*identity
    ];
    // B batches: [[1,2;3,4], [5,6;7,8], [9,10;11,12]]
    let b_vals = vec![
        1.0_f64, 2.0, 3.0, 4.0, // batch (0,0)
        5.0, 6.0, 7.0, 8.0, // batch (0,1)
        9.0, 10.0, 11.0, 12.0, // batch (0,2)
    ];
    let left = Tensor::dense_f64(vec![2, 1, 2, 2], a_vals).expect("left");
    let right = Tensor::dense_f64(vec![1, 3, 2, 2], b_vals).expect("right");
    let result = batched_matmul(&left, &right).expect("matmul");
    assert_eq!(result.shape(), &[2, 3, 2, 2]);
    // Row 0 (outer=0, inner=0,1,2): I@B[0], I@B[1], I@B[2] → same as B batches
    // Row 1 (outer=1, inner=0,1,2): 2I@B[0], 2I@B[1], 2I@B[2]
    let expected = vec![
        1.0, 2.0, 3.0, 4.0, // (0,0)
        5.0, 6.0, 7.0, 8.0, // (0,1)
        9.0, 10.0, 11.0, 12.0, // (0,2)
        2.0, 4.0, 6.0, 8.0, // (1,0)
        10.0, 12.0, 14.0, 16.0, // (1,1)
        18.0, 20.0, 22.0, 24.0, // (1,2)
    ];
    assert_eq!(result.flattened_f64().expect("values"), expected);
}

#[test]
fn batched_matmul_rejects_incompatible_inner_dims() {
    // A: [2, 3], B: [4, 5] — A.shape[-1]=3, B.shape[-2]=4 mismatch
    let left = Tensor::dense_f64(vec![2, 3], vec![1.0; 6]).expect("left");
    let right = Tensor::dense_f64(vec![4, 5], vec![1.0; 20]).expect("right");
    let err = batched_matmul(&left, &right).expect_err("shape mismatch");
    assert!(
        err.to_string()
            .contains("TensorOpError::MatmulShapeMismatch"),
        "unexpected error: {err}"
    );
}

#[test]
fn batched_matmul_rejects_incompatible_batch_dims() {
    // A: [2, 2, 3], B: [3, 3, 4] — batch [2] vs [3] incompatible
    let left = Tensor::dense_f64(vec![2, 2, 3], vec![1.0; 12]).expect("left");
    let right = Tensor::dense_f64(vec![3, 3, 4], vec![1.0; 36]).expect("right");
    let err = batched_matmul(&left, &right).expect_err("batch mismatch");
    assert!(
        err.to_string()
            .contains("TensorOpError::BroadcastShapeMismatch"),
        "unexpected error: {err}"
    );
}

#[test]
fn batched_matmul_rejects_rank_below_2() {
    let vec1d = Tensor::dense_f64(vec![3], vec![1.0, 2.0, 3.0]).expect("1d");
    let mat2d = Tensor::dense_f64(vec![3, 2], vec![1.0; 6]).expect("2d");
    let err = batched_matmul(&vec1d, &mat2d).expect_err("rank error");
    assert!(
        err.to_string().contains("TensorOpError::InvalidRank"),
        "unexpected error: {err}"
    );
    let err2 = batched_matmul(&mat2d, &vec1d).expect_err("rank error right");
    assert!(
        err2.to_string().contains("TensorOpError::InvalidRank"),
        "unexpected error: {err2}"
    );
}

#[test]
fn tensor_transpose_permutates_2d_tensor() {
    let tensor = Tensor::dense_f64(vec![2, 3], vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0]).expect("tensor");

    let result = tensor_transpose(&tensor, &[1, 0]).expect("transpose");

    assert_eq!(result.shape(), &[3, 2]);
    assert_eq!(
        result.flattened_f64().expect("values"),
        vec![0.0, 3.0, 1.0, 4.0, 2.0, 5.0]
    );
}

#[test]
fn tensor_transpose_permutates_rank4_tensor() {
    let tensor = Tensor::dense_f64(
        vec![2, 3, 2, 2],
        (0..24).map(|value| value as f64).collect(),
    )
    .expect("tensor");

    let result = tensor_transpose(&tensor, &[1, 0, 3, 2]).expect("transpose");

    assert_eq!(result.shape(), &[3, 2, 2, 2]);
    assert_eq!(
        result.flattened_f64().expect("values"),
        vec![
            0.0, 2.0, 1.0, 3.0, 12.0, 14.0, 13.0, 15.0, 4.0, 6.0, 5.0, 7.0, 16.0, 18.0, 17.0, 19.0,
            8.0, 10.0, 9.0, 11.0, 20.0, 22.0, 21.0, 23.0,
        ]
    );
}

#[test]
fn tensor_transpose_identity_preserves_shape_values_and_dtype() {
    let tensor = Tensor::dense_f32(vec![2, 2], vec![1.0, 2.0, 3.0, 4.0]).expect("tensor");

    let result = tensor_transpose(&tensor, &[0, 1]).expect("transpose");

    assert_eq!(result.dtype_tag(), "f32");
    assert_eq!(result.shape(), &[2, 2]);
    assert_eq!(
        result.flattened_f32().expect("values"),
        vec![1.0, 2.0, 3.0, 4.0]
    );
}

#[test]
fn tensor_transpose_computes_output_shape() {
    let shape = transpose_output_shape(&[2, 3, 4], &[2, 0, 1]).expect("shape");

    assert_eq!(shape, vec![4, 2, 3]);
}

#[test]
fn tensor_transpose_rejects_invalid_permutations() {
    let tensor = Tensor::dense_f64(vec![2, 3], vec![1.0; 6]).expect("tensor");

    for permutation in [vec![0], vec![0, 0], vec![0, 2]] {
        let err = tensor_transpose(&tensor, &permutation).expect_err("invalid permutation");
        assert!(
            err.to_string()
                .contains("TensorOpError::InvalidPermutation"),
            "unexpected error for {permutation:?}: {err}"
        );
    }
}

#[test]
fn tensor_transpose_rejects_non_float_dtype() {
    let tensor = Tensor::dense_u64(vec![2, 2], vec![1, 2, 3, 4]).expect("tensor");

    let err = tensor_transpose(&tensor, &[1, 0]).expect_err("dtype rejected");

    assert!(
        err.to_string().contains("TensorOpError::DtypeNotSupported"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn opdef_get_transpose_via_form() {
    let form = vec![(
        "result".parse().expect("Id"),
        Scalar::Ref(Box::new(TCRef::Op(OpRef::Get((
            Subject::Ref(
                "$x".parse().expect("IdRef"),
                "transpose".parse().expect("Path"),
            ),
            Scalar::Tuple(vec![
                Scalar::Value(Value::Number(Number::from(1_u64))),
                Scalar::Value(Value::Number(Number::from(0_u64))),
            ]),
        ))))),
    )];
    let op = OpDef::Post(form);

    let mut params = Map::new();
    params.insert(
        "x".parse().expect("Id"),
        State::Collection(Collection::Tensor(
            Tensor::dense_f32(vec![2, 3], vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0]).expect("x"),
        )),
    );

    let txn = crate::txn::TxnManager::with_host_id("test-host").begin();
    let result = execute_post(&txn, op, params).await.expect("exec");

    match result {
        State::Collection(Collection::Tensor(tensor)) => {
            assert_eq!(tensor.dtype_tag(), "f32");
            assert_eq!(tensor.shape(), &[3, 2]);
            assert_eq!(
                tensor.flattened_f32().expect("values"),
                vec![0.0, 3.0, 1.0, 4.0, 2.0, 5.0]
            );
        }
        other => panic!("unexpected result {other:?}"),
    }
}

#[tokio::test]
async fn opdef_post_transpose_via_form() {
    let form = vec![(
        "result".parse().expect("Id"),
        Scalar::Ref(Box::new(TCRef::Op(OpRef::Post((
            Subject::Ref(
                "$x".parse().expect("IdRef"),
                "transpose".parse().expect("Path"),
            ),
            {
                let mut params = Map::new();
                params.insert(
                    "perm".parse().expect("Id"),
                    Scalar::Tuple(vec![
                        Scalar::Value(Value::Number(Number::from(1_u64))),
                        Scalar::Value(Value::Number(Number::from(0_u64))),
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
            Tensor::dense_f64(vec![2, 3], vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0]).expect("x"),
        )),
    );

    let txn = crate::txn::TxnManager::with_host_id("test-host").begin();
    let result = execute_post(&txn, op, params).await.expect("exec");

    match result {
        State::Collection(Collection::Tensor(tensor)) => {
            assert_eq!(tensor.shape(), &[3, 2]);
            assert_eq!(
                tensor.flattened_f64().expect("values"),
                vec![0.0, 3.0, 1.0, 4.0, 2.0, 5.0]
            );
        }
        other => panic!("unexpected result {other:?}"),
    }
}
