use number_general::{FloatType, Number, NumberType};
use tc_ir::{Cond, Map, OpDef, OpRef, Scalar, Subject, TCRef};
use tc_state::{Collection, NativeClass, State, Tensor, TensorType};
use tc_value::{Value, number_type_path};

use super::*;

#[test]
fn executor_and_codec_do_not_dispatch_concrete_collections() {
    for source in [
        include_str!("mod.rs"),
        include_str!("resolve/mod.rs"),
        include_str!("../http/codec.rs"),
    ] {
        assert!(!source.contains("Collection::BTree"));
        assert!(!source.contains("Collection::Table"));
        assert!(!source.contains("Collection::Tensor"));
        assert!(!source.contains("btree_json_stream"));
    }

    let pyo3_adapter = concat!(
        include_str!("../pyo3_runtime/kernel.rs"),
        include_str!("../pyo3_runtime/state_handle_conversions.rs"),
        include_str!("../pyo3_runtime/wire.rs"),
    );
    for collection in ["Collection::", "BTree", "Table", "Tensor"] {
        assert!(!pyo3_adapter.contains(collection));
    }
    assert!(!pyo3_adapter.contains("block_on"));

    for adapter in [
        include_str!("../http/server.rs"),
        include_str!("../wasm/http.rs"),
        include_str!("../pyo3_runtime/types.rs"),
    ] {
        assert!(!adapter.contains("Collection::Table"));
    }

    assert!(include_str!("collection.rs").contains("Public::put"));
    assert!(include_str!("resolve/mod.rs").contains("collection::put"));
}

#[test]
fn production_adapters_do_not_block_on_async_work() {
    for source in [
        include_str!("../pyo3_runtime/kernel.rs"),
        include_str!("../pyo3_runtime/types.rs"),
        include_str!("../pyo3_runtime/state_handle_conversions.rs"),
        include_str!("../http_client.rs"),
        include_str!("../../../tc-wasm/src/abi.rs"),
    ] {
        let production = source.split("#[cfg(test)]").next().expect("source");
        assert!(
            !production.contains("block_on"),
            "production adapters must not block on async work"
        );
    }
}

#[test]
fn native_contracts_have_one_owner_and_one_representation() {
    let kernel_types = include_str!("../kernel/types.rs");
    assert!(!kernel_types.contains("enum Method"));
    assert!(include_str!("../kernel/mod.rs").contains("pub use tc_ir::Method"));

    let resolver = include_str!("../resolve.rs");
    assert!(!resolver.contains("trait Resolve"));

    let gateway = include_str!("../gateway.rs");
    assert!(!gateway.contains("key: Value"));
    assert!(gateway.contains("key: Scalar"));

    let http_client = include_str!("../http_client.rs");
    assert!(!http_client.contains("outbound_http::DEFAULT_TIMEOUT"));
    assert!(http_client.contains("txn.deadline()"));

    let http_server = include_str!("../http/server.rs");
    assert!(!http_server.contains("timeout_at"));

    let pyo_types = include_str!("../pyo3_runtime/types.rs");
    assert!(!pyo_types.contains("super::runtime()"));
    assert!(!include_str!("../pyo3_runtime/kernel.rs").contains("wait_on_tokio"));
}

#[test]
fn native_routes_do_not_depend_on_serialization_or_result_envelopes() {
    for source in [
        include_str!("../kernel/kernel.rs"),
        include_str!("../kernel/resolver.rs"),
        include_str!("../state.rs"),
        include_str!("../reflect.rs"),
        include_str!("collection.rs"),
    ] {
        for forbidden in [
            "destream_json",
            "CollectionResponse",
            "KernelResponse",
            "RouteResponse",
            "HandleGet",
            "HandlePut",
            "HandlePost",
            "HandleDelete",
            "hyper::",
        ] {
            assert!(
                !source.contains(forbidden),
                "native routing must not depend on {forbidden}"
            );
        }
    }

    let ir = include_str!("../ir/mod.rs");
    let execution = ir
        .split_once("impl Handler<State> for IrHandler")
        .expect("IR handler")
        .1
        .split_once("pub async fn compile_ir_library")
        .expect("IR compiler")
        .0;
    assert!(!execution.contains("destream_json"));
    assert!(!execution.contains("IntoView"));

    for source in [
        include_str!("../kernel/types.rs"),
        include_str!("../library/mod.rs"),
        ir,
    ] {
        assert!(!source.contains("NativeKernelHandler"));
    }

    let codec = include_str!("../http/codec.rs");
    for forbidden in [
        "KernelRequest",
        "resolve_native",
        "OpDef",
        "LibraryRegistry",
    ] {
        assert!(
            !codec.contains(forbidden),
            "HTTP codec must not know {forbidden}"
        );
    }
}

#[test]
fn only_the_kernel_issues_production_transaction_contexts() {
    for source in [
        include_str!("collection.rs"),
        include_str!("execute.rs"),
        include_str!("resolve/mod.rs"),
        include_str!("../http/codec.rs"),
        include_str!("../http/parse.rs"),
        include_str!("../http_client.rs"),
        include_str!("../wasm/http.rs"),
        include_str!("../kernel/resolver.rs"),
        include_str!("../pyo3_runtime/kernel.rs"),
        include_str!("../pyo3_runtime/state_handle_conversions.rs"),
        include_str!("../pyo3_runtime/wire.rs"),
    ] {
        for forbidden in [
            "null_transaction",
            "NullTransaction",
            "impl Transaction for",
        ] {
            assert!(
                !source.contains(forbidden),
                "only tc-server::txn::TxnHandle may create a production transaction context; found {forbidden}"
            );
        }
    }

    assert!(include_str!("../txn/handle.rs").contains("impl Transaction for TxnHandle"));
}

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

    let txn = crate::txn::test_txn("test-host");
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

    let txn = crate::txn::test_txn("test-host");
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

    let txn = crate::txn::test_txn("test-host");
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

    let txn = crate::txn::test_txn("test-host");
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

    let txn = crate::txn::test_txn("test-host");
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

    let txn = crate::txn::test_txn("test-host");
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

    let txn = crate::txn::test_txn("test-host");
    let result = execute_post(&txn, op, params).await.expect("exec");

    match result {
        State::Collection(Collection::Tensor(tensor)) => {
            assert_eq!(tensor.shape(), &[2, 1]);
            assert_eq!(tensor.flattened_f64().expect("values"), vec![3.0, 7.0]);
        }
        other => panic!("unexpected result {other:?}"),
    }
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

        let txn = crate::txn::test_txn("test-host");
        let result = execute_post(&txn, OpDef::Post(form), Map::new())
            .await
            .expect("exec");

        match result {
            State::Collection(Collection::Tensor(tensor)) => tensor,
            other => panic!("unexpected result {other:?}"),
        }
    }

    let f32_tensor =
        decode_literal(&number_type_path(&NumberType::Float(FloatType::F32)).to_string()).await;
    assert_eq!(f32_tensor.dtype_tag(), "f32");
    assert_eq!(f32_tensor.flattened_f32().expect("values"), vec![3.0, 5.0]);

    let f64_tensor =
        decode_literal(&number_type_path(&NumberType::Float(FloatType::F64)).to_string()).await;
    assert_eq!(f64_tensor.dtype_tag(), "f64");
    assert_eq!(f64_tensor.flattened_f64().expect("values"), vec![3.0, 5.0]);
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

    let txn = crate::txn::test_txn("test-host");
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

    let txn = crate::txn::test_txn("test-host");
    let result = execute_post(&txn, op, params).await.expect("exec");

    match result {
        State::Collection(Collection::Tensor(tensor)) => {
            assert_eq!(tensor.shape(), &[2]);
            assert_eq!(tensor.flattened_f32().expect("values"), vec![4.0, 6.0]);
        }
        other => panic!("unexpected result {other:?}"),
    }
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

    let txn = crate::txn::test_txn("test-host");
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

    let txn = crate::txn::test_txn("test-host");
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
