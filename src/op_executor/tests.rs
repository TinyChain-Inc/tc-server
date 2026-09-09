use super::*;
use number_general::Number;
use tc_ir::{Cond, Map, OpDef, OpRef, Scalar, Subject, TCRef};
use tc_state::{Collection, State, Tensor};
use tc_value::Value;

#[tokio::test]
async fn nested_graph_resolution_inherits_admission() {
    let mut limits = crate::HostLimits::default();
    limits.execution.parallel_graph_ops = 1;
    limits.execution.request_deadline = std::time::Duration::from_millis(50);
    let workspace = crate::txn::test_workspace("nested-graph-admission");
    let kernel = crate::txn::test_kernel_with_limits(
        "nested-graph-admission",
        std::time::Duration::from_secs(3),
        workspace,
        crate::HostResources::new(limits),
    )
    .await;
    let txn = kernel.test_txn().await;
    let _permit = txn
        .resources()
        .admit_graph_op(txn.deadline())
        .await
        .expect("outer graph admission");
    let txn = txn.with_graph_admission();
    let state = resolve_with_admission(
        Scalar::from(Value::from(1_u64)),
        &std::sync::Arc::new(std::collections::HashMap::new()),
        &txn,
        None,
    )
    .await
    .expect("nested resolution must not reacquire the only permit");
    assert!(matches!(
        state,
        State::Scalar(Scalar::Value(Value::Number(number)))
            if number == Number::from(1_u64)
    ));
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
    let txn = crate::txn::test_txn("test-host").await;
    let result = execute_post(&txn, op, Map::new()).await.expect("exec");
    match result {
        State::Scalar(Scalar::Value(Value::Number(n))) => {
            assert_eq!(n, Number::from(1_u64));
        }
        other => panic!("unexpected result {other:?}"),
    }
}

#[tokio::test]
async fn post_resolution_ignores_extra_inputs_and_has_no_namespace_fallback() {
    let op = OpDef::Post(vec![(
        "result".parse().expect("Id"),
        Scalar::Ref(Box::new(TCRef::Id("$input".parse().expect("IdRef")))),
    )]);
    let txn = crate::txn::test_txn("lexical-post-inputs").await;
    let mut params = Map::new();
    params.insert(
        "input".parse().expect("Id"),
        State::Scalar(Scalar::from(Value::from(1_u64))),
    );
    params.insert(
        "extra".parse().expect("Id"),
        State::Scalar(Scalar::from(Value::from(2_u64))),
    );
    let result = execute_post(&txn, op, params).await.expect("resolve input");
    assert!(matches!(
        result,
        State::Scalar(Scalar::Value(Value::Number(number)))
            if number == Number::from(1_u64)
    ));

    let missing = OpDef::Post(vec![(
        "result".parse().expect("Id"),
        Scalar::Ref(Box::new(TCRef::Id("$not_in_frame".parse().expect("IdRef")))),
    )]);
    let error = execute_post(&txn, missing, Map::new())
        .await
        .expect_err("missing lexical input");
    assert!(error.to_string().contains("missing input value"));
}
#[tokio::test]
async fn nested_opdef_cannot_shadow_parent_bindings() {
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
    let txn = crate::txn::test_txn("test-host").await;
    let error = execute_post(&txn, op, Map::new())
        .await
        .expect_err("reject nested shadowing");
    assert!(error.to_string().contains("shadowed OpDef binding $_tmp1"));
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
    let txn = crate::txn::test_txn("test-host").await;
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
