use futures::TryStreamExt;
use number_general::Number;
use safecast::{CastInto, TryCastFrom};
use tc_ir::{Cond, Map, OpDef, OpRef, Scalar, Subject, TCRef};
use tc_state::{Collection, OpExecutor, State, Tensor};
use tc_value::Value;

use super::*;

async fn test_kernel(name: &str, limits: crate::HostLimits) -> crate::Kernel {
    crate::txn::test_kernel_with_limits(
        name,
        crate::txn::test_workspace(name),
        limits,
    )
    .await
}

#[tokio::test]
async fn nested_graph_resolution_inherits_admission() {
    let mut limits = crate::HostLimits::default();
    limits.execution.parallel_graph_ops = 1;
    limits.execution.request_deadline = std::time::Duration::from_millis(50);
    let kernel = test_kernel("nested-graph-admission", limits).await;
    let txn = kernel.test_txn().await;
    let _permit = txn.admit_graph_op().await.expect("outer graph admission");
    let txn = txn.with_graph_admission();
    let state = resolve_with_admission(
        Scalar::from(1_u64),
        &std::collections::HashMap::new(),
        &txn,
        None,
    )
    .await
    .expect("nested resolution must not reacquire the only permit");

    assert!(matches!(
        &state,
        State::Scalar(scalar)
            if matches!(&scalar, Scalar::Value(value)
                if matches!(&value, Value::Number(number) if *number == Number::from(1_u64)))
    ));
}

#[tokio::test]
async fn request_budget_is_shared_by_nested_handle_clones() {
    let mut limits = crate::HostLimits::default();
    limits.execution.max_op_invocations = 1;
    let kernel = test_kernel("operation-budget", limits).await;
    let txn = kernel.test_txn().await;
    let op = || OpDef::Post(vec![("result".parse().unwrap(), Scalar::from(1_u64))]);
    txn.execute_op(op(), State::Map(Map::new()), None, None)
        .await
        .expect("first invocation");

    let error = txn
        .clone()
        .execute_op(op(), State::Map(Map::new()), None, None)
        .await
        .expect_err("shared budget exhausted");

    assert_eq!(error.code(), tc_error::ErrorKind::Unavailable);
}

#[tokio::test]
async fn graph_shape_limits_are_checked_before_provider_execution() {
    let mut limits = crate::HostLimits::default();
    limits.execution.max_graph_nodes = 1;
    let kernel = test_kernel("graph-shape-limit", limits).await;
    let txn = kernel.test_txn().await;
    let op = OpDef::Post(vec![
        ("first".parse().unwrap(), Scalar::from(1_u64)),
        (
            "result".parse().unwrap(),
            Scalar::from(TCRef::Id("$first".parse().unwrap())),
        ),
    ]);

    let error = txn
        .execute_op(op, State::Map(Map::new()), None, None)
        .await
        .expect_err("node limit");

    assert_eq!(error.code(), tc_error::ErrorKind::BadRequest);
    assert!(error.to_string().contains("provider limit"));
}

#[tokio::test]
async fn nested_depth_and_control_flow_share_the_request_budget() {
    let mut limits = crate::HostLimits::default();
    limits.execution.max_execution_depth = 1;
    let kernel = test_kernel("operation-depth", limits).await;
    let txn = kernel.test_txn().await;
    let nested = OpDef::Post(vec![(
        "nested_result".parse().unwrap(),
        Scalar::from(1_u64),
    )]);

    let outer = OpDef::Post(vec![(
        "result".parse().unwrap(),
        Scalar::from(TCRef::Cond(Box::new(Cond::new(
            TCRef::Id("$condition".parse().unwrap()),
            Scalar::from(nested),
            Scalar::from(0_u64),
        )))),
    )]);

    let mut args = Map::new();
    args.insert("condition".parse().unwrap(), State::from(Value::from(true)));
    let error = txn
        .execute_op(outer, State::from(args), None, None)
        .await
        .expect_err("nested depth limit");

    assert_eq!(error.code(), tc_error::ErrorKind::Unavailable);
    assert!(error.to_string().contains("nesting"));

    let mut limits = crate::HostLimits::default();
    limits.execution.max_op_invocations = 4;
    let kernel = test_kernel("while-budget", limits).await;
    let txn = kernel.test_txn().await;
    let cond = OpDef::Post(vec![(
        "continue".parse().unwrap(),
        Scalar::from(Value::from(true)),
    )]);

    let closure = OpDef::Post(vec![(
        "next".parse().unwrap(),
        Scalar::from(TCRef::Id("$state".parse().unwrap())),
    )]);

    let looping = OpDef::Post(vec![(
        "result".parse().unwrap(),
        Scalar::from(TCRef::While(Box::new(tc_ir::While::new(
            Scalar::from(cond),
            Scalar::from(closure),
            Scalar::from(0_u64),
        )))),
    )]);

    let error = txn
        .execute_op(looping, State::Map(Map::new()), None, None)
        .await
        .expect_err("While consumes the shared operation budget");

    assert_eq!(error.code(), tc_error::ErrorKind::Unavailable);
}

#[tokio::test]
async fn one_executor_accepts_each_opdef_argument_shape() {
    let txn = crate::txn::test_txn("opdef-argument-shapes").await;
    let key = Scalar::from(7_u64);
    let form = |reference: &str| {
        vec![(
            "result".parse().unwrap(),
            Scalar::from(TCRef::Id(reference.parse().unwrap())),
        )]
    };

    let get = OpDef::Get(("key".parse().unwrap(), form("$key")));
    execute(&txn, get, State::from(key.clone()), None)
        .await
        .expect("GET execution");

    let put = OpDef::Put((
        "key".parse().unwrap(),
        "value".parse().unwrap(),
        form("$value"),
    ));

    execute(
        &txn,
        put,
        State::from(vec![State::from(key.clone()), State::from(8_u64)]),
        None,
    )
    .await
    .expect("PUT execution");

    let post = OpDef::Post(form("$input"));
    execute(
        &txn,
        post,
        State::from(Map::from_iter([(
            "input".parse().unwrap(),
            State::from(9_u64),
        )])),
        None,
    )
    .await
    .expect("POST execution");

    let delete = OpDef::Delete(("key".parse().unwrap(), form("$key")));
    execute(&txn, delete, State::from(key), None)
        .await
        .expect("DELETE execution");
}

#[tokio::test]
async fn one_executor_rejects_malformed_opdef_arguments() {
    let txn = crate::txn::test_txn("malformed-opdef-arguments").await;
    let form = || vec![("result".parse().unwrap(), Scalar::from(1_u64))];
    let tensor = Tensor::dense_f64(&txn, vec![1], vec![1.0]).await.unwrap();
    let nonscalar = || State::from(Collection::Tensor(tensor.clone()));

    let cases = [
        (
            OpDef::Get(("key".parse().unwrap(), form())),
            nonscalar(),
            "GET and DELETE OpDefs expect a scalar key",
        ),
        (
            OpDef::Put(("key".parse().unwrap(), "value".parse().unwrap(), form())),
            State::from(vec![State::from(1_u64)]),
            "PUT OpDef expects [key, value]",
        ),
        (
            OpDef::Post(form()),
            State::from(1_u64),
            "POST OpDef expects a parameter map",
        ),
        (
            OpDef::Delete(("key".parse().unwrap(), form())),
            nonscalar(),
            "GET and DELETE OpDefs expect a scalar key",
        ),
    ];

    for (definition, args, expected) in cases {
        let error = execute(&txn, definition, args, None)
            .await
            .expect_err("malformed arguments");

        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[tokio::test]
async fn post_resolution_ignores_extra_inputs_and_has_no_namespace_fallback() {
    let op = OpDef::Post(vec![(
        "result".parse().expect("Id"),
        Scalar::from(TCRef::Id("$input".parse().expect("IdRef"))),
    )]);

    let txn = crate::txn::test_txn("lexical-post-inputs").await;
    let mut params = Map::new();
    params.insert("input".parse().expect("Id"), State::from(1_u64));
    params.insert("extra".parse().expect("Id"), State::from(2_u64));
    let result = execute(&txn, op, State::from(params), None)
        .await
        .expect("resolve input");

    assert!(matches!(
        &result,
        State::Scalar(scalar)
            if matches!(&scalar, Scalar::Value(value)
                if matches!(&value, Value::Number(number) if *number == Number::from(1_u64)))
    ));

    let missing = OpDef::Post(vec![(
        "result".parse().expect("Id"),
        Scalar::from(TCRef::Id("$not_in_frame".parse().expect("IdRef"))),
    )]);

    let error = execute(&txn, missing, State::Map(Map::new()), None)
        .await
        .expect_err("missing lexical input");

    assert!(error.to_string().contains("missing input value"));
}

#[tokio::test]
async fn nested_opdef_cannot_shadow_parent_bindings() {
    let branch = OpDef::Post(vec![
        (
            "_tmp1".parse().expect("Id"),
            Scalar::Tuple(vec![Scalar::from(10_u64), Scalar::from(20_u64)]),
        ),
        (
            "result".parse().expect("Id"),
            Scalar::from(TCRef::Op(OpRef::Post((
                Subject::Ref(
                    "$_tmp1".parse().expect("IdRef"),
                    "get".parse().expect("Path"),
                ),
                Map::from_iter([("i".parse().expect("Id"), Scalar::from(1_u64))]),
            )))),
        ),
    ]);

    let form = vec![
        ("cond".parse().expect("Id"), Scalar::from(Value::from(true))),
        ("_tmp1".parse().expect("Id"), Scalar::from(1_u64)),
        (
            "result".parse().expect("Id"),
            Scalar::from(TCRef::Cond(Box::new(Cond::new(
                TCRef::Id("$cond".parse().expect("IdRef")),
                Scalar::from(branch),
                Scalar::from(0_u64),
            )))),
        ),
    ];

    let op = OpDef::Post(form);
    let txn = crate::txn::test_txn("test-host").await;
    let error = execute(&txn, op, State::Map(Map::new()), None)
        .await
        .expect_err("reject nested shadowing");

    assert!(error.to_string().contains("shadowed OpDef binding $_tmp1"));
}

#[tokio::test]
async fn executes_tensor_metadata_and_matmul_refs() {
    let form = vec![
        (
            "shape".parse().expect("Id"),
            Scalar::from(TCRef::Op(OpRef::Post((
                Subject::Ref("$x".parse().expect("IdRef"), "shape".parse().expect("Path")),
                Map::new(),
            )))),
        ),
        (
            "product".parse().expect("Id"),
            Scalar::from(TCRef::Op(OpRef::Post((
                Subject::Ref(
                    "$x".parse().expect("IdRef"),
                    "matmul".parse().expect("Path"),
                ),
                Map::from_iter([(
                    "r".parse().expect("Id"),
                    Scalar::from(TCRef::Id("$y".parse().expect("IdRef"))),
                )]),
            )))),
        ),
    ];

    let txn = crate::txn::test_txn("test-host").await;
    let op = OpDef::Post(form);
    let mut params = Map::new();
    params.insert(
        "x".parse().expect("Id"),
        State::from(Collection::Tensor(
            Tensor::dense_f64(&txn, vec![2, 2], vec![1.0, 2.0, 3.0, 4.0])
                .await
                .expect("left tensor"),
        )),
    );

    params.insert(
        "y".parse().expect("Id"),
        State::from(Collection::Tensor(
            Tensor::dense_f64(&txn, vec![2, 2], vec![5.0, 6.0, 7.0, 8.0])
                .await
                .expect("right tensor"),
        )),
    );

    let result = execute(&txn, op, State::from(params), None)
        .await
        .expect("exec");

    match result {
        State::Collection(Collection::Tensor(tensor)) => {
            assert_eq!(tensor.shape(), &[2, 2]);
            assert_eq!(
                tensor
                    .elements(txn.id())
                    .await
                    .unwrap()
                    .map_ok(|n| -> f64 { n.cast_into() })
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap(),
                vec![19.0, 22.0, 43.0, 50.0]
            );
        }
        other => panic!("unexpected result {:?}", other),
    }
}

#[tokio::test]
async fn graph_waves_retain_repeated_inputs_and_move_provider_results() {
    let kernel = test_kernel("borrowed-graph-waves", crate::HostLimits::default()).await;
    let txn = kernel.test_txn().await;
    let reference = |name: &str| Scalar::from(TCRef::Id(name.parse().unwrap()));
    let op = OpDef::Post(vec![
        ("left".parse().unwrap(), reference("$input")),
        ("right".parse().unwrap(), reference("$input")),
        (
            "result".parse().unwrap(),
            Scalar::Tuple(vec![
                reference("$left"),
                reference("$right"),
                reference("$self"),
            ]),
        ),
    ]);

    let input = State::from(Value::from("retained input"));
    let args = State::Map(Map::from_iter([("input".parse().unwrap(), input.clone())]));
    let result = execute(&txn, op, args, Some(State::from(9_u64)))
        .await
        .unwrap();

    let State::Tuple(values) = result else {
        panic!("expected resolved tuple")
    };

    assert_eq!(values.len(), 3);
    for value in &values[..2] {
        assert_eq!(
            Value::opt_cast_from(value.clone()),
            Value::opt_cast_from(input.clone())
        );
    }

    assert_eq!(
        Value::opt_cast_from(values[2].clone()),
        Some(Value::from(9_u64))
    );

    drop(txn);
    kernel.shutdown().await;
}
