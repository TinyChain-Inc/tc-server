use super::*;
use number_general::Number;
use tc_ir::{Map, OpDef, Scalar, TCRef};
use tc_state::State;
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
