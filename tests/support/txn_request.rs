use tc_ir::{NetworkTime, TxnId};

use super::*;

#[test]
fn parses_full_transaction_id_with_trace() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;

    let txn_id = TxnId::from_parts(NetworkTime::from_nanos(now), 3).with_trace([4; 32]);
    let parsed = parse_txn_id_query(Some(&format!("txn_id={txn_id}")))
        .expect("parse txn id")
        .expect("txn id");

    assert_eq!(parsed, txn_id);
    assert_eq!(
        split_path_and_txn_id(&format!("/lib/demo?txn_id={txn_id}")).unwrap(),
        ("/lib/demo".into(), Some(txn_id))
    );

    assert_eq!(
        split_path_and_txn_id("/lib/demo").unwrap(),
        ("/lib/demo".into(), None)
    );
}

#[test]
fn rejects_invalid_transaction_ids() {
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
        + MAX_INBOUND_TXN_CLOCK_SKEW_NANOS
        + 1_000_000_000;

    let id =
        |timestamp| TxnId::from_parts(NetworkTime::from_nanos(timestamp), 0).with_trace([1; 32]);

    for value in [
        "7-3".into(),
        id(0).to_string(),
        id(1).to_string(),
        id(future).to_string(),
    ] {
        let error = parse_txn_id_query(Some(&format!("txn_id={value}"))).unwrap_err();
        assert_eq!(error.code(), tc_error::ErrorKind::BadRequest);
    }
}

#[test]
fn split_path_rejects_partial_transaction_id_without_trace() {
    let error = split_path_and_txn_id("/lib/demo?txn_id=7-3").unwrap_err();
    assert_eq!(error.code(), tc_error::ErrorKind::BadRequest);
}
