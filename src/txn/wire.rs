use tc_error::{TCError, TCResult};
use tc_ir::TxnId;
use url::form_urlencoded;

const MAX_INBOUND_TXN_CLOCK_SKEW_NANOS: u64 = 3_000_000_000;

fn validate_inbound_txn_id(txn_id: TxnId) -> TCResult<TxnId> {
    let timestamp = txn_id.timestamp().as_nanos();
    if timestamp == 0 {
        return Err(TCError::bad_request(
            "invalid transaction id: timestamp must be nonzero",
        ));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let oldest = now.saturating_sub(MAX_INBOUND_TXN_CLOCK_SKEW_NANOS);
    let newest = now.saturating_add(MAX_INBOUND_TXN_CLOCK_SKEW_NANOS);
    if timestamp < oldest || timestamp > newest {
        return Err(TCError::bad_request(
            "invalid transaction id: timestamp outside accepted transaction window",
        ));
    }

    Ok(txn_id)
}

pub(crate) fn parse_txn_id_query(query: Option<&str>) -> TCResult<Option<TxnId>> {
    let query = query.unwrap_or("");
    let txn_id = form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .find(|(key, _)| key.eq_ignore_ascii_case("txn_id"))
        .map(|(_, value)| value);

    match txn_id {
        Some(value) => value
            .parse::<TxnId>()
            .and_then(|txn_id| validate_inbound_txn_id(txn_id).map_err(|_| "invalid TxnId"))
            .map(Some)
            .map_err(|_| TCError::bad_request("invalid transaction id")),
        None => Ok(None),
    }
}

#[cfg(feature = "pyo3")]
pub(crate) fn split_path_and_txn_id(raw: &str) -> TCResult<(String, Option<TxnId>)> {
    if let Some((path, query)) = raw.split_once('?') {
        Ok((path.to_string(), parse_txn_id_query(Some(query))?))
    } else {
        Ok((raw.to_string(), None))
    }
}

#[cfg(test)]
mod tests {
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
    }

    #[test]
    fn rejects_partial_transaction_id_without_trace() {
        let err = parse_txn_id_query(Some("txn_id=7-3")).expect_err("partial txn id rejected");
        assert!(err.message().contains("invalid transaction id"));
    }

    #[test]
    fn rejects_zero_timestamp_transaction_id() {
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(0), 0).with_trace([1; 32]);
        let err = parse_txn_id_query(Some(&format!("txn_id={txn_id}")))
            .expect_err("zero timestamp rejected");
        assert!(err.message().contains("invalid transaction id"));
    }

    #[test]
    fn rejects_stale_transaction_id() {
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(1), 0).with_trace([1; 32]);
        let err =
            parse_txn_id_query(Some(&format!("txn_id={txn_id}"))).expect_err("stale txn rejected");
        assert!(err.message().contains("invalid transaction id"));
    }

    #[test]
    fn rejects_future_transaction_id() {
        let future = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
            + MAX_INBOUND_TXN_CLOCK_SKEW_NANOS
            + 1_000_000_000;
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(future), 0).with_trace([1; 32]);
        let err =
            parse_txn_id_query(Some(&format!("txn_id={txn_id}"))).expect_err("future txn rejected");
        assert!(err.message().contains("invalid transaction id"));
    }

    #[cfg(feature = "pyo3")]
    #[test]
    fn split_path_rejects_partial_transaction_id_without_trace() {
        let err =
            split_path_and_txn_id("/lib/demo?txn_id=7-3").expect_err("partial txn id rejected");
        assert!(err.message().contains("invalid transaction id"));
    }

    #[cfg(feature = "pyo3")]
    #[test]
    fn split_path_rejects_zero_timestamp_transaction_id() {
        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(0), 0).with_trace([1; 32]);
        let err = split_path_and_txn_id(&format!("/lib/demo?txn_id={txn_id}"))
            .expect_err("zero timestamp rejected");
        assert!(err.message().contains("invalid transaction id"));
    }
}
