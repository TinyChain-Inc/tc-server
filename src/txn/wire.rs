use tc_error::{TCError, TCResult};
use tc_ir::TxnId;
use url::form_urlencoded;

pub(crate) const MAX_INBOUND_TXN_CLOCK_SKEW_NANOS: u64 = 3_000_000_000;

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

pub(crate) fn split_path_and_txn_id(raw: &str) -> TCResult<(String, Option<TxnId>)> {
    if let Some((path, query)) = raw.split_once('?') {
        Ok((path.to_string(), parse_txn_id_query(Some(query))?))
    } else {
        Ok((raw.to_string(), None))
    }
}

#[cfg(test)]
#[path = "../../tests/support/txn_request.rs"]
mod tests;
