use tc_error::{TCError, TCResult};
use tc_ir::TxnId;
use url::form_urlencoded;

pub(crate) fn parse_txn_id_query(query: Option<&str>) -> TCResult<Option<TxnId>> {
    let query = query.unwrap_or("");
    let txn_id = form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .find(|(key, _)| key.eq_ignore_ascii_case("txn_id"))
        .map(|(_, value)| value);

    match txn_id {
        Some(value) => value
            .parse::<TxnId>()
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
