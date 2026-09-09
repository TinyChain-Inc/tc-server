pub(crate) const HOST_ROOT: &str = "/host";
pub(crate) const HOST_HEALTH: &str = "/healthz";
pub(crate) const HOST_ROOT_PREFIX: &str = "/host/";
pub(crate) const HOST_METRICS: &str = "/host/metrics";
pub(crate) const HOST_PUBLIC_KEY: &str = "/host/public_key";
pub(crate) const HOST_AUTH_CONTEXT: &str = "/host/auth/context";
pub const HOST_TXN_PREFIX: &str = "/host/txn/";
pub(crate) fn transaction_path(txn_id: tc_ir::TxnId) -> String {
    format!("{HOST_TXN_PREFIX}{txn_id}")
}

#[cfg(feature = "http-client")]
pub(crate) fn append_kernel_txn_id(
    url: &mut url::Url,
    txn_id: tc_ir::TxnId,
) -> tc_error::TCResult<String> {
    if url
        .query_pairs()
        .any(|(key, _)| key.eq_ignore_ascii_case("txn_id"))
    {
        return Err(tc_error::TCError::bad_request(
            "outbound targets must not include txn_id; it is supplied by the kernel".to_string(),
        ));
    }

    url.query_pairs_mut()
        .append_pair("txn_id", &txn_id.to_string());

    Ok(url.as_str().to_string())
}

pub(crate) fn normalize_path(path: &str) -> &str {
    if path.len() > 1 {
        path.trim_end_matches('/')
    } else {
        path
    }
}
