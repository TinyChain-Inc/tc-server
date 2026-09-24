use hyper::Body;
use tc_error::{TCError, TCResult};
use tc_ir::TxnId;

pub(crate) async fn bootstrap_seed(
    seed: &str,
    txn_id: TxnId,
    resource: &pathlink::Link,
    identity: &super::Replica,
    issuer: &super::ReplicationIssuer,
    deadline: crate::Deadline,
) -> TCResult<crate::cluster::BootstrapSession> {
    let seed = super::normalize_peer(seed)?;
    let url = crate::http_client::peer_txn_url(&seed, crate::uri::HOST_ROOT, txn_id)?;
    let requests = issuer
        .bootstrap_requests(txn_id, resource, identity)
        .await?;
    if requests.is_empty() {
        return Err(TCError::bad_gateway("bootstrap has no configured PSK"));
    }
    let mut last_error = None;
    for (nonce, ciphertext) in requests {
        let encoded = crate::http_body::json_body(tc_ir::Scalar::Tuple(vec![
            tc_ir::Scalar::Value(tc_value::Value::Bytes(nonce.into())),
            tc_ir::Scalar::Value(tc_value::Value::Bytes(ciphertext.into())),
        ]));
        match crate::http_client::send_http(
            &hyper::Client::new(),
            hyper::Method::GET,
            url.clone(),
            None,
            Some("application/json"),
            encoded,
            deadline,
        )
        .await
        {
            Ok(response) => {
                let scalar: tc_ir::Scalar = match crate::outbound_http::decode(
                    response,
                    (),
                    deadline,
                    crate::literal::MAX_DEFINITION_BYTES,
                )
                .await
                {
                    Ok(scalar) => scalar,
                    Err(error) => {
                        last_error = Some(error);
                        continue;
                    }
                };
                let tc_ir::Scalar::Tuple(response) = scalar else {
                    return Err(TCError::bad_gateway(
                        "bootstrap response was not an encrypted tuple",
                    ));
                };
                let [nonce, ciphertext]: [tc_ir::Scalar; 2] =
                    response.try_into().map_err(|_| {
                        TCError::bad_gateway("bootstrap response must contain nonce and ciphertext")
                    })?;
                let bytes = |value| match value {
                    tc_ir::Scalar::Value(tc_value::Value::Bytes(bytes)) => Ok(bytes),
                    _ => Err(TCError::bad_gateway(
                        "bootstrap encrypted fields must be bytes",
                    )),
                };
                let nonce = bytes(nonce)?;
                let ciphertext = bytes(ciphertext)?;
                match issuer
                    .open_response(seed.clone(), &nonce, &ciphertext)
                    .await
                {
                    Ok(session) => return Ok(session),
                    Err(error) => last_error = Some(error),
                }
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| TCError::bad_gateway("bootstrap failed")))
}

pub(crate) async fn read_seed_state(
    seed: &str,
    token: &str,
    txn: &crate::TxnHandle,
    target: &pathlink::Link,
    response_bound: usize,
) -> TCResult<crate::State> {
    let deadline = txn.deadline();
    let response = seed_response(seed, token, txn, target).await?;
    let content_type = response
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());
    if content_type == Some("application/wasm") {
        return crate::outbound_http::collect_bytes(response, deadline, response_bound)
            .await
            .map(|bytes| crate::State::from(tc_value::Value::Bytes(bytes)));
    }

    // Definitions contain scalar declaration references, not allocated collections.
    crate::outbound_http::decode(response, (), deadline, response_bound)
        .await
        .map(crate::State::from_scalar)
}

pub(crate) async fn read_seed_snapshot(
    seed: &str,
    token: &str,
    txn: &crate::TxnHandle,
    target: &pathlink::Link,
    response_bound: usize,
) -> TCResult<crate::State> {
    use tc_collection::StorageContext;

    let response = seed_response(seed, token, txn, target).await?;
    crate::outbound_http::decode(
        response,
        txn.subcontext_unique(),
        txn.deadline(),
        response_bound,
    )
    .await
}

async fn seed_response(
    seed: &str,
    token: &str,
    txn: &crate::TxnHandle,
    target: &pathlink::Link,
) -> TCResult<hyper::Response<Body>> {
    let seed = super::normalize_peer(seed)?;
    let url = crate::http_client::peer_txn_url(&seed, &target.to_string(), txn.id())?;
    crate::http_client::send_http(
        &hyper::Client::new(),
        hyper::Method::GET,
        url,
        Some(format!("Bearer {token}")),
        None,
        Body::empty(),
        txn.deadline(),
    )
    .await
}
