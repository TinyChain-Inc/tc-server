use hyper::Body;
use tc_error::{TCError, TCResult};
use tc_ir::TxnId;

pub(crate) async fn bootstrap_seed(
    seed: &str,
    txn_id: TxnId,
    resource: &pathlink::Link,
    identity: &super::Replica,
    issuer: &super::ReplicationIssuer,
) -> TCResult<super::issuer::BootstrapSession> {
    let seed = super::normalize_peer(seed)?;
    let url = crate::http_client::peer_txn_url(&seed, crate::uri::HOST_ROOT, txn_id)?;
    let requests = issuer.bootstrap_requests(txn_id, resource, identity)?;
    if requests.is_empty() {
        return Err(TCError::bad_gateway("bootstrap has no configured PSK"));
    }
    let mut last_error = None;
    for encrypted in requests {
        let encoded = crate::literal::encode_json(
            tc_ir::Scalar::Value(tc_value::Value::Bytes(encrypted.into())),
            crate::literal::MAX_DEFINITION_BYTES,
        )
        .await?;
        match crate::http_client::send_http(
            &hyper::Client::new(),
            hyper::Method::GET,
            url.clone(),
            None,
            Some("application/json"),
            encoded,
            crate::Deadline::after(crate::outbound_http::DEFAULT_TIMEOUT),
        )
        .await
        {
            Ok((status, body)) if status.is_success() => {
                let stream = futures::stream::iter([Ok::<_, std::io::Error>(body)]);
                let scalar: tc_ir::Scalar =
                    destream_json::try_decode((), stream)
                        .await
                        .map_err(|error| {
                            TCError::bad_gateway(format!("invalid bootstrap body: {error}"))
                        })?;
                let tc_ir::Scalar::Value(tc_value::Value::Bytes(encrypted)) = scalar else {
                    return Err(TCError::bad_gateway(
                        "bootstrap response was not encrypted bytes",
                    ));
                };
                return issuer.open_response(seed, &encrypted);
            }
            Ok((status, body)) => {
                last_error = Some(TCError::bad_gateway(format!(
                    "seed returned {status}: {}",
                    String::from_utf8_lossy(&body)
                )))
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| TCError::bad_gateway("bootstrap failed")))
}

pub(crate) async fn read_seed_state(
    seed: &str,
    token: &str,
    txn_id: TxnId,
    target: &pathlink::Link,
    deadline: crate::Deadline,
) -> TCResult<crate::State> {
    let seed = super::normalize_peer(seed)?;
    let url = crate::http_client::peer_txn_url(&seed, &target.to_string(), txn_id)?;
    let (status, body) = crate::http_client::send_http(
        &hyper::Client::new(),
        hyper::Method::GET,
        url,
        Some(format!("Bearer {token}")),
        None,
        Body::empty(),
        deadline,
    )
    .await?;
    if !status.is_success() {
        return Err(crate::outbound_http::error_from_status(status, body));
    }
    let input = futures::stream::iter([Ok::<_, std::io::Error>(body)]);
    let scalar: tc_ir::Scalar = destream_json::try_decode((), input)
        .await
        .map_err(|error| TCError::bad_gateway(format!("invalid bootstrap state: {error}")))?;
    Ok(crate::State::from_scalar(scalar))
}
