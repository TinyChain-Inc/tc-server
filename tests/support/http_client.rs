use super::*;

#[tokio::test]
async fn explicit_null_is_an_ordinary_request_body() {
    let txn = crate::txn::test_txn("http-rpc-null").await;
    let body = encode_state_body(State::None, txn)
        .await
        .expect("encode explicit null");
    let decoded: Scalar = destream_json::try_decode((), body).await.unwrap();
    assert_eq!(decoded, Scalar::Value(tc_value::Value::None));
}

#[test]
fn attaches_application_authority_headers() {
    let request = build_http_request(
        hyper::Method::GET,
        "http://localhost:8702/lib?txn_id=1".to_string(),
        Some("Bearer abc.def".to_string()),
        Some("application/json"),
        Vec::new(),
    )
    .expect("request");

    let auth = request.headers().get("authorization").expect("auth header");
    assert_eq!(auth.to_str().expect("auth header str"), "Bearer abc.def");
}
