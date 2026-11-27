use tc_state::State;

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

#[tokio::test]
async fn byte_responses_use_state_codec_and_encoded_bound() {
    let txn = crate::txn::test_txn("http-rpc-bytes").await;
    let bytes = std::sync::Arc::<[u8]>::from([0, 1, 255]);
    let body = encode_state_body(
        State::from(tc_value::Value::from(bytes.clone())),
        txn.clone(),
    )
    .await
    .unwrap();

    let state = decode_state_response(hyper::Response::new(body), &txn)
        .await
        .unwrap();

    let value = tc_value::Value::opt_cast_from(state).unwrap();
    assert_eq!(value, tc_value::Value::from(bytes));

    // The bound covers the encoded State, not just its underlying byte payload.
    let bytes = std::sync::Arc::<[u8]>::from(vec![255; txn.request_body_limit()]);
    let body = encode_state_body(State::from(tc_value::Value::from(bytes)), txn.clone())
        .await
        .unwrap();

    let error = decode_state_response(hyper::Response::new(body), &txn)
        .await
        .unwrap_err();

    assert_eq!(error.code(), tc_error::ErrorKind::PayloadTooLarge);
}

#[cfg(feature = "http-server")]
#[tokio::test]
async fn bootstrap_membership_comparison_ignores_wire_order_and_rejects_duplicates() {
    let record = |endpoint: &str| {
        vec![
            tc_value::Value::from(endpoint.to_string()),
            tc_value::Value::from("https://peer.example".to_string()),
            tc_value::Value::from("peer-actor".to_string()),
            tc_value::Value::from(rjwt::AlgKind::Falcon512.name().to_string()),
            tc_value::Value::from("public-key".to_string()),
        ]
    };

    let a = record("http://a.example");
    let b = record("http://b.example");
    let replies = [
        vec![b.clone(), a.clone()],
        vec![a.clone(), b],
        vec![a.clone(), a],
    ];

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for reply in replies {
            let (socket, _) = listener.accept().await.unwrap();
            let service = hyper::service::service_fn(move |_| {
                let body = crate::http_body::json_body(reply.clone());
                async move { Ok::<_, std::convert::Infallible>(hyper::Response::new(body)) }
            });

            hyper::server::conn::Http::new()
                .http1_keep_alive(false)
                .serve_connection(socket, service)
                .await
                .unwrap();
        }
    });

    let kernel = crate::txn::test_kernel("bootstrap-membership-order").await;
    let txn = kernel.test_txn().await;
    let target = pathlink::Link::from(pathlink::path_label(&["lib", "replicas"]));
    let first = crate::cluster::seed_replicas(&endpoint, "test-token", &txn, &target)
        .await
        .unwrap();

    let second = crate::cluster::seed_replicas(&endpoint, "test-token", &txn, &target)
        .await
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(first[0].endpoint, "http://a.example");
    assert_eq!(first[1].endpoint, "http://b.example");
    let error = crate::cluster::seed_replicas(&endpoint, "test-token", &txn, &target)
        .await
        .unwrap_err();

    assert_eq!(error.code(), tc_error::ErrorKind::BadGateway);
    server.await.unwrap();
    drop(txn);
    kernel.shutdown().await;
}

#[test]
fn peer_transaction_url_escapes_native_paths_once() {
    let txn = TxnId::from_parts(tc_ir::NetworkTime::from_nanos(1), 1);
    for name in ["λ", "literal%name", "%2f"] {
        let path = pathlink::PathBuf::from(name.parse::<pathlink::Id>().unwrap());
        let uri = pathlink::ToUrl::from(&path[..]).to_string();
        let url = peer_txn_url("http://localhost:8702", &uri, txn).unwrap();
        let parsed = url::Url::parse(&url).unwrap();
        let link: pathlink::Link = parsed.path().parse().unwrap();
        assert_eq!(link.path(), &path);
    }
}
