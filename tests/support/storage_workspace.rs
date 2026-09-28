use super::*;

#[tokio::test]
async fn authority_record_uses_its_canonical_stream_codec() {
    use futures::TryStreamExt;

    let record = AuthorityRecord {
        actor_id: "host-actor".parse().unwrap(),
        algorithm: rjwt::AlgKind::Falcon512,
        signing_key: vec![7; 4096],
    };
    let encoded = destream_json::encode(&record)
        .unwrap()
        .try_fold(Vec::new(), |mut bytes, chunk| async move {
            bytes.extend_from_slice(&chunk);
            Ok(bytes)
        })
        .await
        .unwrap();
    let expected = serde_json::json!({"actor_id":"host-actor", "algorithm":record.algorithm.name(), "signing_key":record.signing_key});
    assert_eq!(encoded, serde_json::to_vec(&expected).unwrap());
    let decoded: AuthorityRecord = destream_json::try_decode(
        (),
        futures::stream::iter([Ok::<_, std::io::Error>(bytes::Bytes::from(encoded))]),
    )
    .await
    .unwrap();
    assert_eq!(decoded, record);
}
