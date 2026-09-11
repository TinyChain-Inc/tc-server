use super::*;

#[tokio::test]
async fn authority_record_uses_its_canonical_stream_codec() {
    let record = AuthorityRecord {
        actor_id: "host-actor".parse().unwrap(),
        algorithm: rjwt::AlgKind::Falcon512,
        signing_key: vec![1, 2, 3, 4],
    };
    let encoded = destream_json::encode(&record).unwrap();
    let decoded: AuthorityRecord = destream_json::try_decode((), encoded).await.unwrap();
    assert_eq!(decoded, record);
}
