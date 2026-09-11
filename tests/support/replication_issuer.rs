use super::*;

fn issuer(label: &str, keys: Vec<Key<Aes256GcmSiv>>) -> ReplicationIssuer {
    let actor = Actor::new_falcon512(label.to_string()).expect("test actor");
    let authority = std::sync::Arc::new(crate::ProtocolAuthority::new(
        format!("https://{label}.example").parse().expect("host"),
        actor,
    ));
    ReplicationIssuer::new(authority, keys, KeyringActorResolver::default())
        .expect("replication issuer")
}

#[tokio::test]
async fn psk_rotation_accepts_overlap_and_rejects_a_retired_key() {
    let old = Key::<Aes256GcmSiv>::from([7; 32]);
    let new = Key::<Aes256GcmSiv>::from([8; 32]);
    let seed = issuer("seed", vec![old, new]);
    let old_client = issuer("old-client", vec![old]);
    let new_client = issuer("new-client", vec![new]);
    let txn_id = TxnId::from_parts(tc_ir::NetworkTime::from_nanos(1), 0);
    let resource: pathlink::Link = "/lib".parse().expect("resource");

    for client in [&old_client, &new_client] {
        let identity = client
            .self_identity("http://127.0.0.1:8702".to_string())
            .expect("identity");
        let encrypted = client
            .bootstrap_requests(txn_id, &resource, &identity)
            .await
            .expect("bootstrap request")
            .remove(0);
        seed.open_request(txn_id, &encrypted.0, &encrypted.1)
            .await
            .expect("overlapping PSK accepted");
    }

    let rotated = issuer("rotated-seed", vec![new]);
    let identity = old_client
        .self_identity("http://127.0.0.1:8703".to_string())
        .expect("identity");
    let encrypted = old_client
        .bootstrap_requests(txn_id, &resource, &identity)
        .await
        .expect("bootstrap request")
        .remove(0);
    assert!(
        rotated
            .open_request(txn_id, &encrypted.0, &encrypted.1)
            .await
            .is_err()
    );
}
