use crate::auth::TokenVerifier;
use super::*;

fn issuer(label: &str, keys: Vec<Key<Aes256GcmSiv>>) -> ReplicationIssuer {
    let actor = Actor::new(label.to_string(), rjwt::AlgKind::Falcon512).expect("test actor");
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
    const RESOURCE: pathlink::PathLabel = pathlink::path_label(&["lib"]);
    let resource = pathlink::Link::from(RESOURCE);

    for client in [&old_client, &new_client] {
        let identity = client
            .self_identity("http://127.0.0.1:8702".to_string())
            .expect("identity");
        let encrypted = client
            .bootstrap_requests(txn_id, &resource, &identity)
            .await
            .expect("bootstrap request")
            .remove(0);

        let (requested, key, owner, peer) = seed
            .open_request(txn_id, &encrypted.0, &encrypted.1)
            .await
            .expect("overlapping PSK accepted");

        assert_eq!(requested, resource);

        let verifier =
            crate::auth::RjwtTokenVerifier::new(std::sync::Arc::new(seed.keyring.clone()));

        let context = verifier.verify(owner).await.expect("signed joining owner");
        let signed = context.signed().unwrap();
        let claims = crate::txn::protocol_snapshot(txn_id, signed).unwrap();
        let expected_owner = (peer.host.clone(), peer.actor_id.clone());
        assert_eq!(claims.owner, Some(expected_owner.clone()));
        assert!(claims.leaders.is_empty());
        assert!(!claims.locked);

        // The seed extends that same owner; response encryption must not create
        // a second transaction identity or replace the joining owner's claim.
        let signed = seed
            .authority
            .extend(
                (**signed).clone(),
                crate::auth::wire_claim(Claim::new(resource.clone(), USER_EXEC)),
                std::time::SystemTime::now(),
            )
            .unwrap();

        let hash = [9; 32];
        let response = seed
            .seal_response(signed.into_jwt(), hash, &key)
            .await
            .unwrap();

        let session = client
            .open_response("http://127.0.0.1:8701".into(), &response.0, &response.1)
            .await
            .unwrap();

        let verifier =
            crate::auth::RjwtTokenVerifier::new(std::sync::Arc::new(client.keyring.clone()));

        let context = verifier.verify(session.token().to_owned()).await.unwrap();
        let claims = crate::txn::protocol_snapshot(txn_id, context.signed().unwrap()).unwrap();
        assert_eq!(claims.owner, Some(expected_owner));
        assert_eq!(
            claims.leaders.get(resource.path()),
            Some(&(
                seed.authority.host().to_string(),
                seed.authority.actor_id().to_string(),
            ))
        );

        assert_eq!(session.state_hash(), hex::encode(hash));
        assert!(!claims.locked);
        assert_eq!(claims.decision, None);
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
