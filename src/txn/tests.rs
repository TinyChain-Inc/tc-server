use super::*;
use crate::auth::{RjwtTokenVerifier, Token, TokenVerifier};
use std::str::FromStr;
use tc_ir::{Claim, NetworkTime, TxnId};
use umask::Mode;

#[test]
fn mints_host_signed_bearer_token_for_unauthenticated_txn() {
    let manager = TxnManager::with_host_id("test-host");
    let handle = manager.begin();

    let bearer = handle
        .raw_token()
        .expect("anonymous transactions must carry host-signed protocol bearer tokens");

    let txn_link = pathlink::Link::from_str(&format!("/txn/{}", handle.id())).expect("txn link");
    assert!(handle.has_claim(&txn_link, umask::USER_EXEC));
    assert!(handle.has_claim(&txn_link, umask::USER_WRITE));

    let keyring = crate::auth::KeyringActorResolver::default().with_actor(
        manager.protocol_host().clone(),
        manager.protocol_actor().as_ref().clone(),
    );
    let verifier = RjwtTokenVerifier::new(std::sync::Arc::new(keyring));
    let ctx = futures::executor::block_on(verifier.verify(bearer.to_string()))
        .expect("host-signed protocol token verifies");
    assert_eq!(
        crate::txn::owner_id_from_token(handle.id(), &ctx).expect("txn owner"),
        format!(
            "{}::{}",
            manager.protocol_host(),
            manager.protocol_actor().id()
        )
    );
}

#[test]
fn enforces_canonical_claim_position() {
    use std::time::{Duration, SystemTime};

    use rjwt::Actor;

    let manager = TxnManager::with_host_id("test-host");
    let handle = manager.begin();
    let txn_id = handle.id();

    let txn_claim = Claim::new(
        pathlink::Link::from_str(&format!("/txn/{txn_id}")).expect("txn claim"),
        umask::USER_EXEC,
    );
    let auth_claim = Claim::new(
        pathlink::Link::from_str("/lib/auth").expect("auth link"),
        Mode::all(),
    );

    let host = pathlink::Link::from_str("/host").expect("host link");
    let actor = Actor::new_falcon512("actor-a".to_string()).expect("generate Falcon-512 actor");
    let now = SystemTime::now();
    let ttl = Duration::from_secs(30);

    let token = Token::new(
        host.clone(),
        now,
        ttl,
        actor.id().clone(),
        auth_claim.clone(),
    );
    let signed = actor.sign_token(token).expect("signed token");
    let signed = actor
        .consume_and_sign(signed, host.clone(), txn_claim.clone(), now)
        .expect("consume token");
    let updated = handle.with_signed_token(signed).expect("token accepted");
    assert_eq!(updated.claim(), &txn_claim);

    let other_claim = Claim::new(
        pathlink::Link::from_str("/lib/other").expect("other link"),
        Mode::all(),
    );
    let final_claim = Claim::new(
        pathlink::Link::from_str("/lib/final").expect("final link"),
        Mode::all(),
    );
    let token = Token::new(host.clone(), now, ttl, actor.id().clone(), txn_claim);
    let signed = actor.sign_token(token).expect("signed token");
    let signed = actor
        .consume_and_sign(signed, host.clone(), other_claim, now)
        .expect("consume token");
    let signed = actor
        .consume_and_sign(signed, host, final_claim, now)
        .expect("consume token");
    let err = handle.with_signed_token(signed).expect_err("should reject");
    assert!(
        err.message()
            .contains("canonical claim must be first or second")
    );
}

#[test]
fn rejects_signed_token_for_different_transaction_id() {
    use std::time::{Duration, SystemTime};

    use rjwt::Actor;

    let manager = TxnManager::with_host_id("test-host");
    let handle = manager.begin();
    let other_txn_id = TxnId::from_parts(NetworkTime::from_nanos(99), 1).with_trace([9; 32]);

    let host = pathlink::Link::from_str("/host").expect("host link");
    let actor = Actor::new_falcon512("actor-a".to_string()).expect("generate Falcon-512 actor");
    let now = SystemTime::now();
    let token = Token::new(
        host,
        now,
        Duration::from_secs(30),
        actor.id().clone(),
        Claim::new(
            pathlink::Link::from_str(&format!("/txn/{other_txn_id}")).expect("other txn claim"),
            umask::USER_EXEC,
        ),
    );
    let signed = actor.sign_token(token).expect("signed token");

    let err = handle
        .with_signed_token(signed)
        .expect_err("token for a different transaction must be rejected");
    assert!(err.message().contains("cannot initialize txn"));
}

#[test]
fn unknown_txn_continuation_requires_authenticated_owner() {
    let manager = TxnManager::with_host_id("test-host");
    let unknown = TxnId::from_parts(NetworkTime::from_nanos(7), 7).with_trace([1; 32]);
    let rejected = manager.interpret_request(Some(unknown), None, None);
    assert!(matches!(rejected, Err(TxnError::NotFound)));

    let accepted = manager.interpret_request(Some(unknown), Some("owner-a"), Some("token-a"));
    let handle = match accepted {
        Ok(TxnFlow::Begin(handle)) => handle,
        Ok(TxnFlow::Use(_)) => panic!("unknown participant txn must create a local continuation"),
        Err(err) => panic!("authenticated continuation rejected: {err:?}"),
    };
    assert_eq!(
        handle.id(),
        unknown,
        "participant continuation must reuse the inbound txn ID"
    );
}

#[test]
fn inbound_transaction_id_is_not_retraced() {
    let manager = TxnManager::with_host_id("test-host");
    let inbound = TxnId::from_parts(NetworkTime::from_nanos(11), 3);
    assert!(
        inbound.trace_bytes().iter().all(|byte| *byte == 0),
        "test fixture must exercise a zero-trace inbound ID"
    );

    let accepted = manager.interpret_request(Some(inbound), Some("owner-a"), Some("token-a"));
    let handle = match accepted {
        Ok(TxnFlow::Begin(handle)) => handle,
        Ok(TxnFlow::Use(_)) => panic!("unknown participant txn must create a continuation"),
        Err(err) => panic!("authenticated continuation rejected: {err:?}"),
    };

    assert_eq!(
        handle.id(),
        inbound,
        "participant nodes must preserve the exact inbound transaction ID"
    );
    assert!(
        manager.get_with_owner(&inbound, Some("owner-a")).is_ok(),
        "stored participant transaction must be addressed by the exact inbound ID"
    );
}

#[test]
fn attaches_structured_auth_context_to_txn_handle() {
    let manager = TxnManager::with_host_id("test-host");
    let handle =
        manager.begin_with_owner(Some("http://127.0.0.1:8702::example-admin"), Some("token"));

    let claim = Claim::new(
        pathlink::Link::from_str("/lib/example-devco/a/0.1.0").expect("claim link"),
        Mode::all(),
    );
    let mut token = crate::auth::TokenContext::new("http://127.0.0.1:8702::example-admin", "token");
    token = token.with_claim(
        "http://127.0.0.1:8702".to_string(),
        "example-admin".to_string(),
        claim,
    );

    let handle = handle.with_auth_context(AuthContext::from_token_context(&token));
    let auth = handle.auth_context().expect("auth context");
    assert_eq!(auth.principal, "http://127.0.0.1:8702::example-admin");
    assert_eq!(auth.claims.len(), 1);
    assert_eq!(
        auth.token_hosts(),
        vec!["http://127.0.0.1:8702".to_string()]
    );
}
