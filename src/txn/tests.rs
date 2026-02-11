use super::*;
use crate::auth::Token;
use std::str::FromStr;
use tc_ir::Claim;
use umask::Mode;

#[test]
fn mints_bearer_token_for_anonymous_txn() {
    let manager = TxnManager::with_host_id("test-host");
    let handle = manager.begin();

    assert!(handle.raw_token().is_some());

    let txn_link = pathlink::Link::from_str(&format!("/txn/{}", handle.id())).expect("txn link");
    assert!(handle.has_claim(&txn_link, umask::USER_EXEC));
    assert!(handle.has_claim(&txn_link, umask::USER_WRITE));
}

#[test]
fn enforces_canonical_claim_position() {
    use std::time::{Duration, SystemTime};

    use rjwt::Actor;
    use tc_value::Value;

    let manager = TxnManager::with_host_id("test-host");
    let handle = manager.begin();
    let txn_id = handle.id();

    let txn_claim = Claim::new(
        pathlink::Link::from_str(&format!("/txn/{txn_id}")).expect("txn claim"),
        umask::USER_EXEC,
    );
    let auth_claim = Claim::new(pathlink::Link::from_str("/lib/auth").expect("auth link"), Mode::all());

    let host = pathlink::Link::from_str("/host").expect("host link");
    let actor = Actor::new(Value::from("actor-a"));
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
