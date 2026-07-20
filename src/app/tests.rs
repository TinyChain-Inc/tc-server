use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use base64::Engine as _;
use pathlink::Link;
use tc_ir::Claim;
use tinychain::auth::{Actor, KeyringActorResolver, RjwtTokenVerifier, TokenVerifier};
use tinychain::replication::{PeerMembership, parse_psk_list};
use umask::{USER_READ, USER_WRITE};

use super::discovery::dedupe_peers;
use super::trusted_installers::{
    TrustedInstaller, TrustedInstallerPolicy, TrustedInstallerTokenVerifier,
};

#[test]
fn parses_psk_list() {
    let keys = parse_psk_list("a, b,,c");
    assert_eq!(keys, vec!["a", "b", "c"]);
}

#[test]
fn dedupes_peers() {
    let peers = dedupe_peers(vec!["1.2.3.4:5".to_string(), "1.2.3.4:5".to_string()]);
    assert_eq!(peers.len(), 1);
}

#[tokio::test]
async fn trusted_installer_verifier_rejects_opaque_unauthenticated_tokens() {
    let policy =
        TrustedInstallerPolicy::from_installers(&[], "/lib/example-devco").expect("policy");
    let verifier = TrustedInstallerTokenVerifier::new(
        RjwtTokenVerifier::new(Arc::new(KeyringActorResolver::default())),
        policy,
        PeerMembership::new(Vec::new()),
        "replication:local".to_string(),
    );

    let result = verifier
        .verify("unauthenticated-session-token".to_string())
        .await;
    assert!(matches!(
        result,
        Err(tinychain::txn::TxnError::Unauthorized)
    ));
}

#[tokio::test]
async fn trusted_installer_verifier_still_enforces_claim_policy_for_signed_tokens() {
    let host = Link::from_str("http://127.0.0.1:8702").expect("host");
    let actor =
        Actor::new_falcon512("trusted-installer".to_string()).expect("generate Falcon-512 actor");
    let keyring = KeyringActorResolver::default().with_actor(host.clone(), actor.clone());

    let policy = TrustedInstallerPolicy::from_installers(
        &[TrustedInstaller {
            host: host.to_string(),
            actor_id: "trusted-installer".to_string(),
            public_key_b64: base64::engine::general_purpose::STANDARD
                .encode(actor.verifying_key().to_bytes()),
            allowed_lib_prefixes: vec!["/lib/example-devco".to_string()],
        }],
        "/lib/example-devco",
    )
    .expect("policy");

    let verifier = TrustedInstallerTokenVerifier::new(
        RjwtTokenVerifier::new(Arc::new(keyring)),
        policy,
        PeerMembership::new(Vec::new()),
        "replication:local".to_string(),
    );

    let denied_claim = Claim::new(
        Link::from_str("/lib/otherco/private/1.0.0").expect("claim"),
        USER_WRITE,
    );
    let token = tinychain::auth::Token::new(
        host,
        SystemTime::now(),
        Duration::from_secs(30),
        actor.id().clone(),
        denied_claim,
    );
    let signed = actor.sign_token(token).expect("signed").into_jwt();

    let result = verifier.verify(signed).await;
    assert!(matches!(
        result,
        Err(tinychain::txn::TxnError::Unauthorized)
    ));
}

#[tokio::test]
async fn trusted_installer_verifier_rejects_jwt_shaped_garbage_tokens() {
    let policy =
        TrustedInstallerPolicy::from_installers(&[], "/lib/example-devco").expect("policy");
    let verifier = TrustedInstallerTokenVerifier::new(
        RjwtTokenVerifier::new(Arc::new(KeyringActorResolver::default())),
        policy,
        PeerMembership::new(Vec::new()),
        "replication:local".to_string(),
    );

    let result = verifier.verify("aaa.bbb.ccc".to_string()).await;
    assert!(matches!(
        result,
        Err(tinychain::txn::TxnError::Unauthorized)
    ));
}

#[tokio::test]
async fn trusted_installer_policy_rejects_unconfigured_external_actor() {
    let host = Link::from_str("http://127.0.0.1:8702").expect("host");
    let actor =
        Actor::new_falcon512("external-installer".to_string()).expect("generate Falcon-512 actor");
    let keyring = KeyringActorResolver::default().with_actor(host.clone(), actor.clone());
    let policy =
        TrustedInstallerPolicy::from_installers(&[], "/lib/example-devco").expect("policy");
    let verifier = TrustedInstallerTokenVerifier::new(
        RjwtTokenVerifier::new(Arc::new(keyring)),
        policy,
        PeerMembership::new(Vec::new()),
        "replication:local".to_string(),
    );

    let claim = Claim::new(
        Link::from_str("/lib/example-devco/example/1.0.0").expect("claim"),
        USER_WRITE,
    );
    let token = tinychain::auth::Token::new(
        host,
        SystemTime::now(),
        Duration::from_secs(30),
        actor.id().clone(),
        claim,
    );
    let signed = actor.sign_token(token).expect("signed").into_jwt();

    let result = verifier.verify(signed).await;
    assert!(matches!(
        result,
        Err(tinychain::txn::TxnError::Unauthorized)
    ));
}

#[tokio::test]
async fn trusted_installer_policy_allows_host_replication_actor() {
    let host = Link::from_str("/host").expect("host");
    let actor =
        Actor::new_falcon512("replication:node-a".to_string()).expect("generate Falcon-512 actor");
    let keyring = KeyringActorResolver::default().with_actor(host.clone(), actor.clone());
    let policy =
        TrustedInstallerPolicy::from_installers(&[], "/lib/example-devco").expect("policy");
    let verifier = TrustedInstallerTokenVerifier::new(
        RjwtTokenVerifier::new(Arc::new(keyring)),
        policy,
        PeerMembership::new(Vec::new()),
        "replication:node-a".to_string(),
    );

    let claim = Claim::new(
        Link::from_str("/lib/example-devco/example/1.0.0").expect("claim"),
        USER_READ,
    );
    let token = tinychain::auth::Token::new(
        host,
        SystemTime::now(),
        Duration::from_secs(30),
        actor.id().clone(),
        claim,
    );
    let signed = actor.sign_token(token).expect("signed").into_jwt();

    let result = verifier.verify(signed).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn trusted_installer_policy_allows_known_peer_replication_actor() {
    let host = Link::from_str("/host").expect("host");
    let actor =
        Actor::new_falcon512("replication:node-b".to_string()).expect("generate Falcon-512 actor");
    let keyring = KeyringActorResolver::default().with_actor(host.clone(), actor.clone());
    let policy =
        TrustedInstallerPolicy::from_installers(&[], "/lib/example-devco").expect("policy");
    let membership = PeerMembership::new(Vec::new());
    membership.upsert_identity(tinychain::replication::PeerIdentity {
        peer: "http://10.0.0.2:8702".to_string(),
        actor_id: "replication:node-b".to_string(),
        public_key_b64: base64::engine::general_purpose::STANDARD
            .encode(actor.verifying_key().to_bytes()),
    });
    let verifier = TrustedInstallerTokenVerifier::new(
        RjwtTokenVerifier::new(Arc::new(keyring)),
        policy,
        membership,
        "replication:node-a".to_string(),
    );

    let claim = Claim::new(
        Link::from_str("/lib/example-devco/example/1.0.0").expect("claim"),
        USER_READ,
    );
    let token = tinychain::auth::Token::new(
        host,
        SystemTime::now(),
        Duration::from_secs(30),
        actor.id().clone(),
        claim,
    );
    let signed = actor.sign_token(token).expect("signed").into_jwt();

    let result = verifier.verify(signed).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn trusted_installer_policy_rejects_host_replication_outside_cluster_root() {
    let host = Link::from_str("/host").expect("host");
    let actor =
        Actor::new_falcon512("replication:node-a".to_string()).expect("generate Falcon-512 actor");
    let keyring = KeyringActorResolver::default().with_actor(host.clone(), actor.clone());
    let policy =
        TrustedInstallerPolicy::from_installers(&[], "/lib/example-devco").expect("policy");
    let verifier = TrustedInstallerTokenVerifier::new(
        RjwtTokenVerifier::new(Arc::new(keyring)),
        policy,
        PeerMembership::new(Vec::new()),
        "replication:node-a".to_string(),
    );

    let claim = Claim::new(
        Link::from_str("/lib/otherco/private/1.0.0").expect("claim"),
        USER_READ,
    );
    let token = tinychain::auth::Token::new(
        host,
        SystemTime::now(),
        Duration::from_secs(30),
        actor.id().clone(),
        claim,
    );
    let signed = actor.sign_token(token).expect("signed").into_jwt();

    let result = verifier.verify(signed).await;
    assert!(matches!(
        result,
        Err(tinychain::txn::TxnError::Unauthorized)
    ));
}

#[test]
fn trusted_installer_policy_accepts_service_prefixes() {
    let policy = TrustedInstallerPolicy::from_installers(
        &[TrustedInstaller {
            host: "http://127.0.0.1:8702".to_string(),
            actor_id: "trusted-installer".to_string(),
            public_key_b64: "AAAA".to_string(),
            allowed_lib_prefixes: vec!["/service/example-devco".to_string()],
        }],
        "/service/example-devco",
    )
    .expect("policy");

    assert_eq!(policy.replication_root(), "/service/example-devco");
}
