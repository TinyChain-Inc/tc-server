#[cfg(feature = "mdns")]
use super::discovery::MdnsService;
use super::discovery::dedupe_peers;
use super::trusted_installers::{
    TrustedInstaller, TrustedInstallerPolicy, TrustedInstallerTokenVerifier,
};
use base64::Engine as _;
use pathlink::Link;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tinychain::Claim;
use tinychain::auth::{Actor, KeyringActorResolver, RjwtTokenVerifier, TokenVerifier};
use umask::{USER_READ, USER_WRITE};

#[cfg(feature = "mdns")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mdns_service_advertises_and_discovers_without_detaching_its_daemon() {
    use mdns_sd::DaemonStatus;
    use std::net::{SocketAddr, UdpSocket};

    let instance = format!("tinychain-test-{}", std::process::id());
    let port = 30_000 + (std::process::id() % 20_000) as u16;
    let probe = UdpSocket::bind("0.0.0.0:0").expect("bind interface probe");
    probe
        .connect("192.0.2.1:9")
        .expect("select a multicast-capable interface");
    let address = probe.local_addr().expect("local interface").ip();

    let mut advertiser = MdnsService::new().expect("mDNS advertiser");
    let browser = MdnsService::new().expect("mDNS browser");
    let advertiser_daemon = advertiser.daemon();
    let browser_daemon = browser.daemon();
    assert_eq!(mdns_status(&advertiser_daemon), DaemonStatus::Running);
    assert_eq!(mdns_status(&browser_daemon), DaemonStatus::Running);

    advertiser
        .advertise(&instance, address, port)
        .expect("advertise TinyChain service");

    let peers = browser
        .discover(Duration::from_secs(3))
        .await
        .expect("discover TinyChain service");
    assert!(
        peers.contains(&SocketAddr::new(address, port).to_string()),
        "discovered peers did not contain the advertised endpoint: {peers:?}"
    );

    assert_eq!(mdns_status(&advertiser_daemon), DaemonStatus::Running);
    assert_eq!(mdns_status(&browser_daemon), DaemonStatus::Running);

    drop(advertiser);
    drop(browser);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(mdns_status(&advertiser_daemon), DaemonStatus::Shutdown);
    assert_eq!(mdns_status(&browser_daemon), DaemonStatus::Shutdown);
}

#[cfg(feature = "mdns")]
fn mdns_status(daemon: &mdns_sd::ServiceDaemon) -> mdns_sd::DaemonStatus {
    daemon
        .status()
        .expect("request daemon status")
        .recv_timeout(Duration::from_secs(1))
        .expect("receive daemon status")
}
#[test]
fn dedupes_peers() {
    let peers = dedupe_peers(vec!["1.2.3.4:5".to_string(), "1.2.3.4:5".to_string()]);
    assert_eq!(peers.len(), 1);
}

#[tokio::test]
async fn trusted_installer_verifier_rejects_opaque_unauthenticated_tokens() {
    let policy = TrustedInstallerPolicy::from_installers(&[]).expect("policy");
    let verifier = TrustedInstallerTokenVerifier::new(
        RjwtTokenVerifier::new(Arc::new(KeyringActorResolver::default())),
        policy,
    );

    let result = verifier
        .verify("unauthenticated-session-token".to_string())
        .await;
    assert_eq!(
        result.expect_err("opaque token must fail").code(),
        tc_error::ErrorKind::Unauthorized
    );
}

#[tokio::test]
async fn trusted_installer_verifier_still_enforces_claim_policy_for_signed_tokens() {
    let host = Link::from_str("http://127.0.0.1:8702").expect("host");
    let actor =
        Actor::new_falcon512("trusted-installer".to_string()).expect("generate Falcon-512 actor");
    let keyring = KeyringActorResolver::default();
    keyring
        .insert(host.clone(), actor.clone())
        .expect("unique actor");

    let policy = TrustedInstallerPolicy::from_installers(&[TrustedInstaller {
        host: host.to_string(),
        actor_id: "trusted-installer".to_string(),
        algorithm: rjwt::AlgKind::Falcon512,
        public_key_b64: base64::engine::general_purpose::STANDARD
            .encode(actor.verifying_key().to_bytes()),
        allowed_prefixes: vec!["/lib/example-devco".to_string()],
    }])
    .expect("policy");

    let verifier =
        TrustedInstallerTokenVerifier::new(RjwtTokenVerifier::new(Arc::new(keyring)), policy);

    let denied_claim = Claim::new(
        Link::from_str("/lib/otherco/private/1.0.0").expect("claim"),
        USER_WRITE,
    );
    let token = tinychain::auth::Token::new(
        host,
        SystemTime::now(),
        Duration::from_secs(30),
        actor.id().clone(),
        tinychain::auth::wire_claim(denied_claim),
    );
    let signed = actor.sign_token(token).expect("signed").into_jwt();

    let result = verifier.verify(signed).await;
    assert_eq!(
        result.expect_err("denied claim must fail").code(),
        tc_error::ErrorKind::Unauthorized
    );
}

#[tokio::test]
async fn trusted_installer_policy_rejects_unconfigured_external_actor() {
    let host = Link::from_str("http://127.0.0.1:8702").expect("host");
    let actor =
        Actor::new_falcon512("external-installer".to_string()).expect("generate Falcon-512 actor");
    let keyring = KeyringActorResolver::default();
    keyring
        .insert(host.clone(), actor.clone())
        .expect("unique actor");
    let policy = TrustedInstallerPolicy::from_installers(&[]).expect("policy");
    let verifier =
        TrustedInstallerTokenVerifier::new(RjwtTokenVerifier::new(Arc::new(keyring)), policy);

    let claim = Claim::new(
        Link::from_str("/lib/example-devco/example/1.0.0").expect("claim"),
        USER_WRITE,
    );
    let token = tinychain::auth::Token::new(
        host,
        SystemTime::now(),
        Duration::from_secs(30),
        actor.id().clone(),
        tinychain::auth::wire_claim(claim),
    );
    let signed = actor.sign_token(token).expect("signed").into_jwt();

    let result = verifier.verify(signed).await;
    assert_eq!(
        result.expect_err("unconfigured actor must fail").code(),
        tc_error::ErrorKind::Unauthorized
    );
}

#[tokio::test]
async fn trusted_installer_policy_allows_host_replication_actor() {
    let host = Link::from_str("/host").expect("host");
    let actor = Actor::new_falcon512("node-a".to_string()).expect("generate Falcon-512 actor");
    let keyring = KeyringActorResolver::default();
    keyring
        .insert(host.clone(), actor.clone())
        .expect("unique actor");
    let policy = TrustedInstallerPolicy::from_installers(&[]).expect("policy");
    let verifier =
        TrustedInstallerTokenVerifier::new(RjwtTokenVerifier::new(Arc::new(keyring)), policy);

    let claim = Claim::new(
        Link::from_str("/lib/example-devco/example/1.0.0").expect("claim"),
        USER_READ,
    );
    let token = tinychain::auth::Token::new(
        host,
        SystemTime::now(),
        Duration::from_secs(30),
        actor.id().clone(),
        tinychain::auth::wire_claim(claim),
    );
    let signed = actor.sign_token(token).expect("signed").into_jwt();

    let result = verifier.verify(signed).await;
    assert!(result.is_ok());
}

#[test]
fn trusted_installer_policy_accepts_service_prefixes() {
    TrustedInstallerPolicy::from_installers(&[TrustedInstaller {
        host: "http://127.0.0.1:8702".to_string(),
        actor_id: "trusted-installer".to_string(),
        algorithm: rjwt::AlgKind::Falcon512,
        public_key_b64: "AAAA".to_string(),
        allowed_prefixes: vec!["/service/example-devco".to_string()],
    }])
    .expect("policy");
}
