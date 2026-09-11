use super::*;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::auth::Actor;
fn current_txn_id(nonce: u16) -> TxnId {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time")
        .as_nanos() as u64;
    TxnId::from_parts(NetworkTime::from_nanos(now), nonce)
}
#[tokio::test]
async fn workspace_subcontexts_preserve_transaction_identity() {
    let workspace = test_workspace("subcontexts");
    let txn = test_txn_with_workspace("workspace-test", workspace).await;
    let named = txn
        .subcontext("state")
        .subcontext("collection")
        .subcontext("btree");
    let temporary = txn.subcontext_unique();
    assert_eq!(named.id(), txn.id());
    assert_eq!(temporary.id(), txn.id());
    let named = named.context().await.expect("named context");
    let temporary = temporary.context().await.expect("temporary context");
    let mut named = named.write().await;
    named
        .get_or_create_dir("named-only".to_string())
        .expect("named child");
    drop(named);
    let temporary = temporary.read().await;
    assert!(temporary.get_dir("named-only").is_none());
}
#[tokio::test]
async fn cluster_claims_are_request_local_and_deterministic() {
    let root =
        std::env::temp_dir().join(format!("tc-txn-resource-enlistment-{}", std::process::id()));
    std::fs::create_dir_all(&root).expect("workspace root");
    let first: pathlink::PathBuf = "/lib".parse().expect("path");
    let second: pathlink::PathBuf = "/class".parse().expect("path");
    let workspace = crate::HostStorage::new(&crate::HostLimits::default().storage)
        .workspace(&root)
        .expect("workspace");
    let kernel = test_kernel_with(
        "resource-enlistment",
        std::time::Duration::from_secs(3),
        workspace.clone(),
    )
    .await;
    let txn = kernel.test_txn().await;
    assert!(txn.raw_token().is_none(), "allocation must be ownerless");
    kernel
        .inner
        .libraries
        .claim(&txn)
        .await
        .expect("first claim");
    kernel
        .inner
        .classes
        .claim(&txn)
        .await
        .expect("second claim");
    kernel
        .inner
        .libraries
        .claim(&txn)
        .await
        .expect("idempotent claim");
    assert!(
        txn.raw_token().is_some(),
        "first cluster must claim ownership"
    );
    let resources = txn
        .claimed_paths()
        .into_iter()
        .map(|path| path.to_string())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        resources,
        std::collections::BTreeSet::from([first.to_string(), second.to_string()])
    );
    assert!(!txn.protocol_claims.lock().mutated);
    let ids = workspace.transaction_ids().await.expect("transaction IDs");
    assert!(
        ids.is_empty(),
        "application claims must not create workspaces"
    );
    std::fs::remove_dir_all(root).expect("remove workspace root");
}
#[tokio::test]
async fn workspace_cleanup_removes_every_transaction_child_together() {
    let workspace = test_workspace("cleanup");
    let ttl = std::time::Duration::from_millis(10);
    let kernel = test_kernel_with("workspace-cleanup-test", ttl, workspace.clone()).await;
    let txn = kernel.test_txn().await;
    txn.subcontext("state")
        .subcontext("collection")
        .subcontext("btree")
        .context()
        .await
        .expect("named context");
    txn.subcontext_unique()
        .context()
        .await
        .expect("temporary context");
    assert!(
        workspace
            .has_transaction(txn.id())
            .await
            .expect("transaction exists")
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while workspace
            .has_transaction(txn.id())
            .await
            .expect("transaction state")
        {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("TTL workspace cleanup");
    assert!(
        !workspace
            .has_transaction(txn.id())
            .await
            .expect("transaction removed")
    );
}
use crate::auth::{RjwtTokenVerifier, Token, TokenVerifier};
use tc_ir::{NetworkTime, TxnId};

use crate::Claim;
use umask::Mode;
#[tokio::test]
async fn mints_host_signed_bearer_token_for_unauthenticated_txn() {
    let kernel = test_kernel("test-host").await;
    let server = &kernel.txn_server;
    let handle = kernel.test_txn().await;
    assert!(handle.raw_token().is_none(), "allocation must be ownerless");
    kernel
        .inner
        .libraries
        .claim(&handle)
        .await
        .expect("first cluster claims transaction ownership");
    let bearer = handle
        .raw_token()
        .expect("the first cluster must mint a protocol bearer token");
    let txn_link =
        pathlink::Link::from_str(&crate::uri::transaction_path(handle.id())).expect("txn link");
    assert!(handle.has_claim(&txn_link, umask::USER_EXEC));
    assert!(!handle.has_claim(&txn_link, umask::USER_WRITE));
    let keyring = crate::auth::KeyringActorResolver::default();
    keyring
        .insert(
            server.protocol_authority().host.clone(),
            server.protocol_authority().actor.as_ref().clone(),
        )
        .expect("unique protocol actor");
    let verifier = RjwtTokenVerifier::new(std::sync::Arc::new(keyring));
    let ctx = verifier
        .verify(bearer.to_string())
        .await
        .expect("host-signed protocol token verifies");
    let snapshot = crate::txn::protocol_snapshot(
        handle.id(),
        ctx.signed.as_deref().expect("verified signed token"),
    )
    .expect("protocol claims");
    assert_eq!(
        snapshot.owner,
        Some((
            server.protocol_authority().host.to_string(),
            server.protocol_authority().actor.id().to_string(),
        ))
    );
}

#[tokio::test]
async fn preserves_an_append_only_claim_chain() {
    use std::time::{Duration, SystemTime};

    use rjwt::Actor;

    let handle = test_txn("test-host").await;
    let txn_id = handle.id();

    let txn_claim = Claim::new(
        pathlink::Link::from_str(&crate::uri::transaction_path(txn_id)).expect("txn claim"),
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
        crate::auth::wire_claim(auth_claim.clone()),
    );
    let signed = actor.sign_token(token).expect("signed token");
    let signed = actor
        .consume_and_sign(
            signed,
            host.clone(),
            crate::auth::wire_claim(txn_claim.clone()),
            now,
        )
        .expect("consume token");
    let updated = handle.with_signed_token(signed).expect("token accepted");
    assert!(updated.has_claim(&txn_claim.link, txn_claim.mask));

    let other_claim = Claim::new(
        pathlink::Link::from_str("/lib/other").expect("other link"),
        Mode::all(),
    );
    let final_claim = Claim::new(
        pathlink::Link::from_str("/lib/final").expect("final link"),
        Mode::all(),
    );
    let token = Token::new(
        host.clone(),
        now,
        ttl,
        actor.id().clone(),
        crate::auth::wire_claim(txn_claim),
    );
    let signed = actor.sign_token(token).expect("signed token");
    let signed = actor
        .consume_and_sign(
            signed,
            host.clone(),
            crate::auth::wire_claim(other_claim),
            now,
        )
        .expect("consume token");
    let signed = actor
        .consume_and_sign(signed, host, crate::auth::wire_claim(final_claim), now)
        .expect("consume token");
    handle
        .with_signed_token(signed)
        .expect("later resource grants preserve the transaction owner");
}

#[tokio::test]
async fn rejects_signed_token_for_different_transaction_id() {
    use std::time::{Duration, SystemTime};

    use rjwt::Actor;

    let handle = test_txn("test-host").await;
    let other_txn_id = TxnId::from_parts(NetworkTime::from_nanos(99), 1).with_trace([9; 32]);

    let host = pathlink::Link::from_str("/host").expect("host link");
    let actor = Actor::new_falcon512("actor-a".to_string()).expect("generate Falcon-512 actor");
    let now = SystemTime::now();
    let token = Token::new(
        host,
        now,
        Duration::from_secs(30),
        actor.id().clone(),
        crate::auth::wire_claim(Claim::new(
            pathlink::Link::from_str(&crate::uri::transaction_path(other_txn_id))
                .expect("other txn claim"),
            umask::USER_EXEC,
        )),
    );
    let signed = actor.sign_token(token).expect("signed token");

    let err = handle
        .with_signed_token(signed)
        .expect_err("token for a different transaction must be rejected");
    assert!(err.message().contains("another transaction"));
}

#[tokio::test]
async fn unknown_txn_continuation_requires_authenticated_owner() {
    let kernel = test_kernel("test-host").await;
    let server = &kernel.txn_server;
    let unknown = current_txn_id(7).with_trace([1; 32]);
    let rejected = server
        .bind(Some(unknown), None, Arc::clone(&kernel.inner))
        .await;
    assert!(rejected.is_err());

    let token = txn_token(unknown, "host-a", "owner-a", "/lib/test/a/1.0.0");
    let handle = server
        .bind(Some(unknown), Some(&token), Arc::clone(&kernel.inner))
        .await
        .unwrap_or_else(|err| panic!("authenticated continuation rejected: {err:?}"));
    assert_eq!(
        handle.id(),
        unknown,
        "peer continuation must reuse the inbound txn ID"
    );
}

#[tokio::test]
async fn inbound_transaction_id_is_not_retraced() {
    let kernel = test_kernel("test-host").await;
    let server = &kernel.txn_server;
    let inbound = current_txn_id(3);
    assert!(
        inbound.trace_bytes().iter().all(|byte| *byte == 0),
        "test fixture must exercise a zero-trace inbound ID"
    );

    let token = txn_token(inbound, "host-a", "owner-a", "/lib/test/a/1.0.0");
    let handle = server
        .bind(Some(inbound), Some(&token), Arc::clone(&kernel.inner))
        .await
        .unwrap_or_else(|err| panic!("authenticated continuation rejected: {err:?}"));

    assert_eq!(
        handle.id(),
        inbound,
        "peer nodes must preserve the exact inbound transaction ID"
    );
    assert!(server.contains(&inbound));
}

#[tokio::test]
async fn attaches_structured_auth_context_to_txn_handle() {
    let handle = test_txn("test-host").await;

    let claim = Claim::new(
        pathlink::Link::from_str("/lib/example-devco/a/0.1.0").expect("claim link"),
        Mode::all(),
    );
    let mut token = crate::auth::AuthContext::new("http://127.0.0.1:8702::example-admin");
    token = token.with_claim(
        "http://127.0.0.1:8702".to_string(),
        "example-admin".to_string(),
        claim,
    );

    let handle = handle.with_auth_context(token);
    let auth = handle.auth_context().expect("auth context");
    assert_eq!(auth.principal, "http://127.0.0.1:8702::example-admin");
    assert_eq!(auth.claims.len(), 1);
    assert_eq!(
        auth.token_hosts(),
        vec!["http://127.0.0.1:8702".to_string()]
    );
}

fn txn_token(txn_id: TxnId, host: &str, actor: &str, component: &str) -> crate::auth::AuthContext {
    let signer = Actor::new_falcon512(actor.to_string()).expect("test transaction actor");
    let host_link: pathlink::Link = format!("http://{host}")
        .parse()
        .expect("test transaction host");
    let txn_link =
        pathlink::Link::from_str(&crate::uri::transaction_path(txn_id)).expect("transaction claim");
    let component_link = pathlink::Link::from_str(component).expect("component claim");
    let claims = std::collections::BTreeMap::from([
        (
            txn_link.path().clone(),
            u32::from(umask::USER_EXEC | umask::USER_WRITE),
        ),
        (component_link.path().clone(), u32::from(Mode::all())),
    ]);
    let signed = signer
        .sign_token(Token::new(
            host_link,
            SystemTime::now(),
            Duration::from_secs(30),
            signer.id().clone(),
            claims,
        ))
        .expect("signed transaction token");
    let mut context = crate::auth::AuthContext::new(format!("{host}::{actor}"));
    context.signed = Some(Arc::new(signed));
    context
        .with_claim(
            host.to_string(),
            actor.to_string(),
            Claim::new(txn_link, umask::USER_EXEC | umask::USER_WRITE),
        )
        .with_claim(
            host.to_string(),
            actor.to_string(),
            Claim::new(component_link, Mode::all()),
        )
}
