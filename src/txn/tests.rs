use super::*;
use std::str::FromStr;

#[tokio::test]
async fn workspace_subcontexts_preserve_transaction_identity() {
    let root = std::env::temp_dir().join(format!(
        "tc-txn-workspace-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    ));
    let workspace = crate::HostStorage::new(&crate::HostLimits::default().storage)
        .workspace(root)
        .expect("workspace");
    let txn = test_txn_with_workspace("workspace-test", workspace);
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
async fn workspace_cleanup_removes_every_transaction_child_together() {
    let root = std::env::temp_dir().join(format!(
        "tc-txn-workspace-cleanup-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    ));
    let workspace = crate::HostStorage::new(&crate::HostLimits::default().storage)
        .workspace(root)
        .expect("workspace");
    let ttl = std::time::Duration::from_millis(10);
    let txn_server = TxnServer::test_with("workspace-cleanup-test", ttl, Some(workspace.clone()));
    txn_server.start_expiry(&tokio::runtime::Handle::current());
    let txn = txn_server.test_txn();

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

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while txn_server.contains(&txn.id()) {
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

#[tokio::test]
async fn named_collection_uses_the_canonical_uri_once() {
    let root = std::env::temp_dir().join(format!(
        "tc-named-collection-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    ));
    let workspace = crate::HostStorage::new(&crate::HostLimits::default().storage)
        .workspace(&root)
        .expect("workspace");
    let uri = pathlink::Link::from_str("/state/collection/btree/example").expect("URI");
    let dir = workspace
        .named_collection("btree", &uri)
        .await
        .expect("named collection directory");

    let dir = dir.read().await;
    assert!(dir.get_dir("state").is_none());
    drop(dir);
}

#[tokio::test]
async fn named_btrees_isolate_transaction_deltas_by_canonical_uri() {
    let root = std::env::temp_dir().join(format!(
        "tc-named-btree-deltas-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    ));
    let workspace = crate::HostStorage::new(&crate::HostLimits::default().storage)
        .workspace(root)
        .expect("workspace");
    let txn = test_txn_with_workspace("named-btree-test", workspace.clone());
    let first_uri = pathlink::Link::from_str("/state/collection/btree/first").expect("URI");
    let second_uri = pathlink::Link::from_str("/state/collection/btree/second").expect("URI");

    let first = tc_collection::btree::BTree::named(
        &first_uri,
        workspace
            .named_collection("btree", &first_uri)
            .await
            .expect("first persistent directory"),
        tc_collection::btree::BTreeSchema::default(),
    )
    .expect("first BTree");
    let second = tc_collection::btree::BTree::named(
        &second_uri,
        workspace
            .named_collection("btree", &second_uri)
            .await
            .expect("second persistent directory"),
        tc_collection::btree::BTreeSchema::default(),
    )
    .expect("second BTree");

    first
        .insert_row(&txn, vec![tc_value::Value::from(1_u64)])
        .await
        .expect("first insert");
    second
        .insert_row(&txn, vec![tc_value::Value::from(2_u64)])
        .await
        .expect("second insert");

    let btree_dir = txn
        .subcontext("state")
        .subcontext("collection")
        .subcontext("btree")
        .context()
        .await
        .expect("BTree transaction directory");
    let btree_dir = btree_dir.read().await;
    let first = btree_dir.get_dir("first").expect("first delta directory");
    let second = btree_dir.get_dir("second").expect("second delta directory");
    assert!(first.read().await.get_dir("inserts").is_some());
    assert!(second.read().await.get_dir("inserts").is_some());
}

#[tokio::test]
async fn named_tables_isolate_transaction_deltas_by_canonical_uri() {
    let root = std::env::temp_dir().join(format!(
        "tc-named-table-deltas-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    ));
    let workspace = crate::HostStorage::new(&crate::HostLimits::default().storage)
        .workspace(root)
        .expect("workspace");
    let txn = test_txn_with_workspace("named-table-test", workspace.clone());
    let first_uri = pathlink::Link::from_str("/state/collection/table/first").expect("URI");
    let second_uri = pathlink::Link::from_str("/state/collection/table/second").expect("URI");
    let schema = tc_collection::table::TableSchema::new(
        vec![tc_collection::table::Column {
            name: "id".parse().expect("column name"),
            dtype: tc_value::ValueType::Number,
        }],
        vec![tc_collection::table::Column {
            name: "value".parse().expect("column name"),
            dtype: tc_value::ValueType::String,
        }],
        Vec::new(),
        tc_collection::btree::StorageConfig::default(),
    )
    .expect("table schema");

    let first = tc_collection::table::PersistentTable::named(
        &first_uri,
        workspace
            .named_collection("table", &first_uri)
            .await
            .expect("first persistent directory"),
        schema.clone(),
    )
    .expect("first Table");
    let second = tc_collection::table::PersistentTable::named(
        &second_uri,
        workspace
            .named_collection("table", &second_uri)
            .await
            .expect("second persistent directory"),
        schema,
    )
    .expect("second Table");

    first
        .upsert_row(
            &txn,
            vec![tc_value::Value::from(1_u64)],
            vec![tc_value::Value::from("first")],
        )
        .await
        .expect("first upsert");
    second
        .upsert_row(
            &txn,
            vec![tc_value::Value::from(2_u64)],
            vec![tc_value::Value::from("second")],
        )
        .await
        .expect("second upsert");

    let table_dir = txn
        .subcontext("state")
        .subcontext("collection")
        .subcontext("table")
        .context()
        .await
        .expect("Table transaction directory");
    let table_dir = table_dir.read().await;
    let first = table_dir.get_dir("first").expect("first delta directory");
    let second = table_dir.get_dir("second").expect("second delta directory");
    assert!(first.read().await.get_dir("inserts").is_some());
    assert!(second.read().await.get_dir("inserts").is_some());
}
use crate::auth::{RjwtTokenVerifier, Token, TokenVerifier};
use tc_ir::{Claim, NetworkTime, TxnId};
use umask::Mode;

#[tokio::test]
async fn mints_host_signed_bearer_token_for_unauthenticated_txn() {
    let server = TxnServer::test("test-host");
    let handle = server.test_txn();

    let bearer = handle
        .raw_token()
        .expect("anonymous transactions must carry host-signed protocol bearer tokens");

    let txn_link = pathlink::Link::from_str(&format!("/txn/{}", handle.id())).expect("txn link");
    assert!(handle.has_claim(&txn_link, umask::USER_EXEC));
    assert!(handle.has_claim(&txn_link, umask::USER_WRITE));

    let keyring = crate::auth::KeyringActorResolver::default().with_actor(
        server.protocol_host().clone(),
        server.protocol_actor().as_ref().clone(),
    );
    let verifier = RjwtTokenVerifier::new(std::sync::Arc::new(keyring));
    let ctx = verifier
        .verify(bearer.to_string())
        .await
        .expect("host-signed protocol token verifies");
    assert_eq!(
        crate::txn::owner_id_from_token(handle.id(), &ctx).expect("txn owner"),
        format!(
            "{}::{}",
            server.protocol_host(),
            server.protocol_actor().id()
        )
    );
}

#[test]
fn enforces_canonical_claim_position() {
    use std::time::{Duration, SystemTime};

    use rjwt::Actor;

    let handle = test_txn("test-host");
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
            .contains("canonical transaction claim must be first or second")
    );
}

#[test]
fn rejects_signed_token_for_different_transaction_id() {
    use std::time::{Duration, SystemTime};

    use rjwt::Actor;

    let handle = test_txn("test-host");
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
    assert!(err.message().contains("another transaction"));
}

#[test]
fn unknown_txn_continuation_requires_authenticated_owner() {
    let server = TxnServer::test("test-host");
    let unknown = TxnId::from_parts(NetworkTime::from_nanos(7), 7).with_trace([1; 32]);
    let rejected = server.bind(Some(unknown), None, Some("/lib/test/a/1.0.0"));
    assert!(matches!(rejected, Err(TxnError::Unauthorized)));

    let token = txn_token(unknown, "host-a", "owner-a", "/lib/test/a/1.0.0");
    let handle = server
        .bind(Some(unknown), Some(&token), Some("/lib/test/a/1.0.0"))
        .unwrap_or_else(|err| panic!("authenticated continuation rejected: {err:?}"));
    assert_eq!(
        handle.id(),
        unknown,
        "participant continuation must reuse the inbound txn ID"
    );
}

#[test]
fn inbound_transaction_id_is_not_retraced() {
    let server = TxnServer::test("test-host");
    let inbound = TxnId::from_parts(NetworkTime::from_nanos(11), 3);
    assert!(
        inbound.trace_bytes().iter().all(|byte| *byte == 0),
        "test fixture must exercise a zero-trace inbound ID"
    );

    let token = txn_token(inbound, "host-a", "owner-a", "/lib/test/a/1.0.0");
    let handle = server
        .bind(Some(inbound), Some(&token), Some("/lib/test/a/1.0.0"))
        .unwrap_or_else(|err| panic!("authenticated continuation rejected: {err:?}"));

    assert_eq!(
        handle.id(),
        inbound,
        "participant nodes must preserve the exact inbound transaction ID"
    );
    assert!(server.contains(&inbound));
}

#[test]
fn attaches_structured_auth_context_to_txn_handle() {
    let handle = test_txn("test-host");

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

fn txn_token(txn_id: TxnId, host: &str, actor: &str, component: &str) -> crate::auth::TokenContext {
    crate::auth::TokenContext::new(format!("{host}::{actor}"), "token")
        .with_claim(
            host.to_string(),
            actor.to_string(),
            Claim::new(
                pathlink::Link::from_str(&format!("/txn/{txn_id}")).expect("transaction claim"),
                umask::USER_EXEC | umask::USER_WRITE,
            ),
        )
        .with_claim(
            host.to_string(),
            actor.to_string(),
            Claim::new(
                pathlink::Link::from_str(component).expect("component claim"),
                Mode::all(),
            ),
        )
}
