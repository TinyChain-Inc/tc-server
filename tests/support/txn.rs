use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tc_collection::StorageContext;
use tc_ir::{NetworkTime, TxnId};
use umask::Mode;

use crate::Claim;
use crate::auth::{Actor, RjwtTokenVerifier, SignedToken, Token, TokenVerifier};
use super::*;

impl TxnHandle {
    pub(crate) fn claimed_paths(&self) -> std::collections::BTreeSet<pathlink::PathBuf> {
        self.protocol_snapshot()
            .map(|claims| claims.leaders.into_keys().collect())
            .unwrap_or_default()
    }
}

pub(crate) async fn test_txn(host_id: &str) -> TxnHandle {
    let kernel = test_kernel(host_id).await;
    kernel.test_txn().await
}

async fn test_txn_with_workspace(host_id: &str, workspace: crate::Workspace) -> TxnHandle {
    let kernel = test_kernel_with(host_id, std::time::Duration::from_secs(3), workspace).await;
    kernel.test_txn().await
}

pub(crate) async fn test_kernel(host_id: &str) -> crate::Kernel {
    test_kernel_with(
        host_id,
        std::time::Duration::from_secs(3),
        test_workspace(host_id),
    )
    .await
}

pub(crate) async fn test_kernel_with(
    host_id: &str,
    ttl: std::time::Duration,
    workspace: crate::Workspace,
) -> crate::Kernel {
    test_kernel_with_limits(
        host_id,
        workspace,
        crate::HostLimits {
            transaction_ttl: ttl,
            ..Default::default()
        },
    )
    .await
}

pub(crate) async fn test_kernel_with_limits(
    host_id: &str,
    workspace: crate::Workspace,
    limits: crate::HostLimits,
) -> crate::Kernel {
    let (authority, _) = workspace
        .load_or_create_protocol_authority(
            &host_id.parse().expect("test host ID"),
            crate::uri::HOST_ROOT.parse().expect("host root"),
        )
        .await
        .expect("test protocol authority");

    let storage = crate::HostStorage::new(&crate::HostLimits::default().storage);
    let application_roots = storage
        .application_roots(test_path(&format!("apps-{host_id}")))
        .await
        .expect("test application roots");

    let resources = crate::HostResources::new(limits).unwrap();
    let config = TxnConfig::new(authority.clone(), workspace.clone(), resources.clone());
    let verifier = server::test_verifier(&config);
    let actors = crate::auth::KeyringActorResolver::default();
    let bootstrap = std::sync::Arc::new(
        crate::replication::ReplicationIssuer::local(&authority, actors.clone())
            .expect("test replication issuer"),
    );

    crate::Kernel::new(
        crate::HostServices {
            application_roots,
            replication: std::sync::Arc::new(crate::replication::LocalClusterGateway),
            rpc: std::sync::Arc::new(crate::gateway::LocalRpcGateway),
            resources,
            protocol: authority,
            verifier,
            actors,
            bootstrap,
            bootstrap_required: false,
        },
        workspace,
    )
    .await
    .expect("construct test kernel")
}

pub(crate) fn test_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "tc-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

pub(crate) fn test_workspace(name: &str) -> crate::Workspace {
    crate::HostStorage::new(&crate::HostLimits::default().storage)
        .workspace(test_path(&format!("txn-{name}")))
        .expect("test workspace")
}

fn current_txn_id(nonce: u16) -> TxnId {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time")
        .as_nanos() as u64;
    TxnId::from_parts(NetworkTime::from_nanos(now), nonce)
}

#[tokio::test]
async fn a_bound_transaction_deadline_can_only_be_restricted() {
    let txn = test_txn("deadline-restriction").await;
    let original = txn.deadline();
    let later = crate::Deadline::after(Duration::from_secs(60));
    let unchanged = txn.restrict_deadline(later);
    assert_eq!(unchanged.deadline().instant(), original.instant());

    let earlier = crate::Deadline::after(Duration::from_millis(1));
    let restricted = txn.restrict_deadline(earlier);
    assert_eq!(restricted.deadline().instant(), earlier.instant());
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
        .test_libraries()
        .claim(&txn)
        .await
        .expect("first claim");
    kernel
        .test_classes()
        .claim(&txn)
        .await
        .expect("second claim");
    kernel
        .test_libraries()
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

    assert!(!txn.is_locked(), "claiming does not select a decision");
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

    let txn_id = txn.id();
    drop(txn);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while workspace
            .has_transaction(txn_id)
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
            .has_transaction(txn_id)
            .await
            .expect("transaction removed")
    );
}

fn signed_claim_chain(actor: &Actor, claims: impl IntoIterator<Item = Claim>) -> SignedToken {
    let host = pathlink::Link::from_str(crate::uri::HOST_ROOT).expect("host link");
    let now = SystemTime::now();
    let mut claims = claims.into_iter();
    let first = claims.next().expect("nonempty claim chain");
    let token = Token::new(
        host.clone(),
        now,
        Duration::from_secs(30),
        actor.id().clone(),
        crate::auth::wire_claim(first),
    )
    .expect("valid token time");

    claims.fold(
        actor.sign_token(token).expect("signed token"),
        |token, claim| {
            actor
                .consume_and_sign(token, host.clone(), crate::auth::wire_claim(claim), now)
                .expect("extend signed token")
        },
    )
}

fn claim(path: &str) -> Claim {
    Claim::new(
        pathlink::Link::from_str(path).expect("claim link"),
        umask::USER_EXEC,
    )
}

fn snapshot_error(txn_id: TxnId, token: &SignedToken) -> tc_error::TCError {
    match crate::txn::protocol_snapshot(txn_id, token) {
        Ok(_) => panic!("invalid protocol claims must fail"),
        Err(error) => error,
    }
}

#[tokio::test]
async fn mints_host_signed_bearer_token_for_unauthenticated_txn() {
    let kernel = test_kernel("test-host").await;
    let server = kernel.test_txn_server();
    let handle = kernel.test_txn().await;
    assert!(handle.raw_token().is_none(), "allocation must be ownerless");
    kernel
        .test_libraries()
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
            server.protocol_authority().host().clone(),
            Actor::with_verifying_key(
                server.protocol_authority().actor_id().to_string(),
                server.protocol_authority().verifying_key(),
            ),
        )
        .expect("unique protocol actor");
    let verifier = RjwtTokenVerifier::new(std::sync::Arc::new(keyring));
    let ctx = verifier
        .verify(bearer.to_string())
        .await
        .expect("host-signed protocol token verifies");
    let snapshot = crate::txn::protocol_snapshot(
        handle.id(),
        ctx.signed()
            .map(Arc::as_ref)
            .expect("verified signed token"),
    )
    .expect("protocol claims");
    assert_eq!(
        snapshot.owner,
        Some((
            server.protocol_authority().host().to_string(),
            server.protocol_authority().actor_id().to_string(),
        ))
    );
}

#[tokio::test]
async fn automatic_rollback_requires_ownership_but_not_successful_mutation() {
    let kernel = test_kernel("automatic-rollback").await;
    let owner = kernel.test_txn().await;
    assert!(
        owner
            .lock_coordinator(TransactionOutcome::Rollback, true)
            .unwrap()
            .is_none()
    );

    assert!(!owner.is_locked());

    kernel.test_libraries().claim(&owner).await.unwrap();
    assert!(!owner.is_locked());
    let successful = kernel.test_txn().await;
    kernel.test_libraries().claim(&successful).await.unwrap();
    assert!(
        successful
            .lock_coordinator(TransactionOutcome::Commit, true)
            .unwrap()
            .is_some(),
        "successful owner execution can commit without a separate mutation flag"
    );

    let participant = kernel.test_txn().await;
    let actor = Actor::new("other-owner".into(), rjwt::AlgKind::Falcon512).unwrap();
    let resource = owner.claimed_paths().into_iter().next().unwrap();
    let token = signed_claim_chain(
        &actor,
        [
            claim(&crate::uri::transaction_path(participant.id())),
            Claim::new(resource.clone().into(), umask::USER_EXEC),
        ],
    );

    let participant = participant.with_signed_token(token).unwrap();
    assert!(
        participant
            .lock_coordinator(TransactionOutcome::Rollback, true)
            .unwrap()
            .is_none(),
        "a participant must report the error to its owner"
    );

    assert!(!participant.is_locked());

    assert_eq!(
        owner
            .lock_coordinator(TransactionOutcome::Rollback, true)
            .unwrap(),
        Some(resource)
    );

    assert!(owner.is_locked());
    assert!(
        owner
            .lock_coordinator(TransactionOutcome::Rollback, true)
            .unwrap()
            .is_none(),
        "automatic failure handling must not repeat a decision"
    );

    kernel.shutdown().await;
}

#[tokio::test]
async fn read_only_scope_is_signed_and_branch_local() {
    use pathlink::{Link, PathLabel, path_label};

    const TARGET: PathLabel = path_label(&["service", "test", "read-only", "1.0.0"]);
    let kernel = test_kernel("read-only-branch").await;
    let owner = kernel.test_txn().await;
    kernel.test_libraries().claim(&owner).await.unwrap();
    owner.check_write().unwrap();

    let read = owner.read_only();
    assert_eq!(
        read.check_write().unwrap_err().code(),
        tc_error::ErrorKind::Unauthorized
    );

    let branch = read
        .grant_claim(Claim::new(
            TARGET.into(),
            umask::USER_READ | umask::USER_WRITE,
        ))
        .await
        .unwrap();

    let snapshot = branch.protocol_snapshot().unwrap();
    assert!(snapshot.read_only);
    assert!(
        !snapshot.leaders.contains_key(Link::from(TARGET).path()),
        "delegated permission does not select the destination leader"
    );

    assert!(!owner.protocol_snapshot().unwrap().read_only);
    assert!(
        !owner
            .protocol_snapshot()
            .unwrap()
            .leaders
            .contains_key(Link::from(TARGET).path())
    );

    owner.check_write().unwrap();
    assert_eq!(
        branch.check_write().unwrap_err().code(),
        tc_error::ErrorKind::Unauthorized
    );

    let peer = Actor::new("read-only-peer".into(), rjwt::AlgKind::Falcon512).unwrap();
    let txn = Claim::new(
        crate::uri::transaction_path(owner.id()).parse().unwrap(),
        umask::USER_EXEC,
    );

    let signed = signed_claim_chain(
        &peer,
        [
            txn.clone(),
            Claim::new(TARGET.into(), umask::USER_EXEC),
            Claim::new(txn.link, umask::USER_READ),
        ],
    );

    let context = crate::auth::AuthContext::new(peer.id().clone()).with_signed(signed);
    let inbound = kernel
        .test_bind(Some(owner.id()), Some(&context))
        .await
        .unwrap();

    assert_eq!(
        inbound.check_write().unwrap_err().code(),
        tc_error::ErrorKind::Unauthorized
    );

    assert_eq!(
        inbound.branch().check_write().unwrap_err().code(),
        tc_error::ErrorKind::Unauthorized
    );

    assert!(
        inbound
            .lock_coordinator(TransactionOutcome::Commit, true)
            .unwrap()
            .is_none()
    );

    kernel.shutdown().await;
}

#[tokio::test]
async fn decision_ancestry_is_outcome_bound_and_branch_local() {
    use pathlink::{Link, PathBuf, PathLabel, path_label};

    const FIRST: PathLabel = path_label(&["service", "test", "first", "1.0.0"]);
    const SECOND: PathLabel = path_label(&["service", "test", "second", "1.0.0"]);
    let kernel = test_kernel("decision-branches").await;
    let owner = kernel.test_txn().await;
    kernel.test_libraries().claim(&owner).await.unwrap();
    let coordinator = owner
        .lock_coordinator(TransactionOutcome::Commit, true)
        .unwrap()
        .unwrap();

    assert!(owner.protocol_snapshot().unwrap().visited.is_empty());
    assert_eq!(
        owner.protocol_snapshot().unwrap().decision,
        Some(TransactionOutcome::Commit)
    );

    assert_eq!(
        owner
            .lock_coordinator(TransactionOutcome::Rollback, false)
            .unwrap_err()
            .code(),
        tc_error::ErrorKind::Conflict
    );

    let root = owner
        .decision_branch(&coordinator, TransactionOutcome::Commit)
        .unwrap();

    let first_path = PathBuf::from(FIRST);
    let second_path = PathBuf::from(SECOND);
    let first = root
        .decision_branch(&first_path, TransactionOutcome::Commit)
        .unwrap();

    let sibling = root
        .decision_branch(&second_path, TransactionOutcome::Commit)
        .unwrap();

    let authority = kernel.test_txn_server().protocol_authority();
    let coordinator_entry = (
        authority.host().to_string(),
        authority.actor_id().to_string(),
        coordinator.clone(),
    );

    for branch in [&root, &first, &sibling] {
        assert!(
            branch
                .protocol_snapshot()
                .unwrap()
                .visited
                .contains(&coordinator_entry)
        );
    }

    let first_entry = (
        authority.host().to_string(),
        authority.actor_id().to_string(),
        Link::from(FIRST).path().clone(),
    );

    let second_entry = (
        authority.host().to_string(),
        authority.actor_id().to_string(),
        Link::from(SECOND).path().clone(),
    );

    assert!(
        first
            .protocol_snapshot()
            .unwrap()
            .visited
            .contains(&first_entry)
    );

    assert!(
        !first
            .protocol_snapshot()
            .unwrap()
            .visited
            .contains(&second_entry)
    );

    assert!(
        sibling
            .protocol_snapshot()
            .unwrap()
            .visited
            .contains(&second_entry)
    );

    assert!(
        !sibling
            .protocol_snapshot()
            .unwrap()
            .visited
            .contains(&first_entry)
    );

    assert_eq!(first.protocol_snapshot().unwrap().visited.len(), 2);
    assert_eq!(sibling.protocol_snapshot().unwrap().visited.len(), 2);

    assert_eq!(
        first
            .decision_branch(&second_path, TransactionOutcome::Rollback)
            .unwrap_err()
            .code(),
        tc_error::ErrorKind::Conflict
    );

    assert_eq!(
        first
            .decision_branch(&first_path, TransactionOutcome::Commit)
            .unwrap_err()
            .code(),
        tc_error::ErrorKind::BadRequest
    );

    let child = first
        .decision_branch(&second_path, TransactionOutcome::Commit)
        .unwrap();

    assert_eq!(child.protocol_snapshot().unwrap().visited.len(), 3);
    assert_eq!(first.protocol_snapshot().unwrap().visited.len(), 2);
    assert_eq!(sibling.protocol_snapshot().unwrap().visited.len(), 2);
    assert_eq!(root.protocol_snapshot().unwrap().visited.len(), 1);
    assert!(owner.protocol_snapshot().unwrap().visited.is_empty());

    let rollback = kernel.test_txn().await;
    kernel.test_libraries().claim(&rollback).await.unwrap();
    rollback
        .lock_coordinator(TransactionOutcome::Rollback, false)
        .unwrap()
        .unwrap();

    let claims = rollback.protocol_snapshot().unwrap();
    assert!(claims.locked);
    assert!(claims.visited.is_empty());
    assert_eq!(claims.decision, Some(TransactionOutcome::Rollback));
    assert_eq!(
        rollback
            .lock_coordinator(TransactionOutcome::Commit, false)
            .unwrap_err()
            .code(),
        tc_error::ErrorKind::Conflict
    );

    kernel.shutdown().await;
}

#[test]
fn malformed_read_scopes_and_decision_ancestry_are_rejected() {
    use pathlink::{PathLabel, path_label};

    const FIRST: PathLabel = path_label(&["service", "test", "first", "1.0.0"]);
    const SECOND: PathLabel = path_label(&["service", "test", "second", "1.0.0"]);
    let txn_id = current_txn_id(12);
    let actor = Actor::new("decision-authority".into(), rjwt::AlgKind::Falcon512).unwrap();
    let owner = claim(&crate::uri::transaction_path(txn_id));
    let read = Claim::new(owner.link.clone(), umask::USER_READ);
    let error = snapshot_error(
        txn_id,
        &signed_claim_chain(&actor, [read.clone(), owner.clone()]),
    );

    assert!(
        error
            .message()
            .contains("read-only delegation precedes ownership")
    );

    let lock = Claim::new(owner.link.clone(), umask::USER_WRITE);
    let prefix = [
        owner,
        Claim::new(FIRST.into(), umask::USER_EXEC),
        lock.clone(),
    ];

    let cases = [
        (
            "authority after lock",
            vec![read],
            "authority changed after locking",
        ),
        (
            "repeated lock",
            vec![lock],
            "authority changed after locking",
        ),
        (
            "late leadership",
            vec![Claim::new(SECOND.into(), umask::USER_EXEC)],
            "invalid decision ancestry grant",
        ),
        (
            "mixed outcomes",
            vec![
                Claim::new(FIRST.into(), umask::USER_WRITE),
                Claim::new(SECOND.into(), umask::USER_READ),
            ],
            "inconsistent transaction outcomes",
        ),
        (
            "duplicate ancestor",
            vec![
                Claim::new(FIRST.into(), umask::USER_WRITE),
                Claim::new(FIRST.into(), umask::USER_WRITE),
            ],
            "repeated decision ancestor",
        ),
    ];

    for (name, suffix, expected) in cases {
        let token = signed_claim_chain(&actor, prefix.clone().into_iter().chain(suffix));
        assert!(
            snapshot_error(txn_id, &token).message().contains(expected),
            "{name}"
        );
    }

    let token = signed_claim_chain(&actor, prefix);
    let ambiguous = actor
        .consume_and_sign(
            token,
            crate::uri::HOST_ROOT.parse().unwrap(),
            std::collections::BTreeMap::from([
                (FIRST.into(), u32::from(umask::USER_WRITE)),
                (SECOND.into(), u32::from(umask::USER_WRITE)),
            ]),
            SystemTime::now(),
        )
        .unwrap();

    assert!(
        snapshot_error(txn_id, &ambiguous)
            .message()
            .contains("ambiguous decision ancestry")
    );
}

#[tokio::test]
async fn preserves_an_append_only_claim_chain() {
    let handle = test_txn("test-host").await;
    let txn_id = handle.id();
    let actor = Actor::new("actor-a".to_string(), rjwt::AlgKind::Falcon512)
        .expect("generate Falcon-512 actor");

    let txn_claim = claim(&crate::uri::transaction_path(txn_id));
    let signed = signed_claim_chain(&actor, [txn_claim.clone(), claim("/lib/auth")]);
    let updated = handle.with_signed_token(signed).expect("token accepted");
    assert!(updated.has_claim(&txn_claim.link, txn_claim.mask));
    handle
        .with_signed_token(signed_claim_chain(
            &actor,
            [txn_claim, claim("/lib/other"), claim("/lib/final")],
        ))
        .expect("later resource grants preserve the transaction owner");
}

#[tokio::test]
async fn coordinator_is_the_first_claimed_resource_not_the_first_sorted_path() {
    let handle = test_txn("claim-order").await;
    let txn_id = handle.id();
    let actor =
        Actor::new("actor-a".to_string(), rjwt::AlgKind::Falcon512).expect("generate actor");

    let signed = signed_claim_chain(
        &actor,
        [
            claim(&crate::uri::transaction_path(txn_id)),
            claim("/service/z/1.0.0"),
            claim("/class/a/1.0.0"),
        ],
    );
    let snapshot = crate::txn::protocol_snapshot(txn_id, &signed).expect("protocol snapshot");
    assert_eq!(
        snapshot.coordinator.expect("coordinator").to_string(),
        "/service/z/1.0.0"
    );
}

#[tokio::test]
async fn rejects_resource_leadership_before_transaction_ownership() {
    let handle = test_txn("claim-order-invalid").await;
    let txn_id = handle.id();
    let actor =
        Actor::new("actor-a".to_string(), rjwt::AlgKind::Falcon512).expect("generate actor");

    let signed = signed_claim_chain(
        &actor,
        [
            claim("/lib/a/1.0.0"),
            claim(&crate::uri::transaction_path(txn_id)),
        ],
    );
    let error = snapshot_error(txn_id, &signed);
    assert!(error.message().contains("precedes transaction ownership"));
}

#[tokio::test]
async fn rejects_ambiguous_owners_and_conflicting_leaders() {
    let txn_id = test_txn("ambiguous-claims").await.id();
    let actor =
        Actor::new("actor-a".to_string(), rjwt::AlgKind::Falcon512).expect("generate actor");

    let txn = claim(&crate::uri::transaction_path(txn_id));
    let duplicate_owner = signed_claim_chain(&actor, [txn.clone(), txn.clone()]);
    assert!(
        snapshot_error(txn_id, &duplicate_owner)
            .message()
            .contains("multiple transaction owners")
    );

    let host = pathlink::Link::from_str(crate::uri::HOST_ROOT).expect("host link");
    let now = SystemTime::now();
    let grants = [txn.clone(), claim("/lib/a/1.0.0"), claim("/class/a/1.0.0")]
        .into_iter()
        .map(|claim| (claim.link.path().clone(), claim.mask.into()))
        .collect();
    let token = Token::new(
        host.clone(),
        now,
        Duration::from_secs(30),
        actor.id().clone(),
        grants,
    )
    .expect("valid token time");

    let ambiguous = actor.sign_token(token).expect("sign ambiguous claims");
    assert!(
        snapshot_error(txn_id, &ambiguous)
            .message()
            .contains("ambiguous")
    );

    let first = signed_claim_chain(&actor, [txn, claim("/lib/a/1.0.0")]);
    let other =
        Actor::new("actor-b".to_string(), rjwt::AlgKind::Falcon512).expect("generate other actor");

    let conflicting = other
        .consume_and_sign(
            first,
            host,
            crate::auth::wire_claim(claim("/lib/a/1.0.0")),
            now,
        )
        .expect("append conflicting leader");
    assert_eq!(
        snapshot_error(txn_id, &conflicting).code(),
        tc_error::ErrorKind::Conflict
    );
}

#[tokio::test]
async fn handle_construction_rejects_malformed_protocol_claims() {
    let kernel = test_kernel("constructor-claims").await;
    let unseen = current_txn_id(31);
    let actor = Actor::new("actor-a".to_string(), rjwt::AlgKind::Falcon512)
        .expect("generate Falcon-512 actor");

    let signed = signed_claim_chain(&actor, [claim("/lib/example-devco/a/1.0.0")]);
    let context = crate::auth::AuthContext::new(actor.id().clone()).with_signed(signed);

    let error = kernel
        .test_bind(Some(unseen), Some(&context))
        .await
        .expect_err("resource leadership before transaction ownership must be rejected");

    assert_eq!(error.code(), tc_error::ErrorKind::Unauthorized);
    assert!(!kernel.test_txn_server().contains(&unseen));

    let error = kernel.test_bind(None, Some(&context)).await.unwrap_err();
    assert_eq!(error.code(), tc_error::ErrorKind::BadRequest);
}

#[tokio::test]
async fn rejects_signed_token_for_different_transaction_id() {
    use std::time::{Duration, SystemTime};

    use rjwt::Actor;

    let handle = test_txn("test-host").await;
    let other_txn_id = TxnId::from_parts(NetworkTime::from_nanos(99), 1).with_trace([9; 32]);

    let host = pathlink::Link::from_str(crate::uri::HOST_ROOT).expect("host link");
    let actor = Actor::new("actor-a".to_string(), rjwt::AlgKind::Falcon512)
        .expect("generate Falcon-512 actor");

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
    )
    .expect("valid token time");

    let signed = actor.sign_token(token).expect("signed token");

    let err = handle
        .with_signed_token(signed)
        .expect_err("token for a different transaction must be rejected");
    assert!(err.message().contains("another transaction"));
}

#[tokio::test]
async fn unknown_txn_continuation_requires_authenticated_owner() {
    let kernel = test_kernel("test-host").await;
    let unknown = current_txn_id(7).with_trace([1; 32]);
    let rejected = kernel.test_bind(Some(unknown), None).await;
    assert!(rejected.is_err());

    let token = txn_token(unknown, "host-a", "owner-a", "/lib/test/a/1.0.0");
    let handle = kernel
        .test_bind(Some(unknown), Some(&token))
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
    let server = kernel.test_txn_server();
    let inbound = current_txn_id(3);
    assert!(
        inbound.trace_bytes().iter().all(|byte| *byte == 0),
        "test fixture must exercise a zero-trace inbound ID"
    );

    let token = txn_token(inbound, "host-a", "owner-a", "/lib/test/a/1.0.0");
    let handle = kernel
        .test_bind(Some(inbound), Some(&token))
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

#[test]
fn authentication_debug_redacts_signed_authority() {
    let txn_id = current_txn_id(11);
    let context = txn_token(
        txn_id,
        "host-redacted",
        "actor-redacted",
        "/lib/example/a/1.0.0",
    );
    let debug = format!("{context:?}");
    assert!(debug.contains("<redacted>"));
    assert!(
        !debug.contains("eyJ"),
        "debug output must not contain a JWT"
    );
}

fn txn_token(txn_id: TxnId, host: &str, actor: &str, component: &str) -> crate::auth::AuthContext {
    let signer =
        Actor::new(actor.to_string(), rjwt::AlgKind::Falcon512).expect("test transaction actor");

    let host_link: pathlink::Link = format!("http://{host}")
        .parse()
        .expect("test transaction host");
    let txn_link =
        pathlink::Link::from_str(&crate::uri::transaction_path(txn_id)).expect("transaction claim");
    let component_link = pathlink::Link::from_str(component).expect("component claim");
    let now = SystemTime::now();
    let signed = signer
        .sign_token(
            Token::new(
                host_link.clone(),
                now,
                Duration::from_secs(30),
                signer.id().clone(),
                crate::auth::wire_claim(Claim::new(txn_link.clone(), umask::USER_EXEC)),
            )
            .expect("valid token time"),
        )
        .expect("signed transaction owner");

    let signed = signer
        .consume_and_sign(
            signed,
            host_link.clone(),
            crate::auth::wire_claim(Claim::new(component_link.clone(), umask::USER_EXEC)),
            now,
        )
        .expect("signed resource leadership");

    let signed = signer
        .consume_and_sign(
            signed,
            host_link,
            crate::auth::wire_claim(Claim::new(txn_link.clone(), umask::USER_WRITE)),
            now,
        )
        .expect("signed transaction lock");

    crate::auth::AuthContext::new(format!("{host}::{actor}"))
        .with_signed(signed)
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
