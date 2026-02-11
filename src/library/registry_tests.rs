use super::LibraryRegistry;
use pathlink::Link;
use std::collections::BTreeMap;
use std::str::FromStr;
use tc_ir::{Claim, LibrarySchema};
use umask;

#[test]
fn picks_longest_matching_claim_for_egress() {
    let registry = LibraryRegistry::new(None, BTreeMap::new());

    let parent_id = format!("{}/acme/parent/1.0.0", crate::uri::LIB_ROOT);
    let child_id = format!("{}/acme/parent/1.0.0/child/0.1.0", crate::uri::LIB_ROOT);
    let parent = LibrarySchema::new(
        Link::from_str(&parent_id).expect("parent link"),
        "1.0.0",
        vec![],
    );
    let child = LibrarySchema::new(
        Link::from_str(&child_id).expect("child link"),
        "0.1.0",
        vec![],
    );

    futures::executor::block_on(registry.insert_schema(parent.clone())).expect("insert parent");
    futures::executor::block_on(registry.insert_schema(child.clone())).expect("insert child");

    let txn = crate::txn::TxnManager::with_host_id("test-claim")
        .begin()
        .with_claims(vec![
            Claim::new(parent.id().clone(), umask::USER_READ),
            Claim::new(child.id().clone(), umask::USER_READ),
        ]);

    let resolved = registry.schema_for_txn(&txn).expect("schema");

    assert_eq!(resolved.id(), child.id());
}
