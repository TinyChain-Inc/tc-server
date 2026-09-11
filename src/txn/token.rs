use std::str::FromStr;

use pathlink::Link;
use tc_ir::TxnId;

pub(crate) struct ProtocolSnapshot {
    pub owner: Option<(String, String)>,
    pub locked: bool,
    pub leaders: std::collections::BTreeMap<pathlink::PathBuf, (String, String)>,
}

pub(crate) fn protocol_snapshot(
    txn_id: TxnId,
    token: &crate::auth::SignedToken,
) -> tc_error::TCResult<ProtocolSnapshot> {
    let txn_link = Link::from_str(&crate::uri::transaction_path(txn_id))
        .map_err(|_| tc_error::TCError::bad_request("invalid transaction claim"))?;
    let mut snapshot = ProtocolSnapshot {
        owner: None,
        locked: false,
        leaders: std::collections::BTreeMap::new(),
    };
    let mut owner_seen = false;
    let chain = token.claims().iter().collect::<Vec<_>>();
    for (host, actor, claims) in chain.into_iter().rev() {
        let claims = crate::auth::claims_from_wire(claims.clone());
        for claim in claims.iter().filter(|claim| claim.link == txn_link) {
            if claim.link == txn_link {
                let principal = (host.to_string(), actor.to_string());
                if claim.mask.has(umask::USER_EXEC) {
                    if snapshot.owner.replace(principal.clone()).is_some() {
                        return Err(tc_error::TCError::bad_request(
                            "token contains multiple transaction owners",
                        ));
                    }
                    owner_seen = true;
                }
                if claim.mask.has(umask::USER_WRITE) {
                    if snapshot.owner.as_ref() != Some(&principal) {
                        return Err(tc_error::TCError::bad_request(
                            "transaction lock does not match its owner",
                        ));
                    }
                    if snapshot.locked {
                        return Err(tc_error::TCError::bad_request(
                            "token contains multiple transaction locks",
                        ));
                    }
                    snapshot.locked = true;
                }
            }
        }
        for claim in claims.into_iter().filter(|claim| claim.link != txn_link) {
            if claim
                .link
                .to_string()
                .starts_with(crate::uri::HOST_TXN_PREFIX)
            {
                return Err(tc_error::TCError::bad_request(
                    "token contains a claim for another transaction",
                ));
            } else if owner_seen && claim.mask.has(umask::USER_EXEC) {
                let path = claim.link.path().clone();
                let leader = (host.to_string(), actor.to_string());
                if snapshot
                    .leaders
                    .insert(path.clone(), leader.clone())
                    .is_some_and(|existing| existing != leader)
                {
                    return Err(tc_error::TCError::conflict(format!(
                        "resource {path} has conflicting leaders"
                    )));
                }
            }
        }
    }
    Ok(snapshot)
}

pub(crate) fn validate_signed_token(
    txn_id: TxnId,
    token: &crate::auth::SignedToken,
) -> tc_error::TCResult<()> {
    let snapshot = protocol_snapshot(txn_id, token)?;
    snapshot
        .owner
        .ok_or_else(|| tc_error::TCError::bad_request("transaction has no owner"))?;
    Ok(())
}
