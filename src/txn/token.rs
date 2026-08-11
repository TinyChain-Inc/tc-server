use std::str::FromStr;

use pathlink::Link;
use tc_ir::{Claim, TxnId};

use super::TxnError;

pub(crate) fn owner_id_from_token(
    txn_id: TxnId,
    token: &crate::auth::TokenContext,
) -> Result<String, TxnError> {
    validate_authority(
        txn_id,
        token
            .claims
            .iter()
            .enumerate()
            .map(|(index, (host, actor, claim))| (index, host.as_str(), actor.as_str(), claim)),
        false,
    )
    .map(|authority| authority.owner)
    .map_err(|_| TxnError::Unauthorized)
}

pub(crate) fn validate_signed_token(
    txn_id: TxnId,
    token: &crate::auth::SignedToken,
) -> tc_error::TCResult<Claim> {
    validate_authority(
        txn_id,
        token
            .claims()
            .iter()
            .enumerate()
            .map(|(index, (host, actor, claim))| {
                (index, host.to_string(), actor.to_string(), claim)
            }),
        true,
    )
    .map(|authority| authority.claim)
    .map_err(tc_error::TCError::bad_request)
}

struct Authority {
    owner: String,
    claim: Claim,
}

fn validate_authority<'a, H, A>(
    txn_id: TxnId,
    claims: impl IntoIterator<Item = (usize, H, A, &'a Claim)>,
    enforce_position: bool,
) -> Result<Authority, &'static str>
where
    H: AsRef<str>,
    A: AsRef<str>,
{
    let txn_link =
        Link::from_str(&format!("/txn/{txn_id}")).map_err(|_| "invalid transaction claim")?;
    let mut owner: Option<(String, String)> = None;
    let mut lock: Option<(String, String)> = None;
    let mut canonical_claim = None;

    for (index, host, actor, claim) in claims {
        if claim.link != txn_link {
            if claim.link.to_string().starts_with("/txn/") {
                return Err("token contains a claim for another transaction");
            }
            continue;
        }
        if enforce_position && index > 1 {
            return Err("canonical transaction claim must be first or second");
        }
        canonical_claim.get_or_insert_with(|| claim.clone());
        let principal = (host.as_ref().to_string(), actor.as_ref().to_string());
        if claim.mask.has(umask::USER_EXEC) && owner.replace(principal.clone()).is_some() {
            return Err("token contains multiple transaction owners");
        }
        if claim.mask.has(umask::USER_WRITE) && lock.replace(principal).is_some() {
            return Err("token contains multiple transaction locks");
        }
    }

    let claim = canonical_claim.ok_or("token is missing the canonical transaction claim")?;
    let owner = owner.ok_or("transaction claim does not identify an owner")?;
    if lock.is_some_and(|lock| lock != owner) {
        return Err("transaction lock does not match its owner");
    }

    Ok(Authority {
        owner: format!("{}::{}", owner.0, owner.1),
        claim,
    })
}
