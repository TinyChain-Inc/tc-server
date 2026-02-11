use std::str::FromStr;

use pathlink::Link;
use tc_ir::TxnId;

use super::TxnError;

pub(crate) fn owner_id_from_token(
    txn_id: TxnId,
    token: &crate::auth::TokenContext,
) -> Result<String, TxnError> {
    let txn_link = format!("/txn/{txn_id}");
    let txn_link = Link::from_str(&txn_link).map_err(|_| TxnError::Unauthorized)?;

    let mut owner: Option<(&str, &str)> = None;
    let mut lock: Option<(&str, &str)> = None;

    for (host, actor_id, claim) in &token.claims {
        if claim.link != txn_link {
            if claim.link.to_string().starts_with("/txn/") {
                return Err(TxnError::Unauthorized);
            }
            continue;
        }

        if claim.mask.has(umask::USER_EXEC) {
            if owner.is_some() {
                return Err(TxnError::Unauthorized);
            }
            owner = Some((host.as_str(), actor_id.as_str()));
        }

        if claim.mask.has(umask::USER_WRITE) {
            if lock.is_some() {
                return Err(TxnError::Unauthorized);
            }
            lock = Some((host.as_str(), actor_id.as_str()));
        }
    }

    if let Some(lock) = lock {
        if let Some(owner) = owner {
            if owner != lock {
                return Err(TxnError::Unauthorized);
            }
        } else {
            return Err(TxnError::Unauthorized);
        }
    }

    let Some((host, actor_id)) = owner else {
        return Err(TxnError::Unauthorized);
    };

    Ok(format!("{host}::{actor_id}"))
}
