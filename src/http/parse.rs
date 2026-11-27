use futures::TryStreamExt;
use hyper::body::HttpBody;
use hyper::header::AUTHORIZATION;
use tc_error::{TCError, TCResult};
use tokio::sync::OwnedSemaphorePermit;

use crate::BodyContract;
use super::Request;

pub(crate) fn parse_bearer_token(req: &Request) -> Option<String> {
    req.headers()
        .get(AUTHORIZATION)?
        .to_str()
        .ok()
        .and_then(crate::auth::bearer_token)
        .map(str::to_owned)
}

/// Decode transport representations without selecting or executing an operation.
pub(crate) async fn decode_body(
    req: Request,
    request: &crate::BoundRequest,
) -> TCResult<(Option<crate::State>, Option<OwnedSemaphorePermit>)> {
    let content_type = req
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());

    let contract = request.body_contract(content_type)?;
    let (limit, admission) = match contract {
        BodyContract::Native(_) if req.body().size_hint().exact() == Some(0) => {
            return Ok((None, None));
        }
        BodyContract::Native(limit) => (limit, None),
        BodyContract::Definition(limit) | BodyContract::Wasm(limit) => {
            (limit, Some(request.application_admission()))
        }
    };

    let mut body = crate::http_body::BoundedBody::new(req.into_body(), limit, admission);
    let result = if matches!(contract, BodyContract::Wasm(_)) {
        (&mut body)
            .try_fold(Vec::new(), |mut bytes, chunk| async move {
                bytes.extend_from_slice(&chunk);
                Ok(bytes)
            })
            .await
            .map_err(|error| TCError::bad_request(error.to_string()))
            .and_then(|bytes| crate::wasm_installation(bytes.into()))
    } else {
        request.decode_json(&mut body).await
    };

    let state = result.map_err(|error| body.take_failure().unwrap_or(error))?;
    Ok((Some(state), body.permit.take()))
}
