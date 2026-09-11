use hyper::body::HttpBody;
use hyper::header::AUTHORIZATION;
use tc_error::TCResult;

use super::Request;

pub(crate) fn parse_bearer_token(req: &Request) -> Option<String> {
    req.headers()
        .get(AUTHORIZATION)?
        .to_str()
        .ok()
        .and_then(crate::auth::bearer_token)
        .map(str::to_owned)
}

pub(crate) async fn decode_native_body(
    req: Request,
    txn: crate::txn::TxnHandle,
    max_request_bytes: usize,
) -> TCResult<Option<tc_state::State<crate::txn::TxnHandle>>> {
    if req.body().size_hint().exact() == Some(0) {
        return Ok(None);
    }
    let mut stream = crate::http_body::BoundedBody::new(req.into_body(), max_request_bytes, None);
    match destream_json::try_decode(txn, &mut stream).await {
        Ok(state) => Ok(Some(state)),
        Err(err) => Err(stream.decode_error(err)),
    }
}
