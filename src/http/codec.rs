use std::{io, sync::Arc};

use bytes::Bytes;
use futures::{TryStreamExt, stream};
use tc_error::{TCError, TCResult};
use tc_state::State;

use super::{Body, Response, StatusCode, header};

pub(crate) async fn decode_state_bytes(
    body: Bytes,
    txn: Arc<dyn tc_ir::Transaction>,
) -> TCResult<State> {
    if body.is_empty() || body.iter().all(|b| b.is_ascii_whitespace()) {
        return Ok(State::None);
    }

    let stream = stream::iter(vec![Ok::<Bytes, io::Error>(body)]);
    destream_json::try_decode(txn, stream)
        .await
        .map_err(|err| TCError::bad_request(err.to_string()))
}

pub(crate) fn state_response(state: State) -> Response {
    match destream_json::encode(state) {
        Ok(stream) => http::Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::wrap_stream(
                stream.map_err(|err| io::Error::other(err.to_string())),
            ))
            .expect("state response"),
        Err(err) => http::Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from(err.to_string()))
            .expect("state encode error response"),
    }
}
