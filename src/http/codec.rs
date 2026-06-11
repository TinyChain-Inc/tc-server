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

#[cfg_attr(not(feature = "pyo3"), allow(dead_code))]
#[derive(Clone)]
pub(crate) struct NativeStateResponse {
    state: State,
}

#[cfg_attr(not(feature = "pyo3"), allow(dead_code))]
impl NativeStateResponse {
    pub(crate) fn new(state: State) -> Self {
        Self { state }
    }

    pub(crate) fn clone_state(&self) -> State {
        self.state.clone()
    }

    pub(crate) fn is_none(&self) -> bool {
        self.state.is_none()
    }
}

pub(crate) fn state_response(state: State) -> Response {
    #[cfg(feature = "pyo3")]
    let native_state = state.clone();

    match destream_json::encode(state) {
        Ok(stream) => {
            let response = http::Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::wrap_stream(
                    stream.map_err(|err| io::Error::other(err.to_string())),
                ))
                .expect("state response");

            #[cfg(feature = "pyo3")]
            {
                response
                    .extensions_mut()
                    .insert(NativeStateResponse::new(native_state));
            }

            response
        }
        Err(err) => http::Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from(err.to_string()))
            .expect("state encode error response"),
    }
}
