use std::io;

use bytes::Bytes;
use futures::{TryStreamExt, stream};
use tc_error::{TCError, TCResult};
use tc_state::State;

use super::{Body, Response, StatusCode, header};

pub(crate) async fn decode_state_bytes_with_context(
    body: Bytes,
    context: tc_state::StateContext,
) -> TCResult<State> {
    if body.is_empty() || body.iter().all(|b| b.is_ascii_whitespace()) {
        return Ok(State::None);
    }

    let stream = stream::iter(vec![Ok::<Bytes, io::Error>(body)]);
    destream_json::try_decode(context, stream)
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

    #[cfg(feature = "pyo3")]
    if contains_btree(&state) {
        let mut response = http::Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .expect("native state response");
        response
            .extensions_mut()
            .insert(NativeStateResponse::new(native_state));
        return response;
    }

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
            let mut response = response;

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

#[cfg(feature = "pyo3")]
fn contains_btree(state: &State) -> bool {
    match state {
        State::Collection(tc_state::Collection::BTree(_)) => true,
        State::Map(map) => map.values().any(contains_btree),
        State::Tuple(items) => items.iter().any(contains_btree),
        State::None | State::Scalar(_) | State::Collection(_) => false,
    }
}
