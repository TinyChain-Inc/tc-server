use std::io;

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, stream};
use tc_error::{TCError, TCResult};
use tc_ir::NativeClass;
use tc_state::{BTreeCollection, BTreeType, Collection, State};
use tc_value::Value;

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

/// An optional in-process representation of an HTTP state response.
///
/// The HTTP body remains canonical; PyO3 may use this handle to avoid
/// materializing bytes when the response never leaves the host process.
#[cfg_attr(not(feature = "pyo3"), allow(dead_code))]
#[derive(Clone)]
pub(crate) struct NativeStateResponse(State);

#[cfg_attr(not(feature = "pyo3"), allow(dead_code))]
impl NativeStateResponse {
    pub(crate) fn new(state: State) -> Self {
        Self(state)
    }

    pub(crate) fn clone_state(&self) -> State {
        self.0.clone()
    }

    pub(crate) fn is_none(&self) -> bool {
        self.0.is_none()
    }
}

pub(crate) fn state_response(state: State) -> Response {
    #[cfg(feature = "pyo3")]
    let native_state = state.clone();

    let response = http::Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::wrap_stream(state_json_stream(state)))
        .expect("state response");

    #[cfg(feature = "pyo3")]
    let mut response = response;

    #[cfg(feature = "pyo3")]
    response
        .extensions_mut()
        .insert(NativeStateResponse::new(native_state));

    response
}

pub(crate) fn state_json_stream(state: State) -> BoxStream<'static, Result<Bytes, io::Error>> {
    match state {
        State::Collection(Collection::BTree(btree)) => btree_json_stream(*btree),
        State::Map(map) => map_json_stream(map),
        State::Tuple(items) => tuple_json_stream(items),
        state => json_stream(state),
    }
}

fn json_stream<T>(value: T) -> BoxStream<'static, Result<Bytes, io::Error>>
where
    T: for<'en> destream::en::IntoStream<'en> + Send + 'static,
{
    match destream_json::encode(value) {
        Ok(stream) => Box::pin(stream.map_err(|err| io::Error::other(err.to_string()))),
        Err(err) => error_stream(io::Error::other(err.to_string())),
    }
}

fn bytes_stream(bytes: &'static [u8]) -> BoxStream<'static, Result<Bytes, io::Error>> {
    Box::pin(stream::once(async move { Ok(Bytes::from_static(bytes)) }))
}

fn error_stream(err: io::Error) -> BoxStream<'static, Result<Bytes, io::Error>> {
    Box::pin(stream::once(async move { Err(err) }))
}

fn map_json_stream(map: tc_ir::Map<State>) -> BoxStream<'static, Result<Bytes, io::Error>> {
    let entries = map.into_iter().enumerate().map(|(index, (key, value))| {
        let prefix = if index == 0 {
            b"".as_slice()
        } else {
            b",".as_slice()
        };
        Box::pin(
            bytes_stream(prefix)
                .chain(json_stream(key.to_string()))
                .chain(bytes_stream(b":"))
                .chain(state_json_stream(value)),
        ) as BoxStream<'static, Result<Bytes, io::Error>>
    });

    Box::pin(
        bytes_stream(b"{")
            .chain(stream::iter(entries).flatten())
            .chain(bytes_stream(b"}")),
    )
}

fn tuple_json_stream(items: Vec<State>) -> BoxStream<'static, Result<Bytes, io::Error>> {
    let items = items.into_iter().enumerate().map(|(index, value)| {
        let prefix = if index == 0 {
            b"".as_slice()
        } else {
            b",".as_slice()
        };
        Box::pin(bytes_stream(prefix).chain(state_json_stream(value)))
            as BoxStream<'static, Result<Bytes, io::Error>>
    });

    Box::pin(
        bytes_stream(b"[")
            .chain(stream::iter(items).flatten())
            .chain(bytes_stream(b"]")),
    )
}

fn btree_json_stream(btree: BTreeCollection) -> BoxStream<'static, Result<Bytes, io::Error>> {
    let schema = btree.schema.clone();
    let key_arity = schema.len();
    let rows = stream::once(async move { btree.finalized_key_stream().await })
        .map_ok(move |keys| {
            keys.enumerate()
                .map(move |(index, row)| match row {
                    Ok(row) if row.len() == key_arity => {
                        let value = if key_arity == 1 {
                            row.into_iter().next().expect("unary BTree row")
                        } else {
                            Value::Tuple(row.to_vec())
                        };
                        let prefix = if index == 0 {
                            b"".as_slice()
                        } else {
                            b",".as_slice()
                        };
                        Box::pin(bytes_stream(prefix).chain(json_stream(value)))
                            as BoxStream<'static, Result<Bytes, io::Error>>
                    }
                    Ok(row) => error_stream(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "BTree row arity {} does not match schema arity {key_arity}",
                            row.len()
                        ),
                    )),
                    Err(err) => error_stream(err),
                })
                .flatten()
        })
        .try_flatten();

    Box::pin(
        bytes_stream(b"{")
            .chain(json_stream(BTreeType.path().to_string()))
            .chain(bytes_stream(b":["))
            .chain(json_stream(schema))
            .chain(bytes_stream(b",["))
            .chain(rows)
            .chain(bytes_stream(b"]]}")),
    )
}
