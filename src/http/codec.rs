use std::io;

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{TryStreamExt, stream};
use safecast::TryCastFrom;
use tc_error::{TCError, TCResult};
use tc_ir::Scalar;

use crate::State;
use super::{Body, Response, StatusCode, header};

/// Project a native result at the HTTP boundary.
pub(crate) async fn native_state_response(
    state: State,
    request: crate::BoundRequest,
) -> TCResult<Response> {
    let view = request.txn().view(state, request.deadline()).await?;
    let stream = crate::http_body::json_stream(view);
    let stream = request_stream(stream, request);
    Ok(json_stream_response(stream))
}

/// Encode bounded transport-only state which cannot contain persistent collections.
pub(crate) fn state_response<Txn: tc_collection::StorageContext>(
    state: tc_state::State<Txn>,
) -> Response {
    match Scalar::try_cast_from(state, |_| {
        TCError::bad_request("transport endpoint returned non-scalar state")
    }) {
        Ok(scalar) => json_response(scalar),
        Err(err) => super::response::tc_error_response(err),
    }
}

fn json_response<T>(value: T) -> Response
where
    T: for<'en> destream::en::IntoStream<'en> + Send + 'static,
{
    json_stream_response(crate::http_body::json_stream(value))
}

fn json_stream_response(stream: BoxStream<'static, Result<Bytes, io::Error>>) -> Response {
    http::Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::wrap_stream(stream))
        .expect("state response")
}

fn request_stream(
    stream: BoxStream<'static, Result<Bytes, io::Error>>,
    request: crate::BoundRequest,
) -> BoxStream<'static, Result<Bytes, io::Error>> {
    let deadline = request.deadline();
    Box::pin(stream::try_unfold(
        (stream, request),
        move |(mut stream, request)| async move {
            request.txn().check_active().map_err(io::Error::other)?;
            deadline.check().map_err(io::Error::other)?;
            let next = deadline.wait(stream.try_next()).await;
            request.txn().check_active().map_err(io::Error::other)?;
            deadline.check().map_err(io::Error::other)?;
            let next = next.map_err(io::Error::other)??;
            Ok(next.map(|bytes| (bytes, (stream, request))))
        },
    ))
}

#[cfg(test)]
#[path = "../../tests/support/http_body.rs"]
mod tests;
