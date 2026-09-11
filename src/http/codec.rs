use std::io;

use crate::{State, txn::TxnHandle};
use bytes::Bytes;
use futures::{TryStreamExt, stream, stream::BoxStream};
use safecast::TryCastFrom;
use tc_error::{TCError, TCResult};
use tc_ir::{IntoView, Method, Scalar};

use super::{Body, Response, StatusCode, header};

/// Project a native result at the HTTP boundary.
pub(crate) async fn native_state_response(
    state: State,
    txn: TxnHandle,
    request: Option<crate::KernelRequestGuard>,
) -> TCResult<Response> {
    if request.as_ref().is_some_and(|request| {
        let (method, target) = request.request();
        method == Method::Get
            && matches!(target, crate::kernel::KernelTarget::Application(target) if target.path().first().is_some_and(|root| root.as_str() == "lib"))
            && matches!(
                state,
                State::Scalar(Scalar::Value(tc_value::Value::Bytes(_)))
            )
    }) {
        let State::Scalar(Scalar::Value(tc_value::Value::Bytes(bytes))) = state else {
            unreachable!("the HTTP representation check matched a byte value")
        };
        let stream = stream::once(async move { Ok(Bytes::from_owner(bytes)) });
        let stream = completion_stream(Box::pin(stream), request.expect("request"));
        return Ok(http::Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/wasm")
            .body(Body::wrap_stream(stream))
            .expect("WASM response"));
    }
    let view = state.into_view(txn.clone()).await?;
    let stream = crate::http_body::json_stream(view);
    let stream: BoxStream<'static, Result<Bytes, io::Error>> = match request {
        Some(request) => completion_stream(stream, request),
        None => stream,
    };
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

fn completion_stream(
    stream: BoxStream<'static, Result<Bytes, io::Error>>,
    request: crate::KernelRequestGuard,
) -> BoxStream<'static, Result<Bytes, io::Error>> {
    let deadline = request.deadline();
    Box::pin(stream::try_unfold(
        (stream, Some(request)),
        move |(mut stream, request)| async move {
            match tokio::time::timeout_at(deadline.instant(), stream.try_next()).await {
                Err(_) => Err(io::Error::other(deadline.exceeded().to_string())),
                Ok(Err(error)) => Err(error),
                Ok(Ok(Some(bytes))) => Ok(Some((bytes, (stream, request)))),
                Ok(Ok(None)) => {
                    let request = request.expect("completion request");
                    deadline
                        .wait(request.finish_success())
                        .await
                        .and_then(|result| result)
                        .map_err(|error| io::Error::other(error.to_string()))?;
                    Ok(None)
                }
            }
        },
    ))
}

#[cfg(test)]
#[path = "../../tests/support/http_body.rs"]
mod tests;
