use std::io;

use bytes::Bytes;
use futures::{Stream, TryStreamExt, stream, stream::BoxStream};
use safecast::TryCastFrom;
use tc_error::{TCError, TCResult};
use tc_ir::{IntoView, Scalar};

use crate::{State, txn::TxnHandle};

use super::{Body, Response, StatusCode, header};

/// Project a native result at the HTTP boundary.
pub(crate) async fn native_state_response(
    state: State,
    txn: TxnHandle,
    request: Option<crate::KernelRequestGuard>,
) -> TCResult<Response> {
    let view = state.into_view(txn.clone()).await?;
    let stream = json_stream(view);
    let stream: BoxStream<'static, Result<Bytes, io::Error>> = match request {
        Some(request) => Box::pin(CompletionStream::new(stream, request)),
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
    json_stream_response(json_stream(value))
}

fn json_stream_response(stream: BoxStream<'static, Result<Bytes, io::Error>>) -> Response {
    http::Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::wrap_stream(stream))
        .expect("state response")
}

struct CompletionStream {
    stream: BoxStream<'static, Result<Bytes, io::Error>>,
    request: Option<crate::KernelRequestGuard>,
    deadline: std::pin::Pin<Box<tokio::time::Sleep>>,
    timed_out: bool,
}

impl CompletionStream {
    fn new(
        stream: BoxStream<'static, Result<Bytes, io::Error>>,
        request: crate::KernelRequestGuard,
    ) -> Self {
        let deadline = request.deadline();
        Self {
            stream,
            request: Some(request),
            deadline: Box::pin(tokio::time::sleep_until(deadline.instant())),
            timed_out: false,
        }
    }
}

impl Stream for CompletionStream {
    type Item = Result<Bytes, io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        if !this.timed_out && this.deadline.as_mut().poll(cx).is_ready() {
            this.timed_out = true;
            if let Some(request) = this.request.take() {
                let err = request.deadline().exceeded();
                return std::task::Poll::Ready(Some(Err(io::Error::other(err.to_string()))));
            }
        }

        if this.timed_out {
            return std::task::Poll::Ready(None);
        }

        match this.stream.as_mut().poll_next(cx) {
            std::task::Poll::Ready(Some(Err(err))) => {
                this.request.take();
                std::task::Poll::Ready(Some(Err(err)))
            }
            std::task::Poll::Ready(None) => {
                this.request.take();
                std::task::Poll::Ready(None)
            }
            poll => poll,
        }
    }
}

fn json_stream<T>(value: T) -> BoxStream<'static, Result<Bytes, io::Error>>
where
    T: for<'en> destream::en::IntoStream<'en> + Send + 'static,
{
    match destream_json::encode(value) {
        Ok(stream) => Box::pin(stream.map_err(|err| io::Error::other(err.to_string()))),
        Err(err) => Box::pin(stream::once(async move {
            Err(io::Error::other(err.to_string()))
        })),
    }
}
