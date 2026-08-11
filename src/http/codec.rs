use std::io;

use bytes::Bytes;
use futures::{FutureExt, Stream, TryStreamExt, future::BoxFuture, stream, stream::BoxStream};
use safecast::TryCastFrom;
use tc_error::{TCError, TCResult};
use tc_ir::{IntoView, Scalar};

use crate::{State, txn::TxnHandle};

use super::{Body, Response, StatusCode, header};

pub(crate) async fn decode_state_bytes_with_context(
    body: Bytes,
    txn: TxnHandle,
) -> TCResult<State> {
    if body.is_empty() || body.iter().all(|b| b.is_ascii_whitespace()) {
        return Ok(State::None);
    }

    let stream = stream::iter(vec![Ok::<Bytes, io::Error>(body)]);
    destream_json::try_decode(txn, stream)
        .await
        .map_err(|err| TCError::bad_request(err.to_string()))
}

/// Project a native result at the HTTP boundary.
pub(crate) async fn native_state_response(
    state: State,
    txn: TxnHandle,
    finalize: Option<(crate::Kernel, crate::resources::CapacityPermit)>,
) -> TCResult<Response> {
    let view = state.into_view(txn.clone()).await?;
    let stream = json_stream(view);
    let stream: BoxStream<'static, Result<Bytes, io::Error>> = match finalize {
        Some((kernel, request)) => Box::pin(FinalizingStream::new(stream, kernel, txn, request)),
        None => stream,
    };
    Ok(json_stream_response(stream))
}

/// Encode bounded transport-only state which cannot contain persistent collections.
pub(crate) fn state_response<Txn>(state: tc_state::State<Txn>) -> Response {
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

struct FinalizingStream {
    stream: BoxStream<'static, Result<Bytes, io::Error>>,
    finalize: Option<(crate::Kernel, TxnHandle)>,
    future: Option<BoxFuture<'static, TCResult<()>>>,
    deadline: std::pin::Pin<Box<tokio::time::Sleep>>,
    timed_out: bool,
    _request: crate::resources::CapacityPermit,
}

impl FinalizingStream {
    fn new(
        stream: BoxStream<'static, Result<Bytes, io::Error>>,
        kernel: crate::Kernel,
        txn: TxnHandle,
        request: crate::resources::CapacityPermit,
    ) -> Self {
        let deadline = txn.deadline();
        Self {
            stream,
            finalize: Some((kernel, txn)),
            future: None,
            deadline: Box::pin(tokio::time::sleep_until(deadline.instant())),
            timed_out: false,
            _request: request,
        }
    }

    fn begin_finalize(&mut self) {
        if self.future.is_some() {
            return;
        }

        let Some((kernel, txn)) = self.finalize.take() else {
            return;
        };
        self.future = Some(
            async move {
                kernel
                    .complete_transaction(txn, crate::txn::TransactionOutcome::Succeeded)
                    .await
            }
            .boxed(),
        );
    }
}

impl Stream for FinalizingStream {
    type Item = Result<Bytes, io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        if !this.timed_out && this.future.is_none() && this.deadline.as_mut().poll(cx).is_ready() {
            this.timed_out = true;
            if let Some((_kernel, txn)) = this.finalize.take() {
                let err = txn.deadline().exceeded();
                return std::task::Poll::Ready(Some(Err(io::Error::other(err.to_string()))));
            }
        }

        if this.timed_out {
            return std::task::Poll::Ready(None);
        }

        if let Some(future) = &mut this.future {
            return match std::pin::Pin::new(future).poll(cx) {
                std::task::Poll::Ready(Ok(())) => std::task::Poll::Ready(None),
                std::task::Poll::Ready(Err(err)) => {
                    std::task::Poll::Ready(Some(Err(io::Error::other(err.to_string()))))
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            };
        }

        match this.stream.as_mut().poll_next(cx) {
            std::task::Poll::Ready(Some(Err(err))) => {
                // An abandoned implicit transaction is rolled back by the one
                // transaction TTL worker; adapters only release the view.
                this.finalize.take();
                std::task::Poll::Ready(Some(Err(err)))
            }
            std::task::Poll::Ready(None) => {
                this.begin_finalize();
                self.poll_next(cx)
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
