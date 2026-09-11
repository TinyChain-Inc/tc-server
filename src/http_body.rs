use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{Stream, StreamExt, stream};
use hyper::body::HttpBody;
use tc_error::{Pressure, PressureReason, TCError, TCResult};
use tokio::sync::OwnedSemaphorePermit;

use crate::{Deadline, HostResources};

pub(crate) fn json_stream<T>(value: T) -> futures::stream::BoxStream<'static, io::Result<Bytes>>
where
    T: for<'en> destream::en::IntoStream<'en> + Send + 'static,
{
    match destream_json::encode(value) {
        Ok(stream) => stream
            .map(|result| result.map_err(|error| io::Error::other(error.to_string())))
            .boxed(),
        Err(error) => stream::once(async move { Err(io::Error::other(error.to_string())) }).boxed(),
    }
}

pub(crate) fn json_body<T>(value: T) -> hyper::Body
where
    T: for<'en> destream::en::IntoStream<'en> + Send + 'static,
{
    hyper::Body::wrap_stream(json_stream(value))
}

/// A pull-driven HTTP body with one finite byte bound.
pub(crate) struct BoundedBody {
    body: hyper::Body,
    read: usize,
    limit: usize,
    admission: Option<(HostResources, Deadline)>,
    pub(crate) permit: Option<OwnedSemaphorePermit>,
    pending: Option<Pin<Box<dyn Future<Output = TCResult<(OwnedSemaphorePermit, Bytes)>> + Send>>>,
    failure: Option<TCError>,
}

impl BoundedBody {
    pub(crate) fn new(
        body: hyper::Body,
        limit: usize,
        admission: Option<(HostResources, Deadline)>,
    ) -> Self {
        Self {
            body,
            read: 0,
            limit,
            admission,
            permit: None,
            pending: None,
            failure: None,
        }
    }

    #[cfg(feature = "http-server")]
    pub(crate) fn decode_error(&mut self, error: impl std::fmt::Display) -> TCError {
        self.failure
            .take()
            .unwrap_or_else(|| TCError::bad_request(error.to_string()))
    }

    #[cfg(feature = "http-client")]
    pub(crate) fn take_failure(&mut self) -> Option<TCError> {
        self.failure.take()
    }

    fn fail(&mut self, error: TCError) -> Poll<Option<io::Result<Bytes>>> {
        self.failure = Some(error.clone());
        Poll::Ready(Some(Err(io::Error::other(error.to_string()))))
    }
}

impl Stream for BoundedBody {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(pending) = self.pending.as_mut() {
                match pending.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok((permit, chunk))) => {
                        if let Some(admission) = self.permit.as_mut() {
                            admission.merge(permit);
                        } else {
                            self.permit = Some(permit);
                        }
                        self.pending = None;
                        return Poll::Ready(Some(Ok(chunk)));
                    }
                    Poll::Ready(Err(error)) => {
                        self.pending = None;
                        return self.fail(error);
                    }
                }
            }

            match Pin::new(&mut self.body).poll_data(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(error))) => {
                    return self.fail(TCError::bad_gateway(format!(
                        "HTTP body transport failed: {error}"
                    )));
                }
                Poll::Ready(Some(Ok(chunk))) => {
                    let limit = self.limit;
                    let Some(read) = self.read.checked_add(chunk.len()) else {
                        return self.fail(body_limit_error(limit));
                    };
                    if read > limit {
                        return self.fail(body_limit_error(limit));
                    }
                    self.read = read;
                    if chunk.is_empty() || self.admission.is_none() {
                        return Poll::Ready(Some(Ok(chunk)));
                    }

                    let (resources, deadline) = self
                        .admission
                        .as_ref()
                        .expect("admission was checked above")
                        .clone();
                    let bytes = chunk.len();
                    self.pending = Some(Box::pin(async move {
                        let permit = resources.admit_application_bytes(bytes, deadline).await?;
                        Ok((permit, chunk))
                    }));
                }
            }
        }
    }
}

pub(crate) fn body_limit_error(limit: usize) -> TCError {
    TCError::payload_too_large(
        format!("HTTP body exceeds the {limit}-byte limit"),
        Pressure::new("/host/resource/http/body", PressureReason::QuotaExceeded),
    )
}
