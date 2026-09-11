use std::time::Duration;

use destream::de::FromStream;
use futures::TryStreamExt;
use tc_error::{TCError, TCResult};

pub(crate) const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) async fn send(
    client: &hyper::Client<hyper::client::HttpConnector, hyper::Body>,
    request: hyper::Request<hyper::Body>,
    deadline: crate::Deadline,
) -> TCResult<hyper::Response<hyper::Body>> {
    deadline
        .run(async {
            client
                .request(request)
                .await
                .map_err(|err| TCError::bad_gateway(err.to_string()))
        })
        .await
}

pub(crate) async fn decode<T>(
    response: hyper::Response<hyper::Body>,
    context: T::Context,
    deadline: crate::Deadline,
    bound: usize,
) -> TCResult<T>
where
    T: FromStream,
{
    let mut body = success_body(response, deadline, bound).await?;
    match deadline
        .wait(destream_json::try_decode(context, &mut body))
        .await
    {
        Err(error) => Err(error),
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(body
            .take_failure()
            .unwrap_or_else(|| TCError::bad_gateway(format!("invalid JSON response: {error}")))),
    }
}

pub(crate) async fn consume(
    response: hyper::Response<hyper::Body>,
    deadline: crate::Deadline,
    bound: usize,
    require_empty: bool,
) -> TCResult<()> {
    let mut body = success_body(response, deadline, bound).await?;
    let result = deadline
        .wait(async {
            while let Some(chunk) = body.try_next().await? {
                if require_empty && !chunk.is_empty() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "expected an empty response body",
                    ));
                }
            }
            Ok::<_, std::io::Error>(())
        })
        .await?;
    result.map_err(|error| {
        body.take_failure()
            .unwrap_or_else(|| TCError::bad_gateway(error.to_string()))
    })
}

pub(crate) async fn collect_bytes(
    response: hyper::Response<hyper::Body>,
    deadline: crate::Deadline,
    bound: usize,
) -> TCResult<std::sync::Arc<[u8]>> {
    let mut body = success_body(response, deadline, bound).await?;
    let result = deadline
        .wait(
            (&mut body).try_fold(Vec::new(), |mut bytes, chunk| async move {
                bytes.extend_from_slice(&chunk);
                Ok(bytes)
            }),
        )
        .await?;
    result.map(Vec::into).map_err(|error| {
        body.take_failure()
            .unwrap_or_else(|| TCError::bad_gateway(error.to_string()))
    })
}

async fn success_body(
    response: hyper::Response<hyper::Body>,
    deadline: crate::Deadline,
    bound: usize,
) -> TCResult<crate::http_body::BoundedBody> {
    if response.status().is_success() {
        return Ok(crate::http_body::BoundedBody::new(
            response.into_body(),
            bound,
            None,
        ));
    }

    let status = response.status();
    let mut body = crate::http_body::BoundedBody::new(response.into_body(), bound, None);
    match deadline
        .wait(destream_json::try_decode::<_, _, TCError>((), &mut body))
        .await
    {
        Err(error) => Err(error),
        Ok(Ok(error)) => Err(error),
        Ok(Err(error)) => Err(body.take_failure().unwrap_or_else(|| {
            TCError::bad_gateway(format!("invalid error response for HTTP {status}: {error}"))
        })),
    }
}

#[cfg(test)]
#[path = "../tests/support/outbound_http.rs"]
mod tests;
