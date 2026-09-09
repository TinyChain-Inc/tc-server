use std::time::Duration;

use bytes::Bytes;
use serde::Deserialize;
use tc_error::{TCError, TCResult};

pub(crate) const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) async fn send(
    client: &hyper::Client<hyper::client::HttpConnector, hyper::Body>,
    request: hyper::Request<hyper::Body>,
    deadline: crate::resources::Deadline,
) -> TCResult<(hyper::StatusCode, Bytes)> {
    let response = deadline
        .run(async {
            client
                .request(request)
                .await
                .map_err(|err| TCError::bad_gateway(err.to_string()))
        })
        .await?;

    let status = response.status();
    let body = deadline
        .run(async {
            hyper::body::to_bytes(response.into_body())
                .await
                .map_err(|err| TCError::bad_gateway(err.to_string()))
        })
        .await?;

    Ok((status, body))
}

pub(crate) fn error_from_status(status: hyper::StatusCode, body: Bytes) -> TCError {
    let (message, pressure) = decode_error_body(&body);
    match status {
        hyper::StatusCode::BAD_REQUEST => TCError::bad_request(message),
        hyper::StatusCode::UNAUTHORIZED => TCError::unauthorized(message),
        hyper::StatusCode::NOT_FOUND => TCError::not_found(message),
        hyper::StatusCode::CONFLICT => TCError::conflict(message),
        hyper::StatusCode::METHOD_NOT_ALLOWED => TCError::method_not_allowed("request", message),
        hyper::StatusCode::BAD_GATEWAY => TCError::bad_gateway(message),
        hyper::StatusCode::PAYLOAD_TOO_LARGE => TCError::payload_too_large(
            message,
            pressure.unwrap_or_else(|| {
                tc_error::Pressure::new(
                    "/host/resource/remote/payload",
                    tc_error::PressureReason::QuotaExceeded,
                )
            }),
        ),
        hyper::StatusCode::TOO_MANY_REQUESTS => TCError::too_many_requests(
            message,
            pressure.unwrap_or_else(|| {
                tc_error::Pressure::new(
                    "/host/resource/remote/quota",
                    tc_error::PressureReason::QuotaExceeded,
                )
            }),
        ),
        hyper::StatusCode::SERVICE_UNAVAILABLE => TCError::resource_unavailable(
            message,
            pressure.unwrap_or_else(|| {
                tc_error::Pressure::new(
                    "/host/resource/remote",
                    tc_error::PressureReason::Saturated,
                )
            }),
        ),
        _ => TCError::internal(message),
    }
}

fn decode_error_body(body: &[u8]) -> (String, Option<tc_error::Pressure>) {
    #[derive(Deserialize)]
    struct ErrorData {
        message: Option<String>,
        pressure: Option<PressureData>,
    }

    #[derive(Deserialize)]
    struct PressureData {
        resource: String,
        reason: String,
        retry_after_ms: Option<u64>,
        reliability: Option<String>,
    }

    let body_text = String::from_utf8_lossy(body).to_string();
    let Ok(error) = serde_json::from_slice::<std::collections::BTreeMap<String, ErrorData>>(body)
    else {
        return (body_text, None);
    };
    let Some(data) = error.values().next() else {
        return (body_text, None);
    };
    let message = data.message.clone().unwrap_or(body_text);
    let pressure = data.pressure.as_ref().and_then(|pressure| {
        let reason = pressure.reason.parse().ok()?;
        let mut decoded = tc_error::Pressure::new(&pressure.resource, reason);
        if let Some(retry_after_ms) = pressure.retry_after_ms {
            decoded = decoded.with_retry_after_ms(retry_after_ms);
        }
        if let Some(reliability) = pressure
            .reliability
            .as_deref()
            .and_then(|value| value.parse().ok())
        {
            decoded = decoded.with_reliability(reliability);
        }
        Some(decoded)
    });
    (message, pressure)
}

pub(crate) fn ensure_success(status: hyper::StatusCode, body: Bytes) -> TCResult<Bytes> {
    if status.is_success() {
        Ok(body)
    } else {
        Err(error_from_status(status, body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_remote_pressure_metadata() {
        let body = Bytes::from_static(
            br#"{"temporarily_unavailable":{"message":"busy","stack":[],"pressure":{"reason":"saturated","resource":"/host/resource/rpc","retry_after_ms":250,"reliability":"lossless"}}}"#,
        );
        let err = error_from_status(hyper::StatusCode::SERVICE_UNAVAILABLE, body);

        assert_eq!(err.code(), tc_error::ErrorKind::Unavailable);
        let pressure = err.pressure().unwrap();
        assert_eq!(pressure.resource(), "/host/resource/rpc");
        assert_eq!(pressure.retry_after_ms(), Some(250));
    }
}
