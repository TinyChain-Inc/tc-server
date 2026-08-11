use std::time::Duration;

use bytes::Bytes;
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
    let body_text = String::from_utf8_lossy(body).to_string();
    let Ok(serde_json::Value::Object(error)) = serde_json::from_slice(body) else {
        return (body_text, None);
    };
    let Some(serde_json::Value::Object(data)) = error.values().next() else {
        return (body_text, None);
    };
    let message = data
        .get("message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(&body_text)
        .to_string();
    let pressure = data
        .get("pressure")
        .and_then(serde_json::Value::as_object)
        .and_then(|pressure| {
            let resource = pressure.get("resource")?.as_str()?;
            let reason = pressure.get("reason")?.as_str()?.parse().ok()?;
            let mut decoded = tc_error::Pressure::new(resource, reason);
            if let Some(retry_after_ms) = pressure
                .get("retry_after_ms")
                .and_then(serde_json::Value::as_u64)
            {
                decoded = decoded.with_retry_after_ms(retry_after_ms);
            }
            if let Some(reliability) = pressure
                .get("reliability")
                .and_then(serde_json::Value::as_str)
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
