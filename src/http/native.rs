use std::{io, sync::Arc};

use futures::{FutureExt, TryStreamExt};
use tc_error::{TCError, TCResult};
use tc_ir::{LibrarySchema, Route, parse_route_path};
use tc_value::Value;

use crate::library::{NativeLibrary, NativeLibraryHandler};
use crate::{Method, txn::TxnHandle};

use super::parse::decode_value_body;
use super::response::{method_not_allowed, not_found, schema_response, tc_error_response};
use super::server::to_kernel_method;
use super::{Body, Request, StatusCode};

pub(super) fn native_schema_get_handler(schema: LibrarySchema) -> impl crate::KernelHandler {
    move |_req: Request| {
        let schema = schema.clone();
        async move { schema_response(schema) }.boxed()
    }
}

pub(super) fn native_install_not_supported_handler() -> impl crate::KernelHandler {
    |_req: Request| {
        async move {
            hyper::Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Body::from(
                    "native libraries must be installed at compile time",
                ))
                .expect("native install response")
        }
        .boxed()
    }
}

pub(super) fn http_native_routes_handler<H>(
    library: Arc<NativeLibrary<H>>,
) -> impl crate::KernelHandler + 'static
where
    H: NativeLibraryHandler,
{
    move |req: Request| {
        let library = library.clone();
        async move {
            let path = req.uri().path().to_string();
            if !path.starts_with("/lib/") {
                return not_found();
            }

            let relative = &path["/lib".len()..];
            let normalized = if relative.starts_with('/') {
                relative
            } else {
                return not_found();
            };

            let segments = match parse_route_path(normalized) {
                Ok(segments) => segments,
                Err(err) => return tc_error_response(err),
            };

            let routes = library.routes();
            let handler = match routes.as_ref().route(&segments).cloned() {
                Some(handler) => handler,
                None => return not_found(),
            };

            let method = match to_kernel_method(req.method()) {
                Some(method) => method,
                None => return method_not_allowed(),
            };

            let txn = match req.extensions().get::<TxnHandle>().cloned() {
                Some(txn) => txn,
                None => {
                    return tc_error_response(TCError::internal(
                        "missing transaction handle for native library route".to_string(),
                    ));
                }
            };

            let request_value = match decode_value_body(&req).await {
                Ok(Some(value)) => value,
                Ok(None) => Value::None,
                Err(err) => return tc_error_response(err),
            };

            let response_value = match method {
                Method::Get => match handler.get(&txn, request_value.clone()) {
                    Ok(fut) => match fut.await {
                        Ok(value) => value,
                        Err(err) => return tc_error_response(err),
                    },
                    Err(err) => return tc_error_response(err),
                },
                Method::Put => match handler.put(&txn, request_value.clone()) {
                    Ok(fut) => match fut.await {
                        Ok(value) => value,
                        Err(err) => return tc_error_response(err),
                    },
                    Err(err) => return tc_error_response(err),
                },
                Method::Post => match handler.post(&txn, request_value.clone()) {
                    Ok(fut) => match fut.await {
                        Ok(value) => value,
                        Err(err) => return tc_error_response(err),
                    },
                    Err(err) => return tc_error_response(err),
                },
                Method::Delete => match handler.delete(&txn, request_value.clone()) {
                    Ok(fut) => match fut.await {
                        Ok(value) => value,
                        Err(err) => return tc_error_response(err),
                    },
                    Err(err) => return tc_error_response(err),
                },
            };

            match encode_value_body(response_value).await {
                Ok(body) => hyper::Response::builder()
                    .status(StatusCode::OK)
                    .header(hyper::header::CONTENT_TYPE, "application/json")
                    .body(body)
                    .expect("native response"),
                Err(err) => tc_error_response(err),
            }
        }
        .boxed()
    }
}

async fn encode_value_body(value: Value) -> TCResult<Body> {
    let stream = destream_json::encode(value).map_err(|err| TCError::internal(err.to_string()))?;
    let bytes = stream
        .map_err(|err| io::Error::other(err.to_string()))
        .try_fold(Vec::new(), |mut acc, chunk| async move {
            acc.extend_from_slice(&chunk);
            Ok(acc)
        })
        .await
        .map_err(|err| TCError::internal(err.to_string()))?;
    Ok(Body::from(bytes))
}
