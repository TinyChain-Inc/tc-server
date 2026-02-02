use std::str::FromStr;

use pathlink::Link;
use serde::Deserialize;
use tc_error::{TCError, TCResult};
use tc_ir::LibrarySchema;

use crate::{
    KernelHandler,
    library::{RouteMetadata, SchemaRoutes},
    storage::decode_schema_bytes,
    txn::TxnHandle,
};

pub const IR_ARTIFACT_CONTENT_TYPE: &str = "application/tinychain+json";

#[cfg(feature = "http-server")]
pub fn http_ir_route_handler_from_bytes(
    bytes: Vec<u8>,
) -> TCResult<(
    impl KernelHandler<hyper::Request<hyper::Body>, hyper::Response<hyper::Body>>,
    LibrarySchema,
    SchemaRoutes,
)> {
    use futures::FutureExt;
    use hyper::{Body, Request, Response, StatusCode};
    use std::collections::HashMap;
    use tc_ir::{OpRef, Scalar, Subject, TxnHeader, parse_route_path};

    #[derive(Deserialize)]
    struct IrManifest {
        schema: serde_json::Value,
        routes: Vec<IrRoute>,
    }

    #[derive(Deserialize)]
    struct IrRoute {
        path: String,
        #[serde(default)]
        value: Option<serde_json::Value>,
        #[serde(default)]
        op: Option<IrOp>,
    }

    #[derive(Deserialize)]
    struct IrOp {
        method: String,
        path: String,
    }

    let manifest: IrManifest = serde_json::from_slice(&bytes)
        .map_err(|err| TCError::bad_request(format!("invalid ir manifest json: {err}")))?;
    let schema_bytes = serde_json::to_vec(&manifest.schema)
        .map_err(|err| TCError::bad_request(format!("invalid ir schema: {err}")))?;
    let schema = decode_schema_bytes(&schema_bytes).map_err(TCError::bad_request)?;

    #[derive(Clone)]
    enum RouteImpl {
        Value(Vec<u8>),
        Op(OpRef),
    }

    let mut route_entries = Vec::new();
    let mut routes = HashMap::<Vec<pathlink::PathSegment>, RouteImpl>::new();
    for route in manifest.routes {
        let segments = parse_route_path(&route.path)?;
        route_entries.push((segments.clone(), RouteMetadata { export: None }));

        if let Some(value) = route.value {
            let bytes = serde_json::to_vec(&value)
                .map_err(|err| TCError::bad_request(format!("invalid route value: {err}")))?;
            routes.insert(segments, RouteImpl::Value(bytes));
        } else if let Some(op) = route.op {
            if op.method.to_ascii_uppercase() != "GET" {
                return Err(TCError::bad_request(
                    "only GET ops are supported in ir manifests",
                ));
            }
            let link = Link::from_str(&op.path)
                .map_err(|err| TCError::bad_request(format!("invalid op link: {err}")))?;
            let op = OpRef::Get((Subject::Link(link), Scalar::default()));
            routes.insert(segments, RouteImpl::Op(op));
        } else {
            return Err(TCError::bad_request("route missing value or op"));
        }
    }

    let schema_routes = SchemaRoutes::from_entries(route_entries)?;
    let schema_id = schema.id().to_string();

    fn bearer_token(req: &Request<Body>) -> Option<String> {
        req.headers()
            .get(hyper::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|raw| raw.split_once(' '))
            .and_then(|(scheme, token)| {
                if scheme.eq_ignore_ascii_case("bearer") {
                    let token = token.trim();
                    if token.is_empty() {
                        None
                    } else {
                        Some(token.to_string())
                    }
                } else {
                    None
                }
            })
    }

    fn txn_header(req: &Request<Body>) -> TCResult<TxnHeader> {
        req.extensions()
            .get::<TxnHandle>()
            .cloned()
            .ok_or_else(|| TCError::internal("missing transaction handle"))
            .map(|handle| handle.header())
    }

    fn rpc_response(rpc: crate::gateway::RpcResponse) -> Response<Body> {
        let status = StatusCode::from_u16(rpc.status).unwrap_or(StatusCode::BAD_GATEWAY);
        let mut builder = Response::builder().status(status);
        for (name, value) in rpc.headers {
            if let Ok(name) = hyper::header::HeaderName::from_str(&name)
                && let Ok(value) = hyper::header::HeaderValue::from_str(&value)
            {
                if let Some(headers) = builder.headers_mut() {
                    headers.insert(name, value);
                }
            }
        }
        builder.body(Body::from(rpc.body)).unwrap()
    }

    let handler = move |req: Request<Body>| {
        let routes = routes.clone();
        let schema_id = schema_id.clone();
        async move {
            let path = req.uri().path().to_string();
            if !path.starts_with("/lib/") {
                return Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("not found");
            }

            if req.method() != hyper::Method::GET {
                return Response::builder()
                    .status(StatusCode::METHOD_NOT_ALLOWED)
                    .body(Body::empty())
                    .expect("method not allowed");
            }

            let header = match txn_header(&req) {
                Ok(header) => header,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(err.message().to_string()))
                        .expect("error response");
                }
            };

            let kernel = req
                .extensions()
                .get::<crate::Kernel<Request<Body>, Response<Body>>>()
                .cloned();

            let relative = &path["/lib".len()..];
            let normalized = if relative.starts_with('/') {
                relative
            } else {
                return Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("not found");
            };

            let schema_rel = schema_id.strip_prefix("/lib").unwrap_or(&schema_id);
            let normalized = if schema_rel.is_empty() {
                normalized
            } else if normalized.starts_with(schema_rel) {
                &normalized[schema_rel.len()..]
            } else {
                return Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("not found");
            };

            let segments = match parse_route_path(normalized) {
                Ok(segments) => segments,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(err.message().to_string()))
                        .expect("bad request");
                }
            };

            match routes.get(&segments).cloned() {
                Some(RouteImpl::Value(bytes)) => Response::builder()
                    .status(StatusCode::OK)
                    .header(hyper::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(bytes))
                    .expect("value response"),
                Some(RouteImpl::Op(op)) => {
                    let Some(kernel) = kernel else {
                        return Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(Body::from("missing kernel".to_string()))
                            .expect("missing kernel");
                    };

                    match kernel
                        .resolve_op(header.id(), bearer_token(&req), op)
                        .await
                    {
                        Ok(rpc) => rpc_response(rpc),
                        Err(err) => Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(err.message().to_string()))
                            .expect("error response"),
                    }
                }
                None => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("not found"),
            }
        }
        .boxed()
    };

    Ok((handler, schema, schema_routes))
}

#[cfg(feature = "pyo3")]
pub fn pyo3_ir_route_handler_from_bytes(
    bytes: Vec<u8>,
) -> TCResult<(
    impl KernelHandler<
        crate::pyo3_runtime::PyKernelRequest,
        pyo3::PyResult<crate::pyo3_runtime::PyKernelResponse>,
    >,
    LibrarySchema,
    SchemaRoutes,
)> {
    use futures::FutureExt;
    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;
    use std::collections::HashMap;
    use tc_ir::{OpRef, Scalar, Subject, parse_route_path};

    #[derive(Deserialize)]
    struct IrManifest {
        schema: serde_json::Value,
        routes: Vec<IrRoute>,
    }

    #[derive(Deserialize)]
    struct IrRoute {
        path: String,
        #[serde(default)]
        value: Option<serde_json::Value>,
        #[serde(default)]
        op: Option<IrOp>,
    }

    #[derive(Deserialize)]
    struct IrOp {
        method: String,
        path: String,
    }

    let manifest: IrManifest = serde_json::from_slice(&bytes)
        .map_err(|err| TCError::bad_request(format!("invalid ir manifest json: {err}")))?;
    let schema_bytes = serde_json::to_vec(&manifest.schema)
        .map_err(|err| TCError::bad_request(format!("invalid ir schema: {err}")))?;
    let schema = decode_schema_bytes(&schema_bytes).map_err(TCError::bad_request)?;

    #[derive(Clone)]
    enum RouteImpl {
        Value(Vec<u8>),
        Op(OpRef),
    }

    let mut route_entries = Vec::new();
    let mut routes = HashMap::<Vec<pathlink::PathSegment>, RouteImpl>::new();
    for route in manifest.routes {
        let segments = parse_route_path(&route.path)?;
        route_entries.push((segments.clone(), RouteMetadata { export: None }));

        if let Some(value) = route.value {
            let bytes = serde_json::to_vec(&value)
                .map_err(|err| TCError::bad_request(format!("invalid route value: {err}")))?;
            routes.insert(segments, RouteImpl::Value(bytes));
        } else if let Some(op) = route.op {
            if op.method.to_ascii_uppercase() != "GET" {
                return Err(TCError::bad_request(
                    "only GET ops are supported in ir manifests",
                ));
            }
            let link = Link::from_str(&op.path)
                .map_err(|err| TCError::bad_request(format!("invalid op link: {err}")))?;
            let op = OpRef::Get((Subject::Link(link), Scalar::default()));
            routes.insert(segments, RouteImpl::Op(op));
        } else {
            return Err(TCError::bad_request("route missing value or op"));
        }
    }

    let schema_routes = SchemaRoutes::from_entries(route_entries)?;
    let schema_id = schema.id().to_string();

    let handler = move |req: crate::pyo3_runtime::PyKernelRequest| {
        let routes = routes.clone();
        let schema_id = schema_id.clone();
        async move {
            let method = req.method_enum();
            if method != crate::Method::Get {
                return Err(PyValueError::new_err(format!(
                    "method {method} not supported for ir routes"
                )));
            }

            let txn = req
                .txn_handle()
                .ok_or_else(|| PyValueError::new_err("missing transaction handle"))?;

            let path = req.path_owned();
            if !path.starts_with("/lib/") {
                return Ok(crate::pyo3_runtime::PyKernelResponse::new(404, None, None));
            }

            let relative = &path["/lib".len()..];
            let normalized = if relative.starts_with('/') {
                relative
            } else {
                return Ok(crate::pyo3_runtime::PyKernelResponse::new(404, None, None));
            };

            let schema_rel = schema_id.strip_prefix("/lib").unwrap_or(&schema_id);
            let normalized = if schema_rel.is_empty() {
                normalized
            } else if normalized.starts_with(schema_rel) {
                &normalized[schema_rel.len()..]
            } else {
                return Ok(crate::pyo3_runtime::PyKernelResponse::new(404, None, None));
            };

            let segments = parse_route_path(normalized)
                .map_err(|err| PyValueError::new_err(err.message().to_string()))?;

            match routes.get(&segments).cloned() {
                Some(RouteImpl::Value(bytes)) => Python::with_gil(|py| {
                    let body = crate::pyo3_runtime::PyStateHandle::new(
                        pyo3::types::PyBytes::new_bound(py, &bytes).into_py(py),
                    );
                    Ok(crate::pyo3_runtime::PyKernelResponse::new(200, None, Some(body)))
                }),
                Some(RouteImpl::Op(op)) => {
                    let Some(kernel) = req.kernel() else {
                        return Err(PyValueError::new_err("missing kernel"));
                    };
                    let bearer = crate::pyo3_runtime::py_bearer_token(&req);
                    let rpc = kernel
                        .resolve_op(txn.id(), bearer, op)
                        .await
                        .map_err(|err| PyValueError::new_err(err.message().to_string()))?;

                    Python::with_gil(|py| {
                        let body = if rpc.body.is_empty() {
                            None
                        } else {
                            Some(crate::pyo3_runtime::PyStateHandle::new(
                                pyo3::types::PyBytes::new_bound(py, &rpc.body).into_py(py),
                            ))
                        };
                        Ok(crate::pyo3_runtime::PyKernelResponse::new(
                            rpc.status,
                            Some(rpc.headers),
                            body,
                        ))
                    })
                }
                None => Ok(crate::pyo3_runtime::PyKernelResponse::new(404, None, None)),
            }
        }
        .boxed()
    };

    Ok((handler, schema, schema_routes))
}

