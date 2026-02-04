use std::str::FromStr;

use pathlink::Link;
use serde::Deserialize;
use tc_error::{TCError, TCResult};
use tc_ir::LibrarySchema;

use crate::{
    KernelHandler,
    library::{RouteMetadata, SchemaRoutes},
    storage::decode_schema_bytes,
};

#[cfg(feature = "http-server")]
use crate::txn::TxnHandle;

pub const IR_ARTIFACT_CONTENT_TYPE: &str = "application/tinychain+json";

#[cfg(feature = "http-server")]
pub fn http_ir_route_handler_from_bytes(
    bytes: Vec<u8>,
) -> TCResult<(
    impl KernelHandler,
    LibrarySchema,
    SchemaRoutes,
)> {
    use futures::{FutureExt, TryStreamExt};
    use crate::http::{Body, HttpMethod, Request, Response, StatusCode, header};
    use std::{collections::HashMap, io};
    use crate::resolve::Resolve;
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
        Op(Box<OpRef>),
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
            if !op.method.eq_ignore_ascii_case("GET") {
                return Err(TCError::bad_request(
                    "only GET ops are supported in ir manifests",
                ));
            }
            let link = Link::from_str(&op.path)
                .map_err(|err| TCError::bad_request(format!("invalid op link: {err}")))?;
            let op = OpRef::Get((Subject::Link(link), Scalar::default()));
            routes.insert(segments, RouteImpl::Op(Box::new(op)));
        } else {
            return Err(TCError::bad_request("route missing value or op"));
        }
    }

    let schema_routes = SchemaRoutes::from_entries(route_entries)?;
    let schema_id = schema.id().to_string();

    fn state_response(state: tc_state::State) -> Response {
        match destream_json::encode(state) {
            Ok(stream) => http::Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::wrap_stream(stream.map_err(|err| io::Error::other(err.to_string()))))
                .unwrap(),
            Err(err) => http::Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(err.to_string()))
                .expect("error response"),
        }
    }

    let handler = move |req: Request| {
        let routes = routes.clone();
        let schema_id = schema_id.clone();
        async move {
            let path = req.uri().path().to_string();
            if !path.starts_with("/lib/") {
                return http::Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("not found");
            }

            if req.method() != HttpMethod::GET {
                return http::Response::builder()
                    .status(StatusCode::METHOD_NOT_ALLOWED)
                    .body(Body::empty())
                    .expect("method not allowed");
            }

            let txn = match req.extensions().get::<TxnHandle>().cloned() {
                Some(txn) => txn,
                None => {
                    return http::Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from("missing transaction handle".to_string()))
                        .expect("error response");
                }
            };

            let relative = &path["/lib".len()..];
            let normalized = if relative.starts_with('/') {
                relative
            } else {
                return http::Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("not found");
            };

            let schema_rel = schema_id.strip_prefix("/lib").unwrap_or(&schema_id);
            let normalized = if schema_rel.is_empty() {
                normalized
            } else if let Some(normalized) = normalized.strip_prefix(schema_rel) {
                normalized
            } else {
                return http::Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("not found");
            };

            let segments = match parse_route_path(normalized) {
                Ok(segments) => segments,
                Err(err) => {
                    return http::Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(err.message().to_string()))
                        .expect("bad request");
                }
            };

            match routes.get(&segments).cloned() {
                Some(RouteImpl::Value(bytes)) => http::Response::builder()
                    .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(bytes))
                    .expect("value response"),
                Some(RouteImpl::Op(op)) => match op.resolve(&txn).await {
                    Ok(state) => state_response(state),
                    Err(err) => http::Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(err.message().to_string()))
                        .expect("error response"),
                },
                None => http::Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("not found"),
            }
        }
        .boxed()
    };

    Ok((handler, schema, schema_routes))
}
