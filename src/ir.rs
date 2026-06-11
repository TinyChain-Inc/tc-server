use std::str::FromStr;

use pathlink::Link;
use serde::Deserialize;
use tc_error::{TCError, TCResult};
use tc_ir::{Id, LibrarySchema};

use crate::{
    KernelHandler,
    library::{RouteMetadata, SchemaRoutes},
    storage::decode_schema_bytes,
};

#[cfg(feature = "http-server")]
use crate::txn::TxnHandle;

pub const IR_ARTIFACT_CONTENT_TYPE: &str = "application/tinychain+json";
pub const WASM_ARTIFACT_CONTENT_TYPE: &str = "application/wasm";

#[cfg(feature = "http-server")]
pub fn http_ir_route_handler_from_bytes(
    bytes: Vec<u8>,
) -> TCResult<(impl KernelHandler, LibrarySchema, SchemaRoutes)> {
    use crate::http::{
        Body, HttpMethod, NativeStateBody, Request, RequestBody, Response, StatusCode,
        decode_request_body_with_txn, header,
    };
    use crate::resolve::Resolve;
    use bytes::Bytes;
    use futures::FutureExt;
    use std::collections::HashMap;
    use tc_ir::{Map, OpDef, OpRef, Scalar, Subject, parse_route_path};
    use tc_state::State;
    use tc_value::Value;
    use url::form_urlencoded;

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
        #[serde(default)]
        opdef: Option<serde_json::Value>,
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
        OpDef(OpDef),
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
        } else if let Some(opdef) = route.opdef {
            let bytes = serde_json::to_vec(&opdef)
                .map_err(|err| TCError::bad_request(format!("invalid opdef route: {err}")))?;
            let stream =
                futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from(bytes))]);
            let opdef: OpDef = futures::executor::block_on(destream_json::try_decode((), stream))
                .map_err(|err| {
                TCError::bad_request(format!("invalid opdef encoding: {err}"))
            })?;
            routes.insert(segments, RouteImpl::OpDef(opdef));
        } else {
            return Err(TCError::bad_request("route missing value or op"));
        }
    }

    let schema_routes = SchemaRoutes::from_entries(route_entries)?;
    let schema_id = schema.id().to_string();
    let schema_link = schema.id().clone();

    fn state_response(state: tc_state::State) -> Response {
        crate::http::state_response(state)
    }

    let handler = move |req: Request| {
        let routes = routes.clone();
        let schema_id = schema_id.clone();
        let schema_link = schema_link.clone();
        async move {
            let path = req.uri().path().to_string();
            if !path.starts_with(crate::uri::LIB_ROOT_PREFIX) {
                return http::Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("not found");
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

            let relative = &path[crate::uri::LIB_ROOT.len()..];
            let normalized = if relative.starts_with('/') {
                relative
            } else {
                return http::Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("not found");
            };

            let schema_rel = schema_id
                .strip_prefix(crate::uri::LIB_ROOT)
                .unwrap_or(&schema_id);
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
                Some(RouteImpl::Value(bytes)) => {
                    if req.method() != HttpMethod::GET {
                        return http::Response::builder()
                            .status(StatusCode::METHOD_NOT_ALLOWED)
                            .body(Body::empty())
                            .expect("method not allowed");
                    }
                    http::Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(bytes))
                        .expect("value response")
                }
                Some(RouteImpl::Op(op)) => {
                    if req.method() != HttpMethod::GET {
                        return http::Response::builder()
                            .status(StatusCode::METHOD_NOT_ALLOWED)
                            .body(Body::empty())
                            .expect("method not allowed");
                    }
                    match op.resolve(&txn).await {
                        Ok(state) => state_response(state),
                        Err(err) => http::Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(err.message().to_string()))
                            .expect("error response"),
                    }
                }
                Some(RouteImpl::OpDef(opdef)) => {
                    let method = req.method().clone();
                    let result = match opdef {
                        OpDef::Get(_) if method == HttpMethod::GET => {
                            let key_name = match &opdef {
                                OpDef::Get((key_name, _)) => Some(key_name.as_str()),
                                _ => None,
                            };
                            let key = match decode_value_body_for_key(&req, key_name).await {
                                Ok(Some(value)) => value,
                                Ok(None) => Value::None,
                                Err(err) => return tc_error_response(err),
                            };
                            crate::op_executor::execute_get_with_self(
                                &txn,
                                opdef,
                                key,
                                Some(schema_link.clone()),
                            )
                            .await
                        }
                        OpDef::Put(_) if method == HttpMethod::PUT => {
                            let key = match decode_value_body(&req).await {
                                Ok(Some(value)) => value,
                                Ok(None) => Value::None,
                                Err(err) => return tc_error_response(err),
                            };
                            let value = match decode_request_body_with_txn::<State>(&req).await {
                                Ok(Some(state)) => state,
                                Ok(None) => State::None,
                                Err(err) => return tc_error_response(err),
                            };
                            crate::op_executor::execute_put_with_self(
                                &txn,
                                opdef,
                                key,
                                value,
                                Some(schema_link.clone()),
                            )
                            .await
                            .map(|()| State::None)
                        }
                        OpDef::Post(_) if method == HttpMethod::POST => {
                            let params = match decode_scalar_map_body(&req).await {
                                Ok(Some(params)) => params,
                                Ok(None) => Map::new(),
                                Err(err) => return tc_error_response(err),
                            };
                            crate::op_executor::execute_post_with_self(
                                &txn,
                                opdef,
                                params,
                                Some(schema_link.clone()),
                            )
                            .await
                        }
                        OpDef::Delete(_) if method == HttpMethod::DELETE => {
                            let key = match decode_value_body(&req).await {
                                Ok(Some(value)) => value,
                                Ok(None) => Value::None,
                                Err(err) => return tc_error_response(err),
                            };
                            crate::op_executor::execute_delete_with_self(
                                &txn,
                                opdef,
                                key,
                                Some(schema_link.clone()),
                            )
                            .await
                            .map(|()| State::None)
                        }
                        _ => {
                            return http::Response::builder()
                                .status(StatusCode::METHOD_NOT_ALLOWED)
                                .body(Body::empty())
                                .expect("method not allowed");
                        }
                    };

                    match result {
                        Ok(state) => state_response(state),
                        Err(err) => tc_error_response(err),
                    }
                }
                None => http::Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("not found"),
            }
        }
        .boxed()
    };

    fn tc_error_response(err: TCError) -> Response {
        http::Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from(err.message().to_string()))
            .expect("error response")
    }

    #[allow(clippy::collapsible_if)]
    async fn decode_value_body_for_key(
        req: &Request,
        key_name: Option<&str>,
    ) -> TCResult<Option<Value>> {
        if let Some(body) = req.extensions().get::<NativeStateBody>() {
            return decode_value_state_for_key(body.clone_state(), key_name);
        }

        if let Some(body) = req.extensions().get::<RequestBody>() {
            if !body.is_empty() {
                return decode_value_bytes(body.clone_bytes(), key_name).await;
            }
        }

        let query = req.uri().query().unwrap_or("");
        let key = form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .find(|(k, _)| k.eq_ignore_ascii_case("key"))
            .map(|(_, v)| v);

        let Some(raw) = key else {
            return Ok(None);
        };

        if raw.trim().is_empty() {
            return Ok(Some(Value::None));
        }

        decode_value_bytes(Bytes::from(raw.into_bytes()), key_name).await
    }

    async fn decode_value_body(req: &Request) -> TCResult<Option<Value>> {
        decode_value_body_for_key(req, None).await
    }

    fn decode_value_state_for_key(state: State, key_name: Option<&str>) -> TCResult<Option<Value>> {
        if state.is_none() {
            return Ok(None);
        }

        if let (Some(key_name), State::Map(map)) = (key_name, &state) {
            let id = key_name
                .parse::<Id>()
                .map_err(|err| TCError::bad_request(format!("invalid key name: {err}")))?;

            if let Some(item) = map.get(&id) {
                return state_to_value(item.clone()).map(Some);
            }
        }

        state_to_value(state).map(Some)
    }

    fn state_to_value(state: State) -> TCResult<Value> {
        match state {
            State::None => Ok(Value::None),
            State::Scalar(Scalar::Value(value)) => Ok(value),
            _ => Err(TCError::bad_request("expected scalar value request body")),
        }
    }

    async fn decode_value_bytes(bytes: Bytes, key_name: Option<&str>) -> TCResult<Option<Value>> {
        let bytes = if let Some(key_name) = key_name {
            match serde_json::from_slice::<serde_json::Value>(&bytes) {
                Ok(serde_json::Value::Object(mut object)) => {
                    if let Some(value) = object.remove(key_name) {
                        Bytes::from(serde_json::to_vec(&value).map_err(|err| {
                            TCError::bad_request(format!("invalid key value: {err}"))
                        })?)
                    } else {
                        bytes
                    }
                }
                _ => bytes,
            }
        } else {
            bytes
        };

        let stream = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(bytes)]);
        destream_json::try_decode((), stream)
            .await
            .map(Some)
            .map_err(|err| TCError::bad_request(err.to_string()))
    }

    async fn decode_scalar_map_body(req: &Request) -> TCResult<Option<Map<State>>> {
        if let Some(body) = req.extensions().get::<NativeStateBody>() {
            let state = body.clone_state();
            if state.is_none() {
                return Ok(None);
            }

            return match state {
                State::Map(map) => Ok(Some(map)),
                State::Scalar(Scalar::Map(map)) => {
                    let mut out = Map::new();
                    for (id, scalar) in map {
                        let state = match scalar {
                            Scalar::Value(value) => State::from(value),
                            other => State::Scalar(other),
                        };
                        out.insert(id, state);
                    }
                    Ok(Some(out))
                }
                _ => Err(TCError::bad_request(
                    "expected map request body".to_string(),
                )),
            };
        }

        let Some(body) = req.extensions().get::<RequestBody>() else {
            return Ok(None);
        };
        if body.is_empty() {
            return Ok(None);
        }

        let json_value: serde_json::Value = serde_json::from_slice(&body.clone_bytes())
            .map_err(|err| TCError::bad_request(err.to_string()))?;

        let serde_json::Value::Object(map) = json_value else {
            return Err(TCError::bad_request(
                "expected map request body".to_string(),
            ));
        };

        let mut out = Map::new();
        for (key, value) in map {
            let id: Id = key
                .parse::<Id>()
                .map_err(|err| TCError::bad_request(err.to_string()))?;
            let bytes =
                serde_json::to_vec(&value).map_err(|err| TCError::bad_request(err.to_string()))?;
            let stream =
                futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from(bytes))]);
            let scalar: Scalar = destream_json::try_decode((), stream)
                .await
                .map_err(|err| TCError::bad_request(err.to_string()))?;
            let state = match scalar {
                Scalar::Value(value) => State::from(value),
                other => State::Scalar(other),
            };
            out.insert(id, state);
        }

        Ok(Some(out))
    }

    Ok((handler, schema, schema_routes))
}
