use std::{collections::BTreeMap, io, path::PathBuf, sync::Arc};

use crate::http::{Body, Request, Response, StatusCode, header};
use futures::{FutureExt, TryStreamExt};
use hyper::body;
use number_general::Number;
use tc_error::TCResult;
use tc_ir::{LibrarySchema, Map};
use tc_state::State;
use tc_value::Value;

use crate::{
    KernelHandler,
    ir::{IR_ARTIFACT_CONTENT_TYPE, WASM_ARTIFACT_CONTENT_TYPE, http_ir_route_handler_from_bytes},
    storage::LibraryStore,
    wasm::http_wasm_route_handler_from_bytes,
};

use super::{
    InstallError, InstallRequest, LibraryFactory, LibraryHandlers, LibraryRegistry,
    decode_install_request_bytes,
};

pub async fn build_http_library_module(
    initial_schema: LibrarySchema,
    storage_root: Option<PathBuf>,
) -> TCResult<Arc<LibraryRegistry>> {
    let store = match storage_root {
        Some(root) => {
            let root_dir = crate::storage::load_library_root(root).await?;
            Some(LibraryStore::from_root(root_dir))
        }
        None => None,
    };
    build_http_library_module_with_store(initial_schema, store).await
}

pub async fn build_http_library_module_with_store(
    initial_schema: LibrarySchema,
    store: Option<LibraryStore>,
) -> TCResult<Arc<LibraryRegistry>> {
    let wasm_factory: LibraryFactory = Arc::new(|bytes: Vec<u8>| {
        let (handler, schema, schema_routes) = http_wasm_route_handler_from_bytes(bytes)?;
        let handler: Arc<dyn KernelHandler> = Arc::new(handler);
        Ok((schema, schema_routes, handler))
    });

    let ir_factory: LibraryFactory = Arc::new(|bytes: Vec<u8>| {
        let (handler, schema, schema_routes) = http_ir_route_handler_from_bytes(bytes)?;
        let handler: Arc<dyn KernelHandler> = Arc::new(handler);
        Ok((schema, schema_routes, handler))
    });

    let registry = LibraryRegistry::new(
        store,
        BTreeMap::from([
            (WASM_ARTIFACT_CONTENT_TYPE.to_string(), wasm_factory),
            (IR_ARTIFACT_CONTENT_TYPE.to_string(), ir_factory),
        ]),
    );
    registry.insert_schema(initial_schema).await?;
    Ok(Arc::new(registry))
}

pub fn http_library_handlers(module: &Arc<LibraryRegistry>) -> LibraryHandlers {
    let get = schema_get_handler(Arc::clone(module));
    let put = schema_put_handler(Arc::clone(module));
    let route = routes_handler(Arc::clone(module));
    LibraryHandlers::with_route(get, put, route)
}

pub fn schema_get_handler(registry: Arc<LibraryRegistry>) -> impl KernelHandler {
    move |_req: Request| {
        let registry = Arc::clone(&registry);
        async move { respond_with_listing(registry.list_dir(crate::uri::LIB_ROOT)) }.boxed()
    }
}

pub fn schema_put_handler(registry: Arc<LibraryRegistry>) -> impl KernelHandler {
    move |req: Request| {
        let registry = Arc::clone(&registry);
        let txn = req.extensions().get::<crate::txn::TxnHandle>().cloned();
        async move {
            let txn = match txn {
                Some(txn) => txn,
                None => return unauthorized_response("missing transaction context"),
            };

            match decode_body(req).await {
                Ok(InstallRequest::SchemaOnly(schema)) => {
                    if !txn.has_claim(schema.id(), umask::USER_WRITE) {
                        return unauthorized_response("unauthorized library install");
                    }
                    match registry.install_schema(schema).await {
                        Ok(()) => no_content_response(),
                        Err(err) => install_error_response(err),
                    }
                }
                Ok(InstallRequest::WithArtifacts(payload)) => {
                    if !txn.has_claim(payload.schema.id(), umask::USER_WRITE) {
                        return unauthorized_response("unauthorized library install");
                    }
                    match registry.install_payload(payload).await {
                        Ok(()) => no_content_response(),
                        Err(err) => install_error_response(err),
                    }
                }
                Err(err) => install_error_response(err),
            }
        }
        .boxed()
    }
}

fn respond_with_schema(schema: LibrarySchema) -> Response {
    let state = schema_to_state(&schema);
    match destream_json::encode(state) {
        Ok(stream) => {
            let body = Body::wrap_stream(stream.map_err(|err| io::Error::other(err.to_string())));
            http::Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(body)
                .expect("library schema response")
        }
        Err(err) => internal_error(err.to_string()),
    }
}

fn respond_with_listing(listing: Option<tc_ir::Map<bool>>) -> Response {
    let Some(listing) = listing else {
        return http::Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .expect("dir not found response");
    };

    let state = listing_to_state(&listing);
    match destream_json::encode(state) {
        Ok(stream) => {
            let body = Body::wrap_stream(stream.map_err(|err| io::Error::other(err.to_string())));
            http::Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json")
                .body(body)
                .expect("dir listing response")
        }
        Err(err) => internal_error(err.to_string()),
    }
}

fn schema_to_state(schema: &LibrarySchema) -> State {
    let dependencies = schema
        .dependencies()
        .iter()
        .enumerate()
        .map(|(idx, dep)| {
            (
                idx.to_string().parse().expect("Id"),
                State::from(Value::from(dep.to_string())),
            )
        })
        .collect::<Map<State>>();

    let mut map = Map::new();
    map.insert(
        "id".parse().expect("Id"),
        State::from(Value::from(schema.id().to_string())),
    );
    map.insert(
        "version".parse().expect("Id"),
        State::from(Value::from(schema.version().to_string())),
    );
    map.insert(
        "dependencies".parse().expect("Id"),
        State::Map(dependencies),
    );
    State::Map(map)
}

fn listing_to_state(listing: &tc_ir::Map<bool>) -> State {
    let map = listing
        .iter()
        .map(|(name, is_dir)| {
            (
                name.clone(),
                State::from(Value::from(Number::from(*is_dir))),
            )
        })
        .collect::<Map<State>>();
    State::Map(map)
}

async fn decode_body(req: Request) -> Result<InstallRequest, InstallError> {
    let body_bytes = body::to_bytes(req.into_body())
        .await
        .map_err(|err| InstallError::internal(err.to_string()))?;
    decode_install_request_bytes(&body_bytes)
}

fn bad_request(message: String) -> Response {
    http::Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(Body::from(message))
        .expect("bad request response")
}

fn internal_error(message: String) -> Response {
    http::Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from(message))
        .expect("internal error response")
}

fn unauthorized_response(message: impl Into<String>) -> Response {
    http::Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .body(Body::from(message.into()))
        .expect("unauthorized response")
}

fn no_content_response() -> Response {
    http::Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .expect("library install response")
}

fn install_error_response(error: InstallError) -> Response {
    match error {
        InstallError::BadRequest(message) => bad_request(message),
        InstallError::Internal(message) => internal_error(message),
    }
}

pub fn routes_handler(registry: Arc<LibraryRegistry>) -> impl KernelHandler {
    move |req: Request| {
        let path = req.uri().path().to_string();
        let registry = Arc::clone(&registry);
        async move {
            match registry.resolve_runtime_for_path(&path) {
                Some((runtime, true)) => respond_with_schema(runtime.state.schema()),
                Some((runtime, false)) => {
                    let handler = runtime.routes.current_handler();
                    if let Some(handler) = handler {
                        handler.call(req).await
                    } else {
                        http::Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(Body::empty())
                            .expect("missing route")
                    }
                }
                None => respond_with_listing(registry.list_dir(&path)),
            }
        }
        .boxed()
    }
}

#[cfg(all(test, feature = "http-server"))]
mod tests {
    use super::*;
    use crate::http::{Body, header};
    use crate::library::{InstallArtifacts, encode_install_payload_bytes};
    use crate::storage::Artifact;
    use crate::{Method, kernel::Kernel};
    use futures::stream;
    use hyper::body::to_bytes;
    use pathlink::Link;
    use std::str::FromStr;
    use tc_ir::LibrarySchema;
    use tc_ir::Scalar;
    use tc_state::{State, null_transaction};

    async fn decode_state(bytes: Vec<u8>) -> State {
        let stream = stream::iter(vec![Ok::<hyper::body::Bytes, std::io::Error>(
            bytes.clone().into(),
        )]);
        match destream_json::try_decode(null_transaction(), stream).await {
            Ok(state) => state,
            Err(_) => {
                let text = String::from_utf8(bytes).expect("utf8");
                let wrapped = format!(r#"{{"/state/scalar/map":{text}}}"#);
                let stream = stream::iter(vec![Ok::<hyper::body::Bytes, std::io::Error>(
                    wrapped.into_bytes().into(),
                )]);
                destream_json::try_decode(null_transaction(), stream)
                    .await
                    .expect("state decode")
            }
        }
    }

    #[test]
    fn round_trips_install_payload_bytes() {
        let schema = LibrarySchema::new(
            Link::from_str("/lib/example-devco/hello/0.1.0").expect("link"),
            "0.1.0",
            vec![],
        );

        let payload = InstallArtifacts {
            schema: schema.clone(),
            artifacts: vec![Artifact {
                path: schema.id().to_string(),
                content_type: WASM_ARTIFACT_CONTENT_TYPE.to_string(),
                bytes: b"wasm-bytes".to_vec(),
            }],
        };

        let bytes = encode_install_payload_bytes(&payload).expect("encode payload");
        let request = decode_install_request_bytes(&bytes).expect("decode payload");

        match request {
            InstallRequest::WithArtifacts(decoded) => {
                assert_eq!(decoded.schema.id(), schema.id());
                assert_eq!(decoded.schema.version(), schema.version());
                assert_eq!(decoded.artifacts.len(), 1);
                assert_eq!(
                    decoded.artifacts[0].content_type,
                    WASM_ARTIFACT_CONTENT_TYPE
                );
            }
            _ => panic!("expected artifacts payload"),
        }
    }

    #[tokio::test]
    async fn serves_schema_over_http() {
        let initial = LibrarySchema::new(
            Link::from_str("/lib/example-devco/service/0.1.0").expect("link"),
            "0.1.0",
            vec![],
        );
        let module = build_http_library_module(initial.clone(), None)
            .await
            .expect("module");
        let handlers = http_library_handlers(&module);

        let kernel = Kernel::builder()
            .with_host_id("tc-library-test")
            .with_library_module(module, handlers)
            .finish();

        let request = http::Request::builder()
            .method("GET")
            .uri("/lib")
            .body(Body::empty())
            .expect("request");

        let response = kernel
            .dispatch(Method::Get, "/lib", request)
            .expect("handler")
            .await;

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body()).await.expect("body");
        let state = decode_state(body.to_vec()).await;
        let State::Map(listing) = state else {
            panic!("expected State::Map listing");
        };
        assert!(listing.contains_key("example-devco"));

        let schema_request = http::Request::builder()
            .method("GET")
            .uri("/lib/example-devco/service/0.1.0")
            .body(Body::empty())
            .expect("schema request");

        let schema_response = kernel
            .dispatch(
                Method::Get,
                "/lib/example-devco/service/0.1.0",
                schema_request,
            )
            .expect("schema handler")
            .await;

        assert_eq!(schema_response.status(), StatusCode::OK);

        let body = to_bytes(schema_response.into_body()).await.expect("body");
        let state = decode_state(body.to_vec()).await;
        let State::Map(schema_map) = state else {
            panic!("expected State::Map schema");
        };
        let id = match schema_map.get("id") {
            Some(State::Scalar(Scalar::Value(Value::String(value)))) => value.as_str(),
            _ => panic!("missing schema id"),
        };
        let version = match schema_map.get("version") {
            Some(State::Scalar(Scalar::Value(Value::String(value)))) => value.as_str(),
            _ => panic!("missing schema version"),
        };
        assert_eq!(id, "/lib/example-devco/service/0.1.0");
        assert_eq!(version, "0.1.0");
        let deps = match schema_map.get("dependencies") {
            Some(State::Map(map)) => map,
            _ => panic!("missing dependencies"),
        };
        assert!(deps.is_empty());
    }

    #[tokio::test]
    async fn installs_schema_via_put() {
        use tc_ir::Claim;
        let initial = LibrarySchema::new(
            Link::from_str("/lib/example-devco/service/0.1.0").expect("link"),
            "0.1.0",
            vec![],
        );
        let module = build_http_library_module(initial, None)
            .await
            .expect("module");
        let handlers = http_library_handlers(&module);

        let kernel = Kernel::builder()
            .with_host_id("tc-library-test")
            .with_library_module(module, handlers)
            .finish();

        let new_schema = serde_json::json!({
            "id": "/lib/example-devco/updated/0.2.0",
            "version": "0.2.0",
            "dependencies": [],
        });

        let mut put_request = http::Request::builder()
            .method("PUT")
            .uri("/lib")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(new_schema.to_string()))
            .expect("install request");
        let install_claim = Claim::new(
            Link::from_str("/lib/example-devco/updated/0.2.0").expect("install link"),
            umask::USER_WRITE,
        );
        let install_txn = kernel
            .txn_manager()
            .begin()
            .with_claims(vec![install_claim]);
        put_request.extensions_mut().insert(install_txn);

        let put_response = kernel
            .dispatch(Method::Put, "/lib", put_request)
            .expect("put handler")
            .await;

        assert_eq!(put_response.status(), StatusCode::NO_CONTENT);

        let get_request = http::Request::builder()
            .method("GET")
            .uri("/lib/example-devco/updated/0.2.0")
            .body(Body::empty())
            .expect("get request");

        let get_response = kernel
            .dispatch(Method::Get, "/lib/example-devco/updated/0.2.0", get_request)
            .expect("get handler")
            .await;

        let body = to_bytes(get_response.into_body()).await.expect("body");
        let state = decode_state(body.to_vec()).await;
        let State::Map(schema_map) = state else {
            panic!("expected State::Map schema");
        };
        let id = match schema_map.get("id") {
            Some(State::Scalar(Scalar::Value(Value::String(value)))) => value.as_str(),
            _ => panic!("missing schema id"),
        };
        let version = match schema_map.get("version") {
            Some(State::Scalar(Scalar::Value(Value::String(value)))) => value.as_str(),
            _ => panic!("missing schema version"),
        };
        assert_eq!(id, "/lib/example-devco/updated/0.2.0");
        assert_eq!(version, "0.2.0");
    }
}
