use std::{
    convert::Infallible,
    io,
    net::SocketAddr,
    net::TcpListener,
    path::PathBuf,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{FutureExt, TryStreamExt, future::BoxFuture, stream};
use hyper::{Body, Request, Response, StatusCode, body::to_bytes, header::HeaderValue};
use tc_ir::{LibrarySchema, Route, TxnId, parse_route_path};
use tower::Service;
use url::form_urlencoded;

use crate::{
    Kernel, KernelDispatch, KernelHandler, Method,
    library::{
        NativeLibrary, NativeLibraryHandler, default_library_schema,
        http::{build_http_library_module, http_library_handlers},
    },
    storage::LibraryDir,
    txn::TxnHandle,
};
use tc_error::{ErrorKind, TCError, TCResult};
use tc_value::Value;

pub type HttpKernel = Kernel<Request<Body>, Response<Body>>;

/// Configuration options for building an HTTP kernel instance.
#[derive(Clone, Debug)]
pub struct HttpKernelConfig {
    pub data_dir: Option<PathBuf>,
    pub initial_schema: LibrarySchema,
    pub host_id: String,
}

impl Default for HttpKernelConfig {
    fn default() -> Self {
        Self {
            data_dir: None,
            initial_schema: default_library_schema(),
            host_id: "tc-http-host".to_string(),
        }
    }
}

impl HttpKernelConfig {
    pub fn with_data_dir<P: Into<PathBuf>>(mut self, data_dir: P) -> Self {
        self.data_dir = Some(data_dir.into());
        self
    }

    pub fn with_initial_schema(mut self, schema: LibrarySchema) -> Self {
        self.initial_schema = schema;
        self
    }

    pub fn with_host_id(mut self, host_id: impl Into<String>) -> Self {
        self.host_id = host_id.into();
        self
    }
}

/// Helper that wires the shared library module plus storage into a kernel configured for HTTP.
pub fn build_http_kernel_with_config<S, K, H>(
    config: HttpKernelConfig,
    service_handler: S,
    kernel_handler: K,
    health_handler: H,
) -> HttpKernel
where
    S: KernelHandler<Request<Body>, Response<Body>>,
    K: KernelHandler<Request<Body>, Response<Body>>,
    H: KernelHandler<Request<Body>, Response<Body>>,
{
    let storage = config
        .data_dir
        .as_ref()
        .map(|path| LibraryDir::new(path.clone()));
    let module = build_http_library_module(config.initial_schema.clone(), storage);
    let handlers = http_library_handlers(&module);

    Kernel::builder()
        .with_host_id(config.host_id.clone())
        .with_http_rpc_gateway()
        .with_library_module(module, handlers)
        .with_service_handler(service_handler)
        .with_kernel_handler(kernel_handler)
        .with_health_handler(health_handler)
        .finish()
}

pub fn build_http_kernel<S, K, H>(
    service_handler: S,
    kernel_handler: K,
    health_handler: H,
) -> HttpKernel
where
    S: KernelHandler<Request<Body>, Response<Body>>,
    K: KernelHandler<Request<Body>, Response<Body>>,
    H: KernelHandler<Request<Body>, Response<Body>>,
{
    build_http_kernel_with_config(
        HttpKernelConfig::default(),
        service_handler,
        kernel_handler,
        health_handler,
    )
}

pub fn build_http_kernel_with_native_library<H, S, K, He>(
    library: NativeLibrary<H>,
    service_handler: S,
    kernel_handler: K,
    health_handler: He,
) -> HttpKernel
where
    H: NativeLibraryHandler,
    S: KernelHandler<Request<Body>, Response<Body>>,
    K: KernelHandler<Request<Body>, Response<Body>>,
    He: KernelHandler<Request<Body>, Response<Body>>,
{
    build_http_kernel_with_native_library_and_config(
        library,
        HttpKernelConfig::default(),
        service_handler,
        kernel_handler,
        health_handler,
    )
}

pub fn build_http_kernel_with_native_library_and_config<H, S, K, He>(
    library: NativeLibrary<H>,
    config: HttpKernelConfig,
    service_handler: S,
    kernel_handler: K,
    health_handler: He,
) -> HttpKernel
where
    H: NativeLibraryHandler,
    S: KernelHandler<Request<Body>, Response<Body>>,
    K: KernelHandler<Request<Body>, Response<Body>>,
    He: KernelHandler<Request<Body>, Response<Body>>,
{
    let native = Arc::new(library);
    let schema_handler = native_schema_get_handler(native.schema().clone());
    let install_handler = native_install_not_supported_handler();
    let routes_handler = http_native_routes_handler(native);

    Kernel::builder()
        .with_host_id(config.host_id.clone())
        .with_lib_handler(schema_handler)
        .with_lib_put_handler(install_handler)
        .with_lib_route_handler(routes_handler)
        .with_service_handler(service_handler)
        .with_kernel_handler(kernel_handler)
        .with_health_handler(health_handler)
        .finish()
}

pub struct HttpServer {
    kernel: HttpKernel,
}

impl HttpServer {
    pub fn new(kernel: HttpKernel) -> Self {
        Self { kernel }
    }

    pub async fn serve(self, addr: SocketAddr) -> hyper::Result<()> {
        let service = KernelService::new(self.kernel);
        let make_service = tower::make::Shared::new(service);
        hyper::Server::bind(&addr).serve(make_service).await
    }

    pub async fn serve_listener(self, listener: TcpListener) -> hyper::Result<()> {
        let service = KernelService::new(self.kernel);
        let make_service = tower::make::Shared::new(service);
        hyper::Server::from_tcp(listener)?.serve(make_service).await
    }
}

#[derive(Clone)]
pub(crate) struct KernelService {
    kernel: HttpKernel,
}

impl KernelService {
    fn new(kernel: HttpKernel) -> Self {
        Self { kernel }
    }
}

impl Service<Request<Body>> for KernelService {
    type Response = Response<Body>;
    type Error = hyper::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let uri = req.uri().clone();
        let method = req.method().clone();
        let path = uri.path().to_owned();
        let kernel = self.kernel.clone();

        Box::pin(async move {
            let method = match to_kernel_method(&method) {
                Some(method) => method,
                None => return Ok(method_not_allowed()),
            };

            let (req, body_is_none) = match parse_body(req).await {
                Ok(pair) => pair,
                Err(resp) => return Ok(resp),
            };

            let txn_id = match parse_txn_id(&req) {
                Ok(ctx) => ctx,
                Err(TxnParseError::Invalid) => {
                    return Ok(bad_request_response("invalid transaction id"));
                }
            };

            let bearer = parse_bearer_token(&req);
            let inbound_txn_id = txn_id;
            let mut minted_txn_id: Option<TxnId> = None;
            let token = match bearer {
                Some(token) => match kernel.token_verifier().verify(token).await {
                    Ok(token) => Some(token),
                    Err(crate::txn::TxnError::Unauthorized) => {
                        return Ok(Response::builder()
                            .status(StatusCode::UNAUTHORIZED)
                            .body(Body::empty())
                            .expect("unauthorized response"));
                    }
                    Err(crate::txn::TxnError::NotFound) => {
                        unreachable!("verifier does not use NotFound")
                    }
                },
                None => None,
            };

            match kernel.route_request(
                method,
                &path,
                req,
                inbound_txn_id,
                body_is_none,
                token.as_ref(),
                |handle, req| {
                    minted_txn_id = Some(handle.id());
                    req.extensions_mut().insert(handle.clone());
                },
            ) {
                Ok(KernelDispatch::Response(resp)) => {
                    let mut response = resp.await;
                    if inbound_txn_id.is_none()
                        && let Some(txn_id) = minted_txn_id
                        && let Ok(value) = HeaderValue::from_str(&txn_id.to_string())
                    {
                        response.headers_mut().insert("x-tc-txn-id", value);
                    }

                    Ok(response)
                }
                Ok(KernelDispatch::Finalize { commit: _, result }) => {
                    Ok(handle_finalize_result(result))
                }
                Ok(KernelDispatch::NotFound) => Ok(not_found()),
                Err(crate::txn::TxnError::NotFound) => {
                    Ok(bad_request_response("unknown transaction id"))
                }
                Err(crate::txn::TxnError::Unauthorized) => Ok(Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body(Body::empty())
                    .expect("unauthorized response")),
            }
        })
    }
}

fn to_kernel_method(method: &hyper::Method) -> Option<Method> {
    match *method {
        hyper::Method::GET => Some(Method::Get),
        hyper::Method::PUT => Some(Method::Put),
        hyper::Method::POST => Some(Method::Post),
        hyper::Method::DELETE => Some(Method::Delete),
        _ => None,
    }
}

fn method_not_allowed() -> Response<Body> {
    Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .body(Body::empty())
        .expect("method not allowed response")
}

fn not_found() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::empty())
        .expect("not found response")
}

impl tower::Service<()> for KernelService {
    type Response = Self;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: ()) -> Self::Future {
        let svc = self.clone();
        Box::pin(async move { Ok(svc) })
    }
}

fn parse_txn_id(req: &Request<Body>) -> Result<Option<TxnId>, TxnParseError> {
    use std::str::FromStr;

    let query = req.uri().query().unwrap_or("");
    let txn_id_param = form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .find(|(k, _)| k.eq_ignore_ascii_case("txn_id"))
        .map(|(_, v)| v);

    match txn_id_param {
        Some(raw) => TxnId::from_str(&raw)
            .map(Some)
            .map_err(|_| TxnParseError::Invalid),
        None => Ok(None),
    }
}

enum TxnParseError {
    Invalid,
}

fn parse_bearer_token(req: &Request<Body>) -> Option<String> {
    use hyper::header::AUTHORIZATION;

    let header = req.headers().get(AUTHORIZATION)?;
    let value = header.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }

    let token = token.trim();
    if token.is_empty() {
        return None;
    }

    Some(token.to_string())
}

#[cfg(feature = "rjwt-token")]
pub fn host_handler_with_public_keys(
    keys: crate::auth::PublicKeyStore,
) -> impl KernelHandler<Request<Body>, Response<Body>> {
    move |req: Request<Body>| {
        let keys = keys.clone();
        async move {
            match req.uri().path() {
                "/host/metrics" => Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::empty())
                    .expect("metrics response"),
                "/host/public_key" => {
                    use base64::Engine as _;

                    let query = req.uri().query().unwrap_or("");
                    let key = form_urlencoded::parse(query.as_bytes())
                        .into_owned()
                        .find(|(k, _)| k.eq_ignore_ascii_case("key"))
                        .map(|(_, v)| v);

                    let Some(actor_id) = key else {
                        return bad_request_response("missing key query parameter");
                    };

                    let Some(public_key) = keys.public_key(&actor_id) else {
                        return not_found();
                    };

                    let encoded =
                        base64::engine::general_purpose::STANDARD.encode(public_key.to_bytes());
                    let body = match serde_json::to_vec(&encoded) {
                        Ok(body) => body,
                        Err(_) => return internal_error_response("failed to encode public key"),
                    };

                    Response::builder()
                        .status(StatusCode::OK)
                        .header(hyper::header::CONTENT_TYPE, "application/json")
                        .body(Body::from(body))
                        .expect("public key response")
                }
                _ => not_found(),
            }
        }
        .boxed()
    }
}

fn handle_finalize_result(result: Result<(), crate::txn::TxnError>) -> Response<Body> {
    match result {
        Ok(()) => no_content(),
        Err(crate::txn::TxnError::NotFound) => bad_request_response("unknown transaction id"),
        Err(crate::txn::TxnError::Unauthorized) => Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body(Body::empty())
            .expect("unauthorized response"),
    }
}

fn bad_request_response(msg: &str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(Body::from(msg.to_string()))
        .expect("bad request response")
}

fn internal_error_response(msg: &str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from(msg.to_string()))
        .expect("internal error response")
}

fn no_content() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .expect("no content response")
}

async fn parse_body(req: Request<Body>) -> Result<(Request<Body>, bool), Response<Body>> {
    let (parts, body) = req.into_parts();
    let body_bytes = to_bytes(body)
        .await
        .map_err(|_| internal_error_response("failed to read request body"))?;
    let body_is_none = body_bytes.iter().all(|b| b.is_ascii_whitespace());
    let mut req = Request::from_parts(parts, Body::from(body_bytes.clone()));
    if !body_is_none {
        req.extensions_mut().insert(RequestBody::new(body_bytes));
    }
    Ok((req, body_is_none))
}

fn native_schema_get_handler(
    schema: LibrarySchema,
) -> impl KernelHandler<Request<Body>, Response<Body>> {
    move |_req: Request<Body>| {
        let schema = schema.clone();
        async move { schema_response(schema) }.boxed()
    }
}

fn native_install_not_supported_handler() -> impl KernelHandler<Request<Body>, Response<Body>> {
    |_req: Request<Body>| {
        async move {
            Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Body::from(
                    "native libraries must be installed at compile time",
                ))
                .expect("native install response")
        }
        .boxed()
    }
}

fn http_native_routes_handler<H>(
    library: Arc<NativeLibrary<H>>,
) -> impl KernelHandler<Request<Body>, Response<Body>> + 'static
where
    H: NativeLibraryHandler,
{
    move |req: Request<Body>| {
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
                    return internal_error_response(
                        "missing transaction handle for native library route",
                    );
                }
            };

            let request_value = match decode_value_body(&req).await {
                Ok(Some(value)) => value,
                Ok(None) => Value::None,
                Err(err) => return tc_error_response(err),
            };

            let response_value = match method {
                Method::Get => {
                    let fut = match handler.get(&txn, request_value.clone()) {
                        Ok(fut) => fut,
                        Err(err) => return tc_error_response(err),
                    };
                    match fut.await {
                        Ok(value) => value,
                        Err(err) => return tc_error_response(err),
                    }
                }
                Method::Put => {
                    let fut = match handler.put(&txn, request_value.clone()) {
                        Ok(fut) => fut,
                        Err(err) => return tc_error_response(err),
                    };
                    match fut.await {
                        Ok(value) => value,
                        Err(err) => return tc_error_response(err),
                    }
                }
                Method::Post => {
                    let fut = match handler.post(&txn, request_value.clone()) {
                        Ok(fut) => fut,
                        Err(err) => return tc_error_response(err),
                    };
                    match fut.await {
                        Ok(value) => value,
                        Err(err) => return tc_error_response(err),
                    }
                }
                Method::Delete => {
                    let fut = match handler.delete(&txn, request_value.clone()) {
                        Ok(fut) => fut,
                        Err(err) => return tc_error_response(err),
                    };
                    match fut.await {
                        Ok(value) => value,
                        Err(err) => return tc_error_response(err),
                    }
                }
            };

            match encode_value_body(response_value).await {
                Ok(body) => Response::builder()
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

fn tc_error_response(err: TCError) -> Response<Body> {
    let status = match err.code() {
        ErrorKind::BadGateway | ErrorKind::BadRequest => StatusCode::BAD_REQUEST,
        ErrorKind::Conflict => StatusCode::CONFLICT,
        ErrorKind::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
        ErrorKind::NotFound => StatusCode::NOT_FOUND,
        ErrorKind::Unauthorized => StatusCode::UNAUTHORIZED,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };

    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "text/plain")
        .body(Body::from(err.message().to_string()))
        .expect("tc error response")
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone)]
pub(crate) struct RequestBody {
    bytes: Bytes,
}

#[cfg_attr(not(test), allow(dead_code))]
impl RequestBody {
    fn new(bytes: Bytes) -> Self {
        Self { bytes }
    }

    fn is_empty(&self) -> bool {
        self.bytes.is_empty() || self.bytes.iter().all(|b| b.is_ascii_whitespace())
    }

    fn clone_bytes(&self) -> Bytes {
        self.bytes.clone()
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn decode_request_body_with_txn<T>(req: &Request<Body>) -> TCResult<Option<T>>
where
    T: destream::de::FromStream,
    T::Context: From<TxnHandle>,
{
    let body = match req.extensions().get::<RequestBody>() {
        Some(body) if !body.is_empty() => body.clone_bytes(),
        _ => return Ok(None),
    };

    let txn = req
        .extensions()
        .get::<TxnHandle>()
        .cloned()
        .ok_or_else(|| TCError::internal("missing transaction handle for request body"))?;

    let stream = stream::iter(vec![Ok::<Bytes, std::io::Error>(body)]);

    destream_json::try_decode(T::Context::from(txn), stream)
        .await
        .map(Some)
        .map_err(|err| TCError::bad_request(err.to_string()))
}

async fn decode_value_body(req: &Request<Body>) -> TCResult<Option<Value>> {
    let body = match req.extensions().get::<RequestBody>() {
        Some(body) if !body.is_empty() => body.clone_bytes(),
        _ => return Ok(None),
    };

    let stream = stream::iter(vec![Ok::<Bytes, std::io::Error>(body)]);

    destream_json::try_decode((), stream)
        .await
        .map(Some)
        .map_err(|err| TCError::bad_request(err.to_string()))
}

fn schema_response(schema: LibrarySchema) -> Response<Body> {
    match destream_json::encode(schema) {
        Ok(stream) => {
            let body = Body::wrap_stream(stream.map_err(|err| io::Error::other(err.to_string())));
            Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(body)
                .expect("native schema response")
        }
        Err(err) => internal_error_response(&err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NativeLibrary, State};
    use hyper::Body;
    use pathlink::Link;
    use std::str::FromStr;
    use tc_ir::{
        HandleDelete, HandleGet, HandlePost, HandlePut, LibraryModule,
        tc_library_routes,
    };

    fn ok_handler() -> impl crate::KernelHandler<Request<Body>, Response<Body>> {
        move |_req: Request<Body>| {
            async {
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("ok"))
                    .expect("ok response")
            }
            .boxed()
        }
    }

    fn kernel_with_lib_handler() -> HttpKernel {
        build_http_kernel(ok_handler(), ok_handler(), ok_handler())
    }

    fn kernel_with_ok_routing() -> HttpKernel {
        Kernel::builder()
            .with_lib_handler(ok_handler())
            .with_lib_put_handler(ok_handler())
            .with_lib_route_handler(ok_handler())
            .with_service_handler(ok_handler())
            .with_kernel_handler(ok_handler())
            .with_health_handler(ok_handler())
            .finish()
    }

    #[tokio::test]
    async fn txn_begin_and_commit() {
        let kernel = kernel_with_lib_handler();
        let txn_manager = kernel.txn_manager().clone();
        let mut service = KernelService::new(kernel);

        let request = Request::builder()
            .method("GET")
            .uri("/lib")
            .body(Body::empty())
            .expect("begin request");

        let response = service.call(request).await.expect("begin response");
        assert_eq!(response.status(), StatusCode::OK);

        let pending = txn_manager.pending_ids();
        assert_eq!(pending.len(), 1);
        let txn_id_value = pending[0];

        let commit_request = Request::builder()
            .method("POST")
            .uri(format!("/lib?txn_id={txn_id_value}"))
            .body(Body::empty())
            .expect("commit request");

        let commit_response = service.call(commit_request).await.expect("commit response");
        assert_eq!(commit_response.status(), StatusCode::NO_CONTENT);

        // committing again should fail with 400
        let retry_commit = Request::builder()
            .method("POST")
            .uri(format!("/lib?txn_id={txn_id_value}"))
            .body(Body::empty())
            .expect("repeat commit");

        let retry_response = service.call(retry_commit).await.expect("repeat response");
        assert_eq!(retry_response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn rejects_request_with_mismatched_owner_token() {
        let kernel = kernel_with_lib_handler();
        let mut service = KernelService::new(kernel);

        let begin = Request::builder()
            .method("GET")
            .uri("/lib")
            .header(hyper::header::AUTHORIZATION, "Bearer owner-a")
            .body(Body::empty())
            .expect("begin request");

        let response = service.call(begin).await.expect("begin response");
        assert_eq!(response.status(), StatusCode::OK);
        let txn_id = response
            .headers()
            .get("x-tc-txn-id")
            .and_then(|value| value.to_str().ok())
            .expect("missing x-tc-txn-id header");

        let continue_req = Request::builder()
            .method("GET")
            .uri(format!("/lib?txn_id={txn_id}"))
            .header(hyper::header::AUTHORIZATION, "Bearer owner-b")
            .body(Body::empty())
            .expect("continue request");

        let continue_resp = service.call(continue_req).await.expect("continue response");
        assert_eq!(continue_resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn finalize_is_root_only() {
        let kernel = kernel_with_ok_routing();
        let txn_manager = kernel.txn_manager().clone();
        let mut service = KernelService::new(kernel);

        let begin = Request::builder()
            .method("GET")
            .uri("/lib")
            .body(Body::empty())
            .expect("begin request");

        let response = service.call(begin).await.expect("begin response");
        assert_eq!(response.status(), StatusCode::OK);

        let pending = txn_manager.pending_ids();
        assert_eq!(pending.len(), 1);
        let txn_id_value = pending[0];

        let non_root_commit = Request::builder()
            .method("POST")
            .uri(format!("/lib/hello?txn_id={txn_id_value}"))
            .body(Body::empty())
            .expect("non-root commit request");

        let response = service
            .call(non_root_commit)
            .await
            .expect("non-root commit response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(txn_manager.pending_ids(), vec![txn_id_value]);

        let root_commit = Request::builder()
            .method("POST")
            .uri(format!("/lib?txn_id={txn_id_value}"))
            .body(Body::empty())
            .expect("root commit request");

        let response = service
            .call(root_commit)
            .await
            .expect("root commit response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(txn_manager.pending_ids().is_empty());
    }

    #[tokio::test]
    async fn finalize_root_parsing_for_component_paths() {
        let kernel = kernel_with_ok_routing();
        let txn_manager = kernel.txn_manager().clone();
        let mut service = KernelService::new(kernel);

        let begin = Request::builder()
            .method("GET")
            .uri("/lib")
            .body(Body::empty())
            .expect("begin request");

        let response = service.call(begin).await.expect("begin response");
        assert_eq!(response.status(), StatusCode::OK);

        let pending = txn_manager.pending_ids();
        assert_eq!(pending.len(), 1);
        let txn_id_value = pending[0];

        let non_root = Request::builder()
            .method("POST")
            .uri(format!("/lib/acme/foo/1.0.0/echo?txn_id={txn_id_value}"))
            .body(Body::empty())
            .expect("non-root request");

        let response = service.call(non_root).await.expect("non-root response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(txn_manager.pending_ids(), vec![txn_id_value]);

        let root = Request::builder()
            .method("POST")
            .uri(format!("/lib/acme/foo/1.0.0?txn_id={txn_id_value}"))
            .body(Body::empty())
            .expect("root request");

        let response = service.call(root).await.expect("root response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(txn_manager.pending_ids().is_empty());

        let begin = Request::builder()
            .method("GET")
            .uri("/service")
            .body(Body::empty())
            .expect("service begin request");

        let response = service.call(begin).await.expect("service begin response");
        assert_eq!(response.status(), StatusCode::OK);

        let pending = txn_manager.pending_ids();
        assert_eq!(pending.len(), 1);
        let txn_id_value = pending[0];

        let non_root = Request::builder()
            .method("DELETE")
            .uri(format!(
                "/service/acme/ns/foo/1.0.0/echo?txn_id={txn_id_value}"
            ))
            .body(Body::empty())
            .expect("service non-root request");

        let response = service
            .call(non_root)
            .await
            .expect("service non-root response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(txn_manager.pending_ids(), vec![txn_id_value]);

        let root = Request::builder()
            .method("DELETE")
            .uri(format!("/service/acme/ns/foo/1.0.0?txn_id={txn_id_value}"))
            .body(Body::empty())
            .expect("service root request");

        let response = service.call(root).await.expect("service root response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(txn_manager.pending_ids().is_empty());
    }

    #[tokio::test]
    async fn resolves_opref_via_http_gateway() {
        use base64::Engine as _;
        use tc_ir::OpRef;

        let bytes = crate::test_utils::wasm_hello_world_module();
        let initial =
            tc_ir::LibrarySchema::new(Link::from_str("/lib/initial").unwrap(), "0.0.1", vec![]);
        let module = crate::library::http::build_http_library_module(initial, None);
        let handlers = crate::library::http::http_library_handlers(&module);

        let remote_kernel: HttpKernel = Kernel::builder()
            .with_host_id("tc-wasm-test")
            .with_library_module(module, handlers)
            .with_service_handler(ok_handler())
            .with_kernel_handler(ok_handler())
            .with_health_handler(ok_handler())
            .finish();

        let install_payload = serde_json::json!({
            "schema": {
                "id": "/lib/example",
                "version": "0.1.0",
                "dependencies": []
            },
            "artifacts": [{
                "path": "/lib/wasm",
                "content_type": "application/wasm",
                "bytes": base64::engine::general_purpose::STANDARD.encode(&bytes),
            }]
        });

        let install_request = Request::builder()
            .method("PUT")
            .uri("/lib")
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Body::from(install_payload.to_string()))
            .expect("install request");

        let install_response = remote_kernel
            .dispatch(Method::Put, "/lib", install_request)
            .expect("install handler")
            .await;
        assert_eq!(install_response.status(), StatusCode::NO_CONTENT);

        let txn_id = remote_kernel.txn_manager().begin().id();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let server_kernel = remote_kernel.clone();
        let server = hyper::Server::from_tcp(listener)
            .expect("hyper server")
            .serve(tower::make::Shared::new(KernelService::new(server_kernel)));

        let server_task = tokio::spawn(async move { server.await.expect("server") });

        let schema = LibrarySchema::new(
            Link::from_str("/lib/acme/a/1.0.0").expect("schema id"),
            "1.0.0",
            vec![Link::from_str("/lib").expect("dependency root")],
        );

        let module = crate::library::http::build_http_library_module(schema, None);
        let handlers = crate::library::http::http_library_handlers(&module);

        let local_kernel: HttpKernel = crate::Kernel::builder()
            .with_library_module(module, handlers)
            .with_dependency_route("/lib", addr.to_string())
            .with_http_rpc_gateway()
            .finish();

        let link = Link::from_str("/lib/hello").expect("op link");
        let op = OpRef::Get((tc_ir::Subject::Link(link), tc_ir::Scalar::default()));

        let response = local_kernel
            .resolve_op(txn_id, None, op)
            .await
            .expect("resolve response");

        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"\"hello\"");

        server_task.abort();
    }

    #[tokio::test]
    async fn decodes_body_with_txn_context() {
        let request = Request::builder()
            .method("PUT")
            .uri("/lib")
            .body(Body::from("{}"))
            .expect("request");
        let mut request = request;
        let txn = crate::txn::TxnManager::with_host_id("test-host").begin();
        request.extensions_mut().insert(txn);
        request
            .extensions_mut()
            .insert(RequestBody::new(Bytes::from_static(
                b"{\"/state/scalar/value/string\": \"bar\"}",
            )));

        let state = decode_request_body_with_txn::<State>(&request)
            .await
            .expect("decode")
            .expect("some");

        assert!(!matches!(state, State::None));
    }

    #[tokio::test]
    async fn serves_native_library_route() {
        #[derive(Clone)]
        struct HelloHandler;

        impl HandleGet<TxnHandle> for HelloHandler {
            type Request = Value;
            type RequestContext = ();
            type Response = Value;
            type Error = TCError;
            type Fut<'a> = futures::future::BoxFuture<'a, Result<Self::Response, Self::Error>>;

            fn get<'a>(
                &'a self,
                _txn: &'a TxnHandle,
                _request: Self::Request,
            ) -> TCResult<Self::Fut<'a>> {
                Ok(Box::pin(async move { Ok(Value::from(42_u64)) }))
            }
        }

        impl HandlePut<TxnHandle> for HelloHandler {
            type Request = Value;
            type RequestContext = ();
            type Response = Value;
            type Error = TCError;
            type Fut<'a> = futures::future::BoxFuture<'a, Result<Self::Response, Self::Error>>;
        }

        impl HandlePost<TxnHandle> for HelloHandler {
            type Request = Value;
            type RequestContext = ();
            type Response = Value;
            type Error = TCError;
            type Fut<'a> = futures::future::BoxFuture<'a, Result<Self::Response, Self::Error>>;
        }

        impl HandleDelete<TxnHandle> for HelloHandler {
            type Request = Value;
            type RequestContext = ();
            type Response = Value;
            type Error = TCError;
            type Fut<'a> = futures::future::BoxFuture<'a, Result<Self::Response, Self::Error>>;
        }

        let schema = LibrarySchema::new(
            Link::from_str("/lib/native").expect("schema link"),
            "0.1.0",
            vec![],
        );
        let routes = tc_library_routes! {
            "/hello" => HelloHandler,
        }
        .expect("routes");
        let module = LibraryModule::new(schema, routes);
        let library = NativeLibrary::new(module);

        let kernel = build_http_kernel_with_native_library(
            library,
            ok_handler(),
            ok_handler(),
            ok_handler(),
        );

        let txn = kernel.txn_manager().begin();

        let mut request = Request::builder()
            .method("GET")
            .uri("/lib/hello")
            .body(Body::empty())
            .expect("request");
        request.extensions_mut().insert(txn);

        let response = kernel
            .dispatch(Method::Get, "/lib/hello", request)
            .expect("native handler")
            .await;

        assert_eq!(response.status(), StatusCode::OK);

        let body = hyper::body::to_bytes(response.into_body())
            .await
            .expect("body");
        let stream = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(body)]);
        let value: Value = destream_json::try_decode((), stream).await.expect("decode");
        assert_eq!(value, Value::from(42_u64));
    }

    #[cfg(feature = "rjwt-token")]
    #[tokio::test]
    async fn verifies_rjwt_using_host_public_key_endpoint() {
        use crate::auth::TokenVerifier as _;
        use rjwt::Token;
        use std::net::TcpListener;
        use tc_ir::{Claim, NetworkTime, TxnId};
        use tc_value::Value;
        use umask::USER_EXEC;

        let actor = rjwt::Actor::new(Value::from("actor-a"));
        let keys = crate::auth::PublicKeyStore::default();
        keys.insert_actor(&actor);

        let host_handler = super::host_handler_with_public_keys(keys);

        let kernel = build_http_kernel_with_config(
            HttpKernelConfig::default().with_host_id("tc-http-host-keys"),
            ok_handler(),
            host_handler,
            ok_handler(),
        );

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("local addr");

        let server = tokio::spawn(async move {
            HttpServer::new(kernel)
                .serve_listener(listener)
                .await
                .expect("http server");
        });

        tokio::task::yield_now().await;

        let txn_id = TxnId::from_parts(NetworkTime::from_nanos(1), 1).with_trace([0; 32]);
        let host = Link::from_str(&format!("http://{addr}")).expect("host link");

        let claim = Claim::new(
            Link::from_str(&format!("/txn/{txn_id}")).expect("txn claim"),
            USER_EXEC,
        );

        let token = Token::new(
            host.clone(),
            std::time::SystemTime::now(),
            std::time::Duration::from_secs(30),
            actor.id().clone(),
            claim,
        );
        let signed = actor.sign_token(token).expect("signed token");

        let gateway: std::sync::Arc<dyn crate::gateway::RpcGateway> =
            std::sync::Arc::new(crate::http_client::HttpRpcGateway::new());
        let resolver = crate::auth::RpcActorResolver::new(gateway, txn_id);
        let verifier = crate::auth::RjwtTokenVerifier::new(std::sync::Arc::new(resolver));
        let ctx = verifier
            .verify(signed.into_jwt())
            .await
            .expect("verified token via RPC");

        assert_eq!(ctx.owner_id, format!("{host}::actor-a"));

        server.abort();
    }
}
