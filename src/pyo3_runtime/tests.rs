#[cfg(all(test, feature = "pyo3", feature = "http-server", feature = "wasm"))]
mod tests {
    use crate::auth::{TokenContext, TokenVerifier};
    use crate::library::CompiledLibraryPackage;
    use crate::resolve::Resolve;
    use crate::storage::Artifact;
    use crate::{Body, Kernel, KernelHandler, Request, Response, StatusCode};
    use futures::FutureExt;
    use hyper::body::to_bytes;
    use pathlink::Link;
    use std::{net::TcpListener, str::FromStr};
    use tower::Service;
    use url::form_urlencoded;

    #[derive(Clone)]
    struct KernelService {
        kernel: crate::Kernel,
    }

    impl KernelService {
        fn new(kernel: crate::Kernel) -> Self {
            Self { kernel }
        }
    }

    impl Service<Request> for KernelService {
        type Response = Response;
        type Error = hyper::Error;
        type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: Request) -> Self::Future {
            let uri = req.uri().clone();
            let method = req.method().clone();
            let path = uri.path().to_owned();
            let kernel = self.kernel.clone();

            Box::pin(async move {
                let method = match method {
                    crate::HttpMethod::GET => crate::Method::Get,
                    crate::HttpMethod::PUT => crate::Method::Put,
                    crate::HttpMethod::POST => crate::Method::Post,
                    crate::HttpMethod::DELETE => crate::Method::Delete,
                    _ => {
                        return Ok(http::Response::builder()
                            .status(StatusCode::METHOD_NOT_ALLOWED)
                            .body(Body::empty())
                            .expect("method not allowed"));
                    }
                };

                let (req, body_is_none) = match parse_body(req).await {
                    Ok(pair) => pair,
                    Err(resp) => return Ok(resp),
                };

                let txn_id = match parse_txn_id(&req) {
                    Ok(id) => id,
                    Err(()) => {
                        return Ok(http::Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from("invalid transaction id"))
                            .expect("bad request"));
                    }
                };

                let bearer = req
                    .headers()
                    .get(crate::header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|auth| auth.split_once(' '))
                    .and_then(|(scheme, token)| {
                        if scheme.eq_ignore_ascii_case("bearer") {
                            Some(token.trim().to_string())
                        } else {
                            None
                        }
                    });

                let token = match bearer {
                    Some(token) => match kernel.token_verifier().verify(token).await {
                        Ok(token) => Some(token),
                        Err(crate::txn::TxnError::Unauthorized) => {
                            return Ok(http::Response::builder()
                                .status(StatusCode::UNAUTHORIZED)
                                .body(Body::empty())
                                .expect("unauthorized"));
                        }
                        Err(crate::txn::TxnError::NotFound) => None,
                    },
                    None => None,
                };

                match kernel.route_request(
                    method,
                    &path,
                    req,
                    txn_id,
                    body_is_none,
                    token.as_ref(),
                    |handle, req| {
                        req.extensions_mut().insert(handle.clone());
                    },
                ) {
                    Ok(crate::KernelDispatch::Response(resp)) => Ok(resp.await),
                    Ok(crate::KernelDispatch::Finalize { commit, txn }) => {
                        let result = kernel.finalize_transaction(txn, commit).await;
                        let status = if result.is_ok() {
                            StatusCode::NO_CONTENT
                        } else {
                            StatusCode::BAD_REQUEST
                        };
                        Ok(http::Response::builder()
                            .status(status)
                            .body(Body::empty())
                            .expect("finalize response"))
                    }
                    Ok(crate::KernelDispatch::NotFound) => Ok(http::Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Body::empty())
                        .expect("not found")),
                    Err(crate::txn::TxnError::NotFound) => Ok(http::Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("unknown transaction id"))
                        .expect("bad request")),
                    Err(crate::txn::TxnError::Unauthorized) => Ok(http::Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body(Body::empty())
                        .expect("unauthorized")),
                }
            })
        }
    }

    async fn parse_body(req: Request) -> Result<(Request, bool), Response> {
        let (parts, body) = req.into_parts();
        let body_bytes = to_bytes(body).await.map_err(|_| {
            http::Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from("failed to read request body"))
                .expect("internal error")
        })?;
        let body_is_none = body_bytes.iter().all(|b| b.is_ascii_whitespace());
        Ok((
            Request::from_parts(parts, Body::from(body_bytes)),
            body_is_none,
        ))
    }

    fn parse_txn_id(req: &Request) -> Result<Option<tc_ir::TxnId>, ()> {
        let query = req.uri().query().unwrap_or("");
        let txn_id_param = form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .find(|(k, _)| k.eq_ignore_ascii_case("txn_id"))
            .map(|(_, v)| v);

        match txn_id_param {
            Some(raw) => raw.parse::<tc_ir::TxnId>().map(Some).map_err(|_| ()),
            None => Ok(None),
        }
    }

    fn ok_py_handler() -> impl KernelHandler {
        |_req: Request| async move { Response::new(Body::empty()) }.boxed()
    }

    #[derive(Clone, Default)]
    struct TestBearerVerifier;

    impl TokenVerifier for TestBearerVerifier {
        fn verify(
            &self,
            bearer_token: String,
        ) -> futures::future::BoxFuture<
            'static,
            Result<crate::auth::TokenContext, crate::txn::TxnError>,
        > {
            use std::str::FromStr;

            futures::future::ready({
                let mut parts = bearer_token.splitn(2, '|');
                let actor_id = parts.next().unwrap_or_default().trim().to_string();
                if actor_id.is_empty() {
                    Err(crate::txn::TxnError::Unauthorized)
                } else {
                    let owner_id = format!("/host::{actor_id}");
                    let txn_id = parts.next().and_then(|part| tc_ir::TxnId::from_str(part).ok());
                    let mut ctx = TokenContext::new(owner_id, bearer_token);
                    if let Some(txn_id) = txn_id {
                        let claim = tc_ir::Claim::new(
                            Link::from_str(&format!("/txn/{txn_id}")).expect("txn claim link"),
                            umask::USER_EXEC | umask::USER_WRITE,
                        );
                        ctx.claims
                            .push((crate::uri::HOST_ROOT.to_string(), actor_id, claim));
                    }
                    Ok(ctx)
                }
            })
            .boxed()
        }

        fn grant(
            &self,
            token: crate::auth::TokenContext,
            _claim: tc_ir::Claim,
        ) -> futures::future::BoxFuture<
            'static,
            Result<crate::auth::TokenContext, crate::txn::TxnError>,
        > {
            futures::future::ready(Ok(token)).boxed()
        }
    }

    fn wasm_module() -> Vec<u8> {
        crate::test_utils::wasm_hello_world_module()
    }

    #[tokio::test]
    async fn pyo3_kernel_resolves_remote_opref_over_http() {
        use tc_ir::OpRef;

        let bytes = wasm_module();
        let initial =
            tc_ir::LibrarySchema::new(Link::from_str("/lib/initial").unwrap(), "0.0.1", vec![]);
        let module = crate::library::http::build_http_library_module(initial, None)
            .await
            .expect("module");
        let handlers = crate::library::http::http_library_handlers(&module);

        let remote_kernel: crate::Kernel = crate::Kernel::builder()
            .with_host_id("tc-remote-test")
            .with_library_module(module, handlers)
            .with_token_verifier(TestBearerVerifier)
            .with_service_handler(|_req| async move {
                http::Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("service"))
                    .expect("service response")
            })
            .with_kernel_handler(|_req| async move { Response::new(Body::empty()) })
            .with_health_handler(|_req| async move { Response::new(Body::empty()) })
            .finish();

        remote_kernel
            .library_registry()
            .expect("library registry")
            .install_compiled_package(CompiledLibraryPackage {
                schema: tc_ir::LibrarySchema::new(
                    Link::from_str("/lib/example-devco/example/0.1.0").unwrap(),
                    "0.1.0",
                    vec![],
                ),
                artifacts: vec![Artifact {
                    path: "/lib/wasm".to_string(),
                    content_type: crate::ir::WASM_ARTIFACT_CONTENT_TYPE.to_string(),
                    bytes,
                }],
            })
            .await
            .expect("install compiled package");

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let server_kernel = remote_kernel.clone();
        let server = hyper::Server::from_tcp(listener)
            .expect("hyper server")
            .serve(tower::make::Shared::new(KernelService::new(server_kernel)));

        let server_task = tokio::spawn(async move { server.await.expect("server") });

        let dependency_root = "/lib/example-devco/example/0.1.0";
        let local_schema = tc_ir::LibrarySchema::new(
            Link::from_str("/lib/local").unwrap(),
            "0.1.0",
            vec![Link::from_str(dependency_root).unwrap()],
        );
        let local_module = crate::library::http::build_http_library_module(local_schema, None)
            .await
            .expect("module");
        let local_handlers = crate::library::http::http_library_handlers(&local_module);

        let local_kernel: Kernel = crate::Kernel::builder()
            .with_host_id("tc-py-local")
            .with_library_module(local_module, local_handlers)
            .with_dependency_route(dependency_root, addr)
            .with_http_rpc_gateway()
            .with_token_verifier(TestBearerVerifier)
            .with_service_handler(ok_py_handler())
            .with_kernel_handler(ok_py_handler())
            .with_health_handler(ok_py_handler())
            .finish();

        let owner = "/host::pyo3-test-bearer";
        let seeded_remote_txn = remote_kernel
            .txn_manager()
            .begin_with_owner(Some(owner), Some("pyo3-test-bearer"));
        let bearer = format!("pyo3-test-bearer|{}", seeded_remote_txn.id());
        let remote_txn = seeded_remote_txn.with_bearer_token(bearer);
        let txn = local_kernel.with_resolver(remote_txn);
        let op = OpRef::Get((
            tc_ir::Subject::Link(Link::from_str("/lib/example-devco/example/0.1.0/hello").unwrap()),
            tc_ir::Scalar::default(),
        ));

        let state = op.resolve(&txn).await.expect("resolve response");

        assert!(matches!(
            state,
            tc_state::State::Scalar(tc_ir::Scalar::Value(tc_value::Value::String(ref s))) if s == "hello"
        ));

        server_task.abort();
    }

    #[tokio::test]
    async fn pyo3_kernel_rejects_unauthorized_egress() {
        use tc_ir::OpRef;

        let local_schema = tc_ir::LibrarySchema::new(
            Link::from_str("/lib/local").unwrap(),
            "0.1.0",
            vec![Link::from_str("/lib").unwrap()],
        );
        let local_module = crate::library::http::build_http_library_module(local_schema, None)
            .await
            .expect("module");
        let local_handlers = crate::library::http::http_library_handlers(&local_module);

        let local_kernel: Kernel = crate::Kernel::builder()
            .with_host_id("tc-py-local")
            .with_library_module(local_module, local_handlers)
            .with_dependency_route("/lib", "127.0.0.1:1".parse().expect("addr"))
            .with_http_rpc_gateway()
            .with_service_handler(ok_py_handler())
            .with_kernel_handler(ok_py_handler())
            .with_health_handler(ok_py_handler())
            .finish();

        let txn = local_kernel.with_resolver(local_kernel.txn_manager().begin());
        let op = OpRef::Get((
            tc_ir::Subject::Link(Link::from_str("/service").unwrap()),
            tc_ir::Scalar::default(),
        ));

        let err = op.resolve(&txn).await.expect_err("unauthorized");

        assert_eq!(err.code(), tc_error::ErrorKind::Unauthorized);
    }

    #[tokio::test]
    async fn pyo3_request_prefers_native_state_extension() {
        use super::state_handle_conversions::request_body_state;
        use super::wire::py_request_from_http;

        pyo3::prepare_freethreaded_python();

        let mut request = http::Request::builder()
            .method(http::Method::POST)
            .uri("/service/test")
            .body(Body::from("not-json"))
            .expect("request");

        request
            .extensions_mut()
            .insert(crate::http::NativeStateBody::new(crate::State::from(
                tc_value::Value::from("native-request"),
            )));

        let py_request = py_request_from_http(request).await.expect("py request");
        let state = request_body_state(py_request.body())
            .expect("state decode")
            .expect("some state");

        assert!(matches!(
            state,
            tc_state::State::Scalar(tc_ir::Scalar::Value(tc_value::Value::String(ref s))) if s == "native-request"
        ));
    }

    #[tokio::test]
    async fn pyo3_request_native_state_extension_all_methods() {
        use super::state_handle_conversions::request_body_state;
        use super::wire::py_request_from_http;

        pyo3::prepare_freethreaded_python();

        let cases = [
            (http::Method::GET, "GET"),
            (http::Method::PUT, "PUT"),
            (http::Method::POST, "POST"),
            (http::Method::DELETE, "DELETE"),
        ];

        for (method, expected_method) in cases {
            let mut request = http::Request::builder()
                .method(method)
                .uri("/service/test")
                .body(Body::from("not-json"))
                .expect("request");

            request
                .extensions_mut()
                .insert(crate::http::NativeStateBody::new(crate::State::from(
                    tc_value::Value::from("native-request"),
                )));

            let py_request = py_request_from_http(request).await.expect("py request");
            assert_eq!(py_request.method(), expected_method);

            let state = request_body_state(py_request.body())
                .expect("state decode")
                .expect("some state");

            assert!(matches!(
                state,
                tc_state::State::Scalar(tc_ir::Scalar::Value(tc_value::Value::String(ref s))) if s == "native-request"
            ));
        }
    }

    #[tokio::test]
    async fn pyo3_request_rejects_non_native_non_empty_payload() {
        use super::wire::py_request_from_http;

        pyo3::prepare_freethreaded_python();

        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri("/service/test")
            .body(Body::from("{\"x\":1}"))
            .expect("request");

        let err = py_request_from_http(request)
            .await
            .expect_err("non-native payload should fail");

        assert!(err
            .to_string()
            .contains("missing native request body extension"));
    }

    #[tokio::test]
    async fn pyo3_response_to_http_sets_native_state_extension() {
        use super::state_handle_conversions::py_state_handle_from_state;
        use super::wire::py_response_to_http;
        use super::PyKernelResponse;

        pyo3::prepare_freethreaded_python();

        let native = crate::State::from(tc_value::Value::from("native-response"));
        let body = Some(py_state_handle_from_state(native.clone()).expect("state handle"));
        let py_response = PyKernelResponse::new(200, None, body);

        let response = py_response_to_http(py_response).await.expect("http response");
        let native_ext = response
            .extensions()
            .get::<crate::http::NativeStateResponse>()
            .expect("native extension");

        assert!(matches!(
            native_ext.clone_state(),
            tc_state::State::Scalar(tc_ir::Scalar::Value(tc_value::Value::String(ref s))) if s == "native-response"
        ));
    }

    #[tokio::test]
    async fn pyo3_response_prefers_native_state_extension() {
        use super::state_handle_conversions::request_body_state;
        use super::wire::py_response_from_http;

        pyo3::prepare_freethreaded_python();

        let mut response = http::Response::builder()
            .status(StatusCode::OK)
            .body(Body::from("not-json"))
            .expect("response");

        response
            .extensions_mut()
            .insert(crate::http::NativeStateResponse::new(crate::State::from(
                tc_value::Value::from("native"),
            )));

        let py_response = py_response_from_http(response).await.expect("py response");
        let state = request_body_state(py_response.body())
            .expect("state decode")
            .expect("some state");

        assert!(matches!(
            state,
            tc_state::State::Scalar(tc_ir::Scalar::Value(tc_value::Value::String(ref s))) if s == "native"
        ));
    }

    #[tokio::test]
    async fn pyo3_response_rejects_non_native_success_payload() {
        use super::wire::py_response_from_http;

        pyo3::prepare_freethreaded_python();

        let response = http::Response::builder()
            .status(StatusCode::OK)
            .body(Body::from("{\"ok\":true}"))
            .expect("response");

        let err = py_response_from_http(response)
            .await
            .expect_err("non-native success payload should fail");

        assert!(err
            .to_string()
            .contains("missing native response extension"));
    }
}
