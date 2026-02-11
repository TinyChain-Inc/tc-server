#[cfg(all(test, feature = "pyo3", feature = "http-server", feature = "wasm"))]
mod tests {
    use crate::{Body, Kernel, KernelHandler, Request, Response, StatusCode};
    use crate::resolve::Resolve;
    use base64::Engine as _;
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
                    Ok(crate::KernelDispatch::Finalize { commit: _, result }) => {
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
            .with_service_handler(|_req| async move {
                http::Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("service"))
                    .expect("service response")
            })
            .with_kernel_handler(|_req| async move { Response::new(Body::empty()) })
            .with_health_handler(|_req| async move { Response::new(Body::empty()) })
            .finish();

        let install_payload = serde_json::json!({
            "schema": {
                "id": "/lib/example-devco/example/0.1.0",
                "version": "0.1.0",
                "dependencies": []
            },
            "artifacts": [{
                "path": "/lib/wasm",
                "content_type": "application/wasm",
                "bytes": base64::engine::general_purpose::STANDARD.encode(&bytes),
            }]
        });

        let mut install_request = http::Request::builder()
            .method("PUT")
            .uri("/lib")
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Body::from(install_payload.to_string()))
            .expect("install request");
        let install_claim = tc_ir::Claim::new(
            Link::from_str("/lib/example-devco/example/0.1.0").unwrap(),
            umask::USER_WRITE,
        );
        let install_txn = remote_kernel
            .txn_manager()
            .begin()
            .with_claims(vec![install_claim]);
        install_request.extensions_mut().insert(install_txn);

        let install_response = remote_kernel
            .dispatch(crate::Method::Put, "/lib", install_request)
            .expect("install handler")
            .await;
        assert_eq!(install_response.status(), StatusCode::NO_CONTENT);

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
            .with_service_handler(ok_py_handler())
            .with_kernel_handler(ok_py_handler())
            .with_health_handler(ok_py_handler())
            .finish();

        let remote_txn = remote_kernel.txn_manager().begin();
        let owner_id = remote_txn.owner_id().expect("remote txn owner");
        let bearer = remote_txn.raw_token().expect("remote txn bearer token");
        let _ = local_kernel.txn_manager().interpret_request(
            Some(remote_txn.id()),
            Some(owner_id),
            Some(bearer),
        );
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
}
