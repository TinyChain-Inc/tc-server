#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::TokenVerifier;
    use crate::resolve::Resolve;
    use crate::{Kernel, Method, NativeLibrary, State, TxnHandle, Value};
    use bytes::Bytes;
    use futures::FutureExt;
    use hyper::Body;
    use pathlink::Link;
    use std::str::FromStr;
    use tc_error::TCError;
    use tc_error::TCResult;
    use tc_ir::{LibrarySchema, TxnId};
    use tc_ir::{HandleDelete, HandleGet, HandlePost, HandlePut, LibraryModule, tc_library_routes};
    use tower::Service;

    fn ok_handler() -> impl crate::KernelHandler {
        move |_req: Request| {
            async {
                http::Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("ok"))
                    .expect("ok response")
            }
            .boxed()
        }
    }

    fn kernel_with_lib_handler() -> Kernel {
        Kernel::builder()
            .with_lib_handler(ok_handler())
            .with_lib_put_handler(ok_handler())
            .with_lib_route_handler(ok_handler())
            .with_service_handler(ok_handler())
            .with_kernel_handler(ok_handler())
            .with_health_handler(ok_handler())
            .with_token_verifier(TestTokenVerifier)
            .finish()
    }

    fn kernel_with_ok_routing() -> Kernel {
        Kernel::builder()
            .with_lib_handler(ok_handler())
            .with_lib_put_handler(ok_handler())
            .with_lib_route_handler(ok_handler())
            .with_service_handler(ok_handler())
            .with_kernel_handler(ok_handler())
            .with_health_handler(ok_handler())
            .with_token_verifier(TestTokenVerifier)
            .finish()
    }

    #[derive(Clone)]
    struct TestTokenVerifier;

    impl TokenVerifier for TestTokenVerifier {
        fn verify(
            &self,
            bearer_token: String,
        ) -> futures::future::BoxFuture<
            'static,
            Result<crate::auth::TokenContext, crate::txn::TxnError>,
        > {
            use std::str::FromStr;

            let mut parts = bearer_token.splitn(2, '|');
            let actor_id = parts.next().unwrap_or_default().to_string();
            let owner_id = format!("/host::{actor_id}");
            let txn_id = parts.next().and_then(|part| TxnId::from_str(part).ok());

            let mut ctx = crate::auth::TokenContext::new(owner_id.clone(), bearer_token);
            if let Some(txn_id) = txn_id {
                let claim = tc_ir::Claim::new(
                    Link::from_str(&format!("/txn/{txn_id}")).expect("txn claim link"),
                    umask::USER_EXEC | umask::USER_WRITE,
                );
                ctx.claims
                    .push((crate::uri::HOST_ROOT.to_string(), actor_id, claim));
            }

            futures::future::ready(Ok(ctx)).boxed()
        }
    }

    #[tokio::test]
    async fn txn_begin_and_commit() {
        let kernel = kernel_with_lib_handler();
        let txn_manager = kernel.txn_manager().clone();
        let mut service = KernelService::new(kernel, crate::KernelLimits::default());

        let request = http::Request::builder()
            .method("GET")
            .uri("/lib")
            .header(hyper::header::AUTHORIZATION, "Bearer owner-a")
            .body(Body::empty())
            .expect("begin request");

        let response = service.call(request).await.expect("begin response");
        assert_eq!(response.status(), StatusCode::OK);

        let pending = txn_manager.pending_ids();
        assert_eq!(pending.len(), 1);
        let txn_id_value = pending[0];

        let commit_request = http::Request::builder()
            .method("POST")
            .uri(format!("/lib?txn_id={txn_id_value}"))
            .header(
                hyper::header::AUTHORIZATION,
                format!("Bearer owner-a|{txn_id_value}"),
            )
            .body(Body::empty())
            .expect("commit request");

        let commit_response = service.call(commit_request).await.expect("commit response");
        assert_eq!(commit_response.status(), StatusCode::NO_CONTENT);

        // committing again should fail with 400
        let retry_commit = http::Request::builder()
            .method("POST")
            .uri(format!("/lib?txn_id={txn_id_value}"))
            .header(
                hyper::header::AUTHORIZATION,
                format!("Bearer owner-a|{txn_id_value}"),
            )
            .body(Body::empty())
            .expect("repeat commit");

        let retry_response = service.call(retry_commit).await.expect("repeat response");
        assert_eq!(retry_response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn returns_bearer_token_for_anonymous_txn() {
        let kernel: Kernel = Kernel::builder()
            .with_lib_handler(ok_handler())
            .with_lib_put_handler(ok_handler())
            .with_lib_route_handler(ok_handler())
            .with_service_handler(ok_handler())
            .with_kernel_handler(ok_handler())
            .with_health_handler(ok_handler())
            .finish();

        let mut service = KernelService::new(kernel, crate::KernelLimits::default());

        let begin = http::Request::builder()
            .method("GET")
            .uri("/lib")
            .body(Body::empty())
            .expect("begin request");

        let response = service.call(begin).await.expect("begin response");
        assert_eq!(response.status(), StatusCode::OK);

        let txn_id = response
            .headers()
            .get("x-tc-txn-id")
            .and_then(|value| value.to_str().ok())
            .expect("missing x-tc-txn-id header");
        let bearer = response
            .headers()
            .get("x-tc-bearer-token")
            .and_then(|value| value.to_str().ok())
            .expect("missing x-tc-bearer-token header");

        let commit = http::Request::builder()
            .method("POST")
            .uri(format!("/lib?txn_id={txn_id}"))
            .body(Body::empty())
            .expect("commit request");

        let commit_response = service.call(commit).await.expect("commit response");
        assert_eq!(commit_response.status(), StatusCode::UNAUTHORIZED);

        let commit = http::Request::builder()
            .method("POST")
            .uri(format!("/lib?txn_id={txn_id}"))
            .header(hyper::header::AUTHORIZATION, format!("Bearer {bearer}"))
            .body(Body::empty())
            .expect("commit request with bearer");

        let commit_response = service.call(commit).await.expect("commit response");
        assert_eq!(commit_response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn rejects_request_with_mismatched_owner_token() {
        let kernel = kernel_with_lib_handler();
        let mut service = KernelService::new(kernel, crate::KernelLimits::default());

        let begin = http::Request::builder()
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

        let continue_req = http::Request::builder()
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
        let mut service = KernelService::new(kernel, crate::KernelLimits::default());

        let begin = http::Request::builder()
            .method("GET")
            .uri("/lib")
            .header(hyper::header::AUTHORIZATION, "Bearer owner-a")
            .body(Body::empty())
            .expect("begin request");

        let response = service.call(begin).await.expect("begin response");
        assert_eq!(response.status(), StatusCode::OK);

        let pending = txn_manager.pending_ids();
        assert_eq!(pending.len(), 1);
        let txn_id_value = pending[0];

        let non_root_commit = http::Request::builder()
            .method("POST")
            .uri(format!("/lib/hello?txn_id={txn_id_value}"))
            .header(
                hyper::header::AUTHORIZATION,
                format!("Bearer owner-a|{txn_id_value}"),
            )
            .body(Body::empty())
            .expect("non-root commit request");

        let response = service
            .call(non_root_commit)
            .await
            .expect("non-root commit response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(txn_manager.pending_ids(), vec![txn_id_value]);

        let root_commit = http::Request::builder()
            .method("POST")
            .uri(format!("/lib?txn_id={txn_id_value}"))
            .header(
                hyper::header::AUTHORIZATION,
                format!("Bearer owner-a|{txn_id_value}"),
            )
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
        let mut service = KernelService::new(kernel, crate::KernelLimits::default());

        let begin = http::Request::builder()
            .method("GET")
            .uri("/lib")
            .header(hyper::header::AUTHORIZATION, "Bearer owner-a")
            .body(Body::empty())
            .expect("begin request");

        let response = service.call(begin).await.expect("begin response");
        assert_eq!(response.status(), StatusCode::OK);

        let pending = txn_manager.pending_ids();
        assert_eq!(pending.len(), 1);
        let txn_id_value = pending[0];

        let non_root = http::Request::builder()
            .method("POST")
            .uri(format!("/lib/acme/foo/1.0.0/echo?txn_id={txn_id_value}"))
            .header(
                hyper::header::AUTHORIZATION,
                format!("Bearer owner-a|{txn_id_value}"),
            )
            .body(Body::empty())
            .expect("non-root request");

        let response = service.call(non_root).await.expect("non-root response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(txn_manager.pending_ids(), vec![txn_id_value]);

        let root = http::Request::builder()
            .method("POST")
            .uri(format!("/lib/acme/foo/1.0.0?txn_id={txn_id_value}"))
            .header(
                hyper::header::AUTHORIZATION,
                format!("Bearer owner-a|{txn_id_value}"),
            )
            .body(Body::empty())
            .expect("root request");

        let response = service.call(root).await.expect("root response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(txn_manager.pending_ids().is_empty());

        let begin = http::Request::builder()
            .method("GET")
            .uri("/service")
            .header(hyper::header::AUTHORIZATION, "Bearer owner-a")
            .body(Body::empty())
            .expect("service begin request");

        let response = service.call(begin).await.expect("service begin response");
        assert_eq!(response.status(), StatusCode::OK);

        let pending = txn_manager.pending_ids();
        assert_eq!(pending.len(), 1);
        let txn_id_value = pending[0];

        let non_root = http::Request::builder()
            .method("DELETE")
            .uri(format!(
                "/service/acme/ns/foo/1.0.0/echo?txn_id={txn_id_value}"
            ))
            .header(
                hyper::header::AUTHORIZATION,
                format!("Bearer owner-a|{txn_id_value}"),
            )
            .body(Body::empty())
            .expect("service non-root request");

        let response = service
            .call(non_root)
            .await
            .expect("service non-root response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(txn_manager.pending_ids(), vec![txn_id_value]);

        let root = http::Request::builder()
            .method("DELETE")
            .uri(format!("/service/acme/ns/foo/1.0.0?txn_id={txn_id_value}"))
            .header(
                hyper::header::AUTHORIZATION,
                format!("Bearer owner-a|{txn_id_value}"),
            )
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

        let bytes = crate::test_utils::wasm_echo_request_module();
        let initial =
            tc_ir::LibrarySchema::new(Link::from_str("/lib/initial").unwrap(), "0.0.1", vec![]);
        let module = crate::library::http::build_http_library_module(initial, None)
            .await
            .expect("module");
        let handlers = crate::library::http::http_library_handlers(&module);

        let remote_kernel: Kernel = Kernel::builder()
            .with_host_id("tc-wasm-test")
            .with_library_module(module, handlers)
            .with_service_handler(ok_handler())
            .with_kernel_handler(ok_handler())
            .with_health_handler(ok_handler())
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
            Link::from_str("/lib/example-devco/example/0.1.0").expect("install link"),
            umask::USER_WRITE,
        );
        let install_txn = remote_kernel
            .txn_manager()
            .begin()
            .with_claims(vec![install_claim]);
        install_request.extensions_mut().insert(install_txn);

        let install_response = remote_kernel
            .dispatch(Method::Put, "/lib", install_request)
            .expect("install handler")
            .await;
        assert_eq!(install_response.status(), StatusCode::NO_CONTENT);

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let server_kernel = remote_kernel.clone();
        let server = hyper::Server::from_tcp(listener)
            .expect("hyper server")
            .serve(tower::make::Shared::new(KernelService::new(
                server_kernel,
                crate::KernelLimits::default(),
            )));

        let server_task = tokio::spawn(async move { server.await.expect("server") });

        let schema = LibrarySchema::new(
            Link::from_str("/lib/acme/a/1.0.0").expect("schema id"),
            "1.0.0",
            vec![Link::from_str("/lib/example-devco/example/0.1.0").expect("dependency root")],
        );

        let module = crate::library::http::build_http_library_module(schema, None)
            .await
            .expect("module");
        let handlers = crate::library::http::http_library_handlers(&module);

        let local_kernel: Kernel = crate::Kernel::builder()
            .with_library_module(module, handlers)
            .with_dependency_route("/lib/example-devco/example/0.1.0", addr)
            .with_http_rpc_gateway()
            .finish();

        let txn = local_kernel.with_resolver(remote_kernel.txn_manager().begin());

        let link = Link::from_str("/lib/example-devco/example/0.1.0/hello").expect("op link");
        let op = OpRef::Get((
            tc_ir::Subject::Link(link),
            tc_ir::Scalar::Value(tc_value::Value::from("World")),
        ));

        let response = op.resolve(&txn).await.expect("resolve response");

        assert!(matches!(
            response,
            crate::State::Scalar(tc_ir::Scalar::Value(tc_value::Value::String(ref s))) if s == "World"
        ));

        server_task.abort();
    }

    #[tokio::test]
    async fn decodes_body_with_txn_context() {
        let request = http::Request::builder()
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

        let mut request = http::Request::builder()
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
        )
        .await
        .expect("kernel");

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
        let txn = crate::txn::TxnManager::with_host_id("tc-http-host-keys").begin();
        let resolver = crate::auth::RpcActorResolver::new(gateway, txn);
        let verifier = crate::auth::RjwtTokenVerifier::new(std::sync::Arc::new(resolver));
        let ctx = verifier
            .verify(signed.into_jwt())
            .await
            .expect("verified token via RPC");

        assert_eq!(ctx.owner_id, format!("{host}::actor-a"));

        server.abort();
    }
}
