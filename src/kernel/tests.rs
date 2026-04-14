mod tests {
    use std::collections::BTreeMap;
    use std::str::FromStr;
    use std::sync::{Arc, Mutex};

    use futures::{FutureExt, TryStreamExt, future::BoxFuture};
    use pathlink::Link;
    use tc_ir::{Claim, LibrarySchema};
    use tc_state::State;
    use tc_value::Value;
    use umask::USER_READ;

    use super::*;
    use crate::resolve::Resolve;
    use crate::{Body, Request, Response};

    fn ok_handler() -> impl KernelHandler {
        |_req: Request| async { Response::new(Body::empty()) }.boxed()
    }

    fn state_response(state: State) -> Response {
        let stream = destream_json::encode(state).expect("encode state");
        let body = Body::wrap_stream(stream.map_err(|err| std::io::Error::other(err.to_string())));
        http::Response::builder()
            .status(crate::StatusCode::OK)
            .body(body)
            .expect("state response")
    }

    #[test]
    fn enforces_owner_claim_for_structured_tokens() {
        use std::str::FromStr;

        use tc_ir::Claim;
        use umask::USER_EXEC;

        let kernel = Kernel::builder().with_lib_handler(ok_handler()).finish();
        let begin_token = crate::auth::TokenContext::new("host-a::actor-a", "seed");
        let begin = kernel.route_request(
            Method::Get,
            "/lib",
            Request::new(Body::empty()),
            None,
            true,
            Some(&begin_token),
            |_txn, _req| {},
        );
        assert!(begin.is_ok());

        let pending = kernel.txn_manager().pending_ids();
        assert_eq!(pending.len(), 1);
        let txn_id = pending[0];
        let txn_claim = Claim::new(
            pathlink::Link::from_str(&format!("/txn/{txn_id}")).expect("txn claim link"),
            USER_EXEC,
        );

        let token_a = crate::auth::TokenContext {
            owner_id: "ignored".to_string(),
            bearer_token: "a".to_string(),
            claims: vec![(
                "host-a".to_string(),
                "actor-a".to_string(),
                txn_claim.clone(),
            )],
            verified_at_nanos: 0,
        };

        let token_b = crate::auth::TokenContext {
            owner_id: "ignored".to_string(),
            bearer_token: "b".to_string(),
            claims: vec![("host-b".to_string(), "actor-b".to_string(), txn_claim)],
            verified_at_nanos: 0,
        };

        let _ = kernel
            .route_request(
                Method::Get,
                "/lib",
                Request::new(Body::empty()),
                Some(txn_id),
                true,
                Some(&token_a),
                |_txn, _req| {},
            )
            .expect("begin via claimed token");

        let result = kernel.route_request(
            Method::Get,
            "/lib",
            Request::new(Body::empty()),
            Some(txn_id),
            true,
            Some(&token_b),
            |_txn, _req| {},
        );
        assert!(matches!(result, Err(crate::txn::TxnError::Unauthorized)));
    }

    #[test]
    fn verifies_rjwt_and_pins_owner_by_txn_claim() {
        use std::str::FromStr;

        use crate::auth::TokenVerifier;
        use rjwt::Token;
        use tc_ir::Claim;
        use tc_value::Value;
        use umask::USER_EXEC;

        let kernel = Kernel::builder().with_lib_handler(ok_handler()).finish();

        let host = pathlink::Link::from_str(crate::uri::HOST_ROOT).expect("host link");
        let actor_a = rjwt::Actor::new(Value::from("actor-a"));
        let resolver =
            crate::auth::KeyringActorResolver::default().with_actor(host.clone(), actor_a.clone());
        let verifier = crate::auth::RjwtTokenVerifier::new(Arc::new(resolver));

        let begin_token = crate::auth::TokenContext::new(format!("{host}::actor-a"), "seed");
        let begin = kernel.route_request(
            Method::Get,
            "/lib",
            Request::new(Body::empty()),
            None,
            true,
            Some(&begin_token),
            |_txn, _req| {},
        );
        assert!(begin.is_ok());
        let pending = kernel.txn_manager().pending_ids();
        assert_eq!(pending.len(), 1);
        let txn_id = pending[0];

        let claim = Claim::new(
            pathlink::Link::from_str(&format!("/txn/{txn_id}")).expect("txn claim"),
            USER_EXEC,
        );
        let token = Token::new(
            host.clone(),
            std::time::SystemTime::now(),
            std::time::Duration::from_secs(30),
            actor_a.id().clone(),
            claim,
        );
        let signed = actor_a.sign_token(token).expect("signed token");

        let token_ctx =
            futures::executor::block_on(TokenVerifier::verify(&verifier, signed.into_jwt()))
                .expect("verified token");

        let result = kernel.route_request(
            Method::Get,
            "/lib",
            Request::new(Body::empty()),
            Some(txn_id),
            true,
            Some(&token_ctx),
            |_txn, _req| {},
        );
        assert!(result.is_ok());

        let actor_b = rjwt::Actor::new(Value::from("actor-b"));
        let resolver_b =
            crate::auth::KeyringActorResolver::default().with_actor(host.clone(), actor_b.clone());
        let verifier_b = crate::auth::RjwtTokenVerifier::new(Arc::new(resolver_b));
        let claim_b = Claim::new(
            pathlink::Link::from_str(&format!("/txn/{txn_id}")).expect("txn claim"),
            USER_EXEC,
        );
        let token_b = Token::new(
            host,
            std::time::SystemTime::now(),
            std::time::Duration::from_secs(30),
            actor_b.id().clone(),
            claim_b,
        );
        let signed_b = actor_b.sign_token(token_b).expect("signed token");
        let token_ctx_b =
            futures::executor::block_on(TokenVerifier::verify(&verifier_b, signed_b.into_jwt()))
                .expect("verified token");

        let result = kernel.route_request(
            Method::Get,
            "/lib",
            Request::new(Body::empty()),
            Some(txn_id),
            true,
            Some(&token_ctx_b),
            |_txn, _req| {},
        );
        assert!(matches!(result, Err(crate::txn::TxnError::Unauthorized)));
    }

    #[test]
    fn tracks_active_txns_and_expires_them() {
        let kernel = Kernel::builder()
            .with_txn_ttl(std::time::Duration::from_secs(10))
            .with_lib_handler(ok_handler())
            .finish();

        let dispatch = kernel
            .route_request(
                Method::Get,
                "/lib",
                Request::new(Body::empty()),
                None,
                true,
                None,
                |_txn, _req| {},
            )
            .expect("dispatch");

        if let KernelDispatch::Response(fut) = dispatch {
            futures::executor::block_on(fut);
        } else {
            panic!("expected response dispatch");
        }

        let pending = kernel.txn_manager().pending_ids();
        assert_eq!(pending.len(), 1);
        let txn_id = pending[0];
        assert!(kernel.txn_server().contains(&txn_id));

        let expired = kernel
            .expire_transactions_at(std::time::Instant::now() + std::time::Duration::from_secs(11));
        assert_eq!(expired, vec![txn_id]);
        assert!(kernel.txn_manager().pending_ids().is_empty());
        assert!(!kernel.txn_server().contains(&txn_id));
    }

    #[test]
    fn finalize_requires_txn_claim_in_token() {
        let kernel = Kernel::builder().with_lib_handler(ok_handler()).finish();

        let begin = kernel.route_request(
            Method::Get,
            "/lib",
            Request::new(Body::empty()),
            None,
            true,
            None,
            |_txn, _req| {},
        );
        assert!(begin.is_ok());

        let pending = kernel.txn_manager().pending_ids();
        assert_eq!(pending.len(), 1);
        let txn_id = pending[0];

        let token = crate::auth::TokenContext::new("owner-a", "bearer-a");
        let finalize = kernel.route_request(
            Method::Post,
            "/lib",
            Request::new(Body::empty()),
            Some(txn_id),
            true,
            Some(&token),
            |_txn, _req| {},
        );

        assert!(matches!(finalize, Err(crate::txn::TxnError::Unauthorized)));
    }

    #[test]
    fn continuation_allows_owner_pinned_token_without_txn_claim() {
        let kernel = Kernel::builder().with_lib_handler(ok_handler()).finish();

        let owner_token = crate::auth::TokenContext::new("owner-a", "bearer-a");
        let begin = kernel.route_request(
            Method::Get,
            "/lib",
            Request::new(Body::empty()),
            None,
            true,
            Some(&owner_token),
            |_txn, _req| {},
        );
        assert!(begin.is_ok());

        let pending = kernel.txn_manager().pending_ids();
        assert_eq!(pending.len(), 1);
        let txn_id = pending[0];

        let continue_without_claim = kernel.route_request(
            Method::Get,
            "/lib",
            Request::new(Body::empty()),
            Some(txn_id),
            true,
            Some(&owner_token),
            |_txn, _req| {},
        );

        assert!(continue_without_claim.is_ok());
    }

    #[derive(Clone, Default)]
    struct MockGateway {
        calls: Arc<Mutex<Vec<pathlink::Link>>>,
    }

    impl crate::gateway::RpcGateway for MockGateway {
        fn get(
            &self,
            uri: pathlink::Link,
            _txn: crate::txn::TxnHandle,
            _key: tc_value::Value,
        ) -> BoxFuture<'static, tc_error::TCResult<tc_state::State>> {
            self.calls.lock().expect("calls lock").push(uri);
            Box::pin(async move { Ok(tc_state::State::None) })
        }

        fn put(
            &self,
            _uri: pathlink::Link,
            _txn: crate::txn::TxnHandle,
            _key: tc_value::Value,
            _value: tc_state::State,
        ) -> BoxFuture<'static, tc_error::TCResult<()>> {
            Box::pin(async move { Ok(()) })
        }

        fn post(
            &self,
            _uri: pathlink::Link,
            _txn: crate::txn::TxnHandle,
            _params: tc_ir::Map<tc_state::State>,
        ) -> BoxFuture<'static, tc_error::TCResult<tc_state::State>> {
            Box::pin(async move { Ok(tc_state::State::None) })
        }

        fn delete(
            &self,
            _uri: pathlink::Link,
            _txn: crate::txn::TxnHandle,
            _key: tc_value::Value,
        ) -> BoxFuture<'static, tc_error::TCResult<()>> {
            Box::pin(async move { Ok(()) })
        }
    }

    #[test]
    fn local_path_resolves_locally_without_dependency_routes() {
        let schema = LibrarySchema::new(
            "/lib/acme/a/1.0.0".parse().expect("schema id"),
            "1.0.0",
            vec![],
        );

        let registry = crate::library::LibraryRegistry::new(None, BTreeMap::new());
        futures::executor::block_on(registry.insert_schema(schema)).expect("insert schema");
        let module = Arc::new(registry);
        let handlers = crate::library::LibraryHandlers::with_route(
            ok_handler(),
            ok_handler(),
            |_req: Request| async move { state_response(State::from(Value::from("local"))) }.boxed(),
        );

        let kernel = Kernel::builder()
            .with_library_module(module, handlers)
            .finish();

        let op = tc_ir::OpRef::Get((
            tc_ir::Subject::Link(
                Link::from_str("/lib/acme/a/1.0.0/echo").expect("local dependency link"),
            ),
            tc_ir::Scalar::default(),
        ));

        let txn = kernel.with_resolver(kernel.txn_manager().begin());
        let state = futures::executor::block_on(op.resolve(&txn)).expect("local resolve");
        assert!(matches!(
            state,
            State::Scalar(tc_ir::Scalar::Value(Value::String(ref s))) if s == "local"
        ));
    }

    #[test]
    fn unresolved_path_fails_without_route_index_entry() {
        let schema = LibrarySchema::new(
            "/lib/acme/a/1.0.0".parse().expect("schema id"),
            "1.0.0",
            vec![],
        );

        let registry = crate::library::LibraryRegistry::new(None, BTreeMap::new());
        futures::executor::block_on(registry.insert_schema(schema)).expect("insert schema");
        let module = Arc::new(registry);
        let handlers = crate::library::LibraryHandlers::without_route(ok_handler(), ok_handler());

        let kernel = Kernel::builder()
            .with_library_module(module, handlers)
            .finish();

        let op = tc_ir::OpRef::Get((
            tc_ir::Subject::Link(
                Link::from_str("/lib/acme/b/1.0.0/echo").expect("unresolved dependency link"),
            ),
            tc_ir::Scalar::default(),
        ));

        let txn = kernel.with_resolver(kernel.txn_manager().begin());
        let err = futures::executor::block_on(op.resolve(&txn)).expect_err("expected failure");
        assert_eq!(err.code(), tc_error::ErrorKind::Unauthorized);
    }

    #[test]
    fn dependency_path_routes_via_installed_index() {
        let schema_a = LibrarySchema::new(
            "/lib/acme/a/1.0.0".parse().expect("schema a"),
            "1.0.0",
            vec!["/lib/acme/b/1.0.0".parse().expect("dependency b")],
        );
        let schema_b = LibrarySchema::new(
            "/lib/acme/b/1.0.0".parse().expect("schema b"),
            "1.0.0",
            vec![],
        );

        let registry = crate::library::LibraryRegistry::new(None, BTreeMap::new());
        futures::executor::block_on(registry.insert_schema(schema_a.clone())).expect("insert a");
        futures::executor::block_on(registry.insert_schema(schema_b)).expect("insert b");
        let module = Arc::new(registry);
        let handlers = crate::library::LibraryHandlers::with_route(
            ok_handler(),
            ok_handler(),
            |req: Request| {
                let path = req.uri().path().to_string();
                async move {
                    if path.starts_with("/lib/acme/b/1.0.0/") {
                        state_response(State::from(Value::from("dependency-b")))
                    } else {
                        http::Response::builder()
                            .status(crate::StatusCode::NOT_FOUND)
                            .body(Body::empty())
                            .expect("not found")
                    }
                }
                .boxed()
            },
        );

        let kernel = Kernel::builder()
            .with_library_module(module, handlers)
            .finish();

        let claim = Claim::new(schema_a.id().clone(), USER_READ);
        let txn = kernel.with_resolver(kernel.txn_manager().begin().with_claims(vec![claim]));
        let op = tc_ir::OpRef::Get((
            tc_ir::Subject::Link(
                Link::from_str("/lib/acme/b/1.0.0/echo").expect("dependency link"),
            ),
            tc_ir::Scalar::default(),
        ));

        let state = futures::executor::block_on(op.resolve(&txn)).expect("dependency resolve");
        assert!(matches!(
            state,
            State::Scalar(tc_ir::Scalar::Value(Value::String(ref s))) if s == "dependency-b"
        ));
    }

    #[test]
    fn multi_library_hosted_case_needs_no_constructor_dependency_wiring() {
        let schema_a = LibrarySchema::new(
            "/lib/acme/a/1.0.0".parse().expect("schema a"),
            "1.0.0",
            vec![
                "/lib/acme/b/1.0.0".parse().expect("dependency b"),
                "/lib/acme/c/1.0.0".parse().expect("dependency c"),
            ],
        );
        let schema_b = LibrarySchema::new(
            "/lib/acme/b/1.0.0".parse().expect("schema b"),
            "1.0.0",
            vec![],
        );
        let schema_c = LibrarySchema::new(
            "/lib/acme/c/1.0.0".parse().expect("schema c"),
            "1.0.0",
            vec![],
        );

        let registry = crate::library::LibraryRegistry::new(None, BTreeMap::new());
        futures::executor::block_on(registry.insert_schema(schema_a.clone())).expect("insert a");
        futures::executor::block_on(registry.insert_schema(schema_b)).expect("insert b");
        futures::executor::block_on(registry.insert_schema(schema_c)).expect("insert c");
        let module = Arc::new(registry);
        let handlers = crate::library::LibraryHandlers::with_route(
            ok_handler(),
            ok_handler(),
            |req: Request| {
                let path = req.uri().path().to_string();
                async move {
                    if path.starts_with("/lib/acme/b/1.0.0/") {
                        return state_response(State::from(Value::from("b")));
                    }

                    if path.starts_with("/lib/acme/c/1.0.0/") {
                        return state_response(State::from(Value::from("c")));
                    }

                    http::Response::builder()
                        .status(crate::StatusCode::NOT_FOUND)
                        .body(Body::empty())
                        .expect("not found")
                }
                .boxed()
            },
        );

        let kernel = Kernel::builder()
            .with_library_module(module, handlers)
            .finish();

        let claim = Claim::new(schema_a.id().clone(), USER_READ);
        let txn = kernel.with_resolver(kernel.txn_manager().begin().with_claims(vec![claim]));

        let op_b = tc_ir::OpRef::Get((
            tc_ir::Subject::Link(Link::from_str("/lib/acme/b/1.0.0/echo").expect("b link")),
            tc_ir::Scalar::default(),
        ));
        let op_c = tc_ir::OpRef::Get((
            tc_ir::Subject::Link(Link::from_str("/lib/acme/c/1.0.0/echo").expect("c link")),
            tc_ir::Scalar::default(),
        ));

        let state_b = futures::executor::block_on(op_b.resolve(&txn)).expect("resolve b");
        let state_c = futures::executor::block_on(op_c.resolve(&txn)).expect("resolve c");

        assert!(matches!(
            state_b,
            State::Scalar(tc_ir::Scalar::Value(Value::String(ref s))) if s == "b"
        ));
        assert!(matches!(
            state_c,
            State::Scalar(tc_ir::Scalar::Value(Value::String(ref s))) if s == "c"
        ));
    }

    #[test]
    fn egress_is_default_deny_without_manifest_dependency() {
        let schema = LibrarySchema::new(
            "/lib/acme/a/1.0.0".parse().expect("schema id"),
            "1.0.0",
            vec![],
        );

        let registry =
            crate::library::LibraryRegistry::new(None, std::collections::BTreeMap::new());
        futures::executor::block_on(registry.insert_schema(schema)).expect("insert schema");
        let module = Arc::new(registry);
        let handlers = crate::library::LibraryHandlers::without_route(ok_handler(), ok_handler());

        let gateway = MockGateway::default();

        let kernel = Kernel::builder()
            .with_library_module(module, handlers)
            .with_dependency_route("/lib", "127.0.0.1:1234".parse().expect("addr"))
            .with_rpc_gateway(gateway)
            .finish();

        let op = tc_ir::OpRef::Get((
            tc_ir::Subject::Link("http://127.0.0.1:1234/lib".parse().expect("link")),
            tc_ir::Scalar::default(),
        ));

        let txn = kernel.with_resolver(kernel.txn_manager().begin());
        let err = futures::executor::block_on(op.resolve(&txn)).expect_err("expected unauthorized");
        assert_eq!(err.code(), tc_error::ErrorKind::Unauthorized);
    }

    #[test]
    fn resolves_scalar_ref_using_txn_context() {
        use tc_ir::{OpRef, Scalar, Subject, TCRef};
        use tc_state::State;
        use tc_value::Value;

        type GatewayCall = (Method, pathlink::Link, Option<Value>);

        #[derive(Clone)]
        struct RecordingGateway {
            calls: Arc<Mutex<Vec<GatewayCall>>>,
            responses: Arc<Mutex<Vec<State>>>,
        }

        impl RecordingGateway {
            fn new(responses: Vec<State>) -> Self {
                Self {
                    calls: Arc::new(Mutex::new(Vec::new())),
                    responses: Arc::new(Mutex::new(responses)),
                }
            }
        }

        impl crate::gateway::RpcGateway for RecordingGateway {
            fn get(
                &self,
                uri: pathlink::Link,
                _txn: crate::txn::TxnHandle,
                key: Value,
            ) -> BoxFuture<'static, tc_error::TCResult<State>> {
                self.calls
                    .lock()
                    .expect("calls lock")
                    .push((Method::Get, uri, Some(key)));

                let response = self.responses.lock().expect("responses lock").remove(0);

                Box::pin(async move { Ok(response) })
            }

            fn put(
                &self,
                _uri: pathlink::Link,
                _txn: crate::txn::TxnHandle,
                _key: Value,
                _value: State,
            ) -> BoxFuture<'static, tc_error::TCResult<()>> {
                Box::pin(async move { Ok(()) })
            }

            fn post(
                &self,
                _uri: pathlink::Link,
                _txn: crate::txn::TxnHandle,
                _params: tc_ir::Map<State>,
            ) -> BoxFuture<'static, tc_error::TCResult<State>> {
                Box::pin(async move { Ok(State::None) })
            }

            fn delete(
                &self,
                _uri: pathlink::Link,
                _txn: crate::txn::TxnHandle,
                _key: Value,
            ) -> BoxFuture<'static, tc_error::TCResult<()>> {
                Box::pin(async move { Ok(()) })
            }
        }

        let schema = LibrarySchema::new(
            "/lib/example-devco/example/1.0.0"
                .parse()
                .expect("schema id"),
            "1.0.0",
            vec![
                "/lib/example-devco/example/1.0.0"
                    .parse()
                    .expect("dependency root"),
            ],
        );

        let registry =
            crate::library::LibraryRegistry::new(None, std::collections::BTreeMap::new());
        futures::executor::block_on(registry.insert_schema(schema)).expect("insert schema");
        let module = Arc::new(registry);
        let handlers = crate::library::LibraryHandlers::without_route(ok_handler(), ok_handler());

        let gateway = RecordingGateway::new(vec![
            State::from(Value::from("key")),
            State::from(Value::from("ok")),
        ]);

        let kernel = Kernel::builder()
            .with_library_module(module, handlers)
            .with_dependency_route(
                "/lib/example-devco/example/1.0.0",
                "127.0.0.1:1234".parse().expect("addr"),
            )
            .with_rpc_gateway(gateway.clone())
            .finish();

        let txn = kernel.with_resolver(kernel.txn_manager().begin());

        let inner_op = OpRef::Get((
            Subject::Link(
                "http://127.0.0.1:1234/lib/example-devco/example/1.0.0/key"
                    .parse()
                    .expect("inner link"),
            ),
            Scalar::default(),
        ));

        let outer_op = OpRef::Get((
            Subject::Link(
                "http://127.0.0.1:1234/lib/example-devco/example/1.0.0/value"
                    .parse()
                    .expect("outer link"),
            ),
            Scalar::Ref(Box::new(TCRef::Op(inner_op))),
        ));

        let err = futures::executor::block_on(outer_op.resolve(&txn)).expect_err("expected unauthorized");
        assert!(err.message().contains("transaction-chained bearer auth"));

        let calls = gateway.calls.lock().expect("calls lock");
        assert!(calls.is_empty());
    }
}
