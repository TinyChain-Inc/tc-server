use std::{env, io::Write, net::TcpListener, str::FromStr};

use base64::Engine as _;
use futures::{FutureExt, TryStreamExt};
use pathlink::Link;
use tc_error::{TCError, TCResult};
use tc_ir::{
    Dir, HandleDelete, HandleGet, HandlePost, HandlePut, LibraryModule, Map, OpDef, OpRef, Scalar,
    Subject, TCRef, parse_route_path,
};
use tc_value::Value;
use tinychain::auth::{Actor, KeyringActorResolver};
use tinychain::http::build_http_kernel_with_native_library_and_config_and_builder;
use tinychain::http::{Body, HttpMethod, Request, Response, StatusCode};
use tinychain::library::NativeLibrary;
use tinychain::txn::TxnHandle;
use tinychain::{HttpKernelConfig, HttpServer};

const B_ROOT: &str = "/lib/example-devco/example/0.1.0";
const B_HELLO: &str = "/example-devco/example/0.1.0/hello";
const B_OPDEF: &str = "/example-devco/example/0.1.0/opdef";
const DEFAULT_ACTOR_ID: &str = "example-admin";
const DEFAULT_SECRET_KEY_B64: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

#[derive(Clone)]
enum HandlerKind {
    Hello,
    OpDef,
}

#[derive(Clone)]
struct ScalarHandler {
    kind: HandlerKind,
}

impl ScalarHandler {
    fn hello_name(request: Value) -> String {
        match request {
            Value::String(s) => {
                serde_json::from_str::<String>(s.as_ref()).unwrap_or_else(|_| s.to_string())
            }
            Value::None => String::new(),
            other => format!("{other:?}"),
        }
    }

    fn opdef_payload_value() -> Value {
        let subject =
            Link::from_str("/class/example-devco/ops/1.0.0").expect("valid op subject link");
        let opref = OpRef::Get((Subject::Link(subject), Scalar::default()));
        let tcref = TCRef::Op(opref);

        let inner = OpDef::Post(vec![("x".parse().expect("Id"), Scalar::from(tcref))]);

        let mut plain_map = Map::new();
        plain_map.insert("a".parse().expect("Id"), Scalar::from(1_u64));
        plain_map.insert(
            "b".parse().expect("Id"),
            Scalar::Tuple(vec![Scalar::from(2_u64), Scalar::from(3_u64)]),
        );

        let mut nested_map = Map::new();
        nested_map.insert(
            "t".parse().expect("Id"),
            Scalar::Tuple(vec![Scalar::from(inner.clone()), Scalar::from(7_u64)]),
        );

        let outer = OpDef::Post(vec![
            ("inner".parse().expect("Id"), Scalar::from(inner)),
            ("plain".parse().expect("Id"), Scalar::Map(plain_map)),
            ("nested".parse().expect("Id"), Scalar::Map(nested_map)),
        ]);

        let stream = destream_json::encode(outer).expect("encode opdef");
        let bytes =
            futures::executor::block_on(stream.try_fold(Vec::new(), |mut acc, chunk| async move {
                acc.extend_from_slice(&chunk);
                Ok(acc)
            }))
            .expect("collect opdef");
        Value::String(String::from_utf8(bytes).expect("opdef json"))
    }
}

impl HandleGet<TxnHandle> for ScalarHandler {
    type Request = Value;
    type RequestContext = ();
    type Response = Value;
    type Error = TCError;
    type Fut<'a> = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send + 'a>,
    >;

    fn get<'a>(&'a self, _txn: &'a TxnHandle, request: Self::Request) -> TCResult<Self::Fut<'a>> {
        Ok(Box::pin(async move {
            match self.kind {
                HandlerKind::Hello => {
                    let name = Self::hello_name(request);
                    Ok(Value::String(format!("Hello, {name}!")))
                }
                HandlerKind::OpDef => Ok(Self::opdef_payload_value()),
            }
        }))
    }
}

fn method_not_allowed(method: HttpMethod, path: &'static str) -> TCError {
    TCError::method_not_allowed(method, path.to_string())
}

impl HandlePut<TxnHandle> for ScalarHandler {
    type Request = Value;
    type RequestContext = ();
    type Response = Value;
    type Error = TCError;
    type Fut<'a> = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send + 'a>,
    >;

    fn put<'a>(&'a self, _txn: &'a TxnHandle, _request: Self::Request) -> TCResult<Self::Fut<'a>> {
        Ok(Box::pin(async move {
            let path = match self.kind {
                HandlerKind::Hello => "/hello",
                HandlerKind::OpDef => "/opdef",
            };
            Err(method_not_allowed(HttpMethod::PUT, path))
        }))
    }
}

impl HandlePost<TxnHandle> for ScalarHandler {
    type Request = Value;
    type RequestContext = ();
    type Response = Value;
    type Error = TCError;
    type Fut<'a> = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send + 'a>,
    >;

    fn post<'a>(&'a self, _txn: &'a TxnHandle, _request: Self::Request) -> TCResult<Self::Fut<'a>> {
        Ok(Box::pin(async move {
            let path = match self.kind {
                HandlerKind::Hello => "/hello",
                HandlerKind::OpDef => "/opdef",
            };
            Err(method_not_allowed(HttpMethod::POST, path))
        }))
    }
}

impl HandleDelete<TxnHandle> for ScalarHandler {
    type Request = Value;
    type RequestContext = ();
    type Response = Value;
    type Error = TCError;
    type Fut<'a> = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send + 'a>,
    >;

    fn delete<'a>(
        &'a self,
        _txn: &'a TxnHandle,
        _request: Self::Request,
    ) -> TCResult<Self::Fut<'a>> {
        Ok(Box::pin(async move {
            let path = match self.kind {
                HandlerKind::Hello => "/hello",
                HandlerKind::OpDef => "/opdef",
            };
            Err(method_not_allowed(HttpMethod::DELETE, path))
        }))
    }
}

fn ok_handler(_req: Request) -> futures::future::BoxFuture<'static, Response> {
    async move {
        http::Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .expect("ok response")
    }
    .boxed()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut actor_id = DEFAULT_ACTOR_ID.to_string();
    let mut secret_key_b64 = DEFAULT_SECRET_KEY_B64.to_string();
    let bind = env::args()
        .skip(1)
        .fold("127.0.0.1:0".to_string(), |current_bind, arg| {
            if let Some(value) = arg.strip_prefix("--bind=") {
                value.to_string()
            } else if let Some(value) = arg.strip_prefix("--actor-id=") {
                actor_id = value.to_string();
                current_bind
            } else if let Some(value) = arg.strip_prefix("--secret-key-b64=") {
                secret_key_b64 = value.to_string();
                current_bind
            } else {
                current_bind
            }
        });

    let bind_addr = std::net::SocketAddr::from_str(&bind)?;
    let listener = TcpListener::bind(bind_addr)?;
    let addr = listener.local_addr()?;
    let host_link = Link::from_str(&format!("http://{addr}"))?;
    let secret_key_bytes = base64::engine::general_purpose::STANDARD.decode(secret_key_b64)?;
    let secret_key_bytes: [u8; 32] = secret_key_bytes
        .try_into()
        .map_err(|_| "invalid --secret-key-b64: expected 32-byte Ed25519 secret key")?;
    let signing_key = rjwt::SigningKey::from_bytes(&secret_key_bytes);
    let actor = Actor::with_public_key(Value::from(actor_id), signing_key.verifying_key());
    let keyring = KeyringActorResolver::default().with_actor(host_link, actor);

    let schema = tc_ir::LibrarySchema::new(Link::from_str(B_ROOT)?, "0.1.0", vec![]);
    let hello_route = parse_route_path(B_HELLO)?;
    let opdef_route = parse_route_path(B_OPDEF)?;
    let routes = Dir::from_routes(vec![
        (
            hello_route,
            ScalarHandler {
                kind: HandlerKind::Hello,
            },
        ),
        (
            opdef_route,
            ScalarHandler {
                kind: HandlerKind::OpDef,
            },
        ),
    ])?;
    let module = LibraryModule::new(schema.clone(), routes);
    let library = NativeLibrary::new(module);

    let kernel = build_http_kernel_with_native_library_and_config_and_builder(
        library,
        HttpKernelConfig::default().with_host_id("tc-http-rpc-native-host"),
        ok_handler,
        ok_handler,
        ok_handler,
        |builder| builder.with_rjwt_keyring_token_verifier(keyring),
    );
    println!("{addr}");
    eprintln!("serving native B at {B_ROOT}");
    std::io::stdout().flush().ok();

    HttpServer::new(kernel).serve_listener(listener).await?;
    Ok(())
}
