use std::{env, io::Write, net::TcpListener, str::FromStr};

use futures::FutureExt;
use hyper::{Body, Request, Response, StatusCode};
use pathlink::Link;
use tc_error::{TCError, TCResult};
use tc_ir::{Dir, HandleDelete, HandleGet, HandlePost, HandlePut, LibraryModule, parse_route_path};
use tc_value::Value;
use tinychain::{HttpKernelConfig, HttpServer};
use tinychain::library::NativeLibrary;
use tinychain::http::build_http_kernel_with_native_library_and_config;
use tinychain::txn::TxnHandle;

const B_ROOT: &str = "/lib/example-devco/example/0.1.0";
const B_HELLO: &str = "/example-devco/example/0.1.0/hello";

#[derive(Clone)]
struct HelloHandler;

impl HandleGet<TxnHandle> for HelloHandler {
    type Request = Value;
    type RequestContext = ();
    type Response = Value;
    type Error = TCError;
    type Fut<'a> = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send + 'a>,
    >;

    fn get<'a>(&'a self, _txn: &'a TxnHandle, request: Self::Request) -> TCResult<Self::Fut<'a>> {
        Ok(Box::pin(async move {
            let name = match request {
                Value::String(s) => s.to_string(),
                Value::None => String::new(),
                other => format!("{other:?}"),
            };

            Ok(Value::String(format!("Hello, {name}!").into()))
        }))
    }
}

fn method_not_allowed(method: hyper::Method, path: &'static str) -> TCError {
    TCError::method_not_allowed(method, path.to_string())
}

impl HandlePut<TxnHandle> for HelloHandler {
    type Request = Value;
    type RequestContext = ();
    type Response = Value;
    type Error = TCError;
    type Fut<'a> = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send + 'a>,
    >;

    fn put<'a>(&'a self, _txn: &'a TxnHandle, _request: Self::Request) -> TCResult<Self::Fut<'a>> {
        Ok(Box::pin(async move {
            Err(method_not_allowed(hyper::Method::PUT, "/hello"))
        }))
    }
}

impl HandlePost<TxnHandle> for HelloHandler {
    type Request = Value;
    type RequestContext = ();
    type Response = Value;
    type Error = TCError;
    type Fut<'a> = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send + 'a>,
    >;

    fn post<'a>(&'a self, _txn: &'a TxnHandle, _request: Self::Request) -> TCResult<Self::Fut<'a>> {
        Ok(Box::pin(async move {
            Err(method_not_allowed(hyper::Method::POST, "/hello"))
        }))
    }
}

impl HandleDelete<TxnHandle> for HelloHandler {
    type Request = Value;
    type RequestContext = ();
    type Response = Value;
    type Error = TCError;
    type Fut<'a> = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send + 'a>,
    >;

    fn delete<'a>(&'a self, _txn: &'a TxnHandle, _request: Self::Request) -> TCResult<Self::Fut<'a>> {
        Ok(Box::pin(async move {
            Err(method_not_allowed(hyper::Method::DELETE, "/hello"))
        }))
    }
}

fn ok_handler(_req: Request<Body>) -> futures::future::BoxFuture<'static, Response<Body>> {
    async move {
        Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .expect("ok response")
    }
    .boxed()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind = env::args()
        .skip(1)
        .find_map(|arg| arg.strip_prefix("--bind=").map(str::to_string))
        .unwrap_or_else(|| "127.0.0.1:0".to_string());

    let bind_addr = std::net::SocketAddr::from_str(&bind)?;

    let schema = tc_ir::LibrarySchema::new(Link::from_str(B_ROOT)?, "0.1.0", vec![]);
    let route = parse_route_path(B_HELLO)?;
    let routes = Dir::from_routes(vec![(route, HelloHandler)])?;
    let module = LibraryModule::new(schema.clone(), routes);
    let library = NativeLibrary::new(module);

    let kernel = build_http_kernel_with_native_library_and_config(
        library,
        HttpKernelConfig::default().with_host_id("tc-http-rpc-native-host"),
        ok_handler,
        ok_handler,
        ok_handler,
    );

    let listener = TcpListener::bind(bind_addr)?;
    let addr = listener.local_addr()?;
    println!("{addr}");
    eprintln!("serving native B at {B_ROOT}");
    std::io::stdout().flush().ok();

    HttpServer::new(kernel).serve_listener(listener).await?;
    Ok(())
}
