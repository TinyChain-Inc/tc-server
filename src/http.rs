pub use hyper::Method as HttpMethod;
pub use hyper::{Body, StatusCode, header};

pub type Request = hyper::Request<hyper::Body>;
pub type Response = hyper::Response<hyper::Body>;

/// HTTP-only endpoint contract. Native kernel execution never depends on this trait.
pub trait HttpHandler: Send + Sync + 'static {
    fn call(&self, request: Request) -> futures::future::BoxFuture<'static, Response>;
}

impl<F, Fut> HttpHandler for F
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: futures::Future<Output = Response> + Send + 'static,
{
    fn call(&self, request: Request) -> futures::future::BoxFuture<'static, Response> {
        Box::pin((self)(request))
    }
}

mod codec;
mod config;
mod host;
mod parse;
mod response;
mod server;

pub use config::{HttpKernelConfig, HttpRuntime, build_http_runtime_with_config};
pub use host::host_handler_with_public_keys;
pub use server::{HttpRouter, HttpServer};

pub(crate) use codec::{decode_state_bytes_with_context, native_state_response, state_response};
pub(crate) use response::tc_error_response;
