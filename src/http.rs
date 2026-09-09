pub use crate::{Body, HttpMethod, Request, Response, StatusCode, header};

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

#[cfg(feature = "http-server")]
mod codec;
#[cfg(feature = "http-server")]
mod config;
#[cfg(feature = "http-server")]
mod host;
#[cfg(feature = "http-server")]
mod parse;
#[cfg(feature = "http-server")]
mod response;
#[cfg(feature = "http-server")]
mod server;

#[cfg(feature = "http-server")]
pub use config::{HttpKernelConfig, HttpRuntime, build_http_runtime_with_config};
#[cfg(feature = "http-server")]
pub use server::{HttpRouter, HttpServer};

#[cfg(feature = "http-server")]
pub(crate) use codec::{native_state_response, state_response};
