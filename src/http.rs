pub use hyper::Method as HttpMethod;
pub use hyper::{Body, StatusCode, header};

pub type Request = hyper::Request<hyper::Body>;
pub type Response = hyper::Response<hyper::Body>;

mod config;
mod host;
mod native;
mod parse;
mod response;
mod server;

pub use config::{
    HttpKernelConfig, build_http_kernel, build_http_kernel_and_registry_with_config_and_builder,
    build_http_kernel_with_config, build_http_kernel_with_native_library,
    build_http_kernel_with_native_library_and_config,
};
pub use host::host_handler_with_public_keys;
pub use server::HttpServer;

pub(crate) use parse::{RequestBody, decode_request_body_with_txn};
#[cfg(test)]
pub(crate) use server::KernelService;

include!("http/tests.rs");
