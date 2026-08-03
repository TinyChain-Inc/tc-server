pub use hyper::Method as HttpMethod;
pub use hyper::{Body, StatusCode, header};

pub type Request = hyper::Request<hyper::Body>;
pub type Response = hyper::Response<hyper::Body>;

mod codec;
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
    build_http_kernel_with_native_library_and_config_and_builder,
};
pub use host::host_handler_with_public_keys;
pub use server::HttpServer;

#[allow(unused_imports)]
pub(crate) use codec::{
    NativeStateResponse, decode_state_bytes_with_context, state_json_stream, state_response,
};
pub(crate) use parse::state_context_for_request;
#[allow(unused_imports)]
pub(crate) use parse::{BTreeDecodeRoots, NativeStateBody};
pub(crate) use parse::{RequestBody, decode_request_body_with_txn};
pub(crate) use parse::{decode_value_body, decode_value_body_for_key};

#[cfg(feature = "pyo3")]
pub(crate) fn load_btree_decode_roots(
    data_dir: &std::path::Path,
) -> tc_error::TCResult<parse::BTreeDecodeRoots> {
    parse::load_btree_decode_roots(data_dir)
}
#[cfg(test)]
pub(crate) use server::KernelService;

include!("http/tests.rs");
