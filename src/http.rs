#[cfg(feature = "http-server")]
pub use hyper::{Body, Method as HttpMethod, StatusCode, header};
#[cfg(feature = "http-server")]
pub type Request = hyper::Request<Body>;
#[cfg(feature = "http-server")]
pub type Response = hyper::Response<Body>;

#[cfg(feature = "http-server")]
mod codec;
#[cfg(feature = "http-server")]
mod host;
#[cfg(feature = "http-server")]
mod parse;
#[cfg(feature = "http-server")]
mod response;
#[cfg(feature = "http-server")]
mod server;

#[cfg(feature = "http-server")]
pub use server::HttpServer;

#[cfg(feature = "http-server")]
pub(crate) use codec::{native_state_response, state_response};
