mod decode;
#[cfg(feature = "http-server")]
mod http;
mod library;
mod manifest;

#[cfg(test)]
mod tests;

pub use library::WasmLibrary;

#[cfg(feature = "http-server")]
pub use http::http_wasm_route_handler_from_bytes;
