mod decode;
#[cfg(feature = "http-server")]
mod http;
mod library;
mod manifest;

pub use library::WasmLibrary;

#[cfg(feature = "pyo3")]
pub(crate) async fn execute_artifact(
    bytes: &[u8],
    request: crate::KernelRequest,
) -> tc_error::TCResult<crate::State> {
    use futures::TryStreamExt;
    use tc_ir::{IntoView, parse_route_path};

    if request.method != crate::Method::Get {
        return Err(tc_error::TCError::method_not_allowed(
            request.method.as_str(),
            request.path,
        ));
    }

    let engine = wasmtime::Engine::default();
    let mut wasm = WasmLibrary::from_bytes(&engine, bytes)?;
    let path = request.path.to_string();
    let schema_id = wasm.schema().id().to_string();
    let relative = path
        .strip_prefix(&schema_id)
        .ok_or_else(|| tc_error::TCError::not_found(path.clone()))?;
    let route = parse_route_path(relative)?;
    let body = match request.body {
        Some(state) => {
            let view = state.into_view(request.txn.clone()).await?;
            destream_json::encode(view)
                .map_err(tc_error::TCError::internal)?
                .try_fold(Vec::new(), |mut bytes, chunk| async move {
                    bytes.extend_from_slice(&chunk);
                    Ok(bytes)
                })
                .await
                .map_err(tc_error::TCError::internal)?
        }
        None => Vec::new(),
    };
    let bytes = wasm.call_route(&route, &request.txn.header(), &body)?;

    if let Some(reference) = decode::try_decode_wasm_ref(&bytes).await {
        return crate::resolve::resolve(reference, &request.txn).await;
    }

    let stream = futures::stream::iter([Ok::<_, std::io::Error>(bytes::Bytes::from(bytes))]);
    destream_json::try_decode(request.txn, stream)
        .await
        .map_err(tc_error::TCError::bad_request)
}

#[cfg(feature = "http-server")]
pub use http::http_wasm_route_handler_from_bytes;
