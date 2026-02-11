#[cfg(any(feature = "http-server", feature = "pyo3"))]
pub(super) async fn try_decode_wasm_ref(bytes: &[u8]) -> Option<tc_ir::TCRef> {
    if let Ok(r) = try_decode_json_slice::<tc_ir::TCRef>(bytes).await {
        return Some(r);
    }

    if let Ok(op) = try_decode_json_slice::<tc_ir::OpRef>(bytes).await {
        return Some(tc_ir::TCRef::Op(op));
    }

    None
}

#[cfg(any(feature = "http-server", feature = "pyo3"))]
async fn try_decode_json_slice<T>(bytes: &[u8]) -> Result<T, String>
where
    T: destream::de::FromStream<Context = ()>,
{
    use std::io;

    if bytes.is_empty() {
        return Err("empty payload".to_string());
    }

    let stream = futures::stream::iter(vec![Ok::<bytes::Bytes, io::Error>(
        bytes::Bytes::copy_from_slice(bytes),
    )]);
    destream_json::try_decode((), stream)
        .await
        .map_err(|err| err.to_string())
}
