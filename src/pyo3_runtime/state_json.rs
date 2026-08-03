use futures::{TryStreamExt, future::BoxFuture};
use tc_state::State;

/// Materialize the canonical HTTP stream only at the PyO3 byte-handle boundary.
pub(super) fn encode_state_json_bytes(state: State) -> BoxFuture<'static, Result<Vec<u8>, String>> {
    Box::pin(async move {
        crate::http::state_json_stream(state)
            .map_err(|err| err.to_string())
            .try_fold(Vec::new(), |mut bytes, chunk| async move {
                bytes.extend_from_slice(&chunk);
                Ok(bytes)
            })
            .await
    })
}
