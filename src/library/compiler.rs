#[cfg(feature = "wasm")]
use std::sync::Arc;

#[cfg(feature = "wasm")]
use tc_error::TCResult;

#[derive(Clone)]
pub(crate) struct Compiler {
    #[cfg(feature = "wasm")]
    engine: Arc<wasmtime::Engine>,
}

impl Compiler {
    pub(crate) fn new() -> Self {
        Self {
            #[cfg(feature = "wasm")]
            engine: Arc::new(wasmtime::Engine::default()),
        }
    }

    #[cfg(feature = "wasm")]
    pub(crate) async fn module(&self, module: &[u8]) -> TCResult<crate::wasm::WasmLibrary> {
        crate::wasm::WasmLibrary::from_bytes(&self.engine, module).await
    }
}
