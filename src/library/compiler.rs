#[cfg(feature = "wasm")]
use std::sync::Arc;

#[cfg(feature = "wasm")]
use tc_error::TCError;
use tc_error::TCResult;

#[derive(Clone)]
pub(crate) struct Compiler {
    #[cfg(feature = "wasm")]
    resources: crate::HostResources,
    #[cfg(feature = "wasm")]
    engine: Arc<wasmtime::Engine>,
    #[cfg(feature = "wasm")]
    compilations: Arc<tokio::sync::Semaphore>,
    #[cfg(feature = "wasm")]
    limits: crate::wasm::WasmLimits,
}

impl Compiler {
    pub(crate) fn new(resources: &crate::HostResources) -> TCResult<Self> {
        #[cfg(feature = "wasm")]
        {
            let limits = &resources.limits().execution;
            let mut config = wasmtime::Config::new();
            config.consume_fuel(true);
            let engine = wasmtime::Engine::new(&config)
                .map_err(|error| TCError::internal(format!("WASM engine error: {error}")))?;
            return Ok(Self {
                resources: resources.clone(),
                engine: Arc::new(engine),
                compilations: Arc::new(tokio::sync::Semaphore::new(limits.wasm_compilations)),
                limits: crate::wasm::WasmLimits {
                    fuel: limits.wasm_fuel_per_call,
                    memory_bytes: limits.wasm_memory_bytes,
                    result_bytes: limits.wasm_result_bytes,
                },
            });
        }
        #[cfg(not(feature = "wasm"))]
        {
            let _ = resources;
            Ok(Self {})
        }
    }

    #[cfg(feature = "wasm")]
    pub(crate) async fn module(
        &self,
        bytes: Arc<[u8]>,
        deadline: crate::Deadline,
    ) -> TCResult<crate::wasm::WasmLibrary> {
        let permit = deadline
            .wait(Arc::clone(&self.compilations).acquire_owned())
            .await?
            .map_err(|_| TCError::internal("WASM compilation admission closed"))?;
        let engine = Arc::clone(&self.engine);
        let module_bytes = Arc::clone(&bytes);
        let compilation = tokio::task::spawn_blocking(move || {
            let result = wasmtime::Module::new(&engine, module_bytes.as_ref());
            (result, permit)
        });
        let (module, _permit) = deadline
            .wait(compilation)
            .await?
            .map_err(|error| TCError::internal(format!("WASM compiler task failed: {error}")))?;
        let module = module
            .map_err(|error| TCError::bad_request(format!("invalid WASM module: {error}")))?;
        deadline
            .run(crate::wasm::WasmLibrary::from_module(
                Arc::new(module),
                bytes,
                self.limits,
            ))
            .await
    }

    #[cfg(feature = "wasm")]
    pub(crate) fn deadline(&self) -> crate::Deadline {
        self.resources.deadline()
    }
}
