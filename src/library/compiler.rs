use std::sync::Arc;

use super::LibraryDraft;
#[cfg(feature = "wasm")]
use crate::application::encode_definition;
use crate::cluster::Staging;
use crate::storage::Leaf;
use pathlink::Link;
use tc_error::{TCError, TCResult};
use tc_ir::Scalar;

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
    pub(crate) async fn module<F>(&self, module: Arc<[u8]>, leaf: F) -> TCResult<LibraryDraft>
    where
        F: FnOnce(&Link) -> Leaf,
    {
        let wasm = crate::wasm::WasmLibrary::from_bytes(&self.engine, &module).await?;
        let leaf = leaf(wasm.identity());
        self.finish_wasm(leaf, Staging::default(), module, wasm, None)
            .await
    }

    #[cfg(not(feature = "wasm"))]
    pub(crate) async fn module<F>(&self, _module: Arc<[u8]>, _leaf: F) -> TCResult<LibraryDraft>
    where
        F: FnOnce(&Link) -> Leaf,
    {
        Err(TCError::new(
            tc_error::ErrorKind::NotImplemented,
            "this host does not support WASM Libraries",
        ))
    }

    #[cfg(feature = "wasm")]
    pub(crate) async fn wasm(
        &self,
        leaf: Leaf,
        staging: Staging,
        module: Arc<[u8]>,
        expected: Option<(Link, Scalar, Arc<[u8]>)>,
    ) -> TCResult<LibraryDraft> {
        let wasm = crate::wasm::WasmLibrary::from_bytes(&self.engine, &module).await?;
        self.finish_wasm(leaf, staging, module, wasm, expected)
            .await
    }

    #[cfg(feature = "wasm")]
    async fn finish_wasm(
        &self,
        leaf: Leaf,
        staging: Staging,
        module: Arc<[u8]>,
        wasm: crate::wasm::WasmLibrary,
        expected: Option<(Link, Scalar, Arc<[u8]>)>,
    ) -> TCResult<LibraryDraft> {
        let identity = wasm.identity().clone();
        let definition = wasm.definition().clone();
        let manifest = match expected {
            Some((expected_identity, expected, manifest))
                if expected_identity == identity && expected == definition =>
            {
                manifest
            }
            Some(_) => {
                return Err(TCError::internal(
                    "embedded WASM definition does not match manifest.json",
                ));
            }
            None => encode_definition(&identity, &definition, super::MAX_LIBRARY_BYTES)
                .await?
                .into(),
        };
        let analysis = crate::ir::compile_ir_library(definition)?;
        if wasm
            .bindings()
            .any(|binding| crate::ir::member(&analysis.members, &binding.path).is_none())
        {
            return Err(TCError::bad_request(
                "a WASM export does not correspond to an embedded Library member",
            ));
        }
        let wasm = Arc::new(tokio::sync::Mutex::new(wasm));
        LibraryDraft::new_wasm(leaf, identity, manifest, module, analysis, wasm, staging)
    }

    #[cfg(not(feature = "wasm"))]
    pub(crate) async fn wasm(
        &self,
        _leaf: Leaf,
        _staging: Staging,
        _module: Arc<[u8]>,
        _expected: Option<(Link, Scalar, Arc<[u8]>)>,
    ) -> TCResult<LibraryDraft> {
        Err(TCError::new(
            tc_error::ErrorKind::NotImplemented,
            "this host does not support WASM Libraries",
        ))
    }
}

#[cfg(all(test, not(feature = "wasm")))]
mod tests {
    use std::sync::Arc;

    #[tokio::test]
    async fn wasm_is_a_typed_unsupported_capability() {
        let root = crate::txn::test_applications("featureless-wasm").await;
        let Err(error) = super::Compiler::new()
            .module(Arc::from([]), |identity| {
                root.libraries.state().leaf(identity.clone())
            })
            .await
        else {
            panic!("featureless host accepted WASM")
        };
        assert_eq!(error.code(), tc_error::ErrorKind::NotImplemented);
    }
}
