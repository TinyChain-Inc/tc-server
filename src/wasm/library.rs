use async_hash::{Digest as _, Sha256};
use bytes::Bytes;
use futures::stream;
use pathlink::PathSegment;
use tc_error::{TCError, TCResult};
use tc_ir::{IntoView, Scalar};
use wasmtime::{Engine, Func, Instance, Memory, Module, Store};

use super::manifest::{RouteBinding, decode_entry, format_path};

/// Loads a TinyChain-compatible Library embedded in a WASM module.
#[derive(Clone)]
pub struct WasmLibrary {
    identity: pathlink::Link,
    definition: Scalar,
    module_hash: [u8; 32],
    bindings: std::sync::Arc<[RouteBinding]>,
    instance: std::sync::Arc<tokio::sync::Mutex<WasmInstance>>,
}

struct WasmInstance {
    store: Store<()>,
    memory: Memory,
    alloc: Func,
    free: Func,
    routes: Vec<(RouteBinding, Func)>,
}

pub(crate) struct WasmRoute {
    library: WasmLibrary,
    path: Vec<PathSegment>,
}

impl WasmRoute {
    pub(crate) fn new(library: WasmLibrary, path: Vec<PathSegment>) -> Self {
        Self { library, path }
    }
}

impl<'a> tc_ir::Handler<'a, crate::State> for WasmRoute {
    fn get<'txn>(self: Box<Self>) -> Option<tc_ir::GetHandler<'a, 'txn, crate::State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, key| {
            let library = self.library;
            let path = self.path;
            Box::pin(async move {
                invoke(
                    library,
                    &path,
                    txn,
                    crate::Method::Get,
                    Some(crate::State::from_scalar(key)),
                )
                .await
            })
        }))
    }
}

impl WasmLibrary {
    pub async fn from_bytes(engine: &Engine, bytes: &[u8]) -> TCResult<Self> {
        let module = Module::new(engine, bytes).map_err(map_wasm_error)?;
        let mut store = Store::new(engine, ());
        let instance = Instance::new(&mut store, &module, &[]).map_err(map_wasm_error)?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| TCError::internal("WASM module must export memory"))?;
        let alloc = instance
            .get_func(&mut store, "alloc")
            .ok_or_else(|| TCError::internal("WASM module must export alloc"))?;
        let free = instance
            .get_func(&mut store, "free")
            .ok_or_else(|| TCError::internal("WASM module must export free"))?;

        let (identity, definition, bindings) =
            WasmInstance::load_entry(&mut store, &instance, &memory).await?;

        let mut routes = Vec::with_capacity(bindings.len());
        for binding in bindings {
            let func = instance
                .get_func(&mut store, &binding.export)
                .ok_or_else(|| {
                    TCError::internal(format!(
                        "missing export {} for route {}",
                        binding.export,
                        format_path(&binding.path)
                    ))
                })?;
            routes.push((binding, func));
        }

        Ok(Self {
            identity,
            definition,
            module_hash: Sha256::digest(bytes).into(),
            bindings: routes.iter().map(|(binding, _)| binding.clone()).collect(),
            instance: std::sync::Arc::new(tokio::sync::Mutex::new(WasmInstance {
                store,
                memory,
                alloc,
                free,
                routes,
            })),
        })
    }

    pub fn identity(&self) -> &pathlink::Link {
        &self.identity
    }

    pub fn definition(&self) -> &Scalar {
        &self.definition
    }

    pub(crate) fn module_hash(&self) -> [u8; 32] {
        self.module_hash
    }

    pub fn routes(&self) -> impl Iterator<Item = &Vec<PathSegment>> {
        self.bindings.iter().map(|binding| &binding.path)
    }

    pub(crate) fn bindings(&self) -> impl ExactSizeIterator<Item = &RouteBinding> {
        self.bindings.iter()
    }

    pub async fn call_route(
        &self,
        path: &[PathSegment],
        txn_id: &[u8],
        body: &[u8],
    ) -> TCResult<Vec<u8>> {
        let mut instance = self.instance.lock().await;
        instance.call_route(path, txn_id, body)
    }
}

impl WasmInstance {
    fn call_route(
        &mut self,
        path: &[PathSegment],
        txn_id: &[u8],
        body: &[u8],
    ) -> TCResult<Vec<u8>> {
        let func = self
            .routes
            .iter()
            .find(|(binding, _)| binding.path == path)
            .map(|(_, func)| *func)
            .ok_or_else(|| TCError::not_found(format_path(path)))?;

        let (txn_ptr, txn_len) = self.write_buffer(txn_id)?;
        let (body_ptr, body_len) = if body.is_empty() {
            (0, 0)
        } else {
            self.write_buffer(body)?
        };

        let typed = func
            .typed::<(i32, i32, i32, i32), i64>(&self.store)
            .map_err(map_wasm_error)?;
        let packed = typed
            .call(&mut self.store, (txn_ptr, txn_len, body_ptr, body_len))
            .map_err(map_wasm_error)?;
        let (result_ptr, result_len) = unpack_wasm_pair(packed);

        let bytes = self.read_buffer(result_ptr, result_len)?;
        if txn_len > 0 {
            self.free_buffer(txn_ptr, txn_len)?;
        }
        if body_len > 0 {
            self.free_buffer(body_ptr, body_len)?;
        }
        self.free_buffer(result_ptr, result_len)?;

        Ok(bytes)
    }

    async fn load_entry(
        store: &mut Store<()>,
        instance: &Instance,
        memory: &Memory,
    ) -> TCResult<(pathlink::Link, Scalar, Vec<RouteBinding>)> {
        let func = instance
            .get_func(&mut *store, "tc_library_entry")
            .ok_or_else(|| TCError::internal("missing tc_library_entry export"))?;
        let typed = func.typed::<(), i64>(&mut *store).map_err(map_wasm_error)?;
        let packed = typed.call(&mut *store, ()).map_err(map_wasm_error)?;
        let (ptr, len) = unpack_wasm_pair(packed);
        let bytes = read_memory(store, memory, ptr, len)?;
        let entry = decode_entry(bytes).await?;
        Ok((entry.identity, entry.definition, entry.routes))
    }

    fn write_buffer(&mut self, data: &[u8]) -> TCResult<(i32, i32)> {
        if data.is_empty() {
            return Ok((0, 0));
        }

        let len = i32::try_from(data.len())
            .map_err(|_| TCError::bad_request("payload too large for wasm memory"))?;
        let alloc = self
            .alloc
            .typed::<i32, i32>(&self.store)
            .map_err(map_wasm_error)?;
        let ptr = alloc.call(&mut self.store, len).map_err(map_wasm_error)?;
        self.memory
            .write(&mut self.store, ptr as usize, data)
            .map_err(map_wasm_error)?;
        Ok((ptr, len))
    }

    fn read_buffer(&mut self, ptr: i32, len: i32) -> TCResult<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        read_memory(&mut self.store, &self.memory, ptr, len)
    }

    fn free_buffer(&mut self, ptr: i32, len: i32) -> TCResult<()> {
        if len == 0 {
            return Ok(());
        }
        let free = self
            .free
            .typed::<(i32, i32), ()>(&self.store)
            .map_err(map_wasm_error)?;
        free.call(&mut self.store, (ptr, len))
            .map_err(map_wasm_error)?;
        Ok(())
    }
}

/// Cross the WASM ABI once while keeping routing and transaction ownership native.
pub(crate) async fn invoke(
    wasm: WasmLibrary,
    path: &[PathSegment],
    txn: &crate::TxnHandle,
    method: crate::Method,
    body: Option<crate::State>,
) -> TCResult<crate::State> {
    if method != crate::Method::Get {
        return Err(TCError::method_not_allowed(method, "WASM Library route"));
    }
    let body = match body {
        Some(body) => {
            let view = body.into_view(txn.clone()).await?;
            crate::literal::encode_json(view, txn.resources().limits().ingress.request_body_bytes)
                .await?
        }
        None => Vec::new(),
    };
    let txn_id = txn.id().to_string();
    let bytes = wasm.call_route(path, txn_id.as_bytes(), &body).await?;
    let input = stream::iter([Ok::<_, std::io::Error>(Bytes::from(bytes))]);
    destream_json::try_decode(txn.clone(), input)
        .await
        .map_err(|err| TCError::bad_request(format!("invalid WASM response: {err}")))
}

fn read_memory(store: &mut Store<()>, memory: &Memory, ptr: i32, len: i32) -> TCResult<Vec<u8>> {
    let mut buf = vec![0u8; len as usize];
    memory
        .read(store, ptr as usize, &mut buf)
        .map_err(map_wasm_error)?;
    Ok(buf)
}

fn map_wasm_error<E: std::fmt::Display>(err: E) -> TCError {
    TCError::internal(format!("wasm error: {err}"))
}

fn unpack_wasm_pair(value: i64) -> (i32, i32) {
    let value = value as u64;
    let ptr = (value & 0xffff_ffff) as u32 as i32;
    let len = (value >> 32) as u32 as i32;
    (ptr, len)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wat_bytes(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("\\{byte:02x}")).collect()
    }

    #[tokio::test]
    async fn loads_literal_definition_and_executes_its_bound_export() {
        let entry = br#"{"definition":{"/lib/example-devco/wasm/1.0.0":{"answer":42}},"routes":[{"path":"/answer","export":"answer"}]}"#;
        let response = b"42";
        let entry_result = (entry.len() as i64) << 32;
        let response_ptr = 2048_i64;
        let response_result = ((response.len() as i64) << 32) | response_ptr;
        let wat = format!(
            r#"(module
                (memory (export "memory") 1)
                (data (i32.const 0) "{}")
                (data (i32.const {response_ptr}) "{}")
                (func (export "alloc") (param i32) (result i32) i32.const 4096)
                (func (export "free") (param i32 i32))
                (func (export "tc_library_entry") (result i64) i64.const {entry_result})
                (func (export "answer") (param i32 i32 i32 i32) (result i64)
                    i64.const {response_result}))"#,
            wat_bytes(entry),
            wat_bytes(response),
        );

        let library = WasmLibrary::from_bytes(&Engine::default(), wat.as_bytes())
            .await
            .expect("load literal WASM Library");
        assert_eq!(
            library.identity().to_string(),
            "/lib/example-devco/wasm/1.0.0"
        );
        let txn = crate::txn::test_txn("wasm-literal").await;
        let txn_id = txn.id().to_string();
        let output = library
            .call_route(&["answer".parse().unwrap()], txn_id.as_bytes(), &[])
            .await
            .expect("execute bound export");
        assert_eq!(output, response);
    }
}
