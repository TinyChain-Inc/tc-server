use async_hash::{Digest as _, Sha256};
use bytes::Bytes;
use futures::{TryStreamExt, stream};
use pathlink::PathSegment;
use tc_error::{TCError, TCResult};
use tc_ir::{IntoView, Scalar};
use wasmtime::{Func, Instance, Memory, Module, Store, StoreLimits, StoreLimitsBuilder};

use super::manifest::{RouteBinding, decode_entry, format_path};

/// Loads a TinyChain-compatible Library embedded in a WASM module.
#[derive(Clone)]
pub(crate) struct WasmLibrary {
    identity: pathlink::Link,
    definition: Scalar,
    module_hash: [u8; 32],
    bindings: std::sync::Arc<[RouteBinding]>,
    // Wasmtime requires exclusive mutable access to a Store. The Option lets
    // an errored guest instance be discarded before rebuilding it.
    instance: std::sync::Arc<tokio::sync::Mutex<Option<WasmInstance>>>,
    module: std::sync::Arc<Module>,
    limits: WasmLimits,
}

#[derive(Clone, Copy)]
pub(crate) struct WasmLimits {
    pub(crate) fuel: u64,
    pub(crate) memory_bytes: usize,
    pub(crate) result_bytes: usize,
}

struct WasmStore {
    limits: StoreLimits,
}

struct WasmInstance {
    store: Store<WasmStore>,
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
    pub(crate) async fn from_module(
        module: std::sync::Arc<Module>,
        bytes: std::sync::Arc<[u8]>,
        limits: WasmLimits,
    ) -> TCResult<Self> {
        let bootstrap_module = std::sync::Arc::clone(&module);
        let (mut store, instance, memory, alloc, free, entry) =
            tokio::task::spawn_blocking(move || {
                let (mut store, instance) = new_store_and_instance(&bootstrap_module, limits)?;
                let memory = instance
                    .get_memory(&mut store, "memory")
                    .ok_or_else(|| TCError::internal("WASM module must export memory"))?;
                let alloc = instance
                    .get_func(&mut store, "alloc")
                    .ok_or_else(|| TCError::internal("WASM module must export alloc"))?;
                let free = instance
                    .get_func(&mut store, "free")
                    .ok_or_else(|| TCError::internal("WASM module must export free"))?;
                let entry = WasmInstance::load_entry_bytes(
                    &mut store,
                    &instance,
                    &memory,
                    limits.result_bytes,
                )?;
                Ok::<_, TCError>((store, instance, memory, alloc, free, entry))
            })
            .await
            .map_err(|error| TCError::internal(format!("WASM startup task failed: {error}")))??;
        let entry = decode_entry(entry).await?;
        let (identity, definition, bindings) = (entry.identity, entry.definition, entry.routes);

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
            instance: std::sync::Arc::new(tokio::sync::Mutex::new(Some(WasmInstance {
                store,
                memory,
                alloc,
                free,
                routes,
            }))),
            module,
            limits,
        })
    }

    pub(crate) fn identity(&self) -> &pathlink::Link {
        &self.identity
    }

    pub(crate) fn definition(&self) -> &Scalar {
        &self.definition
    }

    pub(crate) fn module_hash(&self) -> [u8; 32] {
        self.module_hash
    }

    pub(crate) fn bindings(&self) -> impl ExactSizeIterator<Item = &RouteBinding> {
        self.bindings.iter()
    }

    pub(crate) async fn call_route(
        &self,
        path: &[PathSegment],
        txn_id: &[u8],
        body: &[u8],
        deadline: crate::Deadline,
    ) -> TCResult<Vec<u8>> {
        let mut instance = deadline
            .wait(std::sync::Arc::clone(&self.instance).lock_owned())
            .await?;
        let path = path.to_vec();
        let txn_id = txn_id.to_vec();
        let body = body.to_vec();
        let module = std::sync::Arc::clone(&self.module);
        let bindings = std::sync::Arc::clone(&self.bindings);
        let limits = self.limits;
        let task = tokio::task::spawn_blocking(move || {
            let Some(mut active) = instance.take() else {
                return Err(TCError::internal("WASM instance is unavailable"));
            };
            let result = active.call_route(&path, &txn_id, &body, limits);
            if result.is_ok() {
                *instance = Some(active);
            } else {
                *instance = WasmInstance::from_bindings(&module, limits, &bindings).ok();
            }
            result
        });
        deadline
            .wait(task)
            .await?
            .map_err(|error| TCError::internal(format!("WASM execution task failed: {error}")))?
    }
}

impl WasmInstance {
    fn call_route(
        &mut self,
        path: &[PathSegment],
        txn_id: &[u8],
        body: &[u8],
        limits: WasmLimits,
    ) -> TCResult<Vec<u8>> {
        self.store.set_fuel(limits.fuel).map_err(map_wasm_error)?;
        let func = self
            .routes
            .iter()
            .find(|(binding, _)| binding.path == path)
            .map(|(_, func)| *func)
            .ok_or_else(|| TCError::not_found(format_path(path)))?;

        let (txn_ptr, txn_len) = self.write_buffer(txn_id, limits.memory_bytes)?;
        let (body_ptr, body_len) = if body.is_empty() {
            (0, 0)
        } else {
            match self.write_buffer(body, limits.memory_bytes) {
                Ok(buffer) => buffer,
                Err(error) => {
                    let _ = self.free_buffer(txn_ptr, txn_len);
                    return Err(error);
                }
            }
        };

        let typed = match func.typed::<(i32, i32, i32, i32), i64>(&self.store) {
            Ok(typed) => typed,
            Err(error) => {
                let _ = self.free_buffer(txn_ptr, txn_len);
                let _ = self.free_buffer(body_ptr, body_len);
                return Err(map_wasm_error(error));
            }
        };
        let packed = match typed.call(&mut self.store, (txn_ptr, txn_len, body_ptr, body_len)) {
            Ok(packed) => packed,
            Err(error) => {
                let _ = self.free_buffer(txn_ptr, txn_len);
                let _ = self.free_buffer(body_ptr, body_len);
                return Err(map_wasm_error(error));
            }
        };
        let (result_ptr, result_len) = unpack_wasm_pair(packed);

        let bytes = self.read_buffer(result_ptr, result_len, limits.result_bytes);
        let cleanup = [
            self.free_buffer(txn_ptr, txn_len),
            self.free_buffer(body_ptr, body_len),
            if result_ptr >= 0 {
                self.free_buffer(result_ptr, result_len)
            } else {
                Ok(())
            },
        ]
        .into_iter()
        .find_map(Result::err);

        match (bytes, cleanup) {
            (Err(error), _) | (Ok(_), Some(error)) => Err(error),
            (Ok(bytes), None) => Ok(bytes),
        }
    }

    fn load_entry_bytes(
        store: &mut Store<WasmStore>,
        instance: &Instance,
        memory: &Memory,
        result_limit: usize,
    ) -> TCResult<Vec<u8>> {
        let func = instance
            .get_func(&mut *store, "tc_library_entry")
            .ok_or_else(|| TCError::internal("missing tc_library_entry export"))?;
        let typed = func.typed::<(), i64>(&mut *store).map_err(map_wasm_error)?;
        let packed = typed.call(&mut *store, ()).map_err(map_wasm_error)?;
        let (ptr, len) = unpack_wasm_pair(packed);
        let bytes = read_memory(store, memory, ptr, len, result_limit);
        let cleanup = if len > 0 {
            instance
                .get_typed_func::<(i32, i32), ()>(&mut *store, "free")
                .map_err(map_wasm_error)
                .and_then(|free| free.call(&mut *store, (ptr, len)).map_err(map_wasm_error))
        } else {
            Ok(())
        };
        match (bytes, cleanup) {
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
            (Ok(bytes), Ok(())) => Ok(bytes),
        }
    }

    fn write_buffer(&mut self, data: &[u8], memory_limit: usize) -> TCResult<(i32, i32)> {
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
        let range = checked_memory_range(&self.store, &self.memory, ptr, len, memory_limit)?;
        self.memory
            .write(&mut self.store, range.start, data)
            .map_err(|error| TCError::bad_request(format!("WASM memory write failed: {error}")))?;
        Ok((ptr, len))
    }

    fn read_buffer(&mut self, ptr: i32, len: i32, result_limit: usize) -> TCResult<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        read_memory(&mut self.store, &self.memory, ptr, len, result_limit)
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

    fn from_bindings(
        module: &Module,
        limits: WasmLimits,
        bindings: &[RouteBinding],
    ) -> TCResult<Self> {
        let (mut store, instance) = new_store_and_instance(module, limits)?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| TCError::bad_request("WASM module must export memory"))?;
        let alloc = instance
            .get_func(&mut store, "alloc")
            .ok_or_else(|| TCError::bad_request("WASM module must export alloc"))?;
        let free = instance
            .get_func(&mut store, "free")
            .ok_or_else(|| TCError::bad_request("WASM module must export free"))?;
        let routes = bindings
            .iter()
            .map(|binding| {
                let func = instance
                    .get_func(&mut store, &binding.export)
                    .ok_or_else(|| {
                        TCError::bad_request(format!(
                            "missing export {} for route {}",
                            binding.export,
                            format_path(&binding.path)
                        ))
                    })?;
                Ok((binding.clone(), func))
            })
            .collect::<TCResult<Vec<_>>>()?;
        Ok(Self {
            store,
            memory,
            alloc,
            free,
            routes,
        })
    }
}

fn new_store_and_instance(
    module: &Module,
    limits: WasmLimits,
) -> TCResult<(Store<WasmStore>, Instance)> {
    let store_limits = StoreLimitsBuilder::new()
        .memory_size(limits.memory_bytes)
        .table_elements(10_000)
        .instances(1)
        .tables(1)
        .memories(1)
        .trap_on_grow_failure(true)
        .build();
    let mut store = Store::new(
        module.engine(),
        WasmStore {
            limits: store_limits,
        },
    );
    store.limiter(|state| &mut state.limits);
    store.set_fuel(limits.fuel).map_err(map_wasm_error)?;
    let instance = Instance::new(&mut store, module, &[]).map_err(map_wasm_error)?;
    Ok((store, instance))
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
            encode_guest_json(view, txn.request_body_limit()).await?
        }
        None => Vec::new(),
    };
    let txn_id = txn.id().to_string();
    let bytes = wasm
        .call_route(path, txn_id.as_bytes(), &body, txn.deadline())
        .await?;
    let input = stream::iter([Ok::<_, std::io::Error>(Bytes::from(bytes))]);
    destream_json::try_decode(txn.clone(), input)
        .await
        .map_err(|err| TCError::bad_request(format!("invalid WASM response: {err}")))
}

async fn encode_guest_json<T>(value: T, bound: usize) -> TCResult<Vec<u8>>
where
    T: for<'en> destream::en::IntoStream<'en>,
{
    destream_json::encode(value)
        .map_err(|error| TCError::bad_request(error.to_string()))?
        .map_err(|error| TCError::bad_request(error.to_string()))
        .try_fold(Vec::new(), |mut bytes, chunk| async move {
            if bytes.len().saturating_add(chunk.len()) > bound {
                return Err(TCError::payload_too_large(
                    "WASM request exceeds its ABI bound",
                    tc_error::Pressure::new(
                        "/host/resource/wasm/request",
                        tc_error::PressureReason::QuotaExceeded,
                    ),
                ));
            }
            bytes.extend_from_slice(&chunk);
            Ok(bytes)
        })
        .await
}

fn checked_memory_range(
    store: &Store<WasmStore>,
    memory: &Memory,
    ptr: i32,
    len: i32,
    max_len: usize,
) -> TCResult<std::ops::Range<usize>> {
    let ptr = usize::try_from(ptr).map_err(|_| TCError::bad_request("negative WASM pointer"))?;
    let len = usize::try_from(len).map_err(|_| TCError::bad_request("negative WASM length"))?;
    if len > max_len {
        return Err(TCError::bad_request(format!(
            "WASM result exceeds the {max_len}-byte limit"
        )));
    }
    let end = ptr
        .checked_add(len)
        .ok_or_else(|| TCError::bad_request("WASM memory range overflow"))?;
    if end > memory.data_size(store) {
        return Err(TCError::bad_request("WASM memory range is out of bounds"));
    }
    Ok(ptr..end)
}

fn read_memory(
    store: &mut Store<WasmStore>,
    memory: &Memory,
    ptr: i32,
    len: i32,
    max_len: usize,
) -> TCResult<Vec<u8>> {
    let range = checked_memory_range(store, memory, ptr, len, max_len)?;
    let mut buf = vec![0u8; range.len()];
    memory
        .read(store, range.start, &mut buf)
        .map_err(|error| TCError::bad_request(format!("WASM memory read failed: {error}")))?;
    Ok(buf)
}

fn map_wasm_error(err: wasmtime::Error) -> TCError {
    if matches!(
        err.downcast_ref::<wasmtime::Trap>(),
        Some(wasmtime::Trap::OutOfFuel)
    ) {
        TCError::resource_unavailable(
            "WASM execution fuel exhausted",
            tc_error::Pressure::new("/host/resource/wasm", tc_error::PressureReason::Saturated),
        )
    } else {
        TCError::bad_request(format!("WASM error: {err}"))
    }
}

fn unpack_wasm_pair(value: i64) -> (i32, i32) {
    let value = value as u64;
    let ptr = (value & 0xffff_ffff) as u32 as i32;
    let len = (value >> 32) as u32 as i32;
    (ptr, len)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    fn compiler_with(
        mut configure: impl FnMut(&mut crate::HostLimits),
    ) -> crate::library::compiler::Compiler {
        let mut limits = crate::HostLimits::default();
        configure(&mut limits);
        let resources = crate::HostResources::new(limits).unwrap();
        crate::library::compiler::Compiler::new(&resources).unwrap()
    }

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

        let resources = crate::HostResources::default();
        let compiler = crate::library::compiler::Compiler::new(&resources).unwrap();
        let library = compiler
            .module(Arc::from(wat.into_bytes()), resources.deadline())
            .await
            .expect("load literal WASM Library");
        assert_eq!(
            library.identity().to_string(),
            "/lib/example-devco/wasm/1.0.0"
        );
        let txn = crate::txn::test_txn("wasm-literal").await;
        let txn_id = txn.id().to_string();
        let output = library
            .call_route(
                &["answer".parse().unwrap()],
                txn_id.as_bytes(),
                &[],
                txn.deadline(),
            )
            .await
            .expect("execute bound export");
        assert_eq!(output, response);
    }

    #[tokio::test]
    async fn fuel_stops_a_nonterminating_guest_and_the_instance_recovers() {
        let entry = br#"{"definition":{"/lib/example-devco/loop/1.0.0":{"answer":42}},"routes":[{"path":"/answer","export":"answer"}]}"#;
        let entry_result = (entry.len() as i64) << 32;
        let wat = format!(
            r#"(module
                (memory (export "memory") 1)
                (data (i32.const 0) "{}")
                (func (export "alloc") (param i32) (result i32) i32.const 4096)
                (func (export "free") (param i32 i32))
                (func (export "tc_library_entry") (result i64) i64.const {entry_result})
                (func (export "answer") (param i32 i32 i32 i32) (result i64)
                    (loop br 0)
                    i64.const 0))"#,
            wat_bytes(entry),
        );
        let compiler = compiler_with(|limits| limits.execution.wasm_fuel_per_call = 10_000);
        let library = compiler
            .module(Arc::from(wat.into_bytes()), compiler.deadline())
            .await
            .expect("compile looping Library");
        let txn = crate::txn::test_txn("wasm-fuel").await;
        let error = library
            .call_route(
                &["answer".parse().unwrap()],
                txn.id().to_string().as_bytes(),
                &[],
                txn.deadline(),
            )
            .await
            .expect_err("fuel must stop the guest");
        assert_eq!(error.code(), tc_error::ErrorKind::Unavailable);

        let second = library
            .call_route(
                &["answer".parse().unwrap()],
                txn.id().to_string().as_bytes(),
                &[],
                txn.deadline(),
            )
            .await
            .expect_err("the recreated instance is bounded too");
        assert_eq!(second.code(), tc_error::ErrorKind::Unavailable);
    }

    #[tokio::test]
    async fn rejects_a_negative_guest_result_before_allocating() {
        let entry = br#"{"definition":{"/lib/example-devco/invalid/1.0.0":{"answer":42}},"routes":[{"path":"/answer","export":"answer"}]}"#;
        let entry_result = (entry.len() as i64) << 32;
        let wat = format!(
            r#"(module
                (memory (export "memory") 1)
                (data (i32.const 0) "{}")
                (func (export "alloc") (param i32) (result i32) i32.const 4096)
                (func (export "free") (param i32 i32))
                (func (export "tc_library_entry") (result i64) i64.const {entry_result})
                (func (export "answer") (param i32 i32 i32 i32) (result i64)
                    i64.const -4294967296))"#,
            wat_bytes(entry),
        );
        let compiler = compiler_with(|_| {});
        let library = compiler
            .module(Arc::from(wat.into_bytes()), compiler.deadline())
            .await
            .expect("compile invalid-result Library");
        let txn = crate::txn::test_txn("wasm-negative-result").await;
        let error = library
            .call_route(
                &["answer".parse().unwrap()],
                txn.id().to_string().as_bytes(),
                &[],
                txn.deadline(),
            )
            .await
            .expect_err("negative result length");
        assert!(error.to_string().contains("negative WASM length"));
    }

    #[tokio::test]
    async fn rejects_oversized_results_and_guest_memory() {
        let entry = br#"{"definition":{"/lib/example-devco/bounded/1.0.0":{"answer":42}},"routes":[{"path":"/answer","export":"answer"}]}"#;
        let entry_result = (entry.len() as i64) << 32;
        let oversized_result = (1024_i64 << 32) | 2048;
        let wat = format!(
            r#"(module
                (memory (export "memory") 1)
                (data (i32.const 0) "{}")
                (func (export "alloc") (param i32) (result i32) i32.const 4096)
                (func (export "free") (param i32 i32))
                (func (export "tc_library_entry") (result i64) i64.const {entry_result})
                (func (export "answer") (param i32 i32 i32 i32) (result i64)
                    i64.const {oversized_result}))"#,
            wat_bytes(entry),
        );
        let compiler = compiler_with(|limits| limits.execution.wasm_result_bytes = 256);
        let library = compiler
            .module(Arc::from(wat.into_bytes()), compiler.deadline())
            .await
            .expect("compile bounded Library");
        let txn = crate::txn::test_txn("wasm-result-limit").await;
        let error = library
            .call_route(
                &["answer".parse().unwrap()],
                txn.id().to_string().as_bytes(),
                &[],
                txn.deadline(),
            )
            .await
            .expect_err("result limit");
        assert!(
            error.to_string().contains("1024-byte limit")
                || error.to_string().contains("256-byte limit")
        );

        let oversized_memory = r#"(module
            (memory (export "memory") 2)
            (func (export "alloc") (param i32) (result i32) i32.const 0)
            (func (export "free") (param i32 i32))
            (func (export "tc_library_entry") (result i64) i64.const 0))"#;
        let compiler = compiler_with(|limits| {
            limits.execution.wasm_memory_bytes = 64 * 1024;
            limits.execution.wasm_result_bytes = 1024;
        });
        let oversized = compiler
            .module(Arc::from(oversized_memory.as_bytes()), compiler.deadline())
            .await;
        assert!(oversized.is_err(), "initial memory exceeds store limit");
    }
}
