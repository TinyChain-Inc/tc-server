use std::collections::HashMap;

use pathlink::PathSegment;
use tc_error::{TCError, TCResult};
use tc_ir::{LibrarySchema, TxnHeader};
use wasmtime::{Engine, Func, Instance, Memory, Module, Store};

use super::manifest::{decode_manifest, format_path, RouteBinding};

/// Loads a TinyChain-compatible Library embedded in a WASM module.
pub struct WasmLibrary {
    store: Store<()>,
    memory: Memory,
    alloc: Func,
    free: Func,
    schema: LibrarySchema,
    routes: HashMap<Vec<PathSegment>, Func>,
    bindings: Vec<RouteBinding>,
}

impl WasmLibrary {
    pub fn from_bytes(engine: &Engine, bytes: &[u8]) -> TCResult<Self> {
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

        let (schema, bindings) = Self::load_manifest(&mut store, &instance, &memory)?;

        let mut route_map = HashMap::new();
        for binding in &bindings {
            let func = instance
                .get_func(&mut store, &binding.export)
                .ok_or_else(|| {
                    TCError::internal(format!(
                        "missing export {} for route {}",
                        binding.export,
                        format_path(&binding.path)
                    ))
                })?;
            route_map.insert(binding.path.clone(), func);
        }

        Ok(Self {
            store,
            memory,
            alloc,
            free,
            schema,
            routes: route_map,
            bindings,
        })
    }

    pub fn schema(&self) -> &LibrarySchema {
        &self.schema
    }

    pub fn routes(&self) -> impl Iterator<Item = &Vec<PathSegment>> {
        self.routes.keys()
    }

    pub(crate) fn bindings(&self) -> &[RouteBinding] {
        &self.bindings
    }

    pub fn call_route(
        &mut self,
        path: &[PathSegment],
        header: &TxnHeader,
        body: &[u8],
    ) -> TCResult<Vec<u8>> {
        let func = self
            .routes
            .get(path)
            .cloned()
            .ok_or_else(|| TCError::not_found(format_path(path)))?;

        let header_bytes = encode_json(header)?;
        let (header_ptr, header_len) = self.write_buffer(&header_bytes)?;
        let (body_ptr, body_len) = if body.is_empty() {
            (0, 0)
        } else {
            self.write_buffer(body)?
        };

        let typed = func
            .typed::<(i32, i32, i32, i32), i64>(&self.store)
            .map_err(map_wasm_error)?;
        let packed = typed
            .call(
                &mut self.store,
                (header_ptr, header_len, body_ptr, body_len),
            )
            .map_err(map_wasm_error)?;
        let (result_ptr, result_len) = unpack_wasm_pair(packed);

        let bytes = self.read_buffer(result_ptr, result_len)?;
        if header_len > 0 {
            self.free_buffer(header_ptr, header_len)?;
        }
        if body_len > 0 {
            self.free_buffer(body_ptr, body_len)?;
        }
        self.free_buffer(result_ptr, result_len)?;

        Ok(bytes)
    }

    fn load_manifest(
        store: &mut Store<()>,
        instance: &Instance,
        memory: &Memory,
    ) -> TCResult<(LibrarySchema, Vec<RouteBinding>)> {
        let func = instance
            .get_func(&mut *store, "tc_library_entry")
            .ok_or_else(|| TCError::internal("missing tc_library_entry export"))?;
        let typed = func.typed::<(), i64>(&mut *store).map_err(map_wasm_error)?;
        let packed = typed.call(&mut *store, ()).map_err(map_wasm_error)?;
        let (ptr, len) = unpack_wasm_pair(packed);
        let bytes = read_memory(store, memory, ptr, len)?;
        let manifest = decode_manifest(bytes)?;
        Ok((manifest.schema, manifest.routes))
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

fn encode_json(header: &TxnHeader) -> TCResult<Vec<u8>> {
    serde_json::to_vec(header)
        .map_err(|err| TCError::internal(format!("failed to encode txn header: {err}")))
}
