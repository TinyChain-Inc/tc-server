use std::str::FromStr;
use std::sync::OnceLock;

use pathlink::Link;
use tc_ir::{Claim, Dir, Library, LibrarySchema, Transaction, TxnId};
use wasm_encoder::{
    CodeSection, ConstExpr, DataSection, ExportKind, ExportSection, Function, FunctionSection,
    GlobalSection, GlobalType, Instruction, MemorySection, MemoryType, Module, TypeSection,
    ValType,
};

pub(crate) fn wasm_hello_world_module() -> Vec<u8> {
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    BYTES.get_or_init(build_wasm_hello_world_module).clone()
}

fn build_wasm_hello_world_module() -> Vec<u8> {
    const SCHEMA_ID: &str = "/lib/example";
    const RESPONSE_JSON: &str = r#""hello""#;
    const MANIFEST_OFFSET: u32 = 8;
    const RESPONSE_OFFSET: u32 = 512;
    const HEAP_INIT: u32 = 1024;

    let schema = LibrarySchema::new(
        Link::from_str(SCHEMA_ID).expect("schema id link"),
        "0.1.0",
        vec![],
    );

    let library = FixtureLibrary::new(schema);
    let routes = [tc_wasm::RouteExport::new("/hello", "hello")];
    let manifest = tc_wasm::manifest_bytes(&library, &routes);

    let manifest_len: u32 = manifest.len().try_into().expect("manifest fits in u32");
    let response_bytes = RESPONSE_JSON.as_bytes();
    let response_len: u32 = response_bytes
        .len()
        .try_into()
        .expect("response fits in u32");

    let manifest_packed: i64 = (((manifest_len as u64) << 32) | (MANIFEST_OFFSET as u64)) as i64;

    let mut module = Module::new();

    let mut types = TypeSection::new();
    types.function([ValType::I32], [ValType::I32]); // 0
    types.function([ValType::I32, ValType::I32], []); // 1
    types.function([], [ValType::I64]); // 2
    types.function(
        [ValType::I32, ValType::I32, ValType::I32, ValType::I32],
        [ValType::I64],
    ); // 3
    let alloc_ty = 0u32;
    let free_ty = 1u32;
    let entry_ty = 2u32;
    let hello_ty = 3u32;

    module.section(&types);

    let mut functions = FunctionSection::new();
    functions.function(alloc_ty);
    functions.function(free_ty);
    functions.function(entry_ty);
    functions.function(hello_ty);
    module.section(&functions);

    let mut memories = MemorySection::new();
    memories.memory(MemoryType {
        minimum: 1,
        maximum: None,
        memory64: false,
        shared: false,
        page_size_log2: None,
    });
    module.section(&memories);

    let mut globals = GlobalSection::new();
    globals.global(
        GlobalType {
            val_type: ValType::I32,
            mutable: true,
            shared: false,
        },
        &ConstExpr::i32_const(HEAP_INIT as i32),
    );
    module.section(&globals);

    let mut exports = ExportSection::new();
    exports.export("memory", ExportKind::Memory, 0);
    exports.export("alloc", ExportKind::Func, 0);
    exports.export("free", ExportKind::Func, 1);
    exports.export("tc_library_entry", ExportKind::Func, 2);
    exports.export("hello", ExportKind::Func, 3);
    module.section(&exports);

    let mut code = CodeSection::new();

    // alloc(len: i32) -> i32
    let mut alloc = Function::new([(1, ValType::I32)]);
    alloc.instruction(&Instruction::GlobalGet(0));
    alloc.instruction(&Instruction::LocalSet(1));
    alloc.instruction(&Instruction::GlobalGet(0));
    alloc.instruction(&Instruction::LocalGet(0));
    alloc.instruction(&Instruction::I32Add);
    alloc.instruction(&Instruction::GlobalSet(0));
    alloc.instruction(&Instruction::LocalGet(1));
    alloc.instruction(&Instruction::End);
    code.function(&alloc);

    // free(ptr: i32, len: i32) -> ()
    let mut free = Function::new([]);
    free.instruction(&Instruction::End);
    code.function(&free);

    // tc_library_entry() -> i64
    let mut entry = Function::new([]);
    entry.instruction(&Instruction::I64Const(manifest_packed));
    entry.instruction(&Instruction::End);
    code.function(&entry);

    // hello(header_ptr, header_len, body_ptr, body_len) -> i64
    // Allocate a response buffer, copy the response bytes into it, then return (len<<32 | ptr).
    let mut hello = Function::new([(1, ValType::I32)]);
    hello.instruction(&Instruction::I32Const(response_len as i32));
    hello.instruction(&Instruction::Call(0));
    hello.instruction(&Instruction::LocalSet(4));
    hello.instruction(&Instruction::LocalGet(4));
    hello.instruction(&Instruction::I32Const(RESPONSE_OFFSET as i32));
    hello.instruction(&Instruction::I32Const(response_len as i32));
    hello.instruction(&Instruction::MemoryCopy {
        src_mem: 0,
        dst_mem: 0,
    });
    hello.instruction(&Instruction::I32Const(response_len as i32));
    hello.instruction(&Instruction::I64ExtendI32U);
    hello.instruction(&Instruction::I64Const(32));
    hello.instruction(&Instruction::I64Shl);
    hello.instruction(&Instruction::LocalGet(4));
    hello.instruction(&Instruction::I64ExtendI32U);
    hello.instruction(&Instruction::I64Or);
    hello.instruction(&Instruction::End);
    code.function(&hello);

    module.section(&code);

    let mut data = DataSection::new();
    data.active(0, &ConstExpr::i32_const(MANIFEST_OFFSET as i32), manifest);
    data.active(
        0,
        &ConstExpr::i32_const(RESPONSE_OFFSET as i32),
        response_bytes.to_vec(),
    );
    module.section(&data);

    module.finish()
}

#[derive(Clone)]
struct FixtureTxn {
    claim: Claim,
}

impl Default for FixtureTxn {
    fn default() -> Self {
        Self {
            claim: Claim::new(
                Link::from_str("/lib/example").expect("claim link"),
                umask::Mode::all(),
            ),
        }
    }
}

impl Transaction for FixtureTxn {
    fn id(&self) -> TxnId {
        tc_ir::TxnId::from_parts(tc_ir::NetworkTime::from_nanos(1), 0)
    }

    fn timestamp(&self) -> tc_ir::NetworkTime {
        tc_ir::NetworkTime::from_nanos(1)
    }

    fn claim(&self) -> &Claim {
        &self.claim
    }
}

struct FixtureLibrary {
    schema: LibrarySchema,
    routes: Dir<()>,
}

impl FixtureLibrary {
    fn new(schema: LibrarySchema) -> Self {
        Self {
            schema,
            routes: Dir::default(),
        }
    }
}

impl Library for FixtureLibrary {
    type Txn = FixtureTxn;
    type Routes = Dir<()>;

    fn schema(&self) -> &LibrarySchema {
        &self.schema
    }

    fn routes(&self) -> &Self::Routes {
        &self.routes
    }
}
