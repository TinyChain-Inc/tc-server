use std::sync::Arc;
use std::time::Duration;

use pathlink::{Link, PathBuf, path_label};
use safecast::TryCastFrom;
use tc_error::ErrorKind;
use tc_ir::{Map, Public, Route, Scalar};
use tc_value::Value;

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

fn entry(name: &str, routes: &[&str]) -> (Link, Vec<u8>) {
    let identity = Link::from(
        ["lib", "example-devco", name, "1.0.0"]
            .into_iter()
            .map(|segment| segment.parse().unwrap())
            .collect::<PathBuf>(),
    );

    let definition: serde_json::Map<_, _> = routes
        .iter()
        .map(|name| (name.to_string(), serde_json::json!(42)))
        .collect();

    let routes: Vec<_> = routes
        .iter()
        .map(|name| {
            let path = PathBuf::from(name.parse::<tc_ir::Id>().unwrap());
            serde_json::json!({"path": path.to_string(), "export": name})
        })
        .collect();

    let entry = serde_json::json!({
        "definition": {identity.to_string(): definition},
        "routes": routes,
    });

    (identity, serde_json::to_vec(&entry).unwrap())
}

#[tokio::test]
async fn loads_literal_definition_and_executes_its_bound_export() {
    let (identity, entry) = entry("wasm", &["answer"]);
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
        wat_bytes(&entry),
        wat_bytes(response),
    );

    let resources = crate::HostResources::default();
    let compiler = crate::library::compiler::Compiler::new(&resources).unwrap();
    let library = compiler
        .module(
            Arc::from(wat::parse_str(&wat).unwrap()),
            resources.deadline(),
        )
        .await
        .expect("load literal WASM Library");

    assert_eq!(library.identity(), &identity);
    let txn = crate::txn::test_txn("wasm-literal").await;
    let txn_id = txn.id().to_string();
    let output = library
        .call_route(0, txn_id.as_bytes(), &[], txn.deadline())
        .await
        .expect("execute bound export");

    assert_eq!(output, response);

    let answer = PathBuf::from(path_label(&["answer"]));
    let missing = PathBuf::from(path_label(&["missing"]));
    let value = library.get(&txn, &answer, Scalar::default()).await.unwrap();
    assert_eq!(Value::opt_cast_from(value), Some(Value::from(42_u64)));

    for (path, expected) in [
        (&answer, ErrorKind::MethodNotAllowed),
        (&missing, ErrorKind::NotFound),
    ] {
        let put = library
            .put(&txn, path, Scalar::default(), crate::State::None)
            .await;

        assert_eq!(put.unwrap_err().code(), expected);
        let post = library.post(&txn, path, Map::new()).await;
        assert_eq!(post.unwrap_err().code(), expected);
        let delete = library.delete(&txn, path, Scalar::default()).await;
        assert_eq!(delete.unwrap_err().code(), expected);
    }

    {
        let _instance = library.instance.lock().await;
        let missing = tokio::time::timeout(
            Duration::from_secs(1),
            library.get(&txn, &missing, Scalar::default()),
        )
        .await
        .expect("an unknown route must not wait for the guest instance")
        .unwrap_err();

        assert_eq!(missing.code(), ErrorKind::NotFound);
    }

    for (export, message) in [
        ("memory", "WASM module must export memory"),
        ("alloc", "WASM module must export alloc"),
        ("free", "WASM module must export free"),
        ("tc_library_entry", "missing tc_library_entry export"),
        ("answer", ""),
    ] {
        let malformed = wat.replace(&format!("(export \"{export}\")"), "");
        let result = compiler
            .module(
                Arc::from(wat::parse_str(malformed).unwrap()),
                resources.deadline(),
            )
            .await;

        let error = result.err().expect("a required export must be present");
        assert_eq!(error.code(), ErrorKind::Internal);
        if export == "answer" {
            assert_eq!(
                error.message(),
                format!("missing export answer for route {answer}")
            );
        } else {
            assert_eq!(error.message(), message);
        }
    }
}

#[tokio::test]
async fn fuel_stops_a_nonterminating_guest_and_the_instance_recovers() {
    let (_, entry) = entry("loop", &["recovered", "answer"]);
    let entry_result = (entry.len() as i64) << 32;
    let response = b"42";
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
                (loop br 0)
                i64.const 0)
            (func (export "recovered") (param i32 i32 i32 i32) (result i64)
                i64.const {response_result}))"#,
        wat_bytes(&entry),
        wat_bytes(response),
    );

    let compiler = compiler_with(|limits| limits.execution.wasm_fuel_per_call = 10_000);
    let library = compiler
        .module(Arc::from(wat::parse_str(wat).unwrap()), compiler.deadline())
        .await
        .expect("compile looping Library");

    let txn = crate::txn::test_txn("wasm-fuel").await;
    let recovered = PathBuf::from(path_label(&["recovered"]));
    let recovered = library.route(&recovered).unwrap().get().unwrap();
    let error = library
        .call_route(1, txn.id().to_string().as_bytes(), &[], txn.deadline())
        .await
        .expect_err("fuel must stop the guest");

    assert_eq!(error.code(), tc_error::ErrorKind::Unavailable);
    let value = recovered(&txn, Scalar::default()).await.unwrap();
    assert_eq!(Value::opt_cast_from(value), Some(Value::from(42_u64)));

    let second = library
        .call_route(1, txn.id().to_string().as_bytes(), &[], txn.deadline())
        .await
        .expect_err("the recreated instance is bounded too");

    assert_eq!(second.code(), tc_error::ErrorKind::Unavailable);
}

#[tokio::test]
async fn rejects_a_negative_guest_result_before_allocating() {
    let (_, entry) = entry("invalid", &["answer"]);
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
        wat_bytes(&entry),
    );

    let compiler = compiler_with(|_| {});
    let library = compiler
        .module(Arc::from(wat::parse_str(wat).unwrap()), compiler.deadline())
        .await
        .expect("compile invalid-result Library");

    let txn = crate::txn::test_txn("wasm-negative-result").await;
    let error = library
        .call_route(0, txn.id().to_string().as_bytes(), &[], txn.deadline())
        .await
        .expect_err("negative result length");

    assert!(error.to_string().contains("negative WASM length"));
}

#[tokio::test]
async fn rejects_oversized_results_and_guest_memory() {
    let (_, entry) = entry("bounded", &["answer"]);
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
        wat_bytes(&entry),
    );

    let compiler = compiler_with(|limits| limits.execution.wasm_result_bytes = 256);
    let library = compiler
        .module(Arc::from(wat::parse_str(wat).unwrap()), compiler.deadline())
        .await
        .expect("compile bounded Library");

    let txn = crate::txn::test_txn("wasm-result-limit").await;
    let error = library
        .call_route(0, txn.id().to_string().as_bytes(), &[], txn.deadline())
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
        .module(
            Arc::from(wat::parse_str(oversized_memory).unwrap()),
            compiler.deadline(),
        )
        .await;

    assert!(oversized.is_err(), "initial memory exceeds store limit");
}

#[tokio::test]
async fn rejects_wat_text_before_loading_a_library() {
    let compiler = compiler_with(|_| {});
    let result = compiler
        .module(Arc::from(&b"(module)"[..]), compiler.deadline())
        .await;

    let error = result.err().expect("production input must be binary WASM");
    assert_eq!(error.code(), ErrorKind::BadRequest);
}
