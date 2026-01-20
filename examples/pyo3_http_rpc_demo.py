import argparse
import json
import sys


def lib_id(publisher: str, name: str, version: str) -> str:
    return f"/lib/{publisher}/{name}/{version}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--authority",
        required=True,
        help="HTTP authority (host:port) of the remote TinyChain host serving B",
    )
    args = parser.parse_args()

    try:
        from tinychain import KernelHandle
    except Exception as e:
        print(
            "failed to import the PyO3 module 'tinychain'\n"
            "build it first, e.g.:\n"
            "  cargo build -p tinychain --features pyo3\n",
            file=sys.stderr,
        )
        raise e

    b_root = lib_id("example-devco", "b", "0.1.0")
    a_root = lib_id("example-devco", "a", "0.1.0")

    schema_a = {
        "id": a_root,
        "version": "0.1.0",
        "dependencies": [b_root],
    }

    kernel = KernelHandle.with_library_schema_and_dependency_route(
        json.dumps(schema_a), b_root, args.authority
    )

    ok = kernel.resolve_get(f"{b_root}/hello")
    print("A → B status:", ok.status)
    if ok.body is not None:
        print("A → B body:", ok.body.bytes.decode("utf-8"))

    c_root = lib_id("example-devco", "c", "0.1.0")
    try:
        kernel.resolve_get(f"{c_root}/hello")
        print("ERROR: expected A → C to fail, but it succeeded")
        return 1
    except Exception as e:
        print("A → C unauthorized (expected):", str(e))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

