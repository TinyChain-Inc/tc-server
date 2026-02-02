use std::{
    fs, io,
    path::{Path, PathBuf},
    str::FromStr,
};

use pathlink::Link;
use serde::{Deserialize, Serialize};
use tc_error::{TCError, TCResult};
use tc_ir::LibrarySchema;

/// On-disk layout for persisted libraries.
///
/// ```text
/// <root>/lib/<id>/<version>/schema.json
/// <root>/lib/<id>/<version>/library.wasm
/// <root>/lib/<id>/<version>/library.ir.json
/// ```
#[derive(Clone)]
pub struct LibraryDir {
    root: PathBuf,
}

#[derive(Clone)]
pub enum LibraryArtifact {
    Wasm(Vec<u8>),
    Ir(Vec<u8>),
}

impl LibraryDir {
    pub fn new<P: Into<PathBuf>>(root: P) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn load_all(&self) -> TCResult<Vec<(LibrarySchema, LibraryArtifact)>> {
        let mut out = Vec::new();
        let lib_root = self.root.join("lib");
        if !lib_root.exists() {
            return Ok(out);
        }

        for entry in fs::read_dir(&lib_root).map_err(map_io)? {
            let id_dir = entry.map_err(map_io)?.path();
            if !id_dir.is_dir() {
                continue;
            }

            for version_entry in fs::read_dir(&id_dir).map_err(map_io)? {
                let version_dir = version_entry.map_err(map_io)?.path();
                if !version_dir.is_dir() {
                    continue;
                }
                let schema_path = version_dir.join("schema.json");
                let wasm_path = version_dir.join("library.wasm");
                let ir_path = version_dir.join("library.ir.json");
                if !schema_path.exists() {
                    continue;
                }
                let schema_bytes = fs::read(&schema_path).map_err(map_io)?;
                let schema = decode_schema_bytes(&schema_bytes).map_err(TCError::internal)?;
                if schema_bytes.is_empty() {
                    continue;
                }

                if wasm_path.exists() {
                    let wasm_bytes = fs::read(&wasm_path).map_err(map_io)?;
                    if !wasm_bytes.is_empty() {
                        out.push((schema, LibraryArtifact::Wasm(wasm_bytes)));
                        continue;
                    }
                }

                if ir_path.exists() {
                    let ir_bytes = fs::read(&ir_path).map_err(map_io)?;
                    if !ir_bytes.is_empty() {
                        out.push((schema, LibraryArtifact::Ir(ir_bytes)));
                    }
                }
            }
        }

        Ok(out)
    }

    pub fn persist_schema(&self, schema: &LibrarySchema) -> TCResult<()> {
        let dir = self.ensure_lib_dir(schema);
        fs::create_dir_all(&dir).map_err(map_io)?;
        let bytes = encode_schema(schema).map_err(map_io_str)?;
        fs::write(dir.join("schema.json"), bytes).map_err(map_io)?;
        Ok(())
    }

    pub fn persist_wasm_library(&self, schema: &LibrarySchema, wasm: &[u8]) -> TCResult<()> {
        let dir = self.ensure_lib_dir(schema);
        fs::create_dir_all(&dir).map_err(map_io)?;
        let bytes = encode_schema(schema).map_err(map_io_str)?;
        fs::write(dir.join("schema.json"), bytes).map_err(map_io)?;
        fs::write(dir.join("library.wasm"), wasm).map_err(map_io)?;
        Ok(())
    }

    pub fn persist_ir_library(&self, schema: &LibrarySchema, bytes: &[u8]) -> TCResult<()> {
        let dir = self.ensure_lib_dir(schema);
        fs::create_dir_all(&dir).map_err(map_io)?;
        let schema_bytes = encode_schema(schema).map_err(map_io_str)?;
        fs::write(dir.join("schema.json"), schema_bytes).map_err(map_io)?;
        fs::write(dir.join("library.ir.json"), bytes).map_err(map_io)?;
        Ok(())
    }

    fn ensure_lib_dir(&self, schema: &LibrarySchema) -> PathBuf {
        self.root
            .join("lib")
            .join(sanitize_id(schema.id()))
            .join(schema.version())
    }
}

fn map_io(err: io::Error) -> TCError {
    TCError::internal(err.to_string())
}

fn map_io_str(err: String) -> io::Error {
    io::Error::other(err)
}

// Schema payloads are tiny (tens of bytes) and must already be buffered so we can
// distinguish between schema-only installs and schema+artifact payloads. Using
// `serde_json` here avoids spinning up nested async executors inside adapters.
pub fn decode_schema_bytes(bytes: &[u8]) -> Result<LibrarySchema, String> {
    let parsed: RawSchema =
        serde_json::from_slice(bytes).map_err(|err| format!("invalid schema json: {err}"))?;
    parsed.try_into()
}

pub fn encode_schema(schema: &LibrarySchema) -> Result<Vec<u8>, String> {
    let raw = RawSchema {
        id: schema.id().to_string(),
        version: schema.version().to_string(),
        dependencies: schema
            .dependencies()
            .iter()
            .map(|dep| dep.to_string())
            .collect(),
    };
    serde_json::to_vec(&raw).map_err(|err| err.to_string())
}

pub fn sanitize_id(link: &Link) -> String {
    link.to_string().trim_start_matches('/').replace('/', "_")
}

#[derive(Deserialize, Serialize)]
struct RawSchema {
    id: String,
    version: String,
    #[serde(default)]
    dependencies: Vec<String>,
}

impl TryFrom<RawSchema> for LibrarySchema {
    type Error = String;

    fn try_from(raw: RawSchema) -> Result<Self, Self::Error> {
        let id = Link::from_str(&raw.id).map_err(|err| err.to_string())?;
        let deps = raw
            .dependencies
            .into_iter()
            .map(|dep| Link::from_str(&dep).map_err(|err| err.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(LibrarySchema::new(id, raw.version, deps))
    }
}
