use std::{io, path::{Path, PathBuf}, str::FromStr};

use freqfs::{Cache, DirEntry, DirLock, FileLock, FileLoad, FileSave};
use pathlink::Link;
use serde::{Deserialize, Serialize};
use safecast::AsType;
use tc_error::{TCError, TCResult};
use tc_ir::LibrarySchema;
use crate::ir::{IR_ARTIFACT_CONTENT_TYPE, WASM_ARTIFACT_CONTENT_TYPE};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const LIB_ROOT: &str = "lib";
const SCHEMA_FILE: &str = "schema.json";
const WASM_FILE: &str = "library.wasm";
const IR_FILE: &str = "library.ir.json";
const DEFAULT_CACHE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_CACHE_HANDLES: usize = 1024;

#[derive(Clone, Debug)]
pub struct Artifact {
    pub path: String,
    pub content_type: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone)]
pub struct LibraryStore {
    root: DirLock<LibraryFile>,
    dir: DirLock<LibraryFile>,
}

pub(crate) type LibraryRoot = DirLock<LibraryFile>;

#[derive(Clone)]
pub(crate) enum LibraryFile {
    Bytes(Vec<u8>),
}

impl LibraryFile {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Bytes(bytes) => bytes,
        }
    }
}

impl AsType<LibraryFile> for LibraryFile {
    fn as_type(&self) -> Option<&LibraryFile> {
        Some(self)
    }

    fn as_type_mut(&mut self) -> Option<&mut LibraryFile> {
        Some(self)
    }

    fn into_type(self) -> Option<LibraryFile> {
        Some(self)
    }
}

impl FileLoad for LibraryFile {
    async fn load(
        _path: &Path,
        mut file: tokio::fs::File,
        _metadata: std::fs::Metadata,
    ) -> io::Result<Self> {
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        Ok(Self::Bytes(bytes))
    }
}

impl FileSave for LibraryFile {
    async fn save(&self, file: &mut tokio::fs::File) -> io::Result<u64> {
        let bytes = self.bytes();
        file.write_all(bytes).await?;
        Ok(bytes.len() as u64)
    }
}

impl LibraryStore {
    pub async fn open(root: PathBuf) -> TCResult<Self> {
        let root_dir = load_library_root(root).await?;
        Ok(Self::from_root(root_dir))
    }

    pub(crate) fn from_root(root: LibraryRoot) -> Self {
        Self {
            root: root.clone(),
            dir: root,
        }
    }

    pub async fn for_schema(&self, schema: &LibrarySchema) -> TCResult<Self> {
        let segments = library_segments(schema.id())?;
        let dir = ensure_dir(self.root.clone(), &segments).await?;
        Ok(Self {
            root: self.root.clone(),
            dir,
        })
    }

    pub async fn discover_schemas(&self) -> TCResult<Vec<LibrarySchema>> {
        let mut schemas = Vec::new();
        discover_schemas(&self.root, &mut schemas).await?;
        Ok(schemas)
    }
}

impl LibraryStore {
    pub async fn persist_schema(&self, schema: &LibrarySchema) -> TCResult<()> {
        let bytes = encode_schema(schema).map_err(map_io_str)?;
        write_file(&self.dir, SCHEMA_FILE, bytes).await
    }

    pub async fn persist_artifact(&self, schema: &LibrarySchema, artifact: &Artifact) -> TCResult<()> {
        if artifact.content_type == WASM_ARTIFACT_CONTENT_TYPE {
            return self.persist_wasm_library(schema, &artifact.bytes).await;
        }
        if artifact.content_type == IR_ARTIFACT_CONTENT_TYPE {
            return self.persist_ir_library(schema, &artifact.bytes).await;
        }
        Err(TCError::bad_request(format!(
            "unsupported library artifact content type {}",
            artifact.content_type
        )))
    }

    async fn persist_wasm_library(&self, schema: &LibrarySchema, wasm: &[u8]) -> TCResult<()> {
        self.persist_schema(schema).await?;
        write_file(&self.dir, WASM_FILE, wasm.to_vec()).await
    }

    async fn persist_ir_library(&self, schema: &LibrarySchema, bytes: &[u8]) -> TCResult<()> {
        self.persist_schema(schema).await?;
        write_file(&self.dir, IR_FILE, bytes.to_vec()).await
    }

    pub async fn load_artifact(&self, schema: &LibrarySchema) -> TCResult<Option<Artifact>> {
        if let Some(schema_bytes) = read_file(&self.dir, SCHEMA_FILE).await? {
            let stored = decode_schema_bytes(&schema_bytes).map_err(TCError::internal)?;
            if stored.id() != schema.id() || stored.version() != schema.version() {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        match read_file(&self.dir, WASM_FILE).await? {
            Some(wasm_bytes) if !wasm_bytes.is_empty() => {
                return Ok(Some(Artifact {
                    path: schema.id().to_string(),
                    content_type: WASM_ARTIFACT_CONTENT_TYPE.to_string(),
                    bytes: wasm_bytes,
                }));
            }
            _ => {}
        }

        match read_file(&self.dir, IR_FILE).await? {
            Some(ir_bytes) if !ir_bytes.is_empty() => {
                return Ok(Some(Artifact {
                    path: schema.id().to_string(),
                    content_type: IR_ARTIFACT_CONTENT_TYPE.to_string(),
                    bytes: ir_bytes,
                }));
            }
            _ => {}
        }

        Ok(None)
    }
}

pub(crate) async fn load_library_root(root: PathBuf) -> TCResult<LibraryRoot> {
    let lib_root = root.join(LIB_ROOT);
    tokio::fs::create_dir_all(&lib_root).await.map_err(map_io)?;

    let cache = Cache::new(DEFAULT_CACHE_BYTES, Some(DEFAULT_CACHE_HANDLES));
    cache.load(lib_root).map_err(map_io)
}



async fn discover_schemas(dir: &DirLock<LibraryFile>, schemas: &mut Vec<LibrarySchema>) -> TCResult<()> {
    let mut pending = vec![dir.clone()];
    while let Some(current) = pending.pop() {
        let entries = read_dir_entries(&current).await?;
        for (name, entry) in entries {
            match entry {
                DirEntry::Dir(subdir) => pending.push(subdir),
                DirEntry::File(file) if name == SCHEMA_FILE => {
                    if let Some(bytes) = read_file_from_lock(&file).await? {
                        let schema = decode_schema_bytes(&bytes).map_err(TCError::internal)?;
                        schemas.push(schema);
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

async fn read_dir_entries(dir: &DirLock<LibraryFile>) -> TCResult<Vec<(String, DirEntry<LibraryFile>)>> {
    let guard = dir.read().await;
    Ok(guard.iter().map(|(name, entry)| (name.clone(), entry.clone())).collect())
}

async fn read_file(dir: &DirLock<LibraryFile>, name: &str) -> TCResult<Option<Vec<u8>>> {
    let guard = dir.read().await;
    let file = guard.get_file(name).cloned();

    match file {
        Some(file) => read_file_from_lock(&file).await,
        None => Ok(None),
    }
}

async fn read_file_from_lock(file: &FileLock<LibraryFile>) -> TCResult<Option<Vec<u8>>> {
    let guard = file.read::<LibraryFile>().await.map_err(map_io)?;
    Ok(Some(guard.bytes().to_vec()))
}

async fn write_file(dir: &DirLock<LibraryFile>, name: &str, bytes: Vec<u8>) -> TCResult<()> {
    let size = bytes.len();
    let mut guard = dir.write().await;
    if let Some(file) = guard.get_file(name) {
        let mut contents = file.write::<LibraryFile>().await.map_err(map_io)?;
        *contents = LibraryFile::Bytes(bytes);
        drop(contents);
        file.sync().await.map_err(map_io)?;
        Ok(())
    } else {
        let file = guard
            .create_file(name.to_string(), LibraryFile::Bytes(bytes), size)
            .map_err(map_io)?;
        file.sync().await.map_err(map_io)
    }
}

async fn ensure_dir(
    root: DirLock<LibraryFile>,
    segments: &[String],
) -> TCResult<DirLock<LibraryFile>> {
    let mut current = root;
    for segment in segments {
        let next = {
            let mut guard = current.write().await;
            if let Some(dir) = guard.get_dir(segment) {
                dir.clone()
            } else {
                guard.create_dir(segment.to_string()).map_err(map_io)?
            }
        };
        current = next;
    }
    Ok(current)
}

fn library_segments(link: &Link) -> TCResult<Vec<String>> {
    let raw = link.to_string();
    let path = raw.trim();
    let path = if path.starts_with('/') {
        path
    } else if let Some((_, rest)) = path.split_once("://") {
        rest.find('/').map(|idx| &rest[idx..]).ok_or_else(|| {
            TCError::bad_request("library id must be a path")
        })?
    } else {
        return Err(TCError::bad_request("library id must be a path"));
    };
    let path = path.strip_prefix('/').unwrap_or(path);

    if !path.starts_with("lib/") {
        return Err(TCError::bad_request("library id must start with /lib"));
    }

    let trimmed = path.strip_prefix("lib/").unwrap_or("");
    if trimmed.is_empty() {
        return Err(TCError::bad_request("library id missing path segments"));
    }

    Ok(trimmed.split('/').map(|s| s.to_string()).collect())
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
