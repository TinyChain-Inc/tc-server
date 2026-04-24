use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt, io,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
};

use crate::ir::{IR_ARTIFACT_CONTENT_TYPE, WASM_ARTIFACT_CONTENT_TYPE};
use freqfs::{Cache, FileLoad, FileSave, Name};
use get_size::GetSize;
use pathlink::Link;
use safecast::AsType;
use serde::{Deserialize, Serialize};
use tc_error::{TCError, TCResult};
use tc_ir::{LibrarySchema, TxnId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use txfs::{Dir as TxDir, DirEntry as TxDirEntry, Id as TxName};

const LIB_ROOT: &str = "lib";
const SCHEMA_FILE: &str = "schema.json";
const WASM_FILE: &str = "library.wasm";
const IR_FILE: &str = "library.ir.json";
const DEFAULT_CACHE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_CACHE_HANDLES: usize = 1024;
const BOOTSTRAP_TXN_VERSION: TxnVersionId = TxnVersionId(1);
const FIRST_DYNAMIC_TXN_VERSION: u64 = 2;

#[derive(Clone, Debug)]
pub struct Artifact {
    pub path: String,
    pub content_type: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone)]
pub struct LibraryStore {
    root: LibraryRoot,
    segments: Arc<Vec<TxName>>,
    versions: Arc<TxnVersionTracker>,
}

pub(crate) type LibraryRoot = TxDir<TxnVersionId, LibraryFile>;

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

impl GetSize for LibraryFile {
    fn get_size(&self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes.len(),
        }
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

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub(crate) struct TxnVersionId(u64);

impl fmt::Display for TxnVersionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for TxnVersionId {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u64>()
            .map(Self)
            .map_err(|_| "invalid transaction version id")
    }
}

impl Name for TxnVersionId {
    fn partial_cmp(&self, key: &str) -> Option<Ordering> {
        let key: TxnVersionId = key.parse().ok()?;
        PartialOrd::partial_cmp(self, &key)
    }
}

impl PartialEq<str> for TxnVersionId {
    fn eq(&self, other: &str) -> bool {
        TxnVersionId::from_str(other).is_ok_and(|other| self == &other)
    }
}

impl PartialOrd<str> for TxnVersionId {
    fn partial_cmp(&self, other: &str) -> Option<Ordering> {
        let other: TxnVersionId = other.parse().ok()?;
        PartialOrd::partial_cmp(self, &other)
    }
}

#[derive(Default)]
struct TxnVersionTracker {
    next: AtomicU64,
    finalized: AtomicU64,
    mapped: Mutex<HashMap<TxnId, TxnVersionId>>,
}

impl TxnVersionTracker {
    fn new() -> Self {
        Self {
            next: AtomicU64::new(FIRST_DYNAMIC_TXN_VERSION),
            finalized: AtomicU64::new(BOOTSTRAP_TXN_VERSION.0),
            mapped: Mutex::new(HashMap::new()),
        }
    }

    fn allocate_transient(&self) -> TxnVersionId {
        let value = self.next.fetch_add(1, AtomicOrdering::Relaxed);
        TxnVersionId(value)
    }

    fn for_txn(&self, txn_id: TxnId) -> TxnVersionId {
        let mut mapped = self.mapped.lock().expect("txn version map");
        if let Some(existing) = mapped.get(&txn_id) {
            return *existing;
        }

        let version = self.allocate_transient();
        mapped.insert(txn_id, version);
        version
    }

    fn take(&self, txn_id: &TxnId) -> Option<TxnVersionId> {
        self.mapped.lock().expect("txn version map").remove(txn_id)
    }

    fn mark_finalized(&self, version: TxnVersionId) {
        self.finalized.fetch_max(version.0, AtomicOrdering::Relaxed);
    }

    fn read_version(&self) -> TxnVersionId {
        TxnVersionId(self.finalized.load(AtomicOrdering::Relaxed))
    }
}

impl LibraryStore {
    pub async fn open(root: PathBuf) -> TCResult<Self> {
        let root_dir = load_library_root(root).await?;
        Ok(Self::from_root(root_dir))
    }

    pub(crate) fn from_root(root: LibraryRoot) -> Self {
        Self {
            root,
            segments: Arc::new(Vec::new()),
            versions: Arc::new(TxnVersionTracker::new()),
        }
    }

    pub async fn for_schema(&self, schema: &LibrarySchema) -> TCResult<Self> {
        let segments = library_segments(schema.id())?
            .into_iter()
            .map(|segment| parse_name(&segment))
            .collect::<TCResult<Vec<_>>>()?;

        Ok(Self {
            root: self.root.clone(),
            segments: Arc::new(segments),
            versions: self.versions.clone(),
        })
    }

    pub async fn discover_schemas(&self) -> TCResult<Vec<LibrarySchema>> {
        let mut schemas = Vec::new();
        discover_schemas(&self.root, self.versions.read_version(), &mut schemas).await?;
        Ok(schemas)
    }
}

impl LibraryStore {
    pub async fn persist_schema(&self, schema: &LibrarySchema) -> TCResult<()> {
        let version = self.versions.allocate_transient();
        let staged = self.persist_schema_at(version, schema).await;
        match staged {
            Ok(()) => self.finalize_version(version, true).await,
            Err(err) => {
                let _ = self.finalize_version(version, false).await;
                Err(err)
            }
        }
    }

    pub async fn stage_schema(&self, txn_id: TxnId, schema: &LibrarySchema) -> TCResult<()> {
        let version = self.versions.for_txn(txn_id);
        self.persist_schema_at(version, schema).await
    }

    pub async fn persist_artifact(
        &self,
        schema: &LibrarySchema,
        artifact: &Artifact,
    ) -> TCResult<()> {
        let version = self.versions.allocate_transient();
        let staged = self.persist_artifact_at(version, schema, artifact).await;
        match staged {
            Ok(()) => self.finalize_version(version, true).await,
            Err(err) => {
                let _ = self.finalize_version(version, false).await;
                Err(err)
            }
        }
    }

    pub async fn stage_artifact(
        &self,
        txn_id: TxnId,
        schema: &LibrarySchema,
        artifact: &Artifact,
    ) -> TCResult<()> {
        let version = self.versions.for_txn(txn_id);
        self.persist_artifact_at(version, schema, artifact).await
    }

    pub async fn finalize_txn(&self, txn_id: TxnId, commit: bool) -> TCResult<()> {
        let Some(version) = self.versions.take(&txn_id) else {
            return Ok(());
        };

        self.finalize_version(version, commit).await
    }

    pub async fn load_artifact(&self, schema: &LibrarySchema) -> TCResult<Option<Artifact>> {
        let read_version = self.versions.read_version();
        let Some(dir) = self.resolve_dir(read_version, false).await? else {
            return Ok(None);
        };

        if let Some(schema_bytes) = read_file(&dir, read_version, SCHEMA_FILE).await? {
            let stored = decode_schema_bytes(&schema_bytes).map_err(TCError::internal)?;
            if stored.id() != schema.id() || stored.version() != schema.version() {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        match read_file(&dir, read_version, WASM_FILE).await? {
            Some(wasm_bytes) if !wasm_bytes.is_empty() => {
                return Ok(Some(Artifact {
                    path: schema.id().to_string(),
                    content_type: WASM_ARTIFACT_CONTENT_TYPE.to_string(),
                    bytes: wasm_bytes,
                }));
            }
            _ => {}
        }

        match read_file(&dir, read_version, IR_FILE).await? {
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

    async fn persist_schema_at(
        &self,
        version: TxnVersionId,
        schema: &LibrarySchema,
    ) -> TCResult<()> {
        let bytes = encode_schema(schema).map_err(map_io_str)?;
        let dir = self.resolve_dir(version, true).await?.expect("create dir");
        write_file(&dir, version, SCHEMA_FILE, bytes).await
    }

    async fn persist_artifact_at(
        &self,
        version: TxnVersionId,
        schema: &LibrarySchema,
        artifact: &Artifact,
    ) -> TCResult<()> {
        if artifact.content_type == WASM_ARTIFACT_CONTENT_TYPE {
            return self
                .persist_wasm_library(version, schema, &artifact.bytes)
                .await;
        }
        if artifact.content_type == IR_ARTIFACT_CONTENT_TYPE {
            return self
                .persist_ir_library(version, schema, &artifact.bytes)
                .await;
        }

        Err(TCError::bad_request(format!(
            "unsupported library artifact content type {}",
            artifact.content_type
        )))
    }

    async fn persist_wasm_library(
        &self,
        version: TxnVersionId,
        schema: &LibrarySchema,
        wasm: &[u8],
    ) -> TCResult<()> {
        self.persist_schema_at(version, schema).await?;
        let dir = self.resolve_dir(version, true).await?.expect("create dir");
        write_file(&dir, version, WASM_FILE, wasm.to_vec()).await
    }

    async fn persist_ir_library(
        &self,
        version: TxnVersionId,
        schema: &LibrarySchema,
        bytes: &[u8],
    ) -> TCResult<()> {
        self.persist_schema_at(version, schema).await?;
        let dir = self.resolve_dir(version, true).await?.expect("create dir");
        write_file(&dir, version, IR_FILE, bytes.to_vec()).await
    }

    async fn resolve_dir(
        &self,
        version: TxnVersionId,
        create: bool,
    ) -> TCResult<Option<LibraryRoot>> {
        let mut current = self.root.clone();

        for segment in self.segments.iter() {
            if create {
                match current.create_dir(version, segment.clone()).await {
                    Ok(created) => {
                        current = created;
                        continue;
                    }
                    Err(txfs::Error::IO(err)) if err.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(err) => return Err(map_txfs(err)),
                }
            }

            let Some(next) = current.get_dir(version, segment).await.map_err(map_txfs)? else {
                return Ok(None);
            };

            current = (*next).clone();
        }

        Ok(Some(current))
    }

    async fn finalize_version(&self, version: TxnVersionId, commit: bool) -> TCResult<()> {
        if commit {
            self.root.commit(version, true).await;
        } else {
            self.root.rollback(version, true).await;
        }

        self.root.finalize(version).await;
        self.versions.mark_finalized(version);
        Ok(())
    }
}

pub(crate) async fn load_library_root(root: PathBuf) -> TCResult<LibraryRoot> {
    let lib_root = root.join(LIB_ROOT);
    tokio::fs::create_dir_all(&lib_root).await.map_err(map_io)?;

    let cache = Cache::new(DEFAULT_CACHE_BYTES, Some(DEFAULT_CACHE_HANDLES));
    let canonical = cache.load(lib_root).map_err(map_io)?;
    let root = TxDir::load(BOOTSTRAP_TXN_VERSION, canonical)
        .await
        .map_err(map_txfs)?;

    // Seal the loaded snapshot so subsequent transactions can stage writes without
    // conflicting against the bootstrap transaction.
    root.commit(BOOTSTRAP_TXN_VERSION, true).await;
    root.finalize(BOOTSTRAP_TXN_VERSION).await;

    Ok(root)
}

async fn discover_schemas(
    dir: &LibraryRoot,
    read_version: TxnVersionId,
    schemas: &mut Vec<LibrarySchema>,
) -> TCResult<()> {
    let mut pending = vec![dir.clone()];
    let schema_name = parse_name(SCHEMA_FILE)?;

    while let Some(current) = pending.pop() {
        let iter = current.iter(read_version).await.map_err(map_txfs)?;

        for (name, entry) in iter {
            match &*entry {
                TxDirEntry::Dir(subdir) => pending.push(subdir.clone()),
                TxDirEntry::File(file) if name == schema_name => {
                    let guard = file
                        .read::<LibraryFile>(read_version)
                        .await
                        .map_err(map_txfs)?;
                    let schema = decode_schema_bytes(guard.bytes()).map_err(TCError::internal)?;
                    schemas.push(schema);
                }
                _ => {}
            }
        }
    }

    Ok(())
}

async fn read_file(
    dir: &LibraryRoot,
    version: TxnVersionId,
    name: &str,
) -> TCResult<Option<Vec<u8>>> {
    let name = parse_name(name)?;
    let Some(file) = dir.get_file(version, &name).await.map_err(map_txfs)? else {
        return Ok(None);
    };

    let guard = file.read::<LibraryFile>(version).await.map_err(map_txfs)?;
    Ok(Some(guard.bytes().to_vec()))
}

async fn write_file(
    dir: &LibraryRoot,
    version: TxnVersionId,
    name: &str,
    bytes: Vec<u8>,
) -> TCResult<()> {
    let name = parse_name(name)?;
    if let Some(file) = dir.get_file(version, &name).await.map_err(map_txfs)? {
        let mut contents = file.write::<LibraryFile>(version).await.map_err(map_txfs)?;
        *contents = LibraryFile::Bytes(bytes);
        return Ok(());
    }

    dir.create_file(version, name, LibraryFile::Bytes(bytes))
        .await
        .map_err(map_txfs)?;

    Ok(())
}

fn parse_name(name: &str) -> TCResult<TxName> {
    name.parse::<TxName>()
        .map_err(|err| TCError::bad_request(err.to_string()))
}

fn library_segments(link: &Link) -> TCResult<Vec<String>> {
    let raw = link.to_string();
    let path = raw.trim();
    let path = if path.starts_with('/') {
        path
    } else if let Some((_, rest)) = path.split_once("://") {
        rest.find('/')
            .map(|idx| &rest[idx..])
            .ok_or_else(|| TCError::bad_request("library id must be a path"))?
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

fn map_txfs(err: txfs::Error) -> TCError {
    TCError::internal(err.to_string())
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
