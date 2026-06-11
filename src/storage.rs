use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    io,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
};

use crate::ir::{IR_ARTIFACT_CONTENT_TYPE, WASM_ARTIFACT_CONTENT_TYPE};
use freqfs::{Cache, DirEntry as FsDirEntry, DirLock as FsDirLock, FileLoad, FileSave, Name};
use get_size::GetSize;
use pathlink::Link;
use safecast::AsType;
use serde::{Deserialize, Serialize};
use tc_error::{TCError, TCResult};
use tc_ir::{LibrarySchema, NetworkTime, TxnId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use txfs::{Dir as TxDir, Id as TxName};

const LIB_ROOT: &str = "lib";
const SCHEMA_FILE: &str = "schema.json";
const WASM_FILE: &str = "library.wasm";
const IR_FILE: &str = "library.ir.json";
const DEFAULT_CACHE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_CACHE_HANDLES: usize = 1024;
const BOOTSTRAP_TXN: StorageTxnId = StorageTxnId(TxnId::from_parts(NetworkTime::from_nanos(1), 0));
static IMMEDIATE_TXN_NONCE: AtomicU64 = AtomicU64::new(1);

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
}

pub(crate) type LibraryRoot = TxDir<StorageTxnId, LibraryFile>;

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

#[derive(Copy, Clone, Debug)]
pub(crate) struct StorageTxnId(TxnId);

impl From<TxnId> for StorageTxnId {
    fn from(txn_id: TxnId) -> Self {
        Self(txn_id)
    }
}

impl fmt::Display for StorageTxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl PartialEq for StorageTxnId {
    fn eq(&self, other: &Self) -> bool {
        self.0.timestamp() == other.0.timestamp() && self.0.nonce() == other.0.nonce()
    }
}

impl Eq for StorageTxnId {}

impl Hash for StorageTxnId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.timestamp().hash(state);
        self.0.nonce().hash(state);
    }
}

impl PartialOrd for StorageTxnId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StorageTxnId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .timestamp()
            .cmp(&other.0.timestamp())
            .then_with(|| self.0.nonce().cmp(&other.0.nonce()))
    }
}

impl FromStr for StorageTxnId {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        TxnId::from_str(s).map(Self)
    }
}

impl Name for StorageTxnId {
    fn partial_cmp(&self, key: &str) -> Option<Ordering> {
        let key: StorageTxnId = key.parse().ok()?;
        PartialOrd::partial_cmp(self, &key)
    }
}

impl PartialEq<str> for StorageTxnId {
    fn eq(&self, other: &str) -> bool {
        StorageTxnId::from_str(other).is_ok_and(|other| self == &other)
    }
}

impl PartialOrd<str> for StorageTxnId {
    fn partial_cmp(&self, other: &str) -> Option<Ordering> {
        let other: StorageTxnId = other.parse().ok()?;
        PartialOrd::partial_cmp(self, &other)
    }
}

fn immediate_txn_id() -> StorageTxnId {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(2);
    let nonce = IMMEDIATE_TXN_NONCE.fetch_add(1, AtomicOrdering::Relaxed) as u16;
    StorageTxnId(TxnId::from_parts(
        NetworkTime::from_nanos(nanos.max(2)),
        nonce,
    ))
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
        })
    }

    pub async fn discover_schemas(&self) -> TCResult<Vec<LibrarySchema>> {
        let mut schemas = Vec::new();
        discover_schemas(self.root.canonical(), &mut schemas).await?;
        Ok(schemas)
    }
}

impl LibraryStore {
    pub async fn persist_schema_immediate(&self, schema: &LibrarySchema) -> TCResult<()> {
        // Top-level immediate installs still stage through txfs so disk writes
        // commit atomically. Do not call this from inside an active kernel txn.
        let txn_id = immediate_txn_id();
        let staged = self.persist_schema_at(txn_id, schema).await;
        match staged {
            Ok(()) => self.finalize_storage_txn(txn_id, true).await,
            Err(err) => {
                let _ = self.finalize_storage_txn(txn_id, false).await;
                Err(err)
            }
        }
    }

    pub async fn stage_schema(&self, txn_id: TxnId, schema: &LibrarySchema) -> TCResult<()> {
        self.persist_schema_at(txn_id.into(), schema).await
    }

    pub async fn persist_artifact_immediate(
        &self,
        schema: &LibrarySchema,
        artifact: &Artifact,
    ) -> TCResult<()> {
        // Top-level immediate installs still stage through txfs so disk writes
        // commit atomically. Do not call this from inside an active kernel txn.
        let txn_id = immediate_txn_id();
        let staged = self.persist_artifact_at(txn_id, schema, artifact).await;
        match staged {
            Ok(()) => self.finalize_storage_txn(txn_id, true).await,
            Err(err) => {
                let _ = self.finalize_storage_txn(txn_id, false).await;
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
        self.persist_artifact_at(txn_id.into(), schema, artifact)
            .await
    }

    pub async fn finalize_txn(&self, txn_id: TxnId, commit: bool) -> TCResult<()> {
        self.finalize_storage_txn(txn_id.into(), commit).await
    }

    pub async fn load_artifact(&self, schema: &LibrarySchema) -> TCResult<Option<Artifact>> {
        let Some(dir) = self.resolve_canonical_dir().await? else {
            return Ok(None);
        };

        if let Some(schema_bytes) = read_canonical_file(&dir, SCHEMA_FILE).await? {
            let stored = decode_schema_bytes(&schema_bytes).map_err(TCError::internal)?;
            if stored.id() != schema.id() || stored.version() != schema.version() {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        match read_canonical_file(&dir, WASM_FILE).await? {
            Some(wasm_bytes) if !wasm_bytes.is_empty() => {
                return Ok(Some(Artifact {
                    path: schema.id().to_string(),
                    content_type: WASM_ARTIFACT_CONTENT_TYPE.to_string(),
                    bytes: wasm_bytes,
                }));
            }
            _ => {}
        }

        match read_canonical_file(&dir, IR_FILE).await? {
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
        txn_id: StorageTxnId,
        schema: &LibrarySchema,
    ) -> TCResult<()> {
        let bytes = encode_schema(schema).map_err(map_io_str)?;
        let dir = self.resolve_dir(txn_id, true).await?.expect("create dir");
        write_file(&dir, txn_id, SCHEMA_FILE, bytes).await
    }

    async fn persist_artifact_at(
        &self,
        txn_id: StorageTxnId,
        schema: &LibrarySchema,
        artifact: &Artifact,
    ) -> TCResult<()> {
        if artifact.content_type == WASM_ARTIFACT_CONTENT_TYPE {
            return self
                .persist_wasm_library(txn_id, schema, &artifact.bytes)
                .await;
        }
        if artifact.content_type == IR_ARTIFACT_CONTENT_TYPE {
            return self
                .persist_ir_library(txn_id, schema, &artifact.bytes)
                .await;
        }

        Err(TCError::bad_request(format!(
            "unsupported library artifact content type {}",
            artifact.content_type
        )))
    }

    async fn persist_wasm_library(
        &self,
        txn_id: StorageTxnId,
        schema: &LibrarySchema,
        wasm: &[u8],
    ) -> TCResult<()> {
        self.persist_schema_at(txn_id, schema).await?;
        let dir = self.resolve_dir(txn_id, true).await?.expect("create dir");
        write_file(&dir, txn_id, WASM_FILE, wasm.to_vec()).await
    }

    async fn persist_ir_library(
        &self,
        txn_id: StorageTxnId,
        schema: &LibrarySchema,
        bytes: &[u8],
    ) -> TCResult<()> {
        self.persist_schema_at(txn_id, schema).await?;
        let dir = self.resolve_dir(txn_id, true).await?.expect("create dir");
        write_file(&dir, txn_id, IR_FILE, bytes.to_vec()).await
    }

    async fn resolve_dir(
        &self,
        txn_id: StorageTxnId,
        create: bool,
    ) -> TCResult<Option<LibraryRoot>> {
        let mut current = self.root.clone();

        for segment in self.segments.iter() {
            if create {
                match current.create_dir(txn_id, segment.clone()).await {
                    Ok(created) => {
                        current = created;
                        continue;
                    }
                    Err(txfs::Error::IO(err)) if err.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(err) => return Err(map_txfs(err)),
                }
            }

            let Some(next) = current.get_dir(txn_id, segment).await.map_err(map_txfs)? else {
                return Ok(None);
            };

            current = (*next).clone();
        }

        Ok(Some(current))
    }

    async fn resolve_canonical_dir(&self) -> TCResult<Option<FsDirLock<LibraryFile>>> {
        let mut current = self.root.canonical();

        for segment in self.segments.iter() {
            let next = {
                let guard = current.read().await;
                guard.get_dir(&segment.to_string()).cloned()
            };

            let Some(next) = next else {
                return Ok(None);
            };

            current = next;
        }

        Ok(Some(current))
    }

    async fn finalize_storage_txn(&self, txn_id: StorageTxnId, commit: bool) -> TCResult<()> {
        if commit {
            self.root.commit(txn_id, true).await;
        } else {
            self.root.rollback(txn_id, true).await;
        }

        self.root.finalize(txn_id).await;
        Ok(())
    }
}

pub(crate) async fn load_library_root(root: PathBuf) -> TCResult<LibraryRoot> {
    let lib_root = root.join(LIB_ROOT);
    tokio::fs::create_dir_all(&lib_root).await.map_err(map_io)?;

    let cache = Cache::new(DEFAULT_CACHE_BYTES, Some(DEFAULT_CACHE_HANDLES));
    let canonical = cache.load(lib_root).map_err(map_io)?;
    let root = TxDir::load(BOOTSTRAP_TXN, canonical)
        .await
        .map_err(map_txfs)?;

    // Seal the loaded snapshot so subsequent transactions can stage writes without
    // conflicting against the bootstrap transaction.
    root.commit(BOOTSTRAP_TXN, true).await;
    root.finalize(BOOTSTRAP_TXN).await;

    Ok(root)
}

async fn discover_schemas(
    dir: FsDirLock<LibraryFile>,
    schemas: &mut Vec<LibrarySchema>,
) -> TCResult<()> {
    let mut pending = vec![dir];

    while let Some(current) = pending.pop() {
        let entries = {
            let guard = current.read().await;
            guard
                .iter()
                .map(|(name, entry)| match entry {
                    FsDirEntry::Dir(subdir) => (name.clone(), FsDirEntry::Dir(subdir.clone())),
                    FsDirEntry::File(file) => (name.clone(), FsDirEntry::File(file.clone())),
                })
                .collect::<Vec<_>>()
        };

        for (name, entry) in entries {
            match entry {
                FsDirEntry::Dir(subdir) if name != txfs::VERSIONS => pending.push(subdir),
                FsDirEntry::File(file) if name == SCHEMA_FILE => {
                    let guard = file.read::<LibraryFile>().await.map_err(map_io)?;
                    let schema = decode_schema_bytes(guard.bytes()).map_err(TCError::internal)?;
                    schemas.push(schema);
                }
                _ => {}
            }
        }
    }

    Ok(())
}

async fn read_canonical_file(
    dir: &FsDirLock<LibraryFile>,
    name: &str,
) -> TCResult<Option<Vec<u8>>> {
    let file = {
        let guard = dir.read().await;
        guard.get_file(name).cloned()
    };

    let Some(file) = file else {
        return Ok(None);
    };

    let guard = file.read::<LibraryFile>().await.map_err(map_io)?;
    Ok(Some(guard.bytes().to_vec()))
}

async fn write_file(
    dir: &LibraryRoot,
    version: StorageTxnId,
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

// Stored schema payloads are tiny (tens of bytes) and bounded. Using `serde_json`
// here avoids spinning up nested async executors inside storage/bootstrap adapters.
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::time::{SystemTime, UNIX_EPOCH};

    use pathlink::Link;

    use super::*;

    #[tokio::test]
    async fn committed_reads_do_not_create_txfs_versions() {
        let root = unique_temp_dir("tc-storage-committed-read");
        tokio::fs::create_dir_all(&root).await.expect("temp dir");

        let schema = LibrarySchema::new(
            Link::from_str("/lib/example-devco/storage-read/0.1.0").expect("schema link"),
            "0.1.0",
            vec![],
        );
        let root_dir = load_library_root(root.clone()).await.expect("library root");
        let store = LibraryStore::from_root(root_dir);
        let store = store.for_schema(&schema).await.expect("schema store");
        store
            .persist_artifact_immediate(
                &schema,
                &Artifact {
                    path: schema.id().to_string(),
                    content_type: IR_ARTIFACT_CONTENT_TYPE.to_string(),
                    bytes: b"{}".to_vec(),
                },
            )
            .await
            .expect("persist artifact");

        let before = txfs_version_entries(&root);
        let discovered = store.discover_schemas().await.expect("discover schemas");
        let artifact = store.load_artifact(&schema).await.expect("load artifact");
        let after = txfs_version_entries(&root);

        assert!(discovered.iter().any(|found| found.id() == schema.id()));
        assert!(artifact.is_some());
        assert_eq!(
            before, after,
            "committed reads must use canonical snapshots, not synthetic txfs read versions"
        );

        let _ = std::fs::remove_dir_all(root);
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{nanos}-{}", std::process::id()))
    }

    fn txfs_version_entries(root: &Path) -> Vec<String> {
        let mut entries = Vec::new();
        collect_txfs_version_entries(root, root, &mut entries);
        entries.sort();
        entries
    }

    fn collect_txfs_version_entries(root: &Path, path: &Path, entries: &mut Vec<String>) {
        let Ok(read_dir) = std::fs::read_dir(path) else {
            return;
        };

        for entry in read_dir.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().and_then(|name| name.to_str()) == Some(txfs::VERSIONS) {
                    collect_version_dir(root, &path, entries);
                } else {
                    collect_txfs_version_entries(root, &path, entries);
                }
            }
        }
    }

    fn collect_version_dir(root: &Path, path: &Path, entries: &mut Vec<String>) {
        let Ok(read_dir) = std::fs::read_dir(path) else {
            return;
        };

        for entry in read_dir.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_version_dir(root, &path, entries);
            } else if let Ok(relative) = path.strip_prefix(root) {
                entries.push(relative.display().to_string());
            }
        }
    }
}
