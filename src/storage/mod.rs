use std::{
    io,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::Workspace;
use crate::ir::{IR_ARTIFACT_CONTENT_TYPE, WASM_ARTIFACT_CONTENT_TYPE};
use freqfs::{Cache, DirLock};
use pathlink::Link;
use serde::{Deserialize, Serialize};
use tc_collection::PersistentFile;
use tc_error::{TCError, TCResult};
use tc_ir::{LibrarySchema, NetworkTime, TxnId};
use txfs::{Dir as TxDir, Id as TxName};

mod file;
mod txn_key;

pub(crate) use file::LibraryFile;
pub(crate) use txn_key::StorageTxnKey;

const LIB_ROOT: &str = "lib";
const SCHEMA_FILE: &str = "schema.json";
const WASM_FILE: &str = "library.wasm";
const IR_FILE: &str = "library.ir.json";
const BOOTSTRAP_VERSION: StorageTxnKey = StorageTxnKey::Bootstrap;

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
    maintenance: Arc<AtomicU64>,
}

pub(crate) type LibraryRoot = TxDir<StorageTxnKey, LibraryFile>;

/// Bootstrap-owned storage caches, one for each host filesystem root.
#[derive(Clone)]
pub struct HostStorage {
    workspace: Arc<Cache<PersistentFile>>,
    data: Arc<Cache<LibraryFile>>,
    library_maintenance: Arc<AtomicU64>,
}

impl HostStorage {
    pub fn new(limits: &crate::StorageLimits) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
            .unwrap_or(1);
        Self {
            workspace: Cache::new(
                limits.collection_cache_bytes,
                Some(limits.collection_file_handles),
                limits.minimum_free_disk_bytes,
                limits.cache_wait,
            ),
            data: Cache::new(
                limits.library_cache_bytes,
                Some(limits.library_file_handles),
                limits.minimum_free_disk_bytes,
                limits.cache_wait,
            ),
            library_maintenance: Arc::new(AtomicU64::new(now)),
        }
    }

    pub fn workspace(&self, path: impl AsRef<std::path::Path>) -> TCResult<Workspace> {
        std::fs::create_dir_all(path.as_ref()).map_err(map_io)?;
        let root = Arc::clone(&self.workspace)
            .load(path.as_ref().to_path_buf())
            .map_err(map_io)?;
        Ok(Workspace::from_root(root))
    }

    pub async fn library_store(&self, path: impl AsRef<std::path::Path>) -> TCResult<LibraryStore> {
        std::fs::create_dir_all(path.as_ref()).map_err(map_io)?;
        let data = Arc::clone(&self.data)
            .load(path.as_ref().to_path_buf())
            .map_err(map_io)?;
        let root = {
            let mut data = data.write().await;
            data.get_or_create_dir(LIB_ROOT.to_string())
                .map_err(map_io)?
        };
        Ok(LibraryStore::from_root(
            load_library_root(root).await?,
            Arc::clone(&self.library_maintenance),
        ))
    }
}

impl LibraryStore {
    pub(crate) fn from_root(root: LibraryRoot, maintenance: Arc<AtomicU64>) -> Self {
        Self {
            root,
            segments: Arc::new(Vec::new()),
            maintenance,
        }
    }

    fn maintenance_key(&self) -> StorageTxnKey {
        let timestamp = self.maintenance.fetch_add(1, Ordering::Relaxed) + 1;
        StorageTxnKey::Maintenance(NetworkTime::from_nanos(timestamp))
    }

    fn protocol_key(&self, txn_id: TxnId) -> StorageTxnKey {
        self.maintenance
            .fetch_max(txn_id.timestamp().as_nanos(), Ordering::Relaxed);
        StorageTxnKey::Protocol(txn_id)
    }

    pub async fn for_schema(&self, schema: &LibrarySchema) -> TCResult<Self> {
        let segments = library_segments(schema.id())?
            .into_iter()
            .map(|segment| parse_name(&segment))
            .collect::<TCResult<Vec<_>>>()?;

        Ok(Self {
            root: self.root.clone(),
            segments: Arc::new(segments),
            maintenance: Arc::clone(&self.maintenance),
        })
    }

    pub async fn discover_schemas(&self) -> TCResult<Vec<LibrarySchema>> {
        let mut schemas = Vec::new();
        let txn_id = self.maintenance_key();
        let discovered = discover_schemas(self.root.clone(), txn_id, &mut schemas).await;
        self.root.finalize(txn_id).await;
        discovered?;
        Ok(schemas)
    }
}

impl LibraryStore {
    pub async fn persist_schema_immediate(&self, schema: &LibrarySchema) -> TCResult<()> {
        // Top-level immediate installs still stage through txfs so disk writes
        // commit atomically. Do not call this from inside an active kernel txn.
        let txn_id = self.maintenance_key();
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
        self.persist_schema_at(self.protocol_key(txn_id), schema)
            .await
    }

    pub async fn persist_artifact_immediate(
        &self,
        schema: &LibrarySchema,
        artifact: &Artifact,
    ) -> TCResult<()> {
        // Top-level immediate installs still stage through txfs so disk writes
        // commit atomically. Do not call this from inside an active kernel txn.
        let txn_id = self.maintenance_key();
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
        self.persist_artifact_at(self.protocol_key(txn_id), schema, artifact)
            .await
    }

    pub async fn finalize_txn(&self, txn_id: TxnId, commit: bool) -> TCResult<()> {
        self.finalize_storage_txn(self.protocol_key(txn_id), commit)
            .await
    }

    pub async fn load_artifact(&self, schema: &LibrarySchema) -> TCResult<Option<Artifact>> {
        let txn_id = self.maintenance_key();
        let loaded = self.load_artifact_at(txn_id, schema).await;
        self.root.finalize(txn_id).await;
        loaded
    }

    async fn load_artifact_at(
        &self,
        txn_id: StorageTxnKey,
        schema: &LibrarySchema,
    ) -> TCResult<Option<Artifact>> {
        let Some(dir) = self.resolve_canonical_dir(txn_id).await? else {
            return Ok(None);
        };

        if let Some(schema_bytes) = read_canonical_file(&dir, txn_id, SCHEMA_FILE).await? {
            let stored = decode_schema_bytes(&schema_bytes).map_err(TCError::internal)?;
            if stored.id() != schema.id() || stored.version() != schema.version() {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        match read_canonical_file(&dir, txn_id, WASM_FILE).await? {
            Some(wasm_bytes) if !wasm_bytes.is_empty() => {
                return Ok(Some(Artifact {
                    path: schema.id().to_string(),
                    content_type: WASM_ARTIFACT_CONTENT_TYPE.to_string(),
                    bytes: wasm_bytes,
                }));
            }
            _ => {}
        }

        match read_canonical_file(&dir, txn_id, IR_FILE).await? {
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
        txn_id: StorageTxnKey,
        schema: &LibrarySchema,
    ) -> TCResult<()> {
        let bytes = encode_schema(schema).map_err(map_io_str)?;
        let dir = self.resolve_dir(txn_id, true).await?.expect("create dir");
        write_file(&dir, txn_id, SCHEMA_FILE, bytes).await
    }

    async fn persist_artifact_at(
        &self,
        txn_id: StorageTxnKey,
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
        txn_id: StorageTxnKey,
        schema: &LibrarySchema,
        wasm: &[u8],
    ) -> TCResult<()> {
        self.persist_schema_at(txn_id, schema).await?;
        let dir = self.resolve_dir(txn_id, true).await?.expect("create dir");
        write_file(&dir, txn_id, WASM_FILE, wasm.to_vec()).await
    }

    async fn persist_ir_library(
        &self,
        txn_id: StorageTxnKey,
        schema: &LibrarySchema,
        bytes: &[u8],
    ) -> TCResult<()> {
        self.persist_schema_at(txn_id, schema).await?;
        let dir = self.resolve_dir(txn_id, true).await?.expect("create dir");
        write_file(&dir, txn_id, IR_FILE, bytes.to_vec()).await
    }

    async fn resolve_dir(
        &self,
        txn_id: StorageTxnKey,
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

    async fn resolve_canonical_dir(&self, txn_id: StorageTxnKey) -> TCResult<Option<LibraryRoot>> {
        let mut current = self.root.clone();

        for segment in self.segments.iter() {
            let Some(next) = current.get_dir(txn_id, segment).await.map_err(map_txfs)? else {
                return Ok(None);
            };

            current = (*next).clone();
        }

        Ok(Some(current))
    }

    async fn finalize_storage_txn(&self, txn_id: StorageTxnKey, commit: bool) -> TCResult<()> {
        if commit {
            self.root.commit(txn_id, true).await;
        } else {
            self.root.rollback(txn_id, true).await;
        }

        self.root.finalize(txn_id).await;
        Ok(())
    }
}

pub(crate) async fn load_library_root(root: DirLock<LibraryFile>) -> TCResult<LibraryRoot> {
    let root = TxDir::load(BOOTSTRAP_VERSION, root)
        .await
        .map_err(map_txfs)?;

    // Seal the loaded snapshot so subsequent transactions can stage writes without
    // conflicting against the bootstrap transaction.
    root.commit(BOOTSTRAP_VERSION, true).await;
    root.finalize(BOOTSTRAP_VERSION).await;

    Ok(root)
}

async fn discover_schemas(
    dir: LibraryRoot,
    txn_id: StorageTxnKey,
    schemas: &mut Vec<LibrarySchema>,
) -> TCResult<()> {
    let mut pending = vec![dir];

    while let Some(current) = pending.pop() {
        let entries = current.iter(txn_id).await.map_err(map_txfs)?;

        for (name, entry) in entries {
            match &*entry {
                txfs::DirEntry::Dir(subdir) if name.to_string() != txfs::VERSIONS => {
                    pending.push(subdir.clone());
                }
                txfs::DirEntry::File(file) if name.to_string() == SCHEMA_FILE => {
                    let guard = file.read::<LibraryFile>(txn_id).await.map_err(map_txfs)?;
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
    dir: &LibraryRoot,
    txn_id: StorageTxnKey,
    name: &str,
) -> TCResult<Option<Vec<u8>>> {
    let name = parse_name(name)?;
    let Some(file) = dir.get_file(txn_id, &name).await.map_err(map_txfs)? else {
        return Ok(None);
    };

    let guard = file.read::<LibraryFile>(txn_id).await.map_err(map_txfs)?;
    Ok(Some(guard.bytes().to_vec()))
}

async fn write_file(
    dir: &LibraryRoot,
    version: StorageTxnKey,
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
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::path::{Path, PathBuf};
    use std::str::FromStr;
    use std::time::{SystemTime, UNIX_EPOCH};

    use pathlink::Link;
    use tc_ir::NetworkTime;

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
        let store = HostStorage::new(&crate::HostLimits::default().storage)
            .library_store(root.clone())
            .await
            .expect("library root");
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

    #[tokio::test]
    async fn maintenance_versions_are_owned_by_one_host_storage() {
        let root = unique_temp_dir("tc-storage-maintenance-version");
        tokio::fs::create_dir_all(&root).await.expect("temp dir");

        let storage = HostStorage::new(&crate::HostLimits::default().storage);
        let store = storage
            .library_store(root.clone())
            .await
            .expect("library root");
        let schema = LibrarySchema::new(
            Link::from_str("/lib/example-devco/maintenance/0.1.0").expect("schema link"),
            "0.1.0",
            vec![],
        );
        let child = store.for_schema(&schema).await.expect("schema store");

        let first = store.maintenance_key();
        let second = child.maintenance_key();
        assert!(first < second);
        assert_eq!(first.to_string().parse::<StorageTxnKey>(), Ok(first));
        assert_eq!(second.to_string().parse::<StorageTxnKey>(), Ok(second));

        let observed = match second {
            StorageTxnKey::Maintenance(timestamp) => timestamp.as_nanos(),
            _ => unreachable!("maintenance_key must return a maintenance version"),
        };
        let protocol = TxnId::from_parts(NetworkTime::from_nanos(observed + 10), 1);
        let protocol_key = store.protocol_key(protocol);
        let next = child.maintenance_key();
        assert!(protocol_key < next);
        assert_eq!(protocol_key, StorageTxnKey::Protocol(protocol));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn storage_txn_key_uses_full_txn_id_identity() {
        let a = StorageTxnKey::Protocol(
            TxnId::from_parts(NetworkTime::from_nanos(42), 7).with_trace([1u8; 32]),
        );
        let b = StorageTxnKey::Protocol(
            TxnId::from_parts(NetworkTime::from_nanos(42), 7).with_trace([2u8; 32]),
        );

        assert_ne!(
            a, b,
            "trace bytes are part of canonical transaction identity"
        );

        let mut hasher_a = DefaultHasher::new();
        a.hash(&mut hasher_a);

        let mut hasher_b = DefaultHasher::new();
        b.hash(&mut hasher_b);

        assert_ne!(
            hasher_a.finish(),
            hasher_b.finish(),
            "hash must include full TxnId identity"
        );
    }

    #[test]
    fn storage_txn_key_text_preserves_version_order() {
        let protocol = StorageTxnKey::Protocol(
            TxnId::from_parts(NetworkTime::from_nanos(42), 7).with_trace([1u8; 32]),
        );
        let versions = [
            StorageTxnKey::Maintenance(NetworkTime::from_nanos(42)),
            StorageTxnKey::Bootstrap,
            StorageTxnKey::Maintenance(NetworkTime::from_nanos(43)),
            protocol,
        ];

        let mut semantic = versions;
        semantic.sort();
        let mut rendered = versions.map(|version| version.to_string());
        rendered.sort();

        assert_eq!(
            rendered,
            semantic.map(|version| version.to_string()),
            "freqfs filename order must match StorageTxnKey order"
        );
        assert_eq!(protocol.to_string().parse(), Ok(protocol));
    }

    #[test]
    fn production_cache_construction_is_bootstrap_owned() {
        let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        for path in [
            "src/workspace.rs",
            "src/http/config.rs",
            "src/library/http.rs",
            "src/pyo3_runtime/kernel.rs",
        ] {
            let source = std::fs::read_to_string(manifest.join(path)).expect("read source");
            let cache_constructor = ["Cache", "::new("].concat();
            assert!(
                !source.contains(&cache_constructor),
                "{path} must receive bootstrap-owned storage, not construct a cache"
            );
        }

        let source = std::fs::read_to_string(manifest.join("src/storage/mod.rs"))
            .expect("read storage source");
        let cache_constructor = ["Cache", "::new("].concat();
        assert_eq!(
            source.matches(&cache_constructor).count(),
            2,
            "HostStorage must remain the sole production cache owner"
        );
        assert!(source.contains("workspace: Arc<Cache<PersistentFile>>"));
        assert!(source.contains("data: Arc<Cache<LibraryFile>>"));
        assert!(source.contains("Arc::clone(&self.workspace)"));
        assert!(source.contains("Arc::clone(&self.data)"));
        assert!(source.contains("get_or_create_dir(LIB_ROOT.to_string())"));

        let key_source = std::fs::read_to_string(manifest.join("src/storage/txn_key.rs"))
            .expect("read storage key source");
        assert!(!key_source.contains("static IMMEDIATE"));
        assert!(!key_source.contains("Atomic"));
        assert!(!key_source.contains("SystemTime"));
        assert!(!key_source.contains("from_parts"));
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
