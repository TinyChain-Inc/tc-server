use std::{
    collections::BTreeMap,
    str::FromStr,
    sync::{Arc, RwLock},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use destream::de;
use pathlink::{Link, PathSegment};
use serde::{Deserialize, Serialize};
use tc_error::{TCError, TCResult};
use tc_ir::{
    Dir, HandleDelete, HandleGet, HandlePost, HandlePut, Id, Library, LibraryModule, LibrarySchema,
    Map, Route,
};
use tc_value::Value;
use umask::Mode;

use crate::{
    KernelHandler,
    storage::{Artifact, LibraryStore, decode_schema_bytes},
    txn::TxnHandle,
    uri,
};

type HandlerArc = Arc<dyn KernelHandler>;
type LibraryFactory =
    Arc<dyn Fn(Vec<u8>) -> TCResult<(LibrarySchema, SchemaRoutes, HandlerArc)> + Send + Sync>;

#[derive(Clone, Debug, Default)]
pub struct RouteMetadata {
    pub export: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct SchemaRoutes {
    dir: Dir<RouteMetadata>,
}

impl SchemaRoutes {
    pub fn new() -> Self {
        Self { dir: Dir::new() }
    }

    pub fn from_entries(entries: Vec<(Vec<PathSegment>, RouteMetadata)>) -> TCResult<Self> {
        let dir = Dir::from_routes(entries)?;
        Ok(Self { dir })
    }
}

impl Route for SchemaRoutes {
    type Handler = RouteMetadata;

    fn route<'a>(&'a self, path: &'a [PathSegment]) -> Option<&'a Self::Handler> {
        self.dir.route(path)
    }
}

#[derive(Clone)]
pub struct LibraryRegistry {
    entries: Arc<RwLock<BTreeMap<String, Arc<LibraryRuntime>>>>,
    store: Option<LibraryStore>,
    factories: BTreeMap<String, LibraryFactory>,
}

impl LibraryRegistry {
    pub fn new(store: Option<LibraryStore>, factories: BTreeMap<String, LibraryFactory>) -> Self {
        Self {
            entries: Arc::new(RwLock::new(BTreeMap::new())),
            store,
            factories,
        }
    }

    pub async fn insert_schema(&self, schema: LibrarySchema) -> TCResult<()> {
        let key = canonical_link(schema.id());
        let store = match self.store.as_ref() {
            Some(store) => Some(store.for_schema(&schema).await?),
            None => None,
        };
        let runtime = Arc::new(LibraryRuntime::new(schema, store, self.factories.clone()));
        self.entries
            .write()
            .expect("library registry write lock")
            .insert(key, runtime);

        Ok(())
    }

    pub fn list_dir(&self, path: &str) -> Option<Map<bool>> {
        let path = normalize_path(path);
        let entries = self.entries.read().expect("library registry read lock");
        let mut out = Map::new();
        let mut has_match = path == uri::LIB_ROOT;

        for (id, _) in entries.iter() {
            if !is_path_prefix(&path, id) {
                continue;
            }

            has_match = true;

            if path == *id {
                continue;
            }

            let rest = id.strip_prefix(&path).unwrap_or(id).trim_start_matches('/');

            if rest.is_empty() {
                continue;
            }

            let mut segments = rest.split('/');
            let child = segments.next().expect("non-empty rest segment");
            let is_dir = segments.next().is_some();
            let Ok(child_id) = child.parse::<Id>() else {
                continue;
            };
            let entry = out.entry(child_id).or_insert(is_dir);
            if is_dir {
                *entry = true;
            }
        }

        if has_match { Some(out) } else { None }
    }

    pub fn resolve_runtime_for_path(&self, path: &str) -> Option<(Arc<LibraryRuntime>, bool)> {
        let path = normalize_path(path);
        let entries = self.entries.read().expect("library registry read lock");
        let mut best: Option<(&String, Arc<LibraryRuntime>)> = None;

        for (id, runtime) in entries.iter() {
            if !is_path_prefix(id, &path) {
                continue;
            }

            let replace = match &best {
                Some((best_id, _)) => id.len() > best_id.len(),
                None => true,
            };

            if replace {
                best = Some((id, Arc::clone(runtime)));
            }
        }

        best.map(|(id, runtime)| (runtime, id == &path))
    }

    pub fn schema_for_txn(&self, txn: &TxnHandle) -> TCResult<LibrarySchema> {
        let mut best: Option<(usize, LibrarySchema)> = None;

        for claim in txn.claims().iter().chain(std::iter::once(txn.claim())) {
            let path = canonical_link(&claim.link);
            if let Some((runtime, _)) = self.resolve_runtime_for_path(&path) {
                let schema = runtime.state.schema();
                let score = schema.id().to_string().len();
                let replace = best.as_ref().is_none_or(|(len, _)| score > *len);
                if replace {
                    best = Some((score, schema));
                }
            }
        }

        if let Some((_, schema)) = best {
            return Ok(schema);
        }

        let entries = self.entries.read().expect("library registry read lock");
        if entries.len() == 1 {
            let schema = entries
                .values()
                .next()
                .expect("single entry")
                .state
                .schema();
            return Ok(schema);
        }

        Err(TCError::unauthorized(
            "no library manifest loaded (egress is default-deny)",
        ))
    }

    pub async fn install_schema(&self, schema: LibrarySchema) -> Result<(), InstallError> {
        let runtime = self
            .runtime_for_schema(&schema)
            .await
            .map_err(|err| InstallError::internal(err.to_string()))?;
        runtime.state.replace_schema(schema);
        runtime.routes.clear();

        if let Some(store) = runtime.store.as_ref() {
            store
                .persist_schema(&runtime.state.schema())
                .await
                .map_err(|err| InstallError::internal(err.to_string()))?;
        }

        Ok(())
    }

    pub async fn install_payload(&self, payload: InstallArtifacts) -> Result<(), InstallError> {
        let runtime = self
            .runtime_for_schema(&payload.schema)
            .await
            .map_err(|err| InstallError::internal(err.to_string()))?;

        let artifact = payload
            .artifacts
            .into_iter()
            .find(|artifact| self.factories.contains_key(&artifact.content_type))
            .ok_or_else(|| {
                let supported = self
                    .factories
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ");
                InstallError::bad_request(format!(
                    "missing supported artifact (expected one of: {supported})"
                ))
            })?;

        let factory = self
            .factories
            .get(&artifact.content_type)
            .ok_or_else(|| InstallError::bad_request("unsupported artifact content type"))?;

        let (manifest_schema, schema_routes, handler) = factory(artifact.bytes.clone())
            .map_err(|err| InstallError::internal(err.to_string()))?;

        if !schemas_equivalent(&manifest_schema, &payload.schema) {
            return Err(InstallError::bad_request(
                "manifest schema does not match descriptor",
            ));
        }

        runtime
            .state
            .replace_with_routes(manifest_schema.clone(), schema_routes);
        runtime.routes.replace_arc(handler);

        if let Some(store) = runtime.store.as_ref() {
            store
                .persist_artifact(
                    &manifest_schema,
                    &Artifact {
                        content_type: artifact.content_type,
                        bytes: artifact.bytes,
                        path: artifact.path,
                    },
                )
                .await
                .map_err(|err| InstallError::internal(err.to_string()))?;
        }

        Ok(())
    }

    pub async fn export_payload_for_claims(
        &self,
        txn: &TxnHandle,
    ) -> Result<Option<InstallArtifacts>, TCError> {
        let runtimes = {
            let entries = self.entries.read().expect("library registry read lock");
            entries.values().cloned().collect::<Vec<_>>()
        };

        for runtime in runtimes {
            let schema = runtime.state.schema();
            if txn.has_claim(schema.id(), umask::USER_READ) {
                let Some(store) = &runtime.store else {
                    return Ok(None);
                };
                let artifact = match store.load_artifact(&schema).await? {
                    Some(artifact) => artifact,
                    None => return Ok(None),
                };

                let artifacts = vec![Artifact {
                    path: schema.id().to_string(),
                    content_type: artifact.content_type,
                    bytes: artifact.bytes,
                }];

                return Ok(Some(InstallArtifacts { schema, artifacts }));
            }
        }

        Err(TCError::unauthorized("unauthorized"))
    }

    pub async fn hydrate_from_storage(&self) -> TCResult<()> {
        let store = match &self.store {
            Some(store) => store,
            None => return Ok(()),
        };

        let entries = store.discover_schemas().await?;
        for schema in entries {
            let runtime = self.runtime_for_schema(&schema).await?;
            runtime.hydrate_from_storage().await?;
        }

        Ok(())
    }

    async fn runtime_for_schema(&self, schema: &LibrarySchema) -> TCResult<Arc<LibraryRuntime>> {
        let key = canonical_link(schema.id());
        if let Some(existing) = self
            .entries
            .read()
            .expect("library registry read lock")
            .get(&key)
            .cloned()
        {
            return Ok(existing);
        }

        let store = match self.store.as_ref() {
            Some(store) => Some(store.for_schema(schema).await?),
            None => None,
        };
        let runtime = Arc::new(LibraryRuntime::new(
            schema.clone(),
            store,
            self.factories.clone(),
        ));

        let mut entries = self.entries.write().expect("library registry write lock");
        let entry = entries.entry(key).or_insert_with(|| Arc::clone(&runtime));
        Ok(Arc::clone(entry))
    }
}

#[derive(Clone)]
pub struct LibraryState {
    inner: Arc<RwLock<LibraryModule<TxnHandle, SchemaRoutes>>>,
}

impl LibraryState {
    pub fn new(initial: LibrarySchema) -> Self {
        Self {
            inner: Arc::new(RwLock::new(LibraryModule::new(
                initial,
                SchemaRoutes::new(),
            ))),
        }
    }

    pub fn schema(&self) -> LibrarySchema {
        self.inner
            .read()
            .expect("library schema read lock")
            .schema()
            .clone()
    }

    pub fn replace_schema(&self, schema: LibrarySchema) {
        *self.inner.write().expect("library schema write lock") =
            LibraryModule::new(schema, SchemaRoutes::new());
    }

    pub fn replace_with_routes(&self, schema: LibrarySchema, routes: SchemaRoutes) {
        *self.inner.write().expect("library schema write lock") =
            LibraryModule::new(schema, routes);
    }
}

/// Default schema used when no persisted library is present on disk yet.
pub fn default_library_schema() -> LibrarySchema {
    LibrarySchema::new(
        Link::from_str("/lib/default").expect("default library link"),
        "0.0.0",
        vec![],
    )
}

/// Decoded library install payload.
pub enum InstallRequest {
    SchemaOnly(LibrarySchema),
    WithArtifacts(InstallArtifacts),
}

pub struct InstallArtifacts {
    pub schema: LibrarySchema,
    pub artifacts: Vec<Artifact>,
}

impl de::FromStream for Artifact {
    type Context = ();

    async fn from_stream<D: de::Decoder>(
        _context: Self::Context,
        decoder: &mut D,
    ) -> Result<Self, D::Error> {
        struct ArtifactVisitor;

        impl de::Visitor for ArtifactVisitor {
            type Value = Artifact;

            fn expecting() -> &'static str {
                "an install artifact map"
            }

            async fn visit_map<A: de::MapAccess>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut path = None;
                let mut content_type = None;
                let mut bytes = None;

                while let Some(key) = map.next_key::<String>(()).await? {
                    match key.as_str() {
                        "path" => path = Some(map.next_value(()).await?),
                        "content_type" => content_type = Some(map.next_value(()).await?),
                        "bytes" => bytes = Some(map.next_value::<String>(()).await?),
                        _ => {
                            let _ = map.next_value::<de::IgnoredAny>(()).await?;
                        }
                    }
                }

                let path =
                    path.ok_or_else(|| de::Error::custom("install artifact missing path field"))?;
                let content_type = content_type.ok_or_else(|| {
                    de::Error::custom("install artifact missing content_type field")
                })?;
                let bytes = bytes
                    .ok_or_else(|| de::Error::custom("install artifact missing bytes field"))?;
                let bytes = BASE64
                    .decode(bytes.as_bytes())
                    .map_err(|err| de::Error::custom(format!("invalid base64 artifact: {err}")))?;

                Ok(Artifact {
                    path,
                    content_type,
                    bytes,
                })
            }
        }

        decoder.decode_map(ArtifactVisitor).await
    }
}

#[derive(Debug)]
pub enum InstallError {
    BadRequest(String),
    Internal(String),
}

impl InstallError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }

    pub fn message(&self) -> &str {
        match self {
            Self::BadRequest(msg) | Self::Internal(msg) => msg,
        }
    }

    pub fn is_bad_request(&self) -> bool {
        matches!(self, Self::BadRequest(_))
    }
}

pub fn decode_install_request_bytes(bytes: &[u8]) -> Result<InstallRequest, InstallError> {
    if bytes.iter().all(|b| b.is_ascii_whitespace()) {
        return Err(InstallError::bad_request("empty install payload"));
    }

    if let Ok(schema) = decode_schema_bytes(bytes) {
        return Ok(InstallRequest::SchemaOnly(schema));
    }

    let payload = decode_install_payload(bytes)?;

    Ok(InstallRequest::WithArtifacts(payload))
}

pub fn encode_install_payload_bytes(payload: &InstallArtifacts) -> Result<Vec<u8>, String> {
    #[derive(Serialize)]
    struct Payload {
        schema: RawSchema,
        #[serde(default)]
        artifacts: Vec<InstallArtifactRaw>,
    }

    let raw = Payload {
        schema: RawSchema::from(&payload.schema),
        artifacts: payload
            .artifacts
            .iter()
            .map(InstallArtifactRaw::from)
            .collect(),
    };

    serde_json::to_vec(&raw).map_err(|err| err.to_string())
}

fn schemas_equivalent(left: &LibrarySchema, right: &LibrarySchema) -> bool {
    left.version() == right.version()
        && canonical_link(left.id()) == canonical_link(right.id())
        && canonical_links(left.dependencies()) == canonical_links(right.dependencies())
}

fn canonical_links(links: &[Link]) -> Vec<String> {
    let mut out = links.iter().map(canonical_link).collect::<Vec<_>>();
    out.sort();
    out
}

fn canonical_link(link: &Link) -> String {
    let raw = link.to_string();
    if raw.starts_with('/') {
        return normalize_path(&raw);
    }

    // Accept both path-only and absolute link string forms by comparing on the path suffix.
    // This keeps the `/lib` install payload stable even if a `Link` string round-trip adds a
    // scheme or authority prefix.
    match raw.split_once("://") {
        Some((_, rest)) => rest
            .find('/')
            .map(|idx| normalize_path(&rest[idx..]))
            .unwrap_or_else(|| normalize_path(&raw)),
        None => normalize_path(&raw),
    }
}

fn normalize_path(path: &str) -> String {
    uri::normalize_path(path).to_string()
}

fn is_path_prefix(prefix: &str, path: &str) -> bool {
    let prefix = normalize_path(prefix);
    let path = normalize_path(path);
    path == prefix || path.starts_with(&format!("{prefix}/"))
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
        let id = Link::from_str(&raw.id).map_err(|err| format!("invalid schema id: {err}"))?;
        let dependencies = raw
            .dependencies
            .into_iter()
            .map(|dep| {
                Link::from_str(&dep).map_err(|err| format!("invalid dependency link: {err}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(LibrarySchema::new(id, raw.version, dependencies))
    }
}

impl From<&LibrarySchema> for RawSchema {
    fn from(schema: &LibrarySchema) -> Self {
        Self {
            id: schema.id().to_string(),
            version: schema.version().to_string(),
            dependencies: schema
                .dependencies()
                .iter()
                .map(|dep| dep.to_string())
                .collect(),
        }
    }
}

#[derive(Deserialize, Serialize)]
struct InstallArtifactRaw {
    path: String,
    content_type: String,
    bytes: String,
}

impl From<&Artifact> for InstallArtifactRaw {
    fn from(artifact: &Artifact) -> Self {
        Self {
            path: artifact.path.clone(),
            content_type: artifact.content_type.clone(),
            bytes: BASE64.encode(&artifact.bytes),
        }
    }
}

fn decode_install_payload(bytes: &[u8]) -> Result<InstallArtifacts, InstallError> {
    #[derive(Deserialize)]
    struct Payload {
        schema: RawSchema,
        #[serde(default)]
        artifacts: Vec<InstallArtifactRaw>,
    }

    let payload: Payload = serde_json::from_slice(bytes)
        .map_err(|err| InstallError::bad_request(format!("invalid install payload json: {err}")))?;

    let schema = payload
        .schema
        .try_into()
        .map_err(InstallError::bad_request)?;

    let artifacts = payload
        .artifacts
        .into_iter()
        .map(|artifact| {
            let bytes = BASE64.decode(artifact.bytes.as_bytes()).map_err(|err| {
                InstallError::bad_request(format!("invalid base64 artifact: {err}"))
            })?;
            Ok(Artifact {
                path: artifact.path,
                content_type: artifact.content_type,
                bytes,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(InstallArtifacts { schema, artifacts })
}

pub struct LibraryRoutes {
    inner: Arc<RwLock<Option<HandlerArc>>>,
}

impl LibraryRoutes {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(None)),
        }
    }

    pub fn replace<H>(&self, handler: H)
    where
        H: KernelHandler,
    {
        self.replace_arc(Arc::new(handler));
    }

    pub fn replace_arc(&self, handler: HandlerArc) {
        self.inner
            .write()
            .expect("library routes write lock")
            .replace(handler);
    }

    pub fn clear(&self) {
        self.inner
            .write()
            .expect("library routes write lock")
            .take();
    }

    pub fn current_handler(&self) -> Option<HandlerArc> {
        self.inner.read().expect("library routes read lock").clone()
    }
}

impl Default for LibraryRoutes {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for LibraryRoutes {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

pub struct LibraryHandlers {
    get: HandlerArc,
    put: HandlerArc,
    route: Option<HandlerArc>,
}

impl LibraryHandlers {
    pub fn with_route<G, P, R>(get: G, put: P, route: R) -> Self
    where
        G: KernelHandler,
        P: KernelHandler,
        R: KernelHandler,
    {
        Self {
            get: Arc::new(get),
            put: Arc::new(put),
            route: Some(Arc::new(route)),
        }
    }

    pub fn without_route<G, P>(get: G, put: P) -> Self
    where
        G: KernelHandler,
        P: KernelHandler,
    {
        Self {
            get: Arc::new(get),
            put: Arc::new(put),
            route: None,
        }
    }

    pub fn get_handler(&self) -> HandlerArc {
        Arc::clone(&self.get)
    }

    pub fn put_handler(&self) -> HandlerArc {
        Arc::clone(&self.put)
    }

    pub fn route_handler(&self) -> Option<HandlerArc> {
        self.route.as_ref().map(Arc::clone)
    }
}

impl Clone for LibraryHandlers {
    fn clone(&self) -> Self {
        Self {
            get: Arc::clone(&self.get),
            put: Arc::clone(&self.put),
            route: self.route.as_ref().map(Arc::clone),
        }
    }
}

#[cfg(any(feature = "http-server", feature = "pyo3"))]
#[derive(Clone)]
pub struct NativeLibrary<H> {
    schema: LibrarySchema,
    routes: Arc<Dir<H>>,
}

#[cfg(any(feature = "http-server", feature = "pyo3"))]
impl<H> NativeLibrary<H>
where
    H: Clone,
{
    pub fn new(module: LibraryModule<TxnHandle, Dir<H>>) -> Self {
        Self {
            schema: module.schema().clone(),
            routes: Arc::new(module.routes().clone()),
        }
    }

    pub fn schema(&self) -> &LibrarySchema {
        &self.schema
    }

    pub fn routes(&self) -> Arc<Dir<H>> {
        Arc::clone(&self.routes)
    }
}

#[cfg(any(feature = "http-server", feature = "pyo3"))]
pub trait NativeLibraryHandler: HandleGet<TxnHandle, Request = Value, RequestContext = (), Response = Value, Error = TCError>
    + HandlePut<TxnHandle, Request = Value, RequestContext = (), Response = Value, Error = TCError>
    + HandlePost<TxnHandle, Request = Value, RequestContext = (), Response = Value, Error = TCError>
    + HandleDelete<TxnHandle, Request = Value, RequestContext = (), Response = Value, Error = TCError>
    + Clone
    + Send
    + Sync
    + 'static
{
}

#[cfg(any(feature = "http-server", feature = "pyo3"))]
impl<T> NativeLibraryHandler for T where
    T: HandleGet<
            TxnHandle,
            Request = Value,
            RequestContext = (),
            Response = Value,
            Error = TCError,
        > + HandlePut<
            TxnHandle,
            Request = Value,
            RequestContext = (),
            Response = Value,
            Error = TCError,
        > + HandlePost<
            TxnHandle,
            Request = Value,
            RequestContext = (),
            Response = Value,
            Error = TCError,
        > + HandleDelete<
            TxnHandle,
            Request = Value,
            RequestContext = (),
            Response = Value,
            Error = TCError,
        > + Clone
        + Send
        + Sync
        + 'static
{
}

pub struct LibraryRuntime {
    state: LibraryState,
    routes: LibraryRoutes,
    store: Option<LibraryStore>,
    factories: BTreeMap<String, LibraryFactory>,
}

impl LibraryRuntime {
    pub fn new(
        initial_schema: LibrarySchema,
        store: Option<LibraryStore>,
        factories: BTreeMap<String, LibraryFactory>,
    ) -> Self {
        Self {
            state: LibraryState::new(initial_schema),
            routes: LibraryRoutes::new(),
            store,
            factories,
        }
    }

    pub async fn hydrate_from_storage(&self) -> TCResult<()> {
        let store = match &self.store {
            Some(store) => store,
            None => return Ok(()),
        };

        let schema = self.state.schema();
        let artifact = match store.load_artifact(&schema).await? {
            Some(artifact) => artifact,
            None => return Ok(()),
        };

        let factory = match self.factories.get(&artifact.content_type) {
            Some(factory) => Arc::clone(factory),
            None => return Ok(()),
        };
        let (manifest_schema, schema_routes, handler) = factory(artifact.bytes)?;

        if manifest_schema != schema {
            return Err(TCError::internal("persisted schema mismatch"));
        }

        self.state
            .replace_with_routes(manifest_schema, schema_routes);
        self.routes.replace_arc(handler);

        Ok(())
    }

    pub async fn install_artifact_bytes(
        &self,
        content_type: &str,
        bytes: Vec<u8>,
        txn: Option<&crate::txn::TxnHandle>,
    ) -> TCResult<LibrarySchema> {
        let factory = self
            .factories
            .get(content_type)
            .ok_or_else(|| TCError::bad_request("artifact installs are not supported"))?;

        let (manifest_schema, schema_routes, handler) = factory(bytes.clone())?;

        match txn {
            Some(txn)
                if !txn.has_claim(
                    manifest_schema.id(),
                    Mode::new().with_class_perm(umask::USER, umask::WRITE),
                ) =>
            {
                return Err(TCError::unauthorized("unauthorized library install"));
            }
            _ => {}
        }

        self.state
            .replace_with_routes(manifest_schema.clone(), schema_routes);
        self.routes.replace_arc(handler);

        if let Some(store) = &self.store {
            store
                .persist_artifact(
                    &manifest_schema,
                    &Artifact {
                        content_type: content_type.to_string(),
                        bytes,
                        path: manifest_schema.id().to_string(),
                    },
                )
                .await?;
        }

        Ok(manifest_schema)
    }
}

impl Clone for LibraryRuntime {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            routes: self.routes.clone(),
            store: self.store.clone(),
            factories: self.factories.clone(),
        }
    }
}

#[cfg(any(feature = "http-server", feature = "pyo3"))]
pub mod http {
    use std::{collections::BTreeMap, io, path::PathBuf, sync::Arc};

    use crate::http::{Body, Request, Response, StatusCode, header};
    use futures::{FutureExt, TryStreamExt};
    use hyper::body;
    use number_general::Number;
    use tc_error::TCResult;
    use tc_ir::{LibrarySchema, Map};
    use tc_state::State;
    use tc_value::Value;

    use crate::{
        KernelHandler,
        ir::{
            IR_ARTIFACT_CONTENT_TYPE, WASM_ARTIFACT_CONTENT_TYPE, http_ir_route_handler_from_bytes,
        },
        library::{
            InstallError, InstallRequest, LibraryFactory, LibraryHandlers, LibraryRegistry,
            decode_install_request_bytes,
        },
        storage::LibraryStore,
        wasm::http_wasm_route_handler_from_bytes,
    };

    pub async fn build_http_library_module(
        initial_schema: LibrarySchema,
        storage_root: Option<PathBuf>,
    ) -> TCResult<Arc<LibraryRegistry>> {
        let store = match storage_root {
            Some(root) => {
                let root_dir = crate::storage::load_library_root(root).await?;
                Some(LibraryStore::from_root(root_dir))
            }
            None => None,
        };
        build_http_library_module_with_store(initial_schema, store).await
    }

    pub async fn build_http_library_module_with_store(
        initial_schema: LibrarySchema,
        store: Option<LibraryStore>,
    ) -> TCResult<Arc<LibraryRegistry>> {
        let wasm_factory: LibraryFactory = Arc::new(|bytes: Vec<u8>| {
            let (handler, schema, schema_routes) = http_wasm_route_handler_from_bytes(bytes)?;
            let handler: Arc<dyn KernelHandler> = Arc::new(handler);
            Ok((schema, schema_routes, handler))
        });

        let ir_factory: LibraryFactory = Arc::new(|bytes: Vec<u8>| {
            let (handler, schema, schema_routes) = http_ir_route_handler_from_bytes(bytes)?;
            let handler: Arc<dyn KernelHandler> = Arc::new(handler);
            Ok((schema, schema_routes, handler))
        });

        let registry = LibraryRegistry::new(
            store,
            BTreeMap::from([
                (WASM_ARTIFACT_CONTENT_TYPE.to_string(), wasm_factory),
                (IR_ARTIFACT_CONTENT_TYPE.to_string(), ir_factory),
            ]),
        );
        registry.insert_schema(initial_schema).await?;
        Ok(Arc::new(registry))
    }

    pub fn http_library_handlers(module: &Arc<LibraryRegistry>) -> LibraryHandlers {
        let get = schema_get_handler(Arc::clone(module));
        let put = schema_put_handler(Arc::clone(module));
        let route = routes_handler(Arc::clone(module));
        LibraryHandlers::with_route(get, put, route)
    }

    pub fn schema_get_handler(registry: Arc<LibraryRegistry>) -> impl KernelHandler {
        move |_req: Request| {
            let registry = Arc::clone(&registry);
            async move { respond_with_listing(registry.list_dir(crate::uri::LIB_ROOT)) }.boxed()
        }
    }

    pub fn schema_put_handler(registry: Arc<LibraryRegistry>) -> impl KernelHandler {
        move |req: Request| {
            let registry = Arc::clone(&registry);
            let txn = req.extensions().get::<crate::txn::TxnHandle>().cloned();
            async move {
                let txn = match txn {
                    Some(txn) => txn,
                    None => return unauthorized_response("missing transaction context"),
                };

                match decode_body(req).await {
                    Ok(InstallRequest::SchemaOnly(schema)) => {
                        if !txn.has_claim(schema.id(), umask::USER_WRITE) {
                            return unauthorized_response("unauthorized library install");
                        }
                        match registry.install_schema(schema).await {
                            Ok(()) => no_content_response(),
                            Err(err) => install_error_response(err),
                        }
                    }
                    Ok(InstallRequest::WithArtifacts(payload)) => {
                        if !txn.has_claim(payload.schema.id(), umask::USER_WRITE) {
                            return unauthorized_response("unauthorized library install");
                        }
                        match registry.install_payload(payload).await {
                            Ok(()) => no_content_response(),
                            Err(err) => install_error_response(err),
                        }
                    }
                    Err(err) => install_error_response(err),
                }
            }
            .boxed()
        }
    }

    fn respond_with_schema(schema: LibrarySchema) -> Response {
        let state = schema_to_state(&schema);
        match destream_json::encode(state) {
            Ok(stream) => {
                let body =
                    Body::wrap_stream(stream.map_err(|err| io::Error::other(err.to_string())));
                http::Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(body)
                    .expect("library schema response")
            }
            Err(err) => internal_error(err.to_string()),
        }
    }

    fn respond_with_listing(listing: Option<tc_ir::Map<bool>>) -> Response {
        let Some(listing) = listing else {
            return http::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .expect("dir not found response");
        };

        let state = listing_to_state(&listing);
        match destream_json::encode(state) {
            Ok(stream) => {
                let body =
                    Body::wrap_stream(stream.map_err(|err| io::Error::other(err.to_string())));
                http::Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(body)
                    .expect("dir listing response")
            }
            Err(err) => internal_error(err.to_string()),
        }
    }

    fn schema_to_state(schema: &LibrarySchema) -> State {
        let dependencies = schema
            .dependencies()
            .iter()
            .enumerate()
            .map(|(idx, dep)| {
                (
                    idx.to_string().parse().expect("Id"),
                    State::from(Value::from(dep.to_string())),
                )
            })
            .collect::<Map<State>>();

        let mut map = Map::new();
        map.insert(
            "id".parse().expect("Id"),
            State::from(Value::from(schema.id().to_string())),
        );
        map.insert(
            "version".parse().expect("Id"),
            State::from(Value::from(schema.version().to_string())),
        );
        map.insert("dependencies".parse().expect("Id"), State::Map(dependencies));
        State::Map(map)
    }

    fn listing_to_state(listing: &tc_ir::Map<bool>) -> State {
        let map = listing
            .iter()
            .map(|(name, is_dir)| {
                (
                    name.clone(),
                    State::from(Value::from(Number::from(*is_dir))),
                )
            })
            .collect::<Map<State>>();
        State::Map(map)
    }

    async fn decode_body(req: Request) -> Result<InstallRequest, InstallError> {
        let body_bytes = body::to_bytes(req.into_body())
            .await
            .map_err(|err| InstallError::internal(err.to_string()))?;
        decode_install_request_bytes(&body_bytes)
    }

    fn bad_request(message: String) -> Response {
        http::Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from(message))
            .expect("bad request response")
    }

    fn internal_error(message: String) -> Response {
        http::Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(message))
            .expect("internal error response")
    }

    fn unauthorized_response(message: impl Into<String>) -> Response {
        http::Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body(Body::from(message.into()))
            .expect("unauthorized response")
    }

    fn no_content_response() -> Response {
        http::Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .expect("library install response")
    }

    fn install_error_response(error: InstallError) -> Response {
        match error {
            InstallError::BadRequest(message) => bad_request(message),
            InstallError::Internal(message) => internal_error(message),
        }
    }

    pub fn routes_handler(registry: Arc<LibraryRegistry>) -> impl KernelHandler {
        move |req: Request| {
            let path = req.uri().path().to_string();
            let registry = Arc::clone(&registry);
            async move {
                match registry.resolve_runtime_for_path(&path) {
                    Some((runtime, true)) => respond_with_schema(runtime.state.schema()),
                    Some((runtime, false)) => {
                        let handler = runtime.routes.current_handler();
                        if let Some(handler) = handler {
                            handler.call(req).await
                        } else {
                            http::Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .body(Body::empty())
                                .expect("missing route")
                        }
                    }
                    None => respond_with_listing(registry.list_dir(&path)),
                }
            }
            .boxed()
        }
    }

    #[cfg(all(test, feature = "http-server"))]
    mod tests {
        use super::*;
        use crate::http::{Body, header};
        use crate::library::{InstallArtifacts, encode_install_payload_bytes};
        use crate::storage::Artifact;
        use crate::{Method, kernel::Kernel};
        use futures::stream;
        use hyper::body::to_bytes;
        use pathlink::Link;
        use std::str::FromStr;
        use tc_ir::LibrarySchema;
        use tc_ir::Scalar;
        use tc_state::{State, null_transaction};

        async fn decode_state(bytes: Vec<u8>) -> State {
            let stream = stream::iter(vec![Ok::<hyper::body::Bytes, std::io::Error>(
                bytes.clone().into(),
            )]);
            match destream_json::try_decode(null_transaction(), stream).await {
                Ok(state) => state,
                Err(_) => {
                    let text = String::from_utf8(bytes).expect("utf8");
                    let wrapped = format!(r#"{{"/state/scalar/map":{text}}}"#);
                    let stream = stream::iter(vec![Ok::<hyper::body::Bytes, std::io::Error>(
                        wrapped.into_bytes().into(),
                    )]);
                    destream_json::try_decode(null_transaction(), stream)
                        .await
                        .expect("state decode")
                }
            }
        }

        #[test]
        fn round_trips_install_payload_bytes() {
            let schema = LibrarySchema::new(
                Link::from_str("/lib/example-devco/hello/0.1.0").expect("link"),
                "0.1.0",
                vec![],
            );

            let payload = InstallArtifacts {
                schema: schema.clone(),
                artifacts: vec![Artifact {
                    path: schema.id().to_string(),
                    content_type: WASM_ARTIFACT_CONTENT_TYPE.to_string(),
                    bytes: b"wasm-bytes".to_vec(),
                }],
            };

            let bytes = encode_install_payload_bytes(&payload).expect("encode payload");
            let request = decode_install_request_bytes(&bytes).expect("decode payload");

            match request {
                InstallRequest::WithArtifacts(decoded) => {
                    assert_eq!(decoded.schema.id(), schema.id());
                    assert_eq!(decoded.schema.version(), schema.version());
                    assert_eq!(decoded.artifacts.len(), 1);
                    assert_eq!(
                        decoded.artifacts[0].content_type,
                        WASM_ARTIFACT_CONTENT_TYPE
                    );
                }
                _ => panic!("expected artifacts payload"),
            }
        }

        #[tokio::test]
        async fn serves_schema_over_http() {
            let initial = LibrarySchema::new(
                Link::from_str("/lib/example-devco/service/0.1.0").expect("link"),
                "0.1.0",
                vec![],
            );
            let module = build_http_library_module(initial.clone(), None)
                .await
                .expect("module");
            let handlers = http_library_handlers(&module);

            let kernel = Kernel::builder()
                .with_host_id("tc-library-test")
                .with_library_module(module, handlers)
                .finish();

            let request = http::Request::builder()
                .method("GET")
                .uri("/lib")
                .body(Body::empty())
                .expect("request");

            let response = kernel
                .dispatch(Method::Get, "/lib", request)
                .expect("handler")
                .await;

            assert_eq!(response.status(), StatusCode::OK);

            let body = to_bytes(response.into_body()).await.expect("body");
            let state = decode_state(body.to_vec()).await;
            let State::Map(listing) = state else {
                panic!("expected State::Map listing");
            };
            assert!(listing.contains_key("example-devco"));

            let schema_request = http::Request::builder()
                .method("GET")
                .uri("/lib/example-devco/service/0.1.0")
                .body(Body::empty())
                .expect("schema request");

            let schema_response = kernel
                .dispatch(
                    Method::Get,
                    "/lib/example-devco/service/0.1.0",
                    schema_request,
                )
                .expect("schema handler")
                .await;

            assert_eq!(schema_response.status(), StatusCode::OK);

            let body = to_bytes(schema_response.into_body()).await.expect("body");
            let state = decode_state(body.to_vec()).await;
            let State::Map(schema_map) = state else {
                panic!("expected State::Map schema");
            };
            let id = match schema_map.get("id") {
                Some(State::Scalar(Scalar::Value(Value::String(value)))) => value.as_str(),
                _ => panic!("missing schema id"),
            };
            let version = match schema_map.get("version") {
                Some(State::Scalar(Scalar::Value(Value::String(value)))) => value.as_str(),
                _ => panic!("missing schema version"),
            };
            assert_eq!(id, "/lib/example-devco/service/0.1.0");
            assert_eq!(version, "0.1.0");
            let deps = match schema_map.get("dependencies") {
                Some(State::Map(map)) => map,
                _ => panic!("missing dependencies"),
            };
            assert!(deps.is_empty());
        }

        #[tokio::test]
        async fn installs_schema_via_put() {
            use tc_ir::Claim;
            let initial = LibrarySchema::new(
                Link::from_str("/lib/example-devco/service/0.1.0").expect("link"),
                "0.1.0",
                vec![],
            );
            let module = build_http_library_module(initial, None)
                .await
                .expect("module");
            let handlers = http_library_handlers(&module);

            let kernel = Kernel::builder()
                .with_host_id("tc-library-test")
                .with_library_module(module, handlers)
                .finish();

            let new_schema = serde_json::json!({
                "id": "/lib/example-devco/updated/0.2.0",
                "version": "0.2.0",
                "dependencies": [],
            });

            let mut put_request = http::Request::builder()
                .method("PUT")
                .uri("/lib")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(new_schema.to_string()))
                .expect("install request");
            let install_claim = Claim::new(
                Link::from_str("/lib/example-devco/updated/0.2.0").expect("install link"),
                umask::USER_WRITE,
            );
            let install_txn = kernel
                .txn_manager()
                .begin()
                .with_claims(vec![install_claim]);
            put_request.extensions_mut().insert(install_txn);

            let put_response = kernel
                .dispatch(Method::Put, "/lib", put_request)
                .expect("put handler")
                .await;

            assert_eq!(put_response.status(), StatusCode::NO_CONTENT);

            let get_request = http::Request::builder()
                .method("GET")
                .uri("/lib/example-devco/updated/0.2.0")
                .body(Body::empty())
                .expect("get request");

            let get_response = kernel
                .dispatch(Method::Get, "/lib/example-devco/updated/0.2.0", get_request)
                .expect("get handler")
                .await;

            let body = to_bytes(get_response.into_body()).await.expect("body");
            let state = decode_state(body.to_vec()).await;
            let State::Map(schema_map) = state else {
                panic!("expected State::Map schema");
            };
            let id = match schema_map.get("id") {
                Some(State::Scalar(Scalar::Value(Value::String(value)))) => value.as_str(),
                _ => panic!("missing schema id"),
            };
            let version = match schema_map.get("version") {
                Some(State::Scalar(Scalar::Value(Value::String(value)))) => value.as_str(),
                _ => panic!("missing schema version"),
            };
            assert_eq!(id, "/lib/example-devco/updated/0.2.0");
            assert_eq!(version, "0.2.0");
        }
    }
}

#[cfg(test)]
mod registry_tests {
    use super::LibraryRegistry;
    use pathlink::Link;
    use std::collections::BTreeMap;
    use std::str::FromStr;
    use tc_ir::{Claim, LibrarySchema};
    use umask;

    #[test]
    fn picks_longest_matching_claim_for_egress() {
        let registry = LibraryRegistry::new(None, BTreeMap::new());

        let parent_id = format!("{}/acme/parent/1.0.0", crate::uri::LIB_ROOT);
        let child_id = format!("{}/acme/parent/1.0.0/child/0.1.0", crate::uri::LIB_ROOT);
        let parent = LibrarySchema::new(
            Link::from_str(&parent_id).expect("parent link"),
            "1.0.0",
            vec![],
        );
        let child = LibrarySchema::new(
            Link::from_str(&child_id).expect("child link"),
            "0.1.0",
            vec![],
        );

        futures::executor::block_on(registry.insert_schema(parent.clone())).expect("insert parent");
        futures::executor::block_on(registry.insert_schema(child.clone())).expect("insert child");

        let txn = crate::txn::TxnManager::with_host_id("test-claim")
            .begin()
            .with_claims(vec![
                Claim::new(parent.id().clone(), umask::USER_READ),
                Claim::new(child.id().clone(), umask::USER_READ),
            ]);

        let resolved = registry.schema_for_txn(&txn).expect("schema");

        assert_eq!(resolved.id(), child.id());
    }
}
