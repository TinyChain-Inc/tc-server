use std::{
    str::FromStr,
    sync::{Arc, RwLock},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use destream::de;
use pathlink::{Link, PathSegment};
use serde::Deserialize;
use tc_error::{TCError, TCResult};
use tc_ir::{
    Dir, HandleDelete, HandleGet, HandlePost, HandlePut, Library, LibraryModule, LibrarySchema,
    Route,
};
use tc_value::Value;

use crate::{
    KernelHandler,
    ir::IR_ARTIFACT_CONTENT_TYPE,
    storage::{LibraryArtifact, LibraryDir, decode_schema_bytes},
    txn::TxnHandle,
};

type HandlerArc<Request, Response> = Arc<dyn KernelHandler<Request, Response>>;

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
    pub artifacts: Vec<InstallArtifact>,
}

#[derive(Clone, Debug)]
pub struct InstallArtifact {
    pub path: String,
    pub content_type: String,
    pub bytes: String,
}

impl de::FromStream for InstallArtifact {
    type Context = ();

    async fn from_stream<D: de::Decoder>(
        _context: Self::Context,
        decoder: &mut D,
    ) -> Result<Self, D::Error> {
        struct ArtifactVisitor;

        impl de::Visitor for ArtifactVisitor {
            type Value = InstallArtifact;

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
                        "bytes" => bytes = Some(map.next_value(()).await?),
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

                Ok(InstallArtifact {
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

struct InstallPayloadRaw {
    schema: LibrarySchema,
    artifacts: Vec<InstallArtifact>,
}

impl de::FromStream for InstallPayloadRaw {
    type Context = ();

    async fn from_stream<D: de::Decoder>(
        _context: Self::Context,
        decoder: &mut D,
    ) -> Result<Self, D::Error> {
        struct PayloadVisitor;

        impl de::Visitor for PayloadVisitor {
            type Value = InstallPayloadRaw;

            fn expecting() -> &'static str {
                "an install payload map"
            }

            async fn visit_map<A: de::MapAccess>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut schema = None;
                let mut artifacts = None;

                while let Some(key) = map.next_key::<String>(()).await? {
                    match key.as_str() {
                        "schema" => schema = Some(map.next_value(()).await?),
                        "artifacts" => artifacts = Some(map.next_value(()).await?),
                        _ => {
                            let _ = map.next_value::<de::IgnoredAny>(()).await?;
                        }
                    }
                }

                let schema = schema
                    .ok_or_else(|| de::Error::custom("install payload missing schema field"))?;
                let artifacts = artifacts.unwrap_or_default();

                Ok(InstallPayloadRaw { schema, artifacts })
            }
        }

        decoder.decode_map(PayloadVisitor).await
    }
}

pub fn decode_install_request_bytes(bytes: &[u8]) -> Result<InstallRequest, InstallError> {
    if bytes.iter().all(|b| b.is_ascii_whitespace()) {
        return Err(InstallError::bad_request("empty install payload"));
    }

    if let Ok(schema) = decode_schema_bytes(bytes) {
        return Ok(InstallRequest::SchemaOnly(schema));
    }

    let raw = decode_install_payload(bytes)?;

    Ok(InstallRequest::WithArtifacts(InstallArtifacts {
        schema: raw.schema,
        artifacts: raw.artifacts,
    }))
}

pub fn apply_wasm_install<Request, Response>(
    state: &LibraryState,
    routes: &LibraryRoutes<Request, Response>,
    storage: Option<&LibraryDir>,
    route_factory: Option<&LibraryRouteFactory<Request, Response>>,
    payload: InstallArtifacts,
) -> Result<(), InstallError> {
    let artifact = payload
        .artifacts
        .into_iter()
        .find(|artifact| artifact.content_type == "application/wasm")
        .ok_or_else(|| InstallError::bad_request("missing application/wasm artifact"))?;

    let wasm_bytes = BASE64
        .decode(artifact.bytes.as_bytes())
        .map_err(|err| InstallError::bad_request(format!("invalid base64 artifact: {err}")))?;

    let factory = route_factory
        .ok_or_else(|| InstallError::bad_request("wasm installs are not supported"))?;

    let (manifest_schema, schema_routes, handler) =
        factory(wasm_bytes.clone()).map_err(|err| InstallError::internal(err.to_string()))?;

    if !schemas_equivalent(&manifest_schema, &payload.schema) {
        return Err(InstallError::bad_request(
            "manifest schema does not match wasm descriptor",
        ));
    }

    state.replace_with_routes(manifest_schema.clone(), schema_routes);
    routes.replace_arc(handler);

    if let Some(storage) = storage {
        storage
            .persist_wasm_library(&manifest_schema, &wasm_bytes)
            .map_err(|err| InstallError::internal(err.to_string()))?;
    }

    Ok(())
}

pub fn apply_ir_install<Request, Response>(
    state: &LibraryState,
    routes: &LibraryRoutes<Request, Response>,
    storage: Option<&LibraryDir>,
    route_factory: Option<&LibraryRouteFactory<Request, Response>>,
    payload: InstallArtifacts,
) -> Result<(), InstallError> {
    let artifact = payload
        .artifacts
        .into_iter()
        .find(|artifact| artifact.content_type == IR_ARTIFACT_CONTENT_TYPE)
        .ok_or_else(|| InstallError::bad_request("missing application/tinychain+json artifact"))?;

    let ir_bytes = BASE64
        .decode(artifact.bytes.as_bytes())
        .map_err(|err| InstallError::bad_request(format!("invalid base64 artifact: {err}")))?;

    let factory = route_factory
        .ok_or_else(|| InstallError::bad_request("ir installs are not supported"))?;

    let (manifest_schema, schema_routes, handler) =
        factory(ir_bytes.clone()).map_err(|err| InstallError::internal(err.to_string()))?;

    if !schemas_equivalent(&manifest_schema, &payload.schema) {
        return Err(InstallError::bad_request("manifest schema does not match descriptor"));
    }

    state.replace_with_routes(manifest_schema.clone(), schema_routes);
    routes.replace_arc(handler);

    if let Some(storage) = storage {
        storage
            .persist_ir_library(&manifest_schema, &ir_bytes)
            .map_err(|err| InstallError::internal(err.to_string()))?;
    }

    Ok(())
}

pub fn apply_install<Request, Response>(
    state: &LibraryState,
    routes: &LibraryRoutes<Request, Response>,
    storage: Option<&LibraryDir>,
    wasm_factory: Option<&LibraryRouteFactory<Request, Response>>,
    ir_factory: Option<&LibraryRouteFactory<Request, Response>>,
    payload: InstallArtifacts,
) -> Result<(), InstallError> {
    if payload
        .artifacts
        .iter()
        .any(|artifact| artifact.content_type == "application/wasm")
    {
        return apply_wasm_install(state, routes, storage, wasm_factory, payload);
    }

    if payload
        .artifacts
        .iter()
        .any(|artifact| artifact.content_type == IR_ARTIFACT_CONTENT_TYPE)
    {
        return apply_ir_install(state, routes, storage, ir_factory, payload);
    }

    Err(InstallError::bad_request(
        "missing supported artifact (expected application/wasm or application/tinychain+json)",
    ))
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
        return raw;
    }

    // Accept both path-only and absolute link string forms by comparing on the path suffix.
    // This keeps the `/lib` install payload stable even if a `Link` string round-trip adds a
    // scheme or authority prefix.
    match raw.split_once("://") {
        Some((_, rest)) => rest
            .find('/')
            .map(|idx| rest[idx..].to_string())
            .unwrap_or(raw),
        None => raw,
    }
}

fn decode_install_payload(bytes: &[u8]) -> Result<InstallPayloadRaw, InstallError> {
    #[derive(Deserialize)]
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

    #[derive(Deserialize)]
    struct InstallPayload {
        schema: RawSchema,
        #[serde(default)]
        artifacts: Vec<InstallArtifactRaw>,
    }

    #[derive(Deserialize)]
    struct InstallArtifactRaw {
        path: String,
        content_type: String,
        bytes: String,
    }

    let payload: InstallPayload = serde_json::from_slice(bytes)
        .map_err(|err| InstallError::bad_request(format!("invalid install payload json: {err}")))?;

    let schema = payload
        .schema
        .try_into()
        .map_err(InstallError::bad_request)?;

    Ok(InstallPayloadRaw {
        schema,
        artifacts: payload
            .artifacts
            .into_iter()
            .map(|artifact| InstallArtifact {
                path: artifact.path,
                content_type: artifact.content_type,
                bytes: artifact.bytes,
            })
            .collect(),
    })
}

pub type LibraryRouteFactory<Request, Response> = Arc<
    dyn Fn(Vec<u8>) -> TCResult<(LibrarySchema, SchemaRoutes, HandlerArc<Request, Response>)>
        + Send
        + Sync,
>;

pub struct LibraryRoutes<Request, Response> {
    inner: Arc<RwLock<Option<HandlerArc<Request, Response>>>>,
}

impl<Request, Response> Default for LibraryRoutes<Request, Response> {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(None)),
        }
    }
}

impl<Request, Response> LibraryRoutes<Request, Response> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn replace<H>(&self, handler: H)
    where
        H: KernelHandler<Request, Response>,
        Request: Send + 'static,
        Response: Send + 'static,
    {
        self.replace_arc(Arc::new(handler));
    }

    pub fn replace_arc(&self, handler: HandlerArc<Request, Response>) {
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

    pub fn current_handler(&self) -> Option<HandlerArc<Request, Response>> {
        self.inner.read().expect("library routes read lock").clone()
    }
}

impl<Request, Response> Clone for LibraryRoutes<Request, Response> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

pub struct LibraryHandlers<Request, Response> {
    get: HandlerArc<Request, Response>,
    put: HandlerArc<Request, Response>,
    route: Option<HandlerArc<Request, Response>>,
}

impl<Request, Response> LibraryHandlers<Request, Response>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
    pub fn with_route<G, P, R>(get: G, put: P, route: R) -> Self
    where
        G: KernelHandler<Request, Response>,
        P: KernelHandler<Request, Response>,
        R: KernelHandler<Request, Response>,
    {
        Self {
            get: Arc::new(get),
            put: Arc::new(put),
            route: Some(Arc::new(route)),
        }
    }

    pub fn without_route<G, P>(get: G, put: P) -> Self
    where
        G: KernelHandler<Request, Response>,
        P: KernelHandler<Request, Response>,
    {
        Self {
            get: Arc::new(get),
            put: Arc::new(put),
            route: None,
        }
    }

    pub fn get_handler(&self) -> HandlerArc<Request, Response> {
        Arc::clone(&self.get)
    }

    pub fn put_handler(&self) -> HandlerArc<Request, Response> {
        Arc::clone(&self.put)
    }

    pub fn route_handler(&self) -> Option<HandlerArc<Request, Response>> {
        self.route.as_ref().map(Arc::clone)
    }
}

impl<Request, Response> Clone for LibraryHandlers<Request, Response> {
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

pub struct LibraryRuntime<Request, Response> {
    state: LibraryState,
    routes: LibraryRoutes<Request, Response>,
    storage: Option<crate::storage::LibraryDir>,
    wasm_factory: Option<LibraryRouteFactory<Request, Response>>,
    ir_factory: Option<LibraryRouteFactory<Request, Response>>,
}

impl<Request, Response> LibraryRuntime<Request, Response>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
    pub fn new(
        initial_schema: LibrarySchema,
        storage: Option<crate::storage::LibraryDir>,
        wasm_factory: Option<LibraryRouteFactory<Request, Response>>,
        ir_factory: Option<LibraryRouteFactory<Request, Response>>,
    ) -> Self {
        Self {
            state: LibraryState::new(initial_schema),
            routes: LibraryRoutes::new(),
            storage,
            wasm_factory,
            ir_factory,
        }
    }

    pub fn state(&self) -> LibraryState {
        self.state.clone()
    }

    pub fn routes(&self) -> LibraryRoutes<Request, Response> {
        self.routes.clone()
    }

    pub fn storage(&self) -> Option<crate::storage::LibraryDir> {
        self.storage.clone()
    }

    pub fn wasm_factory(&self) -> Option<LibraryRouteFactory<Request, Response>> {
        self.wasm_factory.clone()
    }

    pub fn ir_factory(&self) -> Option<LibraryRouteFactory<Request, Response>> {
        self.ir_factory.clone()
    }

    pub fn hydrate_from_storage(&self) -> TCResult<()> {
        let storage = match &self.storage {
            Some(storage) => storage,
            None => return Ok(()),
        };

        let entries = storage.load_all()?;
        for (schema, artifact) in entries {
            let (manifest_schema, schema_routes, handler) = match artifact {
                LibraryArtifact::Wasm(bytes) => {
                    let factory = match &self.wasm_factory {
                        Some(factory) => Arc::clone(factory),
                        None => continue,
                    };
                    factory(bytes)?
                }
                LibraryArtifact::Ir(bytes) => {
                    let factory = match &self.ir_factory {
                        Some(factory) => Arc::clone(factory),
                        None => continue,
                    };
                    factory(bytes)?
                }
            };

            if manifest_schema != schema {
                return Err(TCError::internal("persisted schema mismatch"));
            }

            self.state.replace_with_routes(
                manifest_schema,
                schema_routes.clone(),
            );
            self.routes.replace_arc(handler);
        }

        Ok(())
    }

    /// Install a WASM library from raw bytes (in-process).
    ///
    /// This bypasses the HTTP `/lib` installer envelope and uses the manifest embedded in the WASM
    /// module as the source of truth for the installed [`LibrarySchema`] and routes.
    ///
    /// The returned schema is the manifest schema parsed from the WASM module.
    pub fn install_wasm_bytes(&self, wasm_bytes: Vec<u8>) -> TCResult<LibrarySchema> {
        let factory = self
            .wasm_factory
            .as_ref()
            .ok_or_else(|| TCError::bad_request("wasm installs are not supported"))?;

        let (manifest_schema, schema_routes, handler) = factory(wasm_bytes.clone())?;

        self.state
            .replace_with_routes(manifest_schema.clone(), schema_routes);
        self.routes.replace_arc(handler);

        if let Some(storage) = &self.storage {
            storage.persist_wasm_library(&manifest_schema, &wasm_bytes)?;
        }

        Ok(manifest_schema)
    }
}

impl<Request, Response> Clone for LibraryRuntime<Request, Response>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            routes: self.routes.clone(),
            storage: self.storage.clone(),
            wasm_factory: self.wasm_factory.clone(),
            ir_factory: self.ir_factory.clone(),
        }
    }
}

#[cfg(feature = "http-server")]
pub mod http {
    use std::{io, sync::Arc};

    use futures::{FutureExt, TryStreamExt};
    use hyper::{Body, Request, Response, StatusCode, body};
    use tc_ir::LibrarySchema;

    use crate::{
        KernelHandler,
        library::{
            InstallError, InstallRequest, LibraryHandlers, LibraryRouteFactory, LibraryRoutes,
            LibraryRuntime, LibraryState, apply_install, decode_install_request_bytes,
        },
        storage::LibraryDir,
        wasm::http_wasm_route_handler_from_bytes,
        ir::http_ir_route_handler_from_bytes,
    };

    pub fn build_http_library_module(
        initial_schema: LibrarySchema,
        storage: Option<LibraryDir>,
    ) -> Arc<LibraryRuntime<Request<Body>, Response<Body>>> {
        let wasm_factory: LibraryRouteFactory<_, _> = Arc::new(|bytes: Vec<u8>| {
            let (handler, schema, schema_routes) = http_wasm_route_handler_from_bytes(bytes)?;
            let handler: Arc<dyn KernelHandler<Request<Body>, Response<Body>>> = Arc::new(handler);
            Ok((schema, schema_routes, handler))
        });

        let ir_factory: LibraryRouteFactory<_, _> = Arc::new(|bytes: Vec<u8>| {
            let (handler, schema, schema_routes) = http_ir_route_handler_from_bytes(bytes)?;
            let handler: Arc<dyn KernelHandler<Request<Body>, Response<Body>>> = Arc::new(handler);
            Ok((schema, schema_routes, handler))
        });

        Arc::new(LibraryRuntime::new(
            initial_schema,
            storage,
            Some(wasm_factory),
            Some(ir_factory),
        ))
    }

    pub fn http_library_handlers(
        module: &Arc<LibraryRuntime<Request<Body>, Response<Body>>>,
    ) -> LibraryHandlers<Request<Body>, Response<Body>> {
        let state = module.state();
        let routes = module.routes();
        let storage = module.storage();
        let wasm_factory = module.wasm_factory();
        let ir_factory = module.ir_factory();
        let get = schema_get_handler(state.clone());
        let put = schema_put_handler(state, routes.clone(), storage, wasm_factory, ir_factory);
        let route = routes_handler(&routes);
        LibraryHandlers::with_route(get, put, route)
    }

    pub fn schema_get_handler(
        state: LibraryState,
    ) -> impl KernelHandler<Request<Body>, Response<Body>> {
        move |_req: Request<Body>| {
            let state = state.clone();
            async move { respond_with_schema(state.schema()) }.boxed()
        }
    }

    pub fn schema_put_handler(
        state: LibraryState,
        routes: LibraryRoutes<Request<Body>, Response<Body>>,
        storage: Option<LibraryDir>,
        wasm_factory: Option<LibraryRouteFactory<Request<Body>, Response<Body>>>,
        ir_factory: Option<LibraryRouteFactory<Request<Body>, Response<Body>>>,
    ) -> impl KernelHandler<Request<Body>, Response<Body>> {
        move |req: Request<Body>| {
            let state = state.clone();
            let routes = routes.clone();
            let storage = storage.clone();
            let wasm_factory = wasm_factory.clone();
            let ir_factory = ir_factory.clone();
            async move {
                match decode_body(req).await {
                    Ok(InstallRequest::SchemaOnly(schema)) => {
                        state.replace_schema(schema);
                        routes.clear();
                        if let Some(storage) = &storage
                            && let Err(err) = storage.persist_schema(&state.schema())
                        {
                            return internal_error(err.to_string());
                        }
                        no_content_response()
                    }
                    Ok(InstallRequest::WithArtifacts(payload)) => match apply_install(
                        &state,
                        &routes,
                        storage.as_ref(),
                        wasm_factory.as_ref(),
                        ir_factory.as_ref(),
                        payload,
                    ) {
                        Ok(()) => no_content_response(),
                        Err(err) => install_error_response(err),
                    },
                    Err(err) => install_error_response(err),
                }
            }
            .boxed()
        }
    }

    fn respond_with_schema(schema: LibrarySchema) -> Response<Body> {
        match destream_json::encode(schema) {
            Ok(stream) => {
                let body =
                    Body::wrap_stream(stream.map_err(|err| io::Error::other(err.to_string())));
                Response::builder()
                    .status(StatusCode::OK)
                    .header(hyper::header::CONTENT_TYPE, "application/json")
                    .body(body)
                    .expect("library schema response")
            }
            Err(err) => internal_error(err.to_string()),
        }
    }

    async fn decode_body(req: Request<Body>) -> Result<InstallRequest, InstallError> {
        let body_bytes = body::to_bytes(req.into_body())
            .await
            .map_err(|err| InstallError::internal(err.to_string()))?;
        decode_install_request_bytes(&body_bytes)
    }

    fn bad_request(message: String) -> Response<Body> {
        Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from(message))
            .expect("bad request response")
    }

    fn internal_error(message: String) -> Response<Body> {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(message))
            .expect("internal error response")
    }

    fn no_content_response() -> Response<Body> {
        Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .expect("library install response")
    }

    fn install_error_response(error: InstallError) -> Response<Body> {
        match error {
            InstallError::BadRequest(message) => bad_request(message),
            InstallError::Internal(message) => internal_error(message),
        }
    }

    pub fn routes_handler(
        routes: &LibraryRoutes<Request<Body>, Response<Body>>,
    ) -> impl KernelHandler<Request<Body>, Response<Body>> + '_ {
        let routes = routes.clone();
        move |req: Request<Body>| {
            let handler = routes.current_handler();
            async move {
                if let Some(handler) = handler {
                    handler.call(req).await
                } else {
                    Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Body::empty())
                        .expect("missing route")
                }
            }
            .boxed()
        }
    }

    #[cfg(all(test, feature = "http-server"))]
    mod tests {
        use super::*;
        use crate::{Method, kernel::Kernel};
        use hyper::{Body, Request, body::to_bytes};
        use pathlink::Link;
        use serde_json::Value as JsonValue;
        use std::str::FromStr;
        use tc_ir::LibrarySchema;

        #[tokio::test]
        async fn serves_schema_over_http() {
            let initial = LibrarySchema::new(
                Link::from_str("/lib/service").expect("link"),
                "0.1.0",
                vec![],
            );
            let module = build_http_library_module(initial.clone(), None);
            let handlers = http_library_handlers(&module);

            let kernel = Kernel::builder()
                .with_host_id("tc-library-test")
                .with_library_module(module, handlers)
                .finish();

            let request = Request::builder()
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
            let json: JsonValue = serde_json::from_slice(&body).expect("json");

            assert_eq!(json["id"], "/lib/service");
            assert_eq!(json["version"], "0.1.0");
            assert!(json["dependencies"].is_array());
        }

        #[tokio::test]
        async fn installs_schema_via_put() {
            let initial = LibrarySchema::new(
                Link::from_str("/lib/service").expect("link"),
                "0.1.0",
                vec![],
            );
            let module = build_http_library_module(initial, None);
            let handlers = http_library_handlers(&module);

            let kernel = Kernel::builder()
                .with_host_id("tc-library-test")
                .with_library_module(module, handlers)
                .finish();

            let new_schema = serde_json::json!({
                "id": "/lib/updated",
                "version": "0.2.0",
                "dependencies": [],
            });

            let put_request = Request::builder()
                .method("PUT")
                .uri("/lib")
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(Body::from(new_schema.to_string()))
                .expect("install request");

            let put_response = kernel
                .dispatch(Method::Put, "/lib", put_request)
                .expect("put handler")
                .await;

            assert_eq!(put_response.status(), StatusCode::NO_CONTENT);

            let get_request = Request::builder()
                .method("GET")
                .uri("/lib")
                .body(Body::empty())
                .expect("get request");

            let get_response = kernel
                .dispatch(Method::Get, "/lib", get_request)
                .expect("get handler")
                .await;

            let json: JsonValue =
                serde_json::from_slice(&to_bytes(get_response.into_body()).await.unwrap())
                    .expect("json");

            assert_eq!(json["id"], "/lib/updated");
            assert_eq!(json["version"], "0.2.0");
        }
    }
}
