use std::str::FromStr;

use pathlink::PathSegment;
use serde::Deserialize;
use tc_error::{TCError, TCResult};
use tc_ir::{LibrarySchema, parse_route_path};

#[derive(Debug)]
pub(super) struct WasmManifest {
    pub(super) schema: LibrarySchema,
    pub(super) routes: Vec<RouteBinding>,
}

#[derive(Debug)]
pub(crate) struct RouteBinding {
    pub(crate) path: Vec<PathSegment>,
    pub(crate) export: String,
}

impl WasmManifest {
    fn new(schema: LibrarySchema, routes: Vec<RouteBinding>) -> Self {
        Self { schema, routes }
    }
}

#[derive(Deserialize)]
struct ManifestSerde {
    schema: serde_json::Value,
    routes: Vec<RouteEntrySerde>,
}

#[derive(Deserialize)]
struct RouteEntrySerde {
    path: String,
    export: String,
}

pub(super) fn decode_manifest(bytes: Vec<u8>) -> TCResult<WasmManifest> {
    let manifest: ManifestSerde = serde_json::from_slice(&bytes)
        .map_err(|err| TCError::bad_request(format!("invalid wasm manifest: {err}")))?;
    let schema = parse_schema_value(&manifest.schema)?;
    let routes = manifest
        .routes
        .into_iter()
        .map(|entry| {
            let path = parse_route_path(&entry.path)
                .map_err(|err| TCError::bad_request(err.message().to_string()))?;
            Ok(RouteBinding {
                path,
                export: entry.export,
            })
        })
        .collect::<TCResult<Vec<_>>>()?;
    Ok(WasmManifest::new(schema, routes))
}

fn parse_schema_value(value: &serde_json::Value) -> TCResult<LibrarySchema> {
    let id = value
        .get("id")
        .and_then(|id| id.as_str())
        .ok_or_else(|| TCError::bad_request("manifest schema missing id"))?;
    let version = value
        .get("version")
        .and_then(|v| v.as_str())
        .ok_or_else(|| TCError::bad_request("manifest schema missing version"))?;
    let dependencies = value
        .get("dependencies")
        .and_then(|deps| deps.as_array())
        .ok_or_else(|| TCError::bad_request("manifest schema missing dependencies array"))?;

    let id = pathlink::Link::from_str(id)
        .map_err(|err| TCError::bad_request(format!("invalid schema id: {err}")))?;
    let deps = dependencies
        .iter()
        .map(|dep| {
            dep.as_str()
                .ok_or_else(|| TCError::bad_request("dependency must be a string"))
                .and_then(|link| {
                    pathlink::Link::from_str(link).map_err(|err| {
                        TCError::bad_request(format!("invalid dependency id: {err}"))
                    })
                })
        })
        .collect::<TCResult<Vec<_>>>()?;

    Ok(LibrarySchema::new(id, version, deps))
}

pub(super) fn format_path(path: &[PathSegment]) -> String {
    format!(
        "/{}",
        path.iter()
            .map(PathSegment::to_string)
            .collect::<Vec<_>>()
            .join("/")
    )
}
