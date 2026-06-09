use std::str::FromStr;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use destream::de;
use pathlink::Link;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tc_ir::LibrarySchema;

use crate::storage::Artifact;

pub struct CompiledLibraryPackage {
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

pub fn decode_install_request_bytes(bytes: &[u8]) -> Result<CompiledLibraryPackage, InstallError> {
    if bytes.iter().all(|b| b.is_ascii_whitespace()) {
        return Err(InstallError::bad_request("empty install payload"));
    }

    let bytes = normalize_install_bytes(bytes)?;

    if let Some(payload) = decode_library_definition(&bytes)? {
        return Ok(payload);
    }

    Err(InstallError::bad_request(
        "expected canonical library definition",
    ))
}

fn normalize_install_bytes(bytes: &[u8]) -> Result<Vec<u8>, InstallError> {
    match serde_json::from_slice::<String>(bytes) {
        Ok(inner) if inner.trim_start().starts_with('{') => Ok(inner.into_bytes()),
        Ok(_) | Err(_) => Ok(bytes.to_vec()),
    }
}

fn decode_library_definition(bytes: &[u8]) -> Result<Option<CompiledLibraryPackage>, InstallError> {
    let definition: JsonValue = match serde_json::from_slice(bytes) {
        Ok(definition) => definition,
        Err(_) => return Ok(None),
    };

    let JsonValue::Object(root) = definition else {
        return Ok(None);
    };

    if root.len() != 1 {
        return Ok(None);
    }

    let (id, members) = root
        .into_iter()
        .next()
        .expect("single library definition entry");

    if !id.starts_with("/lib/") {
        return Ok(None);
    }

    let JsonValue::Object(members) = members else {
        return Err(InstallError::bad_request(
            "library definition members must be a JSON object",
        ));
    };

    let schema = schema_from_library_id(&id)?;
    let routes = members
        .into_iter()
        .map(|(name, member)| {
            if name.trim().is_empty() || name.contains('/') {
                return Err(InstallError::bad_request(format!(
                    "invalid library member name: {name}"
                )));
            }

            if is_op_json(&member) {
                Ok(serde_json::json!({
                    "path": format!("/{name}"),
                    "op": member,
                }))
            } else if is_opdef_json(&member) {
                Ok(serde_json::json!({
                    "path": format!("/{name}"),
                    "opdef": member,
                }))
            } else {
                Ok(serde_json::json!({
                    "path": format!("/{name}"),
                    "value": member,
                }))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    let manifest = serde_json::json!({
        "schema": RawSchema::from(&schema),
        "routes": routes,
    });
    let bytes = serde_json::to_vec(&manifest)
        .map_err(|err| InstallError::bad_request(format!("invalid library definition: {err}")))?;

    Ok(Some(CompiledLibraryPackage {
        schema,
        artifacts: vec![Artifact {
            path: "/lib/ir".to_string(),
            content_type: crate::ir::IR_ARTIFACT_CONTENT_TYPE.to_string(),
            bytes,
        }],
    }))
}

fn is_op_json(value: &JsonValue) -> bool {
    let JsonValue::Object(object) = value else {
        return false;
    };

    object.contains_key("method") && object.contains_key("path")
}

fn is_opdef_json(value: &JsonValue) -> bool {
    let JsonValue::Object(object) = value else {
        return false;
    };

    object
        .keys()
        .any(|key| key.starts_with("/state/scalar/op/"))
}

fn schema_from_library_id(id: &str) -> Result<LibrarySchema, InstallError> {
    let link = Link::from_str(id).map_err(|err| {
        InstallError::bad_request(format!("invalid library definition id: {err}"))
    })?;
    let version = id
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .filter(|version| !version.is_empty())
        .ok_or_else(|| InstallError::bad_request("library definition id missing version"))?;

    Ok(LibrarySchema::new(link, version.to_string(), Vec::new()))
}

pub fn encode_compiled_library_package(
    payload: &CompiledLibraryPackage,
) -> Result<Vec<u8>, String> {
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

pub fn decode_compiled_library_package(
    bytes: &[u8],
) -> Result<CompiledLibraryPackage, InstallError> {
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

    Ok(CompiledLibraryPackage { schema, artifacts })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_canonical_library_definition_as_atomic_payload() {
        let definition = serde_json::json!({
            "/lib/example-devco/greeter/0.1.0": {
                "hello": {
                    "/state/scalar/op/get": [
                        "name",
                        [
                            ["_tmp0", "Hello, {{name}}!"],
                            ["result", {"$_tmp0/render": {"name": {"$name": []}}}]
                        ]
                    ]
                }
            }
        });

        let bytes = serde_json::to_vec(&definition).expect("definition bytes");
        let request = decode_install_request_bytes(&bytes).expect("decode definition");

        let payload = request;
        assert_eq!(
            payload.schema.id().to_string(),
            "/lib/example-devco/greeter/0.1.0"
        );
        assert_eq!(payload.schema.version(), "0.1.0");
        assert_eq!(payload.schema.dependencies().len(), 0);
        assert_eq!(payload.artifacts.len(), 1);
        assert_eq!(payload.artifacts[0].path, "/lib/ir");
        assert_eq!(
            payload.artifacts[0].content_type,
            crate::ir::IR_ARTIFACT_CONTENT_TYPE
        );

        let manifest: serde_json::Value =
            serde_json::from_slice(&payload.artifacts[0].bytes).expect("ir manifest");
        assert_eq!(manifest["schema"]["id"], "/lib/example-devco/greeter/0.1.0");
        assert_eq!(manifest["routes"][0]["path"], "/hello");
        assert!(manifest["routes"][0]["opdef"].is_object());
    }

    #[test]
    fn rejects_invalid_library_definition_member_name() {
        let definition = serde_json::json!({
            "/lib/example-devco/greeter/0.1.0": {
                "bad/name": null
            }
        });

        let bytes = serde_json::to_vec(&definition).expect("definition bytes");
        let err = match decode_install_request_bytes(&bytes) {
            Ok(_) => panic!("invalid member name should fail"),
            Err(err) => err,
        };
        assert!(err.message().contains("invalid library member name"));
    }

    #[test]
    fn rejects_schema_artifact_envelope_as_public_install_request() {
        let payload = serde_json::json!({
            "schema": {
                "id": "/lib/example-devco/greeter/0.1.0",
                "version": "0.1.0",
                "dependencies": []
            },
            "artifacts": []
        });

        let bytes = serde_json::to_vec(&payload).expect("payload bytes");
        let err = match decode_install_request_bytes(&bytes) {
            Ok(_) => panic!("schema/artifact envelope should not be a public install request"),
            Err(err) => err,
        };
        assert!(err.message().contains("canonical library definition"));
    }

    #[test]
    fn decodes_string_encoded_library_definition_from_pyo3_bridge() {
        let definition = serde_json::json!({
            "/lib/example-devco/greeter/0.1.0": {
                "hello": {
                    "/state/scalar/op/get": [
                        "name",
                        [["result", {"$name": []}]]
                    ]
                }
            }
        });
        let bridge_body =
            serde_json::to_vec(&definition.to_string()).expect("string-encoded definition");

        let request = decode_install_request_bytes(&bridge_body).expect("decode definition");
        assert_eq!(
            request.schema.id().to_string(),
            "/lib/example-devco/greeter/0.1.0"
        );
    }

    #[test]
    fn decodes_literal_library_member_as_value_route() {
        let definition = serde_json::json!({
            "/lib/example-devco/static/0.1.0": {
                "hello": "hello"
            }
        });

        let bytes = serde_json::to_vec(&definition).expect("definition bytes");
        let request = decode_install_request_bytes(&bytes).expect("decode definition");

        let payload = request;
        let manifest: serde_json::Value =
            serde_json::from_slice(&payload.artifacts[0].bytes).expect("ir manifest");
        assert_eq!(manifest["routes"][0]["path"], "/hello");
        assert_eq!(manifest["routes"][0]["value"], "hello");
        assert!(manifest["routes"][0].get("opdef").is_none());
    }

    #[test]
    fn decodes_opref_library_member_as_op_route() {
        let definition = serde_json::json!({
            "/lib/example-devco/alias/0.1.0": {
                "ping": {
                    "method": "GET",
                    "path": "/lib/example-devco/source/0.1.0/ping"
                }
            }
        });

        let bytes = serde_json::to_vec(&definition).expect("definition bytes");
        let request = decode_install_request_bytes(&bytes).expect("decode definition");

        let payload = request;
        let manifest: serde_json::Value =
            serde_json::from_slice(&payload.artifacts[0].bytes).expect("ir manifest");
        assert_eq!(manifest["routes"][0]["path"], "/ping");
        assert_eq!(manifest["routes"][0]["op"]["method"], "GET");
        assert_eq!(
            manifest["routes"][0]["op"]["path"],
            "/lib/example-devco/source/0.1.0/ping"
        );
    }
}
