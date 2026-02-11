use std::str::FromStr;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use destream::de;
use pathlink::Link;
use serde::{Deserialize, Serialize};
use tc_ir::LibrarySchema;

use crate::storage::{Artifact, decode_schema_bytes};

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
