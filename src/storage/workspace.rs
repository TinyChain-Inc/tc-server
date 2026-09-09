use std::{io, path::Path, sync::Arc};

use freqfs::{FileLoad, FileSave};
use get_size::GetSize;
use safecast::AsType;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use tc_collection::CollectionNode;
use tc_ir::TxnId;

const MAX_RECORD_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HostRecord {
    pub actor_id: String,
    pub algorithm: String,
    pub signing_key: Vec<u8>,
    #[serde(with = "optional_txn_id")]
    pub latest_finalized: Option<TxnId>,
    #[serde(with = "optional_txn_id")]
    pub last_allocated: Option<TxnId>,
}

mod optional_txn_id {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use tc_ir::TxnId;

    pub fn serialize<S: Serializer>(
        value: &Option<TxnId>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .as_ref()
            .map(ToString::to_string)
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<TxnId>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|value| value.parse().map_err(serde::de::Error::custom))
            .transpose()
    }
}

#[derive(Clone)]
pub enum WorkspaceFile {
    Collection(CollectionNode),
    Host(HostRecord),
    Manifest(Arc<[u8]>),
    Module(Arc<[u8]>),
    Delete,
}

impl From<CollectionNode> for WorkspaceFile {
    fn from(node: CollectionNode) -> Self {
        Self::Collection(node)
    }
}

macro_rules! project {
    ($ty:ty, $variant:ident) => {
        impl AsType<$ty> for WorkspaceFile {
            fn as_type(&self) -> Option<&$ty> {
                if let Self::$variant(value) = self {
                    Some(value)
                } else {
                    None
                }
            }
            fn as_type_mut(&mut self) -> Option<&mut $ty> {
                if let Self::$variant(value) = self {
                    Some(value)
                } else {
                    None
                }
            }
            fn into_type(self) -> Option<$ty> {
                if let Self::$variant(value) = self {
                    Some(value)
                } else {
                    None
                }
            }
        }
    };
}

project!(CollectionNode, Collection);

impl AsType<WorkspaceFile> for WorkspaceFile {
    fn as_type(&self) -> Option<&Self> {
        Some(self)
    }
    fn as_type_mut(&mut self) -> Option<&mut Self> {
        Some(self)
    }
    fn into_type(self) -> Option<Self> {
        Some(self)
    }
}

impl GetSize for WorkspaceFile {
    fn get_size(&self) -> usize {
        match self {
            Self::Collection(node) => node.get_size(),
            Self::Host(record) => record_size(record),
            Self::Manifest(bytes) | Self::Module(bytes) => bytes.len(),
            Self::Delete => 0,
        }
    }
}

impl FileLoad for WorkspaceFile {
    async fn load(
        path: &Path,
        mut file: tokio::fs::File,
        metadata: std::fs::Metadata,
    ) -> io::Result<Self> {
        let name = path.file_name().and_then(|name| name.to_str());
        match name {
            Some("host") => {
                if metadata.len() as usize > MAX_RECORD_BYTES {
                    return Err(invalid("workspace record exceeds its bound"));
                }
                let mut bytes = Vec::with_capacity(metadata.len() as usize);
                file.read_to_end(&mut bytes).await?;
                serde_json::from_slice(&bytes)
                    .map(Self::Host)
                    .map_err(invalid)
            }
            Some("manifest.json" | "module.wasm") => {
                let mut bytes = Vec::with_capacity(metadata.len() as usize);
                file.read_to_end(&mut bytes).await?;
                if name == Some("manifest.json") {
                    Ok(Self::Manifest(bytes.into()))
                } else {
                    Ok(Self::Module(bytes.into()))
                }
            }
            Some("delete") if metadata.len() == 0 => Ok(Self::Delete),
            Some("delete") => Err(invalid("a staged application delete marker must be empty")),
            _ => CollectionNode::load(path, file, metadata)
                .await
                .map(Self::Collection),
        }
    }
}

impl FileSave for WorkspaceFile {
    async fn save(&self, file: &mut tokio::fs::File) -> io::Result<u64> {
        match self {
            Self::Collection(node) => node.save(file).await,
            Self::Host(record) => {
                let bytes = serde_json::to_vec(record).map_err(invalid)?;
                file.write_all(&bytes).await?;
                Ok(bytes.len() as u64)
            }
            Self::Manifest(bytes) | Self::Module(bytes) => {
                file.write_all(bytes).await?;
                Ok(bytes.len() as u64)
            }
            Self::Delete => Ok(0),
        }
    }
}

fn record_size(record: &impl Serialize) -> usize {
    serde_json::to_vec(record).map_or(MAX_RECORD_BYTES, |bytes| bytes.len())
}

fn invalid(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
