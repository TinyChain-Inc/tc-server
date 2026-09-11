use std::{io, path::Path};

use freqfs::{FileLoad, FileSave};
use get_size::GetSize;
use safecast::AsType;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use tc_ir::{Id, TxnId};

const MAX_RECORD_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthorityRecord {
    pub actor_id: Id,
    pub algorithm: rjwt::AlgKind,
    pub signing_key: Vec<u8>,
}

#[derive(Clone)]
pub enum ControlFile {
    Authority(AuthorityRecord),
    Frontier(TxnId),
}

impl AsType<ControlFile> for ControlFile {
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

impl GetSize for ControlFile {
    fn get_size(&self) -> usize {
        match self {
            Self::Authority(record) => record_size(record),
            Self::Frontier(txn_id) => txn_id.to_string().len(),
        }
    }
}

impl FileLoad for ControlFile {
    async fn load(
        path: &Path,
        mut file: tokio::fs::File,
        metadata: std::fs::Metadata,
    ) -> io::Result<Self> {
        let name = path.file_name().and_then(|name| name.to_str());
        match name {
            Some("authority") => {
                if metadata.len() as usize > MAX_RECORD_BYTES {
                    return Err(invalid("workspace record exceeds its bound"));
                }
                let mut bytes = Vec::with_capacity(metadata.len() as usize);
                file.read_to_end(&mut bytes).await?;
                serde_json::from_slice(&bytes)
                    .map(Self::Authority)
                    .map_err(invalid)
            }
            Some("latest_finalized" | "last_allocated") => {
                if metadata.len() as usize > MAX_RECORD_BYTES {
                    return Err(invalid("frontier record exceeds its bound"));
                }
                let mut value = String::with_capacity(metadata.len() as usize);
                file.read_to_string(&mut value).await?;
                value.parse().map(Self::Frontier).map_err(invalid)
            }
            Some(name) => Err(invalid(format!("unknown host-control file {name}"))),
            None => Err(invalid("host-control file has no name")),
        }
    }
}

impl FileSave for ControlFile {
    async fn save(&self, file: &mut tokio::fs::File) -> io::Result<u64> {
        match self {
            Self::Authority(record) => {
                let bytes = serde_json::to_vec(record).map_err(invalid)?;
                file.write_all(&bytes).await?;
                Ok(bytes.len() as u64)
            }
            Self::Frontier(txn_id) => {
                let value = txn_id.to_string();
                file.write_all(value.as_bytes()).await?;
                Ok(value.len() as u64)
            }
        }
    }
}

fn record_size(record: &impl Serialize) -> usize {
    serde_json::to_vec(record).map_or(MAX_RECORD_BYTES, |bytes| bytes.len())
}

fn invalid(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
