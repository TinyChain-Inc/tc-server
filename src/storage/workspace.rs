use std::{io, path::Path};

use freqfs::{FileLoad, FileSave};
use futures::StreamExt;
use get_size::GetSize;
use safecast::AsType;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;

use destream::{EncodeMap, de, en};
use tc_ir::{Id, TxnId};

const MAX_RECORD_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorityRecord {
    pub actor_id: Id,
    pub algorithm: rjwt::AlgKind,
    pub signing_key: Vec<u8>,
}

impl de::FromStream for AuthorityRecord {
    type Context = ();

    async fn from_stream<D: de::Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
        struct Visitor;

        impl de::Visitor for Visitor {
            type Value = AuthorityRecord;

            fn expecting() -> &'static str {
                "a protocol authority record"
            }

            async fn visit_map<A: de::MapAccess>(
                self,
                mut access: A,
            ) -> Result<Self::Value, A::Error> {
                let mut actor_id = None;
                let mut algorithm = None;
                let mut signing_key = None;
                while let Some(field) = access.next_key::<String>(()).await? {
                    match field.as_str() {
                        "actor_id" if actor_id.is_none() => {
                            actor_id = Some(access.next_value(()).await?)
                        }
                        "algorithm" if algorithm.is_none() => {
                            let value: String = access.next_value(()).await?;
                            algorithm = Some(value.parse().map_err(de::Error::custom)?);
                        }
                        "signing_key" if signing_key.is_none() => {
                            signing_key = Some(access.next_value(()).await?)
                        }
                        "actor_id" | "algorithm" | "signing_key" => {
                            return Err(de::Error::custom(format!(
                                "duplicate authority field {field}"
                            )));
                        }
                        _ => {
                            return Err(de::Error::custom(format!(
                                "unexpected authority field {field}"
                            )));
                        }
                    }
                }
                Ok(AuthorityRecord {
                    actor_id: actor_id
                        .ok_or_else(|| de::Error::custom("missing authority field actor_id"))?,
                    algorithm: algorithm
                        .ok_or_else(|| de::Error::custom("missing authority field algorithm"))?,
                    signing_key: signing_key
                        .ok_or_else(|| de::Error::custom("missing authority field signing_key"))?,
                })
            }
        }

        decoder.decode_map(Visitor).await
    }
}

impl<'en> en::ToStream<'en> for AuthorityRecord {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        let mut map = encoder.encode_map(Some(3))?;
        map.encode_entry("actor_id", &self.actor_id)?;
        map.encode_entry("algorithm", self.algorithm.name())?;
        map.encode_entry("signing_key", &self.signing_key)?;
        map.end()
    }
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
                destream_json::try_decode((), ReaderStream::new(file))
                    .await
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
                let mut encoded = destream_json::encode(record).map_err(invalid)?;
                let mut size = 0;
                while let Some(chunk) = encoded.next().await {
                    let chunk = chunk.map_err(invalid)?;
                    file.write_all(&chunk).await?;
                    size += chunk.len() as u64;
                }
                Ok(size)
            }
            Self::Frontier(txn_id) => {
                let value = txn_id.to_string();
                file.write_all(value.as_bytes()).await?;
                Ok(value.len() as u64)
            }
        }
    }
}

fn record_size(record: &AuthorityRecord) -> usize {
    record.actor_id.as_str().len() + record.algorithm.name().len() + record.signing_key.len()
}

fn invalid(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[cfg(test)]
#[path = "../../tests/support/storage_workspace.rs"]
mod tests;
