use std::{io, path::Path, sync::Arc};

use freqfs::{FileLoad, FileSave};
use futures::StreamExt;
use get_size::GetSize;
use safecast::AsType;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;

use pathlink::Link;
use tc_ir::Scalar;

const MAX_APPLICATION_BLOCK_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone)]
pub(crate) enum ApplicationBlock {
    Manifest(Link, Scalar),
    Module(Arc<[u8]>),
}

impl AsType<ApplicationBlock> for ApplicationBlock {
    fn as_type(&self) -> Option<&ApplicationBlock> {
        Some(self)
    }

    fn as_type_mut(&mut self) -> Option<&mut ApplicationBlock> {
        Some(self)
    }

    fn into_type(self) -> Option<ApplicationBlock> {
        Some(self)
    }
}

impl GetSize for ApplicationBlock {
    fn get_size(&self) -> usize {
        match self {
            Self::Manifest(identity, definition) => {
                identity.to_string().len() + definition.get_size()
            }
            Self::Module(bytes) => bytes.len(),
        }
    }
}

impl FileLoad for ApplicationBlock {
    async fn load(
        path: &Path,
        mut file: tokio::fs::File,
        metadata: std::fs::Metadata,
    ) -> io::Result<Self> {
        if metadata.len() > MAX_APPLICATION_BLOCK_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "application storage block exceeds its bound",
            ));
        }
        match path.file_name().and_then(|name| name.to_str()) {
            Some("manifest.json") => {
                let crate::literal::Definition(identity, definition) =
                    destream_json::try_decode((), ReaderStream::new(file))
                        .await
                        .map_err(|error| {
                            io::Error::new(io::ErrorKind::InvalidData, error.to_string())
                        })?;
                Ok(Self::Manifest(identity, definition))
            }
            Some("module.wasm") => {
                let mut bytes = Vec::with_capacity(metadata.len() as usize);
                file.read_to_end(&mut bytes).await?;
                Ok(Self::Module(bytes.into()))
            }
            Some(name) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported application layout file {name}"),
            )),
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing file name",
            )),
        }
    }
}

impl FileSave for ApplicationBlock {
    async fn save(&self, file: &mut tokio::fs::File) -> io::Result<u64> {
        match self {
            Self::Manifest(identity, definition) => {
                let mut encoded = destream_json::encode(crate::literal::Definition(
                    identity.clone(),
                    definition.clone(),
                ))
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
                let mut size = 0;
                while let Some(chunk) = encoded.next().await {
                    let chunk = chunk.map_err(|error| io::Error::other(error.to_string()))?;
                    file.write_all(&chunk).await?;
                    size += chunk.len() as u64;
                }
                Ok(size)
            }
            Self::Module(bytes) => {
                file.write_all(bytes).await?;
                Ok(bytes.len() as u64)
            }
        }
    }
}
