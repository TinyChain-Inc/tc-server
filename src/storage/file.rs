use std::io;
use std::path::Path;
use std::sync::Arc;

use freqfs::{FileLoad, FileSave};
use futures::StreamExt;
use get_size::GetSize;
use pathlink::Link;
use safecast::AsType;
use tc_ir::Scalar;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;

const MAX_APPLICATION_BLOCK_BYTES: u64 = 64 * 1024 * 1024;

fn application_size(size: u64, additional: u64) -> io::Result<u64> {
    size.checked_add(additional)
        .filter(|size| *size <= MAX_APPLICATION_BLOCK_BYTES)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "application block exceeds its bound",
            )
        })
}

#[derive(Clone)]
pub enum ApplicationBlock {
    // Keep manifest graphs out of every native collection/history block's inline size.
    Manifest(Link, Box<Scalar>),
    Module(Arc<[u8]>),
    Collection(tc_collection::PersistentFile),
    Chain(tc_chain::ChainFile),
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

safecast::as_type!(ApplicationBlock, Chain, tc_chain::ChainFile);

tc_collection::delegate_collection_files!(ApplicationBlock, Collection);

impl GetSize for ApplicationBlock {
    fn get_heap_size(&self) -> usize {
        match self {
            Self::Manifest(identity, definition) => {
                identity.get_heap_size() + definition.get_heap_size()
            }
            Self::Module(bytes) => bytes.len() + 2 * std::mem::size_of::<usize>(),
            Self::Collection(file) => file.get_heap_size(),
            Self::Chain(file) => file.get_heap_size(),
        }
    }
}

impl FileLoad for ApplicationBlock {
    async fn load(
        path: &Path,
        mut file: tokio::fs::File,
        metadata: std::fs::Metadata,
    ) -> io::Result<Self> {
        if matches!(
            path.file_name().and_then(|name| name.to_str()),
            Some("manifest.json" | "module.wasm")
        ) && metadata.len() > MAX_APPLICATION_BLOCK_BYTES
        {
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

                Ok(Self::Manifest(identity, Box::new(definition)))
            }
            Some("module.wasm") => {
                let mut bytes = Vec::with_capacity(metadata.len() as usize);
                file.read_to_end(&mut bytes).await?;
                Ok(Self::Module(bytes.into()))
            }
            Some(_)
                if path
                    .extension()
                    .is_some_and(|extension| extension == "chain_block") =>
            {
                tc_chain::ChainFile::load(path, file, metadata)
                    .await
                    .map(Self::Chain)
            }
            Some(_) => tc_collection::PersistentFile::load(path, file, metadata)
                .await
                .map(Self::Collection),
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing file name",
            )),
        }
    }
}

impl FileSave for ApplicationBlock {
    async fn save(&self, file: &mut tokio::fs::File) -> io::Result<u64> {
        #[cfg(test)]
        tensor_fault::before_save(self)?;

        let result = match self {
            Self::Collection(value) => value.save(file).await,
            Self::Chain(value) => value.save(file).await,
            Self::Manifest(identity, definition) => {
                let definition = destream::en::MapStream::from(futures::stream::iter([(
                    identity.to_string(),
                    definition,
                )]));

                let mut encoded = destream_json::encode(definition).map_err(|error| {
                    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
                })?;

                let mut size = 0;
                while let Some(chunk) = encoded.next().await {
                    let chunk = chunk.map_err(|error| io::Error::other(error.to_string()))?;
                    size = application_size(size, chunk.len() as u64)?;
                    file.write_all(&chunk).await?;
                }

                Ok(size)
            }
            Self::Module(bytes) => {
                application_size(0, bytes.len() as u64)?;
                file.write_all(bytes).await.map(|_| bytes.len() as u64)
            }
        };

        #[cfg(test)]
        if result.is_ok() {
            tensor_fault::after_save(self);
        }

        result
    }
}

#[cfg(test)]
#[path = "../../tests/support/tensor_save_fault.rs"]
pub(crate) mod tensor_fault;
#[cfg(test)]
#[path = "../../tests/support/native_storage.rs"]
mod tests;
