use std::{io, path::Path};

use freqfs::{FileLoad, FileSave};
use get_size::GetSize;
use safecast::AsType;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[derive(Clone)]
pub(crate) enum ApplicationFile {
    Manifest(std::sync::Arc<[u8]>),
    Module(std::sync::Arc<[u8]>),
}

impl AsType<ApplicationFile> for ApplicationFile {
    fn as_type(&self) -> Option<&ApplicationFile> {
        Some(self)
    }

    fn as_type_mut(&mut self) -> Option<&mut ApplicationFile> {
        Some(self)
    }

    fn into_type(self) -> Option<ApplicationFile> {
        Some(self)
    }
}

impl GetSize for ApplicationFile {
    fn get_size(&self) -> usize {
        match self {
            Self::Manifest(bytes) | Self::Module(bytes) => bytes.len(),
        }
    }
}

impl FileLoad for ApplicationFile {
    async fn load(
        path: &Path,
        mut file: tokio::fs::File,
        _metadata: std::fs::Metadata,
    ) -> io::Result<Self> {
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        match path.file_name().and_then(|name| name.to_str()) {
            Some("manifest.json") => Ok(Self::Manifest(bytes.into())),
            Some("module.wasm") => Ok(Self::Module(bytes.into())),
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

impl FileSave for ApplicationFile {
    async fn save(&self, file: &mut tokio::fs::File) -> io::Result<u64> {
        match self {
            Self::Manifest(bytes) | Self::Module(bytes) => {
                file.write_all(bytes).await?;
                Ok(bytes.len() as u64)
            }
        }
    }
}
