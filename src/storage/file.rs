use std::{io, path::Path};

use freqfs::{FileLoad, FileSave};
use get_size::GetSize;
use safecast::AsType;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone)]
pub(crate) enum LibraryFile {
    Bytes(Vec<u8>),
}

impl LibraryFile {
    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            Self::Bytes(bytes) => bytes,
        }
    }
}

impl AsType<LibraryFile> for LibraryFile {
    fn as_type(&self) -> Option<&LibraryFile> {
        Some(self)
    }

    fn as_type_mut(&mut self) -> Option<&mut LibraryFile> {
        Some(self)
    }

    fn into_type(self) -> Option<LibraryFile> {
        Some(self)
    }
}

impl GetSize for LibraryFile {
    fn get_size(&self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes.len(),
        }
    }
}

impl FileLoad for LibraryFile {
    async fn load(
        _path: &Path,
        mut file: tokio::fs::File,
        _metadata: std::fs::Metadata,
    ) -> io::Result<Self> {
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        Ok(Self::Bytes(bytes))
    }
}

impl FileSave for LibraryFile {
    async fn save(&self, file: &mut tokio::fs::File) -> io::Result<u64> {
        let bytes = self.bytes();
        file.write_all(bytes).await?;
        Ok(bytes.len() as u64)
    }
}
