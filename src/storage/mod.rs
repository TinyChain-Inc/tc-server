use std::{io, sync::Arc};

use freqfs::Cache;
use tc_error::{TCError, TCResult};

use crate::Workspace;

mod file;
mod leaf;
mod workspace;

pub(crate) use file::ApplicationFile;
pub(crate) use leaf::{Leaf, MANIFEST};
pub use workspace::{HostRecord, WorkspaceFile};

const CLASS_ROOT: &str = "class";
const LIB_ROOT: &str = "lib";
const SERVICE_ROOT: &str = "service";

pub struct ApplicationRoots {
    classes: freqfs::DirLock<ApplicationFile>,
    libraries: freqfs::DirLock<ApplicationFile>,
    services: freqfs::DirLock<ApplicationFile>,
}

impl ApplicationRoots {
    pub(crate) fn into_parts(
        self,
    ) -> (
        freqfs::DirLock<ApplicationFile>,
        freqfs::DirLock<ApplicationFile>,
        freqfs::DirLock<ApplicationFile>,
    ) {
        (self.classes, self.libraries, self.services)
    }
}

#[derive(Clone)]
pub struct HostStorage {
    workspace: Arc<Cache<WorkspaceFile>>,
    data: Arc<Cache<ApplicationFile>>,
}

impl HostStorage {
    pub fn new(limits: &crate::StorageLimits) -> Self {
        Self {
            workspace: Cache::new(
                limits.collection_cache_bytes,
                Some(limits.collection_file_handles),
                limits.minimum_free_disk_bytes,
                limits.cache_wait,
            ),
            data: Cache::new(
                limits.library_cache_bytes,
                Some(limits.library_file_handles),
                limits.minimum_free_disk_bytes,
                limits.cache_wait,
            ),
        }
    }

    pub fn workspace(&self, path: impl AsRef<std::path::Path>) -> TCResult<Workspace> {
        std::fs::create_dir_all(path.as_ref()).map_err(map_io)?;
        let root = Arc::clone(&self.workspace)
            .load(path.as_ref().to_path_buf())
            .map_err(map_io)?;
        Ok(Workspace::from_root(root))
    }

    pub async fn application_roots(
        &self,
        path: impl AsRef<std::path::Path>,
    ) -> TCResult<ApplicationRoots> {
        std::fs::create_dir_all(path.as_ref()).map_err(map_io)?;
        let data = Arc::clone(&self.data)
            .load(path.as_ref().to_path_buf())
            .map_err(map_io)?;
        let (classes, libraries, services) = {
            let mut data = data.write().await;
            (
                data.get_or_create_dir(CLASS_ROOT.to_string())
                    .map_err(map_io)?,
                data.get_or_create_dir(LIB_ROOT.to_string())
                    .map_err(map_io)?,
                data.get_or_create_dir(SERVICE_ROOT.to_string())
                    .map_err(map_io)?,
            )
        };

        Ok(ApplicationRoots {
            classes,
            libraries,
            services,
        })
    }
}

pub(crate) fn map_io(error: io::Error) -> TCError {
    let message = error.to_string();
    if message.contains("unsupported application layout") {
        TCError::bad_request(message)
    } else {
        TCError::internal(message)
    }
}
