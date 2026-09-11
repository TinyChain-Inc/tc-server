use std::sync::Arc;

use freqfs::Cache;
use tc_error::{TCError, TCResult};

use crate::Workspace;

mod file;
mod workspace;

pub(crate) use file::ApplicationBlock;
pub use workspace::{AuthorityRecord, ControlFile};

const CLASS_ROOT: &str = "class";
const LIB_ROOT: &str = "lib";
const SERVICE_ROOT: &str = "service";

pub struct ApplicationRoots {
    classes: txfs::Dir<tc_ir::TxnId, ApplicationBlock>,
    libraries: txfs::Dir<tc_ir::TxnId, ApplicationBlock>,
    services: txfs::Dir<tc_ir::TxnId, ApplicationBlock>,
}

impl ApplicationRoots {
    pub(crate) fn into_parts(
        self,
    ) -> (
        txfs::Dir<tc_ir::TxnId, ApplicationBlock>,
        txfs::Dir<tc_ir::TxnId, ApplicationBlock>,
        txfs::Dir<tc_ir::TxnId, ApplicationBlock>,
    ) {
        (self.classes, self.libraries, self.services)
    }
}

#[derive(Clone)]
pub struct HostStorage {
    control: Arc<Cache<ControlFile>>,
    workspace: Arc<Cache<tc_collection::PersistentFile>>,
    data: Arc<Cache<ApplicationBlock>>,
}

impl HostStorage {
    pub fn new(limits: &crate::StorageLimits) -> Self {
        Self {
            control: Cache::new(
                1024 * 1024,
                Some(8),
                limits.minimum_free_disk_bytes,
                limits.cache_wait,
            ),
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
        // DIRECT_FS_BOOTSTRAP: no cache owns these paths yet. Create them once,
        // then publish the loaded freqfs roots and use only their handles.
        std::fs::create_dir_all(path.as_ref()).map_err(TCError::from)?;
        let control_path = path.as_ref().join("control");
        let transaction_path = path.as_ref().join("txn");
        std::fs::create_dir_all(&control_path).map_err(TCError::from)?;
        std::fs::create_dir_all(&transaction_path).map_err(TCError::from)?;
        let control = Arc::clone(&self.control)
            .load(control_path)
            .map_err(TCError::from)?;
        let transactions = Arc::clone(&self.workspace)
            .load(transaction_path)
            .map_err(TCError::from)?;
        Ok(Workspace::from_roots(control, transactions))
    }

    pub async fn application_roots(
        &self,
        path: impl AsRef<std::path::Path>,
    ) -> TCResult<ApplicationRoots> {
        // DIRECT_FS_BOOTSTRAP: freqfs requires its root to exist before load;
        // all access beneath this root uses the resulting cache handle.
        std::fs::create_dir_all(path.as_ref()).map_err(TCError::from)?;
        let data = Arc::clone(&self.data)
            .load(path.as_ref().to_path_buf())
            .map_err(TCError::from)?;
        let (classes, libraries, services) = {
            let mut data = data.write().await;
            (
                data.get_or_create_dir(CLASS_ROOT.to_string())
                    .map_err(TCError::from)?,
                data.get_or_create_dir(LIB_ROOT.to_string())
                    .map_err(TCError::from)?,
                data.get_or_create_dir(SERVICE_ROOT.to_string())
                    .map_err(TCError::from)?,
            )
        };

        let (classes, libraries, services) = tokio::try_join!(
            txfs::Dir::load(classes),
            txfs::Dir::load(libraries),
            txfs::Dir::load(services),
        )
        .map_err(TCError::from)?;

        Ok(ApplicationRoots {
            classes,
            libraries,
            services,
        })
    }
}
