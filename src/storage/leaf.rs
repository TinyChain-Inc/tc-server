use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use freqfs::{DirEntry, DirLock};
use get_size::GetSize;
use tc_error::{TCError, TCResult};

use super::{ApplicationFile, WorkspaceFile, map_io};

#[derive(Clone)]
pub(crate) struct Leaf {
    root: DirLock<ApplicationFile>,
    identity: pathlink::Link,
}

impl Leaf {
    pub(crate) fn new(root: DirLock<ApplicationFile>, identity: pathlink::Link) -> Self {
        Self { root, identity }
    }

    pub(crate) fn identity(&self) -> &pathlink::Link {
        &self.identity
    }

    pub(crate) async fn committed_dir(&self) -> TCResult<Option<DirLock<ApplicationFile>>> {
        structural(
            &self.root,
            &crate::application::identity_segments(&self.identity)?,
            false,
        )
        .await
    }

    pub(crate) async fn files(&self) -> TCResult<BTreeMap<String, ApplicationFile>> {
        let canonical = self
            .committed_dir()
            .await?
            .ok_or_else(|| TCError::not_found(self.identity.to_string()))?;
        let entries = canonical
            .read()
            .await
            .iter()
            .map(|(name, entry)| (name.clone(), entry.clone()))
            .collect::<Vec<_>>();
        let mut files = BTreeMap::new();
        for (name, entry) in entries {
            let DirEntry::File(file) = entry else {
                return Err(TCError::bad_request(format!(
                    "application leaf {} contains a directory",
                    self.identity
                )));
            };
            let file = file.read_owned::<ApplicationFile>().await.map_err(map_io)?;
            files.insert(name, file.clone());
        }
        Ok(files)
    }

    pub(crate) async fn stage_file(
        &self,
        staging: &DirLock<WorkspaceFile>,
        name: &str,
        file: WorkspaceFile,
    ) -> TCResult<()> {
        let staged = self.staged_files(staging).await?;
        if (name == DELETE && !staged.is_empty() && !staged.contains_key(DELETE))
            || (name != DELETE && staged.contains_key(DELETE))
        {
            return Err(TCError::conflict(
                "application resource has conflicting staged work",
            ));
        }
        if let Some(existing) = staging.read().await.get_file(name).cloned() {
            let existing = existing
                .read_owned::<WorkspaceFile>()
                .await
                .map_err(map_io)?;
            let same = match (&*existing, &file) {
                (WorkspaceFile::Manifest(left), WorkspaceFile::Manifest(right))
                | (WorkspaceFile::Module(left), WorkspaceFile::Module(right)) => left == right,
                (WorkspaceFile::Delete, WorkspaceFile::Delete) => true,
                _ => false,
            };
            if !same {
                return Err(TCError::conflict(format!(
                    "conflicting staged application file {name}"
                )));
            }
            return Ok(());
        }
        let size = file.get_size();
        staging
            .write()
            .await
            .create_file(name.to_string(), file, size)
            .await
            .map_err(map_io)?;
        staging.write().await.sync().await.map_err(map_io)
    }

    pub(crate) async fn staged_files(
        &self,
        dir: &DirLock<WorkspaceFile>,
    ) -> TCResult<BTreeMap<String, WorkspaceFile>> {
        staged_files(dir).await
    }

    pub(crate) async fn stage_manifest(
        &self,
        staging: &DirLock<WorkspaceFile>,
        manifest: Arc<[u8]>,
    ) -> TCResult<()> {
        self.stage_file(staging, MANIFEST, WorkspaceFile::Manifest(manifest))
            .await
    }

    pub(crate) async fn stage_delete(&self, staging: &DirLock<WorkspaceFile>) -> TCResult<()> {
        self.stage_file(staging, DELETE, WorkspaceFile::Delete)
            .await
    }

    pub(crate) async fn commit_manifest(&self, staging: &DirLock<WorkspaceFile>) -> TCResult<()> {
        let mut files = staged_files(staging).await?;
        if files.is_empty() {
            return Ok(());
        }
        if matches!(files.remove(DELETE), Some(WorkspaceFile::Delete)) {
            return if files.is_empty() {
                self.remove(&[MANIFEST]).await
            } else {
                Err(TCError::internal(
                    "mixed staged application deletion and install",
                ))
            };
        }
        let manifest = match files.remove(MANIFEST) {
            Some(WorkspaceFile::Manifest(bytes)) if files.is_empty() => bytes,
            _ => return Err(TCError::internal("invalid staged manifest-only layout")),
        };
        let canonical = self.prepare_publish(&[MANIFEST]).await?;
        self.publish(&canonical, MANIFEST, ApplicationFile::Manifest(manifest))
            .await
    }

    pub(crate) async fn prepare_publish(
        &self,
        expected: &[&str],
    ) -> TCResult<DirLock<ApplicationFile>> {
        let segments = crate::application::identity_segments(&self.identity)?;
        let canonical = structural(&self.root, &segments, true)
            .await?
            .ok_or_else(|| TCError::internal("failed to create application directory"))?;
        validate_committed_for_apply(&canonical, expected).await?;
        Ok(canonical)
    }

    pub(crate) async fn publish(
        &self,
        canonical: &DirLock<ApplicationFile>,
        name: &str,
        contents: ApplicationFile,
    ) -> TCResult<()> {
        write_canonical(canonical, name, contents).await
    }

    pub(crate) async fn remove(&self, expected: &[&str]) -> TCResult<()> {
        let segments = crate::application::identity_segments(&self.identity)?;
        let Some(canonical) = structural(&self.root, &segments, false).await? else {
            return Ok(());
        };
        expect_exact_files(&canonical, expected).await?;
        for depth in (1..=segments.len()).rev() {
            let Some(parent) = structural(&self.root, &segments[..depth - 1], false).await? else {
                break;
            };
            let name = segments[depth - 1].as_str();
            let Some(child) = parent.read().await.get_dir(name).cloned() else {
                continue;
            };
            if depth != segments.len() && !child.read().await.is_empty() {
                break;
            }
            let mut parent = parent.write().await;
            parent.delete(name).await;
            parent.sync().await.map_err(map_io)?;
        }
        Ok(())
    }
}

pub(crate) const DELETE: &str = "delete";
pub(crate) const MANIFEST: &str = "manifest.json";

async fn staged_files(dir: &DirLock<WorkspaceFile>) -> TCResult<BTreeMap<String, WorkspaceFile>> {
    let entries = dir
        .read()
        .await
        .iter()
        .map(|(name, entry)| (name.clone(), entry.clone()))
        .collect::<Vec<_>>();
    let mut files = BTreeMap::new();
    for (name, entry) in entries {
        let DirEntry::File(file) = entry else {
            return Err(TCError::internal("staged application contains a directory"));
        };
        let file = file.read_owned::<WorkspaceFile>().await.map_err(map_io)?;
        files.insert(name, file.clone());
    }
    Ok(files)
}

async fn write_canonical(
    dir: &DirLock<ApplicationFile>,
    name: &str,
    contents: ApplicationFile,
) -> TCResult<()> {
    if let Some(existing) = dir.read().await.get_file(name).cloned() {
        let existing = existing
            .read_owned::<ApplicationFile>()
            .await
            .map_err(map_io)?;
        let equal = match (&*existing, &contents) {
            (ApplicationFile::Manifest(left), ApplicationFile::Manifest(right))
            | (ApplicationFile::Module(left), ApplicationFile::Module(right)) => left == right,
            _ => false,
        };
        return equal
            .then_some(())
            .ok_or_else(|| TCError::conflict("committed application content changed"));
    }
    let size = contents.get_size();
    dir.write()
        .await
        .create_file(name.to_string(), contents, size)
        .await
        .map_err(map_io)?;
    dir.write().await.sync().await.map_err(map_io)
}

async fn validate_committed_for_apply(
    dir: &DirLock<ApplicationFile>,
    expected: &[&str],
) -> TCResult<()> {
    let actual = exact_files(dir).await?;
    let expected = expected.iter().map(ToString::to_string).collect();
    if actual.is_empty() || actual == expected {
        Ok(())
    } else {
        Err(TCError::bad_request(format!(
            "unsupported committed application layout: {actual:?}"
        )))
    }
}

async fn expect_exact_files(dir: &DirLock<ApplicationFile>, expected: &[&str]) -> TCResult<()> {
    let actual = exact_files(dir).await?;
    (actual == expected.iter().map(ToString::to_string).collect())
        .then_some(())
        .ok_or_else(|| TCError::bad_request(format!("unsupported application layout: {actual:?}")))
}

async fn exact_files(dir: &DirLock<ApplicationFile>) -> TCResult<BTreeSet<String>> {
    let mut files = BTreeSet::new();
    for (name, entry) in dir.read().await.iter() {
        if !entry.is_file() {
            return Err(TCError::bad_request(
                "an application leaf cannot contain a directory",
            ));
        }
        files.insert(name.clone());
    }
    Ok(files)
}

pub(crate) async fn structural(
    root: &DirLock<ApplicationFile>,
    segments: &[tc_ir::Id],
    create: bool,
) -> TCResult<Option<DirLock<ApplicationFile>>> {
    let mut dir = root.clone();
    for segment in segments {
        if dir.read().await.get_file(segment.as_str()).is_some() {
            return Err(TCError::bad_request(format!(
                "unsupported application layout: {segment} is a file"
            )));
        }
        let next = if create {
            dir.write()
                .await
                .get_or_create_dir(segment.to_string())
                .map_err(map_io)?
        } else {
            let next = dir.read().await.get_dir(segment.as_str()).cloned();
            let Some(next) = next else {
                return Ok(None);
            };
            next
        };
        dir = next;
    }
    Ok(Some(dir))
}
