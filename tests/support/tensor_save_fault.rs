//! Task-scoped fault injection for one Service's canonical Tensor publication.

use std::cell::Cell;
use std::io;

use super::ApplicationBlock;

// The scope surrounds only the selected Service's finalization. No background
// task inherits this observer, and concurrent test scopes have independent state.
tokio::task_local! {
    pub(crate) static SAVED_PAYLOADS: Cell<usize>;
    pub(crate) static FAIL_NATIVE: bool;
    pub(crate) static EVIDENCE: std::path::PathBuf;
}

pub(crate) fn before_save(value: &ApplicationBlock) -> io::Result<()> {
    let target = FAIL_NATIVE
        .try_with(|native| {
            if *native {
                matches!(
                    value,
                    ApplicationBlock::Collection(tc_collection::PersistentFile::Metadata(_))
                )
            } else {
                matches!(value, ApplicationBlock::Chain(_))
            }
        })
        .unwrap_or(false);

    if target
        && SAVED_PAYLOADS
            .try_with(|saved| saved.get() > 0)
            .unwrap_or(false)
    {
        EVIDENCE.with(|path| {
            std::fs::write(path, SAVED_PAYLOADS.with(|saved| saved.get()).to_string())
        })?;

        return Err(io::Error::other(
            "injected Tensor finalization publication failure",
        ));
    }

    Ok(())
}

pub(crate) fn after_save(value: &ApplicationBlock) {
    if matches!(
        value,
        ApplicationBlock::Collection(
            tc_collection::PersistentFile::U64(_) | tc_collection::PersistentFile::SparseU64(_)
        )
    ) {
        // The task-local scope begins after both commits and initialization.
        let _ = SAVED_PAYLOADS.try_with(|saved| saved.set(saved.get() + 1));
    }
}
