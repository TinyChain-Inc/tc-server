use std::collections::BTreeSet;

use parking_lot::Mutex;
use pathlink::Link;
use tc_error::{TCError, TCResult};
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;

/// The exact resource retains attempted dependencies until cutoff, including
/// failed deliveries. Tokens cover whole calls, not individual Chain mutations.
pub(crate) struct Participation {
    pub(super) owner: (String, String),
    pub(super) leader: (String, String),
    work: Mutex<Work>,
}

struct Work {
    calls: TaskTracker,
    dependencies: BTreeSet<Link>,
}

impl Participation {
    pub(super) fn new(owner: (String, String), leader: (String, String)) -> Self {
        Self {
            owner,
            leader,
            work: Mutex::new(Work {
                calls: TaskTracker::new(),
                dependencies: BTreeSet::new(),
            }),
        }
    }

    pub(super) fn admit(&self) -> TCResult<TaskTrackerToken> {
        let work = self.work.lock();
        if work.calls.is_closed() {
            return Err(TCError::conflict("resource transaction is sealed"));
        }

        Ok(work.calls.token())
    }

    pub(crate) fn enlist(&self, target: Link, limit: usize) -> TCResult<()> {
        let mut work = self.work.lock();
        if work.calls.is_closed() {
            return Err(TCError::conflict("resource transaction is sealed"));
        }

        if !work.dependencies.contains(&target) && work.dependencies.len() >= limit {
            return Err(TCError::bad_request(
                "transaction dependency limit exceeded",
            ));
        }

        work.dependencies.insert(target);
        Ok(())
    }

    pub(super) fn seal(&self) -> TCResult<BTreeSet<Link>> {
        let work = self.work.lock();

        // Closure and admission share this lock. Closing a tracker alone would
        // still allow tokens, and a completed Chain task does not close a POST.
        work.calls.close();
        if !work.calls.is_empty() {
            return Err(TCError::conflict(
                "resource transaction still has active calls",
            ));
        }

        Ok(work.dependencies.clone())
    }
}

#[cfg(test)]
#[path = "../../tests/support/participation.rs"]
mod tests;
