use std::sync::OnceLock;

use parking_lot::Mutex;
use tc_error::{ErrorKind, TCError, TCResult};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;

use crate::Deadline;

/// Admission and closure are serialized; Tokio owns completion accounting.
pub(super) struct Drain {
    requests: Mutex<TaskTracker>,
    stopping: CancellationToken,
    deadline: OnceLock<Deadline>,
}

impl Drain {
    pub(super) fn new() -> Self {
        Self {
            requests: Mutex::new(TaskTracker::new()),
            stopping: CancellationToken::new(),
            deadline: OnceLock::new(),
        }
    }

    pub(super) fn admit(&self) -> TCResult<TaskTrackerToken> {
        let requests = self.requests.lock();
        if requests.is_closed() {
            return Err(TCError::new(
                ErrorKind::Unavailable,
                "host is shutting down",
            ));
        }

        Ok(requests.token())
    }

    pub(super) fn begin(&self, deadline: impl FnOnce() -> Deadline) {
        {
            let requests = self.requests.lock();
            self.deadline.get_or_init(deadline);
            requests.close();
        }

        self.stopping.cancel();
    }

    pub(super) fn is_stopping(&self) -> bool {
        self.deadline.get().is_some()
    }

    #[cfg(feature = "http-server")]
    pub(super) async fn stopped(&self) -> Deadline {
        self.stopping.cancelled().await;
        *self
            .deadline
            .get()
            .expect("shutdown publishes its deadline before notifying")
    }

    pub(super) async fn wait(&self) {
        let requests = self.requests.lock().clone();
        requests.wait().await;
    }
}

#[cfg(test)]
#[path = "../../tests/support/drain.rs"]
mod tests;
