use std::sync::Arc;
use std::time::Duration;

use crate::Deadline;
use super::Drain;

#[tokio::test]
async fn admission_closes_before_drain_and_tokens_release_on_drop() {
    let drain = Drain::new();
    let first = drain.admit().unwrap();
    let second = drain.admit().unwrap();
    drain.begin(|| {
        assert!(
            drain.requests.try_lock().is_none(),
            "deadline is captured under admission ownership"
        );

        Deadline::after(Duration::from_secs(1))
    });

    drain.begin(|| panic!("shutdown must retain its original deadline"));
    assert!(drain.is_stopping());
    assert!(drain.admit().is_err());
    let wait = drain.wait();
    tokio::pin!(wait);
    assert!(futures::poll!(&mut wait).is_pending());
    drop(first);
    assert!(futures::poll!(&mut wait).is_pending());
    drop(second);
    wait.await;
    drain.wait().await;
}

#[test]
fn admission_racing_closure_cannot_escape_tracking() {
    let drain = Arc::new(Drain::new());
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let other = drain.clone();
    let ready = barrier.clone();
    let thread = std::thread::spawn(move || {
        ready.wait();
        other.admit()
    });

    barrier.wait();
    drain.begin(|| Deadline::after(Duration::from_secs(1)));
    let token = thread.join().unwrap().ok();
    assert_eq!(drain.requests.lock().len(), usize::from(token.is_some()));
    assert!(drain.admit().is_err());
    drop(token);
    assert!(drain.requests.lock().is_empty());
}
