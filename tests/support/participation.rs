use std::collections::BTreeSet;
use std::sync::Arc;

use futures::FutureExt;
use pathlink::{Link, PathLabel, path_label};
use tc_error::ErrorKind;

use super::Participation;

const FIRST: PathLabel = path_label(&["service", "test", "first", "1.0.0"]);

const SECOND: PathLabel = path_label(&["service", "test", "second", "1.0.0"]);

fn participation() -> Participation {
    Participation::new(
        ("owner-host".into(), "owner-actor".into()),
        ("leader-host".into(), "leader-actor".into()),
    )
}

#[test]
fn sealing_active_calls_prevents_late_work_and_retains_dependencies() {
    let participant = participation();
    let first = Link::from(FIRST);
    participant.enlist(first.clone(), 2).unwrap();
    let call = participant.admit().unwrap();

    let error = participant.seal().unwrap_err();
    assert_eq!(error.code(), ErrorKind::Conflict);
    assert!(error.message().contains("active calls"));
    assert_eq!(participant.admit().unwrap_err().code(), ErrorKind::Conflict);
    assert_eq!(
        participant.enlist(SECOND.into(), 2).unwrap_err().code(),
        ErrorKind::Conflict
    );

    drop(call);
    let dependencies = BTreeSet::from([first]);
    assert_eq!(participant.seal().unwrap(), dependencies);
    assert_eq!(participant.seal().unwrap(), dependencies);
    assert_eq!(participant.admit().unwrap_err().code(), ErrorKind::Conflict);
}

#[test]
fn dependency_limit_counts_distinct_attempts() {
    let participant = participation();
    let first = Link::from(FIRST);
    participant.enlist(first.clone(), 1).unwrap();
    participant.enlist(first.clone(), 1).unwrap();

    let error = participant.enlist(SECOND.into(), 1).unwrap_err();
    assert_eq!(error.code(), ErrorKind::BadRequest);
    assert!(error.message().contains("dependency limit"));
    assert_eq!(participant.seal().unwrap(), BTreeSet::from([first]));
}

#[tokio::test]
async fn completion_and_cancellation_release_call_tokens() {
    let participant = participation();
    let call = participant.admit().unwrap();
    let (release, wait) = tokio::sync::oneshot::channel::<()>();
    let operation = async move {
        let _call = call;
        wait.await.unwrap();
    };

    let mut operation = std::pin::pin!(operation);
    assert!(operation.as_mut().now_or_never().is_none());
    assert!(!participant.work.lock().calls.is_empty());
    release.send(()).unwrap();
    operation.await;
    assert!(participant.work.lock().calls.is_empty());

    {
        let call = participant.admit().unwrap();
        let operation = async move {
            let _call = call;
            futures::future::pending::<()>().await;
        };

        let mut operation = std::pin::pin!(operation);
        assert!(operation.as_mut().now_or_never().is_none());
    }

    assert!(participant.work.lock().calls.is_empty());

    let call = participant.admit().unwrap();
    let operation = async move {
        let _call = call;
        futures::future::pending::<()>().await;
    };

    drop(operation);
    assert!(participant.seal().unwrap().is_empty());
}

#[test]
fn admission_and_sealing_share_one_boundary() {
    let participant = Arc::new(participation());
    let ready = Arc::new(std::sync::Barrier::new(2));
    let attempt = std::thread::spawn({
        let participant = Arc::clone(&participant);
        let ready = Arc::clone(&ready);
        move || {
            ready.wait();
            participant.admit()
        }
    });

    ready.wait();
    let sealed = participant.seal();
    let admission = attempt.join().unwrap();
    match admission {
        Ok(call) => {
            assert_eq!(sealed.unwrap_err().code(), ErrorKind::Conflict);
            drop(call);
        }
        Err(error) => {
            assert_eq!(error.code(), ErrorKind::Conflict);
            assert!(sealed.unwrap().is_empty());
        }
    }

    assert!(participant.seal().unwrap().is_empty());
    assert_eq!(participant.admit().unwrap_err().code(), ErrorKind::Conflict);
}

#[test]
fn retained_call_tokens_do_not_retain_the_resource_owner() {
    let participant = Arc::new(participation());
    let owner = Arc::downgrade(&participant);
    let call = participant.admit().unwrap();
    drop(participant);
    assert!(owner.upgrade().is_none());
    drop(call);
}
