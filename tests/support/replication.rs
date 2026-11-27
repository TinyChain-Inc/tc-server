use super::*;

#[tokio::test]
async fn fanout_interprets_failures_in_endpoint_order() {
    let peers = std::collections::BTreeSet::from(["a".to_string(), "b".to_string()]);
    let result = fanout_attempt(&peers, |peer| async move {
        if peer == "a" {
            tokio::task::yield_now().await;
        }

        Err(TCError::bad_gateway(peer))
    })
    .await
    .expect("fanout");

    assert_eq!(
        result
            .complete("fanout failed")
            .expect_err("failure")
            .message(),
        "a"
    );
}

#[test]
fn fanout_derives_replica_evictions_only_from_a_strict_majority() {
    let majority = Fanout {
        outcomes: std::collections::BTreeMap::from([
            ("a".to_string(), Ok(())),
            ("b".to_string(), Err(TCError::bad_gateway("b"))),
        ]),
    }
    .strict_majority()
    .expect("local host and one peer form a strict majority of three");

    assert_eq!(
        majority.delivered(),
        &std::collections::BTreeSet::from(["a".to_string()])
    );

    assert_eq!(
        majority.failed(),
        &std::collections::BTreeSet::from(["b".to_string()])
    );

    let tie = Fanout {
        outcomes: std::collections::BTreeMap::from([
            ("a".to_string(), Ok(())),
            ("b".to_string(), Err(TCError::bad_gateway("b"))),
            ("c".to_string(), Err(TCError::bad_gateway("c"))),
        ]),
    };

    assert!(tie.strict_majority().is_err());
}
