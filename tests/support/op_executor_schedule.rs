use tc_ir::{IdRef, Scalar, TCRef};

use super::*;

fn dependency(id: &str) -> Scalar {
    Scalar::from(TCRef::Id(id.parse::<IdRef>().expect("IdRef")))
}

#[test]
fn provider_discovery_is_transitive_and_ordered() {
    let bindings = BTreeMap::from([
        ("a".parse().unwrap(), dependency("$b")),
        ("b".parse().unwrap(), Scalar::from(1_u64)),
    ]);

    let schedule =
        Schedule::build(&"a".parse().unwrap(), &bindings, &HashMap::new(), 10, 10).unwrap();

    assert_eq!(
        schedule
            .indegree
            .into_keys()
            .map(|id| id.to_string())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
}

#[test]
fn provider_discovery_rejects_missing_inputs_and_cycles() {
    let missing = BTreeMap::from([("a".parse().unwrap(), dependency("$missing"))]);
    let error =
        Schedule::build(&"a".parse().unwrap(), &missing, &HashMap::new(), 10, 10).unwrap_err();

    assert!(error.to_string().contains("missing input value"));

    let cyclic = BTreeMap::from([
        ("a".parse().unwrap(), dependency("$b")),
        ("b".parse().unwrap(), dependency("$a")),
    ]);

    let schedule =
        Schedule::build(&"a".parse().unwrap(), &cyclic, &HashMap::new(), 10, 10).unwrap();

    assert!(schedule.ready.is_empty());
}

#[test]
fn scheduler_bounds_edges_and_handles_a_deep_chain_iteratively() {
    let fan_in = BTreeMap::from([
        (
            "result".parse().unwrap(),
            Scalar::Tuple(vec![dependency("$left"), dependency("$right")]),
        ),
        ("left".parse().unwrap(), Scalar::from(1_u64)),
        ("right".parse().unwrap(), Scalar::from(2_u64)),
    ]);

    let error =
        Schedule::build(&"result".parse().unwrap(), &fan_in, &HashMap::new(), 10, 1).unwrap_err();

    assert!(error.to_string().contains("1-edge limit"));

    let mut chain = BTreeMap::new();
    chain.insert("n0000".parse().unwrap(), Scalar::from(0_u64));
    for index in 1..4_096 {
        let id: Id = format!("n{index:04}").parse().unwrap();
        chain.insert(id, dependency(&format!("$n{:04}", index - 1)));
    }

    let schedule = Schedule::build(
        &"n4095".parse().unwrap(),
        &chain,
        &HashMap::new(),
        4_096,
        4_095,
    )
    .expect("iterative chain planning");

    assert_eq!(schedule.indegree.len(), 4_096);
    assert_eq!(schedule.ready.len(), 1);
}
