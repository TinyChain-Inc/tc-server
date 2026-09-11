use super::*;

#[test]
fn resolves_nested_literal_members_without_a_second_directory_type() {
    let mut nested = Map::new();
    nested.insert(
        "answer".parse().unwrap(),
        Scalar::from(tc_value::Value::from(42_u64)),
    );
    let mut members = Map::new();
    members.insert("nested".parse().unwrap(), Scalar::Map(nested));
    let analysis = compile_ir_library(Scalar::Map(members)).unwrap();
    let path = ["nested".parse().unwrap(), "answer".parse().unwrap()];
    assert!(member(&analysis.members, &path).is_some());
}

#[test]
fn dependency_policy_classifies_only_application_links() {
    let application: Link = "/lib/example-devco/math/1.0.0".parse().unwrap();
    let reference = |target| {
        Scalar::from(tc_ir::TCRef::Op(OpRef::Get((
            tc_ir::Subject::Link(target),
            Scalar::default(),
        ))))
    };
    let values = [
        reference(application.clone()),
        reference("/state/scalar/number".parse().unwrap()),
    ];
    let requirements = application_requirements(values.iter());
    assert_eq!(requirements.len(), 1);
    assert!(requirements[&application].contains(&tc_ir::Method::Get));
}
