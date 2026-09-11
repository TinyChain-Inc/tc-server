#[test]
fn bearer_header_has_one_transport_neutral_parser() {
    assert_eq!(tinychain::auth::bearer_token("Bearer token"), Some("token"));
    assert_eq!(
        tinychain::auth::bearer_token("bearer   token  "),
        Some("token")
    );
    assert_eq!(tinychain::auth::bearer_token("Basic token"), None);
    assert_eq!(tinychain::auth::bearer_token("Bearer   "), None);
}

#[test]
fn actor_directory_rejects_conflicting_keys() {
    let directory = tinychain::auth::KeyringActorResolver::default();
    let host: pathlink::Link = "/host".parse().expect("host link");
    let first = tinychain::auth::Actor::new_falcon512("actor".into()).expect("first actor");
    let second = tinychain::auth::Actor::new_falcon512("actor".into()).expect("second actor");
    directory
        .insert(host.clone(), first)
        .expect("first key is accepted");
    assert!(directory.insert(host, second).is_err());
}
