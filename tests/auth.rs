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
