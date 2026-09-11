pub(crate) const HOST_ROOT: &str = "/host";
pub(crate) const HOST_HEALTH: &str = "/healthz";
pub(crate) const HOST_ROOT_PREFIX: &str = "/host/";
pub(crate) const HOST_METRICS: &str = "/host/metrics";
pub(crate) const HOST_PUBLIC_KEY: &str = "/host/public_key";
pub(crate) const HOST_AUTH_CONTEXT: &str = "/host/auth/context";
pub const HOST_TXN_PREFIX: &str = "/host/txn/";

pub(crate) fn application_root(target: &pathlink::Link) -> Option<&str> {
    let root = target.path().first()?.as_str();
    matches!(root, "lib" | "class" | "service").then_some(root)
}

pub(crate) fn is_application_root(target: &pathlink::Link) -> bool {
    target.path().len() == 1 && application_root(target).is_some()
}

pub(crate) fn validate_identity(
    identity: &pathlink::Link,
    expected_root: &str,
) -> tc_error::TCResult<Vec<tc_ir::Id>> {
    if identity.host().is_some() {
        return Err(tc_error::TCError::bad_request(
            "an installed application identity must be local",
        ));
    }
    let (root, segments, suffix) = split_application_link(identity)?;
    if root != expected_root || !suffix.is_empty() || !is_identity_segments(&segments) {
        return Err(tc_error::TCError::bad_request(format!(
            "expected an exact /{expected_root} application identity"
        )));
    }
    Ok(segments)
}

fn is_identity_segments(segments: &[tc_ir::Id]) -> bool {
    segments.len() >= 3
        && segments[..segments.len() - 1]
            .iter()
            .all(|segment| segment.as_str() != "replicas")
        && segments
            .last()
            .is_some_and(|version| semver::Version::parse(version.as_str()).is_ok())
}

pub(crate) fn application_identity(target: &pathlink::Link) -> tc_error::TCResult<pathlink::Link> {
    let (_, segments, _) = split_application_link(target)?;
    let Some(version) = segments.last() else {
        return Err(tc_error::TCError::bad_request(
            "application target has no version",
        ));
    };
    if semver::Version::parse(version.as_str()).is_err() {
        return Err(tc_error::TCError::bad_request(
            "application target has no terminal version",
        ));
    }
    let path = pathlink::PathBuf::from_slice(&target.path()[..segments.len() + 1]);
    Ok(match target.host().cloned() {
        Some(host) => pathlink::Link::new(host, path),
        None => path.into(),
    })
}

pub(crate) fn split_application_link(
    target: &pathlink::Link,
) -> tc_error::TCResult<(&str, Vec<tc_ir::Id>, &[pathlink::PathSegment])> {
    let path = target.path();
    let root = path
        .first()
        .ok_or_else(|| tc_error::TCError::bad_request("an application path is empty"))?
        .as_str();
    if !matches!(root, "lib" | "class" | "service") {
        return Err(tc_error::TCError::bad_request(format!(
            "unsupported application root /{root}"
        )));
    }
    let version = path[1..]
        .iter()
        .position(|segment| semver::Version::parse(segment.as_str()).is_ok())
        .map(|offset| offset + 1);
    let structural_end = version.map_or(path.len(), |version| version + 1);
    if version.is_some_and(|version| version < 3) {
        return Err(tc_error::TCError::bad_request(
            "an application identity requires a publisher and resource before its version",
        ));
    }
    let mut segments = Vec::with_capacity(structural_end.saturating_sub(1));
    for segment in &path[1..structural_end] {
        if matches!(segment.as_str(), ".txfs" | "replicas") {
            return Err(tc_error::TCError::bad_request(format!(
                "{} is a reserved application segment",
                segment.as_str()
            )));
        }
        segments.push(segment.as_str().parse().map_err(|error| {
            tc_error::TCError::bad_request(format!("invalid application path segment: {error}"))
        })?);
    }
    let suffix = version.map_or(&path[path.len()..], |version| &path[version + 1..]);
    Ok((root, segments, suffix))
}
pub(crate) fn transaction_path(txn_id: tc_ir::TxnId) -> String {
    format!("{HOST_TXN_PREFIX}{txn_id}")
}

#[cfg(feature = "http-client")]
pub(crate) fn append_kernel_txn_id(
    url: &mut url::Url,
    txn_id: tc_ir::TxnId,
) -> tc_error::TCResult<String> {
    if url
        .query_pairs()
        .any(|(key, _)| key.eq_ignore_ascii_case("txn_id"))
    {
        return Err(tc_error::TCError::bad_request(
            "outbound targets must not include txn_id; it is supplied by the kernel".to_string(),
        ));
    }

    url.query_pairs_mut()
        .append_pair("txn_id", &txn_id.to_string());

    Ok(url.as_str().to_string())
}

pub(crate) fn normalize_path(path: &str) -> &str {
    if path.len() > 1 {
        path.trim_end_matches('/')
    } else {
        path
    }
}
