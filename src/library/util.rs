use pathlink::Link;
use tc_ir::LibrarySchema;

use crate::uri;

pub(super) fn schemas_equivalent(left: &LibrarySchema, right: &LibrarySchema) -> bool {
    left.version() == right.version()
        && canonical_link(left.id()) == canonical_link(right.id())
        && canonical_links(left.dependencies()) == canonical_links(right.dependencies())
}

pub(super) fn canonical_links(links: &[Link]) -> Vec<String> {
    let mut out = links.iter().map(canonical_link).collect::<Vec<_>>();
    out.sort();
    out
}

pub(super) fn canonical_link(link: &Link) -> String {
    let raw = link.to_string();
    if raw.starts_with('/') {
        return normalize_path(&raw);
    }

    // Accept both path-only and absolute link string forms by comparing on the path suffix.
    // This keeps the `/lib` install payload stable even if a `Link` string round-trip adds a
    // scheme or authority prefix.
    match raw.split_once("://") {
        Some((_, rest)) => rest
            .find('/')
            .map(|idx| normalize_path(&rest[idx..]))
            .unwrap_or_else(|| normalize_path(&raw)),
        None => normalize_path(&raw),
    }
}

pub(super) fn normalize_path(path: &str) -> String {
    uri::normalize_path(path).to_string()
}

pub(super) fn is_path_prefix(prefix: &str, path: &str) -> bool {
    let prefix = normalize_path(prefix);
    let path = normalize_path(path);
    path == prefix || path.starts_with(&format!("{prefix}/"))
}
