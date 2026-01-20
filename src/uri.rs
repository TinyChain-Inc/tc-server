pub(crate) fn normalize_path(path: &str) -> &str {
    if path.len() > 1 {
        path.trim_end_matches('/')
    } else {
        path
    }
}

pub(crate) fn component_root(path: &str) -> Option<&str> {
    let path = normalize_path(path);

    if path == "/lib" {
        return Some(path);
    }

    if path == "/service" {
        return Some(path);
    }

    if path.starts_with("/lib/") {
        return component_root_with_segments(path, 4).or(Some("/lib"));
    }

    if path.starts_with("/service/") {
        return component_root_with_segments(path, 5).or(Some("/service"));
    }

    None
}

fn component_root_with_segments(path: &str, segments: usize) -> Option<&str> {
    debug_assert!(path.starts_with('/'));

    let mut slash_indices = [0usize; 6];
    let mut slash_count = 0usize;

    for (idx, byte) in path.as_bytes().iter().enumerate() {
        if *byte == b'/' {
            if slash_count < slash_indices.len() {
                slash_indices[slash_count] = idx;
            }
            slash_count += 1;
        }
    }

    if slash_count < segments {
        return None;
    }

    let end = if slash_count > segments {
        slash_indices[segments]
    } else {
        path.len()
    };

    Some(&path[..end])
}
