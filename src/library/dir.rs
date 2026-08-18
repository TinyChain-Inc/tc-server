use std::collections::BTreeMap;

use tc_error::{TCError, TCResult};

/// A structural component namespace.
///
/// Unlike the handler-only `tc_ir::Dir`, leaves in this directory may also
/// have children: a mounted component root can be resolved while its remaining
/// path is delegated to that component. This is the v1 `Dir<Library>` /
/// `Dir<Class>` ownership model, without a parallel flat path index.
#[derive(Clone, Default)]
pub(super) struct Dir<T> {
    root: Node<T>,
    len: usize,
}

#[derive(Clone)]
struct Node<T> {
    value: Option<T>,
    children: BTreeMap<String, Node<T>>,
}

impl<T> Default for Node<T> {
    fn default() -> Self {
        Self {
            value: None,
            children: BTreeMap::new(),
        }
    }
}

impl<T> Dir<T> {
    pub fn new() -> Self {
        Self {
            root: Node {
                value: None,
                children: BTreeMap::new(),
            },
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn insert(&mut self, path: &str, value: T) -> TCResult<Option<T>> {
        let segments = segments(path)?;
        let mut node = &mut self.root;
        for segment in segments {
            node = node.children.entry(segment.to_string()).or_default();
        }

        let previous = node.value.replace(value);
        if previous.is_none() {
            self.len += 1;
        }
        Ok(previous)
    }

    pub fn get(&self, path: &str) -> Option<&T> {
        self.node(path).and_then(|node| node.value.as_ref())
    }

    pub fn resolve(&self, path: &str) -> Option<(&T, String, bool)> {
        let segments = segments(path).ok()?;
        let mut node = &self.root;
        let mut best = node.value.as_ref().map(|value| (value, 0));

        for (index, segment) in segments.iter().enumerate() {
            let Some(child) = node.children.get(*segment) else {
                break;
            };
            node = child;
            if let Some(value) = node.value.as_ref() {
                best = Some((value, index + 1));
            }
        }

        let (value, depth) = best?;
        let root = format!("/{}", segments[..depth].join("/"));
        Some((value, root, depth == segments.len()))
    }

    pub fn list(&self, path: &str) -> Option<Vec<(&str, bool)>> {
        let node = self.node(path)?;
        Some(
            node.children
                .iter()
                .map(|(name, child)| (name.as_str(), !child.children.is_empty()))
                .collect(),
        )
    }

    pub fn values(&self) -> Vec<&T> {
        let mut values = Vec::with_capacity(self.len);
        self.root.collect_values(&mut values);
        values
    }

    fn node(&self, path: &str) -> Option<&Node<T>> {
        let mut node = &self.root;
        for segment in segments(path).ok()? {
            node = node.children.get(segment)?;
        }
        Some(node)
    }
}

impl<T> Node<T> {
    fn collect_values<'a>(&'a self, values: &mut Vec<&'a T>) {
        if let Some(value) = self.value.as_ref() {
            values.push(value);
        }
        for child in self.children.values() {
            child.collect_values(values);
        }
    }
}

fn segments(path: &str) -> TCResult<Vec<&str>> {
    if !path.starts_with('/') {
        return Err(TCError::bad_request("component path must be absolute"));
    }
    let segments = path
        .trim_end_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.is_empty() {
        return Err(TCError::bad_request("component path must not be root"));
    }
    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::Dir;

    #[test]
    fn resolves_the_longest_component_root_structurally() {
        let mut dir = Dir::new();
        dir.insert("/lib/acme/math/1.0.0", 1).expect("insert");
        dir.insert("/lib/acme/math/1.0.0/nested/2.0.0", 2)
            .expect("insert nested");

        assert_eq!(
            dir.resolve("/lib/acme/math/1.0.0/nested/2.0.0/add"),
            Some((&2, "/lib/acme/math/1.0.0/nested/2.0.0".into(), false))
        );
    }

    #[test]
    fn lists_direct_children_without_scanning_all_leaves() {
        let mut dir = Dir::new();
        dir.insert("/class/acme/one/1.0.0", 1).expect("insert one");
        dir.insert("/class/acme/two/1.0.0", 2).expect("insert two");

        assert_eq!(
            dir.list("/class/acme"),
            Some(vec![("one", true), ("two", true)])
        );
    }
}
