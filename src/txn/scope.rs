use pathlink::Link;
use std::collections::{BTreeMap, BTreeSet};
use tc_ir::Method;

pub(crate) type Requirements = BTreeMap<Link, BTreeSet<Method>>;

#[derive(Clone)]
pub(crate) struct DependencyScope {
    identity: Link,
    dependencies: Requirements,
}

impl DependencyScope {
    pub(crate) fn new(identity: Link, dependencies: Requirements) -> Self {
        Self {
            identity,
            dependencies,
        }
    }

    pub(crate) fn authorize(&self, target: &Link, method: Method) -> bool {
        if target == &self.identity {
            return true;
        }
        self.dependencies
            .get(target)
            .is_some_and(|methods| methods.contains(&method))
    }
}
