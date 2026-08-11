use pathlink::PathSegment;
use tc_error::TCResult;
use tc_ir::{Dir, Route};

#[derive(Clone, Debug, Default)]
pub struct RouteMetadata {
    pub export: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct SchemaRoutes {
    dir: Dir<RouteMetadata>,
}

impl SchemaRoutes {
    pub fn new() -> Self {
        Self { dir: Dir::new() }
    }

    pub fn from_entries(entries: Vec<(Vec<PathSegment>, RouteMetadata)>) -> TCResult<Self> {
        let dir = Dir::from_routes(entries)?;
        Ok(Self { dir })
    }
}

impl Route<crate::State> for SchemaRoutes {
    type Handler = RouteMetadata;

    fn route(&self, path: &[PathSegment]) -> Option<Self::Handler> {
        self.dir.route(path)
    }
}
