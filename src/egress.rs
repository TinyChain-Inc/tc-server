use std::collections::{HashMap, HashSet};

use tc_error::TCError;
use tc_ir::LibrarySchema;

use crate::uri::{component_root, normalize_path};

#[derive(Clone, Debug, Default)]
pub struct EgressPolicy {
    deps: HashMap<String, DependencyRule>,
}

#[derive(Clone, Debug, Default)]
struct DependencyRule {
    authorities: HashSet<String>,
    default_authority: Option<String>,
}

impl EgressPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    /// Allow an absolute URI authority for the given canonical dependency root path.
    pub fn allow_authority(
        &mut self,
        dependency_root: impl Into<String>,
        authority: impl Into<String>,
    ) {
        let rule = self.deps.entry(dependency_root.into()).or_default();

        rule.authorities.insert(authority.into());
    }

    /// Configure a default authority used to resolve a path-only dependency URI.
    ///
    /// This also implicitly whitelists the same authority for that dependency.
    pub fn route_dependency(
        &mut self,
        dependency_root: impl Into<String>,
        authority: impl Into<String>,
    ) {
        let dependency_root = dependency_root.into();
        let authority = authority.into();
        let rule = self.deps.entry(dependency_root).or_default();

        rule.default_authority = Some(authority.clone());
        rule.authorities.insert(authority);
    }

    pub fn resolve_target(
        &self,
        caller_schema: &LibrarySchema,
        target: &str,
    ) -> Result<String, TCError> {
        let uri: http::Uri = target
            .parse()
            .map_err(|err| TCError::bad_request(format!("invalid target URI: {err}")))?;

        let path = normalize_path(uri.path());
        let root = component_root(path).ok_or_else(|| {
            TCError::bad_request("egress target must be a TinyChain component root or subpath")
        })?;

        let canonical_root = root.to_string();
        if !schema_allows_dependency(caller_schema, &canonical_root) {
            return Err(TCError::unauthorized(format!(
                "unauthorized dependency {canonical_root}"
            )));
        }

        let rule = self.deps.get(&canonical_root).ok_or_else(|| {
            TCError::unauthorized(format!("no egress route for {canonical_root}"))
        })?;

        if let Some(authority) = uri.authority().map(|a| a.to_string()) {
            if !rule.authorities.contains(&authority) {
                return Err(TCError::unauthorized(format!(
                    "unauthorized authority {authority} for {canonical_root}"
                )));
            }

            return Ok(target.to_string());
        }

        let authority = rule.default_authority.as_ref().ok_or_else(|| {
            TCError::unauthorized(format!(
                "no default authority configured for {canonical_root}"
            ))
        })?;

        let query = uri.query().unwrap_or("");
        let path_and_query = if query.is_empty() {
            path.to_string()
        } else {
            format!("{path}?{query}")
        };

        Ok(format!("http://{authority}{path_and_query}"))
    }
}

fn schema_allows_dependency(schema: &LibrarySchema, canonical_root: &str) -> bool {
    let self_id = schema.id().to_string();
    if self_id == canonical_root {
        return true;
    }

    schema
        .dependencies()
        .iter()
        .map(|link| link.to_string())
        .any(|dep| dep == canonical_root)
}
