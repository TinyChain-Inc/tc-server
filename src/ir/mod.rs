use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;
use pathlink::Link;
use serde::Deserialize;
use serde_json::value::RawValue;
use tc_error::{TCError, TCResult};
use tc_ir::{Handler, Map, NativeClass, OpDef, OpRef, Route, Scalar, Subject, parse_route_path};
use tc_state::{ClassDef, ClassParent, StateType};

use crate::State;
use crate::library::{CompiledLibrary, LibraryExecution, RouteMetadata, SchemaRoutes};
use crate::storage::{Artifact, decode_schema_bytes};

pub const IR_ARTIFACT_CONTENT_TYPE: &str = "application/tinychain+json";
pub const WASM_ARTIFACT_CONTENT_TYPE: &str = "application/wasm";

#[derive(Deserialize)]
struct IrManifest {
    schema: Box<RawValue>,
    #[serde(default)]
    classes: Vec<IrClass>,
    routes: Vec<IrRoute>,
}

#[derive(Deserialize)]
struct IrClass {
    id: String,
    parent: String,
    #[serde(default)]
    prototype: HashMap<String, Box<RawValue>>,
}

#[derive(Deserialize)]
struct IrRoute {
    path: String,
    #[serde(default)]
    value: Option<Box<RawValue>>,
    #[serde(default)]
    op: Option<IrOp>,
    #[serde(default)]
    opdef: Option<Box<RawValue>>,
}

#[derive(Deserialize)]
struct IrOp {
    method: String,
    path: String,
}

#[derive(Clone)]
enum RouteImpl {
    Value(State),
    Op(Box<OpRef>),
    OpDef(OpDef),
}

#[derive(Clone)]
pub struct IrRoutes {
    routes: Arc<HashMap<Vec<pathlink::PathSegment>, IrHandler>>,
}

#[derive(Clone)]
pub struct IrHandler {
    schema: Link,
    route: RouteImpl,
}

impl Route<State> for IrRoutes {
    fn route(&self, path: &[pathlink::PathSegment]) -> Option<Box<dyn Handler<State> + '_>> {
        self.routes
            .get(path)
            .cloned()
            .map(|handler| Box::new(handler) as Box<dyn Handler<State>>)
    }
}

#[tc_ir::async_trait]
impl Handler<State> for IrHandler {
    async fn get(&self, txn: &crate::TxnHandle, key: Scalar) -> TCResult<State> {
        match self.route.clone() {
            RouteImpl::Value(state) => Ok(state),
            RouteImpl::Op(op) => crate::resolve::resolve(tc_ir::TCRef::Op(*op), txn).await,
            RouteImpl::OpDef(opdef @ OpDef::Get(_)) => {
                crate::op_executor::execute_get_with_self(
                    txn,
                    opdef,
                    key,
                    Some(self.schema.clone()),
                )
                .await
            }
            _ => Err(TCError::method_not_allowed("GET", self.schema.to_string())),
        }
    }

    async fn put(&self, txn: &crate::TxnHandle, key: Scalar, value: State) -> TCResult<()> {
        match self.route.clone() {
            RouteImpl::OpDef(opdef @ OpDef::Put(_)) => {
                crate::op_executor::execute_put_with_self(
                    txn,
                    opdef,
                    key,
                    value,
                    Some(self.schema.clone()),
                )
                .await
            }
            _ => Err(TCError::method_not_allowed("PUT", self.schema.to_string())),
        }
    }

    async fn post(&self, txn: &crate::TxnHandle, params: Map<State>) -> TCResult<State> {
        match self.route.clone() {
            RouteImpl::OpDef(opdef @ OpDef::Post(_)) => {
                crate::op_executor::execute_post_with_self(
                    txn,
                    opdef,
                    params,
                    Some(self.schema.clone()),
                )
                .await
            }
            _ => Err(TCError::method_not_allowed("POST", self.schema.to_string())),
        }
    }

    async fn delete(&self, txn: &crate::TxnHandle, key: Scalar) -> TCResult<()> {
        match self.route.clone() {
            RouteImpl::OpDef(opdef @ OpDef::Delete(_)) => {
                crate::op_executor::execute_delete_with_self(
                    txn,
                    opdef,
                    key,
                    Some(self.schema.clone()),
                )
                .await
            }
            _ => Err(TCError::method_not_allowed(
                "DELETE",
                self.schema.to_string(),
            )),
        }
    }
}

/// Compile an IR artifact into a transport-neutral library route handler.
pub async fn compile_ir_library(artifact: Artifact) -> TCResult<CompiledLibrary> {
    let manifest: IrManifest = serde_json::from_slice(&artifact.bytes)
        .map_err(|err| TCError::bad_request(format!("invalid ir manifest json: {err}")))?;
    let schema =
        decode_schema_bytes(manifest.schema.get().as_bytes()).map_err(TCError::bad_request)?;

    let mut classes = Vec::with_capacity(manifest.classes.len());
    for class in manifest.classes {
        let identity = Link::from_str(&class.id)
            .map_err(|err| TCError::bad_request(format!("invalid Class identity: {err}")))?;
        if !class.id.starts_with("/class/") || class.id.trim_matches('/').split('/').count() < 3 {
            return Err(TCError::bad_request(format!(
                "Class identity must be a versioned canonical /class path: {}",
                class.id
            )));
        }
        let parent = if class.parent.starts_with("/class/") {
            ClassParent::Class(Link::from_str(&class.parent).map_err(|err| {
                TCError::bad_request(format!("invalid Class parent identity: {err}"))
            })?)
        } else {
            let path = class.parent.parse::<pathlink::PathBuf>().map_err(|err| {
                TCError::bad_request(format!("invalid native Class parent: {err}"))
            })?;
            ClassParent::Native(StateType::from_path(path.as_ref()).ok_or_else(|| {
                TCError::bad_request(format!("unknown native Class parent: {}", class.parent))
            })?)
        };
        let mut prototype = Map::new();
        for (name, value) in class.prototype {
            let name = name.parse().map_err(|err| {
                TCError::bad_request(format!("invalid Class member name {name}: {err}"))
            })?;
            let stream = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(
                Bytes::copy_from_slice(value.get().as_bytes()),
            )]);
            let value = destream_json::try_decode((), stream)
                .await
                .map_err(|err| TCError::bad_request(format!("invalid Class member: {err}")))?;
            prototype.insert(name, value);
        }
        classes.push(ClassDef::new(identity, parent, prototype));
    }

    let mut route_entries = Vec::new();
    let mut routes = HashMap::new();
    for route in manifest.routes {
        let segments = parse_route_path(&route.path)?;
        route_entries.push((segments.clone(), RouteMetadata { export: None }));

        let implementation =
            if let Some(value) = route.value {
                let stream = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(
                    Bytes::copy_from_slice(value.get().as_bytes()),
                )]);
                let scalar: Scalar = destream_json::try_decode((), stream)
                    .await
                    .map_err(|err| TCError::bad_request(format!("invalid route value: {err}")))?;
                RouteImpl::Value(State::from_scalar(scalar))
            } else if let Some(op) = route.op {
                if !op.method.eq_ignore_ascii_case("GET") {
                    return Err(TCError::bad_request(
                        "only GET ops are supported in ir manifests",
                    ));
                }
                let link = Link::from_str(&op.path)
                    .map_err(|err| TCError::bad_request(format!("invalid op link: {err}")))?;
                RouteImpl::Op(Box::new(OpRef::Get((
                    Subject::Link(link),
                    Scalar::default(),
                ))))
            } else if let Some(opdef) = route.opdef {
                let stream = futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(
                    Bytes::copy_from_slice(opdef.get().as_bytes()),
                )]);
                RouteImpl::OpDef(destream_json::try_decode((), stream).await.map_err(|err| {
                    TCError::bad_request(format!("invalid opdef encoding: {err}"))
                })?)
            } else {
                return Err(TCError::bad_request("route missing value or op"));
            };

        routes.insert(
            segments,
            IrHandler {
                schema: schema.id().clone(),
                route: implementation,
            },
        );
    }

    let routes = IrRoutes {
        routes: Arc::new(routes),
    };

    Ok(CompiledLibrary {
        schema,
        classes,
        routes: SchemaRoutes::from_entries(route_entries)?,
        artifact,
        execution: LibraryExecution::Native(routes),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact(classes: &str) -> Artifact {
        Artifact {
            path: "/lib/acme/classes/1.0.0".into(),
            content_type: IR_ARTIFACT_CONTENT_TYPE.into(),
            bytes: format!(
                r#"{{"schema":{{"id":"/lib/acme/classes/1.0.0","version":"1.0.0","dependencies":[]}},"classes":{classes},"routes":[]}}"#
            )
            .into_bytes(),
        }
    }

    #[tokio::test]
    async fn compiles_canonical_class_manifest() {
        let compiled = compile_ir_library(artifact(
            r#"[{"id":"/class/acme/counter/1.0.0","parent":"/state/scalar/value/number","prototype":{"initial":0}}]"#,
        ))
        .await
        .expect("compile Class manifest");

        assert_eq!(compiled.classes.len(), 1);
        assert_eq!(
            compiled.classes[0].identity().to_string(),
            "/class/acme/counter/1.0.0"
        );
    }

    #[tokio::test]
    async fn rejects_noncanonical_class_identity() {
        let result = compile_ir_library(artifact(
            r#"[{"id":"/lib/acme/not-a-class/1.0.0","parent":"/state/scalar/value/number"}]"#,
        ))
        .await;
        let err = match result {
            Ok(_) => panic!("accepted non-Class identity"),
            Err(err) => err,
        };

        assert!(err.message().contains("canonical /class path"));
    }
}
