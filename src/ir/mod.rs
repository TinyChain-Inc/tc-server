use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;
use pathlink::Link;
use serde::Deserialize;
use tc_error::{TCError, TCResult};
use tc_ir::{Handler, Map, OpDef, OpRef, Route, Scalar, Subject, parse_route_path};

use crate::State;
use crate::library::{CompiledLibrary, LibraryExecution, RouteMetadata, SchemaRoutes};
use crate::storage::{Artifact, decode_schema_bytes};

pub const IR_ARTIFACT_CONTENT_TYPE: &str = "application/tinychain+json";
pub const WASM_ARTIFACT_CONTENT_TYPE: &str = "application/wasm";

#[derive(Deserialize)]
struct IrManifest {
    schema: serde_json::Value,
    routes: Vec<IrRoute>,
}

#[derive(Deserialize)]
struct IrRoute {
    path: String,
    #[serde(default)]
    value: Option<serde_json::Value>,
    #[serde(default)]
    op: Option<IrOp>,
    #[serde(default)]
    opdef: Option<serde_json::Value>,
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
    let schema_bytes = serde_json::to_vec(&manifest.schema)
        .map_err(|err| TCError::bad_request(format!("invalid ir schema: {err}")))?;
    let schema = decode_schema_bytes(&schema_bytes).map_err(TCError::bad_request)?;

    let mut route_entries = Vec::new();
    let mut routes = HashMap::new();
    for route in manifest.routes {
        let segments = parse_route_path(&route.path)?;
        route_entries.push((segments.clone(), RouteMetadata { export: None }));

        let implementation =
            if let Some(value) = route.value {
                let bytes = serde_json::to_vec(&value)
                    .map_err(|err| TCError::bad_request(format!("invalid route value: {err}")))?;
                let stream =
                    futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from(bytes))]);
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
                let bytes = serde_json::to_vec(&opdef)
                    .map_err(|err| TCError::bad_request(format!("invalid opdef route: {err}")))?;
                let stream =
                    futures::stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from(bytes))]);
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
        routes: SchemaRoutes::from_entries(route_entries)?,
        artifact,
        execution: LibraryExecution::Native(routes),
    })
}
