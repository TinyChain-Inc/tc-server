use std::sync::Arc;

use pathlink::{Link, PathSegment};
use tc_error::{TCError, TCResult};
use tc_ir::{Method, Transact, TxnId};

use crate::cluster::{Cluster, Dir};
use crate::kernel::bootstrap::load_dir;

type Root<T> = Cluster<Dir<T>>;

pub struct HostServices {
    pub application_roots: crate::storage::ApplicationRoots,
    pub replication: Arc<dyn crate::replication::ClusterGateway>,
    pub rpc: Arc<dyn crate::gateway::RpcGateway>,
    pub resources: crate::HostResources,
    pub protocol: crate::ProtocolAuthority,
    pub verifier: Arc<dyn crate::auth::TokenVerifier>,
    pub actors: crate::auth::KeyringActorResolver,
    pub bootstrap: std::sync::Arc<crate::replication::ReplicationIssuer>,
    pub bootstrap_required: bool,
}

pub(crate) struct KernelInner {
    pub(crate) libraries: Root<crate::library::Library>,
    pub(crate) classes: Root<crate::class::Class>,
    pub(crate) services: Root<crate::service::Service>,
    #[cfg(feature = "wasm")]
    pub(crate) compiler: crate::library::compiler::Compiler,
    pub(crate) rpc: Arc<dyn crate::gateway::RpcGateway>,
    pub(crate) state: tc_state::Static<crate::TxnHandle>,
    resources: crate::HostResources,
    actors: crate::auth::KeyringActorResolver,
    pub(crate) bootstrap: std::sync::Arc<crate::replication::ReplicationIssuer>,
}

impl KernelInner {
    pub(crate) async fn load(
        txn_id: TxnId,
        roots: crate::storage::ApplicationRoots,
        protocol: crate::ProtocolAuthority,
        replication: Arc<dyn crate::replication::ClusterGateway>,
        rpc: Arc<dyn crate::gateway::RpcGateway>,
        resources: crate::HostResources,
        actors: crate::auth::KeyringActorResolver,
        bootstrap: std::sync::Arc<crate::replication::ReplicationIssuer>,
    ) -> TCResult<Self> {
        let protocol = Arc::new(protocol);
        let (class_root, library_root, service_root) = roots.into_parts();
        let path = |root: &str| std::iter::once(root.parse().expect("application root")).collect();
        let compiler = crate::library::compiler::Compiler::new();
        let library_compiler = compiler.clone();
        let (classes, libraries, services) = tokio::try_join!(
            load_dir(
                txn_id,
                class_root,
                path("class"),
                "class",
                Arc::clone(&protocol),
                Arc::clone(&replication),
                crate::class::Class::load,
            ),
            load_dir(
                txn_id,
                library_root,
                path("lib"),
                "lib",
                Arc::clone(&protocol),
                Arc::clone(&replication),
                move |txn_id, storage| {
                    crate::library::Library::load(library_compiler.clone(), txn_id, storage)
                },
            ),
            load_dir(
                txn_id,
                service_root,
                path("service"),
                "service",
                Arc::clone(&protocol),
                Arc::clone(&replication),
                crate::service::Service::load,
            ),
        )?;

        let loaded_classes = classes.state().items(txn_id).await?;
        let definitions = loaded_classes
            .iter()
            .map(|class| (class.identity().clone(), class.definition().clone()))
            .collect::<std::collections::BTreeMap<_, _>>();
        tc_state::analyze_classes(&definitions, definitions.keys().cloned())
            .map_err(|error| TCError::bad_request(error.to_string()))?;

        Ok(Self {
            libraries,
            classes,
            services,
            #[cfg(feature = "wasm")]
            compiler,
            rpc,
            state: tc_state::Static::default(),
            resources,
            actors,
            bootstrap,
        })
    }

    pub(crate) async fn bootstrap(
        &self,
        txn: &crate::TxnHandle,
        state: crate::State,
    ) -> TCResult<crate::State> {
        let crate::State::Scalar(tc_ir::Scalar::Value(tc_value::Value::Bytes(payload))) = state
        else {
            return Err(TCError::bad_request(
                "GET /host requires an encrypted byte value",
            ));
        };
        let (request, key) = self.bootstrap.open_request(txn.id(), &payload)?;
        let resource: Link = request.resource.parse().map_err(|error| {
            TCError::bad_request(format!("invalid bootstrap resource: {error}"))
        })?;
        let hash = match resource.path().first().map(PathSegment::as_str) {
            Some("class") => {
                resolved_hash(
                    self.classes
                        .clone()
                        .lookup(txn, &resource.path()[1..])
                        .await?,
                    txn,
                )
                .await?
            }
            Some("lib") => {
                resolved_hash(
                    self.libraries
                        .clone()
                        .lookup(txn, &resource.path()[1..])
                        .await?,
                    txn,
                )
                .await?
            }
            Some("service") => {
                resolved_hash(
                    self.services
                        .clone()
                        .lookup(txn, &resource.path()[1..])
                        .await?,
                    txn,
                )
                .await?
            }
            _ => {
                return Err(TCError::bad_request(
                    "bootstrap must name an application Cluster",
                ));
            }
        };
        self.bootstrap
            .seal_response(txn.id(), resource, hash, &key)
            .map(|response| crate::State::from(tc_value::Value::Bytes(response.into())))
    }

    pub(crate) async fn dispatch(
        &self,
        txn: &crate::TxnHandle,
        target: &Link,
        method: Method,
        body: Option<crate::State>,
    ) -> TCResult<Option<crate::State>> {
        match target.path().first().map(PathSegment::as_str) {
            Some("lib") => {
                self.libraries
                    .clone()
                    .dispatch(
                        txn,
                        target,
                        method,
                        body,
                        Box::new(crate::library::Root::new(
                            &self.libraries,
                            #[cfg(feature = "wasm")]
                            &self.compiler,
                        )),
                    )
                    .await
            }
            Some("class") => {
                self.classes
                    .clone()
                    .dispatch(
                        txn,
                        target,
                        method,
                        body,
                        Box::new(crate::class::Root(&self.classes)),
                    )
                    .await
            }
            Some("service") => {
                self.services
                    .clone()
                    .dispatch(
                        txn,
                        target,
                        method,
                        body,
                        Box::new(crate::service::Root(&self.services)),
                    )
                    .await
            }
            _ => Err(TCError::not_found(target.to_string())),
        }
    }

    pub(crate) fn metrics(&self) -> crate::State {
        crate::State::Tuple(
            self.resources
                .snapshots()
                .into_iter()
                .map(capacity_state)
                .collect(),
        )
    }

    pub(crate) fn public_key(&self, actor_id: &str) -> TCResult<crate::State> {
        use base64::Engine as _;
        let key = self
            .actors
            .public_key(actor_id)
            .ok_or_else(|| TCError::not_found(actor_id))?;
        Ok(crate::State::from(tc_value::Value::from(
            base64::engine::general_purpose::STANDARD.encode(key.to_bytes()),
        )))
    }

    pub(crate) async fn dispatch_state(
        &self,
        txn: &crate::TxnHandle,
        target: &[pathlink::PathSegment],
        method: Method,
        body: Option<crate::State>,
    ) -> TCResult<Option<crate::State>> {
        let handler =
            tc_ir::Route::route(&self.state, target).ok_or_else(|| TCError::not_found("/state"))?;
        super::invoke_handler(handler, txn, method, body)
            .await
            .map(Some)
    }

    pub(crate) async fn finalize(&self, cutoff: &TxnId) -> TCResult<()> {
        self.libraries.finalize(cutoff).await?;
        self.classes.finalize(cutoff).await?;
        self.services.finalize(cutoff).await
    }
}

async fn resolved_hash<T>(
    resource: crate::cluster::Resolved<T>,
    txn: &crate::TxnHandle,
) -> TCResult<[u8; 32]>
where
    T: crate::cluster::ResourceHash + Clone + Send + Sync + 'static,
{
    use crate::cluster::ResourceHash as _;
    match resource {
        crate::cluster::Resolved::Dir { cluster, unmatched } if unmatched.is_empty() => {
            cluster.state().resource_hash(txn.id()).await
        }
        crate::cluster::Resolved::Item {
            cluster, suffix, ..
        } if suffix.is_empty() => cluster.state().resource_hash(txn.id()).await,
        _ => Err(TCError::not_found("bootstrap resource")),
    }
}

fn capacity_state(snapshot: crate::CapacitySnapshot) -> crate::State {
    use number_general::Number;
    use tc_ir::{Id, Map};
    use tc_value::Value;

    let entry = |name: &str, value| -> (Id, crate::State) {
        (name.parse().expect("capacity field"), value)
    };
    crate::State::Map(Map::from_iter([
        entry(
            "resource",
            crate::State::from(Value::from(snapshot.resource)),
        ),
        entry(
            "limit",
            crate::State::from(Value::from(Number::from(snapshot.limit as u64))),
        ),
        entry(
            "in_flight",
            crate::State::from(Value::from(Number::from(snapshot.in_flight as u64))),
        ),
        entry(
            "wait_count",
            crate::State::from(Value::from(Number::from(snapshot.wait_count))),
        ),
        entry(
            "wait_time_ms",
            crate::State::from(Value::from(Number::from(snapshot.wait_time_ms))),
        ),
        entry(
            "rejection_count",
            crate::State::from(Value::from(Number::from(snapshot.rejection_count))),
        ),
        entry(
            "best_effort_drop_count",
            crate::State::from(Value::from(Number::from(snapshot.best_effort_drop_count))),
        ),
    ]))
}
