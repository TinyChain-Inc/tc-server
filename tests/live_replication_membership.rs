use std::net::TcpListener;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use hyper::{Body, Client, Request, StatusCode};
use pathlink::Link;
use tc_ir::{Claim, LibrarySchema, NetworkTime, TxnId};
use tc_value::Value;
use tinychain::auth::{Actor, KeyringActorResolver, PublicKeyStore, Token};
use tinychain::http::{HttpServer, host_handler_with_public_keys};
use tinychain::kernel::Kernel;
use tinychain::library::CompiledLibraryPackage;
use tinychain::library::http::{build_http_library_module, http_library_handlers};
use tinychain::replication::{
    PeerMembership, ReplicationIssuer, discover_library_paths, export_handler,
    fetch_compiled_library_package, live_replicating_finalize_hook,
    live_replicating_install_put_handler, parse_psk_keys, replication_token_handler,
    request_replication_token,
};
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener as TokioTcpListener, TcpStream};
use umask::USER_WRITE;

#[tokio::test]
async fn replication_export_tracks_new_installs() {
    let server = start_server(
        "replication-export",
        Some(unique_temp_dir("replication-export")),
    )
    .await;

    let schema_a = sample_schema("/lib/example-devco/live-forward/0.1.0");
    install_with_write_token(&server, &schema_a).await;

    let definition_a = fetch_compiled_package_for_schema(&server, &schema_a).await;
    assert_eq!(definition_a.schema.id(), schema_a.id());

    let schema_b = sample_schema("/lib/example-devco/live-forward/0.2.0");
    install_with_write_token(&server, &schema_b).await;

    let definition_b = fetch_compiled_package_for_schema(&server, &schema_b).await;
    assert_eq!(definition_b.schema.id(), schema_b.id());
}

#[tokio::test]
async fn discover_library_paths_lists_installed_libraries() {
    let server = start_server(
        "replication-discovery",
        Some(unique_temp_dir("replication-discovery")),
    )
    .await;

    let schema_a = sample_schema("/lib/example-devco/discovery/0.1.0");
    let schema_b = sample_schema("/lib/example-devco/discovery/0.2.0");

    install_with_write_token(&server, &schema_a).await;
    install_with_write_token(&server, &schema_b).await;

    let mut paths = discover_library_paths(&format!("http://{}", server.addr))
        .await
        .expect("discover paths");
    paths.sort();

    assert!(paths.iter().any(|path| path == &schema_a.id().to_string()));
    assert!(paths.iter().any(|path| path == &schema_b.id().to_string()));
}

#[tokio::test]
async fn live_replicated_install_retry_finalizes_after_peer_recovers() {
    let actor = Actor::new(Value::from("live-replication-installer"));
    let peer_a = start_replicating_server(
        "retry-peer-a",
        actor.clone(),
        Vec::new(),
        Some(unique_temp_dir("retry-peer-a")),
    )
    .await;
    let peer_b = start_replicating_server(
        "retry-peer-b",
        actor.clone(),
        Vec::new(),
        Some(unique_temp_dir("retry-peer-b")),
    )
    .await;
    let peer_b_proxy = TcpProxy::start(peer_b.addr).await;

    let primary = start_replicating_server(
        "retry-primary",
        actor.clone(),
        vec![
            format!("http://{}", peer_a.addr),
            format!("http://{}", peer_b_proxy.addr),
        ],
        Some(unique_temp_dir("retry-primary")),
    )
    .await;

    let schema = sample_schema("/lib/example-devco/live-retry/0.1.0");
    let token = token_for_schema(&actor, &schema, USER_WRITE);
    let txn_id = begin_transaction(primary.addr, token).await;
    let txn_token = token_for_schema_and_txn(&actor, &schema, USER_WRITE, txn_id);
    let response = put_library_definition(
        primary.addr,
        Some(txn_token.clone()),
        library_definition_for_schema(&schema),
        Some(txn_id),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    peer_b_proxy.set_enabled(false);
    let failed_commit = finalize_install(primary.addr, &txn_token, txn_id, true).await;
    assert_eq!(
        failed_commit.status(),
        StatusCode::BAD_REQUEST,
        "commit must fail while a prepared participant is unavailable"
    );

    peer_b_proxy.set_enabled(true);

    let retry_commit = finalize_install(primary.addr, &txn_token, txn_id, true).await;
    assert_eq!(
        retry_commit.status(),
        StatusCode::NO_CONTENT,
        "same transaction should finalize after the prepared participant recovers"
    );

    let primary_package = fetch_compiled_package_for_schema(&primary, &schema).await;
    let peer_a_package = fetch_compiled_package_for_schema(&peer_a, &schema).await;
    let peer_b_package = fetch_compiled_package_for_schema(&peer_b, &schema).await;
    assert_eq!(primary_package.schema.id(), schema.id());
    assert_eq!(peer_a_package.schema.id(), schema.id());
    assert_eq!(peer_b_package.schema.id(), schema.id());
}

struct RunningServer {
    addr: std::net::SocketAddr,
    actor: Actor,
    keys: Vec<aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningServer {
    fn abort(&mut self) {
        self.task.abort();
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.abort();
    }
}

struct TcpProxy {
    addr: std::net::SocketAddr,
    enabled: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl TcpProxy {
    async fn start(target: std::net::SocketAddr) -> Self {
        let listener = TokioTcpListener::bind("127.0.0.1:0")
            .await
            .expect("proxy listener");
        let addr = listener.local_addr().expect("proxy addr");
        let enabled = Arc::new(AtomicBool::new(true));
        let enabled_for_task = Arc::clone(&enabled);
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else {
                    break;
                };

                if !enabled_for_task.load(Ordering::SeqCst) {
                    drop(inbound);
                    continue;
                }

                tokio::spawn(async move {
                    let Ok(mut outbound) = TcpStream::connect(target).await else {
                        return;
                    };
                    let _ = copy_bidirectional(&mut inbound, &mut outbound).await;
                });
            }
        });

        Self {
            addr,
            enabled,
            task,
        }
    }

    fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::SeqCst);
    }
}

impl Drop for TcpProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn unique_temp_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "tc-live-repl-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create dir");
    dir
}

fn sample_schema(id: &str) -> LibrarySchema {
    LibrarySchema::new(Link::from_str(id).expect("schema link"), "0.1.0", vec![])
}

fn library_definition_for_schema(schema: &LibrarySchema) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        schema.id().to_string(): {
            "ok": { "ok": true }
        }
    }))
    .expect("library definition bytes")
}

fn shared_replication_keys() -> Vec<aes_gcm_siv::Key<aes_gcm_siv::Aes256GcmSiv>> {
    parse_psk_keys(&[
        "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
    ])
    .expect("psk keys")
}

async fn start_server(label: &str, storage_root: Option<std::path::PathBuf>) -> RunningServer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    start_server_with_listener(label, storage_root, listener).await
}

async fn start_server_with_listener(
    label: &str,
    storage_root: Option<std::path::PathBuf>,
    listener: TcpListener,
) -> RunningServer {
    let schema = tinychain::library::default_library_schema();
    let module = build_http_library_module(schema, storage_root)
        .await
        .expect("module");
    let handlers = http_library_handlers(&module);

    let host = Link::from_str("/host").expect("host link");
    let actor = Actor::new(Value::from(format!("installer-{label}")));
    let keyring = KeyringActorResolver::default().with_actor(host.clone(), actor.clone());
    let public_keys = PublicKeyStore::default();
    let keys = shared_replication_keys();
    let issuer = Arc::new(ReplicationIssuer::new(
        host,
        keys.clone(),
        Actor::new(Value::from(format!("replication:live:{label}"))),
        keyring.clone(),
        public_keys.clone(),
    ));

    let kernel = Kernel::builder()
        .with_host_id(format!("live-replication-{label}"))
        .with_http_rpc_gateway()
        .with_rjwt_keyring_token_verifier(keyring)
        .with_library_module(module.clone(), handlers)
        .with_service_handler(|_req| async move { hyper::Response::new(Body::empty()) })
        .with_kernel_handler(combine_host_handlers(
            host_handler_with_public_keys(public_keys),
            replication_token_handler(issuer.clone()),
            export_handler(module),
        ))
        .with_health_handler(|_req| async move { hyper::Response::new(Body::empty()) })
        .finish();

    let addr = listener.local_addr().expect("addr");
    let server = HttpServer::new(kernel);
    let task = tokio::spawn(async move {
        let _ = server.serve_listener(listener).await;
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    RunningServer {
        addr,
        actor,
        keys,
        task,
    }
}

async fn start_replicating_server(
    label: &str,
    actor: Actor,
    peers: Vec<String>,
    storage_root: Option<std::path::PathBuf>,
) -> RunningServer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    start_replicating_server_with_listener(label, actor, peers, storage_root, listener).await
}

async fn start_replicating_server_with_listener(
    label: &str,
    actor: Actor,
    peers: Vec<String>,
    storage_root: Option<std::path::PathBuf>,
    listener: TcpListener,
) -> RunningServer {
    let schema = tinychain::library::default_library_schema();
    let module = build_http_library_module(schema, storage_root)
        .await
        .expect("module");
    let handlers = http_library_handlers(&module);

    let host = Link::from_str("/host").expect("host link");
    let keyring = KeyringActorResolver::default().with_actor(host.clone(), actor.clone());
    let public_keys = PublicKeyStore::default();
    let keys = shared_replication_keys();
    let issuer = Arc::new(ReplicationIssuer::new(
        host,
        keys.clone(),
        Actor::new(Value::from(format!("replication:live:{label}"))),
        keyring.clone(),
        public_keys.clone(),
    ));
    let membership = PeerMembership::new(peers);
    let live_put =
        live_replicating_install_put_handler(module.clone(), membership.clone(), keys.clone());
    let finalize_hook = live_replicating_finalize_hook(module.clone());

    let kernel = Kernel::builder()
        .with_host_id(format!("live-replication-{label}"))
        .with_http_rpc_gateway()
        .with_rjwt_keyring_token_verifier(keyring)
        .with_library_module(module.clone(), handlers)
        .with_lib_put_handler(live_put)
        .with_txn_finalize_hook(finalize_hook)
        .with_service_handler(|_req| async move { hyper::Response::new(Body::empty()) })
        .with_kernel_handler(combine_host_handlers(
            host_handler_with_public_keys(public_keys),
            replication_token_handler(issuer.clone()),
            export_handler(module),
        ))
        .with_health_handler(|_req| async move { hyper::Response::new(Body::empty()) })
        .finish();

    let addr = listener.local_addr().expect("addr");
    let server = HttpServer::new(kernel);
    let task = tokio::spawn(async move {
        let _ = server.serve_listener(listener).await;
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    RunningServer {
        addr,
        actor,
        keys,
        task,
    }
}

async fn install_with_write_token(server: &RunningServer, schema: &LibrarySchema) {
    let token = token_for_schema(&server.actor, schema, USER_WRITE);
    let txn_id = begin_transaction(server.addr, token).await;
    let txn_token = token_for_schema_and_txn(&server.actor, schema, USER_WRITE, txn_id);
    let definition = library_definition_for_schema(schema);
    let response = put_library_definition(
        server.addr,
        Some(txn_token.clone()),
        definition,
        Some(txn_id),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let finalize = finalize_install(server.addr, &txn_token, txn_id, true).await;
    assert_eq!(finalize.status(), StatusCode::NO_CONTENT);
}

async fn fetch_compiled_package_for_schema(
    server: &RunningServer,
    schema: &LibrarySchema,
) -> CompiledLibraryPackage {
    let peer = format!("http://{}", server.addr);
    let token = request_replication_token(&peer, &schema.id().to_string(), &server.keys)
        .await
        .expect("replication token");

    fetch_compiled_library_package(&peer, &token)
        .await
        .expect("export request")
        .expect("export compiled package")
}

fn token_for_schema(actor: &Actor, schema: &LibrarySchema, mask: umask::Mode) -> String {
    let host = Link::from_str("/host").expect("host link");
    let claim = Claim::new(schema.id().clone(), mask);
    let token = Token::new(
        host,
        std::time::SystemTime::now(),
        Duration::from_secs(30),
        actor.id().clone(),
        claim,
    );
    let signed = actor.sign_token(token).expect("sign token");
    signed.into_jwt()
}

fn token_for_schema_and_txn(
    actor: &Actor,
    schema: &LibrarySchema,
    mask: umask::Mode,
    txn_id: TxnId,
) -> String {
    let host = Link::from_str("/host").expect("host link");
    let claim = Claim::new(schema.id().clone(), mask);
    let token = Token::new(
        host.clone(),
        std::time::SystemTime::now(),
        Duration::from_secs(30),
        actor.id().clone(),
        claim,
    );
    let token = actor.sign_token(token).expect("sign token");

    let txn_claim = Claim::new(
        Link::from_str(&format!("/txn/{txn_id}")).expect("txn claim link"),
        umask::USER_EXEC | umask::USER_WRITE,
    );
    actor
        .consume_and_sign(token, host, txn_claim, std::time::SystemTime::now())
        .expect("sign txn token")
        .into_jwt()
}

async fn begin_transaction(addr: std::net::SocketAddr, bearer: String) -> TxnId {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_nanos() as u64;
    let txn_id = TxnId::from_parts(NetworkTime::from_nanos(now), 0);
    let request = Request::builder()
        .method("GET")
        .uri(format!("http://{addr}/lib?txn_id={txn_id}"))
        .header(hyper::header::AUTHORIZATION, format!("Bearer {bearer}"))
        .body(Body::empty())
        .expect("begin request");
    let response = Client::new()
        .request(request)
        .await
        .expect("begin response");
    assert_eq!(response.status(), StatusCode::OK);
    txn_id
}

async fn finalize_install(
    addr: std::net::SocketAddr,
    bearer: &str,
    txn_id: TxnId,
    commit: bool,
) -> hyper::Response<Body> {
    let method = if commit { "POST" } else { "DELETE" };
    let request = Request::builder()
        .method(method)
        .uri(format!("http://{addr}/lib?txn_id={txn_id}"))
        .header(hyper::header::AUTHORIZATION, format!("Bearer {bearer}"))
        .body(Body::empty())
        .expect("finalize request");
    Client::new()
        .request(request)
        .await
        .expect("finalize response")
}

async fn put_library_definition(
    addr: std::net::SocketAddr,
    bearer: Option<String>,
    definition: Vec<u8>,
    txn_id: Option<TxnId>,
) -> hyper::Response<Body> {
    let uri = match txn_id {
        Some(txn_id) => format!("http://{addr}/lib?txn_id={txn_id}"),
        None => format!("http://{addr}/lib"),
    };
    let mut req = Request::builder()
        .method("PUT")
        .uri(uri)
        .header(hyper::header::CONTENT_TYPE, "application/json");
    if let Some(token) = bearer {
        req = req.header(hyper::header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let request = req.body(Body::from(definition)).expect("request");
    Client::new().request(request).await.expect("response")
}

fn combine_host_handlers(
    public: impl tinychain::KernelHandler,
    token: impl tinychain::KernelHandler,
    export: impl tinychain::KernelHandler,
) -> impl tinychain::KernelHandler {
    let public: Arc<dyn tinychain::KernelHandler> = Arc::new(public);
    let token: Arc<dyn tinychain::KernelHandler> = Arc::new(token);
    let export: Arc<dyn tinychain::KernelHandler> = Arc::new(export);

    move |req: tinychain::Request| {
        let path = req.uri().path().to_string();
        let public = Arc::clone(&public);
        let token = Arc::clone(&token);
        let export = Arc::clone(&export);
        async move {
            match path.as_str() {
                "/" => token.call(req).await,
                "/host/library/export" => export.call(req).await,
                _ => public.call(req).await,
            }
        }
    }
}
