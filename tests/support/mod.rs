#![allow(dead_code)]

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use hyper::{Body, Client, Request, StatusCode};
use pathlink::Link;
use tc_ir::{Claim, LibrarySchema, TxnId};
use tinychain::auth::{Actor, Token};
use tinychain::replication::is_peer_membership_path;

pub fn token_for_schema(actor: &Actor, schema: &LibrarySchema, mask: umask::Mode) -> String {
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

pub fn token_for_schema_and_txn(
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
    let token = actor
        .consume_and_sign(token, host, txn_claim, std::time::SystemTime::now())
        .expect("sign txn token");

    token.into_jwt()
}

pub async fn begin_transaction(addr: std::net::SocketAddr, bearer: String) -> TxnId {
    let request = Request::builder()
        .method("GET")
        .uri(format!("http://{addr}/lib"))
        .header(hyper::header::AUTHORIZATION, format!("Bearer {bearer}"))
        .body(Body::empty())
        .expect("begin request");
    let response = Client::new()
        .request(request)
        .await
        .expect("begin response");
    assert_eq!(response.status(), StatusCode::OK);
    let raw = response
        .headers()
        .get("x-tc-txn-id")
        .and_then(|value| value.to_str().ok())
        .expect("missing x-tc-txn-id");
    TxnId::from_str(raw).expect("parse txn id")
}

pub async fn put_install_payload(
    addr: std::net::SocketAddr,
    bearer: Option<String>,
    payload: Vec<u8>,
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
    let request = req.body(Body::from(payload)).expect("request");
    Client::new().request(request).await.expect("response")
}

pub async fn finalize_install(
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

pub async fn get_library_status(addr: std::net::SocketAddr, path: &str) -> StatusCode {
    let uri = format!("http://{addr}{path}");
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .expect("request");
    Client::new()
        .request(request)
        .await
        .expect("response")
        .status()
}

pub fn combine_host_handlers(
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

pub fn combine_host_handlers_with_peers(
    public: impl tinychain::KernelHandler,
    token: impl tinychain::KernelHandler,
    export: impl tinychain::KernelHandler,
    peers: impl tinychain::KernelHandler,
) -> impl tinychain::KernelHandler {
    let public: Arc<dyn tinychain::KernelHandler> = Arc::new(public);
    let token: Arc<dyn tinychain::KernelHandler> = Arc::new(token);
    let export: Arc<dyn tinychain::KernelHandler> = Arc::new(export);
    let peers: Arc<dyn tinychain::KernelHandler> = Arc::new(peers);

    move |req: tinychain::Request| {
        let path = req.uri().path().to_string();
        let public = Arc::clone(&public);
        let token = Arc::clone(&token);
        let export = Arc::clone(&export);
        let peers = Arc::clone(&peers);
        async move {
            match path.as_str() {
                "/" => token.call(req).await,
                "/host/library/export" => export.call(req).await,
                path if is_peer_membership_path(path) => peers.call(req).await,
                _ => public.call(req).await,
            }
        }
    }
}
