use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::FutureExt;
use hyper::{Body, Request, Response, StatusCode};

use tinychain::kernel::KernelHandler;
use tinychain::replication::is_peer_membership_path;

pub(crate) fn ok_handler() -> impl KernelHandler {
    |_req: Request<Body>| {
        async move {
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::empty())
                .expect("ok response")
        }
        .boxed()
    }
}

pub(crate) fn health_handler(bootstrap_ready: Arc<AtomicBool>) -> impl KernelHandler {
    move |_req: Request<Body>| {
        let bootstrap_ready = Arc::clone(&bootstrap_ready);
        async move {
            if !bootstrap_ready.load(Ordering::SeqCst) {
                return Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .body(Body::from("bootstrap-pending"))
                    .expect("health response");
            }

            Response::builder()
                .status(StatusCode::OK)
                .body(Body::from("ok"))
                .expect("health response")
        }
        .boxed()
    }
}

pub(crate) fn combined_host_handler(
    public: Arc<dyn KernelHandler>,
    token: Arc<dyn KernelHandler>,
    export: Arc<dyn KernelHandler>,
    peers: Arc<dyn KernelHandler>,
) -> impl KernelHandler {
    move |req: Request<Body>| {
        let path = req.uri().path().to_string();
        let public = Arc::clone(&public);
        let token = Arc::clone(&token);
        let export = Arc::clone(&export);
        let peers = Arc::clone(&peers);
        async move {
            match path.as_str() {
                "/" => token.call(req).await,
                tinychain::uri::HOST_LIBRARY_EXPORT => export.call(req).await,
                path if is_peer_membership_path(path) => peers.call(req).await,
                _ => public.call(req).await,
            }
        }
        .boxed()
    }
}
