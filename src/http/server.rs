use std::net::{SocketAddr, TcpListener};
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use tower::Service;

use super::parse::{
    TxnParseError, decode_native_body, parse_bearer_token, parse_body, parse_txn_id,
};
use super::response::{
    bad_request_response, handle_finalize_result, method_not_allowed, not_found,
};
use super::{Body, HttpHandler, Request, Response, StatusCode};
use crate::kernel::BoundTransaction;
use crate::{Kernel, KernelRequest, Method};

/// HTTP-only endpoint routing. Native kernel routes are deliberately absent
/// from this type.
#[derive(Clone)]
pub struct HttpRouter {
    registry: Arc<crate::library::LibraryRegistry>,
    health: Arc<dyn super::HttpHandler>,
    host: Arc<dyn super::HttpHandler>,
    service: Arc<dyn super::HttpHandler>,
    library_put: Option<Arc<dyn super::HttpHandler>>,
}

impl HttpRouter {
    pub fn new<H, Ho, S>(
        registry: Arc<crate::library::LibraryRegistry>,
        health: H,
        host: Ho,
        service: S,
    ) -> Self
    where
        H: super::HttpHandler,
        Ho: super::HttpHandler,
        S: super::HttpHandler,
    {
        Self {
            registry,
            health: Arc::new(health),
            host: Arc::new(host),
            service: Arc::new(service),
            library_put: None,
        }
    }

    pub fn with_library_put_handler<H>(mut self, handler: H) -> Self
    where
        H: super::HttpHandler,
    {
        self.library_put = Some(Arc::new(handler));
        self
    }

    pub fn is_native(&self, path: &str) -> bool {
        path.starts_with("/state/")
            || path == crate::uri::HOST_AUTH_CONTEXT
            || self.registry.has_class(path)
            || self
                .registry
                .resolve_native(path)
                .is_some_and(|(_, _, is_root)| !is_root)
    }

    fn requires_transaction(&self, path: &str) -> bool {
        self.is_native(path)
            || path == crate::uri::LIB_ROOT
            || path.starts_with(crate::uri::LIB_ROOT_PREFIX)
            || path == crate::uri::CLASS_ROOT
            || path.starts_with(crate::uri::CLASS_ROOT_PREFIX)
            || path == crate::uri::SERVICE_ROOT
            || path.starts_with(crate::uri::SERVICE_ROOT_PREFIX)
            || path == crate::uri::HOST_LIBRARY_EXPORT
    }

    async fn call(&self, method: Method, path: &str, request: Request) -> Response {
        if path == "/healthz" {
            let resources = request.extensions().get::<crate::HostResources>().cloned();
            let mut response = self.health.call(request).await;
            #[allow(clippy::collapsible_if)]
            if response.status().is_success() {
                if let Some(resources) = resources {
                    let body = serde_json::json!({
                        "status": "ok",
                        "resources": resources.snapshots(),
                    });
                    response.headers_mut().insert(
                        hyper::header::CONTENT_TYPE,
                        hyper::header::HeaderValue::from_static("application/json"),
                    );
                    *response.body_mut() = Body::from(body.to_string());
                }
            }
            return response;
        }
        if path == crate::uri::LIB_ROOT {
            return match method {
                Method::Get => {
                    crate::library::http::schema_get_handler(Arc::clone(&self.registry))
                        .call(request)
                        .await
                }
                Method::Put => match &self.library_put {
                    Some(handler) => handler.call(request).await,
                    None => {
                        crate::library::http::schema_put_handler(Arc::clone(&self.registry))
                            .call(request)
                            .await
                    }
                },
                _ => method_not_allowed(),
            };
        }
        if path.starts_with(crate::uri::LIB_ROOT_PREFIX) {
            return crate::library::http::routes_handler(Arc::clone(&self.registry))
                .call(request)
                .await;
        }
        if path == crate::uri::CLASS_ROOT || path.starts_with(crate::uri::CLASS_ROOT_PREFIX) {
            return match method {
                Method::Get => {
                    crate::library::http::respond_with_listing(self.registry.list_class_dir(path))
                }
                _ => method_not_allowed(),
            };
        }
        if path == crate::uri::SERVICE_ROOT || path.starts_with(crate::uri::SERVICE_ROOT_PREFIX) {
            return self.service.call(request).await;
        }
        if path == "/"
            || path == crate::uri::HOST_ROOT
            || path.starts_with(crate::uri::HOST_ROOT_PREFIX)
        {
            return self.host.call(request).await;
        }
        not_found()
    }
}

pub struct HttpServer {
    pub(super) kernel: Kernel,
    pub(super) router: HttpRouter,
}

impl HttpServer {
    pub fn new(kernel: Kernel, router: HttpRouter) -> Self {
        Self { kernel, router }
    }

    pub async fn serve(self, addr: SocketAddr) -> hyper::Result<()> {
        self.kernel
            .start_transaction_expiry(&tokio::runtime::Handle::current());
        let service = KernelService::new(self.kernel, self.router);
        let make_service = MakeKernelService::new(service);
        hyper::Server::bind(&addr).serve(make_service).await
    }

    pub async fn serve_listener(self, listener: TcpListener) -> hyper::Result<()> {
        self.kernel
            .start_transaction_expiry(&tokio::runtime::Handle::current());
        let service = KernelService::new(self.kernel, self.router);
        let make_service = MakeKernelService::new(service);
        hyper::Server::from_tcp(listener)?.serve(make_service).await
    }

    pub async fn serve_with_shutdown<F>(self, addr: SocketAddr, shutdown: F) -> hyper::Result<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.kernel
            .start_transaction_expiry(&tokio::runtime::Handle::current());
        let service = KernelService::new(self.kernel, self.router);
        let make_service = MakeKernelService::new(service);
        hyper::Server::bind(&addr)
            .serve(make_service)
            .with_graceful_shutdown(shutdown)
            .await
    }

    pub async fn serve_listener_with_shutdown<F>(
        self,
        listener: TcpListener,
        shutdown: F,
    ) -> hyper::Result<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.kernel
            .start_transaction_expiry(&tokio::runtime::Handle::current());
        let service = KernelService::new(self.kernel, self.router);
        let make_service = MakeKernelService::new(service);
        hyper::Server::from_tcp(listener)?
            .serve(make_service)
            .with_graceful_shutdown(shutdown)
            .await
    }
}

#[derive(Clone)]
pub(crate) struct KernelService {
    kernel: Kernel,
    router: HttpRouter,
}

impl KernelService {
    pub(crate) fn new(kernel: Kernel, router: HttpRouter) -> Self {
        Self { kernel, router }
    }
}

impl Service<Request> for KernelService {
    type Response = Response;
    type Error = hyper::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let uri = req.uri().clone();
        let method = req.method().clone();
        let path = uri.path().to_owned();
        let kernel = self.kernel.clone();
        let router = self.router.clone();
        let resources = self.kernel.resources().clone();

        Box::pin(async move {
            let deadline = resources.deadline();
            let request_permit = match resources.admit_request(deadline).await {
                Ok(permit) => permit,
                Err(err) => return Ok(super::response::tc_error_response(err)),
            };
            let method = match to_kernel_method(&method) {
                Some(method) => method,
                None => return Ok(method_not_allowed()),
            };

            let mut req = req;
            req.extensions_mut().insert(resources.clone());

            if !router.requires_transaction(&path) {
                return Ok(router.call(method, &path, req).await);
            }

            let txn_id = match parse_txn_id(&req) {
                Ok(ctx) => ctx,
                Err(TxnParseError::Invalid) => {
                    return Ok(bad_request_response("invalid transaction id"));
                }
            };

            let bearer = parse_bearer_token(&req);
            let body_is_none = hyper::body::HttpBody::size_hint(req.body()).exact() == Some(0);
            let inbound_txn_id = txn_id;
            let token = match bearer {
                Some(token) => match deadline.wait(kernel.token_verifier().verify(token)).await {
                    Err(err) => return Ok(super::response::tc_error_response(err)),
                    Ok(result) => match result {
                        Ok(token) => Some(token),
                        Err(crate::txn::TxnError::Unauthorized) => {
                            return Ok(hyper::Response::builder()
                                .status(StatusCode::UNAUTHORIZED)
                                .body(Body::empty())
                                .expect("unauthorized response"));
                        }
                        Err(crate::txn::TxnError::NotFound) => {
                            unreachable!("verifier does not use NotFound")
                        }
                    },
                },
                None => None,
            };

            let binding = match deadline
                .wait(kernel.bind_transaction(
                    method,
                    &path,
                    body_is_none,
                    inbound_txn_id,
                    token.as_ref(),
                    deadline,
                ))
                .await
            {
                Err(err) => return Ok(super::response::tc_error_response(err)),
                Ok(Ok(binding)) => binding,
                Ok(Err(err)) => return Ok(super::response::tc_error_response(err)),
            };

            match binding {
                None => Ok(handle_finalize_result(Ok(()))),
                Some(BoundTransaction { txn, implicit }) => {
                    req.extensions_mut().insert(txn.clone());
                    if !router.is_native(&path) {
                        let (req, _) = match deadline
                            .wait(parse_body(
                                req,
                                resources.limits().ingress.artifact_body_bytes,
                            ))
                            .await
                        {
                            Err(err) => return Ok(super::response::tc_error_response(err)),
                            Ok(result) => match result {
                                Ok(pair) => pair,
                                Err(response) => return Ok(response),
                            },
                        };
                        let mut response =
                            match deadline.wait(router.call(method, &path, req)).await {
                                Ok(response) => response,
                                Err(err) => return Ok(super::response::tc_error_response(err)),
                            };
                        if implicit {
                            let outcome = crate::txn::TransactionOutcome::from_success(
                                response.status().is_success(),
                            );
                            if let Err(err) = kernel.complete_transaction(txn, outcome).await {
                                response = handle_finalize_result(Err(err));
                            }
                        }
                        return Ok(response);
                    }
                    let body = match deadline
                        .wait(decode_native_body(
                            req,
                            txn.clone(),
                            resources.limits().ingress.request_body_bytes,
                        ))
                        .await
                    {
                        Err(err) => return Ok(super::response::tc_error_response(err)),
                        Ok(result) => match result {
                            Ok(body) => body,
                            Err(err) => {
                                let response = super::response::tc_error_response(err);
                                if implicit {
                                    let _ = kernel
                                        .complete_transaction(
                                            txn,
                                            crate::txn::TransactionOutcome::Failed,
                                        )
                                        .await;
                                }
                                return Ok(response);
                            }
                        },
                    };
                    let path = match pathlink::Link::from_str(&path) {
                        Ok(path) => path,
                        Err(err) => return Ok(bad_request_response(&err.to_string())),
                    };
                    let result = deadline
                        .wait(kernel.execute(KernelRequest {
                            method,
                            path,
                            body,
                            txn: txn.clone(),
                        }))
                        .await
                        .unwrap_or_else(Err);
                    let mut deferred_finalize = false;
                    let mut response = match result {
                        Ok(state) => {
                            let finalize = implicit.then(|| (kernel.clone(), request_permit));
                            match deadline
                                .wait(super::native_state_response(state, txn.clone(), finalize))
                                .await
                                .unwrap_or_else(Err)
                            {
                                Ok(response) => {
                                    deferred_finalize = implicit;
                                    response
                                }
                                Err(err) => super::response::tc_error_response(err),
                            }
                        }
                        Err(err) if err.code() == tc_error::ErrorKind::NotFound => not_found(),
                        Err(err) => super::response::tc_error_response(err),
                    };
                    if implicit && !deferred_finalize {
                        let outcome = crate::txn::TransactionOutcome::from_success(
                            response.status().is_success(),
                        );
                        if let Err(err) = kernel.complete_transaction(txn, outcome).await {
                            response = handle_finalize_result(Err(err));
                        }
                    }
                    Ok(response)
                }
            }
        })
    }
}

#[derive(Clone)]
struct MakeKernelService {
    service: KernelService,
}

impl MakeKernelService {
    fn new(service: KernelService) -> Self {
        Self { service }
    }
}

impl<T> Service<T> for MakeKernelService {
    type Response = ConnectionService;
    type Error = tc_error::TCError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _target: T) -> Self::Future {
        let service = self.service.clone();
        let resources = service.kernel.resources().clone();
        Box::pin(async move {
            let permit = resources.admit_connection(resources.deadline()).await?;
            Ok(ConnectionService {
                service,
                _permit: permit,
            })
        })
    }
}

struct ConnectionService {
    service: KernelService,
    _permit: crate::resources::CapacityPermit,
}

impl Service<Request> for ConnectionService {
    type Response = Response;
    type Error = hyper::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <KernelService as Service<Request>>::poll_ready(&mut self.service, cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        self.service.call(request)
    }
}

pub(crate) fn to_kernel_method(method: &hyper::Method) -> Option<Method> {
    match *method {
        hyper::Method::GET => Some(Method::Get),
        hyper::Method::PUT => Some(Method::Put),
        hyper::Method::POST => Some(Method::Post),
        hyper::Method::DELETE => Some(Method::Delete),
        _ => None,
    }
}
