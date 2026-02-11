use std::{
    convert::Infallible,
    net::{SocketAddr, TcpListener},
    task::{Context, Poll},
};

use futures::future::BoxFuture;
use hyper::header::HeaderValue;
use tower::Service;

use crate::{Kernel, KernelDispatch, Method};

use super::parse::{TxnParseError, parse_bearer_token, parse_body, parse_txn_id};
use super::response::{
    bad_request_response, handle_finalize_result, method_not_allowed, not_found,
};
use super::{Body, Request, Response, StatusCode};

pub struct HttpServer {
    pub(super) kernel: Kernel,
    pub(super) limits: crate::KernelLimits,
}

impl HttpServer {
    pub fn new(kernel: Kernel) -> Self {
        Self {
            kernel,
            limits: crate::KernelLimits::default(),
        }
    }

    pub fn new_with_limits(kernel: Kernel, max_request_bytes_unauth: usize) -> Self {
        Self {
            kernel,
            limits: crate::KernelLimits {
                max_request_bytes_unauth,
                ..crate::KernelLimits::default()
            },
        }
    }

    pub async fn serve(self, addr: SocketAddr) -> hyper::Result<()> {
        let service = KernelService::new(self.kernel, self.limits);
        let make_service = tower::make::Shared::new(service);
        hyper::Server::bind(&addr).serve(make_service).await
    }

    pub async fn serve_listener(self, listener: TcpListener) -> hyper::Result<()> {
        let service = KernelService::new(self.kernel, self.limits);
        let make_service = tower::make::Shared::new(service);
        hyper::Server::from_tcp(listener)?.serve(make_service).await
    }

    pub async fn serve_with_shutdown<F>(self, addr: SocketAddr, shutdown: F) -> hyper::Result<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let service = KernelService::new(self.kernel, self.limits);
        let make_service = tower::make::Shared::new(service);
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
        let service = KernelService::new(self.kernel, self.limits);
        let make_service = tower::make::Shared::new(service);
        hyper::Server::from_tcp(listener)?
            .serve(make_service)
            .with_graceful_shutdown(shutdown)
            .await
    }
}

#[derive(Clone)]
pub(crate) struct KernelService {
    kernel: Kernel,
    limits: crate::KernelLimits,
}

impl KernelService {
    pub(crate) fn new(kernel: Kernel, limits: crate::KernelLimits) -> Self {
        Self { kernel, limits }
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
        let max_request_bytes_unauth = self.limits.max_request_bytes_unauth;

        Box::pin(async move {
            let method = match to_kernel_method(&method) {
                Some(method) => method,
                None => return Ok(method_not_allowed()),
            };

            let (req, body_is_none) = match parse_body(req, max_request_bytes_unauth).await {
                Ok(pair) => pair,
                Err(resp) => return Ok(resp),
            };

            let txn_id = match parse_txn_id(&req) {
                Ok(ctx) => ctx,
                Err(TxnParseError::Invalid) => {
                    return Ok(bad_request_response("invalid transaction id"));
                }
            };

            let bearer = parse_bearer_token(&req);
            let inbound_txn_id = txn_id;
            let inbound_bearer = bearer.clone();
            let mut minted_txn_id = None;
            let mut minted_token = None;
            let token = match bearer {
                Some(token) => match kernel.token_verifier().verify(token).await {
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
                None => None,
            };

            match kernel.route_request(
                method,
                &path,
                req,
                inbound_txn_id,
                body_is_none,
                token.as_ref(),
                |handle, req| {
                    minted_txn_id = Some(handle.id());
                    if inbound_bearer.is_none() {
                        minted_token = handle.raw_token().map(str::to_string);
                    }
                    req.extensions_mut().insert(handle.clone());
                },
            ) {
                Ok(KernelDispatch::Response(resp)) => {
                    let mut response = resp.await;
                    if inbound_txn_id.is_none() {
                        if let Some(value) = minted_txn_id
                            .map(|txn_id| HeaderValue::from_str(&txn_id.to_string()))
                            .and_then(Result::ok)
                        {
                            response.headers_mut().insert("x-tc-txn-id", value);
                        }
                        if let Some(value) =
                            minted_token.and_then(|token| HeaderValue::from_str(&token).ok())
                        {
                            response.headers_mut().insert("x-tc-bearer-token", value);
                        }
                    }

                    Ok(response)
                }
                Ok(KernelDispatch::Finalize { commit: _, result }) => {
                    Ok(handle_finalize_result(result))
                }
                Ok(KernelDispatch::NotFound) => Ok(not_found()),
                Err(crate::txn::TxnError::NotFound) => {
                    Ok(bad_request_response("unknown transaction id"))
                }
                Err(crate::txn::TxnError::Unauthorized) => Ok(hyper::Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body(Body::empty())
                    .expect("unauthorized response")),
            }
        })
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

impl tower::Service<()> for KernelService {
    type Response = Self;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: ()) -> Self::Future {
        let svc = self.clone();
        Box::pin(async move { Ok(svc) })
    }
}
