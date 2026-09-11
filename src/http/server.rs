use std::net::TcpListener;
use std::task::{Context, Poll};

use futures::{TryStreamExt, future::BoxFuture};
use tower::Service;

use super::parse::{decode_native_body, parse_bearer_token};
use super::response::{method_not_allowed, not_found};
use super::{Request, Response};
use crate::{Kernel, Method};

pub struct HttpServer {
    pub(super) kernel: Kernel,
}

impl HttpServer {
    pub fn new(kernel: Kernel) -> Self {
        Self { kernel }
    }

    fn into_service(self) -> MakeKernelService {
        MakeKernelService::new(KernelService::new(self.kernel))
    }

    pub async fn serve_listener(self, listener: TcpListener) -> hyper::Result<()> {
        hyper::Server::from_tcp(listener)?
            .serve(self.into_service())
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
        hyper::Server::from_tcp(listener)?
            .serve(self.into_service())
            .with_graceful_shutdown(shutdown)
            .await
    }
}

#[derive(Clone)]
pub(crate) struct KernelService {
    kernel: Kernel,
}

impl KernelService {
    pub(crate) fn new(kernel: Kernel) -> Self {
        Self { kernel }
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
        let resources = self.kernel.resources().clone();

        Box::pin(async move {
            let deadline = resources.deadline();
            let method = match to_kernel_method(&method) {
                Some(method) => method,
                None => return Ok(method_not_allowed()),
            };

            let mut req = req;
            req.extensions_mut().insert(resources.clone());

            if path == crate::uri::HOST_HEALTH
                || path == crate::uri::HOST_METRICS
                || path == crate::uri::HOST_PUBLIC_KEY
            {
                let _request = match resources.admit_request(deadline).await {
                    Ok(permit) => permit,
                    Err(err) => return Ok(super::response::tc_error_response(err)),
                };
                if path == crate::uri::HOST_HEALTH {
                    if !kernel.is_ready() {
                        return Ok(super::response::tc_error_response(tc_error::TCError::new(
                            tc_error::ErrorKind::Unavailable,
                            "host is not ready",
                        )));
                    }
                    return Ok(match kernel.health(method) {
                        Ok(health) => super::state_response(health),
                        Err(error) => super::response::tc_error_response(error),
                    });
                }
                if path == crate::uri::HOST_METRICS {
                    return Ok(match kernel.metrics(method) {
                        Ok(metrics) => super::state_response(metrics),
                        Err(error) => super::response::tc_error_response(error),
                    });
                }
                if path == crate::uri::HOST_PUBLIC_KEY {
                    let actor = match super::host::public_key_actor(&req).await {
                        Ok(actor) => actor,
                        Err(response) => return Ok(*response),
                    };
                    return Ok(match kernel.public_key(method, &actor) {
                        Ok(key) => super::state_response(key),
                        Err(error) => super::response::tc_error_response(error),
                    });
                }
            }

            let bearer = parse_bearer_token(&req);
            let body_is_none = hyper::body::HttpBody::size_hint(req.body()).exact() == Some(0);
            let raw_path = uri
                .path_and_query()
                .map(|path| path.as_str())
                .unwrap_or(&path);

            let guard = match deadline
                .wait(kernel.begin_request(method, raw_path, body_is_none, bearer, deadline))
                .await
            {
                Err(err) => return Ok(super::response::tc_error_response(err)),
                Ok(Ok(guard)) => *guard,
                Ok(Err(err)) => return Ok(super::response::tc_error_response(err)),
            };

            {
                let contract = guard.body_contract();
                let txn = guard.txn().clone();
                req.extensions_mut().insert(txn.clone());
                if let crate::BodyContract::Application { max_bytes: limit } = contract {
                    let content_type = req
                        .headers()
                        .get(http::header::CONTENT_TYPE)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("application/json")
                        .to_string();
                    if req
                        .headers()
                        .get(http::header::CONTENT_LENGTH)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.parse::<usize>().ok())
                        .is_some_and(|length| length > limit)
                    {
                        return Ok(super::response::payload_too_large_response(
                            "application exceeds its request bound",
                        ));
                    }
                    let body = req.into_body().map_err(std::io::Error::other);
                    let result = match guard.admit_body(body, limit).await {
                        Ok(body) => {
                            let state = match content_type.as_str() {
                                "application/wasm" => Ok(crate::State::Tuple(vec![
                                    crate::State::None,
                                    crate::State::from(tc_value::Value::Bytes(body.shared())),
                                ])),
                                "application/json" => crate::literal::decode(&body.shared(), limit)
                                    .await
                                    .map(|(identity, definition)| {
                                        crate::State::Tuple(vec![
                                            crate::State::from(tc_value::Value::Link(identity)),
                                            crate::State::from_scalar(definition),
                                        ])
                                    }),
                                _ => Err(tc_error::TCError::bad_request(
                                    "unsupported application content type",
                                )),
                            };
                            match state {
                                Ok(state) => guard.execute(Some(state)).await.map(|_| ()),
                                Err(error) => Err(error),
                            }
                        }
                        Err(error) => Err(error),
                    };
                    return Ok(match result {
                        Ok(_) => super::response::no_content(),
                        Err(error) => super::response::tc_error_response(error),
                    });
                }
                let body = match deadline
                    .wait(decode_native_body(
                        req,
                        txn.clone(),
                        resources.limits().ingress.request_body_bytes,
                    ))
                    .await
                {
                    Err(err) => {
                        return Ok(super::response::tc_error_response(err));
                    }
                    Ok(result) => match result {
                        Ok(body) => body,
                        Err(err) => {
                            return Ok(super::response::tc_error_response(err));
                        }
                    },
                };
                let result = deadline
                    .wait(guard.execute_bound(body))
                    .await
                    .unwrap_or_else(Err);
                match result {
                    Ok((Some(state), guard)) => {
                        match deadline
                            .wait(super::native_state_response(state, txn, Some(guard)))
                            .await
                            .unwrap_or_else(Err)
                        {
                            Ok(response) => Ok(response),
                            Err(err) => Ok(super::response::tc_error_response(err)),
                        }
                    }
                    Ok((None, _guard)) => Ok(super::response::no_content()),
                    Err(err) if err.code() == tc_error::ErrorKind::NotFound => Ok(not_found()),
                    Err(err) => Ok(super::response::tc_error_response(err)),
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
    method.as_str().parse().ok()
}
