use std::net::TcpListener;
use std::task::{Context, Poll};

use futures::{TryStreamExt, future::BoxFuture};
use tower::Service;

use super::parse::{decode_native_body, parse_bearer_token};
use super::response::method_not_allowed;
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

        Box::pin(async move {
            let deadline = kernel.deadline();
            let method = match to_kernel_method(&method) {
                Some(method) => method,
                None => return Ok(method_not_allowed()),
            };

            if path == crate::uri::HOST_HEALTH
                || path == crate::uri::HOST_METRICS
                || path == crate::uri::HOST_PUBLIC_KEY
            {
                let _request = match kernel.admit_host_request().await {
                    Ok((_, permit)) => permit,
                    Err(err) => return Ok(super::response::tc_error_response(err)),
                };
                if path == crate::uri::HOST_HEALTH {
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
                .wait(kernel.begin_request(method, raw_path, body_is_none, bearer))
                .await
            {
                Err(err) => return Ok(super::response::tc_error_response(err)),
                Ok(Ok(guard)) => *guard,
                Ok(Err(err)) => return Ok(super::response::tc_error_response(err)),
            };

            {
                let contract = guard.body_contract();
                let txn = guard.txn().clone();
                if let crate::BodyContract::Application { max_bytes: limit } = contract {
                    let admission = guard.application_admission();
                    let content_type = req
                        .headers()
                        .get(http::header::CONTENT_TYPE)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("application/json");
                    let is_wasm = match content_type {
                        "application/json" => false,
                        "application/wasm" => true,
                        _ => {
                            return Ok(super::response::tc_error_response(
                                tc_error::TCError::bad_request(
                                    "unsupported application content type",
                                ),
                            ));
                        }
                    };
                    let mut body =
                        crate::http_body::BoundedBody::new(req.into_body(), limit, Some(admission));
                    let state = if is_wasm {
                        match (&mut body)
                            .try_fold(Vec::new(), |mut bytes, chunk| async move {
                                bytes.extend_from_slice(&chunk);
                                Ok(bytes)
                            })
                            .await
                        {
                            Ok(bytes) if bytes.is_empty() => {
                                Err(tc_error::TCError::bad_request("empty WASM Library"))
                            }
                            Ok(bytes) => Ok(crate::State::Tuple(vec![
                                crate::State::None,
                                crate::State::from(tc_value::Value::Bytes(bytes.into())),
                            ])),
                            Err(error) => Err(body.decode_error(error)),
                        }
                    } else {
                        match destream_json::try_decode::<_, _, crate::literal::Definition>(
                            (),
                            &mut body,
                        )
                        .await
                        {
                            Ok(crate::literal::Definition(identity, definition)) => {
                                Ok(crate::State::Tuple(vec![
                                    crate::State::from(tc_value::Value::Link(identity)),
                                    crate::State::from_scalar(definition),
                                ]))
                            }
                            Err(error) => Err(body.decode_error(error)),
                        }
                    };
                    let _admission = body.permit.take();
                    let result = match state {
                        Ok(state) => match guard.execute(Some(state)).await {
                            Ok(Some(_)) => guard.finish_success().await,
                            Ok(None) => Ok(()),
                            Err(error) => Err(error),
                        },
                        Err(error) => Err(error),
                    };
                    return Ok(match result {
                        Ok(_) => super::response::no_content(),
                        Err(error) => super::response::tc_error_response(error),
                    });
                }
                let native_body_limit = guard.native_body_limit();
                let body = match deadline
                    .wait(decode_native_body(req, txn.clone(), native_body_limit))
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
                    Ok((Some(state), raw_application_bytes, guard)) => {
                        match deadline
                            .wait(super::native_state_response(
                                state,
                                txn,
                                raw_application_bytes,
                                Some(guard),
                            ))
                            .await
                            .unwrap_or_else(Err)
                        {
                            Ok(response) => Ok(response),
                            Err(err) => Ok(super::response::tc_error_response(err)),
                        }
                    }
                    Ok((None, _, _guard)) => Ok(super::response::no_content()),
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
        Box::pin(async move {
            let permit = service.kernel.admit_connection().await?;
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
