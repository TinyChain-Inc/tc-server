use std::future::Future;
use std::net::TcpListener;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tower::Service;

use crate::{Kernel, Method};
use super::parse::{decode_body, parse_bearer_token};
use super::response::method_not_allowed;
use super::{Request, Response};

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

    /// Serve until failure or explicit kernel shutdown, then join host work.
    pub async fn serve_listener(self, listener: TcpListener) -> hyper::Result<()> {
        self.serve_listener_with_shutdown(listener, std::future::pending())
            .await
    }

    /// Stop admission on the signal, drain transports, then join finalization.
    /// Keep polling this future until it returns before dropping the runtime.
    pub async fn serve_listener_with_shutdown<F>(
        self,
        listener: TcpListener,
        shutdown: F,
    ) -> hyper::Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let kernel = self.kernel.clone();
        let transport = Transport::new();
        let result = match hyper::Server::from_tcp(listener) {
            Ok(server) => {
                let stopping = kernel.clone();
                let server = server
                    .executor(transport.clone())
                    .serve(self.into_service())
                    .with_graceful_shutdown(async move {
                        tokio::select! {
                            _ = shutdown => {},
                            _ = stopping.shutdown_started() => {},
                        }

                        stopping.begin_shutdown();
                    });

                tokio::pin!(server);
                tokio::select! {
                    result = &mut server => result,
                    deadline = kernel.shutdown_started() => {
                        // A backpressured socket may stop polling its response body.
                        // By this deadline all admitted requests have expired; cancel
                        // transport tasks, never a canonical finalization pass.
                        tokio::time::timeout_at(deadline.instant(), &mut server)
                            .await.unwrap_or(Ok(()))
                    }
                }
            }
            Err(error) => Err(error),
        };

        kernel.begin_shutdown();
        transport.tasks.close();
        // Listener failure also drains already accepted work before cancellation.
        let deadline = kernel.shutdown_started().await;
        let _ = tokio::time::timeout_at(deadline.instant(), transport.tasks.wait()).await;
        transport.stop.cancel();
        transport.tasks.wait().await;
        kernel.shutdown().await;
        result
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
                    Ok((_, permit, request)) => (permit, request),
                    Err(err) => return Ok(super::response::tc_error_response(err)),
                };

                let state = match path.as_str() {
                    crate::uri::HOST_HEALTH => kernel.health(method),
                    crate::uri::HOST_METRICS => kernel.metrics(method),
                    _ => {
                        let actor = match super::host::public_key_actor(&req).await {
                            Ok(actor) => actor,
                            Err(response) => return Ok(*response),
                        };

                        kernel.public_key(method, &actor)
                    }
                };

                return Ok(state
                    .map(super::state_response)
                    .unwrap_or_else(super::response::tc_error_response));
            }

            let bearer = parse_bearer_token(&req);
            let body_is_none = hyper::body::HttpBody::size_hint(req.body()).exact() == Some(0);
            let raw_path = uri
                .path_and_query()
                .map(|path| path.as_str())
                .unwrap_or(&path);

            // Keep the request state on the heap: embedding it in both deadline
            // wrappers duplicates large polling frames on ordinary worker stacks.
            let result = deadline
                .run(Box::pin(async {
                    let request = *kernel
                        .begin_request(method, raw_path, body_is_none, bearer)
                        .await?;

                    let (body, _admission) = decode_body(req, &request).await?;

                    let response = request.execute_bound(body).await?;
                    super::response::request_response(response).await
                }))
                .await;

            Ok(result.unwrap_or_else(super::response::tc_error_response))
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

/// Hyper owns connection processing; Tokio owns task completion and cancellation.
#[derive(Clone)]
struct Transport {
    tasks: TaskTracker,
    stop: CancellationToken,
}

impl Transport {
    fn new() -> Self {
        Self {
            tasks: TaskTracker::new(),
            stop: CancellationToken::new(),
        }
    }
}

impl<F> hyper::rt::Executor<F> for Transport
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    fn execute(&self, future: F) {
        let stop = self.stop.clone();
        self.tasks.spawn(async move {
            tokio::select! {
                biased;
                _ = stop.cancelled() => {},
                _ = future => {},
            }
        });
    }
}

#[cfg(test)]
#[path = "../../tests/support/http_shutdown.rs"]
mod tests;
