use std::sync::Arc;

use futures::future::BoxFuture;
use std::fmt;
use tc_ir::TxnId;

use crate::library::{LibraryHandlers, LibraryRuntime};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Method {
    Get,
    Put,
    Post,
    Delete,
}

impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Put => "PUT",
            Method::Post => "POST",
            Method::Delete => "DELETE",
        }
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub trait KernelHandler<Request, Response>: Send + Sync + 'static {
    fn call(&self, req: Request) -> BoxFuture<'static, Response>;
}

impl<F, Fut, Request, Response> KernelHandler<Request, Response> for F
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: futures::Future<Output = Response> + Send + 'static,
{
    fn call(&self, req: Request) -> BoxFuture<'static, Response> {
        Box::pin((self)(req))
    }
}

pub struct Kernel<Request, Response> {
    lib_get_handler: Arc<dyn KernelHandler<Request, Response>>,
    lib_put_handler: Arc<dyn KernelHandler<Request, Response>>,
    lib_route_handler: Option<Arc<dyn KernelHandler<Request, Response>>>,
    service_handler: Arc<dyn KernelHandler<Request, Response>>,
    kernel_handler: Arc<dyn KernelHandler<Request, Response>>,
    health_handler: Arc<dyn KernelHandler<Request, Response>>,
    txn_manager: crate::txn::TxnManager,
    library_module: Option<Arc<LibraryRuntime<Request, Response>>>,
}

impl<Request, Response> Kernel<Request, Response>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
    pub fn builder() -> KernelBuilder<Request, Response> {
        KernelBuilder::new()
    }

    pub fn dispatch(
        &self,
        method: Method,
        path: &str,
        req: Request,
    ) -> Option<BoxFuture<'static, Response>> {
        if path.starts_with("/lib/")
            && let Some(handler) = &self.lib_route_handler
        {
            return Some(handler.call(req));
        }

        match (method, path) {
            (Method::Get, "/lib") => Some(self.lib_get_handler.call(req)),
            (Method::Put, "/lib") => Some(self.lib_put_handler.call(req)),
            (Method::Get, "/service") => Some(self.service_handler.call(req)),
            (Method::Get, "/kernel/metrics") => Some(self.kernel_handler.call(req)),
            (Method::Get, "/healthz") => Some(self.health_handler.call(req)),
            _ => None,
        }
    }

    pub fn txn_manager(&self) -> &crate::txn::TxnManager {
        &self.txn_manager
    }

    pub fn route_request<F>(
        &self,
        method: Method,
        path: &str,
        mut req: Request,
        txn_id: Option<TxnId>,
        body_is_none: bool,
        mut bind_txn: F,
    ) -> Result<KernelDispatch<Response>, crate::txn::TxnError>
    where
        F: FnMut(&crate::txn::TxnHandle, &mut Request),
    {
        use crate::txn::TxnFlow;

        let flow = self
            .txn_manager
            .interpret_request(method, txn_id, body_is_none)?;

        let dispatch = match flow {
            TxnFlow::Begin(handle) => {
                bind_txn(&handle, &mut req);
                dispatch_or_not_found(self.dispatch(method, path, req))
            }
            TxnFlow::Use(Some(handle)) => {
                bind_txn(&handle, &mut req);
                dispatch_or_not_found(self.dispatch(method, path, req))
            }
            TxnFlow::Use(None) => dispatch_or_not_found(self.dispatch(method, path, req)),
            TxnFlow::Commit(handle) => KernelDispatch::Finalize {
                commit: true,
                result: self.txn_manager.commit(handle.id()),
            },
            TxnFlow::Rollback(handle) => KernelDispatch::Finalize {
                commit: false,
                result: self.txn_manager.rollback(handle.id()),
            },
        };

        Ok(dispatch)
    }
}

fn dispatch_or_not_found<Response>(
    fut: Option<BoxFuture<'static, Response>>,
) -> KernelDispatch<Response> {
    match fut {
        Some(fut) => KernelDispatch::Response(fut),
        None => KernelDispatch::NotFound,
    }
}

pub enum KernelDispatch<Response> {
    Response(BoxFuture<'static, Response>),
    Finalize {
        commit: bool,
        result: Result<(), crate::txn::TxnError>,
    },
    NotFound,
}

impl<Request, Response> Clone for Kernel<Request, Response>
where
    Request: Send + 'static,
    Response: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            lib_get_handler: Arc::clone(&self.lib_get_handler),
            lib_put_handler: Arc::clone(&self.lib_put_handler),
            lib_route_handler: self.lib_route_handler.as_ref().map(Arc::clone),
            service_handler: Arc::clone(&self.service_handler),
            kernel_handler: Arc::clone(&self.kernel_handler),
            health_handler: Arc::clone(&self.health_handler),
            txn_manager: self.txn_manager.clone(),
            library_module: self.library_module.as_ref().map(Arc::clone),
        }
    }
}

pub struct KernelBuilder<Request, Response> {
    lib_get_handler: Option<Arc<dyn KernelHandler<Request, Response>>>,
    lib_put_handler: Option<Arc<dyn KernelHandler<Request, Response>>>,
    lib_route_handler: Option<Arc<dyn KernelHandler<Request, Response>>>,
    service_handler: Option<Arc<dyn KernelHandler<Request, Response>>>,
    kernel_handler: Option<Arc<dyn KernelHandler<Request, Response>>>,
    health_handler: Option<Arc<dyn KernelHandler<Request, Response>>>,
    txn_manager: crate::txn::TxnManager,
    library_module: Option<Arc<LibraryRuntime<Request, Response>>>,
}

impl<Request: Send + 'static, Response: Send + 'static> Default
    for KernelBuilder<Request, Response>
{
    fn default() -> Self {
        Self {
            lib_get_handler: None,
            lib_put_handler: None,
            lib_route_handler: None,
            service_handler: None,
            kernel_handler: None,
            health_handler: None,
            txn_manager: crate::txn::TxnManager::new(),
            library_module: None,
        }
    }
}

impl<Request: Send + 'static, Response: Send + 'static> KernelBuilder<Request, Response> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_lib_handler<H>(mut self, handler: H) -> Self
    where
        H: KernelHandler<Request, Response>,
    {
        self.lib_get_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_lib_put_handler<H>(mut self, handler: H) -> Self
    where
        H: KernelHandler<Request, Response>,
    {
        self.lib_put_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_service_handler<H>(mut self, handler: H) -> Self
    where
        H: KernelHandler<Request, Response>,
    {
        self.service_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_lib_route_handler<H>(mut self, handler: H) -> Self
    where
        H: KernelHandler<Request, Response>,
    {
        self.lib_route_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_kernel_handler<H>(mut self, handler: H) -> Self
    where
        H: KernelHandler<Request, Response>,
    {
        self.kernel_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_health_handler<H>(mut self, handler: H) -> Self
    where
        H: KernelHandler<Request, Response>,
    {
        self.health_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_library_module(
        mut self,
        module: Arc<LibraryRuntime<Request, Response>>,
        handlers: LibraryHandlers<Request, Response>,
    ) -> Self {
        self.library_module = Some(module);
        self.lib_get_handler = Some(handlers.get_handler());
        self.lib_put_handler = Some(handlers.put_handler());
        self.lib_route_handler = handlers.route_handler();
        self
    }

    pub fn with_host_id(mut self, host_id: impl Into<String>) -> Self {
        self.txn_manager = crate::txn::TxnManager::with_host_id(host_id.into());
        self
    }

    pub fn finish(self) -> Kernel<Request, Response> {
        if let Some(module) = &self.library_module
            && let Err(err) = module.hydrate_from_storage()
        {
            panic!("failed to hydrate library storage: {err}");
        }

        Kernel {
            lib_get_handler: self
                .lib_get_handler
                .unwrap_or_else(|| stub_handler("lib GET")),
            lib_put_handler: self
                .lib_put_handler
                .unwrap_or_else(|| stub_handler("lib PUT")),
            lib_route_handler: self.lib_route_handler,
            service_handler: self
                .service_handler
                .unwrap_or_else(|| stub_handler("service")),
            kernel_handler: self
                .kernel_handler
                .unwrap_or_else(|| stub_handler("kernel/metrics")),
            health_handler: self
                .health_handler
                .unwrap_or_else(|| stub_handler("healthz")),
            txn_manager: self.txn_manager,
            library_module: self.library_module,
        }
    }
}

fn stub_handler<Request: Send + 'static, Response: Send + 'static>(
    label: &str,
) -> Arc<dyn KernelHandler<Request, Response>> {
    let label = label.to_string();
    Arc::new(move |_req: Request| {
        let label = label.clone();
        async move { panic!("stub handler {label} not implemented") }
    })
}
