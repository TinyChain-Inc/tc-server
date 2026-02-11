use std::fmt;

use futures::future::BoxFuture;

use crate::{Request, Response};

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

pub trait KernelHandler: Send + Sync + 'static {
    fn call(&self, req: Request) -> BoxFuture<'static, Response>;
}

impl<F, Fut> KernelHandler for F
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: futures::Future<Output = Response> + Send + 'static,
{
    fn call(&self, req: Request) -> BoxFuture<'static, Response> {
        Box::pin((self)(req))
    }
}
