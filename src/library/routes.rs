use std::sync::{Arc, RwLock};

use crate::KernelHandler;

use super::HandlerArc;

pub struct LibraryRoutes {
    inner: Arc<RwLock<Option<HandlerArc>>>,
}

impl LibraryRoutes {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(None)),
        }
    }

    pub fn replace<H>(&self, handler: H)
    where
        H: KernelHandler,
    {
        self.replace_arc(Arc::new(handler));
    }

    pub fn replace_arc(&self, handler: HandlerArc) {
        self.inner
            .write()
            .expect("library routes write lock")
            .replace(handler);
    }

    pub fn clear(&self) {
        self.inner
            .write()
            .expect("library routes write lock")
            .take();
    }

    pub fn current_handler(&self) -> Option<HandlerArc> {
        self.inner.read().expect("library routes read lock").clone()
    }
}

impl Default for LibraryRoutes {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for LibraryRoutes {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

pub struct LibraryHandlers {
    get: HandlerArc,
    put: HandlerArc,
    route: Option<HandlerArc>,
}

impl LibraryHandlers {
    pub fn with_route<G, P, R>(get: G, put: P, route: R) -> Self
    where
        G: KernelHandler,
        P: KernelHandler,
        R: KernelHandler,
    {
        Self {
            get: Arc::new(get),
            put: Arc::new(put),
            route: Some(Arc::new(route)),
        }
    }

    pub fn without_route<G, P>(get: G, put: P) -> Self
    where
        G: KernelHandler,
        P: KernelHandler,
    {
        Self {
            get: Arc::new(get),
            put: Arc::new(put),
            route: None,
        }
    }

    pub fn get_handler(&self) -> HandlerArc {
        Arc::clone(&self.get)
    }

    pub fn put_handler(&self) -> HandlerArc {
        Arc::clone(&self.put)
    }

    pub fn route_handler(&self) -> Option<HandlerArc> {
        self.route.as_ref().map(Arc::clone)
    }
}

impl Clone for LibraryHandlers {
    fn clone(&self) -> Self {
        Self {
            get: Arc::clone(&self.get),
            put: Arc::clone(&self.put),
            route: self.route.as_ref().map(Arc::clone),
        }
    }
}
