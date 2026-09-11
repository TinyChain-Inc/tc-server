#[allow(clippy::module_inception)]
mod kernel;
pub(crate) mod resolver;
mod types;

pub(crate) use kernel::KernelInner;
pub(crate) use kernel::invoke_handler;
pub use kernel::{HostServices, Kernel};
pub use tc_ir::Method;
#[cfg(any(test, feature = "http-server"))]
pub(crate) use types::KernelTarget;
pub use types::{BodyContract, KernelRequestGuard};

#[cfg(test)]
mod tests;
