mod bootstrap;
#[allow(clippy::module_inception)]
mod kernel;
pub(crate) mod resolver;
mod services;
mod types;

pub use kernel::Kernel;
pub(crate) use kernel::invoke_handler;
pub use services::HostServices;
pub(crate) use services::KernelInner;
pub use tc_ir::Method;
#[cfg(test)]
pub(crate) use types::KernelTarget;
pub use types::{BodyContract, KernelRequestGuard};

#[cfg(test)]
mod tests;
