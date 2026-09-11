#[allow(clippy::module_inception)]
mod kernel;
mod types;

pub(crate) use kernel::KernelInner;
pub(crate) use kernel::invoke_handler;
pub use kernel::{HostServices, Kernel};
pub use tc_ir::Method;
pub use types::{BodyContract, KernelRequestGuard};
