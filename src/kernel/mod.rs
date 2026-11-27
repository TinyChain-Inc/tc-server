mod drain;
#[allow(clippy::module_inception)]
mod kernel;
mod types;

pub use kernel::{HostServices, Kernel};
pub(crate) use kernel::{KernelInner, invoke_handler};
pub use tc_ir::Method;
pub use types::{BodyContract, BoundRequest};
