mod builder;
mod dispatch;
#[allow(clippy::module_inception)]
mod kernel;
mod resolver;
mod types;

pub use builder::KernelBuilder;
pub use dispatch::KernelDispatch;
pub use kernel::Kernel;
pub use types::{KernelHandler, Method, TxnFinalizeHook};

#[cfg(test)]
include!("tests.rs");
