mod builder;
#[allow(clippy::module_inception)]
mod kernel;
mod resolver;
mod types;

pub use builder::KernelBuilder;
pub use kernel::Kernel;
pub use tc_ir::Method;
pub(crate) use types::BoundTransaction;
pub use types::KernelRequest;

#[cfg(test)]
mod tests;
