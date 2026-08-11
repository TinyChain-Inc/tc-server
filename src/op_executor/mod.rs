mod collection;
mod execute;
mod executor;
mod reflect;
mod resolve;

pub use execute::{
    execute_delete, execute_delete_with_self, execute_get, execute_get_with_self, execute_post,
    execute_post_with_self, execute_put, execute_put_with_self,
};
pub use executor::Executor;
pub(crate) use resolve::resolve_scalar;

#[cfg(test)]
mod tests;
