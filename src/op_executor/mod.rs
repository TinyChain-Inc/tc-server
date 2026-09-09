mod execute;
mod executor;

pub use execute::{
    execute_delete, execute_delete_with_self, execute_get, execute_get_with_self, execute_post,
    execute_post_with_self, execute_put, execute_put_with_self,
};
pub use executor::Executor;
#[cfg(test)]
use executor::resolve_with_admission;

#[cfg(test)]
mod tests;
