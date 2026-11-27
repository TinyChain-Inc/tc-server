mod execute;
mod executor;

pub(crate) use execute::execute;
#[cfg(test)]
use executor::resolve_with_admission;

#[cfg(test)]
#[path = "../../tests/support/op_executor.rs"]
mod tests;
