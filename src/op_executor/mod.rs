mod execute;
mod executor;

pub(crate) use execute::execute;
#[cfg(test)]
use executor::resolve_with_admission;

#[cfg(test)]
mod tests;
