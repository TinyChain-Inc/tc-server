use number_general::Number;
use tc_ir::{Id, LibrarySchema, Map};
use tc_value::Value;

use crate::State;

fn entry(name: &str, value: State) -> (Id, State) {
    (name.parse().expect("static library field name"), value)
}

pub(crate) fn schema(schema: &LibrarySchema) -> State {
    let dependencies = schema
        .dependencies()
        .iter()
        .map(|dependency| State::from(Value::from(dependency.to_string())))
        .collect();

    State::Map(Map::from_iter([
        entry("id", State::from(Value::from(schema.id().to_string()))),
        entry(
            "version",
            State::from(Value::from(schema.version().to_string())),
        ),
        entry("dependencies", State::Tuple(dependencies)),
    ]))
}

pub(crate) fn listing(listing: tc_ir::Map<bool>) -> State {
    State::Map(
        listing
            .into_iter()
            .map(|(name, is_dir)| (name, State::from(Value::from(Number::from(is_dir)))))
            .collect(),
    )
}
