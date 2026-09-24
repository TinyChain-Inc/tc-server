use pathlink::{Link, PathSegment};
use tc_error::{TCError, TCResult};
use tc_ir::{GetHandler, Handler, Map, OpRef, Scalar};

use crate::State;

struct ValueHandler(State);

impl<'a> Handler<'a, State> for ValueHandler {
    fn get<'txn>(self: Box<Self>) -> Option<GetHandler<'a, 'txn, State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |_txn, _key| {
            Box::pin(async move { Ok(self.0) })
        }))
    }
}

struct RefHandler(OpRef);

impl<'a> Handler<'a, State> for RefHandler {
    fn get<'txn>(self: Box<Self>) -> Option<GetHandler<'a, 'txn, State>>
    where
        'txn: 'a,
    {
        Some(Box::new(move |txn, _key| {
            tc_state::resolve_ref(tc_ir::TCRef::Op(self.0), txn, None)
        }))
    }
}

pub(crate) struct LibraryAnalysis {
    pub(crate) members: Map<Scalar>,
    pub(crate) requirements: crate::txn::Requirements,
}

pub(crate) fn compile_ir_library(definition: Scalar) -> TCResult<LibraryAnalysis> {
    let Scalar::Map(members) = definition else {
        return Err(TCError::bad_request("a Library definition must be a map"));
    };
    let requirements = application_requirements(members.values());
    Ok(LibraryAnalysis {
        members,
        requirements,
    })
}

pub(crate) fn application_requirements<'a>(
    values: impl IntoIterator<Item = &'a Scalar>,
) -> crate::txn::Requirements {
    let mut requirements = crate::txn::Requirements::new();
    for value in values {
        value.visit_referenced_methods(&mut |target, method| {
            if let Ok(identity) = crate::uri::application_identity(target) {
                requirements.entry(identity).or_default().insert(method);
            }
        });
    }
    requirements
}

pub(crate) fn member<'a>(members: &'a Map<Scalar>, path: &[PathSegment]) -> Option<&'a Scalar> {
    let (name, suffix) = path.split_first()?;
    let mut member = members.get(name.as_str())?;
    for segment in suffix {
        let Scalar::Map(children) = member else {
            return None;
        };
        member = children.get(segment.as_str())?;
    }
    Some(member)
}

pub(crate) fn route_member<'a>(
    identity: &Link,
    members: &'a Map<Scalar>,
    path: &[PathSegment],
) -> Option<Box<dyn Handler<'a, State> + 'a>> {
    Some(route_scalar(
        State::from(tc_value::Value::Link(identity.clone())),
        member(members, path)?,
    ))
}

pub(crate) fn route_scalar<'a>(
    subject: State,
    scalar: &Scalar,
) -> Box<dyn Handler<'a, State> + 'a> {
    match scalar.clone() {
        Scalar::Op(definition) => Box::new(tc_state::BoundMethod::new(subject, definition)),
        Scalar::Ref(reference) => match *reference {
            tc_ir::TCRef::Op(op) => Box::new(RefHandler(op)),
            reference => Box::new(ValueHandler(State::from(Scalar::from(reference)))),
        },
        value => Box::new(ValueHandler(State::from_scalar(value))),
    }
}

#[cfg(test)]
#[path = "../../tests/support/ir.rs"]
mod tests;
