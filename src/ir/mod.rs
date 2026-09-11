use pathlink::{Link, PathSegment};
use tc_error::{TCError, TCResult};
use tc_ir::{
    DeleteHandler, GetHandler, Handler, Map, OpDef, OpRef, PostHandler, PutHandler, Scalar,
};

use crate::State;

#[derive(Clone)]
struct OpHandler {
    subject: Link,
    definition: OpDef,
}

impl OpHandler {
    fn subject(&self) -> State {
        State::from(tc_value::Value::Link(self.subject.clone()))
    }
}

impl<'a> Handler<'a, State> for OpHandler {
    fn get<'txn>(self: Box<Self>) -> Option<GetHandler<'a, 'txn, State>>
    where
        'txn: 'a,
    {
        matches!(self.definition, OpDef::Get(_)).then(|| {
            Box::new(move |txn, key| {
                Box::pin(async move {
                    let subject = self.subject();
                    crate::op_executor::execute_get_with_self(
                        txn,
                        self.definition,
                        key,
                        Some(subject),
                    )
                    .await
                }) as tc_ir::HandlerFuture<'a, State>
            }) as GetHandler<'a, 'txn, State>
        })
    }

    fn put<'txn>(self: Box<Self>) -> Option<PutHandler<'a, 'txn, State>>
    where
        'txn: 'a,
    {
        matches!(self.definition, OpDef::Put(_)).then(|| {
            Box::new(move |txn, key, value| {
                Box::pin(async move {
                    let subject = self.subject();
                    crate::op_executor::execute_put_with_self(
                        txn,
                        self.definition,
                        key,
                        value,
                        Some(subject),
                    )
                    .await
                }) as tc_ir::HandlerFuture<'a, ()>
            }) as PutHandler<'a, 'txn, State>
        })
    }

    fn post<'txn>(self: Box<Self>) -> Option<PostHandler<'a, 'txn, State>>
    where
        'txn: 'a,
    {
        matches!(self.definition, OpDef::Post(_)).then(|| {
            Box::new(move |txn, params| {
                Box::pin(async move {
                    let subject = self.subject();
                    crate::op_executor::execute_post_with_self(
                        txn,
                        self.definition,
                        params,
                        Some(subject),
                    )
                    .await
                }) as tc_ir::HandlerFuture<'a, State>
            }) as PostHandler<'a, 'txn, State>
        })
    }

    fn delete<'txn>(self: Box<Self>) -> Option<DeleteHandler<'a, 'txn, State>>
    where
        'txn: 'a,
    {
        matches!(self.definition, OpDef::Delete(_)).then(|| {
            Box::new(move |txn, key| {
                Box::pin(async move {
                    let subject = self.subject();
                    crate::op_executor::execute_delete_with_self(
                        txn,
                        self.definition,
                        key,
                        Some(subject),
                    )
                    .await
                }) as tc_ir::HandlerFuture<'a, ()>
            }) as DeleteHandler<'a, 'txn, State>
        })
    }
}

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

fn application_requirements<'a>(
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
    let member = member(members, path)?.clone();
    Some(match member {
        Scalar::Op(definition) => Box::new(OpHandler {
            subject: identity.clone(),
            definition,
        }),
        Scalar::Ref(reference) => match *reference {
            tc_ir::TCRef::Op(op) => Box::new(RefHandler(op)),
            reference => Box::new(ValueHandler(State::from(Scalar::from(reference)))),
        },
        value => Box::new(ValueHandler(State::from_scalar(value))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_nested_literal_members_without_a_second_directory_type() {
        let mut nested = Map::new();
        nested.insert(
            "answer".parse().unwrap(),
            Scalar::from(tc_value::Value::from(42_u64)),
        );
        let mut members = Map::new();
        members.insert("nested".parse().unwrap(), Scalar::Map(nested));
        let analysis = compile_ir_library(Scalar::Map(members)).unwrap();
        let path = ["nested".parse().unwrap(), "answer".parse().unwrap()];
        assert!(member(&analysis.members, &path).is_some());
    }
}
