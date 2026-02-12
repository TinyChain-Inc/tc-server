use std::collections::HashMap;
use std::str::FromStr;

use pathlink::Link;
use tc_error::{TCError, TCResult};
use tc_ir::{Id, Map, NativeClass, OpDef, OpRef, Scalar, TCRef};
use tc_state::State;
use tc_value::Value;

pub(super) fn reflect_link(
    link: &Link,
    params: &Map<Scalar>,
    values: &HashMap<Id, State>,
) -> TCResult<Option<State>> {
    let Some(kind) = reflect_kind(link) else {
        return Ok(None);
    };

    let scalar = extract_scalar_param(params)?;
    let scalar = resolve_reflect_param(scalar, values)?;

    let state = match kind {
        ReflectKind::ScalarClass => {
            let class = match &scalar {
                Scalar::Value(value) => class_from_value(value),
                Scalar::Op(opdef) => class_from_opdef(opdef),
                Scalar::Ref(r) => class_from_tcref(r.as_ref()),
                Scalar::Map(_) => {
                    Link::from_str(&pathlink::PathBuf::from(tc_ir::SCALAR_MAP).to_string())
                        .expect("scalar map class")
                }
                Scalar::Tuple(_) => {
                    Link::from_str(&pathlink::PathBuf::from(tc_ir::SCALAR_TUPLE).to_string())
                        .expect("scalar tuple class")
                }
            };
            State::from(Value::Link(class))
        }
        ReflectKind::ScalarRefParts => {
            let Scalar::Ref(r) = &scalar else {
                return Ok(Some(State::Scalar(Scalar::Tuple(vec![]))));
            };
            let parts = match r.as_ref() {
                TCRef::If(if_ref) => Scalar::Tuple(vec![
                    Scalar::from(if_ref.cond.clone()),
                    if_ref.then.clone(),
                    if_ref.or_else.clone(),
                ]),
                TCRef::Cond(cond_op) => Scalar::Tuple(vec![
                    Scalar::from(cond_op.cond.clone()),
                    Scalar::Op(cond_op.then.clone()),
                    Scalar::Op(cond_op.or_else.clone()),
                ]),
                TCRef::While(while_ref) => Scalar::Tuple(vec![
                    while_ref.cond.clone(),
                    while_ref.closure.clone(),
                    while_ref.state.clone(),
                ]),
                TCRef::ForEach(for_each) => Scalar::Tuple(vec![
                    for_each.items.clone(),
                    for_each.op.clone(),
                    Scalar::Value(Value::String(for_each.item_name.to_string())),
                ]),
                _ => Scalar::Tuple(vec![]),
            };
            State::Scalar(parts)
        }
        ReflectKind::OpDefForm => {
            let opdef = opdef_from_scalar_param(&scalar)?;
            let form = opdef
                .form()
                .iter()
                .map(|(id, scalar)| {
                    Scalar::Tuple(vec![
                        Scalar::Value(Value::String(id.to_string())),
                        scalar.clone(),
                    ])
                })
                .collect();
            State::Scalar(Scalar::Tuple(form))
        }
        ReflectKind::OpDefLastId => {
            let opdef = opdef_from_scalar_param(&scalar)?;
            let value = match opdef.last_id() {
                Some(id) => Value::String(id.to_string()),
                None => Value::None,
            };
            State::Scalar(Scalar::Value(value))
        }
        ReflectKind::OpDefScalars => {
            let Scalar::Op(opdef) = &scalar else {
                return Ok(Some(State::Scalar(Scalar::Tuple(vec![]))));
            };
            let scalars = opdef.walk_scalars().cloned().collect();
            State::Scalar(Scalar::Tuple(scalars))
        }
    };

    Ok(Some(state))
}

enum ReflectKind {
    ScalarClass,
    ScalarRefParts,
    OpDefForm,
    OpDefLastId,
    OpDefScalars,
}

fn reflect_kind(link: &Link) -> Option<ReflectKind> {
    let normalized = link.path().to_string();
    match normalized.trim_start_matches('/') {
        "state/scalar/reflect/class" => Some(ReflectKind::ScalarClass),
        "state/scalar/reflect/ref_parts" => Some(ReflectKind::ScalarRefParts),
        "state/scalar/op/reflect/form" => Some(ReflectKind::OpDefForm),
        "state/scalar/op/reflect/last_id" => Some(ReflectKind::OpDefLastId),
        "state/scalar/op/reflect/scalars" => Some(ReflectKind::OpDefScalars),
        _ => None,
    }
}

fn extract_scalar_param(params: &Map<Scalar>) -> TCResult<Scalar> {
    let scalar_id: Id = "scalar"
        .parse()
        .map_err(|err| TCError::internal(format!("invalid scalar id: {err}")))?;
    let op_id: Id = "op"
        .parse()
        .map_err(|err| TCError::internal(format!("invalid op id: {err}")))?;
    params
        .get(&scalar_id)
        .or_else(|| params.get(&op_id))
        .cloned()
        .ok_or_else(|| TCError::bad_request("missing scalar parameter".to_string()))
}

fn opdef_from_scalar_param(scalar: &Scalar) -> TCResult<OpDef> {
    match scalar {
        Scalar::Op(opdef) => Ok(opdef.clone()),
        _ => Err(TCError::bad_request(
            "expected OpDef scalar parameter".to_string(),
        )),
    }
}

fn class_from_value(value: &Value) -> Link {
    let path = value.class().path().to_string();
    Link::from_str(&path).expect("value class link")
}

fn class_from_opdef(opdef: &OpDef) -> Link {
    let path = match opdef {
        OpDef::Get(_) => pathlink::PathBuf::from(tc_ir::OPDEF_GET).to_string(),
        OpDef::Put(_) => pathlink::PathBuf::from(tc_ir::OPDEF_PUT).to_string(),
        OpDef::Post(_) => pathlink::PathBuf::from(tc_ir::OPDEF_POST).to_string(),
        OpDef::Delete(_) => pathlink::PathBuf::from(tc_ir::OPDEF_DELETE).to_string(),
    };
    Link::from_str(&path).expect("opdef class link")
}

fn class_from_tcref(tc_ref: &TCRef) -> Link {
    let path = match tc_ref {
        TCRef::If(_) => pathlink::PathBuf::from(tc_ir::TCREF_IF).to_string(),
        TCRef::Cond(_) => pathlink::PathBuf::from(tc_ir::TCREF_COND).to_string(),
        TCRef::While(_) => pathlink::PathBuf::from(tc_ir::TCREF_WHILE).to_string(),
        TCRef::ForEach(_) => pathlink::PathBuf::from(tc_ir::TCREF_FOR_EACH).to_string(),
        TCRef::Id(_) => pathlink::PathBuf::from(tc_ir::SCALAR_REF_PREFIX).to_string(),
        TCRef::Op(opref) => match opref {
            OpRef::Get(_) => pathlink::PathBuf::from(tc_ir::OPREF_GET).to_string(),
            OpRef::Put(_) => pathlink::PathBuf::from(tc_ir::OPREF_PUT).to_string(),
            OpRef::Post(_) => pathlink::PathBuf::from(tc_ir::OPREF_POST).to_string(),
            OpRef::Delete(_) => pathlink::PathBuf::from(tc_ir::OPREF_DELETE).to_string(),
        },
    };
    Link::from_str(&path).expect("tcref class link")
}

fn resolve_reflect_param(scalar: Scalar, values: &HashMap<Id, State>) -> TCResult<Scalar> {
    match &scalar {
        Scalar::Ref(r) => match r.as_ref() {
            TCRef::Id(id_ref) => values
                .get(id_ref.as_str())
                .cloned()
                .ok_or_else(|| TCError::not_found(format!("unknown id ${}", id_ref.as_str())))
                .and_then(state_to_scalar_for_reflect),
            _ => Ok(scalar),
        },
        _ => Ok(scalar),
    }
}

fn state_to_scalar_for_reflect(state: State) -> TCResult<Scalar> {
    match state {
        State::None => Ok(Scalar::Value(Value::None)),
        State::Scalar(scalar) => Ok(scalar),
        State::Map(map) => {
            let mut out = Map::new();
            for (id, value) in map {
                out.insert(id, state_to_scalar_for_reflect(value)?);
            }
            Ok(Scalar::Map(out))
        }
        State::Tuple(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(state_to_scalar_for_reflect(item)?);
            }
            Ok(Scalar::Tuple(out))
        }
        other => Err(TCError::bad_request(format!(
            "expected scalar state while resolving op; found {other:?}"
        ))),
    }
}
