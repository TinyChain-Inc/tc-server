use pathlink::Link;
use safecast::TryCastFrom;
use std::str::FromStr;
use tc_ir::{Id, Map, NativeClass, OpDef, OpRef, Scalar, TCRef};
use tc_value::Value;

enum ReflectPath {
    ScalarClass,
    ScalarRefParts,
    OpDefForm,
    OpDefLastId,
    OpDefScalars,
}

fn reflect_path(path: &str) -> Option<ReflectPath> {
    let normalized = path.trim_start_matches('/');
    if normalized == "state/scalar/reflect/class" {
        return Some(ReflectPath::ScalarClass);
    }
    if normalized == "state/scalar/reflect/ref_parts" {
        return Some(ReflectPath::ScalarRefParts);
    }

    if normalized == "state/scalar/op/reflect/form" {
        return Some(ReflectPath::OpDefForm);
    }

    if normalized == "state/scalar/op/reflect/last_id" {
        return Some(ReflectPath::OpDefLastId);
    }

    if normalized == "state/scalar/op/reflect/scalars" {
        return Some(ReflectPath::OpDefScalars);
    }

    None
}

pub fn is_reflect_path(path: &str) -> bool {
    reflect_path(path).is_some()
}

/// Execute reflection over a decoded scalar request. Reflection is native graph behavior, not an
/// HTTP endpoint; HTTP only decodes the request and projects this result.
pub async fn execute(request: crate::KernelRequest) -> tc_error::TCResult<crate::State> {
    if request.method != crate::Method::Post {
        return Err(tc_error::TCError::not_found(request.path.to_string()));
    }

    let path = request.path.to_string();
    let body = request.body.unwrap_or(crate::State::None);
    let Scalar::Map(params) = Scalar::try_cast_from(body, |_| {
        tc_error::TCError::bad_request("reflection requires scalar map parameters")
    })?
    else {
        return Err(tc_error::TCError::bad_request(
            "reflection requires map parameters",
        ));
    };
    let scalar = extract_scalar_native(&params)?;

    let result = match reflect_path(&path) {
        Some(ReflectPath::ScalarClass) => {
            let class = match scalar {
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
            Scalar::Value(Value::Link(class))
        }
        Some(ReflectPath::ScalarRefParts) => match scalar {
            Scalar::Ref(r) => match r.as_ref() {
                TCRef::Cond(cond) => Scalar::Tuple(vec![
                    Scalar::from(cond.cond.clone()),
                    cond.then.clone(),
                    cond.or_else.clone(),
                ]),
                TCRef::After(after) => Scalar::Tuple(vec![after.when.clone(), after.then.clone()]),
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
            },
            _ => Scalar::Tuple(vec![]),
        },
        Some(ReflectPath::OpDefForm) => {
            let opdef = opdef_from_scalar_native(scalar)?;
            Scalar::Tuple(
                opdef
                    .form()
                    .iter()
                    .map(|(id, scalar)| {
                        Scalar::Tuple(vec![
                            Scalar::Value(Value::String(id.to_string())),
                            scalar.clone(),
                        ])
                    })
                    .collect(),
            )
        }
        Some(ReflectPath::OpDefLastId) => {
            let opdef = opdef_from_scalar_native(scalar)?;
            Scalar::Value(
                opdef
                    .last_id()
                    .map(|id| Value::String(id.to_string()))
                    .unwrap_or(Value::None),
            )
        }
        Some(ReflectPath::OpDefScalars) => {
            let opdef = opdef_from_scalar_native(scalar)?;
            Scalar::Tuple(opdef.walk_scalars().cloned().collect())
        }
        None => return Err(tc_error::TCError::not_found(path)),
    };

    Ok(crate::State::from_scalar(result))
}

fn extract_scalar_native(params: &Map<Scalar>) -> tc_error::TCResult<&Scalar> {
    let scalar_key = Id::from_str("scalar").expect("static parameter id");
    let op_key = Id::from_str("op").expect("static parameter id");
    params
        .get(&scalar_key)
        .or_else(|| params.get(&op_key))
        .ok_or_else(|| tc_error::TCError::bad_request("missing scalar parameter"))
}

fn opdef_from_scalar_native(scalar: &Scalar) -> tc_error::TCResult<OpDef> {
    match scalar {
        Scalar::Op(opdef) => Ok(opdef.clone()),
        _ => Err(tc_error::TCError::bad_request(
            "expected OpDef scalar parameter",
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
        TCRef::Cond(_) => pathlink::PathBuf::from(tc_ir::TCREF_COND).to_string(),
        TCRef::After(_) => pathlink::PathBuf::from(tc_ir::TCREF_AFTER).to_string(),
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
