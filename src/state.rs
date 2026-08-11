use number_general::Number;
use safecast::TryCastFrom;
use std::str::FromStr;
use tc_ir::{Id, Map, Scalar};
use tc_value::Value;

/// Execute the small built-in state route surface after an adapter has decoded its body.
pub async fn execute(request: crate::KernelRequest) -> tc_error::TCResult<Option<crate::State>> {
    let path = request.path.to_string();
    if crate::reflect::is_reflect_path(&path) {
        return crate::reflect::execute(request).await.map(Some);
    }

    let body = request.body.unwrap_or(crate::State::None);
    let scalar = Scalar::try_cast_from(body, |_| {
        tc_error::TCError::bad_request("expected a scalar state request")
    })?;
    let params = match scalar {
        Scalar::Map(params) => params,
        Scalar::Tuple(items) if items.len() == 2 => [
            (Id::from_str("l").expect("static id"), items[0].clone()),
            (Id::from_str("r").expect("static id"), items[1].clone()),
        ]
        .into_iter()
        .collect(),
        _ => return Ok(None),
    };

    let left = number_param(&params, "l")?;
    let right = number_param(&params, "r")?;
    let value = match (path.as_str(), request.method) {
        ("/state/scalar/value/number/add", crate::Method::Get | crate::Method::Post) => {
            Value::Number(left + right)
        }
        ("/state/scalar/value/number/gt", crate::Method::Get | crate::Method::Post) => {
            Value::Number(Number::from(left > right))
        }
        ("/state/scalar/value/number/add" | "/state/scalar/value/number/gt", _) => {
            return Err(tc_error::TCError::bad_request(
                "unsupported state route method",
            ));
        }
        _ => return Ok(None),
    };

    Ok(Some(crate::State::from(value)))
}

fn number_param(params: &Map<Scalar>, name: &str) -> tc_error::TCResult<Number> {
    let id = Id::from_str(name).expect("static parameter id");
    match params.get(&id) {
        Some(Scalar::Value(Value::Number(value))) => Ok(*value),
        Some(_) => Err(tc_error::TCError::bad_request(format!(
            "expected {name} to be a number"
        ))),
        None => Err(tc_error::TCError::bad_request(format!(
            "missing {name} parameter"
        ))),
    }
}
