use super::params::{required_scalar_param, scalar_param};
use super::*;
use crate::op_executor::broadcast_reduce::broadcast_reduce_sum;
use crate::op_executor::tensor_add::broadcast_add;
use crate::op_executor::tensor_dtype::tensor_op_result;
use crate::op_executor::tensor_matmul::batched_matmul;
use crate::op_executor::tensor_transpose::tensor_transpose;
use number_general::{FloatType, UIntType};
use pathlink::PathBuf;
use tc_state::{AxisRange, Range, Tensor, TensorReduceResult};
use tc_value::{NumberType, number_type_from_path, number_type_path};

pub(super) async fn resolve_tensor_get(
    id_ref: &tc_ir::IdRef,
    segments: &[PathSegment],
    key: Scalar,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<Option<State>> {
    let Some(tensor) = tensor_for_id(id_ref, values, txn, self_link).await? else {
        return Ok(None);
    };

    if segments.is_empty() {
        let bounds = scalar_to_state(key, values, txn, self_link).await?;
        let range = tensor_range_from_state(bounds, tensor.shape())?;
        let sliced = tensor.slice(range).map_err(TCError::bad_request)?;
        return Ok(Some(State::Collection(Collection::Tensor(sliced))));
    }

    if segments.len() != 1 {
        return Ok(None);
    }

    let state = match segments[0].as_str() {
        "broadcast" => {
            let shape = scalar_to_state(key, values, txn, self_link).await?;
            let shape = shape_from_state(shape)?;
            tensor.broadcast(shape).map_err(TCError::bad_request)?
        }
        "cast" => {
            let dtype_state = scalar_to_state(key, values, txn, self_link).await?;
            let dtype = tensor_dtype_from_state(dtype_state)?;
            tensor.cast(dtype).map_err(TCError::bad_request)?
        }
        "expand_dims" => {
            let axes = scalar_to_state(key, values, txn, self_link).await?;
            if let Some(axes) = optional_shape_from_state(axes)? {
                tensor
                    .expand_dims(Some(axes))
                    .map_err(TCError::bad_request)?
            } else {
                tensor.expand_dims(None).map_err(TCError::bad_request)?
            }
        }
        "reshape" => {
            let shape = scalar_to_state(key, values, txn, self_link).await?;
            let shape = shape_from_state(shape)?;
            tensor.reshape(shape).map_err(TCError::bad_request)?
        }
        "transpose" => {
            let permutation = scalar_to_state(key, values, txn, self_link).await?;
            let permutation = shape_from_state(permutation)?;
            tensor_op_result(tensor_transpose(&tensor, &permutation))?
        }
        _ => return Ok(None),
    };

    Ok(Some(State::Collection(Collection::Tensor(state))))
}

pub(super) async fn resolve_tensor_post(
    id_ref: &tc_ir::IdRef,
    segments: &[PathSegment],
    params: &Map<Scalar>,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<Option<State>> {
    if segments.len() != 1 {
        return Ok(None);
    }

    let Some(tensor) = tensor_for_id(id_ref, values, txn, self_link).await? else {
        return Ok(None);
    };

    let state = match segments[0].as_str() {
        "dtype" => State::Scalar(Scalar::Value(Value::String(
            number_type_path(&tensor.number_type()).to_string(),
        ))),
        "ndim" => State::from(Value::Number(Number::from(tensor.shape().len() as u64))),
        "shape" => State::Scalar(Scalar::Tuple(
            tensor
                .shape()
                .iter()
                .map(|dim| Scalar::Value(Value::Number(Number::from(*dim as u64))))
                .collect(),
        )),
        "size" => State::from(Value::Number(Number::from(tensor.size() as u64))),
        "all" => tensor_truthy_state(&tensor, true)?,
        "any" => tensor_truthy_state(&tensor, false)?,
        "cond" => {
            let then_tensor = tensor_param(params, "then", values, txn, self_link).await?;
            let else_tensor = tensor_param(params, "or_else", values, txn, self_link).await?;
            State::Collection(Collection::Tensor(
                Tensor::cond(&tensor, &then_tensor, &else_tensor).map_err(TCError::bad_request)?,
            ))
        }
        "max" | "min" | "mean" | "norm" | "product" | "std" | "sum" => {
            let axes = tensor_optional_axes_param(params, values, txn, self_link).await?;
            let keepdims = tensor_keepdims_param(params, values, txn, self_link).await?;

            match tensor
                .reduce_axes(segments[0].as_str(), axes, keepdims)
                .map_err(TCError::bad_request)?
            {
                TensorReduceResult::Scalar(number) => State::from(Value::Number(number)),
                TensorReduceResult::Tensor(tensor) => State::Collection(Collection::Tensor(tensor)),
            }
        }
        "broadcast_reduce" => {
            let target_shape =
                tensor_shape_param(params, "target_shape", values, txn, self_link).await?;
            State::Collection(Collection::Tensor(tensor_op_result(broadcast_reduce_sum(
                &tensor,
                &target_shape,
            ))?))
        }
        "matmul" => {
            let right = tensor_param(params, "r", values, txn, self_link).await?;
            State::Collection(Collection::Tensor(tensor_op_result(batched_matmul(
                &tensor, &right,
            ))?))
        }
        "transpose" => {
            let permutation = tensor_shape_param(params, "perm", values, txn, self_link).await?;
            State::Collection(Collection::Tensor(tensor_op_result(tensor_transpose(
                &tensor,
                &permutation,
            ))?))
        }
        "add" => {
            let right = tensor_param(params, "r", values, txn, self_link).await?;
            State::Collection(Collection::Tensor(tensor_op_result(broadcast_add(
                &tensor, &right,
            ))?))
        }
        "sub" | "mul" | "div" | "and" | "or" | "xor" => {
            let right = tensor_param(params, "r", values, txn, self_link).await?;
            State::Collection(Collection::Tensor(
                tensor
                    .binary_op(&right, segments[0].as_str())
                    .map_err(TCError::bad_request)?,
            ))
        }
        "not" => State::Collection(Collection::Tensor(
            tensor.unary_not().map_err(TCError::bad_request)?,
        )),
        _ => unreachable!("unsupported tensor post op segment"),
    };

    Ok(Some(state))
}

pub(super) async fn tensor_for_id(
    id_ref: &tc_ir::IdRef,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<Option<Tensor>> {
    let Some(state) = values.get(id_ref.as_str()) else {
        return Err(TCError::not_found(format!(
            "unknown id ${}",
            id_ref.as_str()
        )));
    };

    match state {
        State::Collection(Collection::Tensor(tensor)) => Ok(Some(tensor.clone())),
        State::Scalar(scalar) => {
            if let Some(tensor) = tensor_from_scalar_literal(scalar)? {
                return Ok(Some(tensor));
            }

            let resolved = resolve_scalar(scalar.clone(), values, txn, self_link).await?;
            match resolved {
                State::Collection(Collection::Tensor(tensor)) => Ok(Some(tensor)),
                _ => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

async fn tensor_param(
    params: &Map<Scalar>,
    name: &str,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<Tensor> {
    let value = required_scalar_param(params, name, "tensor parameter")?;

    if let Some(tensor) = tensor_from_scalar_literal(value)? {
        return Ok(tensor);
    }

    match scalar_to_state(value.clone(), values, txn, self_link).await? {
        State::Collection(Collection::Tensor(tensor)) => Ok(tensor),
        State::Scalar(scalar) => {
            if let Some(tensor) = tensor_from_scalar_literal(&scalar)? {
                Ok(tensor)
            } else {
                Err(TCError::bad_request(format!(
                    "expected tensor parameter {name} but found scalar {scalar:?}"
                )))
            }
        }
        other => Err(TCError::bad_request(format!(
            "expected tensor parameter {name} but found {other:?}"
        ))),
    }
}

async fn tensor_optional_axes_param(
    params: &Map<Scalar>,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<Option<Vec<usize>>> {
    let value = if let Some(value) = scalar_param(params, "axes", "tensor axes")? {
        value
    } else if let Some(value) = scalar_param(params, "axis", "tensor axis")? {
        value
    } else {
        return Ok(None);
    };

    let state = scalar_to_state(value.clone(), values, txn, self_link).await?;
    optional_axes_from_state(state)
}

async fn tensor_keepdims_param(
    params: &Map<Scalar>,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<bool> {
    let Some(value) = scalar_param(params, "keepdims", "tensor keepdims")? else {
        return Ok(false);
    };

    let state = scalar_to_state(value.clone(), values, txn, self_link).await?;
    bool_from_state(state, "tensor keepdims")
}

async fn tensor_shape_param(
    params: &Map<Scalar>,
    name: &str,
    values: &Arc<HashMap<Id, State>>,
    txn: &crate::txn::TxnHandle,
    self_link: Option<&Link>,
) -> TCResult<Vec<usize>> {
    let value = required_scalar_param(params, name, "tensor shape")?;
    let state = scalar_to_state(value.clone(), values, txn, self_link).await?;
    shape_from_state(state)
}

fn tensor_from_scalar_literal(scalar: &Scalar) -> TCResult<Option<Tensor>> {
    let Scalar::Ref(r) = scalar else {
        return Ok(None);
    };

    let TCRef::Op(OpRef::Put((subject, key, value))) = r.as_ref() else {
        return Ok(None);
    };

    let Subject::Link(link) = subject else {
        return Ok(None);
    };

    let subject_path = link.to_string();
    let tensor_path = TensorType.path().to_string();
    if subject_path != tensor_path {
        return Ok(None);
    }

    parse_tensor_literal_put(key.clone(), value.clone()).map(Some)
}

pub(super) fn parse_tensor_literal_put(key: Scalar, value: Scalar) -> TCResult<Tensor> {
    let Scalar::Tuple(key_parts) = key else {
        return Err(TCError::bad_request(
            "tensor literal key must be a tuple [dtype, shape]".to_string(),
        ));
    };

    if key_parts.len() != 2 {
        return Err(TCError::bad_request(
            "tensor literal key must have exactly two entries [dtype, shape]".to_string(),
        ));
    }

    let dtype = tensor_number_type_from_scalar(key_parts[0].clone(), "tensor literal dtype")?;
    let shape = scalar_to_shape(key_parts[1].clone())?;
    let values = scalar_to_numbers(value)?;

    match dtype {
        NumberType::Float(FloatType::F32) => {
            Tensor::dense_f32(shape, values.into_iter().map(|n| n.cast_into()).collect())
                .map_err(TCError::bad_request)
        }
        NumberType::Float(FloatType::F64) => {
            Tensor::dense_f64(shape, values.into_iter().map(|n| n.cast_into()).collect())
                .map_err(TCError::bad_request)
        }
        NumberType::UInt(UIntType::U64) => {
            Tensor::dense_u64(shape, values.into_iter().map(|n| n.cast_into()).collect())
                .map_err(TCError::bad_request)
        }
        other => Err(TCError::bad_request(format!(
            "unsupported tensor literal dtype {other}"
        ))),
    }
}

fn tensor_number_type_from_scalar(scalar: Scalar, context: &str) -> TCResult<NumberType> {
    match scalar {
        Scalar::Value(Value::String(raw)) => parse_tensor_number_type(&raw).ok_or_else(|| {
            TCError::bad_request(format!(
                "expected {context} to be a supported tensor dtype but found {raw:?}"
            ))
        }),
        Scalar::Value(Value::Link(link)) => {
            let raw = link.to_string();
            parse_tensor_number_type(&raw).ok_or_else(|| {
                TCError::bad_request(format!(
                    "expected {context} to be a supported tensor dtype but found {raw:?}"
                ))
            })
        }
        other => Err(TCError::bad_request(format!(
            "expected {context} to be a string or link but found {other:?}"
        ))),
    }
}

fn scalar_to_shape(scalar: Scalar) -> TCResult<Vec<usize>> {
    scalar_number_tuple(scalar, "tensor literal shape")?
        .into_iter()
        .map(|number| number_to_usize(number, "tensor shape dimension"))
        .collect()
}

fn scalar_to_numbers(scalar: Scalar) -> TCResult<Vec<Number>> {
    scalar_number_tuple(scalar, "tensor literal values")
}

fn shape_from_state(state: State) -> TCResult<Vec<usize>> {
    let items = tuple_state_to_items(state, "tensor shape")?;
    items
        .into_iter()
        .map(|item| match item {
            State::Scalar(Scalar::Value(Value::Number(number))) => {
                number_to_usize(number, "tensor shape dimension")
            }
            other => Err(TCError::bad_request(format!(
                "expected tensor shape dimension to be a number but found {other:?}"
            ))),
        })
        .collect()
}

fn scalar_tuple_items(scalar: Scalar, context: &str) -> TCResult<Vec<Scalar>> {
    match scalar {
        Scalar::Tuple(items) => Ok(items),
        other => Err(TCError::bad_request(format!(
            "expected {context} to be a tuple but found {other:?}"
        ))),
    }
}

fn scalar_number_tuple(scalar: Scalar, context: &str) -> TCResult<Vec<Number>> {
    let items = scalar_tuple_items(scalar, context)?;
    items
        .into_iter()
        .map(|item| match item {
            Scalar::Value(Value::Number(number)) => Ok(number),
            other => Err(TCError::bad_request(format!(
                "expected {context} element to be a number but found {other:?}"
            ))),
        })
        .collect()
}

fn optional_shape_from_state(state: State) -> TCResult<Option<Vec<usize>>> {
    if matches!(
        state,
        State::None | State::Scalar(Scalar::Value(Value::None))
    ) {
        Ok(None)
    } else {
        shape_from_state(state).map(Some)
    }
}

fn optional_axes_from_state(state: State) -> TCResult<Option<Vec<usize>>> {
    match state {
        State::None | State::Scalar(Scalar::Value(Value::None)) => Ok(None),
        State::Scalar(Scalar::Value(Value::Number(number))) => Ok(Some(vec![number_to_usize(
            number,
            "tensor reduction axis",
        )?])),
        other => shape_from_state(other).map(Some),
    }
}

fn tensor_dtype_from_state(state: State) -> TCResult<NumberType> {
    match state {
        State::Scalar(Scalar::Value(Value::String(dtype))) => parse_tensor_number_type(&dtype)
            .ok_or_else(|| TCError::bad_request(format!("unsupported tensor cast dtype {dtype}"))),
        State::Scalar(Scalar::Value(Value::Link(link))) => {
            let raw = link.to_string();
            parse_tensor_number_type(&raw)
                .ok_or_else(|| TCError::bad_request(format!("unsupported tensor cast dtype {raw}")))
        }
        other => Err(TCError::bad_request(format!(
            "expected tensor cast dtype to be a string or link but found {other:?}"
        ))),
    }
}

fn parse_tensor_number_type(raw: &str) -> Option<NumberType> {
    if let Some(dtype) = parse_tensor_number_type_tag(raw) {
        return Some(dtype);
    }

    let path = raw.parse::<PathBuf>().ok()?;
    number_type_from_path(&path)
}

fn parse_tensor_number_type_tag(raw: &str) -> Option<NumberType> {
    match raw {
        "f32" => Some(NumberType::Float(FloatType::F32)),
        "f64" => Some(NumberType::Float(FloatType::F64)),
        "u64" => Some(NumberType::UInt(UIntType::U64)),
        _ => None,
    }
}

fn tensor_range_from_state(bounds_state: State, shape: &[usize]) -> TCResult<Range> {
    let bounds = tuple_state_to_items(bounds_state, "tensor slice")?;
    if bounds.len() != shape.len() {
        return Err(TCError::bad_request(format!(
            "tensor slice bounds rank {} does not match tensor rank {}",
            bounds.len(),
            shape.len()
        )));
    }

    let mut range = Range::with_capacity(bounds.len());
    for (axis, (bound, axis_dim)) in bounds.into_iter().zip(shape.iter().copied()).enumerate() {
        range.push(tensor_axis_range_from_state(bound, axis, axis_dim)?);
    }

    Ok(range)
}

fn tensor_axis_range_from_state(bound: State, axis: usize, axis_dim: usize) -> TCResult<AxisRange> {
    match bound {
        State::Scalar(Scalar::Value(Value::Number(number))) => {
            let index = number_to_usize(number, &format!("tensor slice index at axis {axis}"))?;
            if index >= axis_dim {
                return Err(TCError::bad_request(format!(
                    "tensor slice index {index} is out of bounds for axis {axis} with dim {axis_dim}"
                )));
            }

            Ok(AxisRange::At(index))
        }
        State::Tuple(parts) => tensor_axis_range_from_parts(parts, axis, axis_dim),
        State::Scalar(Scalar::Tuple(parts)) => {
            let states = parts.into_iter().map(State::Scalar).collect();
            tensor_axis_range_from_parts(states, axis, axis_dim)
        }
        other => Err(TCError::bad_request(format!(
            "expected tensor slice bound at axis {axis} to be a number or tuple range but found {other:?}"
        ))),
    }
}

fn tensor_axis_range_from_parts(
    parts: Vec<State>,
    axis: usize,
    axis_dim: usize,
) -> TCResult<AxisRange> {
    if parts.is_empty() || parts.len() > 3 {
        return Err(TCError::bad_request(format!(
            "tensor slice range at axis {axis} must have 1 to 3 components"
        )));
    }

    let start = state_to_usize(
        parts[0].clone(),
        &format!("tensor slice start at axis {axis}"),
    )?;
    let stop = if parts.len() >= 2 {
        state_to_usize(
            parts[1].clone(),
            &format!("tensor slice stop at axis {axis}"),
        )?
    } else {
        axis_dim
    };
    let step = if parts.len() >= 3 {
        state_to_usize(
            parts[2].clone(),
            &format!("tensor slice step at axis {axis}"),
        )?
    } else {
        1
    };

    if step == 0 {
        return Err(TCError::bad_request(format!(
            "tensor slice step at axis {axis} must be positive"
        )));
    }
    if start > stop || stop > axis_dim {
        return Err(TCError::bad_request(format!(
            "tensor slice range [{start}, {stop}) is out of bounds for axis {axis} with dim {axis_dim}"
        )));
    }

    Ok(AxisRange::In(start, stop, step))
}

fn state_to_usize(state: State, context: &str) -> TCResult<usize> {
    match state {
        State::Scalar(Scalar::Value(Value::Number(number))) => number_to_usize(number, context),
        other => Err(TCError::bad_request(format!(
            "expected {context} to be a number but found {other:?}"
        ))),
    }
}

pub(super) fn bool_from_state(state: State, context: &str) -> TCResult<bool> {
    match state {
        State::Scalar(Scalar::Value(Value::Number(number))) => Ok(number.cast_into()),
        other => Err(TCError::bad_request(format!(
            "expected {context} to be a boolean but found {other:?}"
        ))),
    }
}

fn number_to_usize(number: Number, context: &str) -> TCResult<usize> {
    let signed: i64 = number.cast_into();
    if signed < 0 {
        return Err(TCError::bad_request(format!(
            "expected {context} to be non-negative"
        )));
    }

    Ok(signed as usize)
}

pub(super) fn tensor_truthy_state(tensor: &Tensor, require_all: bool) -> TCResult<State> {
    let values = tensor.values_f64().map_err(TCError::bad_request)?;
    let truthy = if require_all {
        values.iter().all(|v| *v != 0.0)
    } else {
        values.iter().any(|v| *v != 0.0)
    };

    Ok(State::from(Value::Number(Number::Bool(truthy.into()))))
}
