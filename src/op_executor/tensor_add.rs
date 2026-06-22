use tc_state::Tensor;

use super::tensor_dtype::{TensorDtypeGuard, TensorOpError};

pub(crate) fn exact_shape_add(left: &Tensor, right: &Tensor) -> Result<Tensor, TensorOpError> {
    TensorDtypeGuard::validate_pair(left, right)?;

    if left.shape() != right.shape() {
        return Err(TensorOpError::ShapeMismatch {
            left: left.shape().to_vec(),
            right: right.shape().to_vec(),
        });
    }

    left.binary_op(right, "add")
        .map_err(|_| TensorOpError::ShapeMismatch {
            left: left.shape().to_vec(),
            right: right.shape().to_vec(),
        })
}

pub(crate) fn broadcast_add(left: &Tensor, right: &Tensor) -> Result<Tensor, TensorOpError> {
    TensorDtypeGuard::validate_pair(left, right)?;

    if left.shape() == right.shape() {
        return exact_shape_add(left, right);
    }

    let output_shape = broadcast_shape(left.shape(), right.shape())?;
    let left = left.clone().broadcast(output_shape.clone()).map_err(|_| {
        TensorOpError::BroadcastIncompatible {
            left: left.shape().to_vec(),
            right: right.shape().to_vec(),
        }
    })?;
    let right = right.clone().broadcast(output_shape).map_err(|_| {
        TensorOpError::BroadcastIncompatible {
            left: left.shape().to_vec(),
            right: right.shape().to_vec(),
        }
    })?;

    exact_shape_add(&left, &right)
}

fn broadcast_shape(left: &[usize], right: &[usize]) -> Result<Vec<usize>, TensorOpError> {
    let rank = left.len().max(right.len());
    let mut output = Vec::with_capacity(rank);

    for axis_from_right in 0..rank {
        let left_dim = dim_from_right(left, axis_from_right).unwrap_or(1);
        let right_dim = dim_from_right(right, axis_from_right).unwrap_or(1);

        let dim = if left_dim == right_dim {
            left_dim
        } else if left_dim == 1 {
            right_dim
        } else if right_dim == 1 {
            left_dim
        } else {
            return Err(TensorOpError::BroadcastIncompatible {
                left: left.to_vec(),
                right: right.to_vec(),
            });
        };

        output.push(dim);
    }

    output.reverse();
    Ok(output)
}

fn dim_from_right(shape: &[usize], axis_from_right: usize) -> Option<usize> {
    shape
        .len()
        .checked_sub(axis_from_right + 1)
        .map(|axis| shape[axis])
}
