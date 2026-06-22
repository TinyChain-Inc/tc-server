use tc_state::{Tensor, TensorReduceResult};

use super::tensor_dtype::{TensorDtypeGuard, TensorOpError};

pub(crate) fn broadcast_reduce_sum(
    input: &Tensor,
    target_shape: &[usize],
) -> Result<Tensor, TensorOpError> {
    TensorDtypeGuard::validate(input)?;

    let axes = broadcast_reduce_axes(input.shape(), target_shape)?;
    let reduced = if axes.is_empty() {
        input.clone()
    } else {
        match input.reduce_axes("sum", Some(axes), true).map_err(|_| {
            TensorOpError::InvalidBroadcastReduce {
                input: input.shape().to_vec(),
                target: target_shape.to_vec(),
            }
        })? {
            TensorReduceResult::Tensor(tensor) => tensor,
            TensorReduceResult::Scalar(_) => {
                return Err(TensorOpError::InvalidBroadcastReduce {
                    input: input.shape().to_vec(),
                    target: target_shape.to_vec(),
                });
            }
        }
    };

    reduced
        .reshape(target_shape.to_vec())
        .map_err(|_| TensorOpError::InvalidBroadcastReduce {
            input: input.shape().to_vec(),
            target: target_shape.to_vec(),
        })
}

fn broadcast_reduce_axes(
    input_shape: &[usize],
    target_shape: &[usize],
) -> Result<Vec<usize>, TensorOpError> {
    if target_shape.len() > input_shape.len() {
        return Err(TensorOpError::InvalidBroadcastReduce {
            input: input_shape.to_vec(),
            target: target_shape.to_vec(),
        });
    }

    let leading = input_shape.len() - target_shape.len();
    let mut axes: Vec<usize> = (0..leading).collect();

    for (target_axis, target_dim) in target_shape.iter().copied().enumerate() {
        let input_axis = leading + target_axis;
        let input_dim = input_shape[input_axis];

        if input_dim == target_dim {
            continue;
        }

        if target_dim == 1 && input_dim > 1 {
            axes.push(input_axis);
            continue;
        }

        return Err(TensorOpError::InvalidBroadcastReduce {
            input: input_shape.to_vec(),
            target: target_shape.to_vec(),
        });
    }

    Ok(axes)
}
