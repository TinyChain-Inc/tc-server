use tc_state::Tensor;

use super::tensor_dtype::{TensorDtypeGuard, TensorOpError};

pub(crate) fn batched_matmul(left: &Tensor, right: &Tensor) -> Result<Tensor, TensorOpError> {
    TensorDtypeGuard::validate_pair(left, right)?;

    let a_shape = left.shape();
    let b_shape = right.shape();

    if a_shape.len() < 2 {
        return Err(TensorOpError::InvalidRank {
            rank: a_shape.len(),
        });
    }
    if b_shape.len() < 2 {
        return Err(TensorOpError::InvalidRank {
            rank: b_shape.len(),
        });
    }

    let a_rows = a_shape[a_shape.len() - 2];
    let a_inner = a_shape[a_shape.len() - 1];
    let b_inner = b_shape[b_shape.len() - 2];
    let b_cols = b_shape[b_shape.len() - 1];

    if a_inner != b_inner {
        return Err(TensorOpError::MatmulShapeMismatch { a_inner, b_inner });
    }

    let a_batch = &a_shape[..a_shape.len() - 2];
    let b_batch = &b_shape[..b_shape.len() - 2];

    let batch_out = broadcast_batch_shape(a_batch, b_batch)?;
    let batch_size: usize = if batch_out.is_empty() {
        1
    } else {
        batch_out.iter().copied().product()
    };

    let a_vals = left
        .values_f64()
        .map_err(|_| TensorOpError::DtypeNotSupported {
            dtype: left.dtype_tag().to_string(),
        })?;
    let b_vals = right
        .values_f64()
        .map_err(|_| TensorOpError::DtypeNotSupported {
            dtype: right.dtype_tag().to_string(),
        })?;

    let mut out = vec![0.0f64; batch_size * a_rows * b_cols];

    for batch_idx in 0..batch_size {
        let a_batch_offset =
            broadcast_batch_offset(batch_idx, &batch_out, a_batch) * a_rows * a_inner;
        let b_batch_offset =
            broadcast_batch_offset(batch_idx, &batch_out, b_batch) * b_inner * b_cols;
        let out_batch_offset = batch_idx * a_rows * b_cols;

        for row in 0..a_rows {
            for col in 0..b_cols {
                let mut sum = 0.0f64;
                for k in 0..a_inner {
                    sum += a_vals[a_batch_offset + row * a_inner + k]
                        * b_vals[b_batch_offset + k * b_cols + col];
                }
                out[out_batch_offset + row * b_cols + col] = sum;
            }
        }
    }

    let mut result_shape = batch_out;
    result_shape.push(a_rows);
    result_shape.push(b_cols);

    match left.dtype_tag() {
        "f32" => {
            Tensor::dense_f32(result_shape, out.into_iter().map(|v| v as f32).collect())
                .map_err(|_| TensorOpError::MatmulShapeMismatch { a_inner, b_inner })
        }
        _ => Tensor::dense_f64(result_shape, out)
            .map_err(|_| TensorOpError::MatmulShapeMismatch { a_inner, b_inner }),
    }
}

fn broadcast_batch_shape(
    batch_a: &[usize],
    batch_b: &[usize],
) -> Result<Vec<usize>, TensorOpError> {
    let rank = batch_a.len().max(batch_b.len());
    let mut output = Vec::with_capacity(rank);

    for axis_from_right in 0..rank {
        let a_dim = dim_from_right(batch_a, axis_from_right).unwrap_or(1);
        let b_dim = dim_from_right(batch_b, axis_from_right).unwrap_or(1);

        let dim = if a_dim == b_dim {
            a_dim
        } else if a_dim == 1 {
            b_dim
        } else if b_dim == 1 {
            a_dim
        } else {
            return Err(TensorOpError::BroadcastShapeMismatch {
                batch_a: batch_a.to_vec(),
                batch_b: batch_b.to_vec(),
            });
        };

        output.push(dim);
    }

    output.reverse();
    Ok(output)
}

fn broadcast_batch_offset(flat_idx: usize, out_batch: &[usize], src_batch: &[usize]) -> usize {
    if src_batch.is_empty() {
        return 0;
    }

    let out_rank = out_batch.len();
    let src_rank = src_batch.len();

    // Decompose flat_idx into multi-dim indices in out_batch (row-major)
    let mut out_indices = vec![0usize; out_rank];
    let mut tmp = flat_idx;
    for i in (0..out_rank).rev() {
        out_indices[i] = tmp % out_batch[i];
        tmp /= out_batch[i];
    }

    // Map to src_batch flat index — src is right-aligned within out
    let mut src_flat = 0usize;
    let mut src_stride = 1usize;
    for i in (0..src_rank).rev() {
        let out_axis = out_rank - src_rank + i;
        let src_dim = src_batch[i];
        let src_idx = if src_dim == 1 { 0 } else { out_indices[out_axis] };
        src_flat += src_idx * src_stride;
        src_stride *= src_dim;
    }

    src_flat
}

fn dim_from_right(shape: &[usize], axis_from_right: usize) -> Option<usize> {
    shape
        .len()
        .checked_sub(axis_from_right + 1)
        .map(|axis| shape[axis])
}
