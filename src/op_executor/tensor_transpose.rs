use tc_state::Tensor;

use super::tensor_dtype::{TensorDtypeGuard, TensorOpError};

pub(crate) fn tensor_transpose(
    input: &Tensor,
    permutation: &[usize],
) -> Result<Tensor, TensorOpError> {
    TensorDtypeGuard::validate(input)?;
    let output_shape = transpose_output_shape(input.shape(), permutation)?;

    let result = input
        .clone()
        .transpose(Some(permutation.to_vec()))
        .map_err(|_| invalid_permutation(input.shape().len(), permutation, "execution failed"))?;

    debug_assert_eq!(result.shape(), output_shape.as_slice());
    Ok(result)
}

pub(crate) fn transpose_output_shape(
    input_shape: &[usize],
    permutation: &[usize],
) -> Result<Vec<usize>, TensorOpError> {
    validate_permutation(input_shape.len(), permutation)?;
    Ok(permutation.iter().map(|axis| input_shape[*axis]).collect())
}

fn validate_permutation(rank: usize, permutation: &[usize]) -> Result<(), TensorOpError> {
    if permutation.len() != rank {
        return Err(invalid_permutation(
            rank,
            permutation,
            "length must match input rank",
        ));
    }

    let mut seen = vec![false; rank];
    for axis in permutation {
        if *axis >= rank {
            return Err(invalid_permutation(rank, permutation, "axis out of range"));
        }
        if seen[*axis] {
            return Err(invalid_permutation(
                rank,
                permutation,
                "axis appears more than once",
            ));
        }
        seen[*axis] = true;
    }

    Ok(())
}

fn invalid_permutation(rank: usize, permutation: &[usize], reason: &str) -> TensorOpError {
    TensorOpError::InvalidPermutation {
        rank,
        permutation: permutation.to_vec(),
        reason: reason.to_string(),
    }
}
