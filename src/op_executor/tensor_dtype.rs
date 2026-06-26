use std::fmt;

use tc_error::{TCError, TCResult};
use tc_state::Tensor;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TensorOpError {
    DtypeNotSupported {
        dtype: String,
    },
    DtypeMismatch {
        left: String,
        right: String,
    },
    ShapeMismatch {
        left: Vec<usize>,
        right: Vec<usize>,
    },
    BroadcastIncompatible {
        left: Vec<usize>,
        right: Vec<usize>,
    },
    InvalidBroadcastReduce {
        input: Vec<usize>,
        target: Vec<usize>,
    },
    InvalidRank {
        rank: usize,
    },
    MatmulShapeMismatch {
        a_inner: usize,
        b_inner: usize,
    },
    BroadcastShapeMismatch {
        batch_a: Vec<usize>,
        batch_b: Vec<usize>,
    },
    InvalidPermutation {
        rank: usize,
        permutation: Vec<usize>,
        reason: String,
    },
}

impl TensorOpError {
    pub(crate) fn into_tc_error(self) -> TCError {
        TCError::bad_request(self.to_string())
    }
}

impl fmt::Display for TensorOpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DtypeNotSupported { dtype } => {
                write!(
                    f,
                    "TensorOpError::DtypeNotSupported: dtype {dtype} is not supported"
                )
            }
            Self::DtypeMismatch { left, right } => {
                write!(
                    f,
                    "TensorOpError::DtypeMismatch: left dtype {left} != right dtype {right}"
                )
            }
            Self::ShapeMismatch { left, right } => {
                write!(
                    f,
                    "TensorOpError::ShapeMismatch: left shape {left:?} != right shape {right:?}"
                )
            }
            Self::BroadcastIncompatible { left, right } => {
                write!(
                    f,
                    "TensorOpError::BroadcastIncompatible: shapes {left:?} and {right:?} are not broadcast-compatible"
                )
            }
            Self::InvalidBroadcastReduce { input, target } => {
                write!(
                    f,
                    "TensorOpError::InvalidBroadcastReduce: input shape {input:?} cannot reduce to target shape {target:?}"
                )
            }
            Self::InvalidRank { rank } => {
                write!(
                    f,
                    "TensorOpError::InvalidRank: matmul requires rank >= 2 but got rank {rank}"
                )
            }
            Self::MatmulShapeMismatch { a_inner, b_inner } => {
                write!(
                    f,
                    "TensorOpError::MatmulShapeMismatch: A inner dim {a_inner} != B inner dim {b_inner}"
                )
            }
            Self::BroadcastShapeMismatch { batch_a, batch_b } => {
                write!(
                    f,
                    "TensorOpError::BroadcastShapeMismatch: batch dims {batch_a:?} and {batch_b:?} are not broadcast-compatible"
                )
            }
            Self::InvalidPermutation {
                rank,
                permutation,
                reason,
            } => {
                write!(
                    f,
                    "TensorOpError::InvalidPermutation: permutation {permutation:?} is invalid for rank {rank}: {reason}"
                )
            }
        }
    }
}

pub(crate) struct TensorDtypeGuard;

impl TensorDtypeGuard {
    pub(crate) fn validate(tensor: &Tensor) -> Result<(), TensorOpError> {
        match tensor.dtype_tag() {
            "f32" | "f64" => Ok(()),
            dtype => Err(TensorOpError::DtypeNotSupported {
                dtype: dtype.to_string(),
            }),
        }
    }

    pub(crate) fn validate_pair(left: &Tensor, right: &Tensor) -> Result<(), TensorOpError> {
        Self::validate(left)?;
        Self::validate(right)?;

        let left_dtype = left.dtype_tag();
        let right_dtype = right.dtype_tag();
        if left_dtype != right_dtype {
            return Err(TensorOpError::DtypeMismatch {
                left: left_dtype.to_string(),
                right: right_dtype.to_string(),
            });
        }

        Ok(())
    }
}

pub(crate) fn tensor_op_result<T>(result: Result<T, TensorOpError>) -> TCResult<T> {
    result.map_err(TensorOpError::into_tc_error)
}
