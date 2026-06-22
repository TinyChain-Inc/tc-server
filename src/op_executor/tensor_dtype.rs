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
