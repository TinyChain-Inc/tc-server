use crate::State;
use safecast::TryCastFrom;
use tc_error::{TCError, TCResult};
use tc_ir::OpDef;

use super::executor::Executor;

pub(crate) async fn execute(
    txn: &crate::txn::TxnHandle,
    op: OpDef,
    args: State,
    self_state: Option<State>,
) -> TCResult<State> {
    op.validate()?;
    let returns_value = matches!(&op, OpDef::Get(_) | OpDef::Post(_));
    let (data, form) = match op {
        OpDef::Get((key_name, form)) | OpDef::Delete((key_name, form)) => {
            let key = tc_ir::Scalar::try_cast_from(args, |_| {
                TCError::bad_request("GET and DELETE OpDefs expect a scalar key")
            })?;
            (vec![(key_name, State::from_scalar(key))], form)
        }
        OpDef::Put((key_name, value_name, form)) => {
            let State::Tuple(mut args) = args else {
                return Err(TCError::bad_request("PUT OpDef expects [key, value]"));
            };
            if args.len() != 2 {
                return Err(TCError::bad_request("PUT OpDef expects [key, value]"));
            }
            let value = args.pop().expect("PUT argument length checked");
            let key = tc_ir::Scalar::try_cast_from(
                args.pop().expect("PUT argument length checked"),
                |_| TCError::bad_request("PUT OpDef expects a scalar key"),
            )?;
            (
                vec![(key_name, State::from_scalar(key)), (value_name, value)],
                form,
            )
        }
        OpDef::Post(form) => {
            let State::Map(params) = args else {
                return Err(TCError::bad_request("POST OpDef expects a parameter map"));
            };
            (params.into_iter().collect(), form)
        }
    };

    let capture = form.last().expect("validated nonempty OpDef").0.clone();
    let result = Executor::new_with_self(txn, data, form, self_state)?
        .capture(capture)
        .await?;
    Ok(if returns_value {
        result
    } else {
        State::default()
    })
}
