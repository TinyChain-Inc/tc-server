use destream::{EncodeMap, de, en};
use pathlink::Link;
use safecast::TryCastFrom;
use tc_error::{TCError, TCResult};
use tc_ir::Scalar;

#[derive(Clone, Debug)]
pub(crate) struct Definition(pub(crate) Link, pub(crate) Scalar);

impl de::FromStream for Definition {
    type Context = ();

    async fn from_stream<D: de::Decoder>(_context: (), decoder: &mut D) -> Result<Self, D::Error> {
        struct Visitor;

        impl de::Visitor for Visitor {
            type Value = Definition;

            fn expecting() -> &'static str {
                "one application URI mapped to its definition"
            }

            async fn visit_map<A: de::MapAccess>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let identity = map
                    .next_key::<String>(())
                    .await?
                    .ok_or_else(|| de::Error::custom("empty application definition"))?
                    .parse()
                    .map_err(de::Error::custom)?;
                let definition = map.next_value(()).await?;
                if map.next_key::<de::IgnoredAny>(()).await?.is_some() {
                    return Err(de::Error::custom(
                        "an application definition must contain exactly one URI",
                    ));
                }
                Ok(Definition(identity, definition))
            }
        }

        decoder.decode_map(Visitor).await
    }
}

impl<'en> en::IntoStream<'en> for Definition {
    fn into_stream<E: en::Encoder<'en>>(self, encoder: E) -> Result<E::Ok, E::Error> {
        let mut map = encoder.encode_map(Some(1))?;
        map.encode_entry(self.0.to_string(), self.1)?;
        map.end()
    }
}

#[cfg(feature = "http-client")]
pub(crate) const MAX_DEFINITION_BYTES: usize = 1024 * 1024;

pub(crate) fn into_put(key: Scalar, value: crate::State) -> TCResult<(Link, Scalar)> {
    let Scalar::Value(tc_value::Value::Link(identity)) = key else {
        return Err(TCError::bad_request(
            "application installation requires its identity as the PUT key",
        ));
    };
    let value = Scalar::try_cast_from(value, |state| {
        TCError::bad_request(format!("expected a scalar value, found {state:?}"))
    })?;
    Ok((identity, value))
}

#[cfg(test)]
#[path = "../tests/support/literal.rs"]
mod tests;
