use std::io;

use bytes::Bytes;
use destream::{EncodeMap, de, en};
#[cfg(any(feature = "http-client", feature = "wasm", test))]
use futures::TryStreamExt;
#[cfg(any(feature = "http-server", test))]
use futures::stream;
use pathlink::Link;
use safecast::TryCastFrom;
use tc_error::{TCError, TCResult};
use tc_ir::Scalar;

#[derive(Clone)]
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

#[cfg(any(feature = "http-client", test))]
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
pub(crate) async fn encode(
    identity: &Link,
    definition: &Scalar,
    bound: usize,
) -> TCResult<Vec<u8>> {
    encode_json(Definition(identity.clone(), definition.clone()), bound).await
}

#[cfg(any(feature = "http-server", test))]
pub(crate) async fn decode(bytes: &[u8], bound: usize) -> TCResult<(Link, Scalar)> {
    let Definition(identity, definition) = decode_json(bytes, bound).await?;
    Ok((identity, definition))
}

pub(crate) async fn decode_stream<S>(input: S) -> io::Result<(Link, Scalar)>
where
    S: futures::Stream<Item = io::Result<Bytes>> + Send + Unpin,
{
    let Definition(identity, definition) = destream_json::try_decode((), input)
        .await
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    Ok((identity, definition))
}

#[cfg(any(feature = "http-client", feature = "wasm", test))]
pub(crate) async fn encode_json<T>(value: T, bound: usize) -> TCResult<Vec<u8>>
where
    T: for<'en> en::IntoStream<'en>,
{
    destream_json::encode(value)
        .map_err(|err| TCError::bad_request(err.to_string()))?
        .map_err(|err| io::Error::other(err.to_string()))
        .try_fold(Vec::new(), |mut bytes, chunk| async move {
            if bytes.len().saturating_add(chunk.len()) > bound {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "encoded application value exceeds its bound",
                ));
            }
            bytes.extend_from_slice(&chunk);
            Ok(bytes)
        })
        .await
        .map_err(|err| TCError::bad_request(err.to_string()))
}

#[cfg(any(feature = "http-server", test))]
pub(crate) async fn decode_json<T>(bytes: &[u8], bound: usize) -> TCResult<T>
where
    T: de::FromStream<Context = ()>,
{
    if bytes.len() > bound {
        return Err(TCError::bad_request("application value exceeds its bound"));
    }
    let input = stream::iter([Ok::<_, io::Error>(Bytes::copy_from_slice(bytes))]);
    destream_json::try_decode((), input)
        .await
        .map_err(|err| TCError::bad_request(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tc_value::Value;

    #[tokio::test]
    async fn definition_is_exactly_one_entry() {
        let identity: Link = "/service/example/catalog/1.0.0".parse().unwrap();
        let definition = Scalar::from(Value::String("catalog".into()));
        let encoded = encode(&identity, &definition, MAX_DEFINITION_BYTES)
            .await
            .unwrap();
        assert_eq!(
            decode(&encoded, MAX_DEFINITION_BYTES).await.unwrap(),
            (identity, definition)
        );
    }

    #[tokio::test]
    async fn definition_rejects_empty_and_multiple_entries() {
        for invalid in [
            br#"{}"#.as_slice(),
            br#"{"/lib/example/a/1.0.0":{},"/lib/example/b/1.0.0":{}}"#.as_slice(),
        ] {
            let error = decode(invalid, MAX_DEFINITION_BYTES).await.unwrap_err();
            assert_eq!(error.code(), tc_error::ErrorKind::BadRequest);
        }
    }
}
