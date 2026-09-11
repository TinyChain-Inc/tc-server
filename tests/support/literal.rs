use super::*;
use bytes::Bytes;
use futures::TryStreamExt;
use std::io;
use tc_value::Value;

#[tokio::test]
async fn definition_is_exactly_one_entry() {
    let identity: Link = "/service/example/catalog/1.0.0".parse().unwrap();
    let definition = Scalar::from(Value::String("catalog".into()));
    let encoded = destream_json::encode(Definition(identity.clone(), definition.clone()))
        .unwrap()
        .try_fold(Vec::new(), |mut bytes, chunk| async move {
            bytes.extend_from_slice(&chunk);
            Ok(bytes)
        })
        .await
        .unwrap();
    let Definition(decoded_identity, decoded_definition) = destream_json::try_decode(
        (),
        futures::stream::iter([Ok::<_, io::Error>(encoded.into())]),
    )
    .await
    .unwrap();
    assert_eq!(
        (decoded_identity, decoded_definition),
        (identity, definition)
    );
}

#[tokio::test]
async fn definition_rejects_empty_and_multiple_entries() {
    for invalid in [
        br#"{}"#.as_slice(),
        br#"{"/lib/example/a/1.0.0":{},"/lib/example/b/1.0.0":{}}"#.as_slice(),
    ] {
        let error = destream_json::try_decode::<_, _, Definition>(
            (),
            futures::stream::iter([Ok::<_, io::Error>(Bytes::copy_from_slice(invalid))]),
        )
        .await
        .unwrap_err();
        assert!(!error.to_string().is_empty());
    }
}
