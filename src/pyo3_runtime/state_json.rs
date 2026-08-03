use futures::{TryStreamExt, future::BoxFuture};
use tc_ir::NativeClass;
use tc_state::{BTreeCollection, BTreeType, Collection, State};
use tc_value::Value;

fn wrap_single_entry_map(path: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"{");
    out.extend_from_slice(path);
    out.extend_from_slice(b":[");
    out.extend_from_slice(payload);
    out.extend_from_slice(b"]}");
    out
}

async fn encode_btree_state_json(btree: BTreeCollection) -> Result<Vec<u8>, String> {
    let path_bytes = encode_json_bytes(BTreeType.path().to_string()).await?;
    let stream = btree.finalized_key_stream().await.map_err(|err| err.to_string())?;
    let payload = encode_btree_payload_json(btree.schema, stream).await?;

    Ok(wrap_single_entry_map(&path_bytes, &payload))
}

async fn encode_json_bytes<T>(value: T) -> Result<Vec<u8>, String>
where
    T: for<'en> destream::en::IntoStream<'en>,
{
    destream_json::encode(value)
        .map_err(|err| err.to_string())?
        .map_err(|err| err.to_string())
        .try_fold(Vec::new(), |mut acc, chunk| async move {
            acc.extend_from_slice(&chunk);
            Ok(acc)
        })
        .await
}

/// Encode a BTree payload in the canonical `[schema, rows]` JSON shape.
async fn encode_btree_payload_json(
    schema: Vec<tc_collection::btree::BTreeColumnSchema>,
    mut stream: b_tree::Keys<Value>,
) -> Result<Vec<u8>, String> {
    let key_arity = schema.len();
    if key_arity == 0 {
        return Err("BTree schema must have at least one column".to_string());
    }

    let mut out = Vec::new();
    out.extend_from_slice(b"[");

    let schema_bytes = encode_json_bytes(schema).await?;
    out.extend_from_slice(&schema_bytes);
    out.extend_from_slice(b",[");

    let mut first = true;
    while let Some(row) = stream.try_next().await.map_err(|err| err.to_string())? {
        if row.len() != key_arity {
            return Err(format!(
                "BTree row arity {} does not match schema arity {}",
                row.len(),
                key_arity
            ));
        }

        if !first {
            out.extend_from_slice(b",");
        }

        let row_bytes = if key_arity == 1 {
            encode_json_bytes(
                row.into_iter().next().expect("arity-1 row has one element"),
            )
            .await?
        } else {
            encode_json_bytes(Value::Tuple(row.to_vec())).await?
        };

        out.extend_from_slice(&row_bytes);
        first = false;
    }

    out.extend_from_slice(b"]]");
    Ok(out)
}

pub(super) fn encode_state_json_bytes(state: State) -> BoxFuture<'static, Result<Vec<u8>, String>> {
    Box::pin(async move {
        match state {
            State::Collection(Collection::BTree(btree)) => encode_btree_state_json(*btree).await,
            State::Map(map) => {
                let mut out = Vec::new();
                out.extend_from_slice(b"{");
                for (index, (key, value)) in map.into_iter().enumerate() {
                    if index > 0 {
                        out.extend_from_slice(b",");
                    }
                    out.extend_from_slice(&encode_json_bytes(key.to_string()).await?);
                    out.extend_from_slice(b":");
                    out.extend_from_slice(&encode_state_json_bytes(value).await?);
                }
                out.extend_from_slice(b"}");
                Ok(out)
            }
            State::Tuple(items) => {
                let mut out = Vec::new();
                out.extend_from_slice(b"[");
                for (index, item) in items.into_iter().enumerate() {
                    if index > 0 {
                        out.extend_from_slice(b",");
                    }
                    out.extend_from_slice(&encode_state_json_bytes(item).await?);
                }
                out.extend_from_slice(b"]");
                Ok(out)
            }
            other => encode_json_bytes(other).await,
        }
    })
}
