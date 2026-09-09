use std::io;

use bytes::Bytes;
use destream::de;
use futures::stream;
use pathlink::{Link, PathSegment};
use tc_error::{TCError, TCResult};
use tc_ir::Scalar;

pub(super) struct WasmEntry {
    pub(super) identity: Link,
    pub(super) definition: Scalar,
    pub(super) routes: Vec<RouteBinding>,
}

#[derive(Debug)]
pub(crate) struct RouteBinding {
    pub(crate) path: Vec<PathSegment>,
    pub(crate) export: String,
}

impl de::FromStream for RouteBinding {
    type Context = ();

    async fn from_stream<D: de::Decoder>(
        _context: Self::Context,
        decoder: &mut D,
    ) -> Result<Self, D::Error> {
        struct BindingVisitor;
        impl de::Visitor for BindingVisitor {
            type Value = RouteBinding;

            fn expecting() -> &'static str {
                "a WASM route binding"
            }

            async fn visit_map<A: de::MapAccess>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut path = None;
                let mut export = None;
                while let Some(key) = map.next_key::<String>(()).await? {
                    match key.as_str() {
                        "path" => path = Some(map.next_value::<String>(()).await?),
                        "export" => export = Some(map.next_value(()).await?),
                        _ => return Err(de::Error::custom(format!("unknown route field {key}"))),
                    }
                }
                let path: Link = path
                    .ok_or_else(|| de::Error::custom("missing route path"))?
                    .parse()
                    .map_err(de::Error::custom)?;
                if path.host().is_some() || path.path().is_empty() {
                    return Err(de::Error::custom(
                        "a WASM route must be a local nonempty path",
                    ));
                }
                Ok(RouteBinding {
                    path: path.path().to_vec(),
                    export: export.ok_or_else(|| de::Error::custom("missing route export"))?,
                })
            }
        }
        decoder.decode_map(BindingVisitor).await
    }
}

impl de::FromStream for WasmEntry {
    type Context = ();

    async fn from_stream<D: de::Decoder>(
        _context: Self::Context,
        decoder: &mut D,
    ) -> Result<Self, D::Error> {
        struct EntryVisitor;
        impl de::Visitor for EntryVisitor {
            type Value = WasmEntry;

            fn expecting() -> &'static str {
                "a canonical WASM Library entry"
            }

            async fn visit_map<A: de::MapAccess>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut definition = None;
                let mut routes = None;
                while let Some(key) = map.next_key::<String>(()).await? {
                    match key.as_str() {
                        "definition" => {
                            let EmbeddedDefinition(identity, value) = map.next_value(()).await?;
                            definition = Some((identity, value));
                        }
                        "routes" => routes = Some(map.next_value(()).await?),
                        _ => return Err(de::Error::custom(format!("unknown WASM field {key}"))),
                    }
                }
                let (identity, definition) =
                    definition.ok_or_else(|| de::Error::custom("missing Library definition"))?;
                Ok(WasmEntry {
                    identity,
                    definition,
                    routes: routes.ok_or_else(|| de::Error::custom("missing routes"))?,
                })
            }
        }
        decoder.decode_map(EntryVisitor).await
    }
}

struct EmbeddedDefinition(Link, Scalar);

impl de::FromStream for EmbeddedDefinition {
    type Context = ();

    async fn from_stream<D: de::Decoder>(_context: (), decoder: &mut D) -> Result<Self, D::Error> {
        struct Visitor;
        impl de::Visitor for Visitor {
            type Value = EmbeddedDefinition;
            fn expecting() -> &'static str {
                "one Library URI mapped to its definition"
            }
            async fn visit_map<A: de::MapAccess>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let identity = map
                    .next_key::<String>(())
                    .await?
                    .ok_or_else(|| de::Error::custom("empty Library definition"))?
                    .parse()
                    .map_err(de::Error::custom)?;
                let definition = map.next_value(()).await?;
                if map.next_key::<de::IgnoredAny>(()).await?.is_some() {
                    return Err(de::Error::custom(
                        "a Library definition must contain exactly one URI",
                    ));
                }
                Ok(EmbeddedDefinition(identity, definition))
            }
        }
        decoder.decode_map(Visitor).await
    }
}

pub(super) async fn decode_entry(bytes: Vec<u8>) -> TCResult<WasmEntry> {
    let input = stream::iter([Ok::<_, io::Error>(Bytes::from(bytes))]);
    destream_json::try_decode((), input)
        .await
        .map_err(|err| TCError::bad_request(format!("invalid WASM entry: {err}")))
}

pub(super) fn format_path(path: &[PathSegment]) -> String {
    format!(
        "/{}",
        path.iter()
            .map(PathSegment::to_string)
            .collect::<Vec<_>>()
            .join("/")
    )
}
