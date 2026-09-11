use bytes::Bytes;
use futures::stream;
use tc_value::Value;
use url::form_urlencoded;

use super::response::bad_request_response;
use super::{Request, Response};

pub(crate) async fn public_key_actor(req: &Request) -> Result<String, Box<Response>> {
    let query = req.uri().query().unwrap_or("");
    let key = form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .find(|(key, _)| key.eq_ignore_ascii_case("key"))
        .map(|(_, value)| value)
        .ok_or_else(|| Box::new(bad_request_response("missing key query parameter")))?;

    match destream_json::try_decode(
        (),
        stream::iter([Ok::<Bytes, std::io::Error>(Bytes::from(key.into_bytes()))]),
    )
    .await
    {
        Ok(Value::String(actor_id)) => Ok(actor_id),
        _ => Err(Box::new(bad_request_response(
            "invalid key query parameter",
        ))),
    }
}
