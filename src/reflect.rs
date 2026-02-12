use std::io;
use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;
use futures::{FutureExt, TryStreamExt, stream};

use crate::{Body, KernelHandler, Request, Response, StatusCode, header};
use pathlink::Link;
use tc_ir::{Id, Map, NativeClass, OpDef, OpRef, Scalar, TCRef};
use tc_state::State;
use tc_value::Value;

enum ReflectPath {
    ScalarClass,
    ScalarRefParts,
    OpDefForm,
    OpDefLastId,
    OpDefScalars,
}

fn reflect_path(path: &str) -> Option<ReflectPath> {
    let normalized = path.trim_start_matches('/');
    if normalized == "state/scalar/reflect/class" {
        return Some(ReflectPath::ScalarClass);
    }
    if normalized == "state/scalar/reflect/ref_parts" {
        return Some(ReflectPath::ScalarRefParts);
    }

    if normalized == "state/scalar/op/reflect/form" {
        return Some(ReflectPath::OpDefForm);
    }

    if normalized == "state/scalar/op/reflect/last_id" {
        return Some(ReflectPath::OpDefLastId);
    }

    if normalized == "state/scalar/op/reflect/scalars" {
        return Some(ReflectPath::OpDefScalars);
    }

    None
}

pub fn is_reflect_path(path: &str) -> bool {
    reflect_path(path).is_some()
}

pub fn reflect_handler() -> Arc<dyn KernelHandler> {
    Arc::new(|req: Request| async move { dispatch(req).await }.boxed())
}

pub fn host_reflect_handler(fallback: Arc<dyn KernelHandler>) -> impl KernelHandler {
    let reflect = reflect_handler();
    move |req: Request| {
        let reflect = reflect.clone();
        let fallback = fallback.clone();
        async move {
            if req.method() == hyper::Method::POST && reflect_path(req.uri().path()).is_some() {
                return reflect.call(req).await;
            }
            fallback.call(req).await
        }
        .boxed()
    }
}

async fn dispatch(req: Request) -> Response {
    if req.method() != hyper::Method::POST {
        return not_found();
    }

    match reflect_path(req.uri().path()) {
        Some(ReflectPath::ScalarClass) => return scalar_class(req).await,
        Some(ReflectPath::ScalarRefParts) => return scalar_ref_parts(req).await,
        Some(ReflectPath::OpDefForm) => return opdef_form(req).await,
        Some(ReflectPath::OpDefLastId) => return opdef_last_id(req).await,
        Some(ReflectPath::OpDefScalars) => return opdef_scalars(req).await,
        None => {}
    }

    not_found()
}

async fn decode_params(req: Request) -> Result<Map<Scalar>, Response> {
    let body_bytes = hyper::body::to_bytes(req.into_body())
        .await
        .map_err(|_| internal_error_response("failed to read request body"))?;

    if body_bytes.is_empty() || body_bytes.iter().all(|b| b.is_ascii_whitespace()) {
        return Err(bad_request_response("missing request body"));
    }

    let stream = stream::iter(vec![Ok::<Bytes, std::io::Error>(body_bytes)]);
    let params: Map<Scalar> = destream_json::try_decode((), stream)
        .await
        .map_err(|err| bad_request_response(&format!("invalid request body: {err}")))?;

    Ok(params)
}

#[allow(clippy::result_large_err)]
fn extract_scalar(params: &Map<Scalar>) -> Result<&Scalar, Response> {
    let scalar_key = Id::from_str("scalar")
        .map_err(|err| internal_error_response(&format!("invalid scalar key id: {err}")))?;
    let op_key = Id::from_str("op")
        .map_err(|err| internal_error_response(&format!("invalid op key id: {err}")))?;

    params
        .get(&scalar_key)
        .or_else(|| params.get(&op_key))
        .ok_or_else(|| bad_request_response("missing scalar parameter"))
}

async fn opdef_from_scalar(scalar: &Scalar) -> Result<OpDef, Response> {
    match scalar {
        Scalar::Op(opdef) => Ok(opdef.clone()),
        Scalar::Value(Value::String(raw)) => {
            let stream = stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::copy_from_slice(
                raw.as_bytes(),
            ))]);
            destream_json::try_decode((), stream)
                .await
                .map_err(|err| bad_request_response(&format!("invalid OpDef JSON string: {err}")))
        }
        _ => Err(bad_request_response("expected OpDef scalar parameter")),
    }
}

async fn scalar_class(req: Request) -> Response {
    let params = match decode_params(req).await {
        Ok(params) => params,
        Err(resp) => return resp,
    };

    let scalar = match extract_scalar(&params) {
        Ok(scalar) => scalar,
        Err(resp) => return resp,
    };

    let class = match scalar {
        Scalar::Value(value) => class_from_value(value),
        Scalar::Op(opdef) => class_from_opdef(opdef),
        Scalar::Ref(r) => class_from_tcref(r.as_ref()),
        Scalar::Map(_) => Link::from_str(&pathlink::PathBuf::from(tc_ir::SCALAR_MAP).to_string())
            .expect("scalar map class"),
        Scalar::Tuple(_) => {
            Link::from_str(&pathlink::PathBuf::from(tc_ir::SCALAR_TUPLE).to_string())
                .expect("scalar tuple class")
        }
    };

    let state = State::from(Value::Link(class));
    state_response(state)
}

fn class_from_value(value: &Value) -> Link {
    let path = value.class().path().to_string();
    Link::from_str(&path).expect("value class link")
}

fn class_from_opdef(opdef: &OpDef) -> Link {
    let path = match opdef {
        OpDef::Get(_) => pathlink::PathBuf::from(tc_ir::OPDEF_GET).to_string(),
        OpDef::Put(_) => pathlink::PathBuf::from(tc_ir::OPDEF_PUT).to_string(),
        OpDef::Post(_) => pathlink::PathBuf::from(tc_ir::OPDEF_POST).to_string(),
        OpDef::Delete(_) => pathlink::PathBuf::from(tc_ir::OPDEF_DELETE).to_string(),
    };
    Link::from_str(&path).expect("opdef class link")
}

fn class_from_tcref(tc_ref: &TCRef) -> Link {
    let path = match tc_ref {
        TCRef::If(_) => pathlink::PathBuf::from(tc_ir::TCREF_IF).to_string(),
        TCRef::Cond(_) => pathlink::PathBuf::from(tc_ir::TCREF_COND).to_string(),
        TCRef::While(_) => pathlink::PathBuf::from(tc_ir::TCREF_WHILE).to_string(),
        TCRef::ForEach(_) => pathlink::PathBuf::from(tc_ir::TCREF_FOR_EACH).to_string(),
        TCRef::Id(_) => pathlink::PathBuf::from(tc_ir::SCALAR_REF_PREFIX).to_string(),
        TCRef::Op(opref) => match opref {
            OpRef::Get(_) => pathlink::PathBuf::from(tc_ir::OPREF_GET).to_string(),
            OpRef::Put(_) => pathlink::PathBuf::from(tc_ir::OPREF_PUT).to_string(),
            OpRef::Post(_) => pathlink::PathBuf::from(tc_ir::OPREF_POST).to_string(),
            OpRef::Delete(_) => pathlink::PathBuf::from(tc_ir::OPREF_DELETE).to_string(),
        },
    };
    Link::from_str(&path).expect("tcref class link")
}

async fn scalar_ref_parts(req: Request) -> Response {
    let params = match decode_params(req).await {
        Ok(params) => params,
        Err(resp) => return resp,
    };

    let scalar = match extract_scalar(&params) {
        Ok(scalar) => scalar,
        Err(resp) => return resp,
    };

    let Scalar::Ref(r) = scalar else {
        return state_response(State::Scalar(Scalar::Tuple(vec![])));
    };

    let parts = match r.as_ref() {
        TCRef::If(if_ref) => Scalar::Tuple(vec![
            Scalar::from(if_ref.cond.clone()),
            if_ref.then.clone(),
            if_ref.or_else.clone(),
        ]),
        TCRef::Cond(cond_op) => Scalar::Tuple(vec![
            Scalar::from(cond_op.cond.clone()),
            Scalar::Op(cond_op.then.clone()),
            Scalar::Op(cond_op.or_else.clone()),
        ]),
        TCRef::While(while_ref) => Scalar::Tuple(vec![
            while_ref.cond.clone(),
            while_ref.closure.clone(),
            while_ref.state.clone(),
        ]),
        TCRef::ForEach(for_each) => Scalar::Tuple(vec![
            for_each.items.clone(),
            for_each.op.clone(),
            Scalar::Value(Value::String(for_each.item_name.to_string())),
        ]),
        _ => Scalar::Tuple(vec![]),
    };

    state_response(State::Scalar(parts))
}

async fn opdef_form(req: Request) -> Response {
    let params = match decode_params(req).await {
        Ok(params) => params,
        Err(resp) => return resp,
    };

    let scalar = match extract_scalar(&params) {
        Ok(scalar) => scalar,
        Err(resp) => return resp,
    };

    let opdef = match opdef_from_scalar(scalar).await {
        Ok(opdef) => opdef,
        Err(resp) => return resp,
    };

    let form = opdef
        .form()
        .iter()
        .map(|(id, scalar)| {
            Scalar::Tuple(vec![
                Scalar::Value(Value::String(id.to_string())),
                scalar.clone(),
            ])
        })
        .collect();

    state_response(State::Scalar(Scalar::Tuple(form)))
}

async fn opdef_last_id(req: Request) -> Response {
    let params = match decode_params(req).await {
        Ok(params) => params,
        Err(resp) => return resp,
    };

    let scalar = match extract_scalar(&params) {
        Ok(scalar) => scalar,
        Err(resp) => return resp,
    };

    let opdef = match opdef_from_scalar(scalar).await {
        Ok(opdef) => opdef,
        Err(resp) => return resp,
    };

    let value = match opdef.last_id() {
        Some(id) => Value::String(id.to_string()),
        None => Value::None,
    };

    state_response(State::Scalar(Scalar::Value(value)))
}

async fn opdef_scalars(req: Request) -> Response {
    let params = match decode_params(req).await {
        Ok(params) => params,
        Err(resp) => return resp,
    };

    let scalar = match extract_scalar(&params) {
        Ok(scalar) => scalar,
        Err(resp) => return resp,
    };

    let opdef = match opdef_from_scalar(scalar).await {
        Ok(opdef) => opdef,
        Err(resp) => return resp,
    };

    let scalars = opdef.walk_scalars().cloned().collect();
    state_response(State::Scalar(Scalar::Tuple(scalars)))
}

fn state_response(state: State) -> Response {
    match destream_json::encode(state) {
        Ok(stream) => http::Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::wrap_stream(
                stream.map_err(|err| io::Error::other(err.to_string())),
            ))
            .expect("state response"),
        Err(err) => internal_error_response(&err.to_string()),
    }
}

fn not_found() -> Response {
    http::Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::empty())
        .expect("not found response")
}

fn bad_request_response(msg: &str) -> Response {
    http::Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from(msg.to_string()))
        .expect("bad request response")
}

fn internal_error_response(msg: &str) -> Response {
    http::Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from(msg.to_string()))
        .expect("internal error response")
}
