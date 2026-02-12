use std::io;
use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;
use futures::{FutureExt, TryStreamExt, stream};
use number_general::Number;
use tc_ir::{Id, Map, Scalar};
use tc_state::State;
use tc_value::Value;
use url::form_urlencoded;

use crate::reflect;
use crate::{Body, KernelHandler, Request, Response, StatusCode, header};

pub fn state_handler() -> Arc<dyn KernelHandler> {
    Arc::new(|req: Request| async move { dispatch(req).await }.boxed())
}

async fn dispatch(req: Request) -> Response {
    if reflect::is_reflect_path(req.uri().path()) {
        return reflect::reflect_handler().call(req).await;
    }
    if req.uri().path() == "/state/scalar/value/number/add" {
        return match *req.method() {
            hyper::Method::POST => number_add(req).await,
            hyper::Method::GET => number_add_get(req).await,
            _ => method_not_allowed(),
        };
    }
    if req.uri().path() == "/state/scalar/value/number/gt" {
        return match *req.method() {
            hyper::Method::POST => number_gt(req).await,
            hyper::Method::GET => number_gt_get(req).await,
            _ => method_not_allowed(),
        };
    }

    not_found()
}

async fn number_add(req: Request) -> Response {
    let query = req.uri().query().map(str::to_string);
    let body_bytes = match hyper::body::to_bytes(req.into_body()).await {
        Ok(bytes) => bytes,
        Err(_) => return internal_error_response("failed to read request body"),
    };

    if body_bytes.is_empty() || body_bytes.iter().all(|b| b.is_ascii_whitespace()) {
        return bad_request_response("missing request body");
    }

    let stream = stream::iter(vec![Ok::<Bytes, std::io::Error>(body_bytes.clone())]);
    let params: Result<Map<Scalar>, _> = destream_json::try_decode((), stream).await;

    let l_key = match Id::from_str("l") {
        Ok(key) => key,
        Err(err) => return internal_error_response(&format!("invalid l key id: {err}")),
    };
    let r_key = match Id::from_str("r") {
        Ok(key) => key,
        Err(err) => return internal_error_response(&format!("invalid r key id: {err}")),
    };

    let mut left: Option<Number> = None;
    let mut right: Option<Number> = None;

    if let Ok(params) = params {
        if let Some(value) = params.get(&l_key) {
            left = match value {
                Scalar::Value(Value::Number(number)) => Some(*number),
                _ => return bad_request_response("expected l to be a number"),
            };
        }
        if let Some(value) = params.get(&r_key) {
            right = match value {
                Scalar::Value(Value::Number(number)) => Some(*number),
                _ => return bad_request_response("expected r to be a number"),
            };
        }
    } else {
        let stream = stream::iter(vec![Ok::<Bytes, std::io::Error>(body_bytes)]);
        let items: Result<Vec<Scalar>, _> = destream_json::try_decode((), stream).await;
        match items {
            Ok(items) => match items.as_slice() {
                [l, r] => {
                    left = match l {
                        Scalar::Value(Value::Number(number)) => Some(*number),
                        _ => return bad_request_response("expected l to be a number"),
                    };
                    right = match r {
                        Scalar::Value(Value::Number(number)) => Some(*number),
                        _ => return bad_request_response("expected r to be a number"),
                    };
                }
                _ => return bad_request_response("invalid add params (expected [l, r])"),
            },
            Err(err) => {
                return bad_request_response(&format!("invalid request body: {err}"));
            }
        }
    }

    if left.is_none() || right.is_none() {
        let query = query.as_deref().unwrap_or("");
        let mut l_query: Option<Number> = None;
        let mut r_query: Option<Number> = None;
        for (k, v) in form_urlencoded::parse(query.as_bytes()) {
            if k.eq_ignore_ascii_case("l") {
                l_query = parse_number_param(&v).ok();
            } else if k.eq_ignore_ascii_case("r") {
                r_query = parse_number_param(&v).ok();
            }
        }
        if left.is_none() {
            left = l_query;
        }
        if right.is_none() {
            right = r_query;
        }
    }

    let Some(left) = left else {
        return bad_request_response("missing l parameter");
    };
    let Some(right) = right else {
        return bad_request_response("missing r parameter");
    };

    let state = State::from(Value::Number(left + right));
    state_response(state)
}

async fn number_add_get(req: Request) -> Response {
    let query = req.uri().query().unwrap_or("");
    let mut left: Option<Number> = None;
    let mut right: Option<Number> = None;

    for (k, v) in form_urlencoded::parse(query.as_bytes()) {
        if k.eq_ignore_ascii_case("l") {
            left = parse_number_param(&v).ok();
        } else if k.eq_ignore_ascii_case("r") {
            right = parse_number_param(&v).ok();
        }
    }

    let Some(left) = left else {
        return bad_request_response("missing l parameter");
    };
    let Some(right) = right else {
        return bad_request_response("missing r parameter");
    };

    let state = State::from(Value::Number(left + right));
    state_response(state)
}

async fn number_gt(req: Request) -> Response {
    let query = req.uri().query().map(str::to_string);
    let body_bytes = match hyper::body::to_bytes(req.into_body()).await {
        Ok(bytes) => bytes,
        Err(_) => return internal_error_response("failed to read request body"),
    };

    if body_bytes.is_empty() || body_bytes.iter().all(|b| b.is_ascii_whitespace()) {
        return bad_request_response("missing request body");
    }

    let stream = stream::iter(vec![Ok::<Bytes, std::io::Error>(body_bytes.clone())]);
    let params: Result<Map<Scalar>, _> = destream_json::try_decode((), stream).await;

    let l_key = match Id::from_str("l") {
        Ok(key) => key,
        Err(err) => return internal_error_response(&format!("invalid l key id: {err}")),
    };
    let r_key = match Id::from_str("r") {
        Ok(key) => key,
        Err(err) => return internal_error_response(&format!("invalid r key id: {err}")),
    };

    let mut left: Option<Number> = None;
    let mut right: Option<Number> = None;

    if let Ok(params) = params {
        if let Some(value) = params.get(&l_key) {
            left = match value {
                Scalar::Value(Value::Number(number)) => Some(*number),
                _ => return bad_request_response("expected l to be a number"),
            };
        }
        if let Some(value) = params.get(&r_key) {
            right = match value {
                Scalar::Value(Value::Number(number)) => Some(*number),
                _ => return bad_request_response("expected r to be a number"),
            };
        }
    } else {
        let stream = stream::iter(vec![Ok::<Bytes, std::io::Error>(body_bytes)]);
        let items: Result<Vec<Scalar>, _> = destream_json::try_decode((), stream).await;
        match items {
            Ok(items) => match items.as_slice() {
                [l, r] => {
                    left = match l {
                        Scalar::Value(Value::Number(number)) => Some(*number),
                        _ => return bad_request_response("expected l to be a number"),
                    };
                    right = match r {
                        Scalar::Value(Value::Number(number)) => Some(*number),
                        _ => return bad_request_response("expected r to be a number"),
                    };
                }
                _ => return bad_request_response("invalid gt params (expected [l, r])"),
            },
            Err(err) => {
                return bad_request_response(&format!("invalid request body: {err}"));
            }
        }
    }

    if left.is_none() || right.is_none() {
        let query = query.as_deref().unwrap_or("");
        let mut l_query: Option<Number> = None;
        let mut r_query: Option<Number> = None;
        for (k, v) in form_urlencoded::parse(query.as_bytes()) {
            if k.eq_ignore_ascii_case("l") {
                l_query = parse_number_param(&v).ok();
            } else if k.eq_ignore_ascii_case("r") {
                r_query = parse_number_param(&v).ok();
            }
        }
        if left.is_none() {
            left = l_query;
        }
        if right.is_none() {
            right = r_query;
        }
    }

    let Some(left) = left else {
        return bad_request_response("missing l parameter");
    };
    let Some(right) = right else {
        return bad_request_response("missing r parameter");
    };

    let state = State::from(Value::Number(Number::from(left > right)));
    state_response(state)
}

async fn number_gt_get(req: Request) -> Response {
    let query = req.uri().query().unwrap_or("");
    let mut left: Option<Number> = None;
    let mut right: Option<Number> = None;

    for (k, v) in form_urlencoded::parse(query.as_bytes()) {
        if k.eq_ignore_ascii_case("l") {
            left = parse_number_param(&v).ok();
        } else if k.eq_ignore_ascii_case("r") {
            right = parse_number_param(&v).ok();
        }
    }

    let Some(left) = left else {
        return bad_request_response("missing l parameter");
    };
    let Some(right) = right else {
        return bad_request_response("missing r parameter");
    };

    let state = State::from(Value::Number(Number::from(left > right)));
    state_response(state)
}

#[allow(clippy::result_large_err)]
fn parse_number_param(raw: &str) -> Result<Number, Response> {
    let stream = stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::copy_from_slice(
        raw.as_bytes(),
    ))]);
    let value: Value = match futures::executor::block_on(destream_json::try_decode((), stream)) {
        Ok(value) => value,
        Err(_) => return Err(bad_request_response("invalid number parameter")),
    };
    match value {
        Value::Number(number) => Ok(number),
        _ => Err(bad_request_response("expected number parameter")),
    }
}

fn method_not_allowed() -> Response {
    http::Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .body(Body::empty())
        .expect("method not allowed response")
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
