use std::sync::Arc;

use aes_gcm_siv::aead::OsRng;
use aes_gcm_siv::aead::rand_core::RngCore;
use futures::FutureExt;
use hyper::body::to_bytes;
use hyper::{Body, Request, Response, StatusCode};

use crate::KernelHandler;
use crate::library::{LibraryRegistry, encode_install_payload_bytes};
use crate::txn::TxnHandle;

use super::crypto::{decode_encrypted_payload, encode_encrypted_payload, encrypt_token_with_key};
use super::{LIBRARY_EXPORT_PATH, ReplicationIssuer, TOKEN_PATH};

pub fn replication_token_handler(issuer: Arc<ReplicationIssuer>) -> impl KernelHandler {
    move |req: Request<Body>| {
        let issuer = issuer.clone();
        async move {
            if req.uri().path() != TOKEN_PATH {
                return Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("not found");
            }

            let body_bytes = match to_bytes(req.into_body()).await {
                Ok(bytes) => bytes,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(err.to_string()))
                        .expect("bad request");
                }
            };

            let (nonce, path_encrypted) = match decode_encrypted_payload(body_bytes) {
                Ok(tuple) => tuple,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(err.to_string()))
                        .expect("bad request");
                }
            };

            let (path, key) = match issuer.decrypt_path_with_key(&nonce, &path_encrypted) {
                Ok(result) => result,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(err.to_string()))
                        .expect("bad request");
                }
            };

            let signed = match issuer.issue_token(&path) {
                Ok(token) => token,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(err.to_string()))
                        .expect("internal error");
                }
            };

            let token = signed.into_jwt();

            let mut nonce = [0u8; 12];
            OsRng.fill_bytes(&mut nonce);
            let encrypted = match encrypt_token_with_key(token, &key, &nonce) {
                Ok(encrypted) => encrypted,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(err.to_string()))
                        .expect("internal error");
                }
            };

            let bytes = match encode_encrypted_payload(&nonce, &encrypted) {
                Ok(bytes) => bytes,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(err.to_string()))
                        .expect("internal error");
                }
            };

            Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(Body::from(bytes))
                .expect("token response")
        }
        .boxed()
    }
}

pub fn export_handler(registry: Arc<LibraryRegistry>) -> impl KernelHandler {
    move |req: Request<Body>| {
        let registry = Arc::clone(&registry);
        async move {
            if req.uri().path() != LIBRARY_EXPORT_PATH {
                return Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("not found");
            }

            let Some(txn) = req.extensions().get::<TxnHandle>() else {
                return Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body(Body::from("missing transaction context"))
                    .expect("unauthorized");
            };

            let payload = match registry.export_payload_for_claims(txn).await {
                Ok(Some(payload)) => payload,
                Ok(None) => {
                    return Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Body::empty())
                        .expect("not found");
                }
                Err(err) if err.code() == tc_error::ErrorKind::Unauthorized => {
                    return Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body(Body::from("unauthorized"))
                        .expect("unauthorized");
                }
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(err.to_string()))
                        .expect("internal error");
                }
            };

            let bytes = match encode_install_payload_bytes(&payload) {
                Ok(bytes) => bytes,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(err))
                        .expect("internal error");
                }
            };

            Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(Body::from(bytes))
                .expect("export response")
        }
        .boxed()
    }
}
