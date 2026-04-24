use std::sync::Arc;

use aes_gcm_siv::aead::OsRng;
use aes_gcm_siv::aead::rand_core::RngCore;
use futures::FutureExt;
use hyper::{Body, Request, StatusCode};

use crate::KernelHandler;
use crate::library::{LibraryRegistry, encode_install_payload_bytes};
use crate::txn::TxnHandle;

use super::crypto::{encode_encrypted_payload, encrypt_token_with_key};
use super::http_util::{
    decode_encrypted_request, empty_response, json_ok, method_not_allowed, text_response,
};
use super::{LIBRARY_EXPORT_PATH, ReplicationIssuer, TOKEN_PATH};

pub fn replication_token_handler(issuer: Arc<ReplicationIssuer>) -> impl KernelHandler {
    move |req: Request<Body>| {
        let issuer = issuer.clone();
        async move {
            if req.uri().path() != TOKEN_PATH {
                return empty_response(StatusCode::NOT_FOUND);
            }
            if req.method() != hyper::Method::POST {
                return method_not_allowed("POST");
            }

            let (path, key) = match decode_encrypted_request(req, &issuer).await {
                Ok(result) => result,
                Err(response) => return response,
            };

            let signed = match issuer.issue_token(&path) {
                Ok(token) => token,
                Err(err) => {
                    return text_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
                }
            };

            let token = signed.into_jwt();

            let mut nonce = [0u8; 12];
            OsRng.fill_bytes(&mut nonce);
            let encrypted = match encrypt_token_with_key(token, &key, &nonce) {
                Ok(encrypted) => encrypted,
                Err(err) => {
                    return text_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
                }
            };

            let bytes = match encode_encrypted_payload(&nonce, &encrypted) {
                Ok(bytes) => bytes,
                Err(err) => {
                    return text_response(StatusCode::INTERNAL_SERVER_ERROR, err.to_string());
                }
            };

            json_ok(bytes)
        }
        .boxed()
    }
}

pub fn export_handler(registry: Arc<LibraryRegistry>) -> impl KernelHandler {
    move |req: Request<Body>| {
        let registry = Arc::clone(&registry);
        async move {
            if req.uri().path() != LIBRARY_EXPORT_PATH {
                return empty_response(StatusCode::NOT_FOUND);
            }

            let Some(txn) = req.extensions().get::<TxnHandle>() else {
                return text_response(StatusCode::UNAUTHORIZED, "missing transaction context");
            };

            let payload = match registry.export_payload_for_claims(txn).await {
                Ok(Some(payload)) => payload,
                Ok(None) => return empty_response(StatusCode::NOT_FOUND),
                Err(err) if err.code() == tc_error::ErrorKind::Unauthorized => {
                    return text_response(StatusCode::UNAUTHORIZED, "unauthorized");
                }
                Err(err) => return text_response(StatusCode::INTERNAL_SERVER_ERROR, err),
            };

            let bytes = match encode_install_payload_bytes(&payload) {
                Ok(bytes) => bytes,
                Err(err) => return text_response(StatusCode::INTERNAL_SERVER_ERROR, err),
            };

            json_ok(bytes)
        }
        .boxed()
    }
}
