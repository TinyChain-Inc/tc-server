use aes_gcm_siv::aead::rand_core::RngCore;
use aes_gcm_siv::aead::{Aead, OsRng};
use aes_gcm_siv::{Aes256GcmSiv, Key, KeyInit, Nonce};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use tc_error::{TCError, TCResult};

#[derive(Deserialize, Serialize)]
struct EncryptedPayload {
    nonce: String,
    data: String,
}

pub(super) fn decode_encrypted_payload(
    body: hyper::body::Bytes,
) -> TCResult<(Vec<u8>, Vec<u8>)> {
    if body.is_empty() || body.iter().all(|b| b.is_ascii_whitespace()) {
        return Err(TCError::bad_request("empty replication payload"));
    }

    let payload: EncryptedPayload = serde_json::from_slice(&body)
        .map_err(|err| TCError::bad_request(format!("invalid replication payload: {err}")))?;

    let nonce = BASE64
        .decode(payload.nonce.as_bytes())
        .map_err(|err| TCError::bad_request(format!("invalid nonce base64: {err}")))?;
    let data = BASE64
        .decode(payload.data.as_bytes())
        .map_err(|err| TCError::bad_request(format!("invalid data base64: {err}")))?;

    Ok((nonce, data))
}

pub(super) fn encode_encrypted_payload(nonce: &[u8], data: &[u8]) -> TCResult<Vec<u8>> {
    let payload = EncryptedPayload {
        nonce: BASE64.encode(nonce),
        data: BASE64.encode(data),
    };

    serde_json::to_vec(&payload)
        .map_err(|err| TCError::internal(format!("encode replication payload failed: {err}")))
}

pub(super) fn encrypt_path_with_key(
    path: &str,
    key: &Key<Aes256GcmSiv>,
) -> TCResult<(Vec<u8>, Vec<u8>)> {
    let cipher = Aes256GcmSiv::new(key);
    let mut nonce = [0u8; 12];
    OsRng.fill_bytes(&mut nonce);
    let encrypted = encrypt_path(&cipher, &nonce, path)?;
    Ok((nonce.to_vec(), encrypted))
}

pub(super) fn decrypt_token_with_key(
    key: &Key<Aes256GcmSiv>,
    nonce: &[u8],
    token_encrypted: &[u8],
) -> TCResult<String> {
    let cipher = Aes256GcmSiv::new(key);
    decrypt_token(&cipher, nonce, token_encrypted)
}

pub(super) fn decrypt_path(
    cipher: &Aes256GcmSiv,
    nonce: &[u8],
    path_encrypted: &[u8],
) -> TCResult<String> {
    let decrypted = cipher
        .decrypt(Nonce::from_slice(nonce), path_encrypted)
        .map_err(|_| TCError::bad_request("unable to decrypt replication path"))?;
    String::from_utf8(decrypted)
        .map_err(|cause| TCError::bad_request(format!("invalid UTF8: {cause}")))
}

fn decrypt_token(
    cipher: &Aes256GcmSiv,
    nonce: &[u8],
    token_encrypted: &[u8],
) -> TCResult<String> {
    match cipher.decrypt(Nonce::from_slice(nonce), token_encrypted) {
        Ok(token_decrypted) => String::from_utf8(token_decrypted)
            .map_err(|cause| TCError::bad_request(format!("invalid UTF8: {cause}"))),
        Err(_cause) => Err(TCError::bad_request("unable to decrypt token")),
    }
}

fn encrypt_path(cipher: &Aes256GcmSiv, nonce: &[u8], path: &str) -> TCResult<Vec<u8>> {
    let nonce: [u8; 12] = nonce
        .try_into()
        .map_err(|_| TCError::bad_request("invalid nonce length"))?;
    let nonce = Nonce::from_slice(&nonce);
    cipher
        .encrypt(nonce, path.as_bytes())
        .map_err(|_| TCError::internal("unable to encrypt path"))
}

fn encrypt_token(cipher: &Aes256GcmSiv, nonce: &[u8], token: String) -> TCResult<Vec<u8>> {
    let nonce: [u8; 12] = nonce
        .try_into()
        .map_err(|_| TCError::bad_request("invalid nonce length"))?;
    let nonce = Nonce::from_slice(&nonce);
    cipher
        .encrypt(nonce, token.as_bytes())
        .map_err(|_| TCError::internal("unable to encrypt token"))
}

pub(super) fn encrypt_token_with_key(
    token: String,
    key: &Key<Aes256GcmSiv>,
    nonce: &[u8],
) -> TCResult<Vec<u8>> {
    let cipher = Aes256GcmSiv::new(key);
    encrypt_token(&cipher, nonce, token)
}
