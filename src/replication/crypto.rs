use aes_gcm_siv::aead::Aead;
use aes_gcm_siv::aead::{OsRng, rand_core::RngCore};
use aes_gcm_siv::{Aes256GcmSiv, Nonce};
use aes_gcm_siv::{Key, KeyInit};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};
use tc_error::{TCError, TCResult};

#[derive(Deserialize, Serialize)]
struct EncryptedPayload {
    nonce: String,
    data: String,
}

pub(super) fn decode_encrypted_payload(body: &[u8]) -> TCResult<(Vec<u8>, Vec<u8>)> {
    let payload: EncryptedPayload = serde_json::from_slice(body)
        .map_err(|error| TCError::bad_request(format!("invalid encrypted payload: {error}")))?;
    let nonce = BASE64
        .decode(payload.nonce)
        .map_err(|error| TCError::bad_request(format!("invalid nonce base64: {error}")))?;
    let data = BASE64
        .decode(payload.data)
        .map_err(|error| TCError::bad_request(format!("invalid data base64: {error}")))?;
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

pub(super) fn decrypt_path(
    cipher: &Aes256GcmSiv,
    nonce: &[u8],
    path_encrypted: &[u8],
) -> TCResult<String> {
    let nonce = decode_nonce(nonce)?;
    let decrypted = cipher
        .decrypt(&nonce, path_encrypted)
        .map_err(|_| TCError::bad_request("unable to decrypt replication path"))?;
    String::from_utf8(decrypted)
        .map_err(|cause| TCError::bad_request(format!("invalid UTF8: {cause}")))
}

fn encrypt_path(cipher: &Aes256GcmSiv, nonce: &[u8], path: &str) -> TCResult<Vec<u8>> {
    let nonce: [u8; 12] = nonce
        .try_into()
        .map_err(|_| TCError::bad_request("invalid nonce length"))?;
    let nonce: Nonce = nonce.into();
    cipher
        .encrypt(&nonce, path.as_bytes())
        .map_err(|_| TCError::internal("unable to encrypt path"))
}

fn decode_nonce(nonce: &[u8]) -> TCResult<Nonce> {
    let nonce: [u8; 12] = nonce
        .try_into()
        .map_err(|_| TCError::bad_request("invalid nonce length"))?;
    Ok(nonce.into())
}
