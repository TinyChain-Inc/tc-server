use aes_gcm_siv::aead::Aead;
use aes_gcm_siv::aead::{OsRng, rand_core::RngCore};
use aes_gcm_siv::{Aes256GcmSiv, Key, KeyInit, Nonce};
use tc_error::{TCError, TCResult};

pub(super) fn encrypt_with_key(
    plaintext: &[u8],
    key: &Key<Aes256GcmSiv>,
) -> TCResult<(Vec<u8>, Vec<u8>)> {
    let cipher = Aes256GcmSiv::new(key);
    let mut nonce = [0u8; 12];
    OsRng.fill_bytes(&mut nonce);
    let encrypted = encrypt(&cipher, &nonce, plaintext)?;
    Ok((nonce.to_vec(), encrypted))
}

pub(super) fn decrypt(cipher: &Aes256GcmSiv, nonce: &[u8], ciphertext: &[u8]) -> TCResult<Vec<u8>> {
    let nonce = decode_nonce(nonce)?;
    cipher
        .decrypt(&nonce, ciphertext)
        .map_err(|_| TCError::bad_request("unable to decrypt replication message"))
}

fn encrypt(cipher: &Aes256GcmSiv, nonce: &[u8], plaintext: &[u8]) -> TCResult<Vec<u8>> {
    let nonce: [u8; 12] = nonce
        .try_into()
        .map_err(|_| TCError::bad_request("invalid nonce length"))?;
    cipher
        .encrypt(&Nonce::from(nonce), plaintext)
        .map_err(|_| TCError::internal("unable to encrypt replication message"))
}

fn decode_nonce(nonce: &[u8]) -> TCResult<Nonce> {
    let nonce: [u8; 12] = nonce
        .try_into()
        .map_err(|_| TCError::bad_request("invalid nonce length"))?;
    Ok(nonce.into())
}
