// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `br/pkg/encryption/master_key/mem_backend.go`: AES-256-GCM over a key
//! held in memory. Every other backend delegates its actual crypto here.

use super::common::{
    Iv, METADATA_KEY_AES_GCM_TAG, METADATA_KEY_IV, METADATA_KEY_METHOD, METADATA_METHOD_AES256_GCM,
};
use super::gcm::Aes256Gcm;
use super::kms::{CryptographyType, PlainKey};
use super::pb::EncryptedContent;

/// Go `gcmTagNotFound`; `file_backend`'s test matches on this text.
pub const GCM_TAG_NOT_FOUND: &str = "aes gcm tag not found";
/// Go `wrongMasterKey`; `file_backend`'s test matches on this text.
pub const WRONG_MASTER_KEY: &str = "wrong master key";

/// Go `MemAesGcmBackend`.
pub struct MemAesGcmBackend {
    key: PlainKey,
    cipher: Aes256Gcm,
}

impl MemAesGcmBackend {
    /// Go `NewMemAesGcmBackend`: the key must be exactly 32 bytes.
    ///
    /// Go builds the `cipher.AEAD` afresh inside every `EncryptContent` /
    /// `DecryptContent` call; the AEAD is stateless apart from the key
    /// schedule, so it is built once here and reused.
    pub fn new(key: &[u8]) -> Result<MemAesGcmBackend, String> {
        let plain_key = PlainKey::new(key, CryptographyType::AesGcm256)
            .map_err(|error| format!("failed to create new mem aes gcm backend: {error}"))?;
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(plain_key.key());
        Ok(MemAesGcmBackend {
            key: plain_key,
            cipher: Aes256Gcm::new(&key_bytes),
        })
    }

    /// The wrapped key, for callers that need to hand it on (Go reads
    /// `m.key.Key()` directly, the fields being package-private).
    pub fn key(&self) -> &PlainKey {
        &self.key
    }

    /// Go `EncryptContent`.
    ///
    /// Go seals into one buffer and then splits the trailing `Overhead()`
    /// bytes off as the tag; [`Aes256Gcm::seal`] returns the two parts already
    /// separated, which is the same split.
    pub fn encrypt_content(&self, plaintext: &[u8], iv: &Iv) -> Result<EncryptedContent, String> {
        let mut content = EncryptedContent::default();
        content.metadata.insert(
            METADATA_KEY_METHOD.to_owned(),
            METADATA_METHOD_AES256_GCM.as_bytes().to_vec(),
        );
        content
            .metadata
            .insert(METADATA_KEY_IV.to_owned(), iv.as_slice().to_vec());

        let (ciphertext, tag) = self
            .cipher
            .seal(iv.as_slice(), plaintext)
            .map_err(|error| error.to_string())?;
        content.content = ciphertext;
        content
            .metadata
            .insert(METADATA_KEY_AES_GCM_TAG.to_owned(), tag.to_vec());
        Ok(content)
    }

    /// Go `DecryptContent`.
    pub fn decrypt_content(&self, content: &EncryptedContent) -> Result<Vec<u8>, String> {
        let Some(method) = content.metadata.get(METADATA_KEY_METHOD) else {
            return Err(format!("metadata {METADATA_KEY_METHOD} not found"));
        };
        if method.as_slice() != METADATA_METHOD_AES256_GCM.as_bytes() {
            return Err(format!(
                "encryption method mismatch, expected {METADATA_METHOD_AES256_GCM} vs actual {}",
                String::from_utf8_lossy(method)
            ));
        }

        let Some(iv_value) = content.metadata.get(METADATA_KEY_IV) else {
            return Err(format!("metadata {METADATA_KEY_IV} not found"));
        };
        let iv = Iv::from_slice(iv_value)?;

        let Some(tag) = content.metadata.get(METADATA_KEY_AES_GCM_TAG) else {
            return Err(GCM_TAG_NOT_FOUND.to_owned());
        };

        self.cipher
            .open(iv.as_slice(), &content.content, tag)
            .map_err(|error| format!("{WRONG_MASTER_KEY} :decrypt in GCM mode failed: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `unwrap_err` needs `Debug` on the success type, which the backends do
    /// not implement; this extracts the message instead.
    fn error_of<T>(result: Result<T, String>) -> String {
        result.err().expect("expected an error")
    }

    /// Go `TestNewMemAesGcmBackend`.
    #[test]
    fn test_new_mem_aes_gcm_backend() {
        let key = [0u8; 32]; // 256-bit key
        MemAesGcmBackend::new(&key).expect("Failed to create MemAesGcmBackend");

        let short_key = [0u8; 16];
        error_of(MemAesGcmBackend::new(&short_key)); // Expected error for short key
    }

    /// Go `TestEncryptDecrypt`.
    #[test]
    fn test_encrypt_decrypt() {
        let key = [0u8; 32];
        let backend = MemAesGcmBackend::new(&key).expect("Failed to create MemAesGcmBackend");

        let plaintext = b"Hello, World!";
        let iv = Iv::new_gcm().expect("failed to create gcm iv");

        let encrypted = backend
            .encrypt_content(plaintext, &iv)
            .expect("Encryption failed");
        let decrypted = backend
            .decrypt_content(&encrypted)
            .expect("Decryption failed");

        assert_eq!(
            decrypted, plaintext,
            "Decrypted text doesn't match original"
        );
    }

    /// Go `TestDecryptWithWrongKey`.
    #[test]
    fn test_decrypt_with_wrong_key() {
        let key1 = [0u8; 32];
        let key2 = [1u8; 32]; // Different from key1

        let backend1 = MemAesGcmBackend::new(&key1).unwrap();
        let backend2 = MemAesGcmBackend::new(&key2).unwrap();

        let plaintext = b"Hello, World!";
        let iv = Iv::new_gcm().expect("failed to create gcm iv");

        let encrypted = backend1.encrypt_content(plaintext, &iv).unwrap();
        backend2
            .decrypt_content(&encrypted)
            .expect_err("Expected decryption with wrong key to fail");
    }

    /// Go `TestDecryptWithTamperedCiphertext`.
    #[test]
    fn test_decrypt_with_tampered_ciphertext() {
        let key = [0u8; 32];
        let backend = MemAesGcmBackend::new(&key).unwrap();

        let plaintext = b"Hello, World!";
        let iv = Iv::new_gcm().expect("failed to create gcm iv");

        let mut encrypted = backend.encrypt_content(plaintext, &iv).unwrap();
        encrypted.content[0] ^= 1; // Tamper with the ciphertext

        backend
            .decrypt_content(&encrypted)
            .expect_err("Expected decryption of tampered ciphertext to fail");
    }

    /// Go `TestDecryptWithMissingMetadata`.
    #[test]
    fn test_decrypt_with_missing_metadata() {
        let key = [0u8; 32];
        let backend = MemAesGcmBackend::new(&key).unwrap();

        let plaintext = b"Hello, World!";
        let iv = Iv::new_gcm().expect("failed to create gcm iv");

        let mut encrypted = backend.encrypt_content(plaintext, &iv).unwrap();
        encrypted.metadata.remove(METADATA_KEY_METHOD);

        backend
            .decrypt_content(&encrypted)
            .expect_err("Expected decryption with missing metadata to fail");
    }

    /// Go `TestEncryptDecryptLargeData`.
    #[test]
    fn test_encrypt_decrypt_large_data() {
        let key = [0u8; 32];
        let backend = MemAesGcmBackend::new(&key).unwrap();

        let plaintext = vec![0u8; 1_000_000]; // 1 MB of data
        let iv = Iv::new_gcm().expect("failed to create gcm iv");

        let encrypted = backend
            .encrypt_content(&plaintext, &iv)
            .expect("Encryption of large data failed");
        let decrypted = backend
            .decrypt_content(&encrypted)
            .expect("Decryption of large data failed");

        assert!(
            plaintext == decrypted,
            "Decrypted large data doesn't match original"
        );
    }

    /// Not in the Go package's test set: the remaining metadata rejections
    /// (wrong method, missing/invalid IV, missing tag) each have their own
    /// message, and `file_backend`'s test only reaches one of them.
    #[test]
    fn test_decrypt_metadata_errors() {
        let backend = MemAesGcmBackend::new(&[0u8; 32]).unwrap();
        let iv = Iv::new_gcm().unwrap();
        let encrypted = backend.encrypt_content(b"payload", &iv).unwrap();

        let mut wrong_method = encrypted.clone();
        wrong_method
            .metadata
            .insert(METADATA_KEY_METHOD.to_owned(), b"aes128-ctr".to_vec());
        assert_eq!(
            backend.decrypt_content(&wrong_method).unwrap_err(),
            "encryption method mismatch, expected aes256-gcm vs actual aes128-ctr"
        );

        let mut missing_iv = encrypted.clone();
        missing_iv.metadata.remove(METADATA_KEY_IV);
        assert_eq!(
            backend.decrypt_content(&missing_iv).unwrap_err(),
            "metadata iv not found"
        );

        let mut bad_iv = encrypted.clone();
        bad_iv
            .metadata
            .insert(METADATA_KEY_IV.to_owned(), vec![0u8; 7]);
        assert_eq!(
            backend.decrypt_content(&bad_iv).unwrap_err(),
            "invalid IV length, must be 12 or 16 bytes, got 7"
        );

        let mut missing_tag = encrypted.clone();
        missing_tag.metadata.remove(METADATA_KEY_AES_GCM_TAG);
        assert_eq!(
            backend.decrypt_content(&missing_tag).unwrap_err(),
            GCM_TAG_NOT_FOUND
        );

        let mut empty = EncryptedContent::default();
        assert_eq!(
            backend.decrypt_content(&empty).unwrap_err(),
            "metadata method not found"
        );
        empty.metadata.insert(
            METADATA_KEY_METHOD.to_owned(),
            METADATA_METHOD_AES256_GCM.as_bytes().to_vec(),
        );
        assert_eq!(
            backend.decrypt_content(&empty).unwrap_err(),
            "metadata iv not found"
        );
    }

    /// Not in the Go package's test set: a 16-byte CTR IV passes
    /// `NewIVFromSlice` but is not a legal GCM nonce. Go *panics* inside
    /// `crypto/cipher` here; this port reports the error instead.
    #[test]
    fn test_ctr_iv_is_rejected_not_panicking() {
        let backend = MemAesGcmBackend::new(&[0u8; 32]).unwrap();
        let iv = Iv::from_slice(&[0u8; 16]).unwrap();
        let error = backend.encrypt_content(b"payload", &iv).unwrap_err();
        assert!(error.contains("incorrect nonce length"), "{error}");
    }

    /// Not in the Go package's test set: the metadata a fresh encryption emits
    /// is a wire contract with TiKV.
    #[test]
    fn test_encrypt_content_metadata_shape() {
        let backend = MemAesGcmBackend::new(&[9u8; 32]).unwrap();
        let iv = Iv::from_slice(&[4u8; 12]).unwrap();
        let encrypted = backend.encrypt_content(b"0123456789abcdef!!", &iv).unwrap();

        assert_eq!(
            encrypted.metadata.get(METADATA_KEY_METHOD).unwrap(),
            b"aes256-gcm"
        );
        assert_eq!(encrypted.metadata.get(METADATA_KEY_IV).unwrap(), &[4u8; 12]);
        assert_eq!(
            encrypted
                .metadata
                .get(METADATA_KEY_AES_GCM_TAG)
                .unwrap()
                .len(),
            16
        );
        // GCM is a stream mode: the ciphertext is exactly as long as the input.
        assert_eq!(encrypted.content.len(), 18);
        assert_eq!(backend.key().key(), &[9u8; 32]);
    }
}
