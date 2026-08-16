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

//! Go `br/pkg/encryption/master_key/kms_backend.go`: a backend whose data key
//! is itself encrypted, and is unwrapped by a cloud KMS on first use.
//!
//! The one piece of state that matters is the cache: the KMS round trip is
//! remote and rate-limited, so the unwrapped key is kept alongside the
//! encrypted key it came from, and reused for as long as successive contents
//! carry that same encrypted key.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::common::{METADATA_KEY_KMS_CIPHERTEXT_KEY, METADATA_KEY_KMS_VENDOR};
use super::kms::{CryptographyType, EncryptedKey, KmsProvider, PlainKey};
use super::mem_backend::MemAesGcmBackend;
use super::pb::EncryptedContent;
use super::Backend;

/// Go `CachedKeys`.
struct CachedKeys {
    encryption_backend: MemAesGcmBackend,
    cached_ciphertext_key: EncryptedKey,
}

/// Go `KmsBackend`.
pub struct KmsBackend {
    /// Go's anonymous `struct { sync.Mutex; cached *CachedKeys }`.
    state: Mutex<Option<CachedKeys>>,
    kms_provider: Arc<dyn KmsProvider>,
}

impl KmsBackend {
    /// Go `NewKmsBackend`. Fallible in Go for symmetry with the other
    /// constructors; it can never actually fail.
    pub fn new(kms_provider: Arc<dyn KmsProvider>) -> Result<KmsBackend, String> {
        Ok(KmsBackend {
            state: Mutex::new(None),
            kms_provider,
        })
    }
}

impl Backend for KmsBackend {
    fn decrypt(&self, content: &EncryptedContent) -> Result<Vec<u8>, String> {
        let vendor_name = self.kms_provider.name();
        match content.metadata.get(METADATA_KEY_KMS_VENDOR) {
            None => return Err("wrong master key: missing KMS vendor".to_owned()),
            Some(value) if value.as_slice() != vendor_name.as_bytes() => {
                return Err(format!(
                    "KMS vendor mismatch expect {vendor_name} got {}",
                    String::from_utf8_lossy(value)
                ));
            }
            Some(_) => {}
        }

        let Some(ciphertext_key_bytes) = content.metadata.get(METADATA_KEY_KMS_CIPHERTEXT_KEY)
        else {
            return Err("KMS ciphertext key not found".to_owned());
        };
        let ciphertext_key = EncryptedKey::new(ciphertext_key_bytes)
            .map_err(|error| format!("failed to create encrypted key: {error}"))?;

        let mut state = self.state.lock().expect("kms backend state");

        if let Some(cached) = state.as_ref() {
            if cached.cached_ciphertext_key.equal(&ciphertext_key) {
                return cached.encryption_backend.decrypt_content(content);
            }
        }

        let decrypted_key =
            retry_all_errors(|| self.kms_provider.decrypt_data_key(&ciphertext_key))
                .map_err(|error| format!("decrypt encrypted key failed: {error}"))?;

        let plaintext_key = PlainKey::new(&decrypted_key, CryptographyType::AesGcm256)
            .map_err(|error| format!("decrypt encrypted key failed: {error}"))?;

        let backend = MemAesGcmBackend::new(plaintext_key.key())
            .map_err(|error| format!("failed to create MemAesGcmBackend: {error}"))?;

        *state = Some(CachedKeys {
            encryption_backend: backend,
            cached_ciphertext_key: ciphertext_key,
        });

        state
            .as_ref()
            .expect("just cached")
            .encryption_backend
            .decrypt_content(content)
    }

    fn close(&self) {
        self.kms_provider.close();
    }
}

/// boundary: `br/pkg/utils.WithRetryV2` driven by
/// `NewBackoffRetryAllErrorStrategy(10, 500*time.Millisecond, 5*time.Second)`.
///
/// `br/pkg/utils`' backoff framework — retry contexts, per-error retryability
/// predicates, sampled logging — is a package of its own; what this call site
/// asks of it is "retry every error, at most 10 attempts, sleeping 500ms and
/// doubling up to a 5s cap", which is what this reproduces. Go's version also
/// aborts early on context cancellation; there is no context here (see
/// [`super::kms::KmsProvider`]).
fn retry_all_errors<T>(mut operation: impl FnMut() -> Result<T, String>) -> Result<T, String> {
    const REMAINING_ATTEMPTS: u32 = 10;
    const DELAY_TIME: Duration = Duration::from_millis(500);
    const MAX_DELAY_TIME: Duration = Duration::from_secs(5);

    let mut all_errors: Vec<String> = Vec::new();
    let mut delay = DELAY_TIME;
    for attempt in 0..REMAINING_ATTEMPTS {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) => all_errors.push(error),
        }
        if attempt + 1 < REMAINING_ATTEMPTS {
            std::thread::sleep(delay);
            delay = (delay * 2).min(MAX_DELAY_TIME);
        }
    }
    Err(all_errors.join("; "))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Go `mockKmsProvider`.
    struct MockKmsProvider {
        name: String,
        decrypt_counter: AtomicUsize,
    }

    impl MockKmsProvider {
        fn new(name: &str) -> Arc<MockKmsProvider> {
            Arc::new(MockKmsProvider {
                name: name.to_owned(),
                decrypt_counter: AtomicUsize::new(0),
            })
        }

        fn counter(&self) -> usize {
            self.decrypt_counter.load(Ordering::SeqCst)
        }
    }

    impl KmsProvider for MockKmsProvider {
        fn decrypt_data_key(&self, _encrypted_key: &EncryptedKey) -> Result<Vec<u8>, String> {
            self.decrypt_counter.fetch_add(1, Ordering::SeqCst);
            let mut key = vec![0u8; 32]; // 256 bits = 32 bytes
            getrandom::fill(&mut key).map_err(|error| error.to_string())?;
            Ok(key)
        }

        fn name(&self) -> String {
            self.name.clone()
        }

        fn close(&self) {
            // do nothing
        }
    }

    fn content_with(vendor: &str, ciphertext_key: &[u8]) -> EncryptedContent {
        let mut content = EncryptedContent {
            content: b"encrypted_content".to_vec(),
            ..EncryptedContent::default()
        };
        content.metadata.insert(
            METADATA_KEY_KMS_VENDOR.to_owned(),
            vendor.as_bytes().to_vec(),
        );
        content.metadata.insert(
            METADATA_KEY_KMS_CIPHERTEXT_KEY.to_owned(),
            ciphertext_key.to_vec(),
        );
        content
    }

    /// Go `TestKmsBackendDecrypt`.
    ///
    /// As in Go, the decryptions themselves fail — the content carries no
    /// method/IV/tag metadata — and only the KMS call count is asserted.
    #[test]
    fn test_kms_backend_decrypt() {
        let mock_provider = MockKmsProvider::new("mock_kms");
        let backend = KmsBackend::new(Arc::clone(&mock_provider) as Arc<dyn KmsProvider>).unwrap();

        let mut content = content_with("mock_kms", b"ciphertext_key");

        // First decryption
        let _ = backend.decrypt(&content);
        assert_eq!(
            mock_provider.counter(),
            1,
            "KMS provider should be called once"
        );

        // Second decryption with the same ciphertext key (should use cache)
        let _ = backend.decrypt(&content);
        assert_eq!(
            mock_provider.counter(),
            1,
            "KMS provider should not be called again"
        );

        // Third decryption with a different ciphertext key
        content.metadata.insert(
            METADATA_KEY_KMS_CIPHERTEXT_KEY.to_owned(),
            b"new_ciphertext_key".to_vec(),
        );
        let _ = backend.decrypt(&content);
        assert_eq!(
            mock_provider.counter(),
            2,
            "KMS provider should be called again for a new key"
        );

        backend.close();
    }

    /// Go `TestKmsBackendDecryptErrors`.
    #[test]
    fn test_kms_backend_decrypt_errors() {
        let mock_provider = MockKmsProvider::new("mock_kms");
        let backend = KmsBackend::new(mock_provider as Arc<dyn KmsProvider>).unwrap();

        let mut missing_vendor = EncryptedContent::default();
        missing_vendor.metadata.insert(
            METADATA_KEY_KMS_CIPHERTEXT_KEY.to_owned(),
            b"ciphertext_key".to_vec(),
        );

        let vendor_mismatch = content_with("wrong_kms", b"ciphertext_key");

        let mut missing_key = EncryptedContent::default();
        missing_key
            .metadata
            .insert(METADATA_KEY_KMS_VENDOR.to_owned(), b"mock_kms".to_vec());

        let cases = [
            (
                "missing KMS vendor",
                missing_vendor,
                "wrong master key: missing KMS vendor",
            ),
            (
                "KMS vendor mismatch",
                vendor_mismatch,
                "KMS vendor mismatch expect mock_kms got wrong_kms",
            ),
            (
                "missing ciphertext key",
                missing_key,
                "KMS ciphertext key not found",
            ),
        ];

        for (name, content, expected) in cases {
            let error = backend.decrypt(&content).unwrap_err();
            assert!(error.contains(expected), "{name}: {error}");
        }
    }

    /// Not in the Go package's test set: the happy path. Go's mock returns a
    /// fresh random key on every call, so a cached backend can never actually
    /// decrypt anything there; a stable mock key shows the cache really is what
    /// makes the second decryption succeed without a KMS round trip.
    #[test]
    fn test_kms_backend_decrypts_with_cached_key() {
        struct StableProvider {
            key: Vec<u8>,
            calls: AtomicUsize,
        }
        impl KmsProvider for StableProvider {
            fn decrypt_data_key(&self, _encrypted_key: &EncryptedKey) -> Result<Vec<u8>, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(self.key.clone())
            }
            fn name(&self) -> String {
                "mock_kms".to_owned()
            }
            fn close(&self) {}
        }

        let provider = Arc::new(StableProvider {
            key: vec![3u8; 32],
            calls: AtomicUsize::new(0),
        });
        let backend = KmsBackend::new(Arc::clone(&provider) as Arc<dyn KmsProvider>).unwrap();

        // Seal with the same data key the KMS will hand back.
        let sealer = MemAesGcmBackend::new(&[3u8; 32]).unwrap();
        let iv = super::super::common::Iv::new_gcm().unwrap();
        let mut content = sealer.encrypt_content(b"top secret", &iv).unwrap();
        content
            .metadata
            .insert(METADATA_KEY_KMS_VENDOR.to_owned(), b"mock_kms".to_vec());
        content.metadata.insert(
            METADATA_KEY_KMS_CIPHERTEXT_KEY.to_owned(),
            b"ciphertext_key".to_vec(),
        );

        assert_eq!(backend.decrypt(&content).unwrap(), b"top secret");
        assert_eq!(backend.decrypt(&content).unwrap(), b"top secret");
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }

    /// Not in the Go package's test set: an empty encrypted key is rejected
    /// before the KMS is ever reached.
    #[test]
    fn test_kms_backend_rejects_empty_ciphertext_key() {
        let mock_provider = MockKmsProvider::new("mock_kms");
        let backend = KmsBackend::new(Arc::clone(&mock_provider) as Arc<dyn KmsProvider>).unwrap();
        let content = content_with("mock_kms", b"");
        let error = backend.decrypt(&content).unwrap_err();
        assert!(error.contains("encrypted key cannot be empty"), "{error}");
        assert_eq!(mock_provider.counter(), 0);
    }

    /// Not in the Go package's test set: the narrowed retry loop must run the
    /// operation until it succeeds and surface every error when it never does.
    #[test]
    fn test_retry_all_errors() {
        let attempts = AtomicUsize::new(0);
        let value = retry_all_errors(|| {
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                // A single retry costs one 500ms sleep; that is the schedule.
                Err("transient".to_owned())
            } else {
                Ok(7)
            }
        })
        .unwrap();
        assert_eq!(value, 7);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
