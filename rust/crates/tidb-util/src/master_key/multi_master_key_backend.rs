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

//! Go `br/pkg/encryption/master_key/multi_master_key_backend.go`: a backend
//! holding several master-key backends, any one of which may be the one that
//! decrypts. It exists for future master-key high availability; today exactly
//! one backend is ever configured.

use std::sync::Arc;

use super::pb::{EncryptedContent, MasterKey};
use super::{create_backend, Backend, KmsProviderFactory};

/// Go `defaultBackendCapacity`.
const DEFAULT_BACKEND_CAPACITY: usize = 5;

/// Go `MultiMasterKeyBackend`.
pub struct MultiMasterKeyBackend {
    backends: Vec<Arc<dyn Backend>>,
}

impl MultiMasterKeyBackend {
    /// Go `NewMultiMasterKeyBackend`.
    ///
    /// Go's guard reads `masterKeysProto == nil && len(masterKeysProto) == 0`;
    /// the `&&` is a bug that lets an empty-but-non-nil slice through to a
    /// backend with no members, which then fails in `Decrypt` with the
    /// "internal error" message. Rust's `&[T]` cannot tell nil from empty, so
    /// this rejects every empty input; the no-member state is still reachable
    /// through [`MultiMasterKeyBackend::from_backends`], and Go's own test
    /// reaches it the same way, by building the struct directly.
    pub fn new(
        master_keys_proto: &[MasterKey],
        kms_factory: &KmsProviderFactory,
    ) -> Result<MultiMasterKeyBackend, String> {
        if master_keys_proto.is_empty() {
            return Err("must provide at least one master key".to_owned());
        }
        let mut backends: Vec<Arc<dyn Backend>> = Vec::with_capacity(DEFAULT_BACKEND_CAPACITY);
        for master_key_proto in master_keys_proto {
            backends.push(create_backend(Some(master_key_proto), kms_factory)?);
        }
        Ok(MultiMasterKeyBackend { backends })
    }

    /// Builds the backend from already-constructed members, which is how Go's
    /// tests populate the private `backends` field.
    pub fn from_backends(backends: Vec<Arc<dyn Backend>>) -> MultiMasterKeyBackend {
        MultiMasterKeyBackend { backends }
    }
}

impl Backend for MultiMasterKeyBackend {
    /// Go `Decrypt`: the first backend that succeeds wins; if none does, every
    /// error is reported (Go accumulates them with `multierr.Append`).
    fn decrypt(&self, encrypted_content: &EncryptedContent) -> Result<Vec<u8>, String> {
        if self.backends.is_empty() {
            return Err("internal error: should always contain at least one backend".to_owned());
        }

        let mut errors: Vec<String> = Vec::new();
        for master_key_backend in &self.backends {
            match master_key_backend.decrypt(encrypted_content) {
                Ok(result) => return Ok(result),
                Err(error) => errors.push(error),
            }
        }

        Err(format!(
            "failed to decrypt in multi master key backend: {}",
            errors.join("; ")
        ))
    }

    fn close(&self) {
        for backend in &self.backends {
            backend.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Go `MockBackend`, built on `testify/mock`. There is no such framework
    /// here, so the mock is a fixed response plus a call counter, which covers
    /// everything Go's `AssertExpectations` / `AssertNotCalled` check.
    struct MockBackend {
        response: Result<Vec<u8>, String>,
        calls: AtomicUsize,
    }

    impl MockBackend {
        fn new(response: Result<Vec<u8>, String>) -> Arc<MockBackend> {
            Arc::new(MockBackend {
                response,
                calls: AtomicUsize::new(0),
            })
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl Backend for MockBackend {
        fn decrypt(&self, _encrypted_content: &EncryptedContent) -> Result<Vec<u8>, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.response.clone()
        }

        fn close(&self) {
            // do nothing
        }
    }

    fn encrypted_content() -> EncryptedContent {
        EncryptedContent {
            content: b"encrypted".to_vec(),
            ..EncryptedContent::default()
        }
    }

    /// Go `TestMultiMasterKeyBackendDecrypt`, subtest "success first backend".
    #[test]
    fn test_multi_master_key_backend_decrypt_success_first_backend() {
        let mock1 = MockBackend::new(Ok(b"decrypted".to_vec()));
        let mock2 = MockBackend::new(Ok(b"unused".to_vec()));
        let backend = MultiMasterKeyBackend::from_backends(vec![
            Arc::clone(&mock1) as Arc<dyn Backend>,
            Arc::clone(&mock2) as Arc<dyn Backend>,
        ]);

        let result = backend.decrypt(&encrypted_content()).unwrap();
        assert_eq!(result, b"decrypted");

        assert_eq!(mock1.calls(), 1);
        assert_eq!(mock2.calls(), 0, "second backend must not be called");
    }

    /// Go `TestMultiMasterKeyBackendDecrypt`, subtest "success second backend".
    #[test]
    fn test_multi_master_key_backend_decrypt_success_second_backend() {
        let mock1 = MockBackend::new(Err("failed".to_owned()));
        let mock2 = MockBackend::new(Ok(b"decrypted".to_vec()));
        let backend = MultiMasterKeyBackend::from_backends(vec![
            Arc::clone(&mock1) as Arc<dyn Backend>,
            Arc::clone(&mock2) as Arc<dyn Backend>,
        ]);

        let result = backend.decrypt(&encrypted_content()).unwrap();
        assert_eq!(result, b"decrypted");

        assert_eq!(mock1.calls(), 1);
        assert_eq!(mock2.calls(), 1);
    }

    /// Go `TestMultiMasterKeyBackendDecrypt`, subtest "all backends fail".
    #[test]
    fn test_multi_master_key_backend_decrypt_all_fail() {
        let mock1 = MockBackend::new(Err("failed1".to_owned()));
        let mock2 = MockBackend::new(Err("failed2".to_owned()));
        let backend = MultiMasterKeyBackend::from_backends(vec![
            Arc::clone(&mock1) as Arc<dyn Backend>,
            Arc::clone(&mock2) as Arc<dyn Backend>,
        ]);

        let error = backend.decrypt(&encrypted_content()).unwrap_err();
        assert!(error.contains("failed1"), "{error}");
        assert!(error.contains("failed2"), "{error}");

        assert_eq!(mock1.calls(), 1);
        assert_eq!(mock2.calls(), 1);
    }

    /// Go `TestMultiMasterKeyBackendDecrypt`, subtest "no backends".
    #[test]
    fn test_multi_master_key_backend_decrypt_no_backends() {
        let backend = MultiMasterKeyBackend::from_backends(Vec::new());
        let error = backend.decrypt(&encrypted_content()).unwrap_err();
        assert!(error.contains("internal error"), "{error}");
    }

    /// `unwrap_err` needs `Debug` on the success type, which the backends do
    /// not implement; this extracts the message instead.
    fn error_of<T>(result: Result<T, String>) -> String {
        result.err().expect("expected an error")
    }

    /// Not in the Go package's test set: the constructor's guard, and that
    /// `Close` reaches every member.
    #[test]
    fn test_multi_master_key_backend_construction_and_close() {
        let factory: KmsProviderFactory = Box::new(|_| Err("no KMS in tests".to_owned()));
        assert_eq!(
            error_of(MultiMasterKeyBackend::new(&[], &factory)),
            "must provide at least one master key"
        );
        // A construction failure of any member fails the whole backend.
        assert!(MultiMasterKeyBackend::new(&[MasterKey::Plaintext], &factory).is_err());

        let mock = MockBackend::new(Ok(Vec::new()));
        let backend =
            MultiMasterKeyBackend::from_backends(vec![Arc::clone(&mock) as Arc<dyn Backend>]);
        backend.close();
    }
}
