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

//! Go `br/pkg/encryption/master_key` lands as a complete package: the metadata
//! constants and IV wrapper (`common.go`), the in-memory AES-256-GCM backend
//! every other backend delegates to (`mem_backend.go`), the on-disk key file
//! backend (`file_backend.go`), the cloud-KMS backend with its data-key cache
//! (`kms_backend.go`), the backend interface and its config dispatch
//! (`master_key.go`), and the multi-backend fan-out
//! (`multi_master_key_backend.go`), with all eleven of the package's test
//! functions.
//!
//! A BR backup's data keys are themselves encrypted under a *master* key. This
//! package is where that master key lives and where the unwrapping happens:
//! [`create_backend`] turns an `encryptionpb.MasterKey` config into a
//! [`Backend`], and every [`Backend::decrypt`] eventually reaches
//! [`MemAesGcmBackend::decrypt_content`], which authenticates the GCM tag
//! before returning a single byte of plaintext. The encrypted form is a
//! [`EncryptedContent`]: ciphertext in one field, and method / IV / tag (and,
//! for KMS, vendor and encrypted data key) in a metadata map whose keys are a
//! wire contract with TiKV's own encryption implementation.
//!
//! # Narrowings and boundaries
//!
//! - **AES-256-GCM.** Go uses `crypto/cipher.NewGCM`. No AEAD crate is
//!   available to this workspace offline, so [`gcm`] implements the GCM *mode*
//!   over the AES-256 **block cipher** from the `aes` crate this crate already
//!   depends on — the same layering `crate::encrypt` uses for CBC/CTR/OFB/CFB.
//!   No cipher primitive is hand-written. See that module for the NIST test
//!   vectors it is pinned against. Go's `cipher.NewGCM` panics on a nonce that
//!   is not 12 bytes; this port returns an error instead, which is reachable
//!   because `NewIVFromSlice` also accepts 16-byte CTR IVs.
//! - **`github.com/pingcap/kvproto/pkg/encryptionpb`** — [`pb`] redeclares
//!   `EncryptedContent`, `MasterKey` and `MasterKeyKms` as plain structs. They
//!   are flat messages; nothing here needs the protobuf codec.
//! - **`br/pkg/kms`** — [`kms`] ports `Provider` (as [`KmsProvider`]) plus the
//!   `EncryptedKey` / `PlainKey` / `CryptographyType` wrappers from
//!   `kms/common.go`. `kms.NewAwsKms` and `kms.NewGcpKms` are AWS/GCP SDK
//!   clients and are not ported; [`create_backend`] takes a
//!   [`KmsProviderFactory`] argument in their place, which is also how Go's own
//!   tests supply a provider.
//! - **`br/pkg/utils.WithRetryV2`** with
//!   `NewBackoffRetryAllErrorStrategy(10, 500ms, 5s)` — the KMS unwrap's retry
//!   loop, narrowed to a local loop with the same schedule; see
//!   `kms_backend::retry_all_errors`.
//! - **`context.Context`** — every Go method here takes one and passes it
//!   through; only the KMS retry loop reads it, to abort early on
//!   cancellation. There is no context type at this layer of the workspace, so
//!   the parameter is dropped.
//! - **`pingcap/log`** — `createCloudBackend`'s informational log about the
//!   KMS region/endpoint/key/vendor goes through `tidb_log`.
//! - **`go.uber.org/multierr`** — the multi-backend's accumulated failures are
//!   joined into one message rather than kept as a structured error list; Go's
//!   test only asserts that both messages appear.
//! - **`pkg/util/encrypt`** (`crate::encrypt`) holds this crate's other AES
//!   work, but shares nothing with this package: it is MySQL's
//!   `AES_ENCRYPT`/`AES_DECRYPT` block modes plus a random-access CTR layer,
//!   with no AEAD and no master-key concept. Its `AesCipher` wrapper is
//!   `pub(super)` to that module, so [`gcm`] uses the `aes` crate directly.

mod common;
mod file_backend;
mod gcm;
mod kms;
mod kms_backend;
mod mem_backend;
mod multi_master_key_backend;
mod pb;

use std::path::Path;
use std::sync::Arc;

pub use common::{
    Iv, IvType, CTR_IV_16, GCM_IV_12, METADATA_KEY_AES_GCM_TAG, METADATA_KEY_IV,
    METADATA_KEY_KMS_CIPHERTEXT_KEY, METADATA_KEY_KMS_VENDOR, METADATA_KEY_METHOD,
    METADATA_METHOD_AES256_GCM,
};
pub use file_backend::{create_file_backend, FileBackend, AES_GCM_KEY_LEN};
pub use gcm::{Aes256Gcm, GcmError};
pub use kms::{CryptographyType, EncryptedKey, KmsProvider, PlainKey};
pub use kms_backend::KmsBackend;
pub use mem_backend::{MemAesGcmBackend, GCM_TAG_NOT_FOUND, WRONG_MASTER_KEY};
pub use multi_master_key_backend::MultiMasterKeyBackend;
pub use pb::{EncryptedContent, MasterKey, MasterKeyKms};

use tidb_log::{Field, Value};

/// Go `StorageVendorNameAWS`.
pub const STORAGE_VENDOR_NAME_AWS: &str = "aws";
/// Go `StorageVendorNameAzure`.
pub const STORAGE_VENDOR_NAME_AZURE: &str = "azure";
/// Go `StorageVendorNameGCP`.
pub const STORAGE_VENDOR_NAME_GCP: &str = "gcp";

/// Go `Backend`: what an encryption backend must be able to do.
///
/// Go's `Decrypt` takes a `context.Context`; see the module's boundary notes.
pub trait Backend: Send + Sync {
    /// Go `Decrypt`: turns an [`EncryptedContent`] back into plaintext.
    fn decrypt(&self, ciphertext: &EncryptedContent) -> Result<Vec<u8>, String>;
    /// Go `Close`.
    fn close(&self);
}

/// boundary: `kms.NewAwsKms` / `kms.NewGcpKms`.
///
/// Go constructs the cloud client inline from the config. Those clients are
/// AWS/GCP SDK code that does not belong in this crate, so the caller supplies
/// them; [`create_backend`] still owns the vendor dispatch that decides whether
/// a client should be built at all.
pub type KmsProviderFactory =
    Box<dyn Fn(&MasterKeyKms) -> Result<Arc<dyn KmsProvider>, String> + Send + Sync>;

/// Go `CreateBackend`.
///
/// `config == nil` is `None` here.
pub fn create_backend(
    config: Option<&MasterKey>,
    kms_factory: &KmsProviderFactory,
) -> Result<Arc<dyn Backend>, String> {
    let Some(config) = config else {
        return Err("master key config is nil".to_owned());
    };

    match config {
        // Callers are supposed to have filtered plaintext out already.
        MasterKey::Plaintext => Err("should not create plaintext master key".to_owned()),
        MasterKey::File { path } => {
            let file_backend = create_file_backend(Path::new(path))
                // Go's annotation here is a copy-paste of the nil-config
                // message; preserved so log greps keep matching.
                .map_err(|error| format!("master key config is nil: {error}"))?;
            Ok(Arc::new(file_backend))
        }
        MasterKey::Kms(kms) => create_cloud_backend(kms, kms_factory),
    }
}

/// Go `createCloudBackend`.
fn create_cloud_backend(
    config: &MasterKeyKms,
    kms_factory: &KmsProviderFactory,
) -> Result<Arc<dyn Backend>, String> {
    tidb_log::info(
        "creating cloud KMS backend",
        &[
            Field::new("region", Value::Str(config.region.clone())),
            Field::new("endpoint", Value::Str(config.endpoint.clone())),
            Field::new("key_id", Value::Str(config.key_id.clone())),
            Field::new("Vendor", Value::Str(config.vendor.clone())),
        ],
    );

    match config.vendor.as_str() {
        STORAGE_VENDOR_NAME_AWS => {
            let kms_provider =
                kms_factory(config).map_err(|error| format!("new AWS KMS: {error}"))?;
            Ok(Arc::new(KmsBackend::new(kms_provider)?))
        }
        STORAGE_VENDOR_NAME_AZURE => Err("not implemented Azure KMS".to_owned()),
        STORAGE_VENDOR_NAME_GCP => {
            let kms_provider =
                kms_factory(config).map_err(|error| format!("new GCP KMS: {error}"))?;
            Ok(Arc::new(KmsBackend::new(kms_provider)?))
        }
        vendor => Err(format!("vendor not found: {vendor}")),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn no_kms() -> KmsProviderFactory {
        Box::new(|_| Err("no KMS client in tests".to_owned()))
    }

    struct StubProvider;
    impl KmsProvider for StubProvider {
        fn decrypt_data_key(&self, _encrypted_key: &EncryptedKey) -> Result<Vec<u8>, String> {
            Ok(vec![1u8; 32])
        }
        fn name(&self) -> String {
            "stub".to_owned()
        }
        fn close(&self) {}
    }

    /// `unwrap_err` needs `Debug` on the success type, which the backends do
    /// not implement; this extracts the message instead.
    fn error_of<T>(result: Result<T, String>) -> String {
        result.err().expect("expected an error")
    }

    /// Not in the Go package's test set: `CreateBackend` and
    /// `createCloudBackend` are entirely untested upstream, yet they are the
    /// only entry point BR itself calls.
    #[test]
    fn test_create_backend_dispatch() {
        let factory = no_kms();

        assert_eq!(
            error_of(create_backend(None, &factory)),
            "master key config is nil"
        );
        assert_eq!(
            error_of(create_backend(Some(&MasterKey::Plaintext), &factory)),
            "should not create plaintext master key"
        );

        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        file.write_all(b"c3d99825f2181f4808acd2068eac7441a65bd428f14d2aab43fefc0129091139\n")
            .expect("write");
        file.flush().expect("flush");
        let config = MasterKey::File {
            path: file.path().to_string_lossy().into_owned(),
        };
        let backend = create_backend(Some(&config), &factory).unwrap();
        // Round-trips through the file backend it just built.
        let sealed = create_file_backend(file.path())
            .unwrap()
            .encrypt(b"payload")
            .unwrap();
        assert_eq!(backend.decrypt(&sealed).unwrap(), b"payload");
        backend.close();

        let missing_file = MasterKey::File {
            path: "/nonexistent/br-master-key".to_owned(),
        };
        assert!(error_of(create_backend(Some(&missing_file), &factory))
            .contains("failed to read master key file from disk"));
    }

    /// Not in the Go package's test set: the cloud vendor dispatch, including
    /// Azure's explicit "not implemented" and the unknown-vendor message.
    #[test]
    fn test_create_cloud_backend_vendor_dispatch() {
        // `createCloudBackend` logs through the global logger.
        let _guard = crate::global_logger_test_guard();
        let failing = no_kms();
        let working: KmsProviderFactory =
            Box::new(|_| Ok(Arc::new(StubProvider) as Arc<dyn KmsProvider>));

        let kms = |vendor: &str| {
            MasterKey::Kms(MasterKeyKms {
                vendor: vendor.to_owned(),
                key_id: "key".to_owned(),
                region: "us-east-1".to_owned(),
                endpoint: String::new(),
            })
        };

        assert!(error_of(create_backend(
            Some(&kms(STORAGE_VENDOR_NAME_AWS)),
            &failing
        ))
        .starts_with("new AWS KMS"));
        assert!(error_of(create_backend(
            Some(&kms(STORAGE_VENDOR_NAME_GCP)),
            &failing
        ))
        .starts_with("new GCP KMS"));
        assert_eq!(
            error_of(create_backend(
                Some(&kms(STORAGE_VENDOR_NAME_AZURE)),
                &failing
            )),
            "not implemented Azure KMS"
        );
        assert_eq!(
            error_of(create_backend(Some(&kms("oracle")), &failing)),
            "vendor not found: oracle"
        );

        // With a working factory both cloud vendors produce a KMS backend.
        assert!(create_backend(Some(&kms(STORAGE_VENDOR_NAME_AWS)), &working).is_ok());
        assert!(create_backend(Some(&kms(STORAGE_VENDOR_NAME_GCP)), &working).is_ok());
    }

    /// Not in the Go package's test set: the file backend reached through
    /// `MultiMasterKeyBackend`, which is how BR actually wires this package up.
    #[test]
    fn test_multi_backend_over_file_config() {
        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        file.write_all(b"c3d99825f2181f4808acd2068eac7441a65bd428f14d2aab43fefc0129091139\n")
            .expect("write");
        file.flush().expect("flush");
        let config = MasterKey::File {
            path: file.path().to_string_lossy().into_owned(),
        };

        let factory = no_kms();
        let multi = MultiMasterKeyBackend::new(std::slice::from_ref(&config), &factory).unwrap();
        let sealed = create_file_backend(file.path())
            .unwrap()
            .encrypt(b"secret")
            .unwrap();
        assert_eq!(multi.decrypt(&sealed).unwrap(), b"secret");

        // A content sealed under a different master key fails every member.
        let other = MemAesGcmBackend::new(&[8u8; 32]).unwrap();
        let foreign = other
            .encrypt_content(b"secret", &Iv::new_gcm().unwrap())
            .unwrap();
        let error = multi.decrypt(&foreign).unwrap_err();
        assert!(
            error.contains("failed to decrypt in multi master key backend"),
            "{error}"
        );
        multi.close();
    }
}
