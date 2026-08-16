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

//! boundary: Go package `br/pkg/kms`, narrowed to what `master_key` consumes.
//!
//! Ported here: `kms.Provider` (as [`KmsProvider`]), and the key wrappers from
//! `kms/common.go` — `EncryptedKey`, `PlainKey` and `CryptographyType`, which
//! are what enforce the 32-byte AES-256-GCM key length.
//!
//! Not ported: `kms.NewAwsKms` and `kms.NewGcpKms` (`aws.go`, `gcp.go`), which
//! are AWS/GCP SDK clients. They are the only `Provider` implementations in
//! Go, and they belong with whatever crate owns cloud storage credentials.
//! [`super::create_backend`] therefore takes the provider factory as an
//! argument rather than calling them.

/// Go `kms.CryptographyType`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CryptographyType {
    /// Go `CryptographyTypePlain`: no length limitation.
    #[default]
    Plain,
    /// Go `CryptographyTypeAesGcm256`: keys must be exactly 32 bytes.
    AesGcm256,
}

impl CryptographyType {
    /// Go `TargetKeySize`; `0` means "no limitation".
    pub fn target_key_size(self) -> usize {
        match self {
            Self::Plain => 0,
            Self::AesGcm256 => 32,
        }
    }
}

/// Go `kms.EncryptedKey`: a non-empty byte slice marked as an encrypted key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncryptedKey(Vec<u8>);

impl EncryptedKey {
    /// Go `NewEncryptedKey`.
    pub fn new(key: &[u8]) -> Result<EncryptedKey, String> {
        if key.is_empty() {
            return Err("encrypted key cannot be empty".to_owned());
        }
        Ok(EncryptedKey(key.to_vec()))
    }

    /// Go `EncryptedKey.Equal`.
    pub fn equal(&self, other: &EncryptedKey) -> bool {
        self.0 == other.0
    }

    /// The underlying bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

/// Go `kms.PlainKey`: a byte slice marked as a plaintext key of a given type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlainKey {
    tag: CryptographyType,
    key: Vec<u8>,
}

impl PlainKey {
    /// Go `NewPlainKey`: rejects a key whose length does not match the type.
    pub fn new(key: &[u8], tag: CryptographyType) -> Result<PlainKey, String> {
        let limitation = tag.target_key_size();
        if limitation > 0 && key.len() != limitation {
            return Err(format!(
                "encryption method and key length mismatch, expect {limitation} got {}",
                key.len()
            ));
        }
        Ok(PlainKey {
            tag,
            key: key.to_vec(),
        })
    }

    /// Go `PlainKey.KeyTag`.
    pub fn key_tag(&self) -> CryptographyType {
        self.tag
    }

    /// Go `PlainKey.Key`.
    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

/// Go `kms.Provider`: a key management service that can turn an encrypted data
/// key back into a plaintext one.
///
/// Go's `DecryptDataKey` takes a `context.Context` for cancellation; there is
/// no context type at this layer of the Rust workspace, and the only caller
/// (`KmsBackend::decrypt`) uses it solely to abort the retry loop, so it is
/// dropped.
pub trait KmsProvider: Send + Sync {
    /// Go `DecryptDataKey`.
    fn decrypt_data_key(&self, encrypted_key: &EncryptedKey) -> Result<Vec<u8>, String>;
    /// Go `Name`: the vendor name recorded in the encrypted content's metadata.
    fn name(&self) -> String;
    /// Go `Close`.
    fn close(&self);
}
