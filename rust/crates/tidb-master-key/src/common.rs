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

//! Go `br/pkg/encryption/master_key/common.go`: the metadata keys shared with
//! TiKV's own encryption implementation, and the IV wrapper.

/// Go `MetadataKeyMethod`. Must stay identical to TiKV's constant.
pub const METADATA_KEY_METHOD: &str = "method";
/// Go `MetadataKeyIv`.
pub const METADATA_KEY_IV: &str = "iv";
/// Go `MetadataKeyAesGcmTag`.
pub const METADATA_KEY_AES_GCM_TAG: &str = "aes_gcm_tag";
/// Go `MetadataKeyKmsVendor`.
pub const METADATA_KEY_KMS_VENDOR: &str = "kms_vendor";
/// Go `MetadataKeyKmsCiphertextKey`.
pub const METADATA_KEY_KMS_CIPHERTEXT_KEY: &str = "kms_ciphertext_key";
/// Go `MetadataMethodAes256Gcm`.
pub const METADATA_METHOD_AES256_GCM: &str = "aes256-gcm";

/// Go `GcmIv12`.
pub const GCM_IV_12: usize = 12;
/// Go `CtrIv16`.
pub const CTR_IV_16: usize = 16;

/// Go `IvType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IvType {
    /// Go `IvTypeGcm`: a 12-byte GCM nonce.
    Gcm,
    /// Go `IvTypeCtr`: a 16-byte CTR IV.
    Ctr,
}

/// Go `IV`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Iv {
    /// Go `IV.Type`.
    pub iv_type: IvType,
    /// Go `IV.Data`.
    pub data: Vec<u8>,
}

impl Iv {
    /// Go `NewIVGcm`: 12 random bytes from the OS CSPRNG (`crypto/rand.Read`).
    pub fn new_gcm() -> Result<Iv, String> {
        let mut data = vec![0u8; GCM_IV_12];
        getrandom::fill(&mut data).map_err(|error| error.to_string())?;
        Ok(Iv {
            iv_type: IvType::Gcm,
            data,
        })
    }

    /// Go `NewIVFromSlice`: the length decides the type.
    pub fn from_slice(src: &[u8]) -> Result<Iv, String> {
        match src.len() {
            CTR_IV_16 => Ok(Iv {
                iv_type: IvType::Ctr,
                data: src.to_vec(),
            }),
            GCM_IV_12 => Ok(Iv {
                iv_type: IvType::Gcm,
                data: src.to_vec(),
            }),
            other => Err(format!(
                "invalid IV length, must be 12 or 16 bytes, got {other}"
            )),
        }
    }

    /// Go `IV.AsSlice`.
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Not in the Go package's test set: `common.go` is entirely untested
    /// upstream, but the IV lengths are a wire contract with TiKV.
    #[test]
    fn test_iv_construction() {
        let gcm = Iv::new_gcm().unwrap();
        assert_eq!(gcm.iv_type, IvType::Gcm);
        assert_eq!(gcm.as_slice().len(), GCM_IV_12);

        // Two draws must differ; a constant IV would be catastrophic for GCM.
        assert_ne!(Iv::new_gcm().unwrap().data, Iv::new_gcm().unwrap().data);

        let from_gcm = Iv::from_slice(&[1u8; 12]).unwrap();
        assert_eq!(from_gcm.iv_type, IvType::Gcm);
        assert_eq!(from_gcm.as_slice(), &[1u8; 12]);

        let from_ctr = Iv::from_slice(&[2u8; 16]).unwrap();
        assert_eq!(from_ctr.iv_type, IvType::Ctr);
        assert_eq!(from_ctr.as_slice(), &[2u8; 16]);

        for length in [0usize, 8, 11, 13, 15, 17, 32] {
            assert_eq!(
                Iv::from_slice(&vec![0u8; length]).unwrap_err(),
                format!("invalid IV length, must be 12 or 16 bytes, got {length}")
            );
        }
    }
}
