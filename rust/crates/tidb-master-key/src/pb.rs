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

//! boundary: `github.com/pingcap/kvproto/pkg/encryptionpb`, narrowed to the
//! three flat messages this package reads and writes. These are wire types
//! shared with TiKV; the field names and semantics are fixed by that contract,
//! but nothing here needs the generated protobuf codec.

use std::collections::BTreeMap;

/// `encryptionpb.EncryptedContent`: ciphertext plus the metadata map that
/// carries the method, IV, GCM tag and (for KMS backends) the vendor and
/// encrypted data key.
///
/// Go's field is `map[string][]byte`, whose iteration order is randomized;
/// nothing here iterates it, and a `BTreeMap` keeps debug output stable.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EncryptedContent {
    /// Go `Metadata`.
    pub metadata: BTreeMap<String, Vec<u8>>,
    /// Go `Content`: the ciphertext, with the GCM tag held separately in
    /// `metadata` rather than appended.
    pub content: Vec<u8>,
}

/// `encryptionpb.MasterKeyKms`: how to reach a cloud KMS.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MasterKeyKms {
    /// Go `Vendor`, one of the `STORAGE_VENDOR_NAME_*` constants.
    pub vendor: String,
    /// Go `KeyId`.
    pub key_id: String,
    /// Go `Region`.
    pub region: String,
    /// Go `Endpoint`.
    pub endpoint: String,
}

/// `encryptionpb.MasterKey`, whose `Backend` is a protobuf `oneof`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MasterKey {
    /// Go `MasterKey_Plaintext`.
    Plaintext,
    /// Go `MasterKey_File`, carrying `MasterKeyFile.Path`.
    File {
        /// Path of the hex-encoded key file on disk.
        path: String,
    },
    /// Go `MasterKey_Kms`.
    Kms(MasterKeyKms),
}
