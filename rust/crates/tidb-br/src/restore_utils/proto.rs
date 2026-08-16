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

//! The two kvproto messages `br/pkg/restore/utils` handles as flat structs.
//!
//! boundary: `import_sstpb.RewriteRule` and `brpb.File` are generated protobuf
//! types. Nothing in this package serializes them — the rules are built,
//! matched by prefix, and handed to the SST importer; the files are grouped by
//! key and summed. Declaring the fields that this package (and `br/pkg/rtree`,
//! through [`crate::rtree::RangeFile`]) actually touches keeps the whole crate
//! free of a protobuf runtime.

use crate::rtree::RangeFile;

/// boundary: `import_sstpb.RewriteRule`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RewriteRule {
    /// The key prefix as it appears in the backup.
    pub old_key_prefix: Vec<u8>,
    /// The key prefix the restore target uses.
    pub new_key_prefix: Vec<u8>,
    /// The commit timestamp rewritten onto imported keys.
    pub new_timestamp: u64,
    /// Drop keys committed at or before this timestamp.
    pub ignore_before_timestamp: u64,
    /// Drop keys committed after this timestamp.
    pub ignore_after_timestamp: u64,
}

impl RewriteRule {
    /// Go's generated `(*RewriteRule).GetOldKeyPrefix`, which yields the zero
    /// value for a `nil` receiver.
    #[must_use]
    pub fn get_old_key_prefix(rule: Option<&Self>) -> &[u8] {
        rule.map_or(&[], |rule| rule.old_key_prefix.as_slice())
    }

    /// Go's generated `(*RewriteRule).GetNewKeyPrefix`.
    #[must_use]
    pub fn get_new_key_prefix(rule: Option<&Self>) -> &[u8] {
        rule.map_or(&[], |rule| rule.new_key_prefix.as_slice())
    }
}

impl std::fmt::Display for RewriteRule {
    /// The `zap.Stringer` rendering Go's `ValidateFileRewriteRule` logs.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "old_key_prefix:{:X?} new_key_prefix:{:X?} new_timestamp:{}",
            self.old_key_prefix, self.new_key_prefix, self.new_timestamp
        )
    }
}

/// boundary: `brpb.File`, the SST/KV file descriptor a backup metafile carries.
///
/// The fields below are those `br/pkg/restore/utils` and `br/pkg/rtree` read;
/// the message additionally carries a SHA-256 digest, CF-specific size hints,
/// and the encryption metadata, none of which this subject touches.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct File {
    /// The object name in external storage.
    pub name: String,
    /// The file's inclusive lower key bound.
    pub start_key: Vec<u8>,
    /// The file's exclusive upper key bound.
    pub end_key: Vec<u8>,
    /// The XOR of the per-KV CRC64 values in this file.
    pub crc64_xor: u64,
    /// The number of key-value pairs.
    pub total_kvs: u64,
    /// The encoded size in bytes.
    pub total_bytes: u64,
    /// The RocksDB column family (`write` / `default`).
    pub cf: String,
}

impl File {
    /// Go's generated `(*File).GetName`.
    #[must_use]
    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// Go's generated `(*File).GetStartKey`.
    #[must_use]
    pub fn get_start_key(&self) -> &[u8] {
        &self.start_key
    }

    /// Go's generated `(*File).GetEndKey`.
    #[must_use]
    pub fn get_end_key(&self) -> &[u8] {
        &self.end_key
    }
}

impl RangeFile for File {
    fn total_kvs(&self) -> u64 {
        self.total_kvs
    }

    fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    fn crc64_xor(&self) -> u64 {
        self.crc64_xor
    }
}
