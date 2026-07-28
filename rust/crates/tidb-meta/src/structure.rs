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

//! `pkg/structure`: the string/hash/list encodings TiDB layers over a raw KV
//! transaction. The catalog lives entirely in the `m` namespace, so every key
//! this module produces starts with [`tidb_codec::table_key::META_PREFIX`].
//!
//! Layout (Go `pkg/structure/type.go`):
//!
//! ```text
//! string data: m + EncodeBytes(key) + EncodeUint('s')
//! hash meta:   m + EncodeBytes(key) + EncodeUint('H')
//! hash data:   m + EncodeBytes(key) + EncodeUint('h') + EncodeBytes(field)
//! list meta:   m + EncodeBytes(key) + EncodeUint('L')
//! list data:   m + EncodeBytes(key) + EncodeUint('l') + EncodeInt(index)
//! ```
//!
//! The hash-data encoders are `tidb-codec`'s existing `encode_meta_key` /
//! `decode_meta_key` / `encode_meta_key_prefix`; this module adds the string
//! and list shapes and re-exports the hash ones under their Go names.

use tidb_codec::table_key::META_PREFIX;
use tidb_codec::{decode_bytes, decode_int, decode_uint, encode_bytes, encode_int, encode_uint};

use crate::error::{MetaError, Result};

/// Go `structure.StringMeta`.
pub const STRING_META: u8 = b'S';
/// Go `structure.StringData`.
pub const STRING_DATA: u8 = b's';
/// Go `structure.HashMeta`.
pub const HASH_META: u8 = b'H';
/// Go `structure.HashData`.
pub const HASH_DATA: u8 = b'h';
/// Go `structure.ListMeta`.
pub const LIST_META: u8 = b'L';
/// Go `structure.ListData`.
pub const LIST_DATA: u8 = b'l';

pub use tidb_codec::table_key::{
    decode_meta_key as decode_hash_data_key, encode_meta_key as encode_hash_data_key,
    encode_meta_key_prefix as encode_hash_data_key_prefix,
};

/// Encodes `m` + `EncodeBytes(key)` + `EncodeUint(flag)`, the shared head of
/// every structure key.
fn encode_head(key: &[u8], flag: u8) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(META_PREFIX.len() + key.len() + 24);
    encoded.extend_from_slice(META_PREFIX);
    encode_bytes(&mut encoded, key);
    encode_uint(&mut encoded, u64::from(flag));
    encoded
}

/// Splits `m` + `EncodeBytes(key)` + `EncodeUint(flag)` back apart, checking
/// the flag. Returns the decoded key and the bytes after the flag.
fn decode_head(encoded: &[u8], expected: u8) -> Result<(Vec<u8>, &[u8])> {
    let tail = encoded
        .strip_prefix(META_PREFIX)
        .ok_or(MetaError::NotMetaKey)?;
    let (tail, key) = decode_bytes(tail).map_err(|_| MetaError::MalformedKey)?;
    let (tail, flag) = decode_uint(tail).map_err(|_| MetaError::MalformedKey)?;
    if flag != u64::from(expected) {
        // Go reports the low byte of the decoded uint as a character.
        #[expect(clippy::cast_possible_truncation, reason = "Go formats %c of the byte")]
        return Err(MetaError::UnexpectedTypeFlag(flag as u8));
    }
    Ok((key, tail))
}

/// Go `TxStructure.EncodeStringDataKey`.
#[must_use]
pub fn encode_string_data_key(key: &[u8]) -> Vec<u8> {
    encode_head(key, STRING_DATA)
}

/// Go `TxStructure.decodeStringDataKey`.
pub fn decode_string_data_key(encoded: &[u8]) -> Result<Vec<u8>> {
    decode_head(encoded, STRING_DATA).map(|(key, _)| key)
}

/// Go `TxStructure.EncodeHashMetaKey` (written by v5.1 and earlier).
#[must_use]
pub fn encode_hash_meta_key(key: &[u8]) -> Vec<u8> {
    encode_head(key, HASH_META)
}

/// Go `TxStructure.encodeListMetaKey`.
#[must_use]
pub fn encode_list_meta_key(key: &[u8]) -> Vec<u8> {
    encode_head(key, LIST_META)
}

/// Go `TxStructure.encodeListDataKey`.
#[must_use]
pub fn encode_list_data_key(key: &[u8], index: i64) -> Vec<u8> {
    let mut encoded = encode_head(key, LIST_DATA);
    encode_int(&mut encoded, index);
    encoded
}

/// Decodes [`encode_list_data_key`] back into its key and index.
pub fn decode_list_data_key(encoded: &[u8]) -> Result<(Vec<u8>, i64)> {
    let (key, tail) = decode_head(encoded, LIST_DATA)?;
    let (_, index) = decode_int(tail).map_err(|_| MetaError::MalformedKey)?;
    Ok((key, index))
}
