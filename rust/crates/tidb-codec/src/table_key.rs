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

//! Canonical TiDB table, record, index, and meta-key codec.

use std::fmt;

use crate::bytes::decode_bytes;
use crate::datum::decode_one;
use crate::number::{decode_int, decode_uint, encode_int};

// Preserve the already-connected DistSQL/resource-group authority as the
// implementation of the three table-row primitives instead of cloning it.
pub use crate::row_index::{decode_table_id, encode_row_key, gen_table_record_prefix};

/// Leading byte of every encoded table record or index key.
pub const TABLE_PREFIX: &[u8] = b"t";
/// Leading byte of every encoded TiDB metadata key.
pub const META_PREFIX: &[u8] = b"m";
const RECORD_PREFIX: &[u8] = b"_r";
const INDEX_PREFIX: &[u8] = b"_i";
const ID_LEN: usize = 8;
const PREFIX_LEN: usize = 1 + ID_LEN + 2;
/// Encoded byte length of a table record key with an integer handle.
pub const RECORD_ROW_KEY_LEN: usize = PREFIX_LEN + ID_LEN;
const HASH_DATA_FLAG: u64 = b'h' as u64;

/// Structural failure while decoding a TiDB table or metadata key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TableKeyError {
    /// The key is neither a valid table record head nor index head.
    InvalidKey,
    /// The key does not contain a valid table record handle.
    InvalidRecordKey,
    /// The key does not contain a valid table index payload.
    InvalidIndexKey,
    /// The key does not follow TiDB's encoded metadata-key layout.
    InvalidMetaKey,
}

impl fmt::Display for TableKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidKey => "invalid key",
            Self::InvalidRecordKey => "invalid record key",
            Self::InvalidIndexKey => "invalid index key",
            Self::InvalidMetaKey => "invalid meta key",
        })
    }
}

impl std::error::Error for TableKeyError {}

/// Decoded table-key namespace and identifiers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyHead {
    /// A record-key namespace for one physical table.
    Record {
        /// Physical table identifier encoded in the key.
        table_id: i64,
    },
    /// An index-key namespace for one index of a physical table.
    Index {
        /// Physical table identifier encoded in the key.
        table_id: i64,
        /// Index identifier encoded after the index marker.
        index_id: i64,
    },
}

/// Row handle decoded from a table record key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecordHandle {
    /// Signed integer row handle.
    Int(i64),
    /// Canonically encoded sequence of Datum values forming a common handle.
    Common(Vec<u8>),
}

impl fmt::Display for RecordHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(value) => value.fmt(f),
            Self::Common(encoded) => {
                f.write_str("{")?;
                let mut separator = "";
                let mut remaining = encoded.as_slice();
                while !remaining.is_empty() {
                    let (remain, datum) = decode_one(remaining).map_err(|_| fmt::Error)?;
                    f.write_str(separator)?;
                    f.write_str(&datum.sql_string().map_err(|_| fmt::Error)?)?;
                    separator = ", ";
                    remaining = remain;
                }
                f.write_str("}")
            }
        }
    }
}

/// Encodes the `t{table_id}` prefix shared by record and index keys.
#[must_use]
pub fn encode_table_prefix(table_id: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + ID_LEN);
    key.extend_from_slice(TABLE_PREFIX);
    encode_int(&mut key, table_id);
    key
}

/// Generates the canonical table prefix for a physical table identifier.
#[must_use]
pub fn gen_table_prefix(table_id: i64) -> Vec<u8> {
    encode_table_prefix(table_id)
}

/// Generates the `t{table_id}_i` prefix shared by every index of a table.
#[must_use]
pub fn gen_table_index_prefix(table_id: i64) -> Vec<u8> {
    let mut key = encode_table_prefix(table_id);
    key.extend_from_slice(INDEX_PREFIX);
    key
}

/// Encodes a row key from a decoded source handle.
#[must_use]
pub fn encode_row_key_with_handle(table_id: i64, handle: &RecordHandle) -> Vec<u8> {
    encode_row_key(table_id, &encode_handle(handle))
}

/// Appends a source handle to an already encoded table-record prefix.
///
/// Partition-handle prefix substitution remains outside this dependency-closed
/// handle representation and is recorded as a residual.
#[must_use]
pub fn encode_record_key(record_prefix: &[u8], handle: &RecordHandle) -> Vec<u8> {
    let encoded = encode_handle(handle);
    let mut key = Vec::with_capacity(record_prefix.len() + encoded.len());
    key.extend_from_slice(record_prefix);
    key.extend_from_slice(&encoded);
    key
}

/// Encodes the complete prefix identifying one table index.
#[must_use]
pub fn encode_table_index_prefix(table_id: i64, index_id: i64) -> Vec<u8> {
    let mut key = gen_table_index_prefix(table_id);
    encode_int(&mut key, index_id);
    key
}

/// Encodes an index seek key by appending canonical encoded index values.
#[must_use]
pub fn encode_index_seek_key(table_id: i64, index_id: i64, encoded_values: &[u8]) -> Vec<u8> {
    let mut key = encode_table_index_prefix(table_id, index_id);
    key.extend_from_slice(encoded_values);
    key
}

/// Decodes whether a key addresses a table record or index and returns its IDs.
pub fn decode_key_head(key: &[u8]) -> Result<KeyHead, TableKeyError> {
    if !key.starts_with(TABLE_PREFIX) {
        return Err(TableKeyError::InvalidKey);
    }
    let (tail, table_id) = decode_int(&key[1..]).map_err(|_| TableKeyError::InvalidKey)?;
    if tail.starts_with(RECORD_PREFIX) {
        return Ok(KeyHead::Record { table_id });
    }
    let tail = tail
        .strip_prefix(INDEX_PREFIX)
        .ok_or(TableKeyError::InvalidKey)?;
    let (_, index_id) = decode_int(tail).map_err(|_| TableKeyError::InvalidKey)?;
    Ok(KeyHead::Index { table_id, index_id })
}

/// Decodes a table record key into its physical table ID and row handle.
pub fn decode_record_key(key: &[u8]) -> Result<(i64, RecordHandle), TableKeyError> {
    let KeyHead::Record { table_id } =
        decode_key_head(key).map_err(|_| TableKeyError::InvalidRecordKey)?
    else {
        return Err(TableKeyError::InvalidRecordKey);
    };
    if key.len() <= PREFIX_LEN {
        return Err(TableKeyError::InvalidRecordKey);
    }
    let encoded = &key[PREFIX_LEN..];
    if encoded.len() == ID_LEN {
        let (remain, handle) = decode_int(encoded).map_err(|_| TableKeyError::InvalidRecordKey)?;
        if remain.is_empty() {
            return Ok((table_id, RecordHandle::Int(handle)));
        }
    }
    validate_common_handle(encoded)?;
    Ok((table_id, RecordHandle::Common(encoded.to_vec())))
}

/// Decodes only the row handle portion of a table record key.
pub fn decode_row_key(key: &[u8]) -> Result<RecordHandle, TableKeyError> {
    decode_record_key(key).map(|(_, handle)| handle)
}

/// Decodes a table index key into table/index IDs and SQL-rendered values.
pub fn decode_index_key(key: &[u8]) -> Result<(i64, i64, Vec<String>), TableKeyError> {
    let KeyHead::Index { table_id, index_id } =
        decode_key_head(key).map_err(|_| TableKeyError::InvalidIndexKey)?
    else {
        return Err(TableKeyError::InvalidIndexKey);
    };
    let mut values = &key[PREFIX_LEN + ID_LEN..];
    let mut decoded = Vec::new();
    while !values.is_empty() {
        let (remain, datum) = decode_one(values).map_err(|_| TableKeyError::InvalidIndexKey)?;
        decoded.push(
            datum
                .sql_string()
                .map_err(|_| TableKeyError::InvalidIndexKey)?,
        );
        values = remain;
    }
    Ok((table_id, index_id, decoded))
}

/// Decodes a hash-data metadata key into its metadata key and field bytes.
pub fn decode_meta_key(key: &[u8]) -> Result<(Vec<u8>, Vec<u8>), TableKeyError> {
    let tail = key
        .strip_prefix(META_PREFIX)
        .ok_or(TableKeyError::InvalidMetaKey)?;
    let (tail, meta_key) = decode_bytes(tail).map_err(|_| TableKeyError::InvalidMetaKey)?;
    let (tail, flag) = decode_uint(tail).map_err(|_| TableKeyError::InvalidMetaKey)?;
    if flag != HASH_DATA_FLAG {
        return Err(TableKeyError::InvalidMetaKey);
    }
    // Go DecodeMetaKey deliberately ignores bytes after the encoded field.
    let (_, field) = decode_bytes(tail).map_err(|_| TableKeyError::InvalidMetaKey)?;
    Ok((meta_key, field))
}

/// Returns the encoded record-handle bytes after the table record prefix.
#[must_use]
pub fn cut_row_key_prefix(key: &[u8]) -> &[u8] {
    &key[PREFIX_LEN.min(key.len())..]
}

/// Returns the encoded index values after the complete table-index prefix.
#[must_use]
pub fn cut_index_prefix(key: &[u8]) -> &[u8] {
    &(key[(PREFIX_LEN + ID_LEN).min(key.len())..])
}

/// Truncates a byte slice to at most one integer-handle record-key length.
#[must_use]
pub fn truncate_to_row_key_len(key: &[u8]) -> &[u8] {
    &key[..RECORD_ROW_KEY_LEN.min(key.len())]
}

fn validate_common_handle(mut encoded: &[u8]) -> Result<(), TableKeyError> {
    while !encoded.is_empty() {
        let (remain, _) = decode_one(encoded).map_err(|_| TableKeyError::InvalidRecordKey)?;
        encoded = remain;
    }
    Ok(())
}

fn encode_handle(handle: &RecordHandle) -> Vec<u8> {
    match handle {
        RecordHandle::Int(value) => {
            let mut encoded = Vec::with_capacity(ID_LEN);
            encode_int(&mut encoded, *value);
            encoded
        }
        RecordHandle::Common(encoded) => encoded.clone(),
    }
}

/// Renders bytes as lowercase hexadecimal without separators.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(DIGITS[(byte >> 4) as usize] as char);
        value.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    value
}
