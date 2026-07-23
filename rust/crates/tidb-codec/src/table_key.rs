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

use tidb_datatype::Datum;

use crate::bytes::decode_bytes;
use crate::datum::{decode_one, encode_key, INT_FLAG};
use crate::number::{decode_int, decode_uint, encode_int};
use crate::CodecError;

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
/// Encoded byte length of `t{table_id}`, used as the table split key.
pub const TABLE_SPLIT_KEY_LEN: usize = 1 + ID_LEN;
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

impl TableKeyError {
    /// MySQL error number used by Go's `dbterror.ClassXEval` mapping.
    #[must_use]
    pub const fn mysql_error_code(&self) -> u16 {
        match self {
            Self::InvalidKey => 8221,
            Self::InvalidRecordKey => 8045,
            Self::InvalidIndexKey => 8222,
            Self::InvalidMetaKey => 1105,
        }
    }
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

/// Wire-level row handle decoded from a table record key.
///
/// Runtime SQL/KV code uses `tidb_txnkv::Handle`; this closed representation
/// keeps dependency-leaf key diagnostics independent of the transaction crate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecordHandle {
    /// Signed integer row handle.
    Int(i64),
    /// Canonically encoded sequence of Datum values forming a common handle.
    Common(Vec<u8>),
    /// Physical partition ID paired with its underlying row handle.
    Partition {
        /// Physical partition identifier.
        partition_id: i64,
        /// Underlying integer or common handle.
        handle: Box<RecordHandle>,
    },
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
            Self::Partition {
                partition_id,
                handle,
            } => write!(f, "partition:{partition_id},{handle}"),
        }
    }
}

impl RecordHandle {
    /// Constructs a partition-aware handle.
    #[must_use]
    pub fn partition(partition_id: i64, handle: Self) -> Self {
        Self::Partition {
            partition_id,
            handle: Box::new(handle),
        }
    }

    /// Returns whether the underlying handle is an integer.
    #[must_use]
    pub const fn is_int(&self) -> bool {
        match self {
            Self::Int(_) => true,
            Self::Common(_) => false,
            Self::Partition { handle, .. } => handle.is_int(),
        }
    }

    /// Returns the underlying integer value.
    #[must_use]
    pub const fn int_value(&self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(*value),
            Self::Common(_) => None,
            Self::Partition { handle, .. } => handle.int_value(),
        }
    }

    /// Returns the physical partition ID when present.
    #[must_use]
    pub const fn partition_id(&self) -> Option<i64> {
        match self {
            Self::Partition { partition_id, .. } => Some(*partition_id),
            _ => None,
        }
    }

    /// Returns the underlying non-partition handle.
    #[must_use]
    pub const fn inner(&self) -> &Self {
        match self {
            Self::Partition { handle, .. } => handle.inner(),
            other => other,
        }
    }

    /// Returns the source handle's persisted encoding.
    #[must_use]
    pub fn encoded(&self) -> Vec<u8> {
        encode_handle(self)
    }

    /// Returns encoded component columns for a common handle.
    pub fn encoded_columns(&self) -> Result<Vec<Vec<u8>>, TableKeyError> {
        match self.inner() {
            Self::Int(value) => {
                let mut encoded = Vec::with_capacity(ID_LEN + 1);
                encoded.push(INT_FLAG);
                encode_int(&mut encoded, *value);
                Ok(vec![encoded])
            }
            Self::Common(encoded) => {
                let mut columns = Vec::new();
                let mut remaining = encoded.as_slice();
                while !remaining.is_empty() {
                    let (column, tail) =
                        crate::cut_one(remaining).map_err(|_| TableKeyError::InvalidRecordKey)?;
                    columns.push(column.to_vec());
                    remaining = tail;
                }
                Ok(columns)
            }
            Self::Partition { .. } => unreachable!("inner removes partition wrappers"),
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

/// Returns the exact integer-handle key range for one physical table.
///
/// This is the direct equivalent of Go's `GetTableHandleKeyRange`: the lower
/// and upper keys are the table's record prefix followed by the
/// memcomparable encodings of `math.MinInt64` and `math.MaxInt64`.
#[must_use]
pub fn get_table_handle_key_range(table_id: i64) -> (Vec<u8>, Vec<u8>) {
    (
        encode_row_key_with_handle(table_id, &RecordHandle::Int(i64::MIN)),
        encode_row_key_with_handle(table_id, &RecordHandle::Int(i64::MAX)),
    )
}

/// Appends a source handle to an already encoded table-record prefix.
#[must_use]
pub fn encode_record_key(record_prefix: &[u8], handle: &RecordHandle) -> Vec<u8> {
    let record_prefix = match handle {
        RecordHandle::Partition { partition_id, .. } => {
            return encode_row_key(*partition_id, &encode_handle(handle));
        }
        _ => record_prefix,
    };
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

/// Extracts the index ID without validating the rest of the index key.
pub fn decode_index_id(key: &[u8]) -> Result<i64, TableKeyError> {
    let bytes = key
        .get(PREFIX_LEN..)
        .ok_or(TableKeyError::InvalidIndexKey)?;
    decode_int(bytes)
        .map(|(_, index_id)| index_id)
        .map_err(|_| TableKeyError::InvalidIndexKey)
}

/// Encodes one entry key of a non-unique secondary index over an integer handle.
///
/// Ported from Go `tablecodec.GenIndexKey`: the index prefix and id, then the
/// memcomparable key encoding of the indexed column values, then — because a
/// non-unique index must keep colliding values distinct — the row handle. Go
/// appends `codec.IntHandleFlag` (which is exactly the memcomparable int key
/// flag, [`INT_FLAG`]) followed by the memcomparable handle, so the suffix is
/// the key encoding of the handle datum. A unique index (whose handle lives in
/// the value, not the key) and a non-integer clustered handle are deliberately
/// not represented here.
pub fn encode_non_unique_index_key(
    table_id: i64,
    index_id: i64,
    indexed_values: &[Datum],
    handle: i64,
) -> Result<Vec<u8>, CodecError> {
    let mut encoded = encode_key(indexed_values)?;
    encoded.push(INT_FLAG);
    encode_int(&mut encoded, handle);
    Ok(encode_index_seek_key(table_id, index_id, &encoded))
}

/// The value byte of a non-unique secondary index entry over an integer handle.
///
/// Ported from Go `tablecodec.genIndexValueVersion0`: for a non-unique index
/// (`distinct == false`) over an integer handle, with no restored collation
/// data, no global-index partition id, and a touched write, every branch that
/// would grow the value is skipped, so it falls to the legacy encoding and emits
/// a single `'0'` byte — the handle lives in the key, not the value. Confirmed
/// byte-exact against real `GenIndexValuePortal`.
#[must_use]
pub fn non_unique_index_value() -> Vec<u8> {
    vec![b'0']
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
    // Rust `tidb-rs` is a standalone SQL node, so API V2 transaction keys are
    // always decoded at this boundary. Go makes the same removal through
    // `rowcodec.RemoveKeyspacePrefix` when next-generation keyspace mode or
    // test mode is active.
    let key = if key.first() == Some(&b'x') && key.len() >= 4 {
        &key[4..]
    } else {
        key
    };
    if key.len() < RECORD_ROW_KEY_LEN
        || key.first() != Some(&b't')
        || key.get(9..11) != Some(RECORD_PREFIX)
    {
        return Err(TableKeyError::InvalidKey);
    }
    let encoded = &key[PREFIX_LEN..];
    if encoded.len() == ID_LEN {
        let (remaining, handle) = decode_int(encoded).map_err(|_| TableKeyError::InvalidKey)?;
        if remaining.is_empty() {
            return Ok(RecordHandle::Int(handle));
        }
    }
    validate_common_handle(encoded).map_err(|_| TableKeyError::InvalidKey)?;
    Ok(RecordHandle::Common(encoded.to_vec()))
}

/// Decodes a table index key into table/index IDs and SQL-rendered values.
pub fn decode_index_key(key: &[u8]) -> Result<(i64, i64, Vec<String>), TableKeyError> {
    let KeyHead::Index { table_id, index_id } =
        decode_key_head(key).map_err(|_| TableKeyError::InvalidIndexKey)?
    else {
        return Err(TableKeyError::InvalidIndexKey);
    };
    let values = decode_values_bytes_to_strings(&key[PREFIX_LEN + ID_LEN..])?;
    Ok((table_id, index_id, values))
}

/// Decodes a complete datum-key stream into source SQL strings.
pub fn decode_values_bytes_to_strings(mut values: &[u8]) -> Result<Vec<String>, TableKeyError> {
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
    Ok(decoded)
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

/// Encodes a structure hash-data metadata key and field.
#[must_use]
pub fn encode_meta_key(key: &[u8], field: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(
        META_PREFIX.len()
            + crate::encoded_bytes_len(key.len())
            + ID_LEN
            + crate::encoded_bytes_len(field.len()),
    );
    encoded.extend_from_slice(META_PREFIX);
    crate::encode_bytes(&mut encoded, key);
    crate::encode_uint(&mut encoded, HASH_DATA_FLAG);
    crate::encode_bytes(&mut encoded, field);
    encoded
}

/// Encodes the prefix shared by every field of one structure hash-data key.
#[must_use]
pub fn encode_meta_key_prefix(key: &[u8]) -> Vec<u8> {
    let mut encoded =
        Vec::with_capacity(META_PREFIX.len() + crate::encoded_bytes_len(key.len()) + ID_LEN);
    encoded.extend_from_slice(META_PREFIX);
    crate::encode_bytes(&mut encoded, key);
    crate::encode_uint(&mut encoded, HASH_DATA_FLAG);
    encoded
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
        RecordHandle::Partition { handle, .. } => encode_handle(handle),
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
