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

use crate::number::{decode_int, encode_int};

const TABLE_PREFIX: u8 = b't';
const TABLE_ID_LEN: usize = 8;
const KIND_PREFIX_LEN: usize = 2;
const KIND_PREFIX_OFFSET: usize = 1 + TABLE_ID_LEN;
const MIN_KEY_LEN: usize = KIND_PREFIX_OFFSET + KIND_PREFIX_LEN;
const RECORD_PREFIX: &[u8; 2] = b"_r";

/// The row/index family identified by a TiDB table-key prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyKind {
    /// The key is too short or does not contain a recognized table-key prefix.
    Unknown,
    /// The key has the `t[table_id]_r` record prefix.
    Row,
    /// The key has the `t[table_id]_i` index prefix.
    Index,
}

/// Classifies a key from only its minimal `t[table_id]_[ri]` prefix.
///
/// The table ID bytes and any bytes after the kind prefix are intentionally
/// opaque. This is the complete observable contract of Go's
/// `rowindexcodec.GetKeyKind`; it does not validate a complete row or index key.
#[must_use]
pub fn get_key_kind(key: &[u8]) -> KeyKind {
    if key.len() < MIN_KEY_LEN || key[0] != TABLE_PREFIX {
        return KeyKind::Unknown;
    }

    match &key[KIND_PREFIX_OFFSET..MIN_KEY_LEN] {
        b"_r" => KeyKind::Row,
        b"_i" => KeyKind::Index,
        _ => KeyKind::Unknown,
    }
}

/// Decodes a table ID from a legacy `t[table_id]...` key.
///
/// The table prefix is followed by TiDB's eight-byte ascending mem-comparable
/// signed integer. API V2 keys carry the four-byte `x + keyspace ID` prefix,
/// which is removed before applying the same decoder.
#[must_use]
pub fn decode_table_id(key: &[u8]) -> i64 {
    let key = if key.first() == Some(&b'x') && key.len() >= 4 {
        &key[4..]
    } else {
        key
    };
    if key.first() != Some(&TABLE_PREFIX) {
        return 0;
    }
    decode_int(&key[1..]).map_or(0, |(_, table_id)| table_id)
}

/// Encodes the canonical `t[table_id]_r` table-record prefix.
///
/// This is Go `tablecodec.GenTableRecordPrefix` and the shared prefix builder
/// used by `EncodeRowKey`. Table IDs use the same ascending mem-comparable
/// signed integer encoding as every other table key.
#[must_use]
pub fn gen_table_record_prefix(table_id: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(MIN_KEY_LEN);
    key.push(TABLE_PREFIX);
    encode_int(&mut key, table_id);
    key.extend_from_slice(RECORD_PREFIX);
    key
}

/// Encodes one table row key from an already encoded handle.
///
/// Handle encoding remains owned by `tidb-txnkv`; this codec layer only
/// prefixes the exact opaque handle bytes, matching Go
/// `tablecodec.EncodeRowKey` without introducing a reverse crate dependency.
#[must_use]
pub fn encode_row_key(table_id: i64, encoded_handle: &[u8]) -> Vec<u8> {
    let mut key = gen_table_record_prefix(table_id);
    key.reserve(encoded_handle.len());
    key.extend_from_slice(encoded_handle);
    key
}
