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

/// The text-protocol marker for a SQL `NULL` value.
pub const NULL_MARKER: u8 = 0xfb;

/// Appends a MySQL length-encoded integer to `buffer`.
///
/// Values up to 250 use one byte. Larger values use the little-endian `0xfc`,
/// `0xfd`, or `0xfe` forms defined by the MySQL text protocol. The `0xfb`
/// marker is reserved for `NULL` and is therefore never emitted for an
/// integer.
pub fn append_length_encoded_int(buffer: &mut Vec<u8>, value: u64) {
    match value {
        0..=250 => buffer.push(value as u8),
        251..=0xffff => {
            buffer.push(0xfc);
            buffer.extend_from_slice(&(value as u16).to_le_bytes());
        }
        0x1_0000..=0xff_ffff => {
            buffer.push(0xfd);
            buffer.push(value as u8);
            buffer.push((value >> 8) as u8);
            buffer.push((value >> 16) as u8);
        }
        _ => {
            buffer.push(0xfe);
            buffer.extend_from_slice(&value.to_le_bytes());
        }
    }
}

/// Appends an optional byte string in MySQL's length-encoded form.
pub fn append_length_encoded_bytes(buffer: &mut Vec<u8>, value: Option<&[u8]>) {
    match value {
        None => buffer.push(NULL_MARKER),
        Some(bytes) => {
            append_length_encoded_int(buffer, bytes.len() as u64);
            buffer.extend_from_slice(bytes);
        }
    }
}

/// Encodes one text-protocol row from already formatted value bytes.
///
/// Formatting typed TiDB datums (temporal values, decimals, JSON, charset
/// conversion, and floating-point precision) remains owned by the future
/// expression/result layer. This primitive owns only the wire framing around
/// each value, including SQL `NULL`.
pub fn encode_text_row(values: &[Option<&[u8]>]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for value in values {
        append_length_encoded_bytes(&mut encoded, *value);
    }
    encoded
}

/// Returns whether a MySQL field type is serialized as a string-like value in
/// text-protocol metadata.
pub fn is_string_column_type(type_code: u8) -> bool {
    matches!(
        type_code,
        TYPE_STRING
            | TYPE_VAR_STRING
            | TYPE_VARCHAR
            | TYPE_BIT
            | TYPE_TINY_BLOB
            | TYPE_MEDIUM_BLOB
            | TYPE_LONG_BLOB
            | TYPE_BLOB
            | TYPE_ENUM
            | TYPE_SET
            | TYPE_JSON
            | TYPE_TIDB_VECTOR_FLOAT32
    )
}

// Keep these constants local to avoid making the result leaf depend on parser
// metadata before the protocol steward integrates the shared type registry.
const TYPE_BIT: u8 = 16;
const TYPE_VARCHAR: u8 = 15;
const TYPE_VAR_STRING: u8 = 0xfd;
const TYPE_STRING: u8 = 0xfe;
const TYPE_JSON: u8 = 0xf5;
const TYPE_ENUM: u8 = 0xf7;
const TYPE_SET: u8 = 0xf8;
const TYPE_TINY_BLOB: u8 = 0xf9;
const TYPE_MEDIUM_BLOB: u8 = 0xfa;
const TYPE_LONG_BLOB: u8 = 0xfb;
const TYPE_BLOB: u8 = 0xfc;
const TYPE_TIDB_VECTOR_FLOAT32: u8 = 0xe1;
