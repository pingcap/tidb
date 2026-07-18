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

//! Boundary decoding for Go `types.BinaryJSON` values.
//!
//! This module deliberately does not deserialize JSON objects or apply SQL
//! JSON semantics.  It owns the source-defined `type + value` byte boundary
//! so a following default-row value cannot be mistaken for part of a JSON
//! document.  The returned [`RawJson`] keeps the type code and value bytes
//! borrowed from the input, just as Go's `BinaryJSON` does after
//! `PeekBytesAsJSON`.

use crate::CodecError;
use tidb_datatype::PackedTime;

/// Go `types.JSONTypeCodeObject`.
pub const JSON_TYPE_CODE_OBJECT: u8 = 0x01;
/// Go `types.JSONTypeCodeArray`.
pub const JSON_TYPE_CODE_ARRAY: u8 = 0x03;
/// Go `types.JSONTypeCodeLiteral`.
pub const JSON_TYPE_CODE_LITERAL: u8 = 0x04;
/// Go `types.JSONTypeCode_INT64`.
pub const JSON_TYPE_CODE_INT64: u8 = 0x09;
/// Go `types.JSONTypeCodeUint64`.
pub const JSON_TYPE_CODE_UINT64: u8 = 0x0a;
/// Go `types.JSONTypeCodeFloat64`.
pub const JSON_TYPE_CODE_FLOAT64: u8 = 0x0b;
/// Go `types.JSONTypeCodeString`.
pub const JSON_TYPE_CODE_STRING: u8 = 0x0c;
/// Go `types.JSONTypeCodeOpaque`.
pub const JSON_TYPE_CODE_OPAQUE: u8 = 0x0d;
/// Go `types.JSONTypeCodeDate`.
pub const JSON_TYPE_CODE_DATE: u8 = 0x0e;
/// Go `types.JSONTypeCodeDatetime`.
pub const JSON_TYPE_CODE_DATETIME: u8 = 0x0f;
/// Go `types.JSONTypeCodeTimestamp`.
pub const JSON_TYPE_CODE_TIMESTAMP: u8 = 0x10;
/// Go `types.JSONTypeCodeDuration`.
pub const JSON_TYPE_CODE_DURATION: u8 = 0x11;

/// The temporal type selected by Go `BinaryJSON.GetTimeWithFsp`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawJsonTemporalKind {
    /// Go `JSONTypeCodeDate` / MySQL `DATE`.
    Date,
    /// Go `JSONTypeCodeDatetime` / MySQL `DATETIME`.
    Datetime,
    /// Go `JSONTypeCodeTimestamp` / MySQL `TIMESTAMP`.
    Timestamp,
}

/// A BinaryJSON temporal value with its physical packed calendar payload.
///
/// The JSON value carries the type code and eight packed calendar bytes, but
/// no FSP or timezone. Those fields remain caller-owned exactly as in
/// `GetTimeWithFsp`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawJsonTemporal {
    kind: RawJsonTemporalKind,
    packed: PackedTime,
}

impl RawJsonTemporal {
    /// Returns the source JSON temporal type.
    pub const fn kind(self) -> RawJsonTemporalKind {
        self.kind
    }

    /// Returns the packed calendar bits without validating calendar fields.
    pub const fn packed_time(self) -> PackedTime {
        self.packed
    }
}

/// One source-encoded BinaryJSON document without deserializing its value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawJson<'a> {
    /// Go's one-byte `JSONTypeCode`.
    type_code: u8,
    /// The exact bytes following the type code in the BinaryJSON payload.
    value: &'a [u8],
}

impl<'a> RawJson<'a> {
    /// Returns Go's one-byte `JSONTypeCode`.
    pub const fn type_code(self) -> u8 {
        self.type_code
    }

    /// Returns the unchanged bytes following the type code.
    pub const fn value(self) -> &'a [u8] {
        self.value
    }

    /// Decodes Go's date/datetime/timestamp payload without SQL conversion.
    ///
    /// `BinaryJSON.GetTimeWithFsp` reads the value as one little-endian uint64
    /// and chooses the field type from the JSON type code. The payload does
    /// not carry fractional precision or timezone metadata; this method
    /// preserves that omission and leaves calendar validation/formatting to a
    /// typed temporal caller.
    pub fn temporal(self) -> Result<RawJsonTemporal, CodecError> {
        let kind = match self.type_code {
            JSON_TYPE_CODE_DATE => RawJsonTemporalKind::Date,
            JSON_TYPE_CODE_DATETIME => RawJsonTemporalKind::Datetime,
            JSON_TYPE_CODE_TIMESTAMP => RawJsonTemporalKind::Timestamp,
            _ => {
                return Err(CodecError::InvalidEncoding(
                    "JSON payload is not a temporal value",
                ))
            }
        };
        let raw = self
            .value
            .get(..8)
            .ok_or(CodecError::InsufficientBytes)?
            .try_into()
            .map(u64::from_le_bytes)
            .expect("slice length was checked");
        if self.value.len() != 8 {
            return Err(CodecError::InvalidEncoding(
                "JSON temporal value has trailing bytes",
            ));
        }
        Ok(RawJsonTemporal {
            kind,
            packed: PackedTime::from_raw(raw),
        })
    }

    /// Decodes Go's JSON duration payload as `(nanoseconds, fsp)`.
    ///
    /// No SQL duration range, rounding, or statement-warning policy is
    /// applied here.  Those belong to the typed expression/session layer.
    pub fn duration(self) -> Result<(i64, u32), CodecError> {
        if self.type_code != JSON_TYPE_CODE_DURATION {
            return Err(CodecError::InvalidEncoding(
                "JSON payload is not a duration",
            ));
        }
        let nanos = self
            .value
            .get(..8)
            .ok_or(CodecError::InsufficientBytes)?
            .try_into()
            .map(i64::from_le_bytes)
            .expect("slice length was checked");
        let fsp = self
            .value
            .get(8..12)
            .ok_or(CodecError::InsufficientBytes)?
            .try_into()
            .map(u32::from_le_bytes)
            .expect("slice length was checked");
        if self.value.len() != 12 {
            return Err(CodecError::InvalidEncoding(
                "JSON duration has trailing bytes",
            ));
        }
        Ok((nanos, fsp))
    }
}

/// Returns the complete BinaryJSON payload length, including its type byte.
pub fn peek_json_len(input: &[u8]) -> Result<usize, CodecError> {
    let &type_code = input.first().ok_or(CodecError::InsufficientBytes)?;
    let length = match type_code {
        JSON_TYPE_CODE_OBJECT | JSON_TYPE_CODE_ARRAY => {
            // `BinaryJSON.Value` stores element-count and total-size headers;
            // the total size includes those eight bytes but excludes type.
            let header = input.get(1..9).ok_or(CodecError::InsufficientBytes)?;
            let size = u32::from_le_bytes(
                header[4..8]
                    .try_into()
                    .expect("fixed-size JSON header slice"),
            ) as usize;
            if size < 8 {
                return Err(CodecError::InvalidEncoding(
                    "JSON container size is smaller than its header",
                ));
            }
            1 + size
        }
        JSON_TYPE_CODE_STRING => {
            let (prefix_len, value_len) = decode_json_uvarint(input.get(1..).unwrap_or_default())?;
            1 + prefix_len + value_len
        }
        JSON_TYPE_CODE_INT64
        | JSON_TYPE_CODE_UINT64
        | JSON_TYPE_CODE_FLOAT64
        | JSON_TYPE_CODE_DATE
        | JSON_TYPE_CODE_DATETIME
        | JSON_TYPE_CODE_TIMESTAMP => 1 + 8,
        JSON_TYPE_CODE_LITERAL => 1 + 1,
        JSON_TYPE_CODE_OPAQUE => {
            let (prefix_len, value_len) =
                decode_json_uvarint(input.get(2..).ok_or(CodecError::InsufficientBytes)?)?;
            1 + 1 + prefix_len + value_len
        }
        JSON_TYPE_CODE_DURATION => 1 + 12,
        _ => return Err(CodecError::InvalidEncoding("unknown JSON type code")),
    };
    if length > input.len() {
        return Err(CodecError::InsufficientBytes);
    }
    Ok(length)
}

/// Decodes one complete BinaryJSON payload and returns the unconsumed suffix.
pub fn decode_json(input: &[u8]) -> Result<(&[u8], RawJson<'_>), CodecError> {
    let length = peek_json_len(input)?;
    let (&type_code, value) = input
        .get(..length)
        .ok_or(CodecError::InsufficientBytes)?
        .split_first()
        .expect("JSON length includes type code");
    Ok((&input[length..], RawJson { type_code, value }))
}

fn decode_json_uvarint(input: &[u8]) -> Result<(usize, usize), CodecError> {
    let mut value = 0_usize;
    for (index, byte) in input.iter().copied().enumerate() {
        if index >= std::mem::size_of::<usize>() || index >= 9 {
            return Err(CodecError::InvalidEncoding("JSON length varint overflow"));
        }
        value |= usize::from(byte & 0x7f) << (7 * index);
        if byte < 0x80 {
            return Ok((index + 1, value));
        }
    }
    Err(CodecError::InsufficientBytes)
}
