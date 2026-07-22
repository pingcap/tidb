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

//! Binary prepared-statement parameter splitting.
//!
//! Ported from Go `parseBinaryParams` (`pkg/server/conn_stmt_params.go`), which
//! is a *length splitter*: for each parameter it derives a byte length from the
//! declared type, slices that many raw bytes out of the `COM_STMT_EXECUTE`
//! value buffer, and tags them with the type / unsigned / NULL flags. It does
//! NOT interpret the bytes into a typed value — that is a separate downstream
//! step (Go `expression.ExecBinaryParam`), so this port needs no temporal or
//! decimal parser. See the HANDOFF risk register for the Unit A / Unit B split.

use crate::{
    TYPE_BIT, TYPE_BLOB, TYPE_DATE, TYPE_DATETIME, TYPE_DOUBLE, TYPE_DURATION, TYPE_ENUM,
    TYPE_FLOAT, TYPE_GEOMETRY, TYPE_INT24, TYPE_LONG, TYPE_LONGLONG, TYPE_LONG_BLOB,
    TYPE_MEDIUM_BLOB, TYPE_NEW_DECIMAL, TYPE_SET, TYPE_SHORT, TYPE_STRING, TYPE_TIMESTAMP,
    TYPE_TINY, TYPE_TINY_BLOB, TYPE_VARCHAR, TYPE_VAR_STRING, TYPE_YEAR,
};

/// MySQL `TypeNull` — a NULL parameter that carries no value bytes.
pub const TYPE_NULL: u8 = 6;
/// MySQL `TypeUnspecified` — decoded like a length-encoded string parameter.
pub const TYPE_UNSPECIFIED: u8 = 0;

/// The MySQL binary-protocol unsigned flag bit in a parameter's type pair.
const UNSIGNED_FLAG: u8 = 0x80;

/// One split binary parameter: the raw value bytes plus the type tags that
/// select how a later stage interprets them.
///
/// This mirrors Go `param.BinaryParam`. `val` holds the raw little-endian /
/// text bytes exactly as they arrived (after the string group's charset decode,
/// which is the identity for the configured node's utf8mb4 input).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinaryParam {
    /// The MySQL field type declared for this parameter (a `TYPE_*` code).
    pub tp: u8,
    /// Whether the parameter's type pair set the unsigned flag bit.
    pub is_unsigned: bool,
    /// Whether the value is SQL `NULL` (a declared `TypeNull` or a
    /// length-encoded NULL marker; the NULL-bitmap path uses `tp == TYPE_NULL`).
    pub is_null: bool,
    /// The raw value bytes sliced from the execute packet.
    pub val: Vec<u8>,
}

/// A rejection from [`parse_binary_params`], mirroring Go's two error returns.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryParamError {
    /// `mysql.ErrMalformPacket`: truncated header, missing length byte, a
    /// length-encoded integer that runs off the end, or a value length larger
    /// than the remaining buffer.
    MalformedPacket,
    /// `errUnknownFieldType`: a declared type this splitter does not model.
    UnknownFieldType {
        /// The unrecognized MySQL field-type code.
        type_code: u8,
    },
}

impl std::fmt::Display for BinaryParamError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MalformedPacket => write!(formatter, "malformed binary parameter packet"),
            Self::UnknownFieldType { type_code } => {
                write!(formatter, "stmt unknown field type {type_code}")
            }
        }
    }
}

impl std::error::Error for BinaryParamError {}

/// Decodes one MySQL length-encoded integer from the front of `bytes`.
///
/// Ported from `util.ParseLengthEncodedInt` (`pkg/server/internal/util`).
/// Returns `(value, is_null, consumed)`. `0xfb` is the NULL marker; `0xfc`,
/// `0xfd`, `0xfe` introduce 2, 3, and 8 little-endian bytes; any other first
/// byte is itself the value. Returns `None` on truncation (Go's `io.EOF`),
/// which every caller maps to [`BinaryParamError::MalformedPacket`].
#[must_use]
pub fn parse_length_encoded_int(bytes: &[u8]) -> Option<(u64, bool, usize)> {
    let first = *bytes.first()?;
    match first {
        0xfb => Some((0, true, 1)),
        0xfc => {
            let b = bytes.get(..3)?;
            Some((u64::from(b[1]) | (u64::from(b[2]) << 8), false, 3))
        }
        0xfd => {
            let b = bytes.get(..4)?;
            Some((
                u64::from(b[1]) | (u64::from(b[2]) << 8) | (u64::from(b[3]) << 16),
                false,
                4,
            ))
        }
        0xfe => {
            let b = bytes.get(..9)?;
            let value = u64::from(b[1])
                | (u64::from(b[2]) << 8)
                | (u64::from(b[3]) << 16)
                | (u64::from(b[4]) << 24)
                | (u64::from(b[5]) << 32)
                | (u64::from(b[6]) << 40)
                | (u64::from(b[7]) << 48)
                | (u64::from(b[8]) << 56);
            Some((value, false, 9))
        }
        // 0-250 are the value; 0xff is undefined and reaches here as its value.
        _ => Some((u64::from(first), false, 1)),
    }
}

/// Slices `length` value bytes starting at `pos`, mirroring Go
/// `takeBinaryParamValue`. Rejects a start past the end or a length larger than
/// the remaining buffer — the same guard that keeps an overflowing
/// length-encoded integer (for example `1 << 63`) from slicing out of bounds.
fn take_binary_param_value(
    param_values: &[u8],
    pos: usize,
    length: u64,
) -> Result<(&[u8], usize), BinaryParamError> {
    if pos > param_values.len() {
        return Err(BinaryParamError::MalformedPacket);
    }
    let remaining = (param_values.len() - pos) as u64;
    if length > remaining {
        return Err(BinaryParamError::MalformedPacket);
    }
    // `length <= remaining <= usize::MAX` here, so the cast cannot truncate.
    let end = pos + length as usize;
    Ok((&param_values[pos..end], end))
}

/// Splits a `COM_STMT_EXECUTE` value buffer into `param_count` raw parameters.
///
/// Ported whole from Go `parseBinaryParams`. For each parameter it, in order:
/// (1) honors a `COM_STMT_SEND_LONG_DATA` bound value (treated as BLOB, or the
/// declared TEXT/BLOB type); (2) applies the NULL bitmap, emitting a
/// `TYPE_NULL` parameter; (3) derives the value length from the declared type —
/// fixed widths for the integer/float family, one leading length byte for the
/// temporal family, and a length-encoded integer for the decimal/blob and
/// string families; (4) slices that many raw bytes.
///
/// `bound_params[i]` is the value delivered earlier via `COM_STMT_SEND_LONG_DATA`
/// for parameter `i`, or `None`. `param_types` is the two-bytes-per-parameter
/// `[type, flags]` vector; `null_bitmap` is the `COM_STMT_EXECUTE` NULL bitmap.
///
/// Charset note: Go decodes the string group through the connection's
/// `InputDecoder`. For the configured node's utf8mb4 input that decoder is a
/// no-op (`charset.FindEncodingTakeUTF8AsNoop`), so the raw bytes pass through
/// unchanged here; a non-utf8 client charset would need a real charset
/// transform, which is deferred (see HANDOFF).
pub fn parse_binary_params(
    param_count: usize,
    bound_params: &[Option<&[u8]>],
    null_bitmap: &[u8],
    param_types: &[u8],
    param_values: &[u8],
) -> Result<Vec<BinaryParam>, BinaryParamError> {
    let mut params = Vec::with_capacity(param_count);
    let mut pos = 0usize;

    for i in 0..param_count {
        // 1. A value already delivered via COM_STMT_SEND_LONG_DATA is used
        //    directly. It defaults to BLOB, refined to the declared TEXT/BLOB
        //    type when the paramTypes vector carries one.
        if let Some(Some(bound)) = bound_params.get(i).copied() {
            let mut tp = TYPE_BLOB;
            let mut val = bound.to_vec();
            if (i << 1) + 1 < param_types.len() {
                let declared = param_types[i << 1];
                match declared {
                    TYPE_VARCHAR | TYPE_VAR_STRING | TYPE_STRING | TYPE_BIT => {
                        tp = declared;
                        val = decode_string_input(val);
                    }
                    TYPE_BLOB | TYPE_TINY_BLOB | TYPE_MEDIUM_BLOB | TYPE_LONG_BLOB => {
                        tp = declared;
                    }
                    _ => {}
                }
            }
            params.push(BinaryParam {
                tp,
                is_unsigned: false,
                is_null: false,
                val,
            });
            continue;
        }

        // 2. The NULL bitmap marks absent arguments. Checked after the bound
        //    check because some clients set the bit even for SEND_LONG_DATA.
        let bitmap_byte = null_bitmap
            .get(i >> 3)
            .copied()
            .ok_or(BinaryParamError::MalformedPacket)?;
        if bitmap_byte & (1 << (i % 8)) != 0 {
            params.push(BinaryParam {
                tp: TYPE_NULL,
                is_unsigned: false,
                is_null: false,
                val: Vec::new(),
            });
            continue;
        }

        if (i << 1) + 1 >= param_types.len() {
            return Err(BinaryParamError::MalformedPacket);
        }
        let tp = param_types[i << 1];
        let is_unsigned = param_types[(i << 1) + 1] & UNSIGNED_FLAG != 0;

        let mut is_null = false;
        let mut decode_with_decoder = false;
        let length: u64 = match tp {
            TYPE_NULL => {
                is_null = true;
                0
            }
            TYPE_TINY => 1,
            TYPE_SHORT | TYPE_YEAR => 2,
            TYPE_INT24 | TYPE_LONG | TYPE_FLOAT => 4,
            TYPE_LONGLONG | TYPE_DOUBLE => 8,
            TYPE_DATE | TYPE_TIMESTAMP | TYPE_DATETIME | TYPE_DURATION => {
                // A temporal value is prefixed by one byte giving its length.
                let byte = param_values
                    .get(pos)
                    .copied()
                    .ok_or(BinaryParamError::MalformedPacket)?;
                pos += 1;
                u64::from(byte)
            }
            TYPE_NEW_DECIMAL | TYPE_BLOB | TYPE_TINY_BLOB | TYPE_MEDIUM_BLOB | TYPE_LONG_BLOB => {
                let tail = param_values
                    .get(pos..)
                    .ok_or(BinaryParamError::MalformedPacket)?;
                let (length, null, consumed) =
                    parse_length_encoded_int(tail).ok_or(BinaryParamError::MalformedPacket)?;
                is_null = null;
                pos += consumed;
                length
            }
            TYPE_UNSPECIFIED | TYPE_VARCHAR | TYPE_VAR_STRING | TYPE_STRING | TYPE_ENUM
            | TYPE_SET | TYPE_GEOMETRY | TYPE_BIT => {
                let tail = param_values
                    .get(pos..)
                    .ok_or(BinaryParamError::MalformedPacket)?;
                let (length, null, consumed) =
                    parse_length_encoded_int(tail).ok_or(BinaryParamError::MalformedPacket)?;
                is_null = null;
                pos += consumed;
                decode_with_decoder = true;
                length
            }
            _ => return Err(BinaryParamError::UnknownFieldType { type_code: tp }),
        };

        let (raw, next_pos) = take_binary_param_value(param_values, pos, length)?;
        let mut val = raw.to_vec();
        if decode_with_decoder {
            val = decode_string_input(val);
        }
        params.push(BinaryParam {
            tp,
            is_unsigned,
            is_null,
            val,
        });
        pos = next_pos;
    }

    Ok(params)
}

/// Applies the string group's `InputDecoder`. The configured node speaks
/// utf8mb4, whose Go decoder is a no-op (`charset.FindEncodingTakeUTF8AsNoop`),
/// so this is the identity. A non-utf8 client charset would transform here;
/// that charset path is deferred (see HANDOFF) — this node asserts utf8 input.
#[inline]
fn decode_string_input(val: Vec<u8>) -> Vec<u8> {
    val
}
