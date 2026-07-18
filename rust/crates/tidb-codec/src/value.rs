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

//! Framing for TiDB's default (value) row codec.
//!
//! `pkg/util/codec.EncodeValue` stores a row as a concatenation of tagged
//! values. This module owns the tag and byte-boundary contract. Raw payloads
//! remain borrowed until a caller requests the source-proven scalar subset of
//! `tidb_datatype::Datum`; temporal, JSON, vector, and schema-dependent
//! conversions remain outside this byte-boundary leaf. Packed temporal and
//! BinaryJSON payload accessors expose only the source-defined raw fields.

use crate::bytes::{decode_bytes, decode_compact_bytes, peek_bytes_len};
use crate::decimal::decode_decimal;
use crate::decimal::peek_decimal_len;
use crate::float::decode_float;
use crate::json::{decode_json, peek_json_len, RawJson};
use crate::number::{decode_int, decode_uint, decode_uvarint, decode_varint};
use crate::temporal::decode_packed_time;
use crate::CodecError;
use tidb_datatype::{Datum, PackedTime};

/// Go `codec`'s SQL NULL value tag.
pub const VALUE_NIL_FLAG: u8 = 0;
/// Go `codec`'s mem-comparable bytes tag.
pub const VALUE_BYTES_FLAG: u8 = 1;
/// Go `codec`'s compact (length-prefixed) bytes tag.
pub const VALUE_COMPACT_BYTES_FLAG: u8 = 2;
/// Go `codec`'s fixed signed integer tag.
pub const VALUE_INT_FLAG: u8 = 3;
/// Go `codec`'s fixed unsigned integer tag.
pub const VALUE_UINT_FLAG: u8 = 4;
/// Go `codec`'s sortable float tag.
pub const VALUE_FLOAT_FLAG: u8 = 5;
/// Go `codec`'s decimal tag.
pub const VALUE_DECIMAL_FLAG: u8 = 6;
/// Go `codec`'s duration tag.
pub const VALUE_DURATION_FLAG: u8 = 7;
/// Go `codec`'s compact signed integer tag.
pub const VALUE_VARINT_FLAG: u8 = 8;
/// Go `codec`'s compact unsigned integer tag.
pub const VALUE_UVARINT_FLAG: u8 = 9;
/// Go `codec`'s binary JSON tag.
pub const VALUE_JSON_FLAG: u8 = 10;
/// Go `codec`'s vector-float32 tag.
pub const VALUE_VECTOR_FLOAT32_FLAG: u8 = 20;
/// Go `codec`'s max-value sentinel tag.
pub const VALUE_MAX_FLAG: u8 = 250;

/// One source-encoded default-row value with optional typed scalar conversion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawValue<'a> {
    /// The `pkg/util/codec` tag byte.
    pub flag: u8,
    /// The exact bytes after the tag that belong to this value.
    pub payload: &'a [u8],
}

impl<'a> RawValue<'a> {
    /// Decodes the source `codec.DecodeOne` scalar subset into a [`Datum`].
    ///
    /// Go's `DecodeOne` is schema-independent for integer, floating-point,
    /// bytes, and decimal tags.  This method follows those exact wire
    /// decoders and insists that the already-framed payload is consumed in
    /// full. Duration, JSON, and vector values remain outside the currently
    /// ported `tidb_datatype::Datum` domain and return an explicit
    /// unsupported-tag error instead of being reinterpreted. In particular,
    /// Go `DecodeOne` rejects `maxFlag`; only the distinct range decoder may
    /// interpret a terminal `maxFlag` as [`Datum::MaxValue`].
    pub fn decode_datum(self) -> Result<Datum, CodecError> {
        fn finish<T>(result: Result<(&[u8], T), CodecError>) -> Result<T, CodecError> {
            let (remain, value) = result?;
            if remain.is_empty() {
                Ok(value)
            } else {
                Err(CodecError::InvalidEncoding(
                    "value payload has trailing bytes",
                ))
            }
        }

        match self.flag {
            VALUE_NIL_FLAG => {
                if self.payload.is_empty() {
                    Ok(Datum::Null)
                } else {
                    Err(CodecError::InvalidEncoding("NULL value has trailing bytes"))
                }
            }
            VALUE_BYTES_FLAG => finish(decode_bytes(self.payload)).map(Datum::new_bytes),
            VALUE_COMPACT_BYTES_FLAG => finish(decode_compact_bytes(self.payload))
                .map(|value| Datum::new_bytes(value.to_vec())),
            VALUE_INT_FLAG => finish(decode_int(self.payload)).map(Datum::new_int),
            VALUE_UINT_FLAG => finish(decode_uint(self.payload)).map(Datum::new_uint),
            VALUE_FLOAT_FLAG => finish(decode_float(self.payload)).map(Datum::new_real),
            VALUE_DECIMAL_FLAG => {
                finish(decode_decimal(self.payload).map(|(remain, value, _, _)| (remain, value)))
                    .map(Datum::new_decimal)
            }
            VALUE_VARINT_FLAG => finish(decode_varint(self.payload)).map(Datum::new_int),
            VALUE_UVARINT_FLAG => finish(decode_uvarint(self.payload)).map(Datum::new_uint),
            flag => Err(CodecError::UnsupportedValueTag(flag)),
        }
    }

    /// Interprets a `jsonFlag` payload without deserializing its document.
    pub fn json(self) -> Result<RawJson<'a>, CodecError> {
        if self.flag != VALUE_JSON_FLAG {
            return Err(CodecError::InvalidEncoding("JSON value needs json tag"));
        }
        let (remain, value) = decode_json(self.payload)?;
        if !remain.is_empty() {
            return Err(CodecError::InvalidEncoding("JSON value has trailing bytes"));
        }
        Ok(value)
    }

    /// Interprets a `uintFlag` value as Go's packed `Time` payload.
    ///
    /// The field type and timezone remain outside this raw value, matching
    /// `DecodeAsDateTime`'s explicit `tp`/`loc` arguments. Non-`uintFlag`
    /// values are rejected instead of allowing a duration or integer to be
    /// silently reinterpreted as a timestamp.
    pub fn packed_time(self) -> Result<PackedTime, CodecError> {
        if self.flag != VALUE_UINT_FLAG {
            return Err(CodecError::InvalidEncoding(
                "packed temporal value needs uint tag",
            ));
        }
        let (remain, value) = decode_packed_time(self.payload)?;
        if !remain.is_empty() {
            return Err(CodecError::InvalidEncoding(
                "packed temporal value has trailing bytes",
            ));
        }
        Ok(value)
    }
}

/// Decodes one complete `EncodeValue` value and returns the unconsumed suffix.
///
/// The supported tags are those whose byte boundary is self-contained in the
/// source codec. Vector values intentionally return
/// [`CodecError::UnsupportedValueTag`], because their typed payload length
/// belongs to the still-unported vector codec. BinaryJSON now uses the exact
/// source `PeekBytesAsJSON` type/length rules instead of guessing a boundary.
pub fn decode_value(input: &[u8]) -> Result<(&[u8], RawValue<'_>), CodecError> {
    let (&flag, payload) = input
        .split_first()
        .ok_or(CodecError::InvalidEncoding("empty encoded value"))?;
    let payload_len = value_payload_len(flag, payload)?;
    let payload = payload
        .get(..payload_len)
        .ok_or(CodecError::InsufficientBytes)?;
    Ok((&input[1 + payload_len..], RawValue { flag, payload }))
}

/// Decodes one default row containing exactly `column_count` values.
///
/// The returned suffix starts at the next row because default rows have no
/// row-level length prefix. Callers must provide the schema's column count;
/// this function never infers it from payload bytes.
pub fn decode_default_row(
    mut input: &[u8],
    column_count: usize,
) -> Result<(&[u8], Vec<RawValue<'_>>), CodecError> {
    let mut row = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let (remain, value) = decode_value(input)?;
        input = remain;
        row.push(value);
    }
    Ok((input, row))
}

/// Decodes all default rows from a concatenated value stream.
///
/// A zero-column row has no byte representation and therefore cannot delimit
/// a non-empty stream. Rejecting it avoids the infinite-loop ambiguity that a
/// permissive decoder would introduce.
pub fn decode_default_rows(
    mut input: &[u8],
    column_count: usize,
) -> Result<Vec<Vec<RawValue<'_>>>, CodecError> {
    if column_count == 0 {
        return if input.is_empty() {
            Ok(Vec::new())
        } else {
            Err(CodecError::InvalidEncoding("zero-column default row"))
        };
    }

    let mut rows = Vec::new();
    while !input.is_empty() {
        let (remain, row) = decode_default_row(input, column_count)?;
        input = remain;
        rows.push(row);
    }
    Ok(rows)
}

fn value_payload_len(flag: u8, input: &[u8]) -> Result<usize, CodecError> {
    match flag {
        VALUE_NIL_FLAG | VALUE_MAX_FLAG => Ok(0),
        VALUE_BYTES_FLAG => peek_bytes_len(input, false),
        VALUE_COMPACT_BYTES_FLAG => compact_bytes_payload_len(input),
        VALUE_INT_FLAG | VALUE_UINT_FLAG | VALUE_FLOAT_FLAG | VALUE_DURATION_FLAG => Ok(8),
        VALUE_DECIMAL_FLAG => peek_decimal_len(input),
        VALUE_VARINT_FLAG => varint_payload_len(input),
        VALUE_UVARINT_FLAG => uvarint_payload_len(input),
        VALUE_JSON_FLAG => peek_json_len(input),
        VALUE_VECTOR_FLOAT32_FLAG => Err(CodecError::UnsupportedValueTag(flag)),
        _ => Err(CodecError::UnsupportedValueTag(flag)),
    }
}

fn compact_bytes_payload_len(input: &[u8]) -> Result<usize, CodecError> {
    let (remain, declared_len) = decode_varint(input)?;
    let length = usize::try_from(declared_len)
        .map_err(|_| CodecError::InvalidEncoding("negative compact byte length"))?;
    remain.get(..length).ok_or(CodecError::InsufficientBytes)?;
    let prefix_len = input.len() - remain.len();
    Ok(prefix_len + length)
}

fn varint_payload_len(input: &[u8]) -> Result<usize, CodecError> {
    let (remain, _) = decode_varint(input)?;
    Ok(input.len() - remain.len())
}

fn uvarint_payload_len(input: &[u8]) -> Result<usize, CodecError> {
    let (remain, _) = decode_uvarint(input)?;
    Ok(input.len() - remain.len())
}
