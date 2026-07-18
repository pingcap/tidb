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

use crate::bytes::{encode_bytes, peek_bytes_len};
use crate::decimal::{decode_decimal, encode_decimal, peek_decimal_len};
use crate::float::{decode_float, encode_float};
use crate::number::{decode_int, decode_uint, encode_int, encode_uint};
use crate::CodecError;
use tidb_datatype::{Collation, Datum, StringDatum};

/// TiDB's SQL NULL tag.
pub const NIL_FLAG: u8 = 0;
/// TiDB's mem-comparable byte-string tag.
pub const BYTES_FLAG: u8 = 1;
/// TiDB's fixed mem-comparable signed-integer tag.
pub const INT_FLAG: u8 = 3;
/// TiDB's fixed mem-comparable unsigned-integer tag.
pub const UINT_FLAG: u8 = 4;
/// TiDB's fixed mem-comparable floating-point tag.
pub const FLOAT_FLAG: u8 = 5;
/// TiDB's packed mem-comparable decimal tag.
pub const DECIMAL_FLAG: u8 = 6;
/// TiDB's maximum range-bound sentinel tag.
pub const MAX_FLAG: u8 = 250;
/// `PrefixNext(MAX_FLAG)`, accepted by Go `DecodeRange` as `MaxValue`.
const PREFIX_NEXT_MAX_FLAG: u8 = MAX_FLAG + 1;

/// A key encoder with an immutable collation-compatibility mode.
///
/// This is the Rust equivalent of Go codec's `Encoder`. Keeping the mode in
/// the value avoids the process-global switch while still letting a server
/// loaded from an old cluster encode legacy raw string keys exactly.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Encoder {
    use_new_collation: bool,
}

impl Encoder {
    /// Creates an encoder for the cluster's persisted new-collation setting.
    pub const fn new(use_new_collation: bool) -> Self {
        Self { use_new_collation }
    }

    /// Returns the fixed collation mode used by this encoder.
    pub const fn use_new_collation(self) -> bool {
        self.use_new_collation
    }

    /// Encodes the dependency-closed `tidb-datatype` scalar domain as a TiDB key.
    pub fn encode_key(self, values: &[Datum]) -> Result<Vec<u8>, CodecError> {
        let mut output = Vec::new();
        for value in values {
            match value {
                Datum::Null => output.push(NIL_FLAG),
                Datum::MinNotNull => output.push(BYTES_FLAG),
                Datum::MaxValue => output.push(MAX_FLAG),
                Datum::Int(value) => {
                    output.push(INT_FLAG);
                    encode_int(&mut output, *value);
                }
                Datum::UInt(value) => {
                    output.push(UINT_FLAG);
                    encode_uint(&mut output, *value);
                }
                Datum::Decimal(value) => {
                    output.push(DECIMAL_FLAG);
                    encode_decimal(&mut output, value)?;
                }
                Datum::Real(value) => {
                    output.push(FLOAT_FLAG);
                    encode_float(&mut output, *value);
                }
                Datum::String(value) => {
                    output.push(BYTES_FLAG);
                    encode_bytes(&mut output, self.string_key(value));
                }
                Datum::Bytes(value) => {
                    output.push(BYTES_FLAG);
                    encode_bytes(&mut output, value);
                }
            }
        }
        Ok(output)
    }

    fn string_key(self, value: &StringDatum) -> &[u8] {
        let bytes = value.bytes();
        if self.use_new_collation && value.collation() == Collation::Utf8Mb4Bin {
            let end = bytes
                .iter()
                .rposition(|byte| *byte != b' ')
                .map_or(0, |index| index + 1);
            &bytes[..end]
        } else {
            bytes
        }
    }
}

/// Encodes a key with TiDB's default new-collation mode.
///
/// Servers opening a cluster whose persisted setting disables new collations
/// must construct [`Encoder::new(false)`] instead.
pub fn encode_key(values: &[Datum]) -> Result<Vec<u8>, CodecError> {
    Encoder::new(true).encode_key(values)
}

/// Decodes one datum produced by TiDB's key codec.
pub fn decode_one(input: &[u8]) -> Result<(&[u8], Datum), CodecError> {
    let (&flag, payload) = input
        .split_first()
        .ok_or(CodecError::InvalidEncoding("empty key"))?;
    match flag {
        NIL_FLAG => Ok((payload, Datum::Null)),
        INT_FLAG => decode_int(payload).map(|(remain, value)| (remain, Datum::new_int(value))),
        UINT_FLAG => decode_uint(payload).map(|(remain, value)| (remain, Datum::new_uint(value))),
        FLOAT_FLAG => decode_float(payload).map(|(remain, value)| (remain, Datum::new_real(value))),
        BYTES_FLAG => {
            crate::decode_bytes(payload).map(|(remain, value)| (remain, Datum::new_bytes(value)))
        }
        DECIMAL_FLAG => {
            decode_decimal(payload).map(|(remain, value, _, _)| (remain, Datum::new_decimal(value)))
        }
        _ => Err(CodecError::InvalidEncoding("unknown datum flag")),
    }
}

/// Decodes Go `codec.DecodeRange`'s schema-independent datum subset.
///
/// Unlike [`decode_one`], `DecodeRange` reserves a final bare `BYTES_FLAG` as
/// `MinNotNull` and a final `MAX_FLAG` (or its `PrefixNext` byte) as
/// `MaxValue`. A non-final `BYTES_FLAG` still starts an ordinary encoded byte
/// payload, so the two APIs must remain distinct.
pub fn decode_range(
    mut input: &[u8],
    expected_values: usize,
) -> Result<(Vec<Datum>, &[u8]), CodecError> {
    if input.is_empty() {
        return Err(CodecError::InvalidEncoding("empty encoded range"));
    }

    let mut values = Vec::with_capacity(expected_values);
    while input.len() > 1 {
        let (remain, value) = decode_one(input)?;
        values.push(value);
        input = remain;
    }

    if let Some(&flag) = input.first() {
        let value = match flag {
            NIL_FLAG => Datum::Null,
            BYTES_FLAG => Datum::min_not_null(),
            MAX_FLAG | PREFIX_NEXT_MAX_FLAG => Datum::max_value(),
            _ => return Err(CodecError::InvalidEncoding("invalid encoded range flag")),
        };
        values.push(value);
    }
    Ok((values, &[]))
}

/// Cuts the first complete encoded datum without decoding its payload.
pub fn cut_one(input: &[u8]) -> Result<(&[u8], &[u8]), CodecError> {
    let length = peek_one_len(input)?;
    Ok((&input[..length], &input[length..]))
}

/// Returns the encoded length of the first complete datum.
pub fn peek_one_len(input: &[u8]) -> Result<usize, CodecError> {
    let (&flag, payload) = input
        .split_first()
        .ok_or(CodecError::InvalidEncoding("empty key"))?;
    let payload_len = match flag {
        NIL_FLAG => 0,
        INT_FLAG | UINT_FLAG | FLOAT_FLAG => 8,
        BYTES_FLAG => peek_bytes_len(payload, false)?,
        DECIMAL_FLAG => peek_decimal_len(payload)?,
        _ => return Err(CodecError::InvalidEncoding("unknown datum flag")),
    };
    let total = payload_len + 1;
    if total > input.len() {
        return Err(CodecError::InsufficientBytes);
    }
    Ok(total)
}
