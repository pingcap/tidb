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
use crate::number::{
    decode_int, decode_uint, decode_uvarint, decode_varint, encode_int, encode_uint,
    encode_uvarint, encode_varint,
};
use crate::CodecError;
use chrono::{TimeZone, Utc};
use tidb_datatype::{
    deserialize_vector_float32, peek_vector_float32, BinaryJSON, BinaryLiteralIntOutcome, Datum,
    MySqlDuration, StringDatum,
};

/// TiDB's SQL NULL tag.
pub const NIL_FLAG: u8 = 0;
/// TiDB's mem-comparable byte-string tag.
pub const BYTES_FLAG: u8 = 1;
/// TiDB's compact length-prefixed byte-string tag.
pub const COMPACT_BYTES_FLAG: u8 = 2;
/// TiDB's fixed mem-comparable signed-integer tag.
pub const INT_FLAG: u8 = 3;
/// TiDB's fixed mem-comparable unsigned-integer tag.
pub const UINT_FLAG: u8 = 4;
/// TiDB's fixed mem-comparable floating-point tag.
pub const FLOAT_FLAG: u8 = 5;
/// TiDB's packed mem-comparable decimal tag.
pub const DECIMAL_FLAG: u8 = 6;
/// TiDB's signed duration tag.
pub const DURATION_FLAG: u8 = 7;
/// TiDB's compact signed-integer tag.
pub const VARINT_FLAG: u8 = 8;
/// TiDB's compact unsigned-integer tag.
pub const UVARINT_FLAG: u8 = 9;
/// TiDB's binary JSON tag.
pub const JSON_FLAG: u8 = 10;
/// TiDB's vector-float32 tag.
pub const VECTOR_FLOAT32_FLAG: u8 = 20;
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
        self.encode_key_in_timezone(&Utc, values)
    }

    /// Source `EncodeKey` with the session time zone used for timestamps.
    pub fn encode_key_in_timezone<TZ: TimeZone>(
        self,
        timezone: &TZ,
        values: &[Datum],
    ) -> Result<Vec<u8>, CodecError> {
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
                Datum::Float32(value) => {
                    output.push(FLOAT_FLAG);
                    encode_float(&mut output, *value);
                }
                Datum::String(value) => {
                    output.push(BYTES_FLAG);
                    encode_bytes(&mut output, &self.string_key(value));
                }
                Datum::Bytes(value) => {
                    output.push(BYTES_FLAG);
                    encode_bytes(&mut output, value);
                }
                Datum::BinaryLiteral(value) | Datum::Bit(value) => {
                    output.push(UINT_FLAG);
                    encode_uint(&mut output, binary_literal_uint(value)?);
                }
                Datum::Duration(value) => {
                    output.push(DURATION_FLAG);
                    encode_int(&mut output, value.nanoseconds());
                }
                Datum::Enum(value, _) => {
                    output.push(UINT_FLAG);
                    encode_uint(&mut output, value.value());
                }
                Datum::Set(value, _) => {
                    output.push(UINT_FLAG);
                    encode_uint(&mut output, value.value());
                }
                Datum::Time(value) => {
                    output.push(UINT_FLAG);
                    crate::package::encode_mysql_time(timezone, *value, None, &mut output)?;
                }
                Datum::Json(value) => {
                    output.push(JSON_FLAG);
                    output.extend_from_slice(&value.encoded());
                }
                Datum::VectorFloat32(value) => {
                    output.push(VECTOR_FLOAT32_FLAG);
                    value.serialize_to(&mut output);
                }
                Datum::Raw(_) => {
                    return Err(CodecError::InvalidEncoding("unsupported raw datum"));
                }
            }
        }
        Ok(output)
    }

    /// Encodes the complete source datum domain using Go `EncodeValue`.
    pub fn encode_value(self, values: &[Datum]) -> Result<Vec<u8>, CodecError> {
        self.encode_value_in_timezone(&Utc, values)
    }

    /// Source `EncodeValue` with the session time zone used for timestamps.
    pub fn encode_value_in_timezone<TZ: TimeZone>(
        self,
        timezone: &TZ,
        values: &[Datum],
    ) -> Result<Vec<u8>, CodecError> {
        let mut output = Vec::new();
        for value in values {
            match value {
                Datum::Null => output.push(NIL_FLAG),
                Datum::MinNotNull => output.push(BYTES_FLAG),
                Datum::MaxValue => output.push(MAX_FLAG),
                Datum::Int(value) => {
                    output.push(VARINT_FLAG);
                    encode_varint(&mut output, *value);
                }
                Datum::UInt(value) => {
                    output.push(UVARINT_FLAG);
                    encode_uvarint(&mut output, *value);
                }
                Datum::Decimal(value) => {
                    output.push(DECIMAL_FLAG);
                    encode_decimal(&mut output, value)?;
                }
                Datum::Real(value) | Datum::Float32(value) => {
                    output.push(FLOAT_FLAG);
                    encode_float(&mut output, *value);
                }
                Datum::String(value) => {
                    output.push(COMPACT_BYTES_FLAG);
                    crate::encode_compact_bytes(&mut output, value.bytes());
                }
                Datum::Bytes(value) => {
                    output.push(COMPACT_BYTES_FLAG);
                    crate::encode_compact_bytes(&mut output, value);
                }
                Datum::BinaryLiteral(value) | Datum::Bit(value) => {
                    output.push(UVARINT_FLAG);
                    encode_uvarint(&mut output, binary_literal_uint(value)?);
                }
                Datum::Duration(value) => {
                    output.push(DURATION_FLAG);
                    encode_int(&mut output, value.nanoseconds());
                }
                Datum::Enum(value, _) => {
                    output.push(UVARINT_FLAG);
                    encode_uvarint(&mut output, value.value());
                }
                Datum::Set(value, _) => {
                    output.push(UVARINT_FLAG);
                    encode_uvarint(&mut output, value.value());
                }
                Datum::Time(value) => {
                    output.push(UINT_FLAG);
                    crate::package::encode_mysql_time(timezone, *value, None, &mut output)?;
                }
                Datum::Json(value) => {
                    output.push(JSON_FLAG);
                    output.extend_from_slice(&value.encoded());
                }
                Datum::VectorFloat32(value) => {
                    output.push(VECTOR_FLOAT32_FLAG);
                    value.serialize_to(&mut output);
                }
                Datum::Raw(_) => return Err(CodecError::UnsupportedDatum("raw")),
            }
        }
        Ok(output)
    }

    /// Source `HashCode`, using lossless value encodings without SQL coercion.
    pub fn hash_code(self, output: &mut Vec<u8>, value: &Datum) {
        match value {
            Datum::Int(value) => {
                output.push(VARINT_FLAG);
                encode_varint(output, *value);
            }
            Datum::UInt(value) => {
                output.push(UVARINT_FLAG);
                encode_uvarint(output, *value);
            }
            Datum::Real(value) | Datum::Float32(value) => {
                output.push(FLOAT_FLAG);
                encode_float(output, *value);
            }
            Datum::String(value) => {
                output.push(COMPACT_BYTES_FLAG);
                crate::encode_compact_bytes(output, value.bytes());
            }
            Datum::Bytes(value) => {
                output.push(COMPACT_BYTES_FLAG);
                crate::encode_compact_bytes(output, value);
            }
            Datum::Time(value) => {
                output.push(UINT_FLAG);
                output.push(UINT_FLAG);
                encode_uint(output, value.core_time().raw());
            }
            Datum::Duration(value) => {
                output.push(DURATION_FLAG);
                encode_int(output, value.nanoseconds());
            }
            Datum::Decimal(value) => {
                output.push(DECIMAL_FLAG);
                let text = value.storage_string().into_bytes();
                output.push(COMPACT_BYTES_FLAG);
                crate::encode_compact_bytes(output, &text);
            }
            Datum::Enum(value, _) => {
                output.push(UVARINT_FLAG);
                encode_uvarint(output, value.value());
            }
            Datum::Set(value, _) => {
                output.push(UVARINT_FLAG);
                encode_uvarint(output, value.value());
            }
            Datum::BinaryLiteral(value) | Datum::Bit(value) => {
                output.push(COMPACT_BYTES_FLAG);
                crate::encode_compact_bytes(output, value.as_bytes());
            }
            Datum::Json(value) => {
                output.push(JSON_FLAG);
                output.extend_from_slice(&value.encoded());
            }
            Datum::VectorFloat32(value) => {
                output.push(VECTOR_FLOAT32_FLAG);
                value.serialize_to(output);
            }
            Datum::Null => output.push(NIL_FLAG),
            Datum::MinNotNull => output.push(BYTES_FLAG),
            Datum::MaxValue => output.push(MAX_FLAG),
            Datum::Raw(_) => {}
        }
    }

    fn string_key(self, value: &StringDatum) -> Vec<u8> {
        if self.use_new_collation {
            value.collation().immutable_key(value.bytes())
        } else {
            value.bytes().to_vec()
        }
    }
}

fn binary_literal_uint(literal: &tidb_datatype::BinaryLiteral) -> Result<u64, CodecError> {
    match literal.to_int() {
        BinaryLiteralIntOutcome::Exact(value) => Ok(value),
        BinaryLiteralIntOutcome::Truncated { .. } => {
            Err(CodecError::InvalidEncoding("binary literal exceeds uint64"))
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

/// Encodes keys with the source session time-zone contract.
pub fn encode_key_in_timezone<TZ: TimeZone>(
    timezone: &TZ,
    values: &[Datum],
) -> Result<Vec<u8>, CodecError> {
    Encoder::new(true).encode_key_in_timezone(timezone, values)
}

/// Encodes values through Go `EncodeValue`'s compact, non-order-preserving form.
pub fn encode_value(values: &[Datum]) -> Result<Vec<u8>, CodecError> {
    Encoder::new(true).encode_value(values)
}

/// Encodes values with the source session time-zone contract.
pub fn encode_value_in_timezone<TZ: TimeZone>(
    timezone: &TZ,
    values: &[Datum],
) -> Result<Vec<u8>, CodecError> {
    Encoder::new(true).encode_value_in_timezone(timezone, values)
}

/// Returns the exact length of one `EncodeValue` datum.
pub fn estimate_value_size(value: &Datum) -> Result<usize, CodecError> {
    Encoder::new(true)
        .encode_value(std::slice::from_ref(value))
        .map(|v| v.len())
}

/// Encodes one datum through Go's lossless `HashCode`.
pub fn hash_code(value: &Datum) -> Vec<u8> {
    let mut output = Vec::new();
    Encoder::new(true).hash_code(&mut output, value);
    output
}

/// Decodes every datum in one `EncodeKey` or `EncodeValue` stream.
pub fn decode(mut input: &[u8], expected_values: usize) -> Result<Vec<Datum>, CodecError> {
    if input.is_empty() {
        return Err(CodecError::InvalidEncoding("empty key"));
    }
    let mut values = Vec::with_capacity(expected_values);
    while !input.is_empty() {
        let (remain, value) = decode_one(input)?;
        values.push(value);
        input = remain;
    }
    Ok(values)
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
        VARINT_FLAG => {
            decode_varint(payload).map(|(remain, value)| (remain, Datum::new_int(value)))
        }
        UVARINT_FLAG => {
            decode_uvarint(payload).map(|(remain, value)| (remain, Datum::new_uint(value)))
        }
        FLOAT_FLAG => decode_float(payload).map(|(remain, value)| (remain, Datum::new_real(value))),
        BYTES_FLAG => {
            crate::decode_bytes(payload).map(|(remain, value)| (remain, Datum::new_bytes(value)))
        }
        COMPACT_BYTES_FLAG => crate::decode_compact_bytes(payload)
            .map(|(remain, value)| (remain, Datum::new_bytes(value))),
        DECIMAL_FLAG => {
            decode_decimal(payload).map(|(remain, value, _, _)| (remain, Datum::new_decimal(value)))
        }
        DURATION_FLAG => decode_int(payload).and_then(|(remain, value)| {
            MySqlDuration::from_nanoseconds(value, 6)
                .map(|value| (remain, Datum::new_duration(value)))
                .map_err(|_| CodecError::InvalidEncoding("invalid duration"))
        }),
        JSON_FLAG => {
            let length = crate::peek_json_len(payload)?;
            let encoded = payload.get(..length).ok_or(CodecError::InsufficientBytes)?;
            let (&type_code, value) = encoded.split_first().ok_or(CodecError::InsufficientBytes)?;
            let json = BinaryJSON::from_raw(type_code, value.to_vec())
                .map_err(|_| CodecError::InvalidEncoding("invalid binary JSON"))?;
            Ok((&payload[length..], Datum::new_json(json)))
        }
        VECTOR_FLOAT32_FLAG => {
            let (value, remain) = deserialize_vector_float32(payload)
                .map_err(|_| CodecError::InvalidEncoding("invalid vector float32"))?;
            Ok((remain, Datum::new_vector_float32(value)))
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
        INT_FLAG | UINT_FLAG | FLOAT_FLAG | DURATION_FLAG => 8,
        BYTES_FLAG => peek_bytes_len(payload, false)?,
        COMPACT_BYTES_FLAG => {
            let (remain, _) = crate::decode_compact_bytes(payload)?;
            payload.len() - remain.len()
        }
        DECIMAL_FLAG => peek_decimal_len(payload)?,
        VARINT_FLAG => {
            let (remain, _) = decode_varint(payload)?;
            payload.len() - remain.len()
        }
        UVARINT_FLAG => {
            let (remain, _) = decode_uvarint(payload)?;
            payload.len() - remain.len()
        }
        JSON_FLAG => crate::peek_json_len(payload)?,
        VECTOR_FLOAT32_FLAG => peek_vector_float32(payload)
            .map_err(|_| CodecError::InvalidEncoding("invalid vector float32"))?,
        _ => return Err(CodecError::InvalidEncoding("unknown datum flag")),
    };
    let total = payload_len + 1;
    if total > input.len() {
        return Err(CodecError::InsufficientBytes);
    }
    Ok(total)
}
