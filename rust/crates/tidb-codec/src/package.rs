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

//! Remaining source surface of `pkg/util/codec/codec.go`.
//!
//! Go's chunk-specific entry points are represented as rows of [`Datum`].
//! This keeps the wire and equality behavior in the codec package without
//! importing an executor-owned column container into the dependency leaf.

use chrono::{TimeZone, Utc};
use tidb_datatype::{
    parse_enum_value, parse_set_value, BinaryLiteral, BinaryLiteralWidth, Datum, EvalType,
    FieldType, FieldTypeCode, FieldTypeFlags, MySqlDuration, Time, TimeType,
};

use crate::{
    decode_one, decode_uint, decode_uvarint, decode_varint, encode_decimal_fixed, encode_float,
    encode_int, encode_uint, encode_varint, CodecError, COMPACT_BYTES_FLAG, DECIMAL_FLAG,
    DURATION_FLAG, FLOAT_FLAG, JSON_FLAG, NIL_FLAG, UINT_FLAG, UVARINT_FLAG, VARINT_FLAG,
    VECTOR_FLOAT32_FLAG,
};

/// Go `SerializeMode`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SerializeMode {
    /// Serialize without an extra sign or length field.
    #[default]
    Normal,
    /// Preserve integer signedness as a leading byte.
    NeedSignFlag,
    /// Prefix variable-width keys with a little-endian `u32` length.
    KeepVarColumnLength,
}

/// Per-row bytes and NULL markers produced by [`hash_column`].
pub type HashColumnOutput = (Vec<Option<Vec<u8>>>, Vec<bool>);

/// Source `valueSizeOfSignedInt`, including the datum tag.
pub const fn value_size_of_signed_int(mut value: i64) -> usize {
    if value < 0 {
        value = 0_i64.wrapping_sub(value).wrapping_sub(1);
    }
    let mut size = 2;
    value >>= 6;
    while value > 0 {
        size += 1;
        value >>= 7;
    }
    size
}

/// Source `valueSizeOfUnsignedInt`, including the datum tag.
pub const fn value_size_of_unsigned_int(mut value: u64) -> usize {
    let mut size = 2;
    value >>= 7;
    while value > 0 {
        size += 1;
        value >>= 7;
    }
    size
}

/// Source `ConvertByCollation`.
pub fn convert_by_collation(raw: &[u8], field_type: &FieldType) -> Vec<u8> {
    field_type.collation().key(raw)
}

/// Source `ConvertByCollationStr`, byte-preserving for Go string semantics.
pub fn convert_by_collation_string(raw: &str, field_type: &FieldType) -> Vec<u8> {
    convert_by_collation(raw.as_bytes(), field_type)
}

/// Source `EncodeMySQLTime`.
pub fn encode_mysql_time<TZ: TimeZone>(
    timezone: &TZ,
    mut value: Time,
    target: Option<TimeType>,
    output: &mut Vec<u8>,
) -> Result<(), CodecError> {
    let kind = target.unwrap_or(value.kind());
    if kind == TimeType::Timestamp {
        value
            .convert_time_zone(timezone, &Utc)
            .map_err(|_| CodecError::InvalidEncoding("invalid MySQL timestamp"))?;
    }
    encode_uint(
        output,
        value
            .to_packed_uint()
            .map_err(|_| CodecError::InvalidEncoding("invalid MySQL time"))?,
    );
    Ok(())
}

/// Source `DecodeAsDateTime`.
pub fn decode_as_datetime<'a, TZ: TimeZone>(
    input: &'a [u8],
    kind: TimeType,
    timezone: Option<&TZ>,
) -> Result<(&'a [u8], Datum), CodecError> {
    let (&flag, payload) = input
        .split_first()
        .ok_or(CodecError::InvalidEncoding("empty key"))?;
    if flag == NIL_FLAG {
        return Ok((payload, Datum::Null));
    }
    let (remain, packed) = match flag {
        UINT_FLAG => decode_uint(payload)?,
        UVARINT_FLAG => decode_uvarint(payload)?,
        _ => return Err(CodecError::InvalidEncoding("invalid datetime datum flag")),
    };
    let mut value = Time::from_packed_uint(packed, kind, 0)
        .map_err(|_| CodecError::InvalidEncoding("invalid packed MySQL time"))?;
    if kind == TimeType::Timestamp && !value.is_zero() {
        if let Some(timezone) = timezone {
            value
                .convert_time_zone(&Utc, timezone)
                .map_err(|_| CodecError::InvalidEncoding("invalid MySQL timestamp"))?;
        }
    }
    Ok((remain, Datum::new_time(value)))
}

/// Source `DecodeAsFloat32`.
pub fn decode_as_float32(input: &[u8]) -> Result<(&[u8], Datum), CodecError> {
    let (remain, value) = decode_one(input)?;
    match value {
        Datum::Real(value) => Ok((remain, Datum::new_float32_from_f64(value))),
        _ => Err(CodecError::InvalidEncoding("expected float datum")),
    }
}

/// Source `CutColumnID`.
pub fn cut_column_id(input: &[u8]) -> Result<(&[u8], i64), CodecError> {
    let (_, payload) = input
        .split_first()
        .ok_or(CodecError::InvalidEncoding("empty key"))?;
    decode_varint(payload)
}

/// Source `SetRawValues`, returning the same zero-copy logical slices as owned
/// Rust raw datums.
pub fn set_raw_values(mut input: &[u8], count: usize) -> Result<Vec<Datum>, CodecError> {
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let (encoded, remain) = crate::cut_one(input)?;
        values.push(Datum::new_raw(encoded));
        input = remain;
    }
    Ok(values)
}

/// Source `EncodeHashChunkRowIdx` over a Rust row.
pub fn encode_hash_datum(
    value: &Datum,
    field_type: &FieldType,
) -> Result<(u8, Vec<u8>), CodecError> {
    if value.is_null() {
        return Ok((NIL_FLAG, Vec::new()));
    }
    let result = match (field_type.code(), value) {
        (
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Year,
            Datum::Int(value),
        ) => (
            if *value < 0 {
                VARINT_FLAG
            } else {
                UVARINT_FLAG
            },
            value.to_le_bytes().to_vec(),
        ),
        (
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Year,
            Datum::UInt(value),
        ) => (UVARINT_FLAG, value.to_le_bytes().to_vec()),
        (FieldTypeCode::Float, Datum::Real(value))
        | (FieldTypeCode::Float, Datum::Float32(value)) => {
            let value = f64::from(*value as f32);
            let value = if value == 0.0 { 0.0 } else { value };
            (FLOAT_FLAG, value.to_le_bytes().to_vec())
        }
        (FieldTypeCode::Double, Datum::Real(value))
        | (FieldTypeCode::Double, Datum::Float32(value)) => {
            let value = if *value == 0.0 { 0.0 } else { *value };
            (FLOAT_FLAG, value.to_le_bytes().to_vec())
        }
        (
            FieldTypeCode::String
            | FieldTypeCode::Varchar
            | FieldTypeCode::VarString
            | FieldTypeCode::Blob
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob,
            Datum::String(value),
        ) => (
            COMPACT_BYTES_FLAG,
            field_type.collation().key(value.bytes()),
        ),
        (
            FieldTypeCode::String
            | FieldTypeCode::Varchar
            | FieldTypeCode::VarString
            | FieldTypeCode::Blob
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob,
            Datum::Bytes(value),
        ) => (COMPACT_BYTES_FLAG, field_type.collation().key(value)),
        (
            FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp,
            Datum::Time(value),
        ) => (
            UINT_FLAG,
            value
                .to_packed_uint()
                .map_err(|_| CodecError::InvalidEncoding("invalid MySQL time"))?
                .to_le_bytes()
                .to_vec(),
        ),
        (FieldTypeCode::Duration, Datum::Duration(value)) => {
            (DURATION_FLAG, value.nanoseconds().to_le_bytes().to_vec())
        }
        (FieldTypeCode::NewDecimal, Datum::Decimal(value)) => (
            DECIMAL_FLAG,
            value
                .to_hash_key()
                .map_err(|_| CodecError::InvalidEncoding("invalid decimal hash key"))?
                .0,
        ),
        (FieldTypeCode::Enum, Datum::Enum(value, _))
            if field_type.has_flag(FieldTypeFlags::ENUM_SET_AS_INT) =>
        {
            (UVARINT_FLAG, value.value().to_le_bytes().to_vec())
        }
        (FieldTypeCode::Enum, Datum::Enum(value, _)) => {
            let name = parse_enum_value(field_type.elems(), value.value())
                .map(|value| value.name().as_bytes().to_vec())
                .unwrap_or_default();
            (COMPACT_BYTES_FLAG, field_type.collation().key(&name))
        }
        (FieldTypeCode::Set, Datum::Set(value, _)) => {
            let value = parse_set_value(field_type.elems(), value.value())
                .map_err(|_| CodecError::InvalidEncoding("invalid set value"))?;
            (
                COMPACT_BYTES_FLAG,
                field_type.collation().key(value.name().as_bytes()),
            )
        }
        (FieldTypeCode::Bit, Datum::Bit(value))
        | (FieldTypeCode::Bit, Datum::BinaryLiteral(value)) => (
            UVARINT_FLAG,
            binary_literal_to_u64(value)?.to_le_bytes().to_vec(),
        ),
        (FieldTypeCode::Json, Datum::Json(value)) => (
            JSON_FLAG,
            value
                .hash_value()
                .map_err(|_| CodecError::InvalidEncoding("invalid binary JSON"))?,
        ),
        (FieldTypeCode::VectorFloat32, Datum::VectorFloat32(value)) => {
            (VECTOR_FLOAT32_FLAG, value.serialize())
        }
        (FieldTypeCode::Null, _) => (NIL_FLAG, Vec::new()),
        _ => {
            return Err(CodecError::InvalidEncoding(
                "datum and field type do not match",
            ))
        }
    };
    Ok(result)
}

/// Source `HashChunkRow`.
pub fn hash_row(
    row: &[Datum],
    field_types: &[FieldType],
    column_indices: &[usize],
) -> Result<Vec<u8>, CodecError> {
    if field_types.len() != column_indices.len() {
        return Err(CodecError::InvalidEncoding("hash column count mismatch"));
    }
    let mut output = Vec::new();
    for (field_type, &index) in field_types.iter().zip(column_indices) {
        let value = row
            .get(index)
            .ok_or(CodecError::InvalidEncoding("hash column index"))?;
        let (flag, bytes) = encode_hash_datum(value, field_type)?;
        output.push(flag);
        output.extend_from_slice(&bytes);
    }
    Ok(output)
}

/// Source `EqualChunkRow`.
pub fn equal_rows(
    left: &[Datum],
    left_types: &[FieldType],
    left_indices: &[usize],
    right: &[Datum],
    right_types: &[FieldType],
    right_indices: &[usize],
) -> Result<bool, CodecError> {
    if left_indices.len() != right_indices.len()
        || left_types.len() != left_indices.len()
        || right_types.len() != right_indices.len()
    {
        return Err(CodecError::InvalidEncoding("hash column count mismatch"));
    }
    for index in 0..left_indices.len() {
        let left = encode_hash_datum(
            left.get(left_indices[index])
                .ok_or(CodecError::InvalidEncoding("hash column index"))?,
            &left_types[index],
        )?;
        let right = encode_hash_datum(
            right
                .get(right_indices[index])
                .ok_or(CodecError::InvalidEncoding("hash column index"))?,
            &right_types[index],
        )?;
        if left != right {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Source `HashGroupKey` over one logical column.
pub fn hash_group_key(
    values: &[Datum],
    field_type: &FieldType,
) -> Result<Vec<Vec<u8>>, CodecError> {
    hash_group_key_in_timezone(&Utc, values, field_type)
}

/// Source `HashGroupKey` with the session time zone used for timestamps.
pub fn hash_group_key_in_timezone<TZ: TimeZone>(
    timezone: &TZ,
    values: &[Datum],
    field_type: &FieldType,
) -> Result<Vec<Vec<u8>>, CodecError> {
    values
        .iter()
        .map(|value| {
            if value.is_null() {
                return Ok(vec![NIL_FLAG]);
            }
            let mut output = Vec::new();
            match (field_type.eval_type(), value) {
                (EvalType::Int, Datum::Int(value)) => {
                    output.push(VARINT_FLAG);
                    encode_varint(&mut output, *value);
                }
                (EvalType::Int, Datum::UInt(value)) => {
                    output.push(VARINT_FLAG);
                    encode_varint(&mut output, *value as i64);
                }
                (EvalType::Int, Datum::Enum(value, _)) => {
                    output.push(VARINT_FLAG);
                    encode_varint(&mut output, value.value() as i64);
                }
                (EvalType::Int, Datum::Bit(value) | Datum::BinaryLiteral(value)) => {
                    output.push(VARINT_FLAG);
                    encode_varint(&mut output, binary_literal_to_u64(value)? as i64);
                }
                (EvalType::Real, Datum::Real(value) | Datum::Float32(value)) => {
                    output.push(FLOAT_FLAG);
                    encode_float(&mut output, *value);
                }
                (EvalType::Decimal, Datum::Decimal(value)) => {
                    output.push(DECIMAL_FLAG);
                    encode_decimal_fixed(
                        &mut output,
                        value,
                        field_type.flen().max(0) as usize,
                        field_type.decimal().max(0) as usize,
                    )?;
                }
                (EvalType::Datetime | EvalType::Timestamp, Datum::Time(value)) => {
                    output.push(UINT_FLAG);
                    encode_mysql_time(timezone, *value, None, &mut output)?;
                }
                (EvalType::Duration, Datum::Duration(value)) => {
                    output.push(DURATION_FLAG);
                    encode_int(&mut output, value.nanoseconds());
                }
                (EvalType::Json, Datum::Json(value)) => {
                    output.push(JSON_FLAG);
                    output.extend_from_slice(
                        &value
                            .hash_value()
                            .map_err(|_| CodecError::InvalidEncoding("invalid binary JSON"))?,
                    );
                }
                (EvalType::String, Datum::String(value)) => {
                    output.push(COMPACT_BYTES_FLAG);
                    let key = field_type.collation().key(value.bytes());
                    crate::encode_compact_bytes(&mut output, &key);
                }
                (EvalType::String, Datum::Bytes(value)) => {
                    output.push(COMPACT_BYTES_FLAG);
                    let key = field_type.collation().key(value);
                    crate::encode_compact_bytes(&mut output, &key);
                }
                (EvalType::String, Datum::Enum(value, _)) => {
                    output.push(COMPACT_BYTES_FLAG);
                    let name = parse_enum_value(field_type.elems(), value.value())
                        .map(|value| value.name().as_bytes().to_vec())
                        .unwrap_or_default();
                    let key = field_type.collation().key(&name);
                    crate::encode_compact_bytes(&mut output, &key);
                }
                (EvalType::String, Datum::Set(value, _)) => {
                    output.push(COMPACT_BYTES_FLAG);
                    let value = parse_set_value(field_type.elems(), value.value())
                        .map_err(|_| CodecError::InvalidEncoding("invalid set value"))?;
                    let key = field_type.collation().key(value.name().as_bytes());
                    crate::encode_compact_bytes(&mut output, &key);
                }
                (EvalType::VectorFloat32, Datum::VectorFloat32(value)) => {
                    value.serialize_to(&mut output);
                }
                _ => {
                    return Err(CodecError::InvalidEncoding(
                        "datum and evaluation type do not match",
                    ))
                }
            }
            Ok(output)
        })
        .collect()
}

/// Source `HashChunkColumns` / `HashChunkSelected`, returning the bytes each
/// selected row writes to its hasher and the corresponding NULL markers.
pub fn hash_column(
    rows: &[Vec<Datum>],
    field_type: &FieldType,
    column_index: usize,
    selection: Option<&[bool]>,
    ignore_null: bool,
) -> Result<HashColumnOutput, CodecError> {
    if selection.is_some_and(|selection| selection.len() != rows.len()) {
        return Err(CodecError::InvalidEncoding("selection length mismatch"));
    }
    let mut encoded = Vec::with_capacity(rows.len());
    let mut is_null = vec![false; rows.len()];
    for (row_index, row) in rows.iter().enumerate() {
        if selection.is_some_and(|selection| !selection[row_index]) {
            encoded.push(None);
            continue;
        }
        let value = row
            .get(column_index)
            .ok_or(CodecError::InvalidEncoding("hash column index"))?;
        if value.is_null() {
            is_null[row_index] = !ignore_null;
        }
        let (flag, bytes) = encode_hash_datum(value, field_type)?;
        let mut output = Vec::with_capacity(1 + bytes.len());
        output.push(flag);
        output.extend_from_slice(&bytes);
        encoded.push(Some(output));
    }
    Ok((encoded, is_null))
}

/// Source `SerializeKeys`, implemented row-first over the Rust datum model.
pub fn serialize_keys(
    rows: &[Vec<Datum>],
    column_indices: &[usize],
    field_types: &[FieldType],
    modes: &[SerializeMode],
    selection: Option<&[bool]>,
) -> Result<(Vec<Vec<u8>>, Vec<bool>), CodecError> {
    if column_indices.len() != field_types.len() || modes.len() != field_types.len() {
        return Err(CodecError::InvalidEncoding(
            "serialize column count mismatch",
        ));
    }
    if selection.is_some_and(|selection| selection.len() != rows.len()) {
        return Err(CodecError::InvalidEncoding("selection length mismatch"));
    }
    let mut keys = vec![Vec::new(); rows.len()];
    let mut nulls = vec![false; rows.len()];
    for (row_index, row) in rows.iter().enumerate() {
        if selection.is_some_and(|selection| !selection[row_index]) {
            continue;
        }
        for ((&column_index, field_type), mode) in column_indices.iter().zip(field_types).zip(modes)
        {
            let value = row
                .get(column_index)
                .ok_or(CodecError::InvalidEncoding("serialize column index"))?;
            if value.is_null() {
                nulls[row_index] = true;
                keys[row_index].clear();
                break;
            }
            let (_, bytes) = encode_hash_datum(value, field_type)?;
            match (field_type.code(), mode) {
                (
                    FieldTypeCode::Tiny
                    | FieldTypeCode::Short
                    | FieldTypeCode::Int24
                    | FieldTypeCode::Long
                    | FieldTypeCode::LongLong
                    | FieldTypeCode::Year
                    | FieldTypeCode::Bit,
                    SerializeMode::NeedSignFlag,
                ) => keys[row_index].push(match value {
                    Datum::Int(value)
                        if !field_type.has_flag(FieldTypeFlags::UNSIGNED) && *value < 0 =>
                    {
                        crate::INT_FLAG
                    }
                    _ => UINT_FLAG,
                }),
                (FieldTypeCode::Enum, SerializeMode::NeedSignFlag)
                    if field_type.has_flag(FieldTypeFlags::ENUM_SET_AS_INT) =>
                {
                    keys[row_index].push(UINT_FLAG)
                }
                (FieldTypeCode::NewDecimal, SerializeMode::KeepVarColumnLength) => {
                    let size = u8::try_from(bytes.len())
                        .map_err(|_| CodecError::InvalidEncoding("decimal hash key too long"))?;
                    keys[row_index].push(size);
                }
                (
                    FieldTypeCode::String
                    | FieldTypeCode::Varchar
                    | FieldTypeCode::VarString
                    | FieldTypeCode::Blob
                    | FieldTypeCode::TinyBlob
                    | FieldTypeCode::MediumBlob
                    | FieldTypeCode::LongBlob
                    | FieldTypeCode::Set
                    | FieldTypeCode::Json,
                    SerializeMode::KeepVarColumnLength,
                ) => keys[row_index].extend_from_slice(&(bytes.len() as u32).to_le_bytes()),
                (FieldTypeCode::Enum, SerializeMode::KeepVarColumnLength)
                    if !field_type.has_flag(FieldTypeFlags::ENUM_SET_AS_INT) =>
                {
                    keys[row_index].extend_from_slice(&(bytes.len() as u32).to_le_bytes())
                }
                _ => {}
            }
            keys[row_index].extend_from_slice(&bytes);
        }
    }
    Ok((keys, nulls))
}

/// Schema-aware source `Decoder.DecodeOne` without a Go chunk dependency.
pub fn decode_one_typed<'a>(
    input: &'a [u8],
    field_type: &FieldType,
) -> Result<(&'a [u8], Datum), CodecError> {
    decode_one_typed_in_timezone::<Utc>(input, field_type, None)
}

/// Schema-aware source `Decoder.DecodeOne` with timestamp time-zone recovery.
pub fn decode_one_typed_in_timezone<'a, TZ: TimeZone>(
    input: &'a [u8],
    field_type: &FieldType,
    timezone: Option<&TZ>,
) -> Result<(&'a [u8], Datum), CodecError> {
    let (remain, mut value) = decode_one(input)?;
    value = match (field_type.code(), value) {
        (FieldTypeCode::Float, Datum::Real(value)) => Datum::new_float32_from_f64(value),
        (FieldTypeCode::NewDecimal, Datum::Decimal(value))
            if field_type.decimal() >= 0 && value.scale() > field_type.decimal() as u32 =>
        {
            Datum::new_decimal(value.round_to_scale(field_type.decimal() as i32))
        }
        (FieldTypeCode::Duration, Datum::Int(value)) => Datum::new_duration(
            MySqlDuration::from_nanoseconds(value, field_type.decimal())
                .map_err(|_| CodecError::InvalidEncoding("invalid duration"))?,
        ),
        (FieldTypeCode::Duration, Datum::Duration(value)) => Datum::new_duration(
            MySqlDuration::from_nanoseconds(value.nanoseconds(), field_type.decimal())
                .map_err(|_| CodecError::InvalidEncoding("invalid duration"))?,
        ),
        (
            FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp,
            Datum::UInt(value),
        ) => {
            let kind = match field_type.code() {
                FieldTypeCode::Date => TimeType::Date,
                FieldTypeCode::Datetime => TimeType::DateTime,
                FieldTypeCode::Timestamp => TimeType::Timestamp,
                _ => unreachable!(),
            };
            let mut value = Time::from_packed_uint(value, kind, field_type.decimal())
                .map_err(|_| CodecError::InvalidEncoding("invalid packed MySQL time"))?;
            if kind == TimeType::Timestamp && !value.is_zero() {
                if let Some(timezone) = timezone {
                    value
                        .convert_time_zone(&Utc, timezone)
                        .map_err(|_| CodecError::InvalidEncoding("invalid MySQL timestamp"))?;
                }
            }
            Datum::new_time(value)
        }
        (FieldTypeCode::Enum, Datum::UInt(value)) => Datum::new_enum(
            parse_enum_value(field_type.elems(), value)
                .unwrap_or_else(|_| tidb_datatype::MysqlEnum::new("", 0)),
            field_type.collation(),
        ),
        (FieldTypeCode::Set, Datum::UInt(value)) => Datum::new_set(
            parse_set_value(field_type.elems(), value)
                .map_err(|_| CodecError::InvalidEncoding("invalid set value"))?,
            field_type.collation(),
        ),
        (FieldTypeCode::Bit, Datum::UInt(value)) => {
            let byte_size = ((field_type.flen() + 7) >> 3).max(0) as usize;
            let width = u8::try_from(byte_size)
                .ok()
                .and_then(|width| BinaryLiteralWidth::try_from(width).ok());
            Datum::new_mysql_bit(BinaryLiteral::from_uint(value, width))
        }
        (_, value) => value,
    };
    Ok((remain, value))
}

fn binary_literal_to_u64(value: &BinaryLiteral) -> Result<u64, CodecError> {
    match value.to_int() {
        tidb_datatype::BinaryLiteralIntOutcome::Exact(value) => Ok(value),
        tidb_datatype::BinaryLiteralIntOutcome::Truncated { .. } => {
            Err(CodecError::InvalidEncoding("binary literal exceeds uint64"))
        }
    }
}
