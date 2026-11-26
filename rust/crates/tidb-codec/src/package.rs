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
//! Value decoding can materialize a [`Datum`] or append directly to a chunk.

use tidb_datatype::RuntimeCollator;
use std::any::Any;
use std::borrow::Cow;

use chrono::{TimeZone, Utc};
use chrono_tz::Tz;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{
    parse_enum_value, parse_set_value, peek_vector_float32, BinaryJSON, BinaryLiteral, BinaryLiteralWidth, Datum,
    CheckedJSONKeys, EncodedJSONLayout, EvalType, JSON_TYPE_CODE_OBJECT,
    FieldType, FieldTypeCode, FieldTypeFlags, MyDecimal, MySqlDuration, RoundMode, SessionTimeZone,
    Time, TimeType,
};

use crate::bytes::with_decoded_bytes;
use crate::decimal::decode_packed_decimal;
use crate::{
    decode_compact_bytes, decode_float, decode_int, decode_one, decode_uint, decode_uvarint, decode_varint,
    encode_decimal_fixed, encode_float, encode_int, encode_uint, encode_varint, peek_json_len, CodecError,
    BYTES_FLAG, COMPACT_BYTES_FLAG, DECIMAL_FLAG, DURATION_FLAG, FLOAT_FLAG, INT_FLAG, JSON_FLAG,
    NIL_FLAG, UINT_FLAG, UVARINT_FLAG, VARINT_FLAG, VECTOR_FLOAT32_FLAG,
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
pub(crate) const fn value_size_of_signed_int(mut value: i64) -> usize {
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
pub(crate) const fn value_size_of_unsigned_int(mut value: u64) -> usize {
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
    field_type.runtime_collator().key(raw)
}

/// Source `ConvertByCollationStr`, byte-preserving for Go string semantics.
pub fn convert_by_collation_string(raw: &str, field_type: &FieldType) -> Vec<u8> {
    convert_by_collation(raw.as_bytes(), field_type)
}

/// Source `EncodeMySQLTime`.
pub fn encode_mysql_time<TZ: TimeZone + 'static>(
    timezone: &TZ,
    mut value: Time,
    target: Option<TimeType>,
    output: &mut Vec<u8>,
) -> Result<(), CodecError> {
    let kind = target.unwrap_or(value.kind());
    if kind == TimeType::Timestamp && !timezone_is_utc(timezone) {
        let original = value;
        if value.convert_time_zone(timezone, &Utc).is_err() {
            // Go returns nil, discarding the supplied prefix on failure.
            output.clear();
            return Err(CodecError::InvalidMysqlTimestamp(original));
        }
    }
    encode_uint(
        output,
        value
            .to_packed_uint()
            .map_err(|_| CodecError::InvalidEncoding("invalid MySQL time"))?,
    );
    Ok(())
}

fn timezone_is_utc<TZ: TimeZone + 'static>(timezone: &TZ) -> bool {
    let timezone = timezone as &dyn Any;
    timezone.is::<Utc>()
        || timezone
            .downcast_ref::<Tz>()
            .is_some_and(|timezone| *timezone == Tz::UTC)
        || timezone
            .downcast_ref::<SessionTimeZone>()
            .is_some_and(SessionTimeZone::is_utc)
}

/// Source `DecodeAsDateTime`.
pub fn decode_as_date_time<'a, TZ: TimeZone>(
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
    let (flag, payload) = hash_datum_payload(value, field_type)?;
    let bytes = match payload {
        HashPayload::Word(word) => word.to_vec(),
        HashPayload::Bytes(bytes) => bytes.into_owned(),
    };
    Ok((flag, bytes))
}

enum HashPayload<'a> {
    Word([u8; 8]),
    Bytes(Cow<'a, [u8]>),
}

impl HashPayload<'_> {
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Word(word) => word,
            Self::Bytes(bytes) => bytes,
        }
    }
}

fn hash_datum_payload<'a>(
    value: &'a Datum,
    field_type: &FieldType,
) -> Result<(u8, HashPayload<'a>), CodecError> {
    use HashPayload::{Bytes, Word};

    if value.is_null() {
        return Ok((NIL_FLAG, Bytes(Cow::Borrowed(&[]))));
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
            // Go only demotes to the signed tag for a *signed* column: an
            // unsigned column whose chunk word is negative still hashes as
            // `uvarintFlag`.
            if *value < 0 && !field_type.has_flag(FieldTypeFlags::UNSIGNED) {
                VARINT_FLAG
            } else {
                UVARINT_FLAG
            },
            Word(value.to_le_bytes()),
        ),
        (
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Year,
            Datum::UInt(value),
        ) => (UVARINT_FLAG, Word(value.to_le_bytes())),
        (FieldTypeCode::Float, Datum::Real(value))
        | (FieldTypeCode::Float, Datum::Float32(value)) => {
            let value = f64::from(*value as f32);
            let value = if value == 0.0 { 0.0 } else { value };
            (FLOAT_FLAG, Word(value.to_le_bytes()))
        }
        (FieldTypeCode::Double, Datum::Real(value))
        | (FieldTypeCode::Double, Datum::Float32(value)) => {
            let value = if *value == 0.0 { 0.0 } else { *value };
            (FLOAT_FLAG, Word(value.to_le_bytes()))
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
            Bytes(field_type.runtime_collator().immutable_key(value.bytes())),
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
        ) => (
            COMPACT_BYTES_FLAG,
            Bytes(field_type.runtime_collator().immutable_key(value)),
        ),
        (
            FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp,
            Datum::Time(value),
        ) => (
            UINT_FLAG,
            Word(
                value
                    .to_packed_uint()
                    .map_err(|_| CodecError::InvalidEncoding("invalid MySQL time"))?
                    .to_le_bytes(),
            ),
        ),
        (FieldTypeCode::Duration, Datum::Duration(value)) => {
            (DURATION_FLAG, Word(value.nanoseconds().to_le_bytes()))
        }
        (FieldTypeCode::NewDecimal, Datum::Decimal(value)) => (
            DECIMAL_FLAG,
            Bytes(Cow::Owned(
                value
                    .to_hash_key()
                    .map_err(|_| CodecError::InvalidEncoding("invalid decimal hash key"))?
                    .0,
            )),
        ),
        (FieldTypeCode::Enum, Datum::Enum(value, _))
            if field_type.has_flag(FieldTypeFlags::ENUM_SET_AS_INT) =>
        {
            (UVARINT_FLAG, Word(value.value().to_le_bytes()))
        }
        (FieldTypeCode::Enum, Datum::Enum(value, _)) => {
            let name = field_type
                .with_elems_visible(|elements| parse_enum_value(elements, value.value()))
                .map(|value| value.name_bytes().to_vec())
                .unwrap_or_default();
            (
                COMPACT_BYTES_FLAG,
                Bytes(Cow::Owned(field_type.runtime_collator().key(&name))),
            )
        }
        (FieldTypeCode::Set, Datum::Set(value, _)) => {
            let value = field_type
                .with_elems_visible(|elements| parse_set_value(elements, value.value()))
                .map_err(|_| CodecError::InvalidEncoding("invalid set value"))?;
            (
                COMPACT_BYTES_FLAG,
                Bytes(Cow::Owned(
                    field_type.runtime_collator().key(value.name_bytes()),
                )),
            )
        }
        (FieldTypeCode::Bit, Datum::Bit(value))
        | (FieldTypeCode::Bit, Datum::BinaryLiteral(value)) => (
            UVARINT_FLAG,
            Word(binary_literal_to_u64(value)?.to_le_bytes()),
        ),
        (FieldTypeCode::Json, Datum::Json(value)) => (
            JSON_FLAG,
            Bytes(Cow::Owned(
                value
                    .hash_value()
                    .map_err(|_| CodecError::InvalidEncoding("invalid binary JSON"))?,
            )),
        ),
        (FieldTypeCode::VectorFloat32, Datum::VectorFloat32(value)) => {
            (VECTOR_FLOAT32_FLAG, Bytes(Cow::Owned(value.serialize())))
        }
        (FieldTypeCode::Null, _) => (NIL_FLAG, Bytes(Cow::Borrowed(&[]))),
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
        let (flag, payload) = hash_datum_payload(value, field_type)?;
        match payload {
            HashPayload::Word(word) => {
                let mut tagged = [flag; 9];
                tagged[1..].copy_from_slice(&word);
                output.extend_from_slice(&tagged);
            }
            HashPayload::Bytes(bytes) => {
                output.reserve(1 + bytes.len());
                output.push(flag);
                output.extend_from_slice(&bytes);
            }
        }
    }
    Ok(output)
}

/// Source `EqualChunkRow`.
pub fn equal_chunk_row(
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
        let (left_flag, left_payload) = hash_datum_payload(
            left.get(left_indices[index])
                .ok_or(CodecError::InvalidEncoding("hash column index"))?,
            &left_types[index],
        )?;
        let (right_flag, right_payload) = hash_datum_payload(
            right
                .get(right_indices[index])
                .ok_or(CodecError::InvalidEncoding("hash column index"))?,
            &right_types[index],
        )?;
        if left_flag != right_flag || left_payload.as_bytes() != right_payload.as_bytes() {
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
pub fn hash_group_key_in_timezone<TZ: TimeZone + 'static>(
    timezone: &TZ,
    values: &[Datum],
    field_type: &FieldType,
) -> Result<Vec<Vec<u8>>, CodecError> {
    values
        .iter()
        .map(|value| {
            let mut output = Vec::new();
            append_hash_group_key_in_timezone(timezone, value, field_type, &mut output)?;
            Ok(output)
        })
        .collect()
}

/// Source `HashGroupKey` for one value, appending its bytes to `output` the
/// way Go appends every column's encoding onto the caller's per-row `buf[i]`
/// instead of returning a fresh slice. A STRING value is encoded through the
/// collator's `ImmutableKey`, which borrows the input under a binary
/// collation and produces the same bytes as `Key`.
pub fn append_hash_group_key_in_timezone<TZ: TimeZone + 'static>(
    timezone: &TZ,
    value: &Datum,
    field_type: &FieldType,
    output: &mut Vec<u8>,
) -> Result<(), CodecError> {
    if value.is_null() {
        output.push(NIL_FLAG);
        return Ok(());
    }
    match (field_type.eval_type(), value) {
        (EvalType::Int, Datum::Int(value)) => {
            output.push(VARINT_FLAG);
            encode_varint(output, *value);
        }
        (EvalType::Int, Datum::UInt(value)) => {
            output.push(VARINT_FLAG);
            encode_varint(output, *value as i64);
        }
        (EvalType::Int, Datum::Enum(value, _)) => {
            output.push(VARINT_FLAG);
            encode_varint(output, value.value() as i64);
        }
        (EvalType::Int, Datum::Bit(value) | Datum::BinaryLiteral(value)) => {
            output.push(VARINT_FLAG);
            encode_varint(output, binary_literal_to_u64(value)? as i64);
        }
        (EvalType::Real, Datum::Real(value) | Datum::Float32(value)) => {
            output.push(FLOAT_FLAG);
            encode_float(output, *value);
        }
        (EvalType::Decimal, Datum::Decimal(value)) => {
            output.push(DECIMAL_FLAG);
            encode_decimal_fixed(output, value, field_type.flen(), field_type.decimal())?;
        }
        (EvalType::Datetime | EvalType::Timestamp, Datum::Time(value)) => {
            output.push(UINT_FLAG);
            encode_mysql_time(timezone, *value, None, output)?;
        }
        (EvalType::Duration, Datum::Duration(value)) => {
            output.push(DURATION_FLAG);
            encode_int(output, value.nanoseconds());
        }
        (EvalType::Json, Datum::Json(value)) => {
            output.push(JSON_FLAG);
            value
                .append_hash_value(output)
                .map_err(|_| CodecError::InvalidEncoding("invalid binary JSON"))?;
        }
        (EvalType::String, Datum::String(value)) => {
            output.push(COMPACT_BYTES_FLAG);
            let key = field_type.runtime_collator().immutable_key(value.bytes());
            crate::encode_compact_bytes(output, &key);
        }
        (EvalType::String, Datum::Bytes(value)) => {
            output.push(COMPACT_BYTES_FLAG);
            let key = field_type.runtime_collator().immutable_key(value);
            crate::encode_compact_bytes(output, &key);
        }
        (EvalType::String, Datum::Enum(value, _)) => {
            output.push(COMPACT_BYTES_FLAG);
            let name = field_type
                .with_elems_visible(|elements| parse_enum_value(elements, value.value()))
                .map(|value| value.name_bytes().to_vec())
                .unwrap_or_default();
            let key = field_type.runtime_collator().immutable_key(&name);
            crate::encode_compact_bytes(output, &key);
        }
        (EvalType::String, Datum::Set(value, _)) => {
            output.push(COMPACT_BYTES_FLAG);
            let value = field_type
                .with_elems_visible(|elements| parse_set_value(elements, value.value()))
                .map_err(|_| CodecError::InvalidEncoding("invalid set value"))?;
            let key = field_type
                .runtime_collator()
                .immutable_key(value.name_bytes());
            crate::encode_compact_bytes(output, &key);
        }
        (EvalType::VectorFloat32, Datum::VectorFloat32(value)) => {
            value.serialize_to(output);
        }
        _ => {
            return Err(CodecError::InvalidEncoding(
                "datum and evaluation type do not match",
            ))
        }
    }
    Ok(())
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
            // Go's `TypeNull` column is null for every row, even when the
            // row-backed representation carries a non-NULL placeholder
            // datum. Mark it before serializing so an empty NULL key cannot
            // collide with an empty byte key in hash join v2.
            if field_type.code() == FieldTypeCode::Null || value.is_null() {
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

/// Go `Decoder.DecodeOne` for a caller that needs a value-layer datum.
pub fn decode_one_typed<'a>(
    input: &'a [u8],
    field_type: &FieldType,
) -> Result<(&'a [u8], Datum), CodecError> {
    decode_one_typed_in_timezone::<Utc>(input, field_type, None)
}

/// Retains validation state across values in one default-encoded response.
#[derive(Default)]
pub struct ChunkValueDecoder {
    json_layout: Option<CachedObjectLayout>,
    tried_key_cache: bool,
}

enum CachedObjectLayout {
    Complete { prefix: Vec<u8>, layout: EncodedJSONLayout },
    Keys { keys: CheckedJSONKeys, consecutive_misses: u8, matched: bool },
}

impl std::fmt::Debug for ChunkValueDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let prefix_len = match &self.json_layout {
            Some(CachedObjectLayout::Complete { prefix, .. }) => prefix.len(),
            Some(CachedObjectLayout::Keys { keys, .. }) => keys.prefix_len(),
            None => 0,
        };
        f.debug_struct("ChunkValueDecoder")
            .field("cached_json_prefix_bytes", &prefix_len)
            .finish()
    }
}

impl ChunkValueDecoder {
    /// Decodes one value into the output column and returns the unread suffix.
    #[inline]
    pub fn decode_one<'a, TZ: TimeZone>(
        &mut self,
        input: &'a [u8],
        output: &mut Chunk,
        column: usize,
        field_type: &FieldType,
        timezone: Option<&TZ>,
    ) -> Result<&'a [u8], CodecError> {
        decode_chunk_value(input, output, column, field_type, timezone, Some(self))
    }

    #[inline]
    fn check_json_layout(&mut self, encoded: &[u8], more_values: bool) -> EncodedJSONLayout {
        match &mut self.json_layout {
            Some(CachedObjectLayout::Complete { prefix, layout }) if encoded.starts_with(prefix) => {
                return *layout;
            }
            Some(CachedObjectLayout::Keys { keys, consecutive_misses, matched }) => {
                if let Some(layout) = keys.check_layout(encoded) {
                    *consecutive_misses = 0;
                    *matched = true;
                    return layout;
                }
                *consecutive_misses += 1;
                // Keep the proof across an interleaved shape. After two
                // misses, try the new shape if the old proof paid off.
                if *consecutive_misses == 2 {
                    self.tried_key_cache = !*matched;
                    self.json_layout = None;
                }
            }
            _ => {}
        }
        if let Some((layout, prefix)) = BinaryJSON::check_flat_object_layout(encoded) {
            if let Some(len) = prefix.filter(|len| more_values && *len <= 4096) {
                match &self.json_layout {
                    None => self.json_layout = Some(CachedObjectLayout::Complete {
                        prefix: encoded[..len].to_vec(),
                        layout,
                    }),
                    Some(CachedObjectLayout::Complete { prefix, .. })
                        if prefix.len() == len && !self.tried_key_cache =>
                    {
                        // A different key set can still reuse its directory checks.
                        self.cache_checked_keys(encoded, CheckedJSONKeys::new);
                    }
                    _ => {}
                }
            } else if prefix.is_none()
                && encoded.len() >= 2048
                && more_values
                && self.json_layout.is_none()
                && !self.tried_key_cache
            {
                self.cache_checked_keys(encoded, CheckedJSONKeys::for_wide_string_run);
            }
            return layout;
        }
        let layout = BinaryJSON::check_encoded_layout(encoded);
        if self.json_layout.is_none() && !self.tried_key_cache && more_values && layout.is_valid() {
            self.cache_checked_keys(encoded, CheckedJSONKeys::new);
        }
        layout
    }

    #[cold]
    fn cache_checked_keys(
        &mut self,
        encoded: &[u8],
        check: fn(&[u8]) -> Option<CheckedJSONKeys>,
    ) {
        self.tried_key_cache = true;
        if let Some(keys) = check(encoded) {
            self.json_layout = Some(CachedObjectLayout::Keys {
                keys,
                consecutive_misses: 0,
                matched: false,
            });
        }
    }
}

#[cfg(test)]
mod chunk_value_decoder_tests {
    use super::*;

    #[test]
    fn large_flat_json_reuses_layout_and_revalidates_text() {
        let make_cell = |text: &str| {
            let members = (0..16)
                .map(|index| format!(r#""key{index:03}":"{text}""#))
                .collect::<Vec<_>>()
                .join(",");
            let mut encoded = vec![JSON_FLAG];
            BinaryJSON::parse(&format!("{{{members}}}")).unwrap().append_encoded_to(&mut encoded);
            encoded
        };
        let ascii = make_cell(&"a".repeat(154));
        let unicode = make_cell(&format!("値{}", "b".repeat(151)));
        let mut invalid = unicode.clone();
        *invalid.last_mut().unwrap() = 0xff;
        let input = [ascii.as_slice(), &unicode, &invalid].concat();
        let field_type = FieldType::new(FieldTypeCode::Json);
        let mut chunk = Chunk::new_chunk_with_capacity(std::slice::from_ref(&field_type), 3);
        let mut decoder = ChunkValueDecoder::default();

        let rest = decoder.decode_one(&input, &mut chunk, 0, &field_type, Some(&Utc)).unwrap();
        assert!(matches!(decoder.json_layout, Some(CachedObjectLayout::Keys { .. })));
        let rest = decoder.decode_one(rest, &mut chunk, 0, &field_type, Some(&Utc)).unwrap();
        assert_eq!(chunk.get_row(0).get_raw(0).as_ref(), &ascii[1..]);
        assert_eq!(chunk.get_row(1).get_raw(0).as_ref(), &unicode[1..]);
        assert!(decoder.decode_one(rest, &mut chunk, 0, &field_type, Some(&Utc)).is_err());
        assert_eq!(chunk.num_rows(), 2);
    }

    #[test]
    fn large_flat_json_without_wide_strings_keeps_flat_validation() {
        let members = (0..128)
            .map(|index| {
                let value = if index % 2 == 0 { "1.5" } else { r#""short""# };
                format!(r#""key{index:03}":{value}"#)
            })
            .collect::<Vec<_>>()
            .join(",");
        let cell = BinaryJSON::parse(&format!("{{{members}}}")).unwrap().encoded();
        assert!(cell.len() >= 2048);
        let mut decoder = ChunkValueDecoder::default();
        assert!(decoder.check_json_layout(&cell, true).is_valid());
        assert!(decoder.tried_key_cache);
        assert!(decoder.json_layout.is_none());
    }
}

/// Go `Decoder.DecodeOne`: appends one value and returns the unread suffix.
#[inline]
pub fn decode_one_to_chunk<'a, TZ: TimeZone>(
    input: &'a [u8],
    output: &mut Chunk,
    column: usize,
    field_type: &FieldType,
    timezone: Option<&TZ>,
) -> Result<&'a [u8], CodecError> {
    decode_chunk_value(input, output, column, field_type, timezone, None)
}

#[inline]
fn decode_chunk_value<'a, TZ: TimeZone>(
    input: &'a [u8],
    output: &mut Chunk,
    column: usize,
    field_type: &FieldType,
    timezone: Option<&TZ>,
    decoder: Option<&mut ChunkValueDecoder>,
) -> Result<&'a [u8], CodecError> {
    match input {
        [NIL_FLAG, rest @ ..] => {
            output.append_null(column);
            Ok(rest)
        }
        [COMPACT_BYTES_FLAG, payload @ ..] => {
            let (remain, bytes) = decode_compact_bytes(payload)?;
            output.append_bytes(column, bytes);
            Ok(remain)
        }
        [BYTES_FLAG, payload @ ..] => {
            with_decoded_bytes(payload, |bytes| output.append_bytes(column, bytes))
        }
        [JSON_FLAG, payload @ ..] => {
            let length = peek_json_len(payload)?;
            let (encoded, remain) = payload.split_at_checked(length).ok_or(CodecError::InsufficientBytes)?;
            let layout = match (encoded.first(), decoder) {
                (Some(&JSON_TYPE_CODE_OBJECT), Some(decoder)) => {
                    decoder.check_json_layout(encoded, !remain.is_empty())
                }
                (Some(&JSON_TYPE_CODE_OBJECT), None) => BinaryJSON::check_flat_object_layout(encoded)
                    .map_or_else(|| BinaryJSON::check_encoded_layout(encoded), |(layout, _)| layout),
                _ => BinaryJSON::check_encoded_layout(encoded),
            };
            if !output.append_json_bytes_with_layout(column, encoded, layout) {
                return Err(CodecError::InvalidEncoding("invalid binary JSON"));
            }
            Ok(remain)
        }
        [VECTOR_FLOAT32_FLAG, payload @ ..] => {
            let length = peek_vector_float32(payload)
                .map_err(|_| CodecError::InvalidEncoding("invalid vector float32"))?;
            let (encoded, remain) = payload.split_at(length);
            output.append_bytes(column, encoded);
            Ok(remain)
        }
        [FLOAT_FLAG, payload @ ..]
            if matches!(field_type.code(), FieldTypeCode::Float | FieldTypeCode::Double) =>
        {
            let (remain, value) = decode_float(payload)?;
            if field_type.code() == FieldTypeCode::Float {
                output.append_float32(column, value as f32);
            } else {
                output.append_uint64(column, value.to_bits());
            }
            Ok(remain)
        }
        [flag @ (DURATION_FLAG | INT_FLAG | VARINT_FLAG), payload @ ..]
            if field_type.code() == FieldTypeCode::Duration =>
        {
            let (remain, nanoseconds) = if *flag == VARINT_FLAG {
                decode_varint(payload)?
            } else {
                decode_int(payload)?
            };
            let value = MySqlDuration::from_nanoseconds(nanoseconds, field_type.decimal())
                .map_err(|_| CodecError::InvalidEncoding("invalid duration"))?;
            output.append_int64(column, value.nanoseconds());
            Ok(remain)
        }
        [flag @ (INT_FLAG | UINT_FLAG | VARINT_FLAG | UVARINT_FLAG), payload @ ..]
            if matches!(
                field_type.code(),
                FieldTypeCode::Tiny
                    | FieldTypeCode::Short
                    | FieldTypeCode::Int24
                    | FieldTypeCode::Long
                    | FieldTypeCode::LongLong
                    | FieldTypeCode::Year
            ) =>
        {
            let (remain, bits) = match *flag {
                INT_FLAG => decode_int(payload).map(|(rest, value)| (rest, value as u64))?,
                VARINT_FLAG => decode_varint(payload).map(|(rest, value)| (rest, value as u64))?,
                UINT_FLAG => decode_uint(payload)?,
                _ => decode_uvarint(payload)?,
            };
            output.append_uint64(column, bits);
            Ok(remain)
        }
        [DECIMAL_FLAG, payload @ ..] if field_type.code() == FieldTypeCode::NewDecimal => {
            let (remain, value, scale) = decode_packed_decimal(payload)?;
            let target = field_type.decimal();
            if target >= 0 && i64::from(scale) > target {
                let mut rounded = MyDecimal::default();
                if value.round(&mut rounded, target as i32, RoundMode::HalfUp).is_some() {
                    return Err(CodecError::InvalidEncoding("cannot round decimal payload"));
                }
                output.append_my_decimal(column, &rounded);
            } else {
                output.append_my_decimal(column, &value);
            }
            Ok(remain)
        }
        _ => {
            let (remain, value) = decode_one_typed_in_timezone(input, field_type, timezone)?;
            output.append_datum(column, &value);
            Ok(remain)
        }
    }
}

/// Schema-aware source `Decoder.DecodeOne` with timestamp time-zone recovery.
pub fn decode_one_typed_in_timezone<'a, TZ: TimeZone>(
    input: &'a [u8],
    field_type: &FieldType,
    timezone: Option<&TZ>,
) -> Result<(&'a [u8], Datum), CodecError> {
    // Most row values already carry the Datum kind the schema needs. Keep the
    // source's schema-aware conversion for temporal, decimal, enum/set, bit,
    // duration and FLOAT values, but avoid matching the decoded Datum again
    // for the common integer/string/DOUBLE/JSON families. This is especially
    // important for wide default-encoded coprocessor batches, where the same
    // field-type dispatch otherwise runs once per value.
    if matches!(
        field_type.code(),
        FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Double
            | FieldTypeCode::String
            | FieldTypeCode::Varchar
            | FieldTypeCode::VarString
            | FieldTypeCode::Blob
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob
            | FieldTypeCode::Json
            | FieldTypeCode::VectorFloat32
    ) {
        return decode_one(input);
    }
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
            field_type
                .with_elems_visible(|elements| parse_enum_value(elements, value))
                .unwrap_or_else(|_| tidb_datatype::MysqlEnum::new("", 0)),
            field_type.collation(),
        ),
        (FieldTypeCode::Set, Datum::UInt(value)) => Datum::new_set(
            field_type
                .with_elems_visible(|elements| parse_set_value(elements, value))
                .map_err(|_| CodecError::InvalidEncoding("invalid set value"))?,
            field_type.collation(),
        ),
        (FieldTypeCode::Bit, Datum::UInt(value)) => {
            let byte_size = (field_type.flen() + 7) >> 3;
            let byte_size = u8::try_from(byte_size).unwrap_or_else(|_| panic!("Invalid byteSize"));
            let width = BinaryLiteralWidth::try_from(byte_size)
                .unwrap_or_else(|_| panic!("Invalid byteSize"));
            Datum::new_mysql_bit(BinaryLiteral::from_uint(value, Some(width)))
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
