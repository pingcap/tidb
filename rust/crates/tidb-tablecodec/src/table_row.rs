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

//! Row-value half of Go `pkg/tablecodec/tablecodec.go`, above the leaf codecs.

use std::borrow::{Borrow, Cow};
use std::collections::{btree_map::Entry, BTreeMap};
use std::fmt;
use std::sync::Arc;

use chrono::Utc;
use smallvec::SmallVec;

use tidb_datatype::{
    parse_enum_value, parse_set_value, BinaryJSON, BinaryLiteralIntOutcome, BinaryLiteralWidth, CoreTime,
    Datum, FieldType, FieldTypeCode, MySqlDuration, MysqlEnum, PackedTime, SessionTimeZone,
    StringDatum, Time, TimeType, VectorFloat32,
};

use tidb_codec::{
    cut_column_id, cut_one, decode_compact_bytes, decode_json, decode_one, decode_varint,
    encode_value_in_timezone, encode_value_refs_in_timezone, CodecError, COMPACT_BYTES_FLAG,
    JSON_FLAG, NIL_FLAG, VARINT_FLAG, VECTOR_FLOAT32_FLAG,
};
use tidb_rowcodec::{
    decode_row_to_map_with_fields, decode_shared_row_to_map_with_fields, encode_row_with_values,
    is_new_format, RowChecksumPolicy, RowPackageError,
    MIN_SHARED_ROW_VALUE_BYTES,
};
use tidb_txnkv::Handle;

/// Failure while translating table rows between SQL datums and persisted
/// codec values.
#[derive(Debug)]
pub enum TableRowError {
    /// Row values and column IDs did not have equal lengths.
    ColumnCountMismatch {
        /// Number of row values.
        values: usize,
        /// Number of column IDs.
        column_ids: usize,
    },
    /// One datum does not match the column/storage representation.
    InvalidDatum(&'static str),
    /// An encoded datum was malformed.
    Codec(CodecError),
    /// The new row format was malformed.
    Row(RowPackageError),
    /// A temporal, enum, set, or bit conversion failed.
    Datatype(String),
}

impl fmt::Display for TableRowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ColumnCountMismatch { values, column_ids } => write!(
                formatter,
                "EncodeRow error: data and columnID count not match {values} vs {column_ids}"
            ),
            Self::InvalidDatum(kind) => write!(formatter, "invalid {kind} datum"),
            Self::Codec(error) => error.fmt(formatter),
            Self::Row(error) => error.fmt(formatter),
            Self::Datatype(error) => formatter.write_str(error),
        }
    }
}

impl std::error::Error for TableRowError {}

impl From<CodecError> for TableRowError {
    fn from(error: CodecError) -> Self {
        Self::Codec(error)
    }
}

impl From<RowPackageError> for TableRowError {
    fn from(error: RowPackageError) -> Self {
        Self::Row(error)
    }
}

/// Converts one typed SQL datum to tablecodec's persisted scalar form.
fn flatten_datum<'a>(
    timezone: Option<&SessionTimeZone>,
    datum: &'a Datum,
) -> Result<Cow<'a, Datum>, TableRowError> {
    let flattened = match datum {
        Datum::Time(value) => {
            let mut value = *value;
            if value.kind() == TimeType::Timestamp {
                if let Some(timezone) = timezone {
                    value
                        .convert_time_zone(timezone, &Utc)
                        .map_err(|error| TableRowError::Datatype(error.to_string()))?;
                }
            }
            Datum::UInt(
                value
                    .to_packed_uint()
                    .map_err(|error| TableRowError::Datatype(error.to_string()))?,
            )
        }
        Datum::Duration(value) => Datum::Int(value.nanoseconds()),
        Datum::Enum(value, _) => Datum::UInt(value.value()),
        Datum::Set(value, _) => Datum::UInt(value.value()),
        Datum::BinaryLiteral(value) | Datum::Bit(value) => {
            let value = match value.to_int() {
                BinaryLiteralIntOutcome::Exact(value) => value,
                BinaryLiteralIntOutcome::Truncated { .. } => {
                    return Err(TableRowError::InvalidDatum("binary literal exceeds uint64"));
                }
            };
            Datum::UInt(value)
        }
        other => return Ok(Cow::Borrowed(other)),
    };
    Ok(Cow::Owned(flattened))
}

/// Restores one persisted scalar according to its SQL column metadata.
pub fn unflatten_datum(
    mut datum: Datum,
    field_type: &FieldType,
    timezone: Option<&SessionTimeZone>,
) -> Result<Datum, TableRowError> {
    unflatten_datum_in_place(&mut datum, field_type, timezone)?;
    Ok(datum)
}

fn is_stored_string(code: FieldTypeCode) -> bool {
    code.is_string() && !code.is_type_unspecified()
}

fn unflatten_datum_in_place(
    datum: &mut Datum,
    field_type: &FieldType,
    timezone: Option<&SessionTimeZone>,
) -> Result<(), TableRowError> {
    if datum.is_null() {
        return Ok(());
    }
    let code = field_type.code();
    let restored = match code {
        FieldTypeCode::Float => Datum::Float32(
            datum
                .as_real()
                .ok_or(TableRowError::InvalidDatum("float"))? as f32 as f64,
        ),
        code if is_stored_string(code) => match datum {
            Datum::String(value) => {
                value.set_collation(field_type.collation());
                return Ok(());
            }
            Datum::Bytes(bytes) | Datum::Raw(bytes) => {
                Datum::new_collation_string_datum(std::mem::take(bytes), field_type.collation())
            }
            _ => return Err(TableRowError::InvalidDatum("string")),
        },
        FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
            let packed = datum.as_uint().ok_or(TableRowError::InvalidDatum("time"))?;
            let kind = match code {
                FieldTypeCode::Date => TimeType::Date,
                FieldTypeCode::Timestamp => TimeType::Timestamp,
                _ => TimeType::DateTime,
            };
            let parts = PackedTime::from_raw(packed).parts();
            let mut value = Time::new(
                CoreTime::from_date(
                    parts.year,
                    parts.month,
                    parts.day,
                    parts.hour,
                    parts.minute,
                    parts.second,
                    parts.microsecond,
                ),
                kind,
                field_type.decimal(),
            )
            .map_err(|error| TableRowError::Datatype(error.to_string()))?;
            if kind == TimeType::Timestamp && !value.is_zero() {
                value
                    .convert_time_zone(&Utc, timezone.expect("Go Unflatten timezone"))
                    .map_err(|error| TableRowError::Datatype(error.to_string()))?;
            }
            Datum::Time(value)
        }
        // Go `datum.GetInt64()` reads `d.i`, which holds the nanoseconds of
        // both an int datum (row values) and the KindMysqlDuration datum an
        // index key decodes into.
        FieldTypeCode::Duration => Datum::Duration(MySqlDuration::from_raw_parts(
            match &datum {
                Datum::Duration(duration) => Some(duration.nanoseconds()),
                _ => datum.as_int(),
            }
            .ok_or(TableRowError::InvalidDatum("duration"))?,
            field_type.decimal(),
        )),
        FieldTypeCode::Enum => {
            let number = datum.as_uint().ok_or(TableRowError::InvalidDatum("enum"))?;
            let value = field_type
                .with_elems_visible(|elements| parse_enum_value(elements, number))
                .unwrap_or_else(|_| MysqlEnum::default());
            Datum::Enum(value, field_type.collation())
        }
        FieldTypeCode::Set => {
            let number = datum.as_uint().ok_or(TableRowError::InvalidDatum("set"))?;
            Datum::Set(
                field_type
                    .with_elems_visible(|elements| parse_set_value(elements, number))
                    .map_err(|error| TableRowError::Datatype(error.to_string()))?,
                field_type.collation(),
            )
        }
        FieldTypeCode::Bit => {
            let byte_size = (field_type.flen() + 7) >> 3;
            let width =
                BinaryLiteralWidth::try_from(u8::try_from(byte_size).expect("Invalid byteSize"))
                    .expect("Invalid byteSize");
            Datum::Bit(tidb_datatype::BinaryLiteral::from_uint(
                datum.as_uint().ok_or(TableRowError::InvalidDatum("bit"))?,
                Some(width),
            ))
        }
        _ => return Ok(()),
    };
    *datum = restored;
    Ok(())
}

/// Encodes one flattened datum as an old codec value.
pub fn encode_table_value(
    timezone: Option<&SessionTimeZone>,
    datum: &Datum,
) -> Result<Vec<u8>, TableRowError> {
    // These values are already flat, and their compact encoding is
    // independent of the time zone.
    let bytes = match datum {
        Datum::Bytes(bytes) => Some(bytes.as_slice()),
        Datum::String(string) => Some(string.bytes()),
        _ => None,
    };
    if let Some(bytes) = bytes {
        let mut output = Vec::with_capacity(11 + bytes.len());
        output.push(tidb_codec::COMPACT_BYTES_FLAG);
        tidb_codec::encode_compact_bytes(&mut output, bytes);
        return Ok(output);
    }
    let flattened = flatten_datum(timezone, datum)?;
    let values = std::slice::from_ref(flattened.as_ref());
    match timezone {
        Some(timezone) => Ok(encode_value_in_timezone(timezone, values)?),
        None => Ok(tidb_codec::encode_value(values)?),
    }
}

/// Encodes a row through either the new rowcodec format or the legacy
/// alternating `column ID, value` format.
pub fn encode_table_row(
    timezone: Option<&SessionTimeZone>,
    row: &[Datum],
    column_ids: &[i64],
    new_format: bool,
    checksum: Option<&RowChecksumPolicy>,
) -> Result<Vec<u8>, TableRowError> {
    encode_table_row_with_values(timezone, row, column_ids, new_format, checksum)
}

/// Encodes a row whose selected datums may be borrowed from another row.
pub fn encode_table_row_with_values(
    timezone: Option<&SessionTimeZone>,
    row: &[impl Borrow<Datum>],
    column_ids: &[i64],
    new_format: bool,
    checksum: Option<&RowChecksumPolicy>,
) -> Result<Vec<u8>, TableRowError> {
    if row.len() != column_ids.len() {
        return Err(TableRowError::ColumnCountMismatch {
            values: row.len(),
            column_ids: column_ids.len(),
        });
    }
    if new_format {
        let mut output = Vec::new();
        encode_row_with_values(
            timezone,
            column_ids,
            row,
            checksum.unwrap_or(&RowChecksumPolicy::None),
            &mut output,
        )?;
        return Ok(output);
    }
    encode_old_table_row_with_values(timezone, row, column_ids)
}

/// Encodes the legacy alternating `column ID, value` row layout.
pub fn encode_old_table_row(
    timezone: Option<&SessionTimeZone>,
    row: &[Datum],
    column_ids: &[i64],
) -> Result<Vec<u8>, TableRowError> {
    encode_old_table_row_with_values(timezone, row, column_ids)
}

fn encode_old_table_row_with_values(
    timezone: Option<&SessionTimeZone>,
    row: &[impl Borrow<Datum>],
    column_ids: &[i64],
) -> Result<Vec<u8>, TableRowError> {
    if row.len() != column_ids.len() {
        return Err(TableRowError::ColumnCountMismatch {
            values: row.len(),
            column_ids: column_ids.len(),
        });
    }
    if row.is_empty() {
        return Ok(vec![NIL_FLAG]);
    }
    let mut values = SmallVec::<[_; 8]>::with_capacity(row.len() * 2);
    for (column_id, datum) in column_ids.iter().zip(row) {
        values.push(Cow::Owned(Datum::Int(*column_id)));
        values.push(flatten_datum(timezone, datum.borrow())?);
    }
    match timezone {
        Some(timezone) => Ok(encode_value_refs_in_timezone(timezone, &values)?),
        None => Ok(encode_value_refs_in_timezone(&Utc, &values)?),
    }
}

/// Decodes one encoded scalar and restores its SQL column type.
pub fn decode_column_value(
    data: &[u8],
    field_type: &FieldType,
    timezone: Option<&SessionTimeZone>,
) -> Result<Datum, TableRowError> {
    decode_column_value_with_backing(data, field_type, timezone, None)
}

#[inline]
fn decode_column_value_with_backing(
    data: &[u8],
    field_type: &FieldType,
    timezone: Option<&SessionTimeZone>,
    backing: Option<&Arc<Vec<u8>>>,
) -> Result<Datum, TableRowError> {
    if let Some(decoded) = compact_string_payload(data, field_type) {
        let (_, bytes) = decoded?;
        let collation = field_type.collation();
        let string = match backing {
            Some(owner) if bytes.len() >= MIN_SHARED_ROW_VALUE_BYTES => {
                StringDatum::from_shared_subslice(owner, bytes, collation)
            }
            _ => StringDatum::from_slice(bytes, collation),
        };
        return Ok(Datum::String(string));
    }
    if let Some(owner) = backing.filter(|_| data.len() >= MIN_SHARED_ROW_VALUE_BYTES) {
        match data[0] {
            JSON_FLAG => {
                let (_, json) = decode_json(&data[1..])?;
                let json = BinaryJSON::from_shared_subslice(json.type_code(), owner, json.value())
                    .validate_layout()
                    .map_err(|_| CodecError::InvalidEncoding("invalid binary JSON"))?;
                return unflatten_datum(Datum::Json(json), field_type, timezone);
            }
            VECTOR_FLOAT32_FLAG => {
                let vector = VectorFloat32::from_shared_subslice(owner, &data[1..])
                    .map_err(|_| CodecError::InvalidEncoding("invalid vector float32"))?;
                return unflatten_datum(Datum::VectorFloat32(vector), field_type, timezone);
            }
            _ => {}
        }
    }
    let (_, datum) = decode_one(data)?;
    unflatten_datum(datum, field_type, timezone)
}

/// Decodes a scalar into an existing output slot.
pub fn decode_column_value_into(
    data: &[u8],
    field_type: &FieldType,
    timezone: Option<&SessionTimeZone>,
    output: &mut Datum,
) -> Result<(), TableRowError> {
    if let Some(decoded) = compact_string_payload(data, field_type) {
        let (_, bytes) = decoded?;
        output.set_copied_string(bytes, field_type.collation());
        return Ok(());
    }
    *output = decode_column_value(data, field_type, timezone)?;
    Ok(())
}

fn compact_string_payload<'a>(
    data: &'a [u8],
    field_type: &FieldType,
) -> Option<Result<(&'a [u8], &'a [u8]), CodecError>> {
    let (&flag, payload) = data.split_first()?;
    if flag == COMPACT_BYTES_FLAG && is_stored_string(field_type.code()) {
        Some(decode_compact_bytes(payload))
    } else {
        None
    }
}

/// Replaces an encoded byte datum with its decoded column value. Wide
/// compact strings can retain the existing allocation when it holds one value.
pub fn decode_column_value_in_place(
    value: &mut Datum,
    field_type: &FieldType,
    timezone: Option<&SessionTimeZone>,
) -> Result<(), TableRowError> {
    let Datum::Bytes(input) = value else {
        return Err(TableRowError::InvalidDatum("encoded column"));
    };
    if let Some(decoded) = compact_string_payload(input, field_type) {
        let (remain, bytes) = decoded?;
        let collation = field_type.collation();
        if remain.is_empty() && bytes.len() > StringDatum::INLINE_CAPACITY {
            let payload = input.len() - bytes.len()..input.len();
            let owner = Arc::new(std::mem::take(input));
            let bytes = tidb_goruntime::GoString::from_shared_vec(owner, payload);
            *value = Datum::String(StringDatum::from_go_string(bytes, collation));
        } else {
            *value = Datum::String(StringDatum::from_slice(bytes, collation));
        }
        return Ok(());
    }
    *value = decode_column_value(input, field_type, timezone)?;
    Ok(())
}

/// Decodes an old or new row into the requested column map.
pub fn decode_table_row_to_map(
    bytes: &[u8],
    columns: &BTreeMap<i64, FieldType>,
    timezone: Option<&SessionTimeZone>,
) -> Result<BTreeMap<i64, Datum>, TableRowError> {
    let mut result = BTreeMap::new();
    decode_table_row_into_map(bytes, columns, timezone, &mut result)?;
    Ok(result)
}

/// Decodes an owned row into the requested columns. Large payloads retain
/// the original row allocation, shared by all views of that row.
pub fn decode_owned_table_row_to_map(
    bytes: Vec<u8>,
    columns: &BTreeMap<i64, FieldType>,
    timezone: Option<&SessionTimeZone>,
) -> Result<BTreeMap<i64, Datum>, TableRowError> {
    if bytes.len() < MIN_SHARED_ROW_VALUE_BYTES {
        return decode_table_row_to_map(&bytes, columns, timezone);
    }
    let owner = Arc::new(bytes);
    let mut result = BTreeMap::new();
    if is_new_format(&owner) {
        let fields = columns.iter().map(|(&id, field_type)| (id, field_type));
        decode_shared_row_to_map_with_fields(&owner, fields, timezone, &mut result)?;
    } else {
        decode_legacy_row_into_map(&owner, columns, timezone, &mut result, Some(&owner))?;
    }
    Ok(result)
}

/// Decodes an old or new row into an existing column map.
///
/// Entries unrelated to the requested columns are preserved. Decoded columns
/// replace their previous values, matching Go `DecodeRowWithMap`.
pub fn decode_table_row_into_map(
    bytes: &[u8],
    columns: &BTreeMap<i64, FieldType>,
    timezone: Option<&SessionTimeZone>,
    result: &mut BTreeMap<i64, Datum>,
) -> Result<(), TableRowError> {
    let new_format = is_new_format(bytes);
    if bytes == [NIL_FLAG] {
        return Ok(());
    }
    if new_format {
        let fields = columns.iter().map(|(&id, field_type)| (id, field_type));
        decode_row_to_map_with_fields(bytes, fields, timezone, result)?;
        return Ok(());
    }
    decode_legacy_row_into_map(bytes, columns, timezone, result, None)
}

#[inline]
fn decode_legacy_row_into_map(
    bytes: &[u8],
    columns: &BTreeMap<i64, FieldType>,
    timezone: Option<&SessionTimeZone>,
    result: &mut BTreeMap<i64, Datum>,
    backing: Option<&Arc<Vec<u8>>>,
) -> Result<(), TableRowError> {
    let mut remaining = bytes;
    let mut decoded_count = 0;
    while !remaining.is_empty() && decoded_count < columns.len() {
        let (tail, column_id) = if let [VARINT_FLAG, id @ ..] = remaining {
            decode_varint(id)?
        } else {
            let (tail, id) = decode_one(remaining)?;
            let id = id.as_int().ok_or(TableRowError::InvalidDatum("column ID"))?;
            (tail, id)
        };
        let (encoded, tail) = cut_one(tail)?;
        remaining = tail;
        if let Some(field_type) = columns.get(&column_id) {
            match result.entry(column_id) {
                Entry::Occupied(mut slot) if backing.is_none() => {
                    decode_column_value_into(encoded, field_type, timezone, slot.get_mut())?;
                }
                Entry::Occupied(mut slot) => {
                    *slot.get_mut() = decode_column_value_with_backing(
                        encoded, field_type, timezone, backing,
                    )?;
                }
                Entry::Vacant(slot) => {
                    slot.insert(decode_column_value_with_backing(
                        encoded, field_type, timezone, backing,
                    )?);
                }
            }
            decoded_count += 1;
        }
    }
    Ok(())
}

/// Adds handle columns to an existing row map without overwriting stored row
/// values.
pub fn decode_handle_to_datum_map(
    handle: Option<&Handle>,
    handle_column_ids: &[i64],
    columns: &BTreeMap<i64, FieldType>,
    timezone: Option<&SessionTimeZone>,
    row: &mut BTreeMap<i64, Datum>,
) -> Result<(), TableRowError> {
    let Some(handle) = handle else {
        return Ok(());
    };
    if handle_column_ids.is_empty() {
        return Ok(());
    }
    for (index, column_id) in handle_column_ids.iter().enumerate() {
        let Some(field_type) = columns.get(column_id) else {
            continue;
        };
        if field_type.need_restored_data() {
            continue;
        }
        let Entry::Vacant(slot) = row.entry(*column_id) else {
            continue;
        };
        let raw = if let Some(value) = handle.int_value() {
            if field_type.is_unsigned() {
                Datum::UInt(value as u64)
            } else {
                Datum::Int(value)
            }
        } else {
            let encoded = handle
                .encoded_column(index)
                .expect("Go CommonHandle.EncodedCol index");
            decode_one(encoded)?.1
        };
        slot.insert(unflatten_datum(raw, field_type, timezone)?);
    }
    Ok(())
}

/// Cuts requested values from a legacy alternating row without decoding them.
pub fn cut_table_row(
    data: &[u8],
    column_offsets: &BTreeMap<i64, usize>,
) -> Result<Vec<Option<Vec<u8>>>, TableRowError> {
    cut_table_row_with(data, column_offsets, <[u8]>::to_vec)
}

/// Borrows requested encoded values from a legacy alternating row.
pub fn cut_table_row_refs<'a>(
    data: &'a [u8],
    column_offsets: &BTreeMap<i64, usize>,
) -> Result<Vec<Option<&'a [u8]>>, TableRowError> {
    cut_table_row_with(data, column_offsets, std::convert::identity)
}

fn cut_table_row_with<'a, T>(
    data: &'a [u8],
    column_offsets: &BTreeMap<i64, usize>,
    project: impl Fn(&'a [u8]) -> T,
) -> Result<Vec<Option<T>>, TableRowError> {
    if data.is_empty() || data == [NIL_FLAG] {
        return Ok(Vec::new());
    }
    let mut row = Vec::with_capacity(column_offsets.len());
    row.resize_with(column_offsets.len(), || None);
    let mut remaining = data;
    let mut found = 0;
    while !remaining.is_empty() && found < column_offsets.len() {
        let (tail, column_id) = cut_column_id(remaining)?;
        let (value, tail) = cut_one(tail)?;
        remaining = tail;
        if let Some(offset) = column_offsets.get(&column_id).copied() {
            row[offset] = Some(project(value));
            // Go counts every matching ID, including repeated IDs.
            found += 1;
        }
    }
    Ok(row)
}

/// Restores a complete datum slice in place.
pub fn unflatten_datums(
    datums: &mut [Datum],
    field_types: &[FieldType],
    timezone: Option<&SessionTimeZone>,
) -> Result<(), TableRowError> {
    for (index, datum) in datums.iter_mut().enumerate() {
        unflatten_datum_in_place(datum, &field_types[index], timezone)?;
    }
    Ok(())
}
