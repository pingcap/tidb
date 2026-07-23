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

use std::collections::BTreeMap;
use std::fmt;

use chrono::Utc;
use chrono_tz::Tz;
use tidb_datatype::{
    parse_enum_value, parse_set_value, BinaryLiteralIntOutcome, Collation, Datum, FieldType,
    FieldTypeCode, MySqlDuration, MysqlEnum, Time, TimeType,
};

use tidb_codec::{
    cut_column_id, cut_one, decode_one, decode_row_to_map, encode_row, encode_row_with_checksum,
    encode_value_in_timezone, is_new_format, CodecError, ColumnInfo, RowChecksumPolicy,
    RowPackageError, NIL_FLAG,
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
    /// A caller-provided output offset was outside the result.
    InvalidOutputOffset(usize),
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
            Self::InvalidOutputOffset(offset) => {
                write!(formatter, "invalid row output offset {offset}")
            }
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
pub fn flatten_datum(timezone: Option<&Tz>, datum: &Datum) -> Result<Datum, TableRowError> {
    Ok(match datum {
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
        other => other.clone(),
    })
}

/// Restores one persisted scalar according to its SQL column metadata.
pub fn unflatten_datum(
    datum: Datum,
    field_type: &FieldType,
    timezone: Option<&Tz>,
) -> Result<Datum, TableRowError> {
    if datum.is_null() {
        return Ok(datum);
    }
    let code = field_type.code();
    Ok(match code {
        FieldTypeCode::Float => Datum::Float32(
            datum
                .as_real()
                .ok_or(TableRowError::InvalidDatum("float"))? as f32 as f64,
        ),
        FieldTypeCode::Varchar
        | FieldTypeCode::String
        | FieldTypeCode::VarString
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::Blob
        | FieldTypeCode::LongBlob => Datum::new_collation_string(
            datum
                .into_raw_bytes()
                .ok_or(TableRowError::InvalidDatum("string"))?,
            field_type.collation(),
        ),
        FieldTypeCode::Tiny
        | FieldTypeCode::Short
        | FieldTypeCode::Year
        | FieldTypeCode::Int24
        | FieldTypeCode::Long
        | FieldTypeCode::LongLong
        | FieldTypeCode::Double => datum,
        FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
            let packed = datum.as_uint().ok_or(TableRowError::InvalidDatum("time"))?;
            let kind = match code {
                FieldTypeCode::Date => TimeType::Date,
                FieldTypeCode::Timestamp => TimeType::Timestamp,
                _ => TimeType::DateTime,
            };
            let mut value = Time::from_packed_uint(packed, kind, field_type.decimal())
                .map_err(|error| TableRowError::Datatype(error.to_string()))?;
            if kind == TimeType::Timestamp && !value.is_zero() {
                if let Some(timezone) = timezone {
                    value
                        .convert_time_zone(&Utc, timezone)
                        .map_err(|error| TableRowError::Datatype(error.to_string()))?;
                }
            }
            Datum::Time(value)
        }
        FieldTypeCode::Duration => Datum::Duration(
            MySqlDuration::from_nanoseconds(
                datum
                    .as_int()
                    .ok_or(TableRowError::InvalidDatum("duration"))?,
                field_type.decimal(),
            )
            .map_err(|error| TableRowError::Datatype(error.to_string()))?,
        ),
        FieldTypeCode::Enum => {
            let number = datum.as_uint().ok_or(TableRowError::InvalidDatum("enum"))?;
            let value = parse_enum_value(field_type.elems(), number)
                .unwrap_or_else(|_| MysqlEnum::default());
            Datum::Enum(value, field_type.collation())
        }
        FieldTypeCode::Set => Datum::Set(
            parse_set_value(
                field_type.elems(),
                datum.as_uint().ok_or(TableRowError::InvalidDatum("set"))?,
            )
            .map_err(|error| TableRowError::Datatype(error.to_string()))?,
            field_type.collation(),
        ),
        FieldTypeCode::Bit => {
            let byte_size = ((field_type.flen().max(0) + 7) >> 3) as u8;
            Datum::Bit(tidb_datatype::BinaryLiteral::from_uint(
                datum.as_uint().ok_or(TableRowError::InvalidDatum("bit"))?,
                tidb_datatype::BinaryLiteralWidth::try_from(byte_size)
                    .map_err(|error| TableRowError::Datatype(error.to_string()))?
                    .into(),
            ))
        }
        _ => datum,
    })
}

/// Encodes one flattened datum as an old codec value.
pub fn encode_table_value(timezone: Option<&Tz>, datum: &Datum) -> Result<Vec<u8>, TableRowError> {
    let flattened = flatten_datum(timezone, datum)?;
    match timezone {
        Some(timezone) => Ok(encode_value_in_timezone(timezone, &[flattened])?),
        None => Ok(tidb_codec::encode_value(&[flattened])?),
    }
}

/// Encodes a row through either the new rowcodec format or the legacy
/// alternating `column ID, value` format.
pub fn encode_table_row(
    timezone: Option<&Tz>,
    row: &[Datum],
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
        match checksum {
            Some(policy) => {
                encode_row_with_checksum(timezone, column_ids, row, policy, &mut output)?;
            }
            None => encode_row(timezone, column_ids, row, &mut output)?,
        }
        return Ok(output);
    }
    encode_old_table_row(timezone, row, column_ids)
}

/// Encodes the legacy alternating `column ID, value` row layout.
pub fn encode_old_table_row(
    timezone: Option<&Tz>,
    row: &[Datum],
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
    let mut values = Vec::with_capacity(row.len() * 2);
    for (column_id, datum) in column_ids.iter().zip(row) {
        values.push(Datum::Int(*column_id));
        values.push(flatten_datum(timezone, datum)?);
    }
    match timezone {
        Some(timezone) => Ok(encode_value_in_timezone(timezone, &values)?),
        None => Ok(tidb_codec::encode_value(&values)?),
    }
}

/// Decodes one encoded scalar and restores its SQL column type.
pub fn decode_column_value(
    data: &[u8],
    field_type: &FieldType,
    timezone: Option<&Tz>,
) -> Result<Datum, TableRowError> {
    let (_, datum) = decode_one(data)?;
    unflatten_datum(datum, field_type, timezone)
}

/// Decodes a scalar into an existing output slot.
pub fn decode_column_value_into(
    data: &[u8],
    field_type: &FieldType,
    timezone: Option<&Tz>,
    output: &mut Datum,
) -> Result<(), TableRowError> {
    *output = decode_column_value(data, field_type, timezone)?;
    Ok(())
}

/// Decodes an old or new row into the requested column map.
pub fn decode_table_row_to_map(
    bytes: &[u8],
    columns: &BTreeMap<i64, FieldType>,
    timezone: Option<&Tz>,
) -> Result<BTreeMap<i64, Datum>, TableRowError> {
    let mut result = BTreeMap::new();
    decode_table_row_into_map(bytes, columns, timezone, &mut result)?;
    Ok(result)
}

/// Decodes an old or new row into an existing column map.
///
/// Entries unrelated to the requested columns are preserved. Decoded columns
/// replace their previous values, matching Go `DecodeRowWithMap`.
pub fn decode_table_row_into_map(
    bytes: &[u8],
    columns: &BTreeMap<i64, FieldType>,
    timezone: Option<&Tz>,
    result: &mut BTreeMap<i64, Datum>,
) -> Result<(), TableRowError> {
    if bytes.is_empty() || bytes == [NIL_FLAG] {
        return Ok(());
    }
    if is_new_format(bytes) {
        let columns = columns
            .iter()
            .map(|(id, field_type)| ColumnInfo {
                id: *id,
                is_pk_handle: false,
                virtual_generated: false,
                field_type: field_type.clone(),
            })
            .collect::<Vec<_>>();
        result.extend(decode_row_to_map(bytes, &columns, timezone)?);
        return Ok(());
    }

    let mut remaining = bytes;
    let mut decoded_count = 0;
    while !remaining.is_empty() && decoded_count < columns.len() {
        let (tail, column_id) = decode_one(remaining)?;
        let column_id = column_id
            .as_int()
            .ok_or(TableRowError::InvalidDatum("column ID"))?;
        let (encoded, tail) = cut_one(tail)?;
        remaining = tail;
        if let Some(field_type) = columns.get(&column_id) {
            result.insert(
                column_id,
                decode_column_value(encoded, field_type, timezone)?,
            );
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
    timezone: Option<&Tz>,
    row: &mut BTreeMap<i64, Datum>,
) -> Result<(), TableRowError> {
    let Some(handle) = handle else {
        return Ok(());
    };
    if handle_column_ids.is_empty() {
        return Ok(());
    }
    let mut encoded_columns: Option<Vec<Vec<u8>>> = None;
    for (index, column_id) in handle_column_ids.iter().enumerate() {
        let Some(field_type) = columns.get(column_id) else {
            continue;
        };
        if field_type.need_restored_data() || row.contains_key(column_id) {
            continue;
        }
        let raw = if let Some(value) = handle.int_value() {
            if field_type.is_unsigned() {
                Datum::UInt(value as u64)
            } else {
                Datum::Int(value)
            }
        } else {
            if encoded_columns.is_none() {
                let count = handle
                    .num_columns()
                    .ok_or(TableRowError::InvalidDatum("common handle"))?;
                encoded_columns = Some(
                    (0..count)
                        .map(|index| {
                            handle
                                .encoded_column(index)
                                .expect("index is bounded by the parsed column count")
                                .to_vec()
                        })
                        .collect(),
                );
            }
            let encoded = encoded_columns
                .as_ref()
                .unwrap()
                .get(index)
                .ok_or(TableRowError::InvalidDatum("common handle column"))?;
            decode_one(encoded)?.1
        };
        row.insert(*column_id, unflatten_datum(raw, field_type, timezone)?);
    }
    Ok(())
}

/// Cuts requested values from a legacy alternating row without decoding them.
pub fn cut_table_row(
    data: &[u8],
    column_offsets: &BTreeMap<i64, usize>,
) -> Result<Vec<Option<Vec<u8>>>, TableRowError> {
    if data.is_empty() || data == [NIL_FLAG] {
        return Ok(Vec::new());
    }
    let mut row = vec![None; column_offsets.len()];
    let mut remaining = data;
    let mut found = 0;
    while !remaining.is_empty() && found < column_offsets.len() {
        let (tail, column_id) = cut_column_id(remaining)?;
        let (value, tail) = cut_one(tail)?;
        remaining = tail;
        if let Some(offset) = column_offsets.get(&column_id).copied() {
            let slot = row
                .get_mut(offset)
                .ok_or(TableRowError::InvalidOutputOffset(offset))?;
            if slot.is_none() {
                found += 1;
            }
            *slot = Some(value.to_vec());
        }
    }
    Ok(row)
}

/// Restores a complete datum slice in place.
pub fn unflatten_datums(
    datums: &mut [Datum],
    field_types: &[FieldType],
    timezone: Option<&Tz>,
) -> Result<(), TableRowError> {
    if datums.len() != field_types.len() {
        return Err(TableRowError::ColumnCountMismatch {
            values: datums.len(),
            column_ids: field_types.len(),
        });
    }
    for (datum, field_type) in datums.iter_mut().zip(field_types) {
        *datum = unflatten_datum(datum.clone(), field_type, timezone)?;
    }
    Ok(())
}

/// Returns the binary collation used by tablecodec's test/setup path.
#[must_use]
pub const fn tablecodec_binary_collation() -> Collation {
    Collation::Binary
}
