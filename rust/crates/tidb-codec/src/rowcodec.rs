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

//! Complete typed package boundary for `pkg/util/rowcodec`.
//!
//! The lower-level row modules expose reusable framing primitives. This module
//! composes them with TiDB datums, field metadata, handles, defaults, checksum
//! policy, and old-datum conversion so consumers never need an "almost
//! rowcodec" adapter.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;

use chrono::Utc;
use crc32fast::Hasher;
use tidb_datatype::{
    deserialize_vector_float32, parse_enum_value, parse_set_value, BinaryJSON, BinaryLiteral,
    BinaryLiteralWidth, Datum, FieldType, FieldTypeCode, MySqlDuration, SessionTimeZone, Time,
    TimeType,
};

use crate::{
    decode_decimal, decode_float, decode_raw_int, decode_raw_uint, encode_compact_bytes,
    encode_decimal_fixed, encode_float, encode_raw_int, encode_raw_row, encode_raw_uint,
    encode_uvarint, encode_varint, CodecError, RawRowColumn, RawRowValue, RowDecoder,
    RowEncodeError, COMPACT_BYTES_FLAG, DECIMAL_FLAG, FLOAT_FLAG, INT_FLAG, JSON_FLAG, NIL_FLAG,
    UINT_FLAG, UVARINT_FLAG, VARINT_FLAG, VECTOR_FLOAT32_FLAG,
};

const CHECKSUM_VERSION_RAW_HANDLE: u8 = 2;
const KEYSPACE_PREFIX_LEN: usize = 4;
const API_V2_TXN_MODE_PREFIX: u8 = b'x';

/// Metadata needed by the package decoders.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnInfo {
    /// Stable TiDB column ID.
    pub id: i64,
    /// Whether this column is the integer primary-key handle.
    pub is_pk_handle: bool,
    /// Whether execution fills this generated column after row decoding.
    pub virtual_generated: bool,
    /// SQL type metadata.
    pub field_type: FieldType,
}

/// Returns the field metadata carried by a model-derived column.
#[must_use]
pub fn field_type_from_column(column: &ColumnInfo) -> FieldType {
    column.field_type.clone()
}

/// One integer or common-handle value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Handle {
    /// Signed integer row handle.
    Int(i64),
    /// Old-datum encoded components of a common handle.
    Common(Vec<Vec<u8>>),
}

impl Handle {
    fn checksum_bytes(&self) -> Vec<u8> {
        match self {
            Self::Int(value) => {
                let mut bytes = Vec::with_capacity(8);
                crate::encode_int(&mut bytes, *value);
                bytes
            }
            Self::Common(parts) => parts.concat(),
        }
    }
}

/// Checksum policy accepted by [`encode_row_with_checksum`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowChecksumPolicy {
    /// Do not append checksum bytes.
    None,
    /// Append checksum version 2 over the row bytes, header, and encoded handle.
    RawHandle(Handle),
}

/// One schema column and datum used by column-level checksum calculation.
#[derive(Clone, Debug, PartialEq)]
pub struct DatumColumn {
    /// Stable column ID, used only for sorting.
    pub id: i64,
    /// SQL type used by TiCDC-compatible checksum encoding.
    pub field_type: FieldType,
    /// Column datum.
    pub datum: Datum,
}

impl DatumColumn {
    /// Encodes this column datum for TiCDC-compatible checksum calculation.
    pub fn encode(
        &self,
        timezone: Option<&SessionTimeZone>,
        buffer: &mut Vec<u8>,
    ) -> Result<(), RowPackageError> {
        append_datum_for_checksum(timezone, buffer, &self.datum, self.field_type.code())
    }
}

/// Caller-ordered row data used by TiCDC-compatible column-level checksums.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RowData {
    /// Columns in encoding order. Callers must sort by ID when schema order is
    /// required, matching Go's `RowData` contract.
    pub columns: Vec<DatumColumn>,
    /// Reusable output buffer.
    pub data: Vec<u8>,
}

impl RowData {
    /// Encodes all columns in caller-provided order.
    pub fn encode(&mut self, timezone: Option<&SessionTimeZone>) -> Result<&[u8], RowPackageError> {
        self.data.clear();
        for column in &self.columns {
            append_datum_for_checksum(
                timezone,
                &mut self.data,
                &column.datum,
                column.field_type.code(),
            )?;
        }
        Ok(&self.data)
    }

    /// Calculates the same incremental IEEE CRC32 as Go `RowData.Checksum`.
    pub fn checksum(&mut self, timezone: Option<&SessionTimeZone>) -> Result<u32, RowPackageError> {
        let mut hasher = Hasher::new();
        for column in &self.columns {
            self.data.clear();
            append_datum_for_checksum(
                timezone,
                &mut self.data,
                &column.datum,
                column.field_type.code(),
            )?;
            hasher.update(&self.data);
        }
        Ok(hasher.finalize())
    }
}

/// Materialized row result used by the chunk-equivalent decoder.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedRow {
    /// One output value per requested column.
    pub values: Vec<Datum>,
    /// Parsed primary checksum, when present.
    pub checksum: Option<u32>,
    /// Parsed checksum version.
    pub checksum_version: u8,
}

/// Runtime inputs for the chunk-equivalent row decoder.
#[derive(Clone, Debug, Default)]
pub struct DecodeRowOptions<'a> {
    /// Column IDs stored by an integer or common handle.
    pub handle_column_ids: &'a [i64],
    /// Row handle, when this scan has one.
    pub handle: Option<&'a Handle>,
    /// Default output values in requested-column order.
    pub defaults: Option<&'a [Datum]>,
    /// Optional pseudo-column receiving the commit timestamp.
    pub commit_ts_column_id: Option<i64>,
    /// Optional pseudo-column reserved for a row checksum.
    pub row_checksum_column_id: Option<i64>,
    /// Commit timestamp value; zero produces SQL NULL.
    pub commit_ts: u64,
    /// Session timezone for timestamp restoration.
    pub timezone: Option<&'a SessionTimeZone>,
}

/// Errors returned by the complete rowcodec boundary.
#[derive(Debug)]
pub enum RowPackageError {
    /// A datum kind does not match the operation.
    UnsupportedDatum(&'static str),
    /// Go `encodeValueDatum` received a kind outside its switch.
    UnsupportedEncodeType(u8),
    /// Go `multierr.Append` accumulated several encoding errors.
    EncodeErrors(Vec<RowPackageError>),
    /// A field type is not handled by rowcodec.
    UnknownFieldType(FieldTypeCode),
    /// Go `errInvalidChecksumTyp`.
    InvalidChecksumType,
    /// Go's recovered type assertion from `appendDatumForChecksum`.
    ChecksumDatumType {
        /// `Datum.String()` at the failure site.
        datum: String,
        /// `types.TypeStr(typ)`.
        field_type: &'static str,
        /// Asserted Go payload type.
        expected: &'static str,
    },
    /// Go's recovered panic from a checksum datum accessor.
    ChecksumDatumValue {
        /// `Datum.String()` at the failure site.
        datum: String,
        /// `types.TypeStr(typ)`.
        field_type: &'static str,
        /// Recovered accessor error.
        cause: String,
    },
    /// A persisted scalar is malformed.
    InvalidValue(&'static str),
    /// A lower-level row framing error.
    Decode(crate::RowDecodeError),
    /// A lower-level row construction error.
    Encode(RowEncodeError),
    /// A scalar codec error.
    Codec(CodecError),
    /// A datatype operation failed.
    Datatype(String),
}

impl fmt::Display for RowPackageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedDatum(kind) => write!(formatter, "unsupported row datum {kind}"),
            Self::UnsupportedEncodeType(kind) => write!(formatter, "unsupport encode type {kind}"),
            Self::EncodeErrors(errors) => {
                for (index, error) in errors.iter().enumerate() {
                    if index != 0 {
                        formatter.write_str("; ")?;
                    }
                    error.fmt(formatter)?;
                }
                Ok(())
            }
            Self::UnknownFieldType(code) => write!(formatter, "unknown row field type {code:?}"),
            Self::InvalidChecksumType => formatter.write_str("invalid type for checksum"),
            Self::ChecksumDatumType {
                datum,
                field_type,
                expected,
            } => write!(
                formatter,
                "encode datum({datum}) as {field_type} for checksum: interface conversion: interface {{}} is nil, not {expected}"
            ),
            Self::ChecksumDatumValue {
                datum,
                field_type,
                cause,
            } => write!(
                formatter,
                "encode datum({datum}) as {field_type} for checksum: {cause}"
            ),
            Self::InvalidValue(kind) => write!(formatter, "invalid persisted {kind} value"),
            Self::Decode(error) => error.fmt(formatter),
            Self::Encode(error) => error.fmt(formatter),
            Self::Codec(error) => error.fmt(formatter),
            Self::Datatype(error) => formatter.write_str(error),
        }
    }
}

impl std::error::Error for RowPackageError {}

impl From<crate::RowDecodeError> for RowPackageError {
    fn from(error: crate::RowDecodeError) -> Self {
        Self::Decode(error)
    }
}

impl From<RowEncodeError> for RowPackageError {
    fn from(error: RowEncodeError) -> Self {
        Self::Encode(error)
    }
}

impl From<CodecError> for RowPackageError {
    fn from(error: CodecError) -> Self {
        Self::Codec(error)
    }
}

/// Encodes a typed new-format row without a checksum.
pub fn encode_row(
    timezone: Option<&SessionTimeZone>,
    column_ids: &[i64],
    values: &[Datum],
    buffer: &mut Vec<u8>,
) -> Result<(), RowPackageError> {
    encode_row_with_checksum(
        timezone,
        column_ids,
        values,
        &RowChecksumPolicy::None,
        buffer,
    )
}

/// Encodes a typed new-format row and applies the requested checksum policy.
pub fn encode_row_with_checksum(
    timezone: Option<&SessionTimeZone>,
    column_ids: &[i64],
    values: &[Datum],
    checksum: &RowChecksumPolicy,
    buffer: &mut Vec<u8>,
) -> Result<(), RowPackageError> {
    let mut payloads = Vec::with_capacity(column_ids.len());
    let mut errors = Vec::new();
    for (index, &id) in column_ids.iter().enumerate() {
        match encode_value_datum(timezone, &values[index]) {
            Ok(value) => payloads.push((id, value)),
            Err(error) => errors.push(error),
        }
    }
    match errors.len() {
        0 => {}
        1 => return Err(errors.pop().expect("one encoding error")),
        _ => return Err(RowPackageError::EncodeErrors(errors)),
    }
    let entries = payloads
        .iter()
        .map(|(id, value)| RawRowColumn {
            id: *id,
            value: value.as_deref(),
        })
        .collect::<Vec<_>>();

    let start = buffer.len();
    encode_raw_row(&entries, buffer)?;
    if let RowChecksumPolicy::RawHandle(handle) = checksum {
        buffer[start + 1] |= crate::ROW_FLAG_CHECKSUM;
        buffer.push(CHECKSUM_VERSION_RAW_HANDLE);
        let mut hasher = Hasher::new();
        hasher.update(&buffer[start..]);
        hasher.update(&handle.checksum_bytes());
        buffer.extend_from_slice(&hasher.finalize().to_le_bytes());
    }
    Ok(())
}

fn encode_value_datum(
    timezone: Option<&SessionTimeZone>,
    datum: &Datum,
) -> Result<Option<Vec<u8>>, RowPackageError> {
    let mut output = Vec::new();
    match datum {
        Datum::Null => return Ok(None),
        Datum::Int(value) => encode_raw_int(&mut output, *value),
        Datum::UInt(value) => encode_raw_uint(&mut output, *value),
        Datum::String(value) => output.extend_from_slice(value.bytes()),
        Datum::Bytes(value) => output.extend_from_slice(value),
        Datum::Time(value) => {
            let mut value = *value;
            if value.kind() == TimeType::Timestamp {
                if let Some(timezone) = timezone {
                    value
                        .convert_time_zone(timezone, &Utc)
                        .map_err(|error| RowPackageError::Datatype(error.to_string()))?;
                }
            }
            encode_raw_uint(
                &mut output,
                value
                    .to_packed_uint()
                    .map_err(|error| RowPackageError::Datatype(error.to_string()))?,
            );
        }
        Datum::Duration(value) => encode_raw_int(&mut output, value.nanoseconds()),
        Datum::Enum(value, _) => encode_raw_uint(&mut output, value.value()),
        Datum::Set(value, _) => encode_raw_uint(&mut output, value.value()),
        Datum::BinaryLiteral(value) | Datum::Bit(value) => {
            encode_raw_uint(&mut output, binary_literal_to_uint(value))
        }
        Datum::Real(value) | Datum::Float32(value) => encode_float(&mut output, *value),
        Datum::Decimal(value) => {
            // Go: `codec.EncodeDecimal(buffer, d.GetMysqlDecimal(), d.Length(),
            // d.Frac())` — the DECLARED column shape, not the value's natural
            // one. `(0, 0)` (no column involved) is Go's unset `Datum.length`,
            // which `EncodeDecimal` itself resolves to `PrecisionAndFrac`.
            let (precision, scale) = value.storage_shape();
            encode_decimal_fixed(&mut output, value, precision as usize, scale as usize)?;
        }
        Datum::Json(value) => output.extend_from_slice(&value.encoded()),
        Datum::VectorFloat32(value) => value.serialize_to(&mut output),
        Datum::MinNotNull => return Err(RowPackageError::UnsupportedEncodeType(15)),
        Datum::MaxValue => return Err(RowPackageError::UnsupportedEncodeType(16)),
        Datum::Raw(_) => return Err(RowPackageError::UnsupportedEncodeType(17)),
    }
    Ok(Some(output))
}

/// Decodes present and explicit-NULL columns into a map. Missing columns are
/// omitted, matching Go `DatumMapDecoder`.
pub fn decode_row_to_map(
    row_data: &[u8],
    columns: &[ColumnInfo],
    timezone: Option<&SessionTimeZone>,
) -> Result<BTreeMap<i64, Datum>, RowPackageError> {
    let (decoder, _) = RowDecoder::parse(row_data)?;
    let mut output = BTreeMap::new();
    for column in columns {
        match decoder.column(column.id)? {
            RawRowValue::NotNull { bytes, .. } => {
                output.insert(
                    column.id,
                    decode_column_datum(bytes, &column.field_type, timezone)?,
                );
            }
            RawRowValue::Null => {
                output.insert(column.id, Datum::Null);
            }
            RawRowValue::Missing => {}
        }
    }
    Ok(output)
}

/// Decodes a row into requested output columns, including defaults, integer
/// and common handles, generated-column NULLs, commit TS, and checksum pseudo
/// columns.
pub fn decode_row_to_datums(
    row_data: &[u8],
    columns: &[ColumnInfo],
    options: &DecodeRowOptions<'_>,
) -> Result<DecodedRow, RowPackageError> {
    let (decoder, _) = RowDecoder::parse(row_data)?;
    let layout = decoder.layout();
    let mut values = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        if Some(column.id) == options.commit_ts_column_id {
            values.push(if options.commit_ts == 0 {
                Datum::Null
            } else {
                Datum::UInt(options.commit_ts)
            });
            continue;
        }
        if column.virtual_generated || Some(column.id) == options.row_checksum_column_id {
            values.push(Datum::Null);
            continue;
        }
        match decoder.column(column.id)? {
            RawRowValue::NotNull { bytes, .. } => {
                values.push(decode_column_datum(
                    bytes,
                    &column.field_type,
                    options.timezone,
                )?);
            }
            RawRowValue::Null => values.push(Datum::Null),
            RawRowValue::Missing => {
                if let Some(value) = decode_handle_column(
                    column,
                    options.handle_column_ids,
                    options.handle,
                    options.timezone,
                )? {
                    values.push(value);
                } else {
                    values.push(
                        options
                            .defaults
                            .and_then(|values| values.get(index))
                            .cloned()
                            .unwrap_or(Datum::Null),
                    );
                }
            }
        }
    }
    Ok(DecodedRow {
        values,
        checksum: layout.checksum().map(|checksum| checksum.checksum()),
        checksum_version: layout.checksum().map_or(0, |checksum| checksum.version()),
    })
}

/// Recalculates a row's bytes-level checksum after replacing the persisted
/// payloads for the supplied columns.
///
/// Checksum version 1 extends the CRC with the raw key; later versions extend
/// it with the encoded handle, preserving the v8.3 compatibility branch.
pub fn calculate_raw_checksum(
    row_data: &[u8],
    timezone: Option<&SessionTimeZone>,
    column_ids: &[i64],
    values: &[Datum],
    key: &[u8],
    handle: &Handle,
) -> Result<u32, RowPackageError> {
    let (decoder, _) = RowDecoder::parse(row_data)?;
    let layout = decoder.layout();
    let checksum = layout
        .checksum()
        .ok_or(RowPackageError::InvalidValue("row checksum"))?;
    let header = layout.header();
    let data_start = crate::ROW_HEADER_LEN
        + header.column_count() * header.column_id_width()
        + usize::from(header.not_null_count()) * header.offset_width();
    let body_end = data_start + layout.data().len();
    let mut body = row_data
        .get(..body_end)
        .ok_or(RowPackageError::InvalidValue("row body"))?
        .to_vec();
    for (index, &column_id) in column_ids.iter().enumerate() {
        let value = &values[index];
        let RawRowValue::NotNull { index, .. } = decoder.column(column_id)? else {
            continue;
        };
        let Some(encoded) = encode_value_datum(timezone, value)? else {
            continue;
        };
        let range = layout
            .value_range(index)
            .map_err(crate::RowDecodeError::from)?;
        let destination = &mut body[data_start + range.start..data_start + range.end];
        let copied = destination.len().min(encoded.len());
        destination[..copied].copy_from_slice(&encoded[..copied]);
    }
    body.push(checksum.header());
    let mut hasher = Hasher::new();
    hasher.update(&body);
    if checksum.version() == 1 {
        hasher.update(key);
    } else {
        hasher.update(&handle.checksum_bytes());
    }
    Ok(hasher.finalize())
}

/// Converts a new-format row back to the old per-column datum encodings used
/// by legacy tablecodec consumers.
pub fn decode_row_to_old_bytes(
    row_data: &[u8],
    columns: &[ColumnInfo],
    output_offsets: &BTreeMap<i64, usize>,
    handle_column_ids: &[i64],
    handle: Option<&Handle>,
    default_bytes: Option<&[Option<Vec<u8>>]>,
) -> Result<Vec<Vec<u8>>, RowPackageError> {
    let (decoder, _) = RowDecoder::parse(row_data)?;
    let mut values = vec![Vec::new(); output_offsets.len()];
    for (index, column) in columns.iter().enumerate() {
        let offset = output_offsets.get(&column.id).copied().unwrap_or_default();
        values[offset] = match decoder.column(column.id)? {
            RawRowValue::NotNull { bytes, .. } => encode_old_raw(column, bytes)?,
            RawRowValue::Null => vec![NIL_FLAG],
            RawRowValue::Missing => {
                if let Some(encoded) = encode_old_handle(column, handle_column_ids, handle)? {
                    encoded
                } else {
                    default_bytes
                        .and_then(|defaults| defaults.get(index))
                        .and_then(Clone::clone)
                        .filter(|value| !value.is_empty())
                        .unwrap_or_else(|| vec![NIL_FLAG])
                }
            }
        };
    }
    Ok(values)
}

fn encode_old_raw(column: &ColumnInfo, bytes: &[u8]) -> Result<Vec<u8>, RowPackageError> {
    let flag = field_type_to_flag(&column.field_type);
    let mut output = Vec::with_capacity(bytes.len() + 10);
    match flag {
        crate::BYTES_FLAG => {
            output.push(COMPACT_BYTES_FLAG);
            encode_compact_bytes(&mut output, bytes);
        }
        INT_FLAG => {
            output.push(VARINT_FLAG);
            encode_varint(&mut output, decode_raw_int(bytes)?);
        }
        UINT_FLAG => {
            output.push(UVARINT_FLAG);
            encode_uvarint(&mut output, decode_raw_uint(bytes)?);
        }
        _ => {
            output.push(flag);
            output.extend_from_slice(bytes);
        }
    }
    Ok(output)
}

fn encode_old_handle(
    column: &ColumnInfo,
    handle_column_ids: &[i64],
    handle: Option<&Handle>,
) -> Result<Option<Vec<u8>>, RowPackageError> {
    if column.field_type.need_restored_data() {
        return Ok(None);
    }
    match handle {
        Some(Handle::Int(value))
            if column.is_pk_handle || handle_column_ids.first() == Some(&column.id) =>
        {
            let mut output = Vec::with_capacity(9);
            if column.field_type.is_unsigned() {
                output.push(UINT_FLAG);
                crate::encode_uint(&mut output, *value as u64);
            } else {
                output.push(INT_FLAG);
                crate::encode_int(&mut output, *value);
            }
            Ok(Some(output))
        }
        Some(Handle::Common(parts)) => Ok(handle_column_ids
            .iter()
            .position(|id| *id == column.id)
            .and_then(|index| parts.get(index))
            .cloned()),
        _ => Ok(None),
    }
}

fn decode_handle_column(
    column: &ColumnInfo,
    handle_column_ids: &[i64],
    handle: Option<&Handle>,
    timezone: Option<&SessionTimeZone>,
) -> Result<Option<Datum>, RowPackageError> {
    let Some(handle) = handle else {
        return Ok(None);
    };
    match handle {
        Handle::Int(value)
            if column.is_pk_handle || handle_column_ids.first() == Some(&column.id) =>
        {
            Ok(Some(if column.field_type.is_unsigned() {
                Datum::UInt(*value as u64)
            } else {
                Datum::Int(*value)
            }))
        }
        Handle::Common(parts) => {
            let Some(index) = handle_column_ids.iter().position(|id| *id == column.id) else {
                return Ok(None);
            };
            let Some(encoded) = parts.get(index) else {
                return Ok(None);
            };
            let (remainder, datum) =
                crate::decode_one_typed_in_timezone(encoded, &column.field_type, timezone)?;
            if !remainder.is_empty() {
                return Err(RowPackageError::InvalidValue("common handle"));
            }
            Ok(Some(datum))
        }
        Handle::Int(_) => Ok(None),
    }
}

fn decode_column_datum(
    bytes: &[u8],
    field_type: &FieldType,
    timezone: Option<&SessionTimeZone>,
) -> Result<Datum, RowPackageError> {
    let code = field_type.code();
    Ok(match code {
        FieldTypeCode::Tiny
        | FieldTypeCode::Short
        | FieldTypeCode::Long
        | FieldTypeCode::LongLong
        | FieldTypeCode::Int24 => {
            if field_type.is_unsigned() {
                Datum::UInt(decode_raw_uint(bytes)?)
            } else {
                Datum::Int(decode_raw_int(bytes)?)
            }
        }
        FieldTypeCode::Year => Datum::Int(decode_raw_int(bytes)?),
        FieldTypeCode::Float => {
            let (_, value) = decode_float(bytes)?;
            Datum::Float32(f64::from(value as f32))
        }
        FieldTypeCode::Double => {
            let (_, value) = decode_float(bytes)?;
            Datum::Real(value)
        }
        FieldTypeCode::VarString
        | FieldTypeCode::Varchar
        | FieldTypeCode::String
        | FieldTypeCode::Blob
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::LongBlob => {
            Datum::new_collation_string(bytes.to_vec(), field_type.collation())
        }
        FieldTypeCode::NewDecimal => {
            let (_, mut value, _, encoded_scale) = decode_decimal(bytes)?;
            if field_type.decimal() >= 0 && i64::from(encoded_scale) > field_type.decimal() {
                value = value.round_to_scale(field_type.decimal() as i32);
            }
            Datum::Decimal(value)
        }
        FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
            let kind = match code {
                FieldTypeCode::Date => TimeType::Date,
                FieldTypeCode::Timestamp => TimeType::Timestamp,
                _ => TimeType::DateTime,
            };
            let mut value =
                Time::from_packed_uint(decode_raw_uint(bytes)?, kind, field_type.decimal())
                    .map_err(|error| RowPackageError::Datatype(error.to_string()))?;
            if kind == TimeType::Timestamp {
                if let Some(timezone) = timezone {
                    value
                        .convert_time_zone(&Utc, timezone)
                        .map_err(|error| RowPackageError::Datatype(error.to_string()))?;
                }
            }
            Datum::Time(value)
        }
        FieldTypeCode::Duration => Datum::Duration(
            MySqlDuration::from_nanoseconds(decode_raw_int(bytes)?, field_type.decimal())
                .map_err(|error| RowPackageError::Datatype(error.to_string()))?,
        ),
        FieldTypeCode::Enum => {
            let value = decode_raw_uint(bytes)?;
            Datum::Enum(
                field_type
                    .with_elems_visible(|elements| parse_enum_value(elements, value))
                    .unwrap_or_default(),
                field_type.collation(),
            )
        }
        FieldTypeCode::Set => {
            let value = decode_raw_uint(bytes)?;
            Datum::Set(
                field_type
                    .with_elems_visible(|elements| parse_set_value(elements, value))
                    .map_err(|error| RowPackageError::Datatype(error.to_string()))?,
                field_type.collation(),
            )
        }
        FieldTypeCode::Bit => {
            let byte_size = ((field_type.flen().max(0) + 7) >> 3) as u8;
            let width = BinaryLiteralWidth::try_from(byte_size)
                .map_err(|error| RowPackageError::Datatype(error.to_string()))?;
            Datum::Bit(BinaryLiteral::from_uint(
                decode_raw_uint(bytes)?,
                Some(width),
            ))
        }
        FieldTypeCode::Json => {
            let (&type_code, value) = bytes
                .split_first()
                .ok_or(RowPackageError::InvalidValue("JSON"))?;
            Datum::Json(BinaryJSON::from_encoded_parts(type_code, value.to_vec()))
        }
        FieldTypeCode::VectorFloat32 => {
            let (value, _) = deserialize_vector_float32(bytes)
                .map_err(|error| RowPackageError::Datatype(error.to_string()))?;
            Datum::VectorFloat32(value)
        }
        _ => return Err(RowPackageError::UnknownFieldType(code)),
    })
}

fn binary_literal_to_uint(value: &BinaryLiteral) -> u64 {
    binary_literal_bytes_to_uint(value.as_bytes())
}

fn binary_literal_bytes_to_uint(bytes: &[u8]) -> u64 {
    if bytes.len() > 8 && bytes[..bytes.len() - 8].iter().any(|byte| *byte != 0) {
        return u64::MAX;
    }
    bytes
        .iter()
        .rev()
        .take(8)
        .enumerate()
        .fold(0_u64, |result, (shift, byte)| {
            result | (u64::from(*byte) << (shift * 8))
        })
}

fn datum_go_i64_bits(datum: &Datum) -> i64 {
    match datum {
        Datum::Int(value) => *value,
        Datum::UInt(value) => *value as i64,
        Datum::Real(value) | Datum::Float32(value) => value.to_bits() as i64,
        Datum::Duration(value) => value.nanoseconds(),
        Datum::Enum(value, _) => value.value() as i64,
        Datum::Set(value, _) => value.value() as i64,
        Datum::Json(value) => i64::from(value.type_code()),
        _ => 0,
    }
}

fn datum_go_bytes(datum: &Datum) -> Cow<'_, [u8]> {
    match datum {
        Datum::Json(value) => Cow::Borrowed(value.value()),
        Datum::VectorFloat32(value) => Cow::Owned(value.serialize()),
        _ => Cow::Borrowed(datum.go_bytes()),
    }
}

fn checksum_datum_type_error(
    datum: &Datum,
    field_type: FieldTypeCode,
    expected: &'static str,
) -> RowPackageError {
    let field_type = match field_type {
        FieldTypeCode::Timestamp => "timestamp",
        FieldTypeCode::Datetime => "datetime",
        FieldTypeCode::Date => "date",
        FieldTypeCode::NewDate => "newdate",
        FieldTypeCode::NewDecimal => "decimal",
        _ => "unknown",
    };
    RowPackageError::ChecksumDatumType {
        datum: datum_debug_string(datum),
        field_type,
        expected,
    }
}

fn datum_debug_string(datum: &Datum) -> String {
    let kind = match datum {
        Datum::Null => "KindNull",
        Datum::Int(_) => "KindInt64",
        Datum::UInt(_) => "KindUint64",
        Datum::Real(_) => "KindFloat64",
        Datum::Float32(_) => "KindFloat32",
        Datum::String(_) => "KindString",
        Datum::Bytes(_) => "KindBytes",
        Datum::BinaryLiteral(_) => "KindBinaryLiteral",
        Datum::Decimal(_) => "KindMysqlDecimal",
        Datum::Duration(_) => "KindMysqlDuration",
        Datum::Enum(_, _) => "KindMysqlEnum",
        Datum::Bit(_) => "KindMysqlBit",
        Datum::Set(_, _) => "KindMysqlSet",
        Datum::Time(_) => "KindMysqlTime",
        Datum::MinNotNull => "KindMinNotNull",
        Datum::MaxValue => "KindMaxValue",
        Datum::Raw(_) => "KindRaw",
        Datum::Json(_) => "KindMysqlJSON",
        Datum::VectorFloat32(_) => "KindVectorFloat32",
    };
    let value = if datum.is_null() {
        "<nil>".to_owned()
    } else {
        datum.sql_string().unwrap_or_else(|_| datum.label())
    };
    format!("{kind} {value}")
}

fn append_datum_for_checksum(
    timezone: Option<&SessionTimeZone>,
    buffer: &mut Vec<u8>,
    datum: &Datum,
    field_type: FieldTypeCode,
) -> Result<(), RowPackageError> {
    if datum.is_null() {
        return Ok(());
    }
    match field_type {
        FieldTypeCode::Tiny
        | FieldTypeCode::Short
        | FieldTypeCode::Long
        | FieldTypeCode::LongLong
        | FieldTypeCode::Int24
        | FieldTypeCode::Year => {
            let value = datum_go_i64_bits(datum) as u64;
            buffer.extend_from_slice(&value.to_le_bytes());
        }
        FieldTypeCode::Varchar
        | FieldTypeCode::VarString
        | FieldTypeCode::String
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::LongBlob
        | FieldTypeCode::Blob => append_length_value(buffer, &datum_go_bytes(datum)),
        FieldTypeCode::Timestamp
        | FieldTypeCode::Datetime
        | FieldTypeCode::Date
        | FieldTypeCode::NewDate => {
            let Datum::Time(mut value) = datum.clone() else {
                return Err(checksum_datum_type_error(datum, field_type, "types.Time"));
            };
            if field_type == FieldTypeCode::Timestamp {
                if let Some(timezone) = timezone {
                    value
                        .convert_time_zone(timezone, &Utc)
                        .map_err(|error| RowPackageError::Datatype(error.to_string()))?;
                }
            }
            append_length_value(buffer, value.to_string().as_bytes());
        }
        FieldTypeCode::Duration => {
            let rendered = if let Datum::Duration(value) = datum {
                value.to_string()
            } else {
                MySqlDuration::from_raw_parts(datum_go_i64_bits(datum), 0).to_string()
            };
            append_length_value(buffer, rendered.as_bytes());
        }
        FieldTypeCode::Float | FieldTypeCode::Double => {
            let mut value = f64::from_bits(datum_go_i64_bits(datum) as u64);
            if !value.is_finite() {
                value = 0.0;
            }
            buffer.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        FieldTypeCode::NewDecimal => {
            let Some(value) = datum.as_decimal() else {
                return Err(checksum_datum_type_error(
                    datum,
                    field_type,
                    "*types.MyDecimal",
                ));
            };
            append_length_value(buffer, value.to_string().as_bytes());
        }
        FieldTypeCode::Enum => {
            buffer.extend_from_slice(&(datum_go_i64_bits(datum) as u64).to_le_bytes());
        }
        FieldTypeCode::Set => {
            buffer.extend_from_slice(&(datum_go_i64_bits(datum) as u64).to_le_bytes());
        }
        FieldTypeCode::Bit => {
            buffer.extend_from_slice(
                &binary_literal_bytes_to_uint(&datum_go_bytes(datum)).to_le_bytes(),
            );
        }
        FieldTypeCode::Json => {
            let value = BinaryJSON::from_encoded_parts(
                datum_go_i64_bits(datum) as u8,
                datum_go_bytes(datum).into_owned(),
            );
            append_length_value(buffer, value.to_string().as_bytes());
        }
        FieldTypeCode::VectorFloat32 => {
            let bytes = datum_go_bytes(datum);
            let (value, _) = deserialize_vector_float32(&bytes).map_err(|error| {
                RowPackageError::ChecksumDatumValue {
                    datum: datum_debug_string(datum),
                    field_type: "vector",
                    cause: error.to_string(),
                }
            })?;
            value.serialize_to(buffer);
        }
        FieldTypeCode::Null | FieldTypeCode::Geometry => {}
        _ => return Err(RowPackageError::InvalidChecksumType),
    }
    Ok(())
}

fn append_length_value(buffer: &mut Vec<u8>, value: &[u8]) {
    buffer.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buffer.extend_from_slice(value);
}

fn field_type_to_flag(field_type: &FieldType) -> u8 {
    match field_type.code() {
        FieldTypeCode::Tiny
        | FieldTypeCode::Short
        | FieldTypeCode::Int24
        | FieldTypeCode::Long
        | FieldTypeCode::LongLong => {
            if field_type.is_unsigned() {
                UINT_FLAG
            } else {
                INT_FLAG
            }
        }
        FieldTypeCode::Float | FieldTypeCode::Double => FLOAT_FLAG,
        FieldTypeCode::Blob
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::LongBlob
        | FieldTypeCode::String
        | FieldTypeCode::Varchar
        | FieldTypeCode::VarString => crate::BYTES_FLAG,
        FieldTypeCode::Datetime | FieldTypeCode::Date | FieldTypeCode::Timestamp => UINT_FLAG,
        FieldTypeCode::Duration | FieldTypeCode::Year => INT_FLAG,
        FieldTypeCode::NewDecimal => DECIMAL_FLAG,
        FieldTypeCode::Enum | FieldTypeCode::Bit | FieldTypeCode::Set => UINT_FLAG,
        FieldTypeCode::Json => JSON_FLAG,
        FieldTypeCode::VectorFloat32 => VECTOR_FLOAT32_FLAG,
        FieldTypeCode::Null => NIL_FLAG,
        other => panic!("unknown field type {}", other.mysql_type()),
    }
}

/// Removes an API-v2 keyspace prefix under the same runtime conditions as the
/// Go helper.
pub fn remove_keyspace_prefix(
    key: &[u8],
    classic_kernel: bool,
    in_test: bool,
    standalone: bool,
) -> &[u8] {
    if classic_kernel
        || (!in_test && !standalone)
        || key.len() <= KEYSPACE_PREFIX_LEN
        || key[0] != API_V2_TXN_MODE_PREFIX
    {
        key
    } else {
        &key[KEYSPACE_PREFIX_LEN..]
    }
}
