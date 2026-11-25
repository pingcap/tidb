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

use std::borrow::{Borrow, Cow};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use chrono::Utc;
use crc32fast::Hasher;
use smallvec::SmallVec;
use tidb_datatype::{
    deserialize_vector_float32, parse_enum_value, parse_set_value, peek_vector_float32, BinaryJSON,
    BinaryLiteral, BinaryLiteralWidth, CoreTime, Datum, FieldType, FieldTypeCode, MySqlDuration,
    SessionTimeZone, StringDatum, Time, TimeType, TruncationPolicy, VectorFloat32,
};

use crate::decode_raw_int;
use crate::decode_raw_uint;
use crate::encode_raw_int;
use crate::encode_raw_uint;
use crate::row_encoder::row_header;
use crate::RawRowValue;
use crate::RowDecoder;
use crate::ROW_HEADER_LEN;
use tidb_codec::decode_decimal;
use tidb_codec::decode_float;
use tidb_codec::encode_compact_bytes;
use tidb_codec::encode_decimal_fixed;
use tidb_codec::encode_float;
use tidb_codec::encode_uvarint;
use tidb_codec::encode_varint;
use tidb_codec::CodecError;
use tidb_codec::COMPACT_BYTES_FLAG;
use tidb_codec::DECIMAL_FLAG;
use tidb_codec::FLOAT_FLAG;
use tidb_codec::INT_FLAG;
use tidb_codec::JSON_FLAG;
use tidb_codec::NIL_FLAG;
use tidb_codec::UINT_FLAG;
use tidb_codec::UVARINT_FLAG;
use tidb_codec::VARINT_FLAG;
use tidb_codec::VECTOR_FLOAT32_FLAG;

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
    fn update_checksum(&self, hasher: &mut Hasher) {
        match self {
            Self::Int(value) => {
                hasher.update(&tidb_codec::encode_int_to_cmp_uint(*value).to_be_bytes());
            }
            Self::Common(parts) => {
                for part in parts {
                    hasher.update(part);
                }
            }
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
#[derive(Clone, Default)]
pub struct DecodeRowOptions<'a> {
    /// Column IDs stored by an integer or common handle.
    pub handle_column_ids: &'a [i64],
    /// Row handle, when this scan has one.
    pub handle: Option<&'a Handle>,
    /// Go `ChunkDecoder.defDatum`, invoked only for a missing column.
    pub default_datum: Option<&'a (dyn Fn(usize) -> Result<Datum, String> + 'a)>,
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
    /// Error returned by Go's default datum/bytes callback.
    DefaultValue(String),
    /// A persisted scalar is malformed.
    InvalidValue(&'static str),
    /// A lower-level row framing error.
    Decode(crate::RowDecodeError),
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
            Self::UnknownFieldType(code) => write!(formatter, "unknown type {}", code.mysql_type()),
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
            Self::DefaultValue(error) => formatter.write_str(error),
            Self::InvalidValue(kind) => write!(formatter, "invalid persisted {kind} value"),
            Self::Decode(error) => error.fmt(formatter),
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
    encode_row_with_values(timezone, column_ids, values, checksum, buffer)
}

/// Encodes a row whose datums may borrow their payloads.
pub fn encode_row_with_values(
    timezone: Option<&SessionTimeZone>,
    column_ids: &[i64],
    values: &[impl Borrow<Datum>],
    checksum: &RowChecksumPolicy,
    buffer: &mut Vec<u8>,
) -> Result<(), RowPackageError> {
    assert!(
        column_ids.len() <= usize::from(u16::MAX),
        "row column count overflows Go's uint16 metadata"
    );
    let large_ids = column_ids.iter().any(|&id| id > i64::from(u8::MAX));
    let mut not_null = SmallVec::<[(u32, usize); 16]>::new();
    let mut null = SmallVec::<[u32; 8]>::new();
    let mut bytes_len = 0;
    for (index, &id) in column_ids.iter().enumerate() {
        // Go truncates IDs before sorting and widens the already-truncated
        // IDs if the data later exceeds the small offset limit.
        let id = if large_ids {
            id as u32
        } else {
            u32::from(id as u8)
        };
        match values[index].borrow() {
            Datum::Null => null.push(id),
            value => {
                bytes_len += match value {
                    Datum::String(value) => value.bytes().len(),
                    Datum::Bytes(value) => value.len(),
                    _ => 0,
                };
                not_null.push((id, index));
            }
        }
    }
    not_null.sort_unstable_by_key(|&(id, _)| id);
    null.sort_unstable();

    let start = buffer.len();
    // Reserve large metadata up front when the byte columns alone require it.
    let mut large = large_ids || bytes_len > usize::from(u16::MAX);
    let small_metadata_len = column_ids.len() + 2 * not_null.len();
    let large_metadata_len = 4 * (column_ids.len() + not_null.len());
    let metadata_len = if large {
        large_metadata_len
    } else {
        small_metadata_len
    };
    let mut data_start = start + ROW_HEADER_LEN + metadata_len;
    if !not_null.is_empty() {
        // Leave room for fixed-width values alongside the known byte payloads.
        buffer.reserve(ROW_HEADER_LEN + metadata_len + bytes_len + 8 * not_null.len());
    }
    buffer.resize(data_start, 0);

    let mut errors = Vec::new();
    for (_, index_or_offset) in &mut not_null {
        let index = *index_or_offset;
        if let Err(error) = encode_value_datum(timezone, values[index].borrow(), buffer) {
            errors.push((index, error));
        }
        *index_or_offset = buffer.len() - data_start;
    }
    if !errors.is_empty() {
        buffer.truncate(start);
        if errors.len() == 1 {
            return Err(errors.pop().expect("one encoding error").1);
        }
        // The typed encoder reports errors in input column order.
        errors.sort_unstable_by_key(|(index, _)| *index);
        return Err(RowPackageError::EncodeErrors(
            errors.into_iter().map(|(_, error)| error).collect(),
        ));
    }

    if !large && buffer.len() - data_start > usize::from(u16::MAX) {
        let end = buffer.len();
        let extra = large_metadata_len - small_metadata_len;
        buffer.resize(end + extra, 0);
        buffer.copy_within(data_start..end, data_start + extra);
        data_start += extra;
        large = true;
    }
    let (header, metadata) = buffer[start..data_start].split_at_mut(ROW_HEADER_LEN);
    header.copy_from_slice(&row_header(large, not_null.len(), null.len()));
    let ids = not_null
        .iter()
        .map(|&(id, _)| id)
        .chain(null.iter().copied());
    if large {
        let (id_bytes, offset_bytes) = metadata.split_at_mut(4 * column_ids.len());
        for (bytes, id) in id_bytes.chunks_exact_mut(4).zip(ids) {
            bytes.copy_from_slice(&id.to_le_bytes());
        }
        for (bytes, &(_, end)) in offset_bytes.chunks_exact_mut(4).zip(&not_null) {
            bytes.copy_from_slice(&(end as u32).to_le_bytes());
        }
    } else {
        let (id_bytes, offset_bytes) = metadata.split_at_mut(column_ids.len());
        for (byte, id) in id_bytes.iter_mut().zip(ids) {
            *byte = id as u8;
        }
        for (bytes, &(_, end)) in offset_bytes.chunks_exact_mut(2).zip(&not_null) {
            bytes.copy_from_slice(&(end as u16).to_le_bytes());
        }
    }
    if let RowChecksumPolicy::RawHandle(handle) = checksum {
        buffer[start + 1] |= crate::ROW_FLAG_CHECKSUM;
        buffer.push(CHECKSUM_VERSION_RAW_HANDLE);
        let mut hasher = Hasher::new();
        hasher.update(buffer);
        handle.update_checksum(&mut hasher);
        buffer.extend_from_slice(&hasher.finalize().to_le_bytes());
    }
    Ok(())
}

fn encode_value_datum(
    timezone: Option<&SessionTimeZone>,
    datum: &Datum,
    output: &mut Vec<u8>,
) -> Result<(), RowPackageError> {
    match datum {
        Datum::Null => {}
        Datum::Int(value) => encode_raw_int(output, *value),
        Datum::UInt(value) => encode_raw_uint(output, *value),
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
                output,
                value
                    .to_packed_uint()
                    .map_err(|error| RowPackageError::Datatype(error.to_string()))?,
            );
        }
        Datum::Duration(value) => encode_raw_int(output, value.nanoseconds()),
        Datum::Enum(value, _) => encode_raw_uint(output, value.value()),
        Datum::Set(value, _) => encode_raw_uint(output, value.value()),
        Datum::BinaryLiteral(value) | Datum::Bit(value) => {
            let (encoded, error) =
                value.to_int_with_policy(TruncationPolicy::STRICT, |_| unreachable!());
            if let Some(error) = error {
                return Err(RowPackageError::Datatype(error.to_string()));
            }
            encode_raw_uint(output, encoded)
        }
        Datum::Real(value) | Datum::Float32(value) => encode_float(output, *value),
        Datum::Decimal(value) => {
            // Go: `codec.EncodeDecimal(buffer, d.GetMysqlDecimal(), d.Length(),
            // d.Frac())` — the DECLARED column shape, not the value's natural
            // one. `(0, 0)` (no column involved) is Go's unset `Datum.length`,
            // which `EncodeDecimal` itself resolves to `PrecisionAndFrac`.
            let (precision, scale) = value.storage_shape();
            encode_decimal_fixed(output, value, precision, scale)?;
        }
        Datum::Json(value) => {
            output.push(value.type_code());
            output.extend_from_slice(value.value());
        }
        Datum::VectorFloat32(value) => value.serialize_to(output),
        Datum::MinNotNull => return Err(RowPackageError::UnsupportedEncodeType(15)),
        Datum::MaxValue => return Err(RowPackageError::UnsupportedEncodeType(16)),
        Datum::Raw(_) => return Err(RowPackageError::UnsupportedEncodeType(17)),
    }
    Ok(())
}

/// Decodes present and explicit-NULL columns into a map. Missing columns are
/// omitted, matching Go `DatumMapDecoder`.
pub fn decode_row_to_map(
    row_data: &[u8],
    columns: &[ColumnInfo],
    timezone: Option<&SessionTimeZone>,
    output: &mut BTreeMap<i64, Datum>,
) -> Result<(), RowPackageError> {
    decode_row_to_map_with_fields(
        row_data,
        columns.iter().map(|column| (column.id, &column.field_type)),
        timezone,
        output,
    )
}

/// Decodes a row into an existing map using borrowed field metadata.
pub fn decode_row_to_map_with_fields<'a>(
    row_data: &[u8],
    fields: impl IntoIterator<Item = (i64, &'a FieldType)>,
    timezone: Option<&SessionTimeZone>,
    output: &mut BTreeMap<i64, Datum>,
) -> Result<(), RowPackageError> {
    decode_row_to_map_with_backing(row_data, fields, timezone, output, None)
}

/// Minimum payload size at which a typed table row retains its backing
/// buffer for a string, JSON value or vector.
pub const MIN_SHARED_ROW_VALUE_BYTES: usize = 1024;

/// Decodes a shared row into an existing map, retaining large payloads as
/// views of the original row buffer.
pub fn decode_shared_row_to_map_with_fields<'a>(
    row_data: &Arc<Vec<u8>>,
    fields: impl IntoIterator<Item = (i64, &'a FieldType)>,
    timezone: Option<&SessionTimeZone>,
    output: &mut BTreeMap<i64, Datum>,
) -> Result<(), RowPackageError> {
    decode_row_to_map_with_backing(row_data, fields, timezone, output, Some(row_data))
}

#[inline]
fn decode_row_to_map_with_backing<'a>(
    row_data: &[u8],
    fields: impl IntoIterator<Item = (i64, &'a FieldType)>,
    timezone: Option<&SessionTimeZone>,
    output: &mut BTreeMap<i64, Datum>,
    backing: Option<&Arc<Vec<u8>>>,
) -> Result<(), RowPackageError> {
    let (decoder, _) = RowDecoder::parse(row_data)?;
    for (id, field_type) in fields {
        match decoder.column(id)? {
            RawRowValue::NotNull { bytes, .. } => {
                let datum = decode_column_with_backing(
                    bytes, field_type, timezone, DecodeTarget::Map, backing,
                )?;
                output.insert(id, datum);
            }
            RawRowValue::Null => {
                output.insert(id, Datum::Null);
            }
            RawRowValue::Missing => {}
        }
    }
    Ok(())
}

/// Decodes a row into requested output columns, including defaults, integer
/// and common handles, generated-column NULLs, commit TS, and checksum pseudo
/// columns.
pub fn decode_row_to_datums(
    row_data: &[u8],
    columns: &[ColumnInfo],
    options: &DecodeRowOptions<'_>,
) -> Result<DecodedRow, RowPackageError> {
    decode_row_to_datums_impl(row_data, columns, options, None, None)
}

/// Decodes projected datums, retaining payloads when sharing the owned row
/// saves individual copies. Borrowed handle parts can replace the options handle.
pub fn decode_row_to_datums_with_refs(
    row_data: Cow<'_, [u8]>,
    columns: &[ColumnInfo],
    options: &DecodeRowOptions<'_>,
    common_handle_parts: Option<&[&[u8]]>,
) -> Result<DecodedRow, RowPackageError> {
    let row_data = match (row_data, columns) {
        (Cow::Owned(bytes), [column])
            if column.field_type.code().is_string()
                && !column.field_type.code().is_type_unspecified()
                && !column.virtual_generated
                && Some(column.id) != options.commit_ts_column_id
                && Some(column.id) != options.row_checksum_column_id =>
        {
            return decode_owned_projected_string(bytes, column, options, common_handle_parts);
        }
        (bytes, _) => bytes,
    };
    // A small row with several projected strings can share one allocation.
    // Keep the backing bounded and leave strings that fit in a datum inline.
    match row_data {
        Cow::Owned(bytes)
            if bytes.len() >= MIN_SHARED_ROW_VALUE_BYTES
                || (bytes.capacity() <= MIN_SHARED_ROW_VALUE_BYTES
                    && bytes.len()
                        > columns.len() * (StringDatum::INLINE_CAPACITY + 3) + ROW_HEADER_LEN
                    && columns.len() >= 3
                    && columns
                        .iter()
                        .filter(|column| column.field_type.code().is_string())
                        .nth(2)
                        .is_some()) =>
        {
            let backing = Arc::new(bytes);
            decode_row_to_datums_impl(
                &backing, columns, options, common_handle_parts, Some(&backing),
            )
        }
        bytes => decode_row_to_datums_impl(&bytes, columns, options, common_handle_parts, None),
    }
}

fn decode_owned_projected_string(
    mut row_data: Vec<u8>,
    column: &ColumnInfo,
    options: &DecodeRowOptions<'_>,
    common_handle_parts: Option<&[&[u8]]>,
) -> Result<DecodedRow, RowPackageError> {
    let (decoder, _) = RowDecoder::parse(&row_data)?;
    let checksum = decoder.layout().checksum();
    let datum = match decoder.column(column.id)? {
        RawRowValue::NotNull { bytes, .. } => {
            let collation = column.field_type.collation();
            let len = bytes.len();
            Datum::String(if len >= MIN_SHARED_ROW_VALUE_BYTES {
                let start = bytes.as_ptr().addr() - row_data.as_ptr().addr();
                let owner = Arc::new(row_data);
                StringDatum::from_shared_subslice(&owner, &owner[start..start + len], collation)
            } else if len > StringDatum::INLINE_CAPACITY
                && row_data.capacity() <= (8 * len).min(4096)
            {
                // Reuse a bounded, exclusive buffer for the only output.
                let start = bytes.as_ptr().addr() - row_data.as_ptr().addr();
                row_data.copy_within(start..start + len, 0);
                row_data.truncate(len);
                StringDatum::new(row_data, collation)
            } else {
                StringDatum::from_slice(bytes, collation)
            })
        }
        RawRowValue::Null => Datum::Null,
        RawRowValue::Missing => missing_datum(0, column, options, common_handle_parts)?,
    };
    Ok(DecodedRow {
        values: vec![datum],
        checksum: checksum.map(|checksum| checksum.checksum()),
        checksum_version: checksum.map_or(0, |checksum| checksum.version()),
    })
}

#[inline]
fn decode_row_to_datums_impl(
    row_data: &[u8],
    columns: &[ColumnInfo],
    options: &DecodeRowOptions<'_>,
    common_handle_parts: Option<&[&[u8]]>,
    backing: Option<&Arc<Vec<u8>>>,
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
                values.push(decode_column_with_backing(
                    bytes,
                    &column.field_type,
                    options.timezone,
                    DecodeTarget::Chunk,
                    backing,
                )?);
            }
            RawRowValue::Null => values.push(Datum::Null),
            RawRowValue::Missing => {
                values.push(missing_datum(index, column, options, common_handle_parts)?);
            }
        }
    }
    Ok(DecodedRow {
        values,
        checksum: layout.checksum().map(|checksum| checksum.checksum()),
        checksum_version: layout.checksum().map_or(0, |checksum| checksum.version()),
    })
}

#[inline]
fn missing_datum(
    index: usize,
    column: &ColumnInfo,
    options: &DecodeRowOptions<'_>,
    common_handle_parts: Option<&[&[u8]]>,
) -> Result<Datum, RowPackageError> {
    if let Some(value) = decode_handle_column(
        column,
        options.handle_column_ids,
        options.handle,
        common_handle_parts,
        options.timezone,
    )? {
        Ok(value)
    } else if let Some(default_datum) = options.default_datum {
        default_datum(index).map_err(RowPackageError::DefaultValue)
    } else {
        Ok(Datum::Null)
    }
}

/// Recalculates a row's bytes-level checksum after replacing the persisted
/// payloads for the supplied columns.
///
/// Checksum version 1 extends the CRC with the raw key; later versions extend
/// it with the encoded handle, preserving the v8.3 compatibility branch.
pub fn calculate_raw_checksum(
    row_data: &mut [u8],
    timezone: Option<&SessionTimeZone>,
    column_ids: &[i64],
    values: &[Datum],
    key: &[u8],
    handle: &Handle,
    buffer: &[u8],
) -> Result<u32, RowPackageError> {
    // Collect every encoded replacement before mutating the caller's row.
    let mut encoded = Vec::new();
    let (body_end, checksum_header, checksum_version, replacements) = {
        let (decoder, _) = RowDecoder::parse(row_data)?;
        let layout = decoder.layout();
        let checksum = layout.checksum();
        let header = layout.header();
        let data_start = crate::ROW_HEADER_LEN
            + header.column_count() * header.column_id_width()
            + usize::from(header.not_null_count()) * header.offset_width();
        let body_end = data_start + layout.data().len();
        let mut replacements = SmallVec::<[_; 8]>::new();
        for (input_index, &column_id) in column_ids.iter().enumerate() {
            let value = &values[input_index];
            if value.is_null() {
                return Err(RowPackageError::UnsupportedEncodeType(0));
            }
            let encoded_start = encoded.len();
            encode_value_datum(timezone, value, &mut encoded)?;
            let RawRowValue::NotNull { index, bytes } = decoder.column(column_id)? else {
                encoded.truncate(encoded_start);
                continue;
            };
            let range = layout
                .value_range(index)
                .map_err(crate::RowDecodeError::from)?;
            let copied = bytes.len().min(encoded.len() - encoded_start);
            encoded.truncate(encoded_start + copied);
            replacements.push((data_start + range.start, encoded_start..encoded.len()));
        }
        (
            body_end,
            checksum.map_or(0, |checksum| checksum.header()),
            checksum.map_or(0, |checksum| checksum.version()),
            replacements,
        )
    };
    for (start, range) in replacements {
        row_data[start..start + range.len()].copy_from_slice(&encoded[range]);
    }
    let mut hasher = Hasher::new();
    hasher.update(buffer);
    hasher.update(&row_data[..body_end]);
    hasher.update(&[checksum_header]);
    if checksum_version == 1 {
        hasher.update(key);
    } else {
        handle.update_checksum(&mut hasher);
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
    cache_bytes: &[u8],
    default_bytes: Option<&dyn Fn(usize) -> Result<Vec<u8>, String>>,
) -> Result<Vec<Vec<u8>>, RowPackageError> {
    let (decoder, _) = RowDecoder::parse(row_data)?;
    let mut values = vec![Vec::new(); output_offsets.len()];
    for (index, column) in columns.iter().enumerate() {
        let offset = output_offsets.get(&column.id).copied().unwrap_or_default();
        values[offset] = match decoder.column(column.id)? {
            RawRowValue::NotNull { bytes, .. } => encode_old_raw(column, bytes)?,
            RawRowValue::Null => vec![NIL_FLAG],
            RawRowValue::Missing => {
                if let Some(encoded) =
                    encode_old_handle(column, handle_column_ids, handle, cache_bytes)?
                {
                    encoded
                } else {
                    default_bytes
                        .map(|default_bytes| default_bytes(index))
                        .transpose()
                        .map_err(RowPackageError::DefaultValue)?
                        .filter(|value| !value.is_empty())
                        .unwrap_or_else(|| vec![NIL_FLAG])
                }
            }
        };
    }
    Ok(values)
}

/// Converts the requested columns in order, using NULL for missing values.
pub fn decode_row_to_old_bytes_in_order(
    row_data: &[u8],
    columns: &[ColumnInfo],
) -> Result<Vec<Vec<u8>>, RowPackageError> {
    let (decoder, _) = RowDecoder::parse(row_data)?;
    columns
        .iter()
        .map(|column| match decoder.column(column.id)? {
            RawRowValue::NotNull { bytes, .. } => encode_old_raw(column, bytes),
            RawRowValue::Null | RawRowValue::Missing => Ok(vec![NIL_FLAG]),
        })
        .collect()
}

fn encode_old_raw(column: &ColumnInfo, bytes: &[u8]) -> Result<Vec<u8>, RowPackageError> {
    let flag = field_type_to_flag(&column.field_type);
    let prefix_capacity = 10 + usize::from(flag == tidb_codec::BYTES_FLAG);
    let mut output = Vec::with_capacity(bytes.len() + prefix_capacity);
    match flag {
        tidb_codec::BYTES_FLAG => {
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
    cache_bytes: &[u8],
) -> Result<Option<Vec<u8>>, RowPackageError> {
    if column.field_type.need_restored_data() {
        return Ok(None);
    }
    match handle {
        Some(Handle::Int(value)) if column.is_pk_handle || column.id == -1 => {
            let mut output = cache_bytes.to_vec();
            output.reserve(9);
            if column.field_type.is_unsigned() {
                output.push(UINT_FLAG);
                tidb_codec::encode_uint(&mut output, *value as u64);
            } else {
                output.push(INT_FLAG);
                tidb_codec::encode_int(&mut output, *value);
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
    common_handle_parts: Option<&[&[u8]]>,
    timezone: Option<&SessionTimeZone>,
) -> Result<Option<Datum>, RowPackageError> {
    if let Some(parts) = common_handle_parts {
        return decode_common_handle_column(column, handle_column_ids, parts, timezone);
    }
    let Some(handle) = handle else {
        return Ok(None);
    };
    match handle {
        // Go `tryDecodeHandle`'s Int arm admits the PK-handle column and the
        // `_tidb_rowid` extra column without consulting `handleColIDs`, so an
        // empty id list (a rowid table projecting no handle column) falls
        // through to the `None` arm instead of indexing out of bounds.
        Handle::Int(value)
            if column.is_pk_handle
                || column.id == -1
                || handle_column_ids.first() == Some(&column.id) =>
        {
            Ok(Some(if column.field_type.is_unsigned() {
                Datum::UInt(*value as u64)
            } else {
                Datum::Int(*value)
            }))
        }
        Handle::Common(parts) => decode_common_handle_column(column, handle_column_ids, parts, timezone),
        Handle::Int(_) => Ok(None),
    }
}

fn decode_common_handle_column(
    column: &ColumnInfo,
    handle_column_ids: &[i64],
    parts: &[impl AsRef<[u8]>],
    timezone: Option<&SessionTimeZone>,
) -> Result<Option<Datum>, RowPackageError> {
    if column.field_type.need_restored_data() {
        return Ok(None);
    }
    let Some(index) = handle_column_ids.iter().position(|id| *id == column.id) else {
        return Ok(None);
    };
    let Some(encoded) = parts.get(index) else {
        return Ok(None);
    };
    Ok(tidb_codec::decode_one_typed_in_timezone(encoded.as_ref(), &column.field_type, timezone)
        .ok()
        .map(|(_, datum)| datum))
}

#[inline]
fn decode_column_with_backing(
    bytes: &[u8],
    field_type: &FieldType,
    timezone: Option<&SessionTimeZone>,
    target: DecodeTarget,
    backing: Option<&Arc<Vec<u8>>>,
) -> Result<Datum, RowPackageError> {
    if let Some(owner) = backing {
        let code = field_type.code();
        let large = bytes.len() >= MIN_SHARED_ROW_VALUE_BYTES;
        if code.is_string()
            && !code.is_type_unspecified()
            && (large
                || (target == DecodeTarget::Chunk
                    && bytes.len() > StringDatum::INLINE_CAPACITY
                    && owner.capacity() <= MIN_SHARED_ROW_VALUE_BYTES))
        {
            return Ok(Datum::String(StringDatum::from_shared_subslice(
                owner,
                bytes,
                field_type.collation(),
            )));
        }
        match code {
            FieldTypeCode::Json if large => {
                return Ok(Datum::Json(BinaryJSON::from_shared_subslice(
                    bytes[0], owner, &bytes[1..],
                )));
            }
            FieldTypeCode::VectorFloat32 if large => {
                return VectorFloat32::from_shared_subslice(owner, bytes)
                    .map(Datum::VectorFloat32)
                    .map_err(|error| RowPackageError::Datatype(error.to_string()));
            }
            _ => {}
        }
    }
    decode_column_datum(bytes, field_type, timezone, target)
}

fn decode_column_datum(
    bytes: &[u8],
    field_type: &FieldType,
    timezone: Option<&SessionTimeZone>,
    target: DecodeTarget,
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
            Datum::String(StringDatum::from_slice(bytes, field_type.collation()))
        }
        FieldTypeCode::NewDecimal => {
            let (_, mut value, encoded_precision, encoded_scale) = decode_decimal(bytes)?;
            match target {
                DecodeTarget::Chunk => {
                    if field_type.decimal() >= 0 && i64::from(encoded_scale) > field_type.decimal()
                    {
                        value = value.round_to_scale(field_type.decimal() as i32);
                    }
                    // A row read here is Go's chunk row read back through
                    // `Row.GetDatum`, which stamps the column's flen and
                    // decimal (the value's own frac when unspecified): the
                    // shape an index key is encoded under, so a backfilled
                    // key is the one INSERT and DELETE compute.
                    let frac = if field_type.decimal() < 0 {
                        i64::from(value.storage_scale())
                    } else {
                        field_type.decimal()
                    };
                    value = value.with_declared_shape(field_type.flen(), frac);
                }
                // Go `DatumMapDecoder.decodeColDatum`: `SetLength(precision)`
                // and `SetFrac(frac)` from the encoding.
                DecodeTarget::Map => {
                    value = value.with_declared_shape(
                        i64::from(encoded_precision),
                        i64::from(encoded_scale),
                    );
                }
            }
            Datum::Decimal(value)
        }
        FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
            let kind = match code {
                FieldTypeCode::Date => TimeType::Date,
                FieldTypeCode::Timestamp => TimeType::Timestamp,
                _ => TimeType::DateTime,
            };
            // Go `Time.FromPackedUint` only extracts bit fields. Calendar
            // validity is checked later by the operation that needs it.
            let parts = tidb_datatype::PackedTime::from_raw(decode_raw_uint(bytes)?).parts();
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
            .map_err(|error| RowPackageError::Datatype(error.to_string()))?;
            if kind == TimeType::Timestamp && !value.is_zero() {
                if target == DecodeTarget::Map {
                    value
                        .convert_time_zone(&Utc, timezone.expect("Go map decoder timezone"))
                        .map_err(|error| RowPackageError::Datatype(error.to_string()))?;
                } else if let Some(timezone) = timezone {
                    value
                        .convert_time_zone(&Utc, timezone)
                        .map_err(|error| RowPackageError::Datatype(error.to_string()))?;
                }
            }
            Datum::Time(value)
        }
        FieldTypeCode::Duration => Datum::Duration(MySqlDuration::from_raw_parts(
            decode_raw_int(bytes)?,
            field_type.decimal(),
        )),
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
            let byte_size = (field_type.flen() + 7) >> 3;
            let width =
                BinaryLiteralWidth::try_from(u8::try_from(byte_size).expect("Invalid byteSize"))
                    .expect("Invalid byteSize");
            Datum::Bit(BinaryLiteral::from_uint(
                decode_raw_uint(bytes)?,
                Some(width),
            ))
        }
        FieldTypeCode::Json => Datum::Json(BinaryJSON::from_encoded_parts(
            bytes[0],
            bytes[1..].to_vec(),
        )),
        FieldTypeCode::VectorFloat32 => {
            let (value, _) = deserialize_vector_float32(bytes)
                .map_err(|error| RowPackageError::Datatype(error.to_string()))?;
            Datum::VectorFloat32(value)
        }
        _ => return Err(RowPackageError::UnknownFieldType(code)),
    })
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum DecodeTarget {
    Map,
    Chunk,
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
        datum: datum.string(),
        field_type,
        expected,
    }
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
            let temporary;
            let value = if let Datum::Json(value) = datum {
                value
            } else {
                temporary = BinaryJSON::from_encoded_parts(
                    datum_go_i64_bits(datum) as u8,
                    datum_go_bytes(datum).into_owned(),
                );
                &temporary
            };
            append_length_value(buffer, value.to_string().as_bytes());
        }
        FieldTypeCode::VectorFloat32 => match datum {
            Datum::VectorFloat32(value) if u32::try_from(value.serialized_size()).is_ok() => {
                value.serialize_to(buffer);
            }
            _ => {
                let bytes = datum_go_bytes(datum);
                let length = peek_vector_float32(&bytes).map_err(|error| {
                    RowPackageError::ChecksumDatumValue {
                        datum: datum.string(),
                        field_type: "vector",
                        cause: error.to_string(),
                    }
                })?;
                buffer.extend_from_slice(&bytes[..length]);
            }
        },
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
    match field_type.array_element_code() {
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
        | FieldTypeCode::VarString => tidb_codec::BYTES_FLAG,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_string_projection_reuses_bounded_storage_and_keeps_checksum() {
        let column = ColumnInfo {
            id: 256,
            is_pk_handle: false,
            virtual_generated: false,
            field_type: FieldType::new(FieldTypeCode::Varchar).with_collation_name("binary"),
        };
        let value = Datum::new_collation_string_datum(vec![0xf0; 128], column.field_type.collation());
        let mut encoded = Vec::new();
        encode_row_with_checksum(
            None,
            &[1, column.id],
            &[Datum::Int(7), value],
            &RowChecksumPolicy::RawHandle(Handle::Int(1)),
            &mut encoded,
        )
        .unwrap();

        let columns = [column];
        let options = DecodeRowOptions::default();
        let expected = decode_row_to_datums(&encoded, &columns, &options).unwrap();
        assert_eq!(expected.checksum_version, 2);
        assert!(expected.checksum.is_some());
        for oversized in [false, true] {
            let mut input = encoded.clone();
            if oversized {
                input.reserve_exact(4096);
            }
            let address = input.as_ptr().addr();
            let decoded =
                decode_row_to_datums_with_refs(Cow::Owned(input), &columns, &options, None)
                    .unwrap();
            assert_eq!(decoded, expected);
            assert_eq!(
                decoded.values[0].as_raw_bytes().unwrap().as_ptr().addr() == address,
                !oversized
            );
        }
    }
}
