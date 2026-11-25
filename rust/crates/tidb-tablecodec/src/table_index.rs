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

//! Index-key and index-value half of Go `pkg/tablecodec/tablecodec.go`.

use std::collections::{BTreeMap, HashSet};
use std::borrow::{Borrow, Cow};
use std::fmt;

use tidb_datatype::{
    is_bin_collation, Datum, FieldType, FieldTypeCode, SessionTimeZone, StringDatum, UNSPECIFIED_LENGTH,
};
use tidb_txnkv::{CommonHandle, Handle, IntHandle, PartitionHandle};

use crate::table_row::{decode_column_value, TableRowError};
use crate::table_key::{append_index_key_prefix, encode_index_seek_key};
use crate::table_key::{
    MAX_OLD_ENCODE_VALUE_LEN, TEMP_INDEX_PREFIX, UNCOMMITTED_INDEX_KV_FLAG,
};
use tidb_codec::{
    cut_one, decode_int, decode_one, encode_bytes, encode_int, encode_uvarint, encode_varint,
    encoded_bytes_len, skip_bytes, split_cut_bytes,
    CodecError, Encoder, BYTES_FLAG, INT_FLAG, UVARINT_FLAG, VARINT_FLAG,
};
use tidb_rowcodec::{
    decode_raw_uint, decode_row_to_old_bytes, decode_row_to_old_bytes_in_order,
    encode_row_with_values, ColumnInfo, RawRowValue, RowChecksumPolicy, RowDecoder,
    RowPackageError, ROW_CODEC_VERSION,
};

const ID_LEN: usize = 8;
const PREFIX_LEN: usize = 1 + ID_LEN + 2;
const INDEX_VALUES_OFFSET: usize = PREFIX_LEN + ID_LEN;
const COMMON_HANDLE_HEADER_LEN: usize = 3;
const PARTITION_ID_SEGMENT_LEN: usize = 1 + ID_LEN;

fn inner_handle(handle: &Handle) -> &Handle {
    match handle {
        Handle::Partition(partition) => inner_handle(partition.inner()),
        other => other,
    }
}

fn handle_partition_id(handle: &Handle) -> Option<i64> {
    match handle {
        Handle::Partition(partition) => Some(partition.partition_id()),
        _ => None,
    }
}

fn partition_handle(partition_id: i64, handle: Handle) -> Handle {
    PartitionHandle::new(partition_id, Some(handle)).into()
}

fn common_handle(encoded: impl Into<Vec<u8>>) -> Result<Handle, CodecError> {
    CommonHandle::new(encoded).map(Handle::from)
}

fn encoded_handle_columns(handle: &Handle) -> Result<Vec<Vec<u8>>, TableIndexError> {
    match inner_handle(handle) {
        Handle::Int(value) => {
            let mut encoded = Vec::with_capacity(ID_LEN + 1);
            encoded.push(INT_FLAG);
            encode_int(&mut encoded, value.value());
            Ok(vec![encoded])
        }
        Handle::Common(value) => Ok((0..value.num_columns())
            .map(|index| {
                value
                    .encoded_column(index)
                    .expect("index is bounded by the parsed column count")
                    .to_vec()
            })
            .collect()),
        Handle::Partition(_) => unreachable!("inner_handle removes partition wrappers"),
    }
}

/// Column-ID keyed cut values and the unconsumed index-key suffix.
pub type CutIndexValues<'a> = (BTreeMap<i64, Vec<u8>>, &'a [u8]);

/// Marker for a common handle in an extensible index value.
pub const COMMON_HANDLE_FLAG: u8 = 127;
/// Marker for a global-index partition ID.
pub const PARTITION_ID_FLAG: u8 = 126;
/// Marker for an explicit clustered-index value version.
pub const INDEX_VERSION_FLAG: u8 = 125;
/// Marker for rowcodec restored data.
pub const RESTORE_DATA_FLAG: u8 = ROW_CODEC_VERSION;
/// Low bits retaining the original index ID.
pub const INDEX_ID_MASK: i64 = 0x0000_ffff_ffff_ffff;

/// Source column metadata used by tablecodec.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableColumn {
    /// Stable column ID.
    pub id: i64,
    /// Position in the table's column array.
    pub offset: usize,
    /// SQL type metadata.
    pub field_type: FieldType,
    /// Whether this is a primary-key column.
    pub primary_key: bool,
    /// Concurrent DDL field type used while modifying the column.
    pub changing_field_type: Option<FieldType>,
}

/// Source index-column metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexColumn {
    /// Position in the table's column array.
    pub offset: usize,
    /// Prefix length, or [`UNSPECIFIED_LENGTH`].
    pub length: i64,
    /// Whether this index column uses the concurrent DDL changing type.
    pub use_changing_type: bool,
}

/// Source index metadata required by tablecodec.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexInfo {
    /// Stable index ID.
    pub id: i64,
    /// Indexed columns in key order.
    pub columns: Vec<IndexColumn>,
    /// Whether the index enforces uniqueness.
    pub unique: bool,
    /// Whether partition ID is carried for a global index.
    pub global: bool,
    /// Global-index key format version.
    pub global_index_version: u8,
    /// Whether this index is the table primary key.
    pub primary: bool,
}

/// Source table metadata required by tablecodec.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TableInfo {
    /// Table columns in offset order.
    pub columns: Vec<TableColumn>,
    /// Table indexes.
    pub indices: Vec<IndexInfo>,
    /// Whether an integer primary key is the row handle.
    pub pk_is_handle: bool,
    /// Whether the primary key is a common handle.
    pub is_common_handle: bool,
    /// Common-handle index-value version.
    pub common_handle_version: u8,
}

impl TableInfo {
    fn index_columns(&self, index: &IndexInfo) -> Result<Vec<ColumnInfo>, TableIndexError> {
        index
            .columns
            .iter()
            .map(|index_column| {
                let column = self
                    .columns
                    .get(index_column.offset)
                    .ok_or(TableIndexError::Metadata("invalid index column offset"))?;
                let field_type = if index_column.use_changing_type {
                    column
                        .changing_field_type
                        .as_ref()
                        .unwrap_or(&column.field_type)
                } else {
                    &column.field_type
                };
                Ok(ColumnInfo {
                    id: column.id,
                    is_pk_handle: self.pk_is_handle && column.primary_key,
                    virtual_generated: false,
                    field_type: field_type.clone(),
                })
            })
            .collect()
    }

    /// Decodes the indexed datums stored by one index entry.
    pub fn decode_index_values_from_index(
        &self,
        use_new_collation: bool,
        timezone: Option<&SessionTimeZone>,
        index: &IndexInfo,
        key: &[u8],
        value: &[u8],
    ) -> Result<Vec<Datum>, TableIndexError> {
        decode_index_values_from_columns(
            use_new_collation,
            timezone,
            &self.index_columns(index)?,
            key,
            value,
        )
    }

    /// Mirrors `TableInfo.HasClusteredIndex`.
    pub const fn has_clustered_index(&self) -> bool {
        self.pk_is_handle || self.is_common_handle
    }

    /// Decodes and renders the indexed columns from one index entry.
    ///
    /// This is the byte-preserving counterpart of Go
    /// `tables.GenIndexValueFromIndex`. Binary strings and bit values escape
    /// every non-printable ASCII byte as an uppercase `\\xNN` sequence.
    pub fn generate_index_values_from_index(
        &self,
        use_new_collation: bool,
        timezone: Option<&SessionTimeZone>,
        index: &IndexInfo,
        key: &[u8],
        value: &[u8],
    ) -> Result<Vec<Vec<u8>>, TableIndexError> {
        render_index_values_from_columns(
            use_new_collation,
            timezone,
            &self.index_columns(index)?,
            key,
            value,
        )
    }
}

/// Decodes an index entry using column metadata in index key order.
pub fn decode_index_values_from_columns(
    use_new_collation: bool,
    timezone: Option<&SessionTimeZone>,
    columns: &[ColumnInfo],
    key: &[u8],
    value: &[u8],
) -> Result<Vec<Datum>, TableIndexError> {
    map_index_values(use_new_collation, columns, key, value, |encoded, field_type| {
        Ok(decode_column_value(encoded, field_type, timezone)?)
    })
}

/// Renders an index entry using column metadata in index key order.
/// Binary strings and BIT values escape non-printable bytes as uppercase hex.
pub fn render_index_values_from_columns(
    use_new_collation: bool,
    timezone: Option<&SessionTimeZone>,
    columns: &[ColumnInfo],
    key: &[u8],
    value: &[u8],
) -> Result<Vec<Vec<u8>>, TableIndexError> {
    map_index_values(use_new_collation, columns, key, value, |encoded, field_type| {
        let datum = decode_column_value(encoded, field_type, timezone)?;
        let mut rendered = datum.sql_bytes().unwrap_or_else(|_| encoded.to_vec());
        if field_type.is_binary_string() || field_type.code() == FieldTypeCode::Bit {
            rendered = format_non_ascii_printable_to_hex(&rendered);
        }
        Ok(rendered)
    })
}

fn map_index_values<T>(
    use_new_collation: bool,
    columns: &[ColumnInfo],
    key: &[u8],
    value: &[u8],
    project: impl Fn(&[u8], &FieldType) -> Result<T, TableIndexError>,
) -> Result<Vec<T>, TableIndexError> {
    if value.len() <= MAX_OLD_ENCODE_VALUE_LEN {
        if let [column] = columns {
            let (encoded, _) = cut_one(&key[INDEX_VALUES_OFFSET..])?;
            return Ok(vec![project(encoded, &column.field_type)?]);
        }
    }
    let encoded_values = decode_index_kv_refs(
        use_new_collation,
        key,
        value,
        columns.len(),
        HandleStatus::NotNeeded,
        columns,
    )?;
    encoded_values
        .iter()
        .zip(columns)
        .map(|(encoded, column)| project(encoded, &column.field_type))
        .collect()
}

fn format_non_ascii_printable_to_hex(bytes: &[u8]) -> Vec<u8> {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut rendered = Vec::with_capacity(bytes.len());
    for &byte in bytes {
        if (32..127).contains(&byte) {
            rendered.push(byte);
        } else {
            rendered.extend_from_slice(&[
                b'\\',
                b'x',
                HEX[usize::from(byte >> 4)],
                HEX[usize::from(byte & 0x0f)],
            ]);
        }
    }
    rendered
}

/// Whether `decode_index_kv` should materialize the row handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandleStatus {
    /// Encode integer handles as signed values.
    Default,
    /// Encode integer handles as unsigned values.
    Unsigned,
    /// Do not decode a handle.
    NotNeeded,
}

/// Failure while translating table index keys or values.
#[derive(Debug)]
pub enum TableIndexError {
    /// Datum codec failure.
    Codec(CodecError),
    /// Table-row conversion failure.
    Row(TableRowError),
    /// Rowcodec restored-data failure.
    RowCodec(RowPackageError),
    /// Malformed index key or value.
    Invalid(&'static str),
    /// Metadata and value arrays were inconsistent.
    Metadata(&'static str),
}

impl fmt::Display for TableIndexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(error) => error.fmt(formatter),
            Self::Row(error) => error.fmt(formatter),
            Self::RowCodec(error) => error.fmt(formatter),
            Self::Invalid(message) | Self::Metadata(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for TableIndexError {}

impl From<CodecError> for TableIndexError {
    fn from(error: CodecError) -> Self {
        Self::Codec(error)
    }
}

impl From<TableRowError> for TableIndexError {
    fn from(error: TableRowError) -> Self {
        Self::Row(error)
    }
}

impl From<RowPackageError> for TableIndexError {
    fn from(error: RowPackageError) -> Self {
        Self::RowCodec(error)
    }
}

/// Returns the exact key range for one table index.
pub fn get_table_index_key_range(table_id: i64, index_id: i64) -> (Vec<u8>, Vec<u8>) {
    (
        encode_index_seek_key(table_id, index_id, &[]),
        encode_index_seek_key(table_id, index_id, &[u8::MAX]),
    )
}

/// Cuts `length` encoded index columns and returns the remaining handle suffix.
pub fn cut_index_key(key: &[u8], length: usize) -> Result<(Vec<Vec<u8>>, &[u8]), TableIndexError> {
    cut_index_key_with(key, length, length, <[u8]>::to_vec)
}

/// Borrows the encoded index columns and the remaining handle suffix.
pub fn cut_index_key_refs(key: &[u8], length: usize) -> Result<(Vec<&[u8]>, &[u8]), TableIndexError> {
    cut_index_key_with(key, length, length, std::convert::identity)
}

fn cut_index_key_with<'a, T>(
    key: &'a [u8],
    length: usize,
    capacity: usize,
    project: impl Fn(&'a [u8]) -> T,
) -> Result<(Vec<T>, &'a [u8]), TableIndexError> {
    let mut remaining = &key[INDEX_VALUES_OFFSET..];
    let mut values = Vec::with_capacity(capacity);
    for _ in 0..length {
        let (value, tail) = cut_one(remaining)?;
        values.push(project(value));
        remaining = tail;
    }
    Ok((values, remaining))
}

/// Returns the handle suffix without materializing the encoded index columns.
pub fn skip_index_key_columns(key: &[u8], length: usize) -> Result<&[u8], TableIndexError> {
    let mut suffix = &key[INDEX_VALUES_OFFSET..];
    for _ in 0..length {
        let (_, remaining) = cut_one(suffix)?;
        suffix = remaining;
    }
    Ok(suffix)
}

/// Cuts index columns into a caller-provided column-ID map.
pub fn cut_index_key_by_ids<'a>(
    key: &'a [u8],
    column_ids: &[i64],
) -> Result<CutIndexValues<'a>, TableIndexError> {
    let (values, remaining) = cut_index_key(key, column_ids.len())?;
    Ok((column_ids.iter().copied().zip(values).collect(), remaining))
}

/// Reuses an index-key buffer by clearing it, or allocates the requested
/// default capacity.
pub fn get_index_key_buffer(buffer: Option<Vec<u8>>, default_capacity: usize) -> Vec<u8> {
    match buffer {
        Some(mut buffer) => {
            buffer.clear();
            buffer
        }
        None => Vec::with_capacity(default_capacity),
    }
}

/// Cuts an encoded common handle after its table-record prefix.
pub fn cut_common_handle(
    key: &[u8],
    length: usize,
) -> Result<(Vec<Vec<u8>>, &[u8]), TableIndexError> {
    let mut remaining = key
        .get(PREFIX_LEN..)
        .ok_or(TableIndexError::Invalid("invalid common handle key"))?;
    let mut values = Vec::with_capacity(length);
    for _ in 0..length {
        let (value, tail) = cut_one(remaining)?;
        values.push(value.to_vec());
        remaining = tail;
    }
    Ok((values, remaining))
}

/// Converts an ordinary index key to its temporary-index ID in place.
pub fn index_key_to_temp_index_key(key: &mut [u8]) -> Result<(), TableIndexError> {
    rewrite_index_id(key, |index_id| TEMP_INDEX_PREFIX | index_id)
}

/// Converts a temporary index key back to its original index ID in place.
pub fn temp_index_key_to_index_key(key: &mut [u8]) -> Result<(), TableIndexError> {
    rewrite_index_id(key, |index_id| index_id & INDEX_ID_MASK)
}

fn rewrite_index_id(
    key: &mut [u8],
    rewrite: impl FnOnce(i64) -> i64,
) -> Result<(), TableIndexError> {
    let bytes = key
        .get_mut(PREFIX_LEN..PREFIX_LEN + ID_LEN)
        .ok_or(TableIndexError::Invalid("short index key"))?;
    let encoded = u64::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| TableIndexError::Invalid("short index ID"))?,
    );
    bytes.copy_from_slice(
        &tidb_codec::encode_int_to_cmp_uint(rewrite(tidb_codec::decode_cmp_uint_to_int(encoded)))
            .to_be_bytes(),
    );
    Ok(())
}

/// Reports whether a key has the complete record-key prefix shape.
pub fn is_record_key(key: &[u8]) -> bool {
    key.len() > PREFIX_LEN && key.first() == Some(&b't') && key.get(10) == Some(&b'r')
}

/// Reports whether a key is exactly `t{table_id}`.
pub fn is_table_key(key: &[u8]) -> bool {
    key.len() == 9 && key.first() == Some(&b't')
}

/// Encodes an integer or common handle in a unique-index value.
pub fn encode_handle_in_unique_index_value(handle: &Handle, untouched: bool) -> Vec<u8> {
    match inner_handle(handle) {
        Handle::Int(value) => value.value().to_be_bytes().to_vec(),
        Handle::Common(handle) => {
            let mut value = vec![u8::from(untouched)];
            encode_common_handle(&mut value, handle.encoded());
            value
        }
        Handle::Partition(_) => unreachable!("inner_handle removes partition wrappers"),
    }
}

fn encode_common_handle(output: &mut Vec<u8>, encoded: &[u8]) {
    output.push(COMMON_HANDLE_FLAG);
    let length = u16::try_from(encoded.len()).expect("common handle is limited to uint16");
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(encoded);
}

fn encode_partition_id(output: &mut Vec<u8>, partition_id: i64) {
    output.push(PARTITION_ID_FLAG);
    encode_int(output, partition_id);
}

/// Decodes an eight-byte legacy integer-handle index value.
pub fn decode_int_handle_in_index_value(data: &[u8]) -> Result<Handle, TableIndexError> {
    Ok(IntHandle::new(read_index_integer(data)).into())
}

fn read_index_integer(data: &[u8]) -> i64 {
    let bytes: [u8; 8] = data[..ID_LEN].try_into().unwrap();
    i64::from_be_bytes(bytes)
}

/// Decodes a handle carried in a non-unique index-key suffix.
pub fn decode_handle_in_index_key(suffix: &[u8]) -> Result<Handle, TableIndexError> {
    if suffix.first() == Some(&PARTITION_ID_FLAG) {
        let (remaining, partition_id) = decode_int(&suffix[1..])?;
        return Ok(partition_handle(
            partition_id,
            decode_handle_in_index_key(remaining)?,
        ));
    }
    match decode_single_int_index_handle(suffix)? {
        Some(value) => Ok(IntHandle::new(value).into()),
        None => Ok(common_handle(suffix)?),
    }
}

fn decode_single_int_index_handle(suffix: &[u8]) -> Result<Option<i64>, TableIndexError> {
    match suffix {
        [INT_FLAG, payload @ ..] => {
            let (remaining, value) = decode_int(payload)?;
            return Ok(remaining.is_empty().then_some(value));
        }
        [BYTES_FLAG, payload @ ..] => {
            skip_bytes(payload)?;
            return Ok(None);
        }
        _ => {}
    }
    let (remaining, datum) = decode_one(suffix)?;
    if remaining.is_empty() {
        return Ok(datum.as_int());
    }
    Ok(None)
}

/// Self-describing segments of an extensible index value.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexValueSegments {
    /// Encoded common handle.
    pub common_handle: Option<Vec<u8>>,
    /// Encoded memcomparable partition ID.
    pub partition_id: Option<Vec<u8>>,
    /// Rowcodec restored values.
    pub restored_values: Option<Vec<u8>>,
    /// Legacy raw eight-byte integer handle.
    pub int_handle: Option<Vec<u8>>,
}

#[derive(Default)]
struct IndexValueSegmentsRef<'a> {
    common_handle: Option<&'a [u8]>,
    partition_id: Option<&'a [u8]>,
    restored_values: Option<&'a [u8]>,
    int_handle: Option<&'a [u8]>,
}

/// Half-open key range accepted by tablecodec's partition verifier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableKeyRange {
    /// Inclusive start key.
    pub start_key: Vec<u8>,
    /// Exclusive end key.
    pub end_key: Vec<u8>,
}

/// Verifies that each partition group contains ranges from exactly one
/// positive physical table ID.
pub fn verify_table_ids_for_ranges(
    partitions: &[Vec<TableKeyRange>],
) -> Result<Vec<i64>, TableIndexError> {
    let mut table_ids = Vec::with_capacity(partitions.len());
    for ranges in partitions {
        let Some(first) = ranges.first() else {
            continue;
        };
        let table_id = crate::table_key::decode_table_id(&first.start_key);
        if table_id <= 0 {
            return Err(TableIndexError::Invalid(
                "Incorrect keyRange is constrcuted",
            ));
        }
        for range in ranges.iter().skip(1) {
            let next_table_id = crate::table_key::decode_table_id(&range.start_key);
            if next_table_id <= 0 {
                return Err(TableIndexError::Invalid(
                    "Incorrect keyRange is constrcuted",
                ));
            }
            if next_table_id != table_id {
                return Err(TableIndexError::Invalid(
                    "Using multi partition's ranges as single table's",
                ));
            }
        }
        table_ids.push(table_id);
    }
    Ok(table_ids)
}

/// Returns the explicit index-value version, or zero for legacy/extensible V0.
pub fn index_value_version(value: &[u8]) -> u8 {
    if value.len() <= MAX_OLD_ENCODE_VALUE_LEN {
        return 0;
    }
    let tail_len = usize::from(value[0]);
    if matches!(tail_len, 0 | 1) && value.get(1) == Some(&INDEX_VERSION_FLAG) {
        value[2]
    } else {
        0
    }
}

/// Splits any supported index-value layout into its semantic segments.
pub fn split_index_value(value: &[u8]) -> Result<IndexValueSegments, TableIndexError> {
    let segments = split_index_value_ref(value);
    Ok(IndexValueSegments {
        common_handle: segments.common_handle.map(<[u8]>::to_vec),
        partition_id: segments.partition_id.map(<[u8]>::to_vec),
        restored_values: segments.restored_values.map(<[u8]>::to_vec),
        int_handle: segments.int_handle.map(<[u8]>::to_vec),
    })
}

fn split_index_value_ref(value: &[u8]) -> IndexValueSegmentsRef<'_> {
    if value.len() <= MAX_OLD_ENCODE_VALUE_LEN {
        return IndexValueSegmentsRef {
            int_handle: Some(value),
            ..IndexValueSegmentsRef::default()
        };
    }

    let header_len = if index_value_version(value) == 0 { 1 } else { 3 };
    split_extensible_index_value(value, header_len)
}

fn split_extensible_index_value(value: &[u8], header_len: usize) -> IndexValueSegmentsRef<'_> {
    let tail_len = usize::from(value[0]);
    let tail = &value[value.len() - tail_len..];
    let mut options = &value[header_len..value.len() - tail_len];
    let mut segments = IndexValueSegmentsRef::default();
    if tail.len() >= ID_LEN {
        segments.int_handle = Some(&tail[..ID_LEN]);
    }
    if options.first() == Some(&COMMON_HANDLE_FLAG) {
        let length = usize::from(u16::from_be_bytes(options[1..3].try_into().unwrap()));
        let end = 3 + length;
        segments.common_handle = Some(&options[3..end]);
        options = &options[end..];
    }
    if options.first() == Some(&PARTITION_ID_FLAG) {
        segments.partition_id = Some(&options[1..9]);
        options = &options[9..];
    }
    if options.first() == Some(&RESTORE_DATA_FLAG) {
        segments.restored_values = Some(options);
    }
    segments
}

/// Decodes a unique-index value handle, including partition wrapping.
pub fn decode_handle_in_index_value(value: &[u8]) -> Result<Option<Handle>, TableIndexError> {
    if value.len() <= MAX_OLD_ENCODE_VALUE_LEN {
        return decode_int_handle_in_index_value(value).map(Some);
    }
    let segments = split_index_value_ref(value);
    let mut handle = segments
        .int_handle
        .map(decode_int_handle_in_index_value)
        .transpose()?;
    if let Some(encoded) = segments.common_handle {
        handle = Some(common_handle(encoded)?);
    }
    if let Some(encoded) = segments.partition_id {
        let (_, partition_id) = decode_int(encoded)?;
        handle = Some(match handle {
            Some(handle) => partition_handle(partition_id, handle),
            None => PartitionHandle::new(partition_id, None).into(),
        });
    }
    Ok(handle)
}

/// Decodes a handle from the index key/value pair.
pub fn decode_index_handle(
    key: &[u8],
    value: &[u8],
    columns_len: usize,
) -> Result<Option<Handle>, TableIndexError> {
    let suffix = skip_index_key_columns(key, columns_len)?;
    if !suffix.is_empty() {
        let mut handle = decode_handle_in_index_key(suffix)?;
        if value.len() > MAX_OLD_ENCODE_VALUE_LEN {
            if let Some(encoded) = split_index_value_ref(value).partition_id {
                let (_, partition_id) = decode_int(encoded)?;
                handle = partition_handle(partition_id, inner_handle(&handle).clone());
            }
        }
        return Ok(Some(handle));
    }
    if value.len() >= ID_LEN {
        return decode_handle_in_index_value(value);
    }
    Err(TableIndexError::Invalid("no handle in index key or value"))
}

/// Reports whether an index value carries a unique-index handle.
pub fn index_kv_is_unique(value: &[u8]) -> bool {
    if value.len() <= MAX_OLD_ENCODE_VALUE_LEN {
        return value.len() == ID_LEN;
    }
    let version = index_value_version(value);
    if version == 1 {
        split_extensible_index_value(value, 3).common_handle.is_some()
    } else {
        let segments = split_extensible_index_value(value, 1);
        segments.int_handle.is_some() || segments.common_handle.is_some()
    }
}

/// Source `tablecodec.TruncateIndexValue`.
///
/// Go's shape is preserved arm for arm because each arm decides a different
/// part of the produced INDEX KEY:
///
/// - a non-string, non-bytes datum is left alone (Go tests `v.Kind()`, so a
///   `Raw` datum -- which `Datum::as_raw_bytes` also answers for -- is NOT a
///   truncation candidate);
/// - `binary`/`ascii` count the prefix in BYTES and keep the datum's kind;
/// - every other charset counts in Go RUNES and always re-lands the value as
///   a STRING carrying the table column's collation, even when the datum
///   arrived as `Bytes` (Go's `v.SetString`), because the collation is what
///   the key encoder then sorts by.
///
/// Verified against `tablecodec.TruncateIndexValue` itself over a byte-level
/// case table (`truncate_index_value_go_vectors`); notably `F0 9F` at prefix
/// length 1 is `EF BF BD` in Go, not the untouched `F0 9F` a
/// `from_utf8_lossy` rune count produces.
pub fn truncate_index_value(
    value: &mut Datum,
    index_column: &IndexColumn,
    table_column: &TableColumn,
) -> Result<(), TableIndexError> {
    cut_datum_by_prefix_len(value, index_column.length, &table_column.field_type);
    Ok(())
}

/// Go's prefix cut, the one body of `tablecodec.TruncateIndexValue` and
/// `ranger.CutDatumByPrefixLen`: a string or bytes datum cut to `length`
/// counted in bytes under the `binary`/`ascii` charsets of `column_type`
/// (keeping a bytes datum's kind) and in Go runes (`bytes.Runes`) under
/// every other, which always re-lands it as a string in `column_type`'s
/// collation. Answers whether a cut happened.
pub fn cut_datum_by_prefix_len(value: &mut Datum, length: i64, column_type: &FieldType) -> bool {
    let Some((end, by_bytes)) = datum_prefix_bound(value, length, column_type) else {
        return false;
    };
    if by_bytes {
        if let Datum::Bytes(bytes) = value {
            bytes.truncate(end);
            return true;
        }
    }
    *value = copy_datum_prefix(value, end, by_bytes, column_type);
    true
}

/// Copies the indexed portion of a borrowed datum into an owned key part.
pub fn clone_datum_by_prefix_len(value: &Datum, length: i64, column_type: &FieldType) -> Datum {
    datum_with_prefix_len(value, length, column_type).into_owned()
}

/// Borrows an unchanged value and copies only the indexed portion when cut.
pub fn datum_with_prefix_len<'a>(value: &'a Datum, length: i64, column_type: &FieldType) -> Cow<'a, Datum> {
    match datum_prefix_bound(value, length, column_type) {
        Some((end, by_bytes)) => Cow::Owned(copy_datum_prefix(value, end, by_bytes, column_type)),
        None => Cow::Borrowed(value),
    }
}

fn datum_prefix_bound(value: &Datum, length: i64, column_type: &FieldType) -> Option<(usize, bool)> {
    if length == UNSPECIFIED_LENGTH {
        return None;
    }
    let bytes = match value {
        Datum::Bytes(bytes) => bytes.as_slice(),
        Datum::String(string) => string.bytes(),
        _ => return None,
    };
    let length = usize::try_from(length).unwrap_or(usize::MAX);
    if bytes.len() <= length {
        return None;
    }
    if matches!(column_type.charset_name(), "binary" | "ascii") {
        return Some((length, true));
    }
    let end = rune_prefix_end(bytes, length)?;
    (end < bytes.len()).then_some((end, false))
}

fn copy_datum_prefix(value: &Datum, end: usize, by_bytes: bool, column_type: &FieldType) -> Datum {
    let bytes = &value.go_bytes()[..end];
    if by_bytes && matches!(value, Datum::Bytes(_)) {
        return Datum::Bytes(bytes.to_vec());
    }
    let collation = column_type.collation();
    if by_bytes || bytes.is_ascii() {
        Datum::String(StringDatum::from_slice(bytes, collation))
    } else {
        Datum::new_collation_string_datum(
            tidb_goruntime::decode_go_utf8_lossy(bytes).into_bytes(),
            collation,
        )
    }
}

/// Go `ranger.ReachPrefixLen`: whether the value is EXACTLY `length` long,
/// counted as [`cut_datum_by_prefix_len`] counts.
#[must_use]
pub fn reach_prefix_len(value: &Datum, length: i64, column_type: &FieldType) -> bool {
    if length == UNSPECIFIED_LENGTH {
        return false;
    }
    let bytes = match value {
        Datum::Bytes(bytes) => bytes.as_slice(),
        Datum::String(string) => string.bytes(),
        _ => return false,
    };
    let length = usize::try_from(length).unwrap_or(usize::MAX);
    match column_type.charset_name() {
        "binary" | "ascii" => bytes.len() == length,
        _ => bytes.len() <= length.saturating_mul(4)
            && rune_prefix_end(bytes, length) == Some(bytes.len()),
    }
}

/// Byte offset after exactly `length` Go runes, if the input has that many.
fn rune_prefix_end(bytes: &[u8], length: usize) -> Option<usize> {
    if bytes.get(..length)?.is_ascii() {
        return Some(length);
    }
    // A rune occupies at most four bytes. Any partial sequence at this
    // bound starts after the requested runes and cannot affect the cut.
    let bound = bytes.len().min(length.saturating_mul(4));
    let mut offset = 0;
    let mut remaining = length;
    for chunk in bytes[..bound].utf8_chunks() {
        let valid = chunk.valid();
        let count = valid.chars().count();
        if remaining < count {
            return valid.char_indices().nth(remaining).map(|(i, _)| offset + i);
        }
        offset += valid.len();
        remaining -= count;
        // Go consumes each invalid byte as a separate replacement rune.
        let invalid = chunk.invalid().len();
        if remaining <= invalid {
            return Some(offset + remaining);
        }
        remaining -= invalid;
        offset += invalid;
    }
    None
}

/// Truncates every indexed value using table/index metadata.
pub fn truncate_index_values(
    table: &TableInfo,
    index: &IndexInfo,
    values: &mut [Datum],
) -> Result<(), TableIndexError> {
    for (position, value) in values.iter_mut().enumerate() {
        let index_column = &index.columns[position];
        let table_column = &table.columns[index_column.offset];
        truncate_index_value(value, index_column, table_column)?;
    }
    Ok(())
}

/// Generates a complete index key and whether its values are distinct.
pub fn generate_index_key(
    encoder: Encoder,
    timezone: Option<&SessionTimeZone>,
    table: &TableInfo,
    index: &IndexInfo,
    physical_table_id: i64,
    indexed_values: &mut [Datum],
    handle: Option<&Handle>,
) -> Result<(Vec<u8>, bool), TableIndexError> {
    truncate_index_values(table, index, indexed_values)?;
    encode_truncated_index_key(
        encoder,
        timezone,
        table.has_clustered_index(),
        index,
        physical_table_id,
        indexed_values,
        handle,
    )
}

/// Builds a key from values whose index prefixes the caller already applied.
pub fn encode_truncated_index_key(
    encoder: Encoder,
    timezone: Option<&SessionTimeZone>,
    has_clustered_index: bool,
    index: &IndexInfo,
    physical_table_id: i64,
    indexed_values: &[Datum],
    handle: Option<&Handle>,
) -> Result<(Vec<u8>, bool), TableIndexError> {
    encode_truncated_index_key_refs(
        encoder,
        timezone,
        has_clustered_index,
        index,
        physical_table_id,
        indexed_values.iter(),
        handle,
    )
}

/// Builds a key from the borrowed values in an already-truncated tuple.
pub fn encode_truncated_index_key_refs<'a>(
    encoder: Encoder,
    timezone: Option<&SessionTimeZone>,
    has_clustered_index: bool,
    index: &IndexInfo,
    physical_table_id: i64,
    indexed_values: impl Clone + Iterator<Item = &'a Datum>,
    handle: Option<&Handle>,
) -> Result<(Vec<u8>, bool), TableIndexError> {
    let distinct = index.unique && indexed_values.clone().all(|value| !value.is_null());
    let suffix = handle.filter(|_| !distinct);
    let suffix_len = suffix.map_or(0, |handle| {
        let partition_len = if index.global_index_version >= 1 { ID_LEN + 1 } else { 0 };
        let handle_len = match inner_handle(handle) {
            Handle::Common(value) => value.encoded().len(),
            _ => ID_LEN + 1,
        };
        partition_len + handle_len
    });
    let values_len = tidb_codec::pre_realloc_size_for_refs(indexed_values.clone(), true).unwrap_or(0);
    let mut key = Vec::with_capacity(INDEX_VALUES_OFFSET + values_len + suffix_len);
    append_index_key_prefix(&mut key, physical_table_id, index.id);
    match timezone {
        Some(timezone) => encoder.append_key_refs_in_timezone(timezone, &mut key, indexed_values)?,
        None => encoder.append_key_refs_in_timezone(&chrono::Utc, &mut key, indexed_values)?,
    }
    if let Some(handle) = suffix {
        if index.global_index_version >= 1 {
            if has_clustered_index {
                return Err(TableIndexError::Metadata(
                    "clustered index is not supported in GlobalIndexVersionV1+",
                ));
            }
            let partition_id = handle_partition_id(handle).ok_or(TableIndexError::Metadata(
                "handle is not a PartitionHandle in GlobalIndexVersionV1+",
            ))?;
            encode_partition_id(&mut key, partition_id);
        }
        match inner_handle(handle) {
            Handle::Int(value) => {
                key.push(INT_FLAG);
                encode_int(&mut key, value.value());
            }
            Handle::Common(value) => key.extend_from_slice(value.encoded()),
            Handle::Partition(_) => {
                unreachable!("inner_handle removes partition wrappers")
            }
        }
    }
    Ok((key, distinct))
}

/// Generates the selected legacy, extensible V0, or common-handle V1 index
/// value.
#[allow(clippy::too_many_arguments)]
pub fn generate_index_value(
    use_new_collation: bool,
    timezone: Option<&SessionTimeZone>,
    table: &TableInfo,
    index: &IndexInfo,
    need_restored_data: bool,
    distinct: bool,
    untouched: bool,
    indexed_values: &[Datum],
    handle: &Handle,
    partition_id: i64,
    handle_restored_data: &[Datum],
) -> Result<Vec<u8>, TableIndexError> {
    generate_index_value_with_columns(
        use_new_collation,
        timezone,
        table.is_common_handle && table.common_handle_version == 1,
        index.global,
        &index.columns,
        need_restored_data,
        distinct,
        untouched,
        indexed_values,
        IndexValueHandle::from(handle),
        partition_id,
        handle_restored_data,
        |offset| table.columns.get(offset).map(Cow::Borrowed),
        || common_pk_restored_column_ids(use_new_collation, table),
    )
}

/// The parts of a row handle used by the index value encoder.
#[derive(Clone, Copy)]
pub enum IndexValueHandle<'a> {
    /// An integer handle.
    Int(i64),
    /// A validated common handle including its minimum-width padding.
    Common(&'a [u8]),
}

impl<'a> From<&'a Handle> for IndexValueHandle<'a> {
    fn from(handle: &'a Handle) -> Self {
        match inner_handle(handle) {
            Handle::Int(handle) => Self::Int(handle.value()),
            Handle::Common(handle) => Self::Common(handle.encoded()),
            Handle::Partition(_) => unreachable!("inner_handle removes partition wrappers"),
        }
    }
}

/// Encodes an index value, resolving table metadata only for restored data.
#[allow(clippy::too_many_arguments)]
pub fn generate_index_value_with_columns<'a>(
    use_new_collation: bool,
    timezone: Option<&SessionTimeZone>,
    common_handle_v1: bool,
    global: bool,
    index_columns: &[IndexColumn],
    need_restored_data: bool,
    distinct: bool,
    untouched: bool,
    indexed_values: &[impl Borrow<Datum>],
    handle: IndexValueHandle<'_>,
    partition_id: i64,
    handle_restored_data: &[Datum],
    column_at: impl Fn(usize) -> Option<Cow<'a, TableColumn>>,
    common_pk_ids: impl FnOnce() -> Vec<i64>,
) -> Result<Vec<u8>, TableIndexError> {
    if common_handle_v1 {
        generate_index_value_v1(
            use_new_collation,
            timezone,
            global,
            index_columns,
            need_restored_data,
            distinct,
            untouched,
            indexed_values,
            handle,
            partition_id,
            handle_restored_data,
            column_at,
            common_pk_ids,
        )
    } else {
        generate_index_value_v0(
            timezone,
            global,
            index_columns,
            need_restored_data,
            distinct,
            untouched,
            indexed_values,
            handle,
            partition_id,
            column_at,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn generate_index_value_v0<'a>(
    timezone: Option<&SessionTimeZone>,
    global: bool,
    index_columns: &[IndexColumn],
    need_restored_data: bool,
    distinct: bool,
    untouched: bool,
    indexed_values: &[impl Borrow<Datum>],
    handle: IndexValueHandle<'_>,
    partition_id: i64,
    column_at: impl Fn(usize) -> Option<Cow<'a, TableColumn>>,
) -> Result<Vec<u8>, TableIndexError> {
    let int_handle = match handle {
        IndexValueHandle::Int(value) if distinct => Some(value),
        _ => None,
    };
    if !global && !need_restored_data {
        if !distinct {
            let marker = if untouched {
                UNCOMMITTED_INDEX_KV_FLAG
            } else {
                b'0'
            };
            return Ok(vec![marker]);
        }
        if let Some(int_handle) = int_handle {
            let mut value = Vec::with_capacity(ID_LEN + usize::from(untouched));
            value.extend_from_slice(&int_handle.to_be_bytes());
            if untouched {
                value.push(UNCOMMITTED_INDEX_KV_FLAG);
            }
            return Ok(value);
        }
    }
    let common_handle = match handle {
        IndexValueHandle::Common(bytes) if distinct => Some(bytes),
        _ => None,
    };
    let common_len = common_handle.map_or(0, |bytes| COMMON_HANDLE_HEADER_LEN + bytes.len());
    let partition_len = usize::from(global) * PARTITION_ID_SEGMENT_LEN;
    let int_len = usize::from(int_handle.is_some()) * ID_LEN;
    let capacity = (1 + common_len + partition_len + int_len).max(10) + usize::from(untouched);
    let mut value = Vec::with_capacity(capacity);
    value.push(0);
    let mut tail_len = 0;
    if let Some(bytes) = common_handle {
        encode_common_handle(&mut value, bytes);
    }
    if global {
        encode_partition_id(&mut value, partition_id);
    }
    if need_restored_data {
        let ids = index_columns
            .iter()
            .map(|column| {
                column_at(column.offset)
                    .map(|column| column.id)
                    .ok_or(TableIndexError::Metadata("invalid index column offset"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        encode_row_with_values(
            timezone,
            &ids,
            indexed_values,
            &RowChecksumPolicy::None,
            &mut value,
        )?;
    }
    if let Some(int_handle) = int_handle {
        tail_len += ID_LEN;
        value.extend_from_slice(&int_handle.to_be_bytes());
    } else if value.len() < 10 {
        let padding = 10 - value.len();
        tail_len += padding;
        value.resize(10, 0);
    }
    if untouched {
        tail_len += 1;
        value.push(UNCOMMITTED_INDEX_KV_FLAG);
    }
    value[0] = u8::try_from(tail_len)
        .map_err(|_| TableIndexError::Invalid("index-value tail exceeds uint8"))?;
    Ok(value)
}

#[allow(clippy::too_many_arguments)]
fn generate_index_value_v1<'a>(
    use_new_collation: bool,
    timezone: Option<&SessionTimeZone>,
    global: bool,
    index_columns: &[IndexColumn],
    need_restored_data: bool,
    distinct: bool,
    untouched: bool,
    indexed_values: &[impl Borrow<Datum>],
    handle: IndexValueHandle<'_>,
    partition_id: i64,
    handle_restored_data: &[Datum],
    column_at: impl Fn(usize) -> Option<Cow<'a, TableColumn>>,
    common_pk_ids: impl FnOnce() -> Vec<i64>,
) -> Result<Vec<u8>, TableIndexError> {
    let int_bytes;
    let encoded_handle = if distinct {
        Some(match handle {
            IndexValueHandle::Int(value) => {
                int_bytes = tidb_codec::encode_int_to_cmp_uint(value).to_be_bytes();
                int_bytes.as_slice()
            }
            IndexValueHandle::Common(bytes) => bytes,
        })
    } else {
        None
    };
    let handle_len = encoded_handle.map_or(0, |bytes| COMMON_HANDLE_HEADER_LEN + bytes.len());
    let partition_len = usize::from(global) * PARTITION_ID_SEGMENT_LEN;
    let mut value = Vec::with_capacity(3 + handle_len + partition_len + usize::from(untouched));
    value.extend_from_slice(&[0, INDEX_VERSION_FLAG, 1]);
    if let Some(bytes) = encoded_handle {
        encode_common_handle(&mut value, bytes);
    }
    if global {
        encode_partition_id(&mut value, partition_id);
    }
    if need_restored_data || !handle_restored_data.is_empty() {
        let mut ids = Vec::new();
        let mut data = Vec::new();
        for (position, index_column) in index_columns.iter().enumerate() {
            let column = column_at(index_column.offset)
                .ok_or(TableIndexError::Metadata("invalid index column offset"))?;
            if column.primary_key {
                continue;
            }
            let field_type = if index_column.use_changing_type {
                column
                    .changing_field_type
                    .as_ref()
                    .unwrap_or(&column.field_type)
            } else {
                &column.field_type
            };
            if field_type.need_restored_data_with_collation(use_new_collation) {
                ids.push(column.id);
                let indexed = indexed_values
                    .get(position)
                    .ok_or(TableIndexError::Metadata("missing indexed value"))?
                    .borrow();
                if is_bin_collation(field_type.collation_name()) {
                    let spaces = indexed
                        .as_raw_bytes()
                        .map(|bytes| bytes.iter().rev().take_while(|byte| **byte == b' ').count())
                        .unwrap_or(0);
                    data.push(Cow::Owned(Datum::UInt(spaces as u64)));
                } else {
                    data.push(Cow::Borrowed(indexed));
                }
            }
        }
        if !handle_restored_data.is_empty() {
            ids.extend(common_pk_ids());
            data.extend(handle_restored_data.iter().map(Cow::Borrowed));
        }
        encode_row_with_values(timezone, &ids, &data, &RowChecksumPolicy::None, &mut value)?;
    }
    if untouched {
        value[0] = 1;
        value.push(UNCOMMITTED_INDEX_KV_FLAG);
    }
    Ok(value)
}

/// Returns common-primary-key column IDs requiring restored data.
pub fn common_pk_restored_column_ids(use_new_collation: bool, table: &TableInfo) -> Vec<i64> {
    table
        .indices
        .iter()
        .find(|index| index.primary)
        .into_iter()
        .flat_map(|index| &index.columns)
        .filter_map(|index_column| table.columns.get(index_column.offset))
        .filter(|column| {
            column
                .field_type
                .need_restored_data_with_collation(use_new_collation)
        })
        .map(|column| column.id)
        .collect()
}

fn append_index_key_handle<'a>(
    results: &mut Vec<Cow<'a, [u8]>>,
    mut suffix: &'a [u8],
    unsigned: bool,
) -> Result<(), TableIndexError> {
    // Partition metadata is returned separately from the value segment.
    while suffix.first() == Some(&PARTITION_ID_FLAG) {
        (suffix, _) = decode_int(&suffix[1..])?;
    }
    if let Some(value) = decode_single_int_index_handle(suffix)? {
        results.push(Cow::Owned(encode_index_integer(value, unsigned)));
    } else {
        append_common_handle_columns(results, suffix)?;
    }
    Ok(())
}

fn append_common_handle_columns<'a>(
    results: &mut Vec<Cow<'a, [u8]>>,
    encoded: &'a [u8],
) -> Result<(), TableIndexError> {
    if encoded.len() <= usize::from(u16::MAX) {
        CommonHandle::scan_encoded_columns(encoded, |column| {
            results.push(Cow::Borrowed(column));
        })?;
    } else {
        // Keep the source's 16-bit offset behavior for oversized inputs.
        let handle = common_handle(encoded)?;
        results.extend(encoded_handle_columns(&handle)?.into_iter().map(Cow::Owned));
    }
    Ok(())
}

fn encode_index_integer(value: i64, unsigned: bool) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(11);
    if unsigned {
        encoded.push(UVARINT_FLAG);
        encode_uvarint(&mut encoded, value as u64);
    } else {
        encoded.push(VARINT_FLAG);
        encode_varint(&mut encoded, value);
    }
    encoded
}

fn decode_restored_values(
    columns: &[ColumnInfo],
    restored: &[u8],
) -> Result<Vec<Vec<u8>>, TableIndexError> {
    if columns.windows(2).all(|pair| pair[0].id < pair[1].id) {
        return Ok(decode_row_to_old_bytes_in_order(restored, columns)?);
    }
    let offsets = columns
        .iter()
        .enumerate()
        .map(|(index, column)| (column.id, index))
        .collect::<BTreeMap<_, _>>();
    Ok(decode_row_to_old_bytes(
        restored,
        columns,
        &offsets,
        &[],
        None,
        &[],
        None,
    )?)
}

fn restored_columns(use_new_collation: bool, columns: &[ColumnInfo]) -> Vec<ColumnInfo> {
    columns
        .iter()
        .filter(|column| {
            column
                .field_type
                .need_restored_data_with_collation(use_new_collation)
        })
        .map(|column| ColumnInfo {
            id: column.id,
            is_pk_handle: column.is_pk_handle,
            virtual_generated: column.virtual_generated,
            field_type: if is_bin_collation(column.field_type.collation_name()) {
                FieldType::new(FieldTypeCode::LongLong).with_unsigned(true)
            } else {
                column.field_type.clone()
            },
        })
        .collect()
}

fn decode_restored_values_v5(
    use_new_collation: bool,
    columns: &[ColumnInfo],
    results: &mut [Cow<'_, [u8]>],
    restored: &[u8],
) -> Result<(), TableIndexError> {
    if let ([column], [result, ..]) = (columns, &mut *results) {
        let field_type = &column.field_type;
        if field_type.code().is_type_varchar()
            && is_bin_collation(field_type.collation_name())
            && field_type.need_restored_data_with_collation(use_new_collation)
        {
            if let Cow::Borrowed([BYTES_FLAG, encoded @ ..]) = *result {
                let (row, _) = RowDecoder::parse(restored).map_err(RowPackageError::Decode)?;
                if let RawRowValue::NotNull { bytes, .. } =
                    row.column(column.id).map_err(RowPackageError::Decode)?
                {
                    let padding = decode_raw_uint(bytes).map_err(RowPackageError::Decode)? as usize;
                    let (prefix, tail) = split_cut_bytes(encoded).map_err(TableRowError::Codec)?;
                    if padding != 0 {
                        *result = Cow::Owned(encode_cut_bytes_with_spaces(prefix, tail, padding));
                    }
                    return Ok(());
                }
            }
        }
    }

    let restore_columns = restored_columns(use_new_collation, columns);
    let offsets = columns
        .iter()
        .enumerate()
        .map(|(index, column)| (column.id, index))
        .collect::<BTreeMap<_, _>>();
    let restored_values =
        decode_row_to_old_bytes(restored, &restore_columns, &offsets, &[], None, &[], None)?;
    for (index, restored_value) in restored_values.into_iter().enumerate() {
        if restored_value.is_empty() {
            continue;
        }
        let field_type = &columns[index].field_type;
        if is_bin_collation(field_type.collation_name()) {
            let padding = || -> Result<usize, TableIndexError> {
                let count_type = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);
                decode_column_value(&restored_value, &count_type, None)?
                    .as_uint()
                    .map(|count| count as usize)
                    .ok_or(TableIndexError::Invalid("invalid restored padding"))
            };
            let cut = match results[index] {
                Cow::Borrowed([BYTES_FLAG, encoded @ ..]) if field_type.code().is_type_varchar() => {
                    Some(encoded)
                }
                _ => None,
            };
            if let Some(encoded) = cut {
                let (prefix, tail) = split_cut_bytes(encoded).map_err(TableRowError::Codec)?;
                let padding = padding()?;
                if padding != 0 {
                    results[index] = Cow::Owned(encode_cut_bytes_with_spaces(prefix, tail, padding));
                }
                continue;
            }
            let original = decode_column_value(&results[index], field_type, None)?;
            let padding = padding()?;
            if padding == 0 {
                continue;
            }
            let mut bytes = original
                .into_raw_bytes()
                .ok_or(TableIndexError::Invalid("invalid restored string"))?;
            bytes.resize(bytes.len() + padding, b' ');
            let mut encoded = vec![BYTES_FLAG];
            encode_bytes(&mut encoded, &bytes);
            results[index] = Cow::Owned(encoded);
        } else {
            results[index] = Cow::Owned(restored_value);
        }
    }
    Ok(())
}

fn encode_cut_bytes_with_spaces(prefix: &[u8], tail: &[u8], padding: usize) -> Vec<u8> {
    let mut tail = smallvec::SmallVec::<[u8; 32]>::from_slice(tail);
    tail.resize(tail.len() + padding, b' ');
    let mut encoded = Vec::with_capacity(1 + prefix.len() + encoded_bytes_len(tail.len()));
    encoded.push(BYTES_FLAG);
    encoded.extend_from_slice(prefix);
    encode_bytes(&mut encoded, &tail);
    encoded
}

/// Decodes index columns and an optional handle into old datum-value bytes.
pub fn decode_index_kv(
    use_new_collation: bool,
    key: &[u8],
    value: &[u8],
    columns_len: usize,
    handle_status: HandleStatus,
    columns: &[ColumnInfo],
) -> Result<Vec<Vec<u8>>, TableIndexError> {
    if value.len() <= MAX_OLD_ENCODE_VALUE_LEN && handle_status == HandleStatus::NotNeeded {
        return cut_index_key(key, columns_len).map(|(values, _)| values);
    }
    let results = decode_index_kv_refs(use_new_collation, key, value, columns_len, handle_status, columns)?;
    Ok(results.into_iter().map(Cow::into_owned).collect())
}

/// Decodes index columns and an optional handle, borrowing unchanged encoded fields.
pub fn decode_index_kv_refs<'a>(
    use_new_collation: bool,
    key: &'a [u8],
    value: &'a [u8],
    columns_len: usize,
    handle_status: HandleStatus,
    columns: &[ColumnInfo],
) -> Result<Vec<Cow<'a, [u8]>>, TableIndexError> {
    let capacity =
        columns_len.saturating_add(usize::from(handle_status != HandleStatus::NotNeeded));
    let (mut results, suffix) = cut_index_key_with(key, columns_len, capacity, Cow::Borrowed)?;
    if value.len() <= MAX_OLD_ENCODE_VALUE_LEN {
        if handle_status == HandleStatus::NotNeeded {
            return Ok(results);
        }
        let unsigned = handle_status == HandleStatus::Unsigned;
        if suffix.is_empty() {
            results.push(Cow::Owned(encode_index_integer(
                read_index_integer(value),
                unsigned,
            )));
        } else {
            append_index_key_handle(&mut results, suffix, unsigned)?;
        }
        return Ok(results);
    }

    let version_one = index_value_version(value) == 1;
    let segments = split_index_value_ref(value);
    if let Some(restored) = segments.restored_values {
        if version_one {
            decode_restored_values_v5(
                use_new_collation,
                &columns[..columns_len],
                &mut results,
                restored,
            )?;
        } else {
            results.clear();
            results.extend(
                decode_restored_values(&columns[..columns_len], restored)?
                    .into_iter()
                    .map(Cow::Owned),
            );
        }
    }
    if handle_status == HandleStatus::NotNeeded {
        return Ok(results);
    }

    if version_one {
        // V1 uses common-handle fields in both the value segment and key suffix.
        let start = results.len();
        let encoded = segments.common_handle.unwrap_or(suffix);
        append_common_handle_columns(&mut results, encoded)?;
        if let Some(restored) = segments.restored_values {
            let handle_columns = columns.get(columns_len..).unwrap_or_default();
            let mut relevant_len = handle_columns.len();
            while relevant_len > 0 && handle_columns[relevant_len - 1].id < 0 {
                relevant_len -= 1;
            }
            decode_restored_values_v5(
                use_new_collation,
                &handle_columns[..relevant_len],
                &mut results[start..],
                restored,
            )?;
        }
    } else {
        let unsigned = handle_status == HandleStatus::Unsigned;
        if let Some(encoded) = segments.int_handle {
            results.push(Cow::Owned(encode_index_integer(
                read_index_integer(encoded),
                unsigned,
            )));
        } else {
            append_index_key_handle(
                &mut results,
                segments.common_handle.unwrap_or(suffix),
                unsigned,
            )?;
        }
    }
    if let Some(encoded) = segments.partition_id {
        let (_, partition_id) = decode_int(encoded)?;
        results.push(Cow::Owned(encode_index_integer(partition_id, false)));
    }
    Ok(results)
}

/// Allocation-reuse entry point corresponding to Go `DecodeIndexKVEx`.
pub fn decode_index_kv_ex(
    use_new_collation: bool,
    key: &[u8],
    value: &[u8],
    columns_len: usize,
    handle_status: HandleStatus,
    columns: &[ColumnInfo],
    output: &mut Vec<Vec<u8>>,
) -> Result<(), TableIndexError> {
    output.clear();
    output.extend(decode_index_kv(
        use_new_collation,
        key,
        value,
        columns_len,
        handle_status,
        columns,
    )?);
    Ok(())
}

/// Temporary-index value operation flag.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum TempIndexValueFlag {
    /// Distinct normal value.
    Normal = 0,
    /// Non-distinct normal value.
    NonDistinctNormal = 1,
    /// Distinct deletion.
    Deleted = 2,
    /// Non-distinct deletion.
    NonDistinctDeleted = 3,
}

/// One temporary-index history element.
#[derive(Clone, Debug)]
pub struct TempIndexValueElem<B = Vec<u8>> {
    /// Encoded ordinary index value.
    pub value: B,
    /// Handle for a distinct deletion.
    pub handle: Option<Handle>,
    /// Temporary-index stage byte.
    pub key_version: u8,
    /// Whether this operation deletes the original index key.
    pub delete: bool,
    /// Whether the index key is distinct.
    pub distinct: bool,
    /// Whether a deleted handle carries a partition ID.
    pub global: bool,
}

/// A temporary-index operation whose value borrows the encoded history.
pub type TempIndexValueElemRef<'a> = TempIndexValueElem<&'a [u8]>;

impl PartialEq for TempIndexValueElem {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
            && self.key_version == other.key_version
            && self.delete == other.delete
            && self.distinct == other.distinct
            && self.global == other.global
            && match (&self.handle, &other.handle) {
                (None, None) => true,
                (Some(left), Some(right)) => handle_identity(left) == handle_identity(right),
                _ => false,
            }
    }
}

impl Eq for TempIndexValueElem {}

/// Temporary-index operation history.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TempIndexValue {
    /// Operations in write order.
    pub elements: Vec<TempIndexValueElem>,
}

impl TempIndexValue {
    /// Reports whether the history has no operations.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Returns the latest operation.
    pub fn current(&self) -> Option<&TempIndexValueElem> {
        self.elements.last()
    }

    /// Removes older operations overwritten by the same distinct handle.
    pub fn filter_overwritten(self) -> Self {
        Self {
            elements: filter_overwritten_temp_index_values(self.elements),
        }
    }
}

impl<B: AsRef<[u8]>> TempIndexValueElem<B> {
    /// Encodes one history element and appends it to `output`.
    pub fn encode(&self, output: &mut Vec<u8>) -> Result<(), TableIndexError> {
        match (self.delete, self.distinct) {
            (true, true) => {
                let handle = self.handle.as_ref().ok_or(TableIndexError::Invalid(
                    "deleted distinct value has no handle",
                ))?;
                let encoded = match inner_handle(handle) {
                    Handle::Int(value) => (value.value() as u64).to_be_bytes().to_vec(),
                    Handle::Common(value) => value.encoded().to_vec(),
                    Handle::Partition(_) => {
                        unreachable!("inner_handle removes partition wrappers")
                    }
                };
                output.push(TempIndexValueFlag::Deleted as u8);
                output.extend_from_slice(
                    &u16::try_from(encoded.len())
                        .map_err(|_| TableIndexError::Invalid("temporary handle too long"))?
                        .to_be_bytes(),
                );
                output.extend_from_slice(&encoded);
                if self.global {
                    output.push(b'p');
                    encode_int(
                        output,
                        handle_partition_id(handle).ok_or(TableIndexError::Invalid(
                            "global temp value requires partition handle",
                        ))?,
                    );
                }
            }
            (true, false) => output.push(TempIndexValueFlag::NonDistinctDeleted as u8),
            (false, true) => {
                let value = self.value.as_ref();
                output.push(TempIndexValueFlag::Normal as u8);
                output.extend_from_slice(
                    &u16::try_from(value.len())
                        .map_err(|_| TableIndexError::Invalid("temporary value too long"))?
                        .to_be_bytes(),
                );
                output.extend_from_slice(value);
            }
            (false, false) => {
                output.push(TempIndexValueFlag::NonDistinctNormal as u8);
                output.extend_from_slice(self.value.as_ref());
            }
        }
        output.push(self.key_version);
        Ok(())
    }
}

impl TempIndexValueElem {
    /// Decodes one history element and returns the remaining bytes.
    pub fn decode(input: &[u8]) -> Result<(Self, &[u8]), TableIndexError> {
        let (element, remaining) = TempIndexValueElemRef::decode_ref(input)?;
        let owned = Self {
            value: element.value.to_vec(),
            handle: element.handle,
            key_version: element.key_version,
            delete: element.delete,
            distinct: element.distinct,
            global: element.global,
        };
        Ok((owned, remaining))
    }
}

impl<'a> TempIndexValueElemRef<'a> {
    fn decode_ref(input: &'a [u8]) -> Result<(Self, &'a [u8]), TableIndexError> {
        let (&flag, mut input) = input
            .split_first()
            .ok_or(TableIndexError::Invalid("empty temporary index value"))?;
        let mut element = Self {
            value: &[],
            handle: None,
            key_version: 0,
            delete: false,
            distinct: false,
            global: false,
        };
        match flag {
            value if value == TempIndexValueFlag::Normal as u8 => {
                let length = read_u16(&mut input)?;
                element.value = take(&mut input, length)?;
                element.key_version = take(&mut input, 1)?[0];
                element.distinct = true;
            }
            value if value == TempIndexValueFlag::NonDistinctNormal as u8 => {
                let (&key_version, value) = input
                    .split_last()
                    .ok_or(TableIndexError::Invalid("short temporary index value"))?;
                element.value = value;
                element.key_version = key_version;
                input = &[];
            }
            value if value == TempIndexValueFlag::Deleted as u8 => {
                let length = read_u16(&mut input)?;
                let encoded = take(&mut input, length)?;
                element.handle = Some(if length == ID_LEN {
                    decode_int_handle_in_index_value(encoded)?
                } else {
                    common_handle(encoded)?
                });
                if input.first() == Some(&b'p') {
                    element.global = true;
                    let (tail, partition_id) = decode_int(&input[1..])?;
                    input = tail;
                    element.handle = Some(partition_handle(
                        partition_id,
                        element.handle.take().unwrap(),
                    ));
                }
                element.key_version = take(&mut input, 1)?[0];
                element.distinct = true;
                element.delete = true;
            }
            value if value == TempIndexValueFlag::NonDistinctDeleted as u8 => {
                element.key_version = take(&mut input, 1)?[0];
                element.delete = true;
            }
            _ => return Err(TableIndexError::Invalid("invalid temp index value")),
        }
        Ok((element, input))
    }
}

fn read_u16(input: &mut &[u8]) -> Result<usize, TableIndexError> {
    let bytes = take(input, 2)?;
    Ok(usize::from(u16::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| TableIndexError::Invalid("short uint16"))?,
    )))
}

fn take<'a>(input: &mut &'a [u8], length: usize) -> Result<&'a [u8], TableIndexError> {
    let value = input
        .get(..length)
        .ok_or(TableIndexError::Invalid("short temporary index value"))?;
    *input = &input[length..];
    Ok(value)
}

/// Decodes every temporary-index history element.
pub fn decode_temp_index_value(
    mut value: &[u8],
) -> Result<Vec<TempIndexValueElem>, TableIndexError> {
    let mut elements = Vec::new();
    while !value.is_empty() {
        let (element, remaining) = TempIndexValueElem::decode(value)?;
        elements.push(element);
        value = remaining;
    }
    Ok(elements)
}

/// Decodes the history while borrowing normal index values from the input.
pub fn decode_temp_index_value_refs(
    mut value: &[u8],
) -> Result<Vec<TempIndexValueElemRef<'_>>, TableIndexError> {
    let mut elements = Vec::new();
    while !value.is_empty() {
        let (element, remaining) = TempIndexValueElemRef::decode_ref(value)?;
        elements.push(element);
        value = remaining;
    }
    Ok(elements)
}

/// Reads the latest operation and validates the history without collecting it.
pub fn decode_current_temp_index_value(
    mut value: &[u8],
) -> Result<Option<TempIndexValueElemRef<'_>>, TableIndexError> {
    let mut current = None;
    while !value.is_empty() {
        let (element, remaining) = TempIndexValueElemRef::decode_ref(value)?;
        current = Some(element);
        value = remaining;
    }
    Ok(current)
}

/// Removes older operations overwritten by the same distinct handle.
pub fn filter_overwritten_temp_index_values<B: AsRef<[u8]>>(
    mut values: Vec<TempIndexValueElem<B>>,
) -> Vec<TempIndexValueElem<B>> {
    if values.len() <= 1 || !values[0].distinct {
        return values;
    }
    let mut seen = HashSet::new();
    values.reverse();
    values.retain(|value| {
        seen.insert(match value.handle.as_ref() {
            Some(handle) => handle_identity(handle),
            None => {
                let mut identity = vec![0xff];
                identity.extend_from_slice(value.value.as_ref());
                identity
            }
        })
    });
    values.reverse();
    values
}

fn handle_identity(handle: &Handle) -> Vec<u8> {
    let mut identity = Vec::new();
    if let Some(partition_id) = handle_partition_id(handle) {
        identity.extend_from_slice(&partition_id.to_be_bytes());
    }
    identity.extend_from_slice(&handle.encoded());
    identity
}

/// Reports whether a temporary-index value ends in the untouched marker.
pub fn temp_index_value_is_untouched(value: &[u8]) -> bool {
    value.last() == Some(&UNCOMMITTED_INDEX_KV_FLAG)
}

#[cfg(test)]
mod source_tests {
    use super::*;
    use crate::table_key::is_untouched_index_kv;
    use tidb_charset::Collation;

    fn long_column(id: i64, offset: usize) -> TableColumn {
        TableColumn {
            id,
            offset,
            field_type: FieldType::new(FieldTypeCode::LongLong),
            primary_key: true,
            changing_field_type: None,
        }
    }

    fn varchar_column(id: i64, offset: usize) -> TableColumn {
        TableColumn {
            id,
            offset,
            field_type: FieldType::new(FieldTypeCode::Varchar)
                .with_collation(Collation::Utf8Mb4Bin),
            primary_key: false,
            changing_field_type: None,
        }
    }

    fn index_column(offset: usize) -> IndexColumn {
        IndexColumn {
            offset,
            length: UNSPECIFIED_LENGTH,
            use_changing_type: false,
        }
    }

    #[test]
    fn test_multi_column_common_handle() {
        // Direct port of
        // pkg/table/tables/index_test.go::TestMultiColumnCommonHandle. Both
        // unique and non-unique entries decode to the indexed value followed
        // by the two common-handle components.
        let table = TableInfo {
            columns: vec![
                long_column(1, 0),
                long_column(2, 1),
                varchar_column(3, 2),
                varchar_column(4, 3),
            ],
            indices: vec![IndexInfo {
                id: 1,
                columns: vec![index_column(0), index_column(1)],
                unique: true,
                global: false,
                global_index_version: 0,
                primary: true,
            }],
            pk_is_handle: false,
            is_common_handle: true,
            common_handle_version: 1,
        };
        let zone = SessionTimeZone::utc();
        let encoded_handle = Encoder::new(true)
            .encode_key_in_timezone(&zone, &[Datum::Int(3), Datum::Int(2)])
            .unwrap();
        let handle: Handle = CommonHandle::new(encoded_handle).unwrap().into();

        for (index_id, unique, offset, column_id) in [(10, true, 2, 3), (11, false, 3, 4)] {
            let index = IndexInfo {
                id: index_id,
                columns: vec![index_column(offset)],
                unique,
                global: false,
                global_index_version: 0,
                primary: false,
            };
            let mut indexed_values = vec![Datum::new_collation_string_datum(
                b"abc".to_vec(),
                Collation::Utf8Mb4Bin,
            )];
            let (key, distinct) = generate_index_key(
                Encoder::new(true),
                Some(&zone),
                &table,
                &index,
                42,
                &mut indexed_values,
                Some(&handle),
            )
            .unwrap();
            assert_eq!(distinct, unique);
            let value = generate_index_value(
                true,
                Some(&zone),
                &table,
                &index,
                true,
                distinct,
                false,
                &indexed_values,
                &handle,
                0,
                &[],
            )
            .unwrap();
            let columns = [
                ColumnInfo {
                    id: column_id,
                    is_pk_handle: false,
                    virtual_generated: false,
                    field_type: table.columns[offset].field_type.clone(),
                },
                ColumnInfo {
                    id: 1,
                    is_pk_handle: false,
                    virtual_generated: false,
                    field_type: table.columns[0].field_type.clone(),
                },
                ColumnInfo {
                    id: 2,
                    is_pk_handle: false,
                    virtual_generated: false,
                    field_type: table.columns[1].field_type.clone(),
                },
            ];

            let decoded =
                decode_index_kv(true, &key, &value, 1, HandleStatus::Default, &columns).unwrap();
            assert_eq!(decoded.len(), 3, "index {index_id}");
            let (_, indexed) = decode_one(&decoded[0]).unwrap();
            let (_, a) = decode_one(&decoded[1]).unwrap();
            let (_, b) = decode_one(&decoded[2]).unwrap();
            assert_eq!(indexed.as_raw_bytes().unwrap(), b"abc");
            assert_eq!(a, Datum::Int(3));
            assert_eq!(b, Datum::Int(2));

            let decoded_handle = decode_index_handle(&key, &value, 1).unwrap().unwrap();
            assert!(!decoded_handle.is_int());
            assert_eq!(
                decoded_handle.encoded(),
                handle.encoded(),
                "index {index_id}"
            );
        }
    }

    #[test]
    fn test_single_column_common_handle() {
        // Direct port of
        // pkg/table/tables/index_test.go::TestSingleColumnCommonHandle. The
        // common handle is recoverable from both unique and non-unique index
        // entries, including the untouched value form used by pessimistic
        // updates.
        let mut primary = varchar_column(1, 0);
        primary.primary_key = true;
        let value_column = |id, offset| TableColumn {
            id,
            offset,
            field_type: FieldType::new(FieldTypeCode::LongLong),
            primary_key: false,
            changing_field_type: None,
        };
        let table = TableInfo {
            columns: vec![primary, value_column(2, 1), value_column(3, 2)],
            indices: vec![IndexInfo {
                id: 1,
                columns: vec![index_column(0)],
                unique: true,
                global: false,
                global_index_version: 0,
                primary: true,
            }],
            pk_is_handle: false,
            is_common_handle: true,
            common_handle_version: 1,
        };
        let zone = SessionTimeZone::utc();
        let encoded_handle = Encoder::new(true)
            .encode_key_in_timezone(
                &zone,
                &[Datum::new_collation_string_datum(
                    b"abc".to_vec(),
                    Collation::Utf8Mb4Bin,
                )],
            )
            .unwrap();
        let handle: Handle = CommonHandle::new(encoded_handle).unwrap().into();

        for (index_id, unique, offset, column_id) in [(10, true, 1, 2), (11, false, 2, 3)] {
            let index = IndexInfo {
                id: index_id,
                columns: vec![index_column(offset)],
                unique,
                global: false,
                global_index_version: 0,
                primary: false,
            };
            let mut indexed_values = vec![Datum::Int(1)];
            let (key, distinct) = generate_index_key(
                Encoder::new(true),
                Some(&zone),
                &table,
                &index,
                42,
                &mut indexed_values,
                Some(&handle),
            )
            .unwrap();
            let value = generate_index_value(
                true,
                Some(&zone),
                &table,
                &index,
                false,
                distinct,
                false,
                &indexed_values,
                &handle,
                0,
                &[Datum::Int(0)],
            )
            .unwrap();
            let columns = [
                ColumnInfo {
                    id: column_id,
                    is_pk_handle: false,
                    virtual_generated: false,
                    field_type: table.columns[offset].field_type.clone(),
                },
                ColumnInfo {
                    id: 1,
                    is_pk_handle: false,
                    virtual_generated: false,
                    field_type: table.columns[0].field_type.clone(),
                },
            ];

            let assert_decodes = |value: &[u8]| {
                let decoded =
                    decode_index_kv(true, &key, value, 1, HandleStatus::Default, &columns).unwrap();
                assert_eq!(decoded.len(), 2, "index {index_id}");
                let (_, indexed) = decode_one(&decoded[0]).unwrap();
                let (_, primary) = decode_one(&decoded[1]).unwrap();
                assert_eq!(indexed, Datum::Int(1));
                assert_eq!(primary.as_raw_bytes().unwrap(), b"abc");
                let decoded_handle = decode_index_handle(&key, value, 1).unwrap().unwrap();
                assert!(!decoded_handle.is_int());
                assert_eq!(
                    decoded_handle.encoded(),
                    handle.encoded(),
                    "index {index_id}"
                );
            };
            assert_decodes(&value);

            let mut untouched = value.clone();
            untouched[0] = 1;
            untouched.push(UNCOMMITTED_INDEX_KV_FLAG);
            assert!(is_untouched_index_kv(&key, &untouched));
            assert_decodes(&untouched);
        }
    }

    #[test]
    fn test_gen_index_value_from_index() {
        // Direct port of
        // pkg/table/tables/index_test.go::TestGenIndexValueFromIndex. A
        // unique secondary-index KV generated for b=23 must render only the
        // indexed column and must not leak the integer row handle.
        let value_column = |id, offset, primary_key| TableColumn {
            id,
            offset,
            field_type: FieldType::new(FieldTypeCode::LongLong),
            primary_key,
            changing_field_type: None,
        };
        let index = IndexInfo {
            id: 10,
            columns: vec![index_column(1)],
            unique: true,
            global: false,
            global_index_version: 0,
            primary: false,
        };
        let table = TableInfo {
            columns: vec![
                value_column(1, 0, true),
                value_column(2, 1, false),
                TableColumn {
                    id: 3,
                    offset: 2,
                    field_type: FieldType::new(FieldTypeCode::Blob)
                        .with_collation(Collation::Utf8Mb4Bin),
                    primary_key: false,
                    changing_field_type: None,
                },
            ],
            indices: vec![index.clone()],
            pk_is_handle: true,
            is_common_handle: false,
            common_handle_version: 0,
        };
        let handle: Handle = IntHandle::new(1).into();
        let mut indexed_values = vec![Datum::Int(23)];
        let (key, distinct) = generate_index_key(
            Encoder::new(true),
            None,
            &table,
            &index,
            42,
            &mut indexed_values,
            Some(&handle),
        )
        .unwrap();
        assert!(distinct);
        let value = generate_index_value(
            true,
            None,
            &table,
            &index,
            false,
            distinct,
            false,
            &indexed_values,
            &handle,
            0,
            &[],
        )
        .unwrap();

        assert_eq!(
            table
                .generate_index_values_from_index(true, None, &index, &key, &value)
                .unwrap(),
            vec![b"23".to_vec()]
        );
    }

    #[test]
    fn test_gen_index_value_with_large_padding_size() {
        // Direct port of
        // pkg/table/tables/index_test.go::TestGenIndexValueWithLargePaddingSize
        // (issue #47115). Restored-data padding must preserve a count of 128,
        // while the two-column common handle remains recoverable.
        let table = TableInfo {
            columns: vec![long_column(1, 0), long_column(2, 1), varchar_column(3, 2)],
            indices: vec![IndexInfo {
                id: 1,
                columns: vec![index_column(0), index_column(1)],
                unique: true,
                global: false,
                global_index_version: 0,
                primary: true,
            }],
            pk_is_handle: false,
            is_common_handle: true,
            common_handle_version: 1,
        };
        let index = IndexInfo {
            id: 10,
            columns: vec![index_column(2)],
            unique: false,
            global: false,
            global_index_version: 0,
            primary: false,
        };
        let zone = SessionTimeZone::utc();
        let encoded_handle = Encoder::new(true)
            .encode_key_in_timezone(&zone, &[Datum::Int(1), Datum::Int(2)])
            .unwrap();
        let handle: Handle = CommonHandle::new(encoded_handle).unwrap().into();
        let mut padded = b"abc".to_vec();
        padded.resize(padded.len() + 128, b' ');
        let mut indexed_values = vec![Datum::new_collation_string_datum(
            padded.clone(),
            Collation::Utf8Mb4Bin,
        )];
        let (key, distinct) = generate_index_key(
            Encoder::new(true),
            Some(&zone),
            &table,
            &index,
            42,
            &mut indexed_values,
            Some(&handle),
        )
        .unwrap();
        assert!(!distinct);
        let value = generate_index_value(
            true,
            Some(&zone),
            &table,
            &index,
            true,
            distinct,
            false,
            &indexed_values,
            &handle,
            0,
            &[],
        )
        .unwrap();
        let columns = [
            ColumnInfo {
                id: 3,
                is_pk_handle: false,
                virtual_generated: false,
                field_type: table.columns[2].field_type.clone(),
            },
            ColumnInfo {
                id: 1,
                is_pk_handle: false,
                virtual_generated: false,
                field_type: table.columns[0].field_type.clone(),
            },
            ColumnInfo {
                id: 2,
                is_pk_handle: false,
                virtual_generated: false,
                field_type: table.columns[1].field_type.clone(),
            },
        ];

        let decoded =
            decode_index_kv(true, &key, &value, 1, HandleStatus::Default, &columns).unwrap();
        assert_eq!(decoded.len(), 3);
        let (_, indexed) = decode_one(&decoded[0]).unwrap();
        let (_, a) = decode_one(&decoded[1]).unwrap();
        let (_, b) = decode_one(&decoded[2]).unwrap();
        assert_eq!(indexed.as_raw_bytes().unwrap(), padded);
        assert_eq!(a, Datum::Int(1));
        assert_eq!(b, Datum::Int(2));
        assert_eq!(
            decode_index_handle(&key, &value, 1)
                .unwrap()
                .unwrap()
                .encoded(),
            handle.encoded()
        );
    }

    /// Go decodes a non-unique clustered-index key suffix via
    /// `kv.NewCommonHandle(keySuffix)` (tablecodec.go:1975), so a
    /// single-int-column common handle must stay a common handle and never
    /// collapse to an int handle in the V1 path. Restored data on the first
    /// indexed column forces the V1 split path (mirroring the large-padding
    /// test's setup, but with a single-int primary key).
    #[test]
    fn test_v1_non_unique_single_int_column_common_handle() {
        let mut padded = b"abc".to_vec();
        padded.resize(padded.len() + 128, b' ');
        let table = TableInfo {
            columns: vec![
                long_column(1, 0),
                long_column(2, 1),
                varchar_column(3, 2),
                long_column(4, 3),
            ],
            indices: vec![IndexInfo {
                id: 1,
                columns: vec![index_column(0)],
                unique: true,
                global: false,
                global_index_version: 0,
                primary: true,
            }],
            pk_is_handle: false,
            is_common_handle: true,
            common_handle_version: 1,
        };
        let index = IndexInfo {
            id: 10,
            columns: vec![index_column(2)],
            unique: false,
            global: false,
            global_index_version: 0,
            primary: false,
        };
        let encoded_handle = Encoder::new(true)
            .encode_key_in_timezone(&SessionTimeZone::utc(), &[Datum::Int(42)])
            .unwrap();
        let handle: Handle = CommonHandle::new(encoded_handle).unwrap().into();
        let indexed_values = vec![Datum::new_collation_string_datum(
            padded.clone(),
            Collation::Utf8Mb4Bin,
        )];
        let (key, distinct) = generate_index_key(
            Encoder::new(true),
            None,
            &table,
            &index,
            42,
            &mut indexed_values.clone(),
            Some(&handle),
        )
        .unwrap();
        assert!(!distinct);
        let value = generate_index_value(
            true,
            None,
            &table,
            &index,
            true,
            distinct,
            false,
            &indexed_values,
            &handle,
            0,
            &[],
        )
        .unwrap();
        let columns = [
            ColumnInfo {
                id: 3,
                is_pk_handle: false,
                virtual_generated: false,
                field_type: table.columns[2].field_type.clone(),
            },
            ColumnInfo {
                id: 1,
                is_pk_handle: false,
                virtual_generated: false,
                field_type: table.columns[0].field_type.clone(),
            },
        ];

        let decoded =
            decode_index_kv(true, &key, &value, 1, HandleStatus::Default, &columns).unwrap();
        assert_eq!(decoded.len(), 2);
        let (_, indexed) = decode_one(&decoded[0]).unwrap();
        let (_, handle_value) = decode_one(&decoded[1]).unwrap();
        assert_eq!(indexed.as_raw_bytes().unwrap(), padded);
        assert_eq!(handle_value, Datum::Int(42));
    }
}
