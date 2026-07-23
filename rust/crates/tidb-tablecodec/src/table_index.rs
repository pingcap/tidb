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
use std::fmt;

use chrono_tz::Tz;
use tidb_datatype::{
    is_bin_collation, Charset, Collation, Datum, FieldType, FieldTypeCode, UNSPECIFIED_LENGTH,
};
use tidb_txnkv::{CommonHandle, Handle, IntHandle, PartitionHandle};

use crate::table_row::{decode_column_value, TableRowError};
use tidb_codec::table_key::{
    encode_index_seek_key, encode_table_index_prefix, gen_table_index_prefix, RECORD_ROW_KEY_LEN,
};
use tidb_codec::{
    cut_one, decode_int, decode_one, decode_row_to_old_bytes, encode_int, encode_row, encode_value,
    is_new_format, CodecError, ColumnInfo, Encoder, RowPackageError, INT_FLAG, ROW_CODEC_VERSION,
    UINT_FLAG,
};

const ID_LEN: usize = 8;
const PREFIX_LEN: usize = 1 + ID_LEN + 2;
const INDEX_VALUES_OFFSET: usize = PREFIX_LEN + ID_LEN;

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
    PartitionHandle::new(partition_id, handle).into()
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

/// Maximum byte length of legacy index values.
pub const MAX_OLD_ENCODE_VALUE_LEN: usize = 9;
/// Marker for a common handle in an extensible index value.
pub const COMMON_HANDLE_FLAG: u8 = 127;
/// Marker for a global-index partition ID.
pub const PARTITION_ID_FLAG: u8 = 126;
/// Marker for an explicit clustered-index value version.
pub const INDEX_VERSION_FLAG: u8 = 125;
/// Marker for rowcodec restored data.
pub const RESTORE_DATA_FLAG: u8 = ROW_CODEC_VERSION;
/// Marker appended to an untouched index value.
pub const UNCOMMITTED_INDEX_KV_FLAG: u8 = b'1';
/// High bits used for temporary index IDs.
pub const TEMP_INDEX_PREFIX: i64 = 0x7fff_0000_0000_0000;
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
    /// Mirrors `TableInfo.HasClusteredIndex`.
    #[must_use]
    pub const fn has_clustered_index(&self) -> bool {
        self.pk_is_handle || self.is_common_handle
    }
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
#[must_use]
pub fn get_table_index_key_range(table_id: i64, index_id: i64) -> (Vec<u8>, Vec<u8>) {
    (
        encode_index_seek_key(table_id, index_id, &[]),
        encode_index_seek_key(table_id, index_id, &[u8::MAX]),
    )
}

/// Cuts `length` encoded index columns and returns the remaining handle suffix.
pub fn cut_index_key(key: &[u8], length: usize) -> Result<(Vec<Vec<u8>>, &[u8]), TableIndexError> {
    let mut remaining = key
        .get(INDEX_VALUES_OFFSET..)
        .ok_or(TableIndexError::Invalid("invalid index key"))?;
    let mut values = Vec::with_capacity(length);
    for _ in 0..length {
        let (value, tail) = cut_one(remaining)?;
        values.push(value.to_vec());
        remaining = tail;
    }
    Ok((values, remaining))
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
#[must_use]
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

/// Reports whether the key contains a temporary index ID.
#[must_use]
pub fn is_temp_index_key(key: &[u8]) -> bool {
    key.get(PREFIX_LEN..PREFIX_LEN + ID_LEN)
        .and_then(|bytes| bytes.try_into().ok())
        .map(|bytes: [u8; 8]| {
            let index_id = tidb_codec::decode_cmp_uint_to_int(u64::from_be_bytes(bytes));
            TEMP_INDEX_PREFIX | index_id == index_id
        })
        .unwrap_or(false)
}

/// Reports whether a key has the complete record-key prefix shape.
#[must_use]
pub fn is_record_key(key: &[u8]) -> bool {
    key.len() > PREFIX_LEN && key.first() == Some(&b't') && key.get(10) == Some(&b'r')
}

/// Reports whether a key has the complete index-key prefix shape.
#[must_use]
pub fn is_index_key(key: &[u8]) -> bool {
    key.len() > PREFIX_LEN && key.first() == Some(&b't') && key.get(10) == Some(&b'i')
}

/// Reports whether a key is exactly `t{table_id}`.
#[must_use]
pub fn is_table_key(key: &[u8]) -> bool {
    key.len() == 9 && key.first() == Some(&b't')
}

/// Reports whether an index key/value pair represents an untouched write.
#[must_use]
pub fn is_untouched_index_kv(key: &[u8], value: &[u8]) -> bool {
    if !is_index_key(key) {
        return false;
    }
    let Some(last) = value.last() else {
        return false;
    };
    if is_temp_index_key(key) {
        return *last == UNCOMMITTED_INDEX_KV_FLAG;
    }
    if value.len() <= MAX_OLD_ENCODE_VALUE_LEN {
        return matches!(value.len(), 1 | 4 | 9) && *last == UNCOMMITTED_INDEX_KV_FLAG;
    }
    let tail_len = usize::from(value[0]);
    if tail_len < 8 {
        tail_len >= 1 && *last == UNCOMMITTED_INDEX_KV_FLAG
    } else {
        tail_len == 9
    }
}

/// Encodes an integer or common handle in a unique-index value.
#[must_use]
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
    let bytes = data
        .get(..ID_LEN)
        .ok_or(TableIndexError::Invalid("short integer index handle"))?;
    Ok(IntHandle::new(i64::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| TableIndexError::Invalid("short integer index handle"))?,
    ))
    .into())
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
    let (remaining, datum) = decode_one(suffix)?;
    if remaining.is_empty() {
        if let Some(value) = datum.as_int() {
            return Ok(IntHandle::new(value).into());
        }
    }
    Ok(common_handle(suffix.to_vec())?)
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
        let table_id = tidb_codec::decode_table_id(&first.start_key);
        if table_id <= 0 {
            return Err(TableIndexError::Invalid(
                "Incorrect keyRange is constrcuted",
            ));
        }
        for range in ranges.iter().skip(1) {
            let next_table_id = tidb_codec::decode_table_id(&range.start_key);
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
#[must_use]
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
    if index_value_version(value) == 0 {
        if value.len() <= MAX_OLD_ENCODE_VALUE_LEN {
            return Ok(IndexValueSegments {
                int_handle: Some(value.to_vec()),
                ..IndexValueSegments::default()
            });
        }
        split_extensible_index_value(value, 1)
    } else {
        split_extensible_index_value(value, 3)
    }
}

fn split_extensible_index_value(
    value: &[u8],
    header_len: usize,
) -> Result<IndexValueSegments, TableIndexError> {
    let tail_len = usize::from(
        *value
            .first()
            .ok_or(TableIndexError::Invalid("empty index value"))?,
    );
    if value.len() < header_len + tail_len {
        return Err(TableIndexError::Invalid("invalid index-value tail"));
    }
    let tail = &value[value.len() - tail_len..];
    let mut options = &value[header_len..value.len() - tail_len];
    let mut segments = IndexValueSegments::default();
    if tail.len() >= ID_LEN {
        segments.int_handle = Some(tail[..ID_LEN].to_vec());
    }
    if options.first() == Some(&COMMON_HANDLE_FLAG) {
        let length_bytes = options
            .get(1..3)
            .ok_or(TableIndexError::Invalid("short common-handle length"))?;
        let length =
            usize::from(u16::from_be_bytes(length_bytes.try_into().map_err(
                |_| TableIndexError::Invalid("short common-handle length"),
            )?));
        let end = 3 + length;
        segments.common_handle = Some(
            options
                .get(3..end)
                .ok_or(TableIndexError::Invalid("short common handle"))?
                .to_vec(),
        );
        options = &options[end..];
    }
    if options.first() == Some(&PARTITION_ID_FLAG) {
        segments.partition_id = Some(
            options
                .get(1..9)
                .ok_or(TableIndexError::Invalid("short partition ID"))?
                .to_vec(),
        );
        options = &options[9..];
    }
    if options.first() == Some(&RESTORE_DATA_FLAG) {
        segments.restored_values = Some(options.to_vec());
    }
    Ok(segments)
}

/// Decodes a unique-index value handle, including partition wrapping.
pub fn decode_handle_in_index_value(value: &[u8]) -> Result<Handle, TableIndexError> {
    if value.len() <= MAX_OLD_ENCODE_VALUE_LEN {
        return decode_int_handle_in_index_value(value);
    }
    let segments = split_index_value(value)?;
    let mut handle = if let Some(encoded) = segments.int_handle {
        decode_int_handle_in_index_value(&encoded)?
    } else if let Some(encoded) = segments.common_handle {
        common_handle(encoded)?
    } else {
        return Err(TableIndexError::Invalid("index value has no handle"));
    };
    if let Some(encoded) = segments.partition_id {
        let (_, partition_id) = decode_int(&encoded)?;
        handle = partition_handle(partition_id, handle);
    }
    Ok(handle)
}

/// Decodes a handle from the index key/value pair.
pub fn decode_index_handle(
    key: &[u8],
    value: &[u8],
    columns_len: usize,
) -> Result<Handle, TableIndexError> {
    let (_, suffix) = cut_index_key(key, columns_len)?;
    if !suffix.is_empty() {
        let mut handle = decode_handle_in_index_key(suffix)?;
        if value.len() >= ID_LEN {
            if let Some(encoded) = split_index_value(value)?.partition_id {
                let (_, partition_id) = decode_int(&encoded)?;
                handle = partition_handle(partition_id, inner_handle(&handle).clone());
            }
        }
        return Ok(handle);
    }
    if value.len() >= ID_LEN {
        return decode_handle_in_index_value(value);
    }
    Err(TableIndexError::Invalid("no handle in index key or value"))
}

/// Reports whether an index value carries a unique-index handle.
#[must_use]
pub fn index_kv_is_unique(value: &[u8]) -> bool {
    if value.len() <= MAX_OLD_ENCODE_VALUE_LEN {
        return value.len() == ID_LEN;
    }
    split_index_value(value).is_ok_and(|segments| {
        if index_value_version(value) == 1 {
            segments.common_handle.is_some()
        } else {
            segments.int_handle.is_some() || segments.common_handle.is_some()
        }
    })
}

/// Truncates one indexed value according to its index prefix length.
pub fn truncate_index_value(
    value: &mut Datum,
    index_column: &IndexColumn,
    table_column: &TableColumn,
) -> Result<(), TableIndexError> {
    if index_column.length == UNSPECIFIED_LENGTH {
        return Ok(());
    }
    let Some(bytes) = value.as_raw_bytes() else {
        return Ok(());
    };
    let length = usize::try_from(index_column.length)
        .map_err(|_| TableIndexError::Metadata("negative index prefix length"))?;
    let truncated = match table_column.field_type.collation().charset() {
        Charset::Binary | Charset::Ascii => bytes.get(..length.min(bytes.len())).unwrap().to_vec(),
        _ => {
            let text = String::from_utf8_lossy(bytes);
            if text.chars().count() <= length {
                bytes.to_vec()
            } else {
                text.chars().take(length).collect::<String>().into_bytes()
            }
        }
    };
    if truncated.as_slice() != bytes {
        match value {
            Datum::Bytes(_) => *value = Datum::new_bytes(truncated),
            Datum::String(_) => {
                *value =
                    Datum::new_collation_string(truncated, table_column.field_type.collation());
            }
            _ => {}
        }
    }
    Ok(())
}

/// Truncates every indexed value using table/index metadata.
pub fn truncate_index_values(
    table: &TableInfo,
    index: &IndexInfo,
    values: &mut [Datum],
) -> Result<(), TableIndexError> {
    if values.len() != index.columns.len() {
        return Err(TableIndexError::Metadata(
            "index values and columns count mismatch",
        ));
    }
    for (value, index_column) in values.iter_mut().zip(&index.columns) {
        let table_column = table
            .columns
            .get(index_column.offset)
            .ok_or(TableIndexError::Metadata("invalid index column offset"))?;
        truncate_index_value(value, index_column, table_column)?;
    }
    Ok(())
}

/// Generates a complete index key and whether its values are distinct.
pub fn generate_index_key(
    encoder: Encoder,
    timezone: Option<&Tz>,
    table: &TableInfo,
    index: &IndexInfo,
    physical_table_id: i64,
    indexed_values: &mut [Datum],
    handle: Option<&Handle>,
) -> Result<(Vec<u8>, bool), TableIndexError> {
    let distinct = index.unique && indexed_values.iter().all(|value| !value.is_null());
    truncate_index_values(table, index, indexed_values)?;
    let encoded = match timezone {
        Some(timezone) => encoder.encode_key_in_timezone(timezone, indexed_values)?,
        None => encoder.encode_key(indexed_values)?,
    };
    let mut key = encode_index_seek_key(physical_table_id, index.id, &encoded);
    if !distinct {
        if let Some(handle) = handle {
            if index.global_index_version >= 1 {
                if table.has_clustered_index() {
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
    }
    Ok((key, distinct))
}

/// Generates the selected legacy, extensible V0, or common-handle V1 index
/// value.
#[allow(clippy::too_many_arguments)]
pub fn generate_index_value(
    use_new_collation: bool,
    timezone: Option<&Tz>,
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
    if table.is_common_handle && table.common_handle_version == 1 {
        generate_index_value_v1(
            use_new_collation,
            timezone,
            table,
            index,
            need_restored_data,
            distinct,
            untouched,
            indexed_values,
            handle,
            partition_id,
            handle_restored_data,
        )
    } else {
        generate_index_value_v0(
            timezone,
            table,
            index,
            need_restored_data,
            distinct,
            untouched,
            indexed_values,
            handle,
            partition_id,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn generate_index_value_v0(
    timezone: Option<&Tz>,
    table: &TableInfo,
    index: &IndexInfo,
    need_restored_data: bool,
    distinct: bool,
    untouched: bool,
    indexed_values: &[Datum],
    handle: &Handle,
    partition_id: i64,
) -> Result<Vec<u8>, TableIndexError> {
    let mut value = vec![0];
    let mut extensible = false;
    let mut tail_len = 0;
    if !handle.is_int() && distinct {
        encode_common_handle(&mut value, &handle.encoded());
        extensible = true;
    }
    if index.global {
        encode_partition_id(&mut value, partition_id);
        extensible = true;
    }
    if need_restored_data {
        let ids = index
            .columns
            .iter()
            .map(|column| {
                table
                    .columns
                    .get(column.offset)
                    .map(|column| column.id)
                    .ok_or(TableIndexError::Metadata("invalid index column offset"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        encode_row(timezone, &ids, indexed_values, &mut value)?;
        extensible = true;
    }
    if extensible {
        if handle.is_int() && distinct {
            tail_len += ID_LEN;
            value.extend_from_slice(&encode_handle_in_unique_index_value(handle, false));
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
        return Ok(value);
    }
    value.clear();
    if distinct {
        value.extend_from_slice(&encode_handle_in_unique_index_value(handle, untouched));
    }
    if untouched {
        value.push(UNCOMMITTED_INDEX_KV_FLAG);
    }
    if value.is_empty() {
        value.push(b'0');
    }
    Ok(value)
}

#[allow(clippy::too_many_arguments)]
fn generate_index_value_v1(
    use_new_collation: bool,
    timezone: Option<&Tz>,
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
    let mut value = vec![0, INDEX_VERSION_FLAG, 1];
    if distinct {
        encode_common_handle(&mut value, &handle.encoded());
    }
    if index.global {
        encode_partition_id(&mut value, partition_id);
    }
    if need_restored_data || !handle_restored_data.is_empty() {
        let mut ids = Vec::new();
        let mut data = Vec::new();
        for (position, index_column) in index.columns.iter().enumerate() {
            let column = table
                .columns
                .get(index_column.offset)
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
                    .ok_or(TableIndexError::Metadata("missing indexed value"))?;
                if is_bin_collation(field_type.collation().name()) {
                    let spaces = indexed
                        .as_raw_bytes()
                        .map(|bytes| bytes.iter().rev().take_while(|byte| **byte == b' ').count())
                        .unwrap_or(0);
                    data.push(Datum::UInt(spaces as u64));
                } else {
                    data.push(indexed.clone());
                }
            }
        }
        if !handle_restored_data.is_empty() {
            ids.extend(common_pk_restored_column_ids(use_new_collation, table));
            data.extend_from_slice(handle_restored_data);
        }
        encode_row(timezone, &ids, &data, &mut value)?;
    }
    if untouched {
        value[0] = 1;
        value.push(UNCOMMITTED_INDEX_KV_FLAG);
    }
    Ok(value)
}

/// Returns common-primary-key column IDs requiring restored data.
#[must_use]
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

fn reencode_handle(handle: &Handle, unsigned: bool) -> Result<Vec<Vec<u8>>, TableIndexError> {
    match inner_handle(handle) {
        Handle::Int(value) => Ok(vec![encode_value(&[if unsigned {
            Datum::UInt(value.value() as u64)
        } else {
            Datum::Int(value.value())
        }])?]),
        Handle::Common(_) => encoded_handle_columns(handle),
        Handle::Partition(_) => unreachable!("inner_handle removes partition wrappers"),
    }
}

fn decode_restored_values(
    columns: &[ColumnInfo],
    restored: &[u8],
) -> Result<Vec<Vec<u8>>, TableIndexError> {
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
            field_type: if is_bin_collation(column.field_type.collation().name()) {
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
    mut results: Vec<Vec<u8>>,
    restored: &[u8],
) -> Result<Vec<Vec<u8>>, TableIndexError> {
    let restore_columns = restored_columns(use_new_collation, columns);
    let offsets = columns
        .iter()
        .enumerate()
        .map(|(index, column)| (column.id, index))
        .collect::<BTreeMap<_, _>>();
    let restored_values =
        decode_row_to_old_bytes(restored, &restore_columns, &offsets, &[], None, None)?;
    for (index, restored_value) in restored_values.into_iter().enumerate() {
        if restored_value.is_empty() {
            continue;
        }
        if is_bin_collation(columns[index].field_type.collation().name()) {
            let original = decode_column_value(&results[index], &columns[index].field_type, None)?;
            let count_type = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);
            let padding = decode_column_value(&restored_value, &count_type, None)?
                .as_uint()
                .ok_or(TableIndexError::Invalid("invalid restored padding"))?
                as usize;
            if padding == 0 {
                continue;
            }
            let mut bytes = original
                .into_raw_bytes()
                .ok_or(TableIndexError::Invalid("invalid restored string"))?;
            bytes.resize(bytes.len() + padding, b' ');
            results[index] = tidb_codec::encode_value(&[Datum::new_collation_string(
                bytes,
                columns[index].field_type.collation(),
            )])?;
        } else {
            results[index] = restored_value;
        }
    }
    Ok(results)
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
    let (mut results, suffix) = cut_index_key(key, columns_len)?;
    if value.len() <= MAX_OLD_ENCODE_VALUE_LEN {
        if handle_status == HandleStatus::NotNeeded {
            return Ok(results);
        }
        let handle = if suffix.is_empty() {
            decode_int_handle_in_index_value(value)?
        } else {
            decode_handle_in_index_key(suffix)?
        };
        results.extend(reencode_handle(
            &handle,
            handle_status == HandleStatus::Unsigned,
        )?);
        return Ok(results);
    }

    let segments = split_index_value(value)?;
    if let Some(restored) = segments.restored_values.as_deref() {
        if index_value_version(value) == 1 {
            results = decode_restored_values_v5(
                use_new_collation,
                &columns[..columns_len],
                results,
                restored,
            )?;
        } else {
            results = decode_restored_values(&columns[..columns_len], restored)?;
        }
    }
    if handle_status == HandleStatus::NotNeeded {
        return Ok(results);
    }
    let handle = if let Some(encoded) = segments.int_handle.as_deref() {
        decode_int_handle_in_index_value(encoded)?
    } else if let Some(encoded) = segments.common_handle {
        common_handle(encoded)?
    } else {
        decode_handle_in_index_key(suffix)?
    };
    if index_value_version(value) == 1 && !handle.is_int() {
        let handle_columns = columns.get(columns_len..).unwrap_or_default();
        let restored = segments.restored_values.as_deref().unwrap_or_default();
        let mut encoded = encoded_handle_columns(&handle)?;
        if !restored.is_empty() {
            let mut relevant_len = handle_columns.len();
            while relevant_len > 0 && handle_columns[relevant_len - 1].id < 0 {
                relevant_len -= 1;
            }
            encoded = decode_restored_values_v5(
                use_new_collation,
                &handle_columns[..relevant_len],
                encoded,
                restored,
            )?;
        }
        results.extend(encoded);
    } else {
        results.extend(reencode_handle(
            &handle,
            handle_status == HandleStatus::Unsigned,
        )?);
    }
    if let Some(encoded) = segments.partition_id {
        let (_, partition_id) = decode_int(&encoded)?;
        results.push(encode_value(&[Datum::Int(partition_id)])?);
    }
    Ok(results)
}

/// Allocation-reuse entry point corresponding to Go `DecodeIndexKVEx`.
pub fn decode_index_kv_into(
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
pub struct TempIndexValueElem {
    /// Encoded ordinary index value.
    pub value: Vec<u8>,
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
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Returns the latest operation.
    #[must_use]
    pub fn current(&self) -> Option<&TempIndexValueElem> {
        self.elements.last()
    }

    /// Removes older operations overwritten by the same distinct handle.
    #[must_use]
    pub fn filter_overwritten(self) -> Self {
        Self {
            elements: filter_overwritten_temp_index_values(self.elements),
        }
    }
}

impl TempIndexValueElem {
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
                output.push(TempIndexValueFlag::Normal as u8);
                output.extend_from_slice(
                    &u16::try_from(self.value.len())
                        .map_err(|_| TableIndexError::Invalid("temporary value too long"))?
                        .to_be_bytes(),
                );
                output.extend_from_slice(&self.value);
            }
            (false, false) => {
                output.push(TempIndexValueFlag::NonDistinctNormal as u8);
                output.extend_from_slice(&self.value);
            }
        }
        output.push(self.key_version);
        Ok(())
    }

    /// Decodes one history element and returns the remaining bytes.
    pub fn decode(input: &[u8]) -> Result<(Self, &[u8]), TableIndexError> {
        let (&flag, mut input) = input
            .split_first()
            .ok_or(TableIndexError::Invalid("empty temporary index value"))?;
        let mut element = Self {
            value: Vec::new(),
            handle: None,
            key_version: 0,
            delete: false,
            distinct: false,
            global: false,
        };
        match flag {
            value if value == TempIndexValueFlag::Normal as u8 => {
                let length = read_u16(&mut input)?;
                element.value = take(&mut input, length)?.to_vec();
                element.key_version = take(&mut input, 1)?[0];
                element.distinct = true;
            }
            value if value == TempIndexValueFlag::NonDistinctNormal as u8 => {
                let (&key_version, value) = input
                    .split_last()
                    .ok_or(TableIndexError::Invalid("short temporary index value"))?;
                element.value = value.to_vec();
                element.key_version = key_version;
                input = &[];
            }
            value if value == TempIndexValueFlag::Deleted as u8 => {
                let length = read_u16(&mut input)?;
                let encoded = take(&mut input, length)?;
                element.handle = Some(if length == ID_LEN {
                    decode_int_handle_in_index_value(encoded)?
                } else {
                    common_handle(encoded.to_vec())?
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

/// Removes older operations overwritten by the same distinct handle.
#[must_use]
pub fn filter_overwritten_temp_index_values(
    mut values: Vec<TempIndexValueElem>,
) -> Vec<TempIndexValueElem> {
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
                identity.extend_from_slice(&value.value);
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
#[must_use]
pub fn temp_index_value_is_untouched(value: &[u8]) -> bool {
    value.last() == Some(&UNCOMMITTED_INDEX_KV_FLAG)
}

/// Produces the canonical single-byte non-unique local index value.
#[must_use]
pub fn legacy_non_unique_index_value() -> Vec<u8> {
    vec![b'0']
}

/// Returns the canonical table-index prefix, retained here so all tablecodec
/// operations have one import surface.
#[must_use]
pub fn table_index_prefix(table_id: i64) -> Vec<u8> {
    gen_table_index_prefix(table_id)
}

/// Returns the canonical table-index key, retained here so all tablecodec
/// operations have one import surface.
#[must_use]
pub fn table_index_key(table_id: i64, index_id: i64) -> Vec<u8> {
    encode_table_index_prefix(table_id, index_id)
}

/// Returns true when bytes are a rowcodec restored-data segment.
#[must_use]
pub fn is_restored_data(value: &[u8]) -> bool {
    is_new_format(value)
}

/// Returns the field type used for restored binary-collation padding counts.
#[must_use]
pub fn restored_padding_field_type() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
        .with_unsigned(true)
        .with_collation(Collation::Binary)
}

/// Exposes the source integer-handle datum flag used in index-key suffixes.
pub const INDEX_INT_HANDLE_FLAG: u8 = INT_FLAG;
/// Exposes the source unsigned datum flag used by restored padding.
pub const INDEX_UINT_FLAG: u8 = UINT_FLAG;
/// Encoded table record key width used by tablecodec allocation sizing.
pub const TABLE_RECORD_ROW_KEY_LEN: usize = RECORD_ROW_KEY_LEN;
