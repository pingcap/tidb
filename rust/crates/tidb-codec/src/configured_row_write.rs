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

//! Persisted bytes for one bounded signed-`BIGINT` clustered-handle row.
//!
//! This composes the existing record-key and new-format row primitives into
//! the exact pair TiKV stores for a table whose primary key is one signed
//! integer handle. Go's `tables.CanSkip` (`pkg/table/tables/tables.go`) skips
//! `col.IsPKHandleColumn`, so the handle is carried only by the record key and
//! must never appear in the value; returning both halves from one call keeps
//! that invariant structural instead of a rule every caller has to remember.

use std::fmt;

use crate::{
    decode_raw_int, encode_raw_int, encode_raw_row,
    table_key::{encode_row_key_with_handle, RecordHandle},
    RawRowColumn, RawRowValue, RowDecodeError, RowDecoder, RowEncodeError,
};

/// One stored signed-`BIGINT` column of a configured row.
///
/// The clustered handle column is not representable here on purpose: it lives
/// in the record key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfiguredRowColumn {
    /// Stable TiDB column identity.
    pub id: i64,
    /// Signed value persisted in the row's opaque payload.
    pub value: i64,
}

impl ConfiguredRowColumn {
    /// Builds one stored column entry.
    #[must_use]
    pub const fn new(id: i64, value: i64) -> Self {
        Self { id, value }
    }
}

/// One stored column value: either a signed integer or raw string bytes.
///
/// A `CHAR(N)` at the default `utf8mb4_bin` collation stores its raw value
/// bytes with no restored-collation data (Go's `NeedRestoredDataWithCollate` is
/// false for that case), so `Bytes` carries exactly those bytes. An empty
/// string is `Bytes(vec![])` — present with zero length, distinct from SQL
/// `NULL` and from a missing column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfiguredValue {
    /// A signed integer stored in rowcodec's compact width.
    Int(i64),
    /// Raw stored bytes, used by no-restored-data string columns.
    Bytes(Vec<u8>),
}

impl ConfiguredValue {
    /// Builds this value's opaque row payload.
    fn to_payload(&self) -> Vec<u8> {
        match self {
            Self::Int(value) => {
                let mut payload = Vec::with_capacity(8);
                encode_raw_int(&mut payload, *value);
                payload
            }
            Self::Bytes(bytes) => bytes.clone(),
        }
    }
}

/// Why a configured row cannot be encoded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfiguredRowWriteError {
    /// A configured row persists at least one stored column.
    NoStoredColumns,
    /// Row format v2 addresses column IDs with unsigned 32-bit metadata.
    ColumnIdOutOfRange(i64),
    /// One row holds exactly one entry per stored column ID.
    DuplicateColumnId(i64),
    /// The shared new-row encoder rejected the assembled payload.
    Encode(RowEncodeError),
}

impl fmt::Display for ConfiguredRowWriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoStoredColumns => {
                formatter.write_str("configured row requires at least one stored column")
            }
            Self::ColumnIdOutOfRange(id) => write!(
                formatter,
                "configured column ID {id} is outside the row-format column ID domain"
            ),
            Self::DuplicateColumnId(id) => {
                write!(formatter, "configured row repeats column ID {id}")
            }
            Self::Encode(error) => write!(formatter, "configured row encoding failed: {error}"),
        }
    }
}

impl std::error::Error for ConfiguredRowWriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Encode(error) => Some(error),
            Self::NoStoredColumns | Self::ColumnIdOutOfRange(_) | Self::DuplicateColumnId(_) => {
                None
            }
        }
    }
}

impl From<RowEncodeError> for ConfiguredRowWriteError {
    fn from(error: RowEncodeError) -> Self {
        Self::Encode(error)
    }
}

/// Why a stored signed-`BIGINT` column cannot be read back from a row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfiguredRowReadError {
    /// The persisted row framing or payload is malformed.
    Decode(RowDecodeError),
    /// The row carries no entry for this configured column.
    MissingColumn(i64),
    /// The row explicitly stores SQL `NULL` for a `NOT NULL` configured column.
    NullColumn(i64),
    /// Trailing bytes remain after one complete row.
    TrailingBytes(usize),
}

impl fmt::Display for ConfiguredRowReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(error) => write!(formatter, "configured row decoding failed: {error}"),
            Self::MissingColumn(id) => {
                write!(formatter, "configured row is missing column ID {id}")
            }
            Self::NullColumn(id) => write!(
                formatter,
                "configured NOT NULL column ID {id} is stored as NULL"
            ),
            Self::TrailingBytes(length) => write!(
                formatter,
                "configured row value has {length} trailing bytes after one row"
            ),
        }
    }
}

impl std::error::Error for ConfiguredRowReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Decode(error) => Some(error),
            Self::MissingColumn(_) | Self::NullColumn(_) | Self::TrailingBytes(_) => None,
        }
    }
}

impl From<RowDecodeError> for ConfiguredRowReadError {
    fn from(error: RowDecodeError) -> Self {
        Self::Decode(error)
    }
}

/// Encodes the record key and new-format value for one configured row.
///
/// The returned key is `t{table_id}_r{encoded handle}` and the value carries
/// only the stored columns, exactly as Go writes a clustered signed-handle row.
pub fn encode_configured_row(
    table_id: i64,
    handle: i64,
    columns: &[ConfiguredRowColumn],
) -> Result<(Vec<u8>, Vec<u8>), ConfiguredRowWriteError> {
    let value = encode_configured_row_value(columns)?;
    Ok((
        encode_row_key_with_handle(table_id, &RecordHandle::Int(handle)),
        value,
    ))
}

/// Encodes only the new-format value half of an all-integer configured row.
pub fn encode_configured_row_value(
    columns: &[ConfiguredRowColumn],
) -> Result<Vec<u8>, ConfiguredRowWriteError> {
    let typed = columns
        .iter()
        .map(|column| (column.id, ConfiguredValue::Int(column.value)))
        .collect::<Vec<_>>();
    encode_configured_row_value_typed(&typed)
}

/// Encodes the record key and new-format value for a mixed integer/string row.
pub fn encode_configured_mixed_row(
    table_id: i64,
    handle: i64,
    columns: &[(i64, ConfiguredValue)],
) -> Result<(Vec<u8>, Vec<u8>), ConfiguredRowWriteError> {
    let value = encode_configured_row_value_typed(columns)?;
    Ok((
        encode_row_key_with_handle(table_id, &RecordHandle::Int(handle)),
        value,
    ))
}

/// Encodes the new-format value half of a mixed integer/string row.
///
/// This is the one row-value builder; the integer-only helper above is a thin
/// wrapper, so both paths share the exact column-ID validation, duplicate
/// rejection, and offset-table layout.
pub fn encode_configured_row_value_typed(
    columns: &[(i64, ConfiguredValue)],
) -> Result<Vec<u8>, ConfiguredRowWriteError> {
    if columns.is_empty() {
        return Err(ConfiguredRowWriteError::NoStoredColumns);
    }
    let mut payloads = Vec::with_capacity(columns.len());
    for (index, (col_id, value)) in columns.iter().enumerate() {
        // TiDB allocates column IDs from 1, and row format v2 addresses them
        // with unsigned 32-bit metadata; both ends are rejected here so a
        // malformed schema cannot reach persisted bytes.
        let id = u32::try_from(*col_id)
            .ok()
            .filter(|id| *id != 0)
            .ok_or(ConfiguredRowWriteError::ColumnIdOutOfRange(*col_id))?;
        if columns[..index].iter().any(|(seen, _)| seen == col_id) {
            return Err(ConfiguredRowWriteError::DuplicateColumnId(*col_id));
        }
        payloads.push((id, value.to_payload()));
    }

    let entries = payloads
        .iter()
        .map(|(id, payload)| RawRowColumn {
            id: *id,
            value: Some(payload.as_slice()),
        })
        .collect::<Vec<_>>();
    let mut value = Vec::new();
    encode_raw_row(&entries, &mut value)?;
    Ok(value)
}

/// Reads one stored signed-`BIGINT` column out of a persisted row value.
pub fn decode_configured_row_int(
    value: &[u8],
    column_id: i64,
) -> Result<i64, ConfiguredRowReadError> {
    let (decoder, remainder) = RowDecoder::parse(value)?;
    if !remainder.is_empty() {
        return Err(ConfiguredRowReadError::TrailingBytes(remainder.len()));
    }
    match decoder.column(column_id)? {
        RawRowValue::NotNull { bytes, .. } => Ok(decode_raw_int(bytes)?),
        RawRowValue::Null => Err(ConfiguredRowReadError::NullColumn(column_id)),
        RawRowValue::Missing => Err(ConfiguredRowReadError::MissingColumn(column_id)),
    }
}

/// Reads one stored string column's raw bytes out of a persisted row value.
///
/// The bytes are returned exactly as stored — no trailing-space trimming, no
/// charset conversion — because that is what a no-restored-data `CHAR` persists.
pub fn decode_configured_row_bytes(
    value: &[u8],
    column_id: i64,
) -> Result<Vec<u8>, ConfiguredRowReadError> {
    let (decoder, remainder) = RowDecoder::parse(value)?;
    if !remainder.is_empty() {
        return Err(ConfiguredRowReadError::TrailingBytes(remainder.len()));
    }
    match decoder.column(column_id)? {
        RawRowValue::NotNull { bytes, .. } => Ok(bytes.to_vec()),
        RawRowValue::Null => Err(ConfiguredRowReadError::NullColumn(column_id)),
        RawRowValue::Missing => Err(ConfiguredRowReadError::MissingColumn(column_id)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encode_int;

    #[test]
    fn record_key_carries_the_handle_and_the_value_never_does() {
        let (key, value) = encode_configured_row(114, 10, &[ConfiguredRowColumn::new(2, 100)])
            .expect("configured row");

        let mut expected_key = vec![b't'];
        encode_int(&mut expected_key, 114);
        expected_key.extend_from_slice(b"_r");
        encode_int(&mut expected_key, 10);
        assert_eq!(key, expected_key);

        assert_eq!(decode_configured_row_int(&value, 2), Ok(100));
        assert_eq!(
            decode_configured_row_int(&value, 1),
            Err(ConfiguredRowReadError::MissingColumn(1))
        );
    }

    #[test]
    fn stored_columns_round_trip_the_signed_domain() {
        for stored in [i64::MIN, -1, 0, 1, i64::from(i32::MAX) + 1, i64::MAX] {
            let value = encode_configured_row_value(&[ConfiguredRowColumn::new(2, stored)])
                .expect("configured row value");
            assert_eq!(decode_configured_row_int(&value, 2), Ok(stored));
        }
    }

    #[test]
    fn invalid_column_sets_fail_before_any_row_bytes_exist() {
        assert_eq!(
            encode_configured_row_value(&[]),
            Err(ConfiguredRowWriteError::NoStoredColumns)
        );
        assert_eq!(
            encode_configured_row_value(&[
                ConfiguredRowColumn::new(2, 1),
                ConfiguredRowColumn::new(2, 2),
            ]),
            Err(ConfiguredRowWriteError::DuplicateColumnId(2))
        );
        assert_eq!(
            encode_configured_row_value(&[ConfiguredRowColumn::new(-1, 1)]),
            Err(ConfiguredRowWriteError::ColumnIdOutOfRange(-1))
        );
        assert_eq!(
            encode_configured_row_value(&[ConfiguredRowColumn::new(0, 1)]),
            Err(ConfiguredRowWriteError::ColumnIdOutOfRange(0))
        );
        assert_eq!(
            encode_configured_row_value(&[ConfiguredRowColumn::new(i64::from(u32::MAX) + 1, 1)]),
            Err(ConfiguredRowWriteError::ColumnIdOutOfRange(
                i64::from(u32::MAX) + 1
            ))
        );
    }

    #[test]
    fn malformed_and_trailing_row_bytes_are_typed_errors() {
        let mut value =
            encode_configured_row_value(&[ConfiguredRowColumn::new(2, 7)]).expect("row value");
        value.push(0);
        assert_eq!(
            decode_configured_row_int(&value, 2),
            Err(ConfiguredRowReadError::TrailingBytes(1))
        );
        assert!(matches!(
            decode_configured_row_int(&[], 2),
            Err(ConfiguredRowReadError::Decode(_))
        ));
    }
}
