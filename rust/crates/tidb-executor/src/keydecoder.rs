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

//! Source-shaped decoding for the key information shown in lock diagnostics.
//!
//! Go's `pkg/util/keydecoder` receives an `infoschema.InfoSchema`. Rust has a
//! persisted cluster catalog and an executor catalog, so the byte contract is
//! kept here below both consumers while [`KeyInfoCatalog`] supplies the small
//! metadata view the decoder needs.

use std::fmt::Write as _;

use serde::Serialize;
use tidb_codec::table_key::{
    decode_index_key, decode_key_head, decode_record_key, KeyHead, TableKeyError,
};

/// One index visible through [`KeyInfoCatalog`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyInfoIndex {
    /// Go `IndexInfo.ID`.
    pub id: i64,
    /// Go `IndexInfo.Name` in its original spelling.
    pub name: String,
}

/// The metadata Go's `InfoSchema` returns for one physical table ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyInfoTable {
    /// Database name in its original spelling.
    pub db_name: String,
    /// Database ID.
    pub db_id: i64,
    /// Logical table name in its original spelling.
    pub table_name: String,
    /// Logical table ID.
    pub table_id: i64,
    /// Partition name when the physical ID names a partition.
    pub partition_name: String,
    /// Partition ID, or zero for a non-partitioned table key.
    pub partition_id: i64,
    /// Every index currently present on the logical table.
    pub indexes: Vec<KeyInfoIndex>,
}

/// The two successful table lookup states exposed by Go's `InfoSchema`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyInfoTableLookup {
    /// Metadata is available and payload decoding should continue.
    Resolved(KeyInfoTable),
    /// The table is visible, but schema churn removed its association.
    TableWithoutSchema {
        /// Logical table name in its original spelling.
        table_name: String,
        /// Logical table ID.
        table_id: i64,
    },
}

/// The `InfoSchema` lookup needed by [`decode_key`].
pub trait KeyInfoCatalog {
    /// Resolves a logical table ID first, then a physical partition ID.
    fn resolve_physical_table(&self, physical_id: i64) -> Option<KeyInfoTableLookup>;
}

/// Go `keydecoder.HandleType`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub enum HandleType {
    /// The result has no record handle, or the source zero value.
    #[default]
    #[serde(rename = "")]
    None,
    /// An integer row handle.
    #[serde(rename = "int")]
    Int,
    /// A common (multi-column) row handle.
    #[serde(rename = "common")]
    Common,
    /// A handle implementation not known to the source decoder.
    #[serde(rename = "unknown")]
    Unknown,
}

impl HandleType {
    fn is_empty(&self) -> bool {
        matches!(self, Self::None)
    }
}

/// Go's `keydecoder.DecodedKey`, including JSON field names and omission
/// rules. `table_id` is intentionally always serialized, as in Go.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct DecodedKey {
    #[serde(rename = "db_name", skip_serializing_if = "String::is_empty")]
    /// Database name, when the physical ID resolves to a table.
    pub db_name: String,
    #[serde(rename = "table_name", skip_serializing_if = "String::is_empty")]
    /// Logical table name, when the physical ID resolves to a table.
    pub table_name: String,
    #[serde(rename = "partition_name", skip_serializing_if = "String::is_empty")]
    /// Partition name, when the physical ID resolves to a partition.
    pub partition_name: String,
    #[serde(rename = "handle_type", skip_serializing_if = "HandleType::is_empty")]
    /// Integer/common/unknown handle kind, or the source zero value.
    pub handle_type: HandleType,
    #[serde(rename = "handle_value", skip_serializing_if = "String::is_empty")]
    /// Source-formatted record handle.
    pub handle_value: String,
    #[serde(rename = "index_name", skip_serializing_if = "String::is_empty")]
    /// Index name, when the index still exists in the resolved table.
    pub index_name: String,
    #[serde(rename = "index_values", skip_serializing_if = "Option::is_none")]
    /// Decoded index values. `None` preserves Go's nil slice distinction.
    pub index_values: Option<Vec<String>>,
    #[serde(rename = "db_id", skip_serializing_if = "is_zero")]
    /// Database ID.
    pub db_id: i64,
    /// Logical table ID when resolved, or the original physical ID otherwise.
    /// This field is always emitted by Go's JSON encoder.
    pub table_id: i64,
    #[serde(rename = "partition_id", skip_serializing_if = "is_zero")]
    /// Physical partition ID.
    pub partition_id: i64,
    #[serde(rename = "index_id", skip_serializing_if = "is_zero")]
    /// Physical index ID.
    pub index_id: i64,
    #[serde(rename = "partition_handle", skip_serializing_if = "is_false")]
    /// Whether the source returned a partition-wrapped handle.
    pub is_partition_handle: bool,
}

fn is_zero(value: &i64) -> bool {
    *value == 0
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Failure returned by [`decode_key`]. The partially populated result is kept
/// because Go returns that value alongside record-key decode errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyDecoderFailure {
    /// Fields that Go had filled before the failure.
    pub decoded: Box<DecodedKey>,
    /// The structural key error.
    pub error: KeyDecoderError,
}

impl std::fmt::Display for KeyDecoderFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for KeyDecoderFailure {}

/// Structural failures surfaced by Go's key decoder.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyDecoderError {
    /// The key is neither a shallow record nor index key.
    UnknownKey(Vec<u8>),
    /// The key head could not be decoded.
    InvalidHead(TableKeyError),
    /// A record payload could not be decoded after metadata lookup.
    InvalidRecord {
        /// Physical table ID used in the diagnostic message.
        table_id: i64,
    },
}

impl std::fmt::Display for KeyDecoderError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownKey(key) => {
                formatter.write_str("Unknown key type for key [")?;
                for (index, byte) in key.iter().enumerate() {
                    if index != 0 {
                        formatter.write_char(' ')?;
                    }
                    write!(formatter, "{byte}")?;
                }
                formatter.write_char(']')
            }
            Self::InvalidHead(error) => error.fmt(formatter),
            Self::InvalidRecord { table_id } => {
                write!(formatter, "cannot decode record key of table {table_id}")
            }
        }
    }
}

impl std::error::Error for KeyDecoderError {}

/// Decodes one physical table record or index key against a catalog snapshot.
///
/// This preserves Go's intentionally shallow `IsRecordKey`/`IsIndexKey`
/// precheck, partial metadata on record errors, and swallowed index payload
/// errors. A missing index is not an error: its ID is retained while its name
/// and values remain empty/nil.
pub fn decode_key<C: KeyInfoCatalog + ?Sized>(
    key: &[u8],
    catalog: &C,
) -> Result<DecodedKey, KeyDecoderFailure> {
    let is_record = key.len() > 11 && key.first() == Some(&b't') && key[10] == b'r';
    let is_index = key.len() > 11 && key.first() == Some(&b't') && key[10] == b'i';
    if !is_record && !is_index {
        return Err(failure(
            DecodedKey::default(),
            KeyDecoderError::UnknownKey(key.to_vec()),
        ));
    }

    let head = decode_key_head(key)
        .map_err(|error| failure(DecodedKey::default(), KeyDecoderError::InvalidHead(error)))?;
    let physical_table_id = match head {
        KeyHead::Record { table_id } | KeyHead::Index { table_id, .. } => table_id,
    };
    let mut decoded = DecodedKey {
        table_id: physical_table_id,
        ..DecodedKey::default()
    };

    let table = match catalog.resolve_physical_table(physical_table_id) {
        Some(KeyInfoTableLookup::Resolved(table)) => {
            decoded.table_id = table.table_id;
            decoded.table_name.clone_from(&table.table_name);
            decoded.db_id = table.db_id;
            decoded.db_name.clone_from(&table.db_name);
            decoded.partition_id = table.partition_id;
            decoded.partition_name.clone_from(&table.partition_name);
            Some(table)
        }
        Some(KeyInfoTableLookup::TableWithoutSchema {
            table_name,
            table_id,
        }) => {
            decoded.table_name = table_name;
            decoded.table_id = table_id;
            return Ok(decoded);
        }
        None => None,
    };

    if is_record {
        return decode_record(key, decoded, physical_table_id);
    }
    decode_index(key, decoded, table.as_ref())
}

fn failure(decoded: DecodedKey, error: KeyDecoderError) -> KeyDecoderFailure {
    KeyDecoderFailure {
        decoded: Box::new(decoded),
        error,
    }
}

fn decode_record(
    key: &[u8],
    mut decoded: DecodedKey,
    physical_table_id: i64,
) -> Result<DecodedKey, KeyDecoderFailure> {
    let (_, handle) = decode_record_key(key).map_err(|_| {
        failure(
            decoded.clone(),
            KeyDecoderError::InvalidRecord {
                table_id: physical_table_id,
            },
        )
    })?;
    decoded.handle_type = if handle.is_int() {
        HandleType::Int
    } else {
        HandleType::Common
    };
    decoded.is_partition_handle = handle.partition_id().is_some();
    decoded.handle_value = handle.to_string();
    Ok(decoded)
}

fn decode_index(
    key: &[u8],
    mut decoded: DecodedKey,
    table: Option<&KeyInfoTable>,
) -> Result<DecodedKey, KeyDecoderFailure> {
    let Ok((_, index_id, values)) = decode_index_key(key) else {
        // Go logs and returns the partial result with nil error here.
        return Ok(decoded);
    };
    decoded.index_id = index_id;
    if let Some(index) =
        table.and_then(|table| table.indexes.iter().find(|index| index.id == index_id))
    {
        decoded.index_name.clone_from(&index.name);
        if !values.is_empty() {
            decoded.index_values = Some(values);
        }
    }
    Ok(decoded)
}
