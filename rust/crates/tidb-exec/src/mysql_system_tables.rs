// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Reading `mysql.*` rows a Go TiDB wrote.
//!
//! The bounded read path ([`crate::cluster_catalog::configure_loaded_table`])
//! serves only tables whose handle is a signed `BIGINT` primary key. Every
//! `mysql.*` table in scope here fails that test, and they fail it in three
//! different ways, which is why this module reads a row from its *key and*
//! value rather than the value alone:
//!
//! * The grant tables (`mysql.user`, `mysql.db`, …) carry a **non-clustered**
//!   `PRIMARY KEY (Host, User)` — verified against a live v8.5.7 cluster's
//!   stored `TableInfo`: `pk_is_handle=false`, `is_common_handle=false`. Every
//!   declared column lives in the row value and the key holds only an
//!   anonymous `_tidb_rowid`.
//! * `mysql.stats_meta` declares `PRIMARY KEY (table_id) CLUSTERED` over a
//!   single `BIGINT`, so `pk_is_handle=true`: `table_id` is *not* in the row
//!   value at all, it is the integer record handle.
//! * `mysql.stats_histograms` / `stats_buckets` / `stats_fm_sketch` declare a
//!   composite `CLUSTERED` primary key, so `is_common_handle=true` and their
//!   key columns are datum-key-encoded in the record key.
//!
//! [`HandleLayout`] is that distinction made once, read off the cluster's own
//! `TableInfo`, so a caller names a column and gets its value wherever the
//! cluster chose to store it.
//!
//! Two further properties of a real cluster's `mysql.*` shape this module:
//!
//! * **Both row formats occur.** A live v8.5.7 playground stores `mysql.tidb`
//!   in the *old* (v1) row format and later-written rows in v2, because
//!   bootstrap DML runs before `tidb_row_format_version` is in effect. Rather
//!   than branch on the version, this reads every row through
//!   [`tidb_tablecodec::decode_table_row_to_map`], which is Go
//!   `tablecodec.DecodeRowWithMap` and already owns both formats.
//! * **Column identity is the cluster's, not this node's.** A column is
//!   located by name in the stored `TableInfo` and decoded at exactly the type
//!   that `TableInfo` declares, so `ENUM`/`SET` labels come from the cluster's
//!   own `Elems` and a schema this node does not recognize is refused by name
//!   rather than silently misread.
//!
//! Only the columns a caller names are decoded. That is what keeps a table
//! like `mysql.user` — which also carries `JSON`, `TIMESTAMP`, and unsigned
//! integer columns no caller here reads — from ever needing a "type this
//! reader does not support" branch.

use std::collections::BTreeMap;
use std::fmt;

use tidb_codec::table_key::cut_row_key_prefix;
use tidb_codec::{decode as decode_datums, encode_key, encode_row_key, gen_table_record_prefix};
use tidb_datatype::{Datum, FieldType};
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::TableInfo;
use tidb_tablecodec::decode_table_row_to_map;

use crate::cluster_catalog::{ClusterCatalog, ClusterCatalogError, MetaPairs, MetaSnapshot};

/// Go `mysql.SystemDB`: the schema every table in this module lives in.
pub const SYSTEM_DB: &str = "mysql";

/// Why a `mysql.*` row could not be read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SystemTableError {
    /// The schema or table is not in the loaded catalog at all.
    Missing {
        /// Fully qualified `mysql.<table>` name.
        name: String,
    },
    /// The table exists but does not carry a column this reader needs.
    MissingColumn {
        /// Fully qualified `mysql.<table>` name.
        name: String,
        /// Column name as this reader spells it.
        column: String,
    },
    /// The column exists but decoded to a value shape this reader cannot use.
    UnexpectedColumnValue {
        /// Fully qualified `mysql.<table>` name.
        name: String,
        /// Column name as this reader spells it.
        column: String,
        /// What the reader wanted.
        wanted: &'static str,
        /// What the row actually held.
        stored: String,
    },
    /// A stored row value did not decode.
    Decode {
        /// Fully qualified `mysql.<table>` name.
        name: String,
        /// Self-contained explanation.
        detail: String,
    },
    /// The snapshot read itself failed.
    Snapshot(String),
}

impl fmt::Display for SystemTableError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { name } => {
                write!(formatter, "the cluster catalog has no table `{name}`")
            }
            Self::MissingColumn { name, column } => {
                write!(formatter, "`{name}` has no column `{column}`")
            }
            Self::UnexpectedColumnValue {
                name,
                column,
                wanted,
                stored,
            } => write!(
                formatter,
                "`{name}`.`{column}` holds {stored}, which this reader cannot read as {wanted}"
            ),
            Self::Decode { name, detail } => {
                write!(formatter, "a `{name}` row did not decode: {detail}")
            }
            Self::Snapshot(detail) => {
                write!(formatter, "system-table snapshot read failed: {detail}")
            }
        }
    }
}

impl std::error::Error for SystemTableError {}

impl From<ClusterCatalogError> for SystemTableError {
    fn from(error: ClusterCatalogError) -> Self {
        Self::Snapshot(error.to_string())
    }
}

/// Where a table's clustered key columns live, read off the cluster's own
/// `TableInfo`.
///
/// This is Go's `TableInfo.PKIsHandle` / `IsCommonHandle` pair, stated as the
/// one thing a reader actually needs from it: which declared columns are
/// absent from the row value because the record *key* already carries them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HandleLayout {
    /// No clustered handle. The key holds an anonymous `_tidb_rowid` and
    /// every declared column is in the row value.
    RowId,
    /// `pk_is_handle`: this single integer column *is* the record handle.
    Int(String),
    /// `is_common_handle`: these columns, in this order, are datum-key-encoded
    /// in the record key.
    Common(Vec<String>),
}

impl HandleLayout {
    /// Reads the layout a stored `TableInfo` declares.
    #[must_use]
    pub fn of(table: &TableInfo) -> Self {
        if table.pk_is_handle {
            if let Some(column) = table.get_pk_col_info() {
                return Self::Int(column.name.lowercase().to_owned());
            }
        }
        if table.is_common_handle {
            if let Some(primary) = table.indices.iter().find(|index| index.primary) {
                return Self::Common(
                    primary
                        .columns
                        .iter()
                        .map(|column| column.name.lowercase().to_owned())
                        .collect(),
                );
            }
        }
        Self::RowId
    }

    /// The key columns this layout carries, in key order.
    #[must_use]
    pub fn columns(&self) -> &[String] {
        match self {
            Self::RowId => &[],
            Self::Int(column) => std::slice::from_ref(column),
            Self::Common(columns) => columns.as_slice(),
        }
    }
}

/// One `mysql.*` table located in a loaded catalog, projected to the columns
/// one caller reads.
///
/// A named column the stored table does not carry is simply absent from the
/// projection rather than an error: the `mysql.user` privilege columns grew
/// over TiDB versions, so an older cluster legitimately lacks some of them,
/// and [`SystemRow::has_column`] is how a caller tells that apart from a
/// column that is present and set to `N`.
#[derive(Clone, Debug)]
pub struct SystemTableView {
    name: String,
    table_id: i64,
    /// Projected column name -> stored column ID.
    ids: BTreeMap<String, i64>,
    /// Stored column ID -> declared type, the shape the row decoder takes.
    types: BTreeMap<i64, FieldType>,
    /// Which projected columns the record key carries instead of the value.
    handle: HandleLayout,
}

impl SystemTableView {
    /// Locates one `mysql.<table>` in a loaded catalog and projects it to the
    /// named columns.
    ///
    /// Only public columns are projected: a column mid-DDL is not one whose
    /// stored values this node may interpret.
    pub fn locate(
        catalog: &ClusterCatalog,
        table: &str,
        columns: &[&str],
    ) -> Result<Self, SystemTableError> {
        let qualified = format!("{SYSTEM_DB}.{table}");
        let missing = || SystemTableError::Missing {
            name: qualified.clone(),
        };
        let database = catalog
            .databases
            .iter()
            .find(|database| database.info.name.lowercase() == SYSTEM_DB)
            .ok_or_else(missing)?;
        let info = database
            .tables
            .iter()
            .find(|stored| stored.name.lowercase() == table)
            .ok_or_else(missing)?;
        Ok(Self::project(&qualified, info, columns))
    }

    /// Projects one already-located `TableInfo` under the given display name.
    #[must_use]
    pub fn project(name: &str, table: &TableInfo, columns: &[&str]) -> Self {
        let handle = HandleLayout::of(table);
        let key_columns = handle.columns();
        let mut ids = BTreeMap::new();
        let mut types = BTreeMap::new();
        for column in table.cols() {
            if column.state != SchemaState::PUBLIC {
                continue;
            }
            let lowercase = column.name.lowercase();
            if !columns.contains(&lowercase) {
                continue;
            }
            ids.insert(lowercase.to_owned(), column.id);
            // A key column has no entry in the row value, so handing its type
            // to the row decoder would only invite it to look for one.
            if !key_columns.iter().any(|key| key == lowercase) {
                types.insert(column.id, column.field_type.clone());
            }
        }
        Self {
            name: name.to_owned(),
            table_id: table.id,
            ids,
            types,
            handle,
        }
    }

    /// Which projected columns this table's record key carries.
    #[must_use]
    pub const fn handle(&self) -> &HandleLayout {
        &self.handle
    }

    /// The record-key prefix selecting every row whose leading key columns
    /// equal `prefix`.
    ///
    /// This is the whole reason [`HandleLayout`] is modelled: `mysql.stats_*`
    /// are cluster-wide tables, and reading one table's statistics must not
    /// mean scanning every table's. An empty `prefix`, or a table with no
    /// clustered handle, yields the plain record prefix — a full scan, stated
    /// rather than hidden.
    pub fn record_prefix(&self, prefix: &[Datum]) -> Result<Vec<u8>, SystemTableError> {
        if prefix.is_empty() {
            return Ok(gen_table_record_prefix(self.table_id));
        }
        match &self.handle {
            HandleLayout::RowId => Ok(gen_table_record_prefix(self.table_id)),
            // An integer handle is the fixed-width big-endian, sign-flipped
            // encoding of the whole handle, so only an exact single-column
            // match narrows anything; a longer prefix has no key to name.
            HandleLayout::Int(_) => {
                let encoded =
                    encode_key(&prefix[..1]).map_err(|error| SystemTableError::Decode {
                        name: self.name.clone(),
                        detail: error.to_string(),
                    })?;
                // `encode_key` writes the flag byte; a record handle is the
                // bare 8-byte payload.
                Ok(encode_row_key(self.table_id, &encoded[1..]))
            }
            HandleLayout::Common(columns) => {
                let taken = prefix.len().min(columns.len());
                let encoded =
                    encode_key(&prefix[..taken]).map_err(|error| SystemTableError::Decode {
                        name: self.name.clone(),
                        detail: error.to_string(),
                    })?;
                Ok(encode_row_key(self.table_id, &encoded))
            }
        }
    }

    /// The fully qualified `mysql.<table>` name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The stored table ID whose record range holds these rows.
    #[must_use]
    pub const fn table_id(&self) -> i64 {
        self.table_id
    }

    /// Whether the stored table carries a projected column of this name.
    #[must_use]
    pub fn has_column(&self, column: &str) -> bool {
        self.ids.contains_key(column)
    }

    fn column_id(&self, column: &str) -> Result<i64, SystemTableError> {
        self.ids
            .get(column)
            .copied()
            .ok_or_else(|| SystemTableError::MissingColumn {
                name: self.name.clone(),
                column: column.to_owned(),
            })
    }
}

/// Reads every row in one `mysql.*` table's record range, in key order.
///
/// Both halves of each pair matter and neither may be dropped: the key carries
/// the clustered key columns for a [`HandleLayout::Int`] or
/// [`HandleLayout::Common`] table, and it is also the only place a
/// `_tidb_rowid` handle survives — which is what a *writer* rewrites on UPDATE
/// and removes on DELETE.
pub fn scan_system_table<S: MetaSnapshot>(
    snapshot: &mut S,
    view: &SystemTableView,
) -> Result<MetaPairs, SystemTableError> {
    scan_system_table_prefixed(snapshot, view, &[])
}

/// Reads the rows whose leading clustered key columns equal `prefix`.
///
/// See [`SystemTableView::record_prefix`] for what a prefix can narrow.
pub fn scan_system_table_prefixed<S: MetaSnapshot>(
    snapshot: &mut S,
    view: &SystemTableView,
    prefix: &[Datum],
) -> Result<MetaPairs, SystemTableError> {
    let key_prefix = view.record_prefix(prefix)?;
    Ok(snapshot.scan_prefix(&key_prefix)?)
}

/// Decodes the clustered key columns one record key carries.
///
/// A [`HandleLayout::RowId`] table's key holds only an anonymous
/// `_tidb_rowid`, which names no declared column, so it decodes to nothing.
fn decode_handle_columns(
    view: &SystemTableView,
    key: &[u8],
) -> Result<Vec<(String, Datum)>, SystemTableError> {
    let columns = view.handle.columns();
    if columns.is_empty() {
        return Ok(Vec::new());
    }
    let encoded = cut_row_key_prefix(key);
    let decoded = match &view.handle {
        // An integer handle is the bare 8-byte payload with no flag byte, so
        // it cannot go through the datum decoder as stored.
        HandleLayout::Int(_) => {
            let bytes: [u8; 8] = encoded.try_into().map_err(|_| SystemTableError::Decode {
                name: view.name.clone(),
                detail: format!(
                    "an integer record handle is 8 bytes, this key carries {}",
                    encoded.len()
                ),
            })?;
            // Go `codec.DecodeInt`: the sign bit is flipped so that the
            // big-endian bytes sort in signed order.
            vec![Datum::Int((u64::from_be_bytes(bytes) ^ (1 << 63)) as i64)]
        }
        HandleLayout::Common(_) | HandleLayout::RowId => decode_datums(encoded, columns.len())
            .map_err(|error| SystemTableError::Decode {
                name: view.name.clone(),
                detail: error.to_string(),
            })?,
    };
    if decoded.len() != columns.len() {
        return Err(SystemTableError::Decode {
            name: view.name.clone(),
            detail: format!(
                "the record key decoded to {} handle columns, the table declares {}",
                decoded.len(),
                columns.len()
            ),
        });
    }
    Ok(columns.iter().cloned().zip(decoded).collect())
}

/// One stored row, decoded to the projected columns.
pub struct SystemRow<'view> {
    view: &'view SystemTableView,
    values: BTreeMap<i64, Datum>,
}

impl<'view> SystemRow<'view> {
    /// Decodes one stored row's projected columns from its record key and its
    /// row value.
    ///
    /// The key is not optional. A clustered table stores its key columns
    /// *only* in the key, so a parse that ignored it would report those
    /// columns as absent — a silent wrong answer rather than an error, which
    /// for `mysql.stats_buckets` would mean every bucket losing the
    /// `table_id`/`hist_id` that says which histogram it belongs to.
    ///
    /// The timezone is deliberately `None`: no projected column is a
    /// `TIMESTAMP`, so there is no value whose meaning a session timezone
    /// could change.
    pub fn parse(
        view: &'view SystemTableView,
        key: &[u8],
        value: &[u8],
    ) -> Result<Self, SystemTableError> {
        let mut values = decode_table_row_to_map(value, &view.types, None).map_err(|error| {
            SystemTableError::Decode {
                name: view.name.clone(),
                detail: error.to_string(),
            }
        })?;
        for (column, datum) in decode_handle_columns(view, key)? {
            if let Some(id) = view.ids.get(&column) {
                values.insert(*id, datum);
            }
        }
        Ok(Self { view, values })
    }

    /// Whether the stored table carries a projected column of this name.
    #[must_use]
    pub fn has_column(&self, column: &str) -> bool {
        self.view.has_column(column)
    }

    fn value(&self, column: &str) -> Result<Option<&Datum>, SystemTableError> {
        let id = self.view.column_id(column)?;
        // A row that does not carry the column is indistinguishable from one
        // that stores SQL NULL there, and every caller here treats both as the
        // column's declared default.
        Ok(self
            .values
            .get(&id)
            .filter(|datum| !matches!(datum, Datum::Null)))
    }

    fn wrong_value(&self, column: &str, wanted: &'static str, stored: &Datum) -> SystemTableError {
        SystemTableError::UnexpectedColumnValue {
            name: self.view.name.clone(),
            column: column.to_owned(),
            wanted,
            stored: format!("{stored:?}"),
        }
    }

    /// Reads a character column as UTF-8 text.
    pub fn text(&self, column: &str) -> Result<Option<String>, SystemTableError> {
        let Some(datum) = self.value(column)? else {
            return Ok(None);
        };
        let bytes = match datum {
            Datum::String(string) => string.bytes(),
            Datum::Bytes(bytes) => bytes.as_slice(),
            other => return Err(self.wrong_value(column, "text", other)),
        };
        String::from_utf8(bytes.to_vec())
            .map(Some)
            .map_err(|error| SystemTableError::Decode {
                name: self.view.name.clone(),
                detail: error.to_string(),
            })
    }

    /// Reads a raw byte column (`BLOB`/`LONGBLOB`) without charset
    /// interpretation.
    ///
    /// `mysql.stats_buckets`'s bounds and `mysql.stats_top_n`'s values are
    /// stored bytes that are frequently *not* valid UTF-8 — an index bound is
    /// a datum-key-encoded key — so reading them as text would corrupt them.
    pub fn bytes(&self, column: &str) -> Result<Option<Vec<u8>>, SystemTableError> {
        let Some(datum) = self.value(column)? else {
            return Ok(None);
        };
        match datum {
            Datum::Bytes(bytes) | Datum::Raw(bytes) => Ok(Some(bytes.clone())),
            Datum::String(string) => Ok(Some(string.bytes().to_vec())),
            other => Err(self.wrong_value(column, "bytes", other)),
        }
    }

    /// Reads an integer column as `i64`.
    ///
    /// A `BIGINT UNSIGNED` column whose stored value exceeds `i64::MAX` is
    /// refused rather than wrapped; use [`Self::u64`] for the columns that
    /// legitimately hold one (`mysql.stats_meta.version` is a TSO).
    pub fn i64(&self, column: &str) -> Result<Option<i64>, SystemTableError> {
        let Some(datum) = self.value(column)? else {
            return Ok(None);
        };
        match datum {
            Datum::Int(value) => Ok(Some(*value)),
            Datum::UInt(value) => i64::try_from(*value)
                .map(Some)
                .map_err(|_| self.wrong_value(column, "a signed integer", datum)),
            other => Err(self.wrong_value(column, "an integer", other)),
        }
    }

    /// Reads an integer column as `u64`.
    pub fn u64(&self, column: &str) -> Result<Option<u64>, SystemTableError> {
        let Some(datum) = self.value(column)? else {
            return Ok(None);
        };
        match datum {
            Datum::UInt(value) => Ok(Some(*value)),
            Datum::Int(value) => u64::try_from(*value)
                .map(Some)
                .map_err(|_| self.wrong_value(column, "an unsigned integer", datum)),
            other => Err(self.wrong_value(column, "an integer", other)),
        }
    }

    /// Reads a `DOUBLE` column as `f64`.
    pub fn f64(&self, column: &str) -> Result<Option<f64>, SystemTableError> {
        let Some(datum) = self.value(column)? else {
            return Ok(None);
        };
        match datum {
            Datum::Real(value) | Datum::Float32(value) => Ok(Some(*value)),
            Datum::Int(value) => Ok(Some(*value as f64)),
            Datum::UInt(value) => Ok(Some(*value as f64)),
            other => Err(self.wrong_value(column, "a float", other)),
        }
    }

    /// Reads an `ENUM` column as its stored label.
    pub fn enum_label(&self, column: &str) -> Result<Option<String>, SystemTableError> {
        let Some(datum) = self.value(column)? else {
            return Ok(None);
        };
        match datum {
            Datum::Enum(value, _) => Ok(Some(value.name().to_owned())),
            other => Err(self.wrong_value(column, "ENUM", other)),
        }
    }

    /// Whether an `ENUM('N','Y')` privilege column holds `Y`.
    ///
    /// Every `mysql.user`/`mysql.db` privilege column is declared
    /// `ENUM('N','Y') NOT NULL DEFAULT 'N'`, so a value-less column is `N` —
    /// the same answer Go's `SELECT` produces from the declared default.
    pub fn is_yes(&self, column: &str) -> Result<bool, SystemTableError> {
        Ok(self.enum_label(column)?.as_deref() == Some("Y"))
    }

    /// Reads a `SET` column as the labels its stored bitmask selects.
    ///
    /// Go renders a `SET` value as its comma-separated element names
    /// (`types.ParseSetValue`), and an empty selection as the empty string —
    /// which is no labels, not one empty label.
    pub fn set_labels(&self, column: &str) -> Result<Vec<String>, SystemTableError> {
        let Some(datum) = self.value(column)? else {
            return Ok(Vec::new());
        };
        match datum {
            Datum::Set(value, _) if value.name().is_empty() => Ok(Vec::new()),
            Datum::Set(value, _) => Ok(value.name().split(',').map(str::to_owned).collect()),
            other => Err(self.wrong_value(column, "SET", other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use tidb_ast::CiString;
    use tidb_datatype::FieldTypeCode;
    use tidb_model::column::ColumnInfo;

    use super::*;

    /// One `mysql.user` row exactly as a v8.5.7 Go TiDB wrote it, captured
    /// from a live playground's record range. It is the `root`@`%` row Go's
    /// `doDMLWorks` seeds: every privilege `Y`, `Account_locked` `N`, and an
    /// empty `authentication_string`.
    ///
    /// Note the leading `8`, a varint column-ID flag: bootstrap DML runs
    /// before `tidb_row_format_version` takes effect, so a real cluster's
    /// `mysql.*` is in the OLD row format even on a current TiDB. That is
    /// precisely the case a v2-only decoder gets wrong.
    const ROOT_ROW: &[u8] = &[
        8, 2, 2, 2, 37, 8, 4, 2, 8, 114, 111, 111, 116, 8, 6, 2, 0, 8, 8, 2, 42, 109, 121, 115,
        113, 108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 8,
        10, 9, 2, 8, 12, 9, 2, 8, 14, 9, 2, 8, 16, 9, 2, 8, 18, 9, 2, 8, 20, 9, 2, 8, 22, 9, 2, 8,
        24, 9, 2, 8, 26, 9, 2, 8, 28, 9, 2, 8, 30, 9, 2, 8, 32, 9, 2, 8, 34, 9, 2, 8, 36, 9, 2, 8,
        38, 9, 2, 8, 40, 9, 2, 8, 42, 9, 2, 8, 44, 9, 2, 8, 46, 9, 2, 8, 48, 9, 2, 8, 50, 9, 2, 8,
        52, 9, 2, 8, 54, 9, 2, 8, 56, 9, 2, 8, 58, 9, 2, 8, 60, 9, 2, 8, 62, 9, 2, 8, 64, 9, 1, 8,
        66, 9, 2, 8, 68, 9, 2, 8, 70, 9, 2, 8, 72, 9, 2, 8, 74, 9, 2, 8, 82, 2, 0, 8, 84, 9, 1, 8,
        86, 9, 128, 128, 128, 176, 186, 167, 158, 221, 25, 8, 90, 9, 0,
    ];

    /// The `bridge`@`%` row the same cluster wrote for
    /// `CREATE USER 'bridge'@'%' IDENTIFIED BY 'bridgepw'`, then
    /// `GRANT SELECT ON *.*` and `GRANT SUPER ON *.*`: a stage-two password
    /// hash, `Select_priv` and `Super_priv` `Y`, and an unlocked account.
    const BRIDGE_ROW: &[u8] = &[
        8, 2, 2, 2, 37, 8, 4, 2, 12, 98, 114, 105, 100, 103, 101, 8, 6, 2, 82, 42, 55, 53, 54, 51,
        48, 52, 69, 66, 70, 57, 56, 57, 56, 52, 48, 55, 56, 57, 57, 49, 52, 52, 66, 51, 55, 52, 69,
        69, 67, 66, 50, 67, 67, 49, 66, 57, 52, 53, 57, 55, 8, 8, 2, 42, 109, 121, 115, 113, 108,
        95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 8, 10, 9, 2, 8,
        12, 9, 1, 8, 14, 9, 1, 8, 16, 9, 1, 8, 18, 9, 1, 8, 20, 9, 1, 8, 22, 9, 1, 8, 24, 9, 1, 8,
        26, 9, 1, 8, 28, 9, 1, 8, 30, 9, 1, 8, 32, 9, 2, 8, 34, 9, 1, 8, 36, 9, 1, 8, 38, 9, 1, 8,
        40, 9, 1, 8, 42, 9, 1, 8, 44, 9, 1, 8, 46, 9, 1, 8, 48, 9, 1, 8, 50, 9, 1, 8, 52, 9, 1, 8,
        54, 9, 1, 8, 56, 9, 1, 8, 58, 9, 1, 8, 60, 9, 1, 8, 62, 9, 1, 8, 64, 9, 1, 8, 66, 9, 1, 8,
        68, 9, 1, 8, 70, 9, 1, 8, 72, 9, 1, 8, 74, 9, 1, 8, 80, 10, 1, 0, 0, 0, 0, 8, 0, 0, 0, 8,
        82, 2, 0, 8, 84, 9, 1, 8, 86, 9, 128, 128, 128, 160, 191, 167, 158, 221, 25, 8, 90, 9, 0,
    ];

    /// The `analyst` row the same cluster wrote for `CREATE ROLE 'analyst'`.
    const ANALYST_ROLE_ROW: &[u8] = &[
        8, 2, 2, 2, 37, 8, 4, 2, 14, 97, 110, 97, 108, 121, 115, 116, 8, 6, 2, 0, 8, 8, 2, 42, 109,
        121, 115, 113, 108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114,
        100, 8, 10, 9, 1, 8, 12, 9, 1, 8, 14, 9, 1, 8, 16, 9, 1, 8, 18, 9, 1, 8, 20, 9, 1, 8, 22,
        9, 1, 8, 24, 9, 1, 8, 26, 9, 1, 8, 28, 9, 1, 8, 30, 9, 1, 8, 32, 9, 1, 8, 34, 9, 1, 8, 36,
        9, 1, 8, 38, 9, 1, 8, 40, 9, 1, 8, 42, 9, 1, 8, 44, 9, 1, 8, 46, 9, 1, 8, 48, 9, 1, 8, 50,
        9, 1, 8, 52, 9, 1, 8, 54, 9, 1, 8, 56, 9, 1, 8, 58, 9, 1, 8, 60, 9, 1, 8, 62, 9, 1, 8, 64,
        9, 2, 8, 66, 9, 1, 8, 68, 9, 1, 8, 70, 9, 1, 8, 72, 9, 1, 8, 74, 9, 1, 8, 80, 10, 1, 0, 0,
        0, 0, 8, 0, 0, 0, 8, 82, 2, 0, 8, 84, 9, 2, 8, 86, 9, 128, 128, 128, 184, 162, 168, 158,
        221, 25, 8, 90, 9, 0,
    ];

    /// The `bootstrapped = 'True'` row of the same cluster's `mysql.tidb`.
    const BOOTSTRAPPED_ROW: &[u8] = &[
        8, 2, 2, 24, 98, 111, 111, 116, 115, 116, 114, 97, 112, 112, 101, 100, 8, 4, 2, 8, 84, 114,
        117, 101, 8, 6, 2, 60, 66, 111, 111, 116, 115, 116, 114, 97, 112, 32, 102, 108, 97, 103,
        46, 32, 68, 111, 32, 110, 111, 116, 32, 100, 101, 108, 101, 116, 101, 46,
    ];

    /// `TableInfo::cols` places columns by their declared offset, so a
    /// fixture table must give each one its own.
    fn column(offset: i32, id: i64, name: &str, mut field_type: FieldType) -> ColumnInfo {
        if field_type.code() == FieldTypeCode::Enum {
            field_type.set_elems(vec!["N".to_owned(), "Y".to_owned()]);
        }
        let mut column = ColumnInfo::new(id, name, field_type);
        column.offset = offset;
        column
    }

    /// The column IDs, names, and declared types are the live cluster's own,
    /// read back from its stored `TableInfo`.
    fn user_table() -> TableInfo {
        let char_type = FieldType::new(FieldTypeCode::String);
        let blob_type = FieldType::new(FieldTypeCode::Blob);
        let enum_type = FieldType::new(FieldTypeCode::Enum);
        TableInfo {
            id: 4,
            name: CiString::new("user"),
            columns: vec![
                column(0, 1, "Host", char_type.clone()),
                column(1, 2, "User", char_type.clone()),
                column(2, 3, "authentication_string", blob_type),
                column(3, 4, "plugin", char_type),
                column(4, 5, "Select_priv", enum_type.clone()),
                column(5, 16, "Super_priv", enum_type.clone()),
                column(6, 32, "Account_locked", enum_type),
            ],
            ..TableInfo::default()
        }
    }

    fn user_view() -> SystemTableView {
        SystemTableView::project(
            "mysql.user",
            &user_table(),
            &[
                "host",
                "user",
                "authentication_string",
                "plugin",
                "select_priv",
                "super_priv",
                "account_locked",
            ],
        )
    }

    #[test]
    fn a_go_written_root_row_reads_back_as_the_row_go_seeds() {
        let view = user_view();
        let row = SystemRow::parse(&view, &[], ROOT_ROW).expect("the captured row decodes");
        assert_eq!(row.text("host").unwrap().as_deref(), Some("%"));
        assert_eq!(row.text("user").unwrap().as_deref(), Some("root"));
        // Go's non-secure bootstrap INSERT stores the empty string, not NULL;
        // an empty stored value must stay distinguishable from an absent one.
        assert_eq!(
            row.text("authentication_string").unwrap().as_deref(),
            Some("")
        );
        assert_eq!(
            row.text("plugin").unwrap().as_deref(),
            Some("mysql_native_password")
        );
        assert!(row.is_yes("select_priv").unwrap());
        assert!(row.is_yes("super_priv").unwrap());
        assert!(!row.is_yes("account_locked").unwrap());
    }

    #[test]
    fn a_go_created_account_reads_back_with_the_hash_go_stored_for_its_password() {
        let view = user_view();
        let row = SystemRow::parse(&view, &[], BRIDGE_ROW).expect("the captured row decodes");
        assert_eq!(row.text("user").unwrap().as_deref(), Some("bridge"));
        // This exact hash is what `IDENTIFIED BY 'bridgepw'` produced, and
        // what this node's native-password verifier must match a client's
        // scramble against.
        assert_eq!(
            row.text("authentication_string").unwrap().as_deref(),
            Some("*756304EBF9898407899144B374EECB2CC1B94597")
        );
        assert!(row.is_yes("select_priv").unwrap());
        assert!(row.is_yes("super_priv").unwrap());
        assert!(!row.is_yes("account_locked").unwrap());
    }

    #[test]
    fn a_go_created_role_reads_back_as_a_locked_passwordless_account() {
        // `CREATE ROLE 'analyst'` on the same cluster: Go writes a
        // `mysql.user` row with `Account_locked = 'Y'` and no password, which
        // is exactly what makes a role unable to log in.
        let view = user_view();
        let row = SystemRow::parse(&view, &[], ANALYST_ROLE_ROW).expect("the captured row decodes");
        assert_eq!(row.text("user").unwrap().as_deref(), Some("analyst"));
        assert_eq!(
            row.text("authentication_string").unwrap().as_deref(),
            Some("")
        );
        assert!(row.is_yes("account_locked").unwrap());
        assert!(!row.is_yes("select_priv").unwrap());
    }

    #[test]
    fn a_column_the_projection_does_not_carry_is_absent_rather_than_false() {
        // `Shutdown_priv` is a real `mysql.user` column this projection
        // deliberately omits, standing in for the older-cluster case: the
        // reader must be able to say "not there" instead of "N".
        let view = user_view();
        let row = SystemRow::parse(&view, &[], ROOT_ROW).expect("the captured row decodes");
        assert!(!row.has_column("shutdown_priv"));
        assert!(matches!(
            row.is_yes("shutdown_priv"),
            Err(SystemTableError::MissingColumn { .. })
        ));
    }

    #[test]
    fn the_bootstrap_flag_row_reads_back_as_go_wrote_it() {
        let varchar = FieldType::new(FieldTypeCode::Varchar);
        let table = TableInfo {
            id: 18,
            name: CiString::new("tidb"),
            columns: vec![
                column(0, 1, "VARIABLE_NAME", varchar.clone()),
                column(1, 2, "VARIABLE_VALUE", varchar),
            ],
            ..TableInfo::default()
        };
        let view =
            SystemTableView::project("mysql.tidb", &table, &["variable_name", "variable_value"]);
        let row = SystemRow::parse(&view, &[], BOOTSTRAPPED_ROW).expect("the captured row decodes");
        assert_eq!(
            row.text("variable_name").unwrap().as_deref(),
            Some("bootstrapped")
        );
        assert_eq!(row.text("variable_value").unwrap().as_deref(), Some("True"));
    }

    #[test]
    fn reading_a_character_column_as_an_enum_is_refused_rather_than_guessed() {
        let view = user_view();
        let row = SystemRow::parse(&view, &[], ROOT_ROW).expect("the captured row decodes");
        assert!(matches!(
            row.enum_label("plugin"),
            Err(SystemTableError::UnexpectedColumnValue { .. })
        ));
    }
}

#[cfg(test)]
mod clustered_handle_tests {
    use tidb_ast::CiString;
    use tidb_datatype::{FieldTypeCode, FieldTypeFlags};
    use tidb_model::column::ColumnInfo;
    use tidb_model::index::{IndexColumn, IndexInfo};

    use super::*;

    /// One `mysql.stats_buckets` record, key and value, exactly as a live
    /// v8.5.7 playground wrote it for `ANALYZE TABLE ... WITH 3 BUCKETS` on a
    /// table with `id BIGINT`: histogram `hist_id = 1`, `bucket_id = 2`,
    /// bounds `"11"` and `"12"`.
    ///
    /// The key is what makes this fixture worth keeping. It reads
    /// `t` + table 26 + `_r` + four datum-key-encoded integers — `table_id`
    /// 114, `is_index` 0, `hist_id` 1, `bucket_id` 2 — and *none* of those
    /// four appear in the value, which carries only column IDs 5..=9. A
    /// value-only reader gets an anonymous bucket belonging to no histogram.
    const BUCKET_KEY: &[u8] = &[
        0x74, 0x80, 0, 0, 0, 0, 0, 0, 0x1A, 0x5F, 0x72, //
        0x03, 0x80, 0, 0, 0, 0, 0, 0, 0x72, // table_id = 114
        0x03, 0x80, 0, 0, 0, 0, 0, 0, 0x00, // is_index = 0
        0x03, 0x80, 0, 0, 0, 0, 0, 0, 0x01, // hist_id = 1
        0x03, 0x80, 0, 0, 0, 0, 0, 0, 0x02, // bucket_id = 2
    ];
    const BUCKET_VALUE: &[u8] = &[
        0x08, 0x0A, 0x08, 0x04, // count = 2
        0x08, 0x0C, 0x08, 0x02, // repeats = 1
        0x08, 0x0E, 0x02, 0x04, 0x31, 0x32, // upper_bound = "12"
        0x08, 0x10, 0x02, 0x04, 0x31, 0x31, // lower_bound = "11"
        0x08, 0x12, 0x08, 0x00, // ndv = 0
    ];

    /// The `mysql.stats_meta` record for the same table, from the same
    /// cluster. `PRIMARY KEY (table_id) CLUSTERED` over one `BIGINT` makes
    /// `table_id` the *integer* handle: eight bare bytes with no flag byte,
    /// and again absent from the value.
    const META_KEY: &[u8] = &[
        0x74, 0x80, 0, 0, 0, 0, 0, 0, 0x16, 0x5F, 0x72, //
        0x80, 0, 0, 0, 0, 0, 0, 0x72, // table_id = 114
    ];
    const META_VALUE: &[u8] = &[
        0x08, 0x02, 0x09, 0x93, 0x80, 0xF0, 0xEB, 0x9D, 0xDF, 0xAB, 0xBF, 0x06, // version
        0x08, 0x06, 0x08, 0x00, // modify_count = 0
        0x08, 0x08, 0x09, 0x0C, // count = 12
        0x08, 0x0A, 0x09, 0x83, 0x80, 0xF0, 0xEB, 0x9D, 0xDF, 0xAB, 0xBF, 0x06, 0x08, 0x0C, 0x09,
        0x93, 0x80, 0xF0, 0xEB, 0x9D, 0xDF, 0xAB, 0xBF, 0x06,
    ];

    fn stats_column(offset: i32, id: i64, name: &str, code: FieldTypeCode) -> ColumnInfo {
        let mut column = ColumnInfo::new(id, name, FieldType::new(code));
        column.offset = offset;
        column
    }

    fn primary(columns: &[&str]) -> IndexInfo {
        IndexInfo {
            name: CiString::new("PRIMARY"),
            primary: true,
            columns: columns
                .iter()
                .enumerate()
                .map(|(offset, name)| IndexColumn {
                    name: CiString::new(*name),
                    offset: offset as i32,
                    ..IndexColumn::default()
                })
                .collect(),
            ..IndexInfo::default()
        }
    }

    fn stats_buckets_table() -> TableInfo {
        TableInfo {
            id: 26,
            name: CiString::new("stats_buckets"),
            is_common_handle: true,
            indices: vec![primary(&["table_id", "is_index", "hist_id", "bucket_id"])],
            columns: vec![
                stats_column(0, 1, "table_id", FieldTypeCode::LongLong),
                stats_column(1, 2, "is_index", FieldTypeCode::Tiny),
                stats_column(2, 3, "hist_id", FieldTypeCode::LongLong),
                stats_column(3, 4, "bucket_id", FieldTypeCode::LongLong),
                stats_column(4, 5, "count", FieldTypeCode::LongLong),
                stats_column(5, 6, "repeats", FieldTypeCode::LongLong),
                stats_column(6, 7, "upper_bound", FieldTypeCode::LongBlob),
                stats_column(7, 8, "lower_bound", FieldTypeCode::LongBlob),
                stats_column(8, 9, "ndv", FieldTypeCode::LongLong),
            ],
            ..TableInfo::default()
        }
    }

    fn stats_meta_table() -> TableInfo {
        // The column IDs are the cluster's own: `version` is 1 and `table_id`
        // is 2, and the stored value below carries no column 2 at all.
        let mut table_id = stats_column(1, 2, "table_id", FieldTypeCode::LongLong);
        table_id.set_flag(table_id.get_flag() | FieldTypeFlags::PRI_KEY);
        let mut version = stats_column(0, 1, "version", FieldTypeCode::LongLong);
        version.set_flag(version.get_flag() | FieldTypeFlags::UNSIGNED);
        let mut count = stats_column(3, 4, "count", FieldTypeCode::LongLong);
        count.set_flag(count.get_flag() | FieldTypeFlags::UNSIGNED);
        TableInfo {
            id: 22,
            name: CiString::new("stats_meta"),
            pk_is_handle: true,
            columns: vec![
                version,
                table_id,
                stats_column(2, 3, "modify_count", FieldTypeCode::LongLong),
                count,
            ],
            ..TableInfo::default()
        }
    }

    #[test]
    fn a_common_handle_bucket_row_reads_its_identity_out_of_the_record_key() {
        let table = stats_buckets_table();
        let view = SystemTableView::project(
            "mysql.stats_buckets",
            &table,
            &[
                "table_id",
                "is_index",
                "hist_id",
                "bucket_id",
                "count",
                "repeats",
                "lower_bound",
                "upper_bound",
                "ndv",
            ],
        );
        assert_eq!(
            view.handle(),
            &HandleLayout::Common(vec![
                "table_id".to_owned(),
                "is_index".to_owned(),
                "hist_id".to_owned(),
                "bucket_id".to_owned(),
            ])
        );
        let row = SystemRow::parse(&view, BUCKET_KEY, BUCKET_VALUE).expect("the record decodes");
        // The four key columns: without them this bucket belongs to nothing.
        assert_eq!(row.i64("table_id").unwrap(), Some(114));
        assert_eq!(row.i64("is_index").unwrap(), Some(0));
        assert_eq!(row.i64("hist_id").unwrap(), Some(1));
        assert_eq!(row.i64("bucket_id").unwrap(), Some(2));
        // The five value columns.
        assert_eq!(row.i64("count").unwrap(), Some(2));
        assert_eq!(row.i64("repeats").unwrap(), Some(1));
        assert_eq!(row.bytes("lower_bound").unwrap(), Some(b"11".to_vec()));
        assert_eq!(row.bytes("upper_bound").unwrap(), Some(b"12".to_vec()));
        assert_eq!(row.i64("ndv").unwrap(), Some(0));
    }

    #[test]
    fn an_integer_handle_row_reads_its_primary_key_out_of_the_record_key() {
        let table = stats_meta_table();
        let view = SystemTableView::project(
            "mysql.stats_meta",
            &table,
            &["table_id", "version", "modify_count", "count"],
        );
        assert_eq!(view.handle(), &HandleLayout::Int("table_id".to_owned()));
        let row = SystemRow::parse(&view, META_KEY, META_VALUE).expect("the record decodes");
        assert_eq!(row.i64("table_id").unwrap(), Some(114));
        assert_eq!(row.i64("modify_count").unwrap(), Some(0));
        assert_eq!(row.u64("count").unwrap(), Some(12));
        // The version is a TSO: an unsigned column that must come back
        // through `u64` rather than being read as a signed count.
        assert_eq!(row.u64("version").unwrap(), Some(468_003_799_479_091_219));
    }

    #[test]
    fn a_table_id_prefix_selects_exactly_that_tables_records() {
        // This is what keeps reading one table's statistics from scanning
        // every analyzed table's: the prefix is the real record key up to
        // `table_id`, so the scan starts and ends inside one table's rows.
        let buckets = stats_buckets_table();
        let view = SystemTableView::project("mysql.stats_buckets", &buckets, &["table_id"]);
        let prefix = view.record_prefix(&[Datum::Int(114)]).expect("it encodes");
        assert_eq!(prefix.as_slice(), &BUCKET_KEY[..20]);
        assert!(BUCKET_KEY.starts_with(&prefix));

        // An integer handle admits only the whole handle, and that yields the
        // row's exact key -- a point read, not a scan.
        let meta = stats_meta_table();
        let view = SystemTableView::project("mysql.stats_meta", &meta, &["table_id"]);
        let key = view.record_prefix(&[Datum::Int(114)]).expect("it encodes");
        assert_eq!(key.as_slice(), META_KEY);
    }

    #[test]
    fn a_row_id_table_has_no_key_columns_to_decode() {
        // `mysql.user`'s non-clustered PRIMARY KEY leaves the key anonymous,
        // and reading it as if it named columns would invent values.
        let table = TableInfo {
            id: 4,
            name: CiString::new("user"),
            columns: vec![stats_column(0, 1, "Host", FieldTypeCode::String)],
            ..TableInfo::default()
        };
        let view = SystemTableView::project("mysql.user", &table, &["host"]);
        assert_eq!(view.handle(), &HandleLayout::RowId);
        assert!(decode_handle_columns(&view, BUCKET_KEY).unwrap().is_empty());
    }
}
