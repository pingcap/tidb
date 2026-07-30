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

//! What a [`KvTable`](super::KvTable) IS, as opposed to what it does: the row
//! identifier, the column, index and foreign-key descriptions, and the
//! table's default character set.
//!
//! Inside: [`TableHandle`] (Go `kv.Handle`), [`IndexRange`] (Go
//! `ranger.Range`, the interval an index scan is given), [`KvIndex`],
//! [`FkAction`] + [`KvForeignKey`], [`KvColumn`] with the read-time cast that
//! makes `OriginDefaultValue` behave, and [`TableCharset`].
//!
//! Mirrors Go `pkg/meta/model`'s `IndexInfo`, `FKInfo` and `ColumnInfo`
//! together with `TableInfo.Charset`/`Collate`, plus `pkg/kv`'s `Handle`. The
//! operations that read and write these -- encode, decode, index maintenance,
//! scan -- stay in the parent module.

use tidb_codec::table_key::RecordHandle;
use tidb_datatype::{Charset, Collation, Datum, FieldType};

/// Go `kv.Handle`: the row identifier a record key encodes.
///
/// An integer handle comes from a single-column integer primary key (or the
/// allocated `_tidb_rowid` when the table has none); a common handle is the
/// codec encoding of a clustered primary key's columns, which is what makes a
/// string or multi-column primary key clustered.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TableHandle {
    /// Go `kv.IntHandle`.
    Int(i64),
    /// Go `kv.CommonHandle`, holding the encoded key datums.
    Common(Vec<u8>),
}

impl TableHandle {
    /// The record-key component this handle contributes.
    pub(crate) fn record_handle(&self) -> RecordHandle {
        match self {
            TableHandle::Int(value) => RecordHandle::Int(*value),
            TableHandle::Common(bytes) => RecordHandle::Common(bytes.clone()),
        }
    }

    /// The integer value, for the callers that only support int handles.
    #[must_use]
    pub fn int_value(&self) -> Option<i64> {
        match self {
            TableHandle::Int(value) => Some(*value),
            TableHandle::Common(_) => None,
        }
    }
}

/// Go `ranger.Range`: one scanned interval of an index.
///
/// Both bounds are datum tuples over the index's leading columns, with a flag
/// for whether each end is excluded. Go's builder always produces bounds that
/// exclude NULL for an ordinary comparison -- a `<`/`<=` range starts at
/// `MinNotNull`, not at NULL -- which is why a NULL value never satisfies a
/// comparison.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexRange {
    /// Go `Range.LowVal`.
    pub low: Vec<Datum>,
    /// Go `Range.HighVal`.
    pub high: Vec<Datum>,
    /// Go `Range.LowExclude`.
    pub low_exclusive: bool,
    /// Go `Range.HighExclude`.
    pub high_exclusive: bool,
}

/// One index of a [`KvTable`]: Go `model.IndexInfo`, reduced to what an index
/// write and a uniqueness check need.
#[derive(Clone, Debug)]
pub struct KvIndex {
    /// The index id (Go `IndexInfo.ID`), the `_i` key component.
    pub id: i64,
    /// The index name, which a duplicate-key error reports.
    pub name: String,
    /// Go `IndexInfo.Unique`.
    pub unique: bool,
    /// The indexed columns' offsets in the row, in index order.
    pub column_offsets: Vec<usize>,
    /// Go `!IndexInfo.Invisible`. An invisible index is maintained by every
    /// write and reported by `SHOW INDEX` exactly like any other, and is only
    /// hidden from the *planner*: it never becomes an access path, and naming
    /// it in `USE INDEX`/`FORCE INDEX` is Go's 1176 "Key ... doesn't exist".
    pub visible: bool,
}

/// What a parent-side mutation does to the rows that reference it: Go's
/// `ast.ReferOptionType` reduced to the three behaviours that differ.
///
/// `NO ACTION`, `SET DEFAULT` and a missing clause all collapse into
/// [`FkAction::Restrict`]. That is not an approximation: MySQL's InnoDB --
/// and TiDB after it -- never implemented `SET DEFAULT`, and `NO ACTION` is
/// not deferred to commit either, so all three reject the parent mutation
/// outright (re-confirmed via `gorun`, not assumed).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FkAction {
    /// Reject the parent mutation while a referencing row exists.
    #[default]
    Restrict,
    /// Delete the referencing rows, or repoint them at the new value.
    Cascade,
    /// Null out the referencing columns.
    SetNull,
}

/// One foreign key of a [`KvTable`]: Go `model.FKInfo`, reduced to what a
/// referential check and a cascade need.
///
/// The referencing side is stored as column OFFSETS because the constraint
/// lives on the table that declares it, so its own schema is fixed here; the
/// referenced side is stored as NAMES because the parent is resolved by name
/// at check time, exactly as Go resolves it through the information schema.
#[derive(Clone, Debug)]
pub struct KvForeignKey {
    /// The constraint name, which a violation reports.
    pub name: String,
    /// The referencing columns' offsets in this table's rows.
    pub cols: Vec<usize>,
    /// The referenced schema.
    pub ref_schema: String,
    /// The referenced table.
    pub ref_table: String,
    /// The referenced columns' names, in the same order as `cols`.
    pub ref_cols: Vec<String>,
    /// Go `FKInfo.OnDelete`.
    pub on_delete: FkAction,
    /// Go `FKInfo.OnUpdate`.
    pub on_update: FkAction,
}

/// A column of a [`KvTable`]: name, column id, and type.
#[derive(Clone, Debug)]
pub struct KvColumn {
    /// The column name.
    pub name: String,
    /// The column id (Go `ColumnInfo.ID`), the key of the row-format entries.
    pub id: i64,
    /// The column type.
    pub field_type: FieldType,
    /// Go `ColumnInfo.DefaultValue`: the value an omitted column takes.
    /// `None` means no `DEFAULT` was written, which is not the same as a
    /// `DEFAULT NULL`.
    pub default_value: Option<Datum>,
    /// Go `ColumnInfo.OriginDefaultValue`: what a row written BEFORE this
    /// column existed reads back as. `ADD COLUMN ... DEFAULT 7` gives the
    /// existing rows 7, not NULL, and the row bytes are never rewritten --
    /// the value is filled in on read.
    pub origin_default: Option<Datum>,
    /// Go `ColumnInfo.GeneratedExprString`/`GeneratedStored`: the column's
    /// value comes from an expression rather than from the row bytes. `None`
    /// is an ordinary column. See [`crate::generated_column`].
    pub generated: Option<crate::generated_column::GeneratedColumn>,
}

impl crate::generated_column::GeneratedColumnSlot for KvColumn {
    fn generation(&self) -> Option<&crate::generated_column::GeneratedColumn> {
        self.generated.as_ref()
    }

    fn column_type(&self) -> &FieldType {
        &self.field_type
    }
}

/// A table's effective character set and collation: what an unqualified
/// string column inherits, and what `SHOW CREATE TABLE` prints in its tail.
///
/// Go keeps this on `TableInfo.Charset`/`Collate`, resolved by
/// `ResolveCharsetCollation` from the table options, the schema's default and
/// finally the server default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableCharset {
    /// The table's default character set.
    pub charset: Charset,
    /// The table's default collation.
    pub collation: Collation,
}

impl Default for TableCharset {
    /// The server default this tier runs with: `utf8mb4` / `utf8mb4_bin`.
    ///
    /// Captured: `@@character_set_server` is `utf8mb4` and `@@collation_server`
    /// is `utf8mb4_bin` -- TiDB does NOT use MySQL 8's `utf8mb4_0900_ai_ci`.
    fn default() -> Self {
        Self {
            charset: Charset::Utf8Mb4,
            collation: Collation::DEFAULT,
        }
    }
}

/// Go `mysql.NotNullFlag`.
pub(crate) const NOT_NULL_FLAG: u32 = 1;

impl KvColumn {
    /// The value a row written before this column existed reads back.
    ///
    /// Go stores the written DEFAULT as given and casts it to the column's
    /// own type when a row reads it (`GetColOriginDefaultValue` ->
    /// `CastValue`), which is why `DECIMAL(6,2) DEFAULT 3.14159` reports
    /// `'3.14159'` in SHOW CREATE but reads back as `3.14`.
    pub(crate) fn origin_default_value(&self) -> Datum {
        let Some(value) = self.origin_default.clone() else {
            return Datum::Null;
        };
        match value.convert_to(&self.field_type, tidb_datatype::DEFAULT_STATEMENT_FLAGS) {
            Ok(converted) => converted.value,
            // A default the column cannot hold is refused at DDL time, so
            // this is unreachable for a table this tier built.
            Err(_) => value,
        }
    }
}
