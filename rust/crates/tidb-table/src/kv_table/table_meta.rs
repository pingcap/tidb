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
//! Inside: [`TableHandle`] (Go `kv.Handle`), [`KvIndex`],
//! [`FkAction`] + [`KvForeignKey`], [`KvColumn`] with the read-time cast that
//! makes `OriginDefaultValue` behave, and [`TableCharset`].
//!
//! Mirrors Go `pkg/meta/model`'s `IndexInfo`, `FKInfo` and `ColumnInfo`
//! together with `TableInfo.Charset`/`Collate`, plus `pkg/kv`'s `Handle`. The
//! operations that read and write these -- encode, decode, index maintenance,
//! scan -- stay in the parent module.

use tidb_datatype::UNSPECIFIED_LENGTH;
use tidb_tablecodec::table_key::RecordHandle;
use tidb_datatype::{Charset, Collation, ConversionFlags, Datum, FieldType, SessionTimeZone};

/// The statement-owned facts needed while stored row bytes become Datums.
///
/// Go hands row decoding the statement's expression/type context. Keeping
/// that context intact makes defaults, generated expressions, warnings,
/// SQL-mode error levels, and temporal values one statement-owned decision.
#[derive(Clone)]
pub struct RowDecodeContext {
    type_flags: ConversionFlags,
    zone: SessionTimeZone,
    expression: std::sync::Arc<dyn tidb_tblctx::MutateContext>,
}

/// The execute-time facts a cached prepared PointGet needs while decoding
/// ordinary projected columns.
///
/// The prepared-plan admission gate rejects generated columns, so this path
/// deliberately carries no expression context. Go likewise keeps the
/// immutable row-decoder shape on the cached point plan and supplies only the
/// current statement's type context when the executor is recreated.
#[derive(Clone)]
pub struct PreparedPointGetDecodeContext {
    origin_default_flags: ConversionFlags,
    zone: SessionTimeZone,
}

impl PreparedPointGetDecodeContext {
    /// Go `ResetContextOfStmt`'s SELECT flags plus this execution's session
    /// time zone.
    #[must_use]
    pub fn for_query(allow_invalid_dates: bool, zone: SessionTimeZone) -> Self {
        Self {
            origin_default_flags: tidb_datatype::DEFAULT_STATEMENT_FLAGS
                .with_truncate_as_warning(true)
                .with_ignore_zero_in_date_err(true)
                .with_ignore_invalid_date_err(allow_invalid_dates),
            zone,
        }
    }

    #[must_use]
    pub fn zone(&self) -> &SessionTimeZone {
        &self.zone
    }

    #[must_use]
    pub(crate) fn origin_default_flags(&self) -> ConversionFlags {
        self.origin_default_flags
    }
}

impl RowDecodeContext {

    /// A SELECT/read-operator context. Go's SELECT arm always tolerates a
    /// zero-in-date and treats truncation as a warning.
    #[must_use]
    pub fn for_query(ctx: &dyn tidb_tblctx::MutateContext) -> Self {
        Self {
            type_flags: ctx.query_default_conversion_flags(),
            zone: ctx.session_zone(),
            expression: ctx.to_shared(),
        }
    }

    /// A DML/FK context. Reading an old row is part of the write statement,
    /// so its SQL-mode-dependent write flags decide the origin-default cast.
    #[must_use]
    pub fn for_write(ctx: &dyn tidb_tblctx::MutateContext) -> Self {
        Self {
            type_flags: ctx
                .write_conversion_flags()
                .with_ignore_truncate_err(
                    tidb_expr::Columns::truncate_level(ctx) == tidb_expr::ErrorLevel::Ignore,
                )
                .with_truncate_as_warning(
                    tidb_expr::Columns::truncate_level(ctx) == tidb_expr::ErrorLevel::Warn,
                ),
            zone: ctx.session_zone(),
            expression: ctx.to_shared(),
        }
    }

    /// A DDL reorganization context with Go reorg flags and error levels.
    #[must_use]
    pub fn for_ddl(ctx: &dyn tidb_tblctx::MutateContext) -> Self {
        let ctx = ctx.for_reorg();
        Self {
            type_flags: ctx.reorg_default_conversion_flags(),
            zone: ctx.session_zone(),
            expression: ctx,
        }
    }

    /// An in-process ANALYZE context.
    ///
    /// Accepted Go `ResetContextOfStmt` has no ANALYZE switch arm, so ANALYZE
    /// takes the default arm: ignore truncation, ignore zero-in-date, and
    /// honor `ALLOW_INVALID_DATES`. That is exactly the flag set exposed as
    /// `show_default_conversion_flags`; this constructor names the distinct
    /// caller class instead of making ANALYZE pretend to be SHOW.
    #[must_use]
    pub fn for_analyze(ctx: &dyn tidb_tblctx::MutateContext) -> Self {
        Self {
            type_flags: ctx.show_default_conversion_flags(),
            zone: ctx.session_zone(),
            expression: ctx.to_shared(),
        }
    }

    /// A decode context of explicit parts.
    #[must_use]
    pub fn new(
        type_flags: ConversionFlags,
        zone: SessionTimeZone,
        expression: std::sync::Arc<dyn tidb_tblctx::MutateContext>,
    ) -> Self {
        Self {
            type_flags,
            zone,
            expression,
        }
    }

    /// The temporal location used by row codecs, index codecs, and generated
    /// columns while this row is decoded.
    #[must_use]
    pub fn zone(&self) -> &SessionTimeZone {
        &self.zone
    }

    /// The statement's processed-keys budget, which a reader executor
    /// installs on its own table copy.
    #[must_use]
    pub fn keys_read(&self) -> &tidb_kv::keys_read::KeysReadBudget {
        self.expression.keys_read()
    }

    /// The caller's type-context flags for defaults and column conversion.
    #[must_use]
    pub fn type_flags(&self) -> ConversionFlags {
        self.type_flags
    }

    /// Bind the table/task mode to expression conversion as well as key decoding.
    pub fn with_new_collation_enabled(mut self, enabled: bool) -> Self {
        if self.expression.new_collation_enabled() != enabled {
            self.expression = self.expression.with_new_collation_enabled(enabled);
        }
        self
    }

    pub fn expression(&self) -> &dyn tidb_tblctx::MutateContext {
        self.expression.as_ref()
    }

}

/// Go `kv.Handle`: the row identifier a record key encodes.
///
/// An integer handle comes from a single-column integer primary key (or the
/// allocated `_tidb_rowid` when the table has none); a common handle is the
/// codec encoding of a clustered primary key's columns, which is what makes a
/// string or multi-column primary key clustered.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TableHandle {
    /// Go `kv.IntHandle`.
    Int(i64),
    /// Go `kv.CommonHandle`, holding the encoded key datums.
    Common(Vec<u8>),
}

impl TableHandle {
    /// The record-key component this handle contributes.
    pub fn record_handle(&self) -> RecordHandle {
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

/// One index of a [`KvTable`]: Go `model.IndexInfo`, reduced to what an index
/// write and a uniqueness check need.
#[derive(Clone, Debug)]
pub struct KvIndex {
    /// The index id (Go `IndexInfo.ID`), the `_i` key component.
    pub id: i64,
    /// The index name, which a duplicate-key error reports.
    pub name: String,
    /// Go `IndexInfo.Comment`, preserved verbatim in metadata and escaped
    /// only when a SQL renderer emits it.
    pub comment: String,
    /// Go `IndexInfo.Unique`.
    pub unique: bool,
    /// The indexed columns' offsets in the row, in index order.
    pub column_offsets: Vec<usize>,
    /// Go `IndexColumn.Length`, one per entry of `column_offsets`:
    /// [`tidb_datatype::UNSPECIFIED_LENGTH`] for a key part that
    /// stores the whole column, and the declared prefix otherwise.
    ///
    /// A prefix key part changes what the index MEANS, not just how large it
    /// is: the entry holds `'abc'` where the row holds `'abcdef'`, so the
    /// index no longer covers that column ([`KvIndex::covers`]), no longer
    /// orders by it ([`KvIndex::ordered_column_offsets`]), and can no longer
    /// answer a point get ([`KvIndex::has_prefix`]).
    pub prefix_lengths: Vec<i64>,
    /// Go `!IndexInfo.Invisible`. An invisible index is maintained by every
    /// write and reported by `SHOW INDEX` exactly like any other, and is only
    /// hidden from the *planner*: it never becomes an access path, and naming
    /// it in `USE INDEX`/`FORCE INDEX` is Go's 1176 "Key ... doesn't exist".
    pub visible: bool,
    /// Go `IndexInfo.Global`: a `GLOBAL` index of a PARTITIONED table, whose
    /// single entry set spans every partition instead of living inside one.
    ///
    /// Its keys live under the logical table id and each value carries the
    /// physical partition id. The flag also drives Go's visible hint rule:
    /// `checkIndexLookUpPushDownSupported` (`planbuilder.go:1274`) refuses
    /// `INDEX_LOOKUP_PUSHDOWN` on a global index with 1815, because a
    /// coprocessor-local lookup cannot follow a handle out of its own region's
    /// partition. Always false on an unpartitioned table: Go's DDL records
    /// `GLOBAL` only where partitioning makes it mean something.
    pub global: bool,
    /// Go `IndexInfo.GlobalIndexVersion`, selecting whether a non-distinct
    /// global-index key also carries its partition id.
    pub global_index_version: u8,
    /// Whether this entry IS the clustered record handle's PRIMARY key
    /// (Go keeps the IndexInfo in metadata but never maintains `_i` entries
    /// for it). Write paths skip it; the planner still reads its stats so
    /// multi-column join NDV estimation sees the composite key.
    pub clustered_primary: bool,
    /// Go `IndexInfo.Primary`: the PRIMARY KEY's index, clustered or not. A
    /// legacy secondary index may be called `primary` (TiDB #14243), so the
    /// name never stands in for this flag.
    pub primary: bool,
}

/// An index Go's DDL is still adding or dropping (an `IndexInfo` in a
/// state other than public and `StateNone`). Only the write path sees it:
/// Go's `DeletableIndices` delete from every such index and
/// `IsIndexWritable` ones are written, each through
/// `GenTempIdxKeyByState`; no reader, planner or SHOW statement sees it.
#[derive(Clone, Debug)]
pub(crate) struct NonPublicIndex {
    /// The index as a write builds its keys and values.
    pub index: KvIndex,
    /// Go `IndexInfo.State`.
    pub state: tidb_model::SchemaState,
    /// Go `IndexInfo.BackfillState`: whether DML writes go to the
    /// temporary index, to both, or (inapplicable) to the index itself.
    pub backfill_state: tidb_model::BackfillState,
}

/// A column Go's DDL is adding, dropping or changing (a `ColumnInfo` in a
/// state other than public and `StateNone`). Only the write path sees it, as
/// Go's `WritableCols` / `DeletableCols` extend `Cols`: an INSERT writes its
/// original default (or, for a changing column, the cast of the column it
/// replaces), an UPDATE keeps what the row holds, and index entries over it
/// follow its index's state.
#[derive(Clone, Debug)]
pub struct NonPublicColumn {
    /// The column as a write encodes it.
    pub column: KvColumn,
    /// Go `ColumnInfo.State`.
    pub state: tidb_model::SchemaState,
    /// Go `ChangeStateInfo.DependencyColumnOffset`: for the temporary column
    /// a MODIFY COLUMN writes, the public column whose value it casts.
    pub dependency: Option<usize>,
}

impl NonPublicColumn {
    /// Go `col.State != StateDeleteOnly && != StateDeleteReorganization`:
    /// DML writes its value.
    #[must_use]
    pub(crate) fn is_writable(&self) -> bool {
        self.state != tidb_model::SchemaState::DELETE_ONLY
            && self.state != tidb_model::SchemaState::DELETE_REORGANIZATION
    }
}

impl NonPublicIndex {
    /// Go `IsIndexWritable`: every state except the two delete-only ones.
    #[must_use]
    pub(crate) fn is_writable(&self) -> bool {
        self.state != tidb_model::SchemaState::DELETE_ONLY
            && self.state != tidb_model::SchemaState::DELETE_REORGANIZATION
    }
}

impl KvIndex {
    /// The prefix declared on the key part at `position`, or
    /// [`UNSPECIFIED_LENGTH`] when the key part stores the whole column.
    ///
    /// A missing entry reads as "whole column", so an index built before this
    /// field existed cannot be mistaken for a prefix one.
    #[must_use]
    pub fn prefix_length(&self, position: usize) -> i64 {
        self.prefix_lengths
            .get(position)
            .copied()
            .unwrap_or(UNSPECIFIED_LENGTH)
    }

    /// Go `IndexInfo.HasPrefixIndex`: any key part stores less than its whole
    /// column.
    ///
    /// Go consults this to REFUSE a plan shape outright -- `PointGetPlan`
    /// declines such an index (`pkg/planner/core/point_get_plan.go`), because
    /// an entry found by a prefix does not prove the row matches.
    #[must_use]
    pub fn has_prefix(&self) -> bool {
        self.prefix_lengths
            .iter()
            .any(|length| *length != UNSPECIFIED_LENGTH)
    }

    /// The leading key parts whose order is the COLUMN's order, which is the
    /// only order an index scan can hand to an `ORDER BY`.
    ///
    /// Go reaches the same answer from the other side: `matchIndicesProp`
    /// (`pkg/planner/core/operator/logicalop/logical_index_scan.go`) rejects
    /// the property as soon as one sort item lands on a key part with a
    /// declared length. Cutting the list here rather than testing each item
    /// makes that unrepresentable: entries beyond the cut never reach a
    /// comparison at all. Captured from real TiDB: `select a from t order by
    /// a` over `key idx(a(3))` plans `Sort` over `TableFullScan`, not an
    /// ordered index read.
    #[must_use]
    pub fn ordered_column_offsets(&self) -> &[usize] {
        let ordered = self
            .prefix_lengths
            .iter()
            .take_while(|length| **length == UNSPECIFIED_LENGTH)
            .count()
            .min(self.column_offsets.len());
        // An index with no recorded lengths stores every column whole.
        if self.prefix_lengths.is_empty() {
            return &self.column_offsets;
        }
        &self.column_offsets[..ordered]
    }

    /// Whether this index stores enough of the column at `offset` to ANSWER a
    /// read of it, which is Go's `isIndexColsCoveringCol`
    /// (`pkg/planner/core/operator/logicalop/logical_datasource.go`): the key
    /// part must have no declared length, or a length that already reaches
    /// the column's own `Flen`.
    ///
    /// `column_flen` is the indexed column's `FieldType::flen`. A key part
    /// that stores less than that holds a CUT value, and answering from it
    /// returns `'abc'` where the row holds `'abcdef'`.
    #[must_use]
    pub fn covers(&self, offset: usize, column_flen: i64) -> bool {
        self.column_offsets
            .iter()
            .enumerate()
            .any(|(position, indexed)| {
                *indexed == offset && {
                    let length = self.prefix_length(position);
                    length == UNSPECIFIED_LENGTH || length == column_flen
                }
            })
    }
}

/// Go `ast.ReferOptionType`: both its exact metadata spelling and the parent
/// mutation behavior TiDB gives that spelling.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FkAction {
    /// No clause was supplied. It behaves as `RESTRICT`, while SHOW omits it
    /// and `REFERENTIAL_CONSTRAINTS` reports `NO ACTION`.
    #[default]
    NoOption,
    /// Reject the parent mutation while a referencing row exists.
    Restrict,
    /// Delete the referencing rows, or repoint them at the new value.
    Cascade,
    /// Null out the referencing columns.
    SetNull,
    /// TiDB stores this spelling but, like MySQL, does not defer the check.
    NoAction,
    /// TiDB stores this spelling but does not implement default propagation.
    SetDefault,
}

impl FkAction {
}

/// One foreign key of a [`KvTable`]: Go `model.FKInfo`, reduced to what a
/// referential check and a cascade need.
///
/// BOTH sides are stored as NAMES (Go `FKInfo.Cols` / `RefCols`, both
/// `CIStr`). The referencing side used to be offsets on the theory that the
/// declaring table's own schema is fixed -- it is not: `ALTER TABLE ... ADD
/// COLUMN ... FIRST` shifts every offset above it, and a constraint that kept
/// its old ones silently starts checking the wrong columns.
#[derive(Clone, Debug)]
pub struct KvForeignKey {
    /// Go `FKInfo.Version`: version zero retains metadata without active enforcement.
    pub version: i64,
    /// The constraint name, which a violation reports.
    pub name: String,
    /// Go `FKInfo.Cols`: the referencing columns' NAMES in this table.
    /// Resolved to offsets against the current column list where they are
    /// used ([`KvTable::foreign_key_offsets`]).
    pub cols: Vec<String>,
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
    /// Go `ColumnInfo.Version`: literal TIMESTAMP defaults written by v0 are
    /// system-local wall clocks, while v1 and later persist UTC wall clocks.
    pub column_info_version: u64,
    /// Go `ColumnInfo.DefaultValue` + `DefaultIsExpr`: where the value of an
    /// omitted column comes from. `None` means no `DEFAULT` was written,
    /// which is not the same as a `DEFAULT NULL`. See
    /// [`crate::column_default`] for the computed forms.
    pub default_value: Option<crate::column_default::ColumnDefault>,
    /// Go `ColumnInfo.OriginDefaultValue`: what a row written BEFORE this
    /// column existed reads back as. `ADD COLUMN ... DEFAULT 7` gives the
    /// existing rows 7, not NULL, and the row bytes are never rewritten --
    /// the value is filled in on read.
    pub origin_default: Option<Datum>,
    /// Go `ColumnInfo.Comment`: what `SHOW CREATE TABLE` prints after the
    /// column and what `information_schema.columns.column_comment` reports.
    /// Empty when the column was declared without one.
    pub comment: String,
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

    fn column_name(&self) -> &str {
        &self.name
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
pub const NOT_NULL_FLAG: u32 = 1;

impl KvColumn {
    /// The value a row written before this column existed reads back.
    ///
    /// Go stores the written DEFAULT as given and casts it to the column's
    /// own type when a row reads it (`GetColOriginDefaultValue` ->
    /// `CastValue`), which is why `DECIMAL(6,2) DEFAULT 3.14159` reports
    /// `'3.14159'` in SHOW CREATE but reads back as `3.14`.
    pub fn origin_default_value(
        &self,
        flags: ConversionFlags,
        zone: &SessionTimeZone,
    ) -> Result<Datum, tidb_datatype::DatumValueError> {
        let Some(value) = self.origin_default.clone() else {
            return Ok(Datum::Null);
        };
        crate::column_default::materialize_stored_literal(
            &value,
            &self.field_type,
            self.column_info_version,
            flags,
            zone,
        )
        .map(|converted| converted.value)
    }

    /// The column as Go's infoschema holds it once the DDL that built it
    /// returns: the job carries the `ColumnInfo` through `encoding/json`, the
    /// owner writes it to meta the same way, and `infoschema.Builder` reads
    /// it back, so every Go string of the column -- the `FieldType`'s
    /// `Elems`, `DefaultValue`, `OriginDefaultValue`, `Comment` -- has each
    /// byte that is not UTF-8 replaced by U+FFFD (`encodeState.string`). The
    /// DDL validates against the raw bytes first (a latin1 client's
    /// `ENUM('d\xE9') DEFAULT 'd\xE9'` is admitted), and what it stores is
    /// the replaced form. A BIT default is the exception: Go keeps it in the
    /// base64 `DefaultValueBit`/`OriginDefaultValueBit`, which round-trip
    /// exactly. A LOCAL temporary table never reaches meta and must not be
    /// passed through here.
    pub fn reload_from_meta_json(&mut self) {
        self.field_type = FieldType::from_json(
            &self
                .field_type
                .to_json()
                .expect("a column FieldType always encodes as Go JSON"),
        )
        .expect("Go JSON a FieldType encoded decodes back");
        let bit = self.field_type.code() == tidb_datatype::FieldTypeCode::Bit;
        if !bit {
            if let Some(crate::column_default::ColumnDefault::Value(value)) =
                &mut self.default_value
            {
                *value = go_json_string_round_trip(std::mem::take(value));
            }
            if let Some(value) = &mut self.origin_default {
                *value = go_json_string_round_trip(std::mem::take(value));
            }
        }
        go_json_text_round_trip(&mut self.comment);
        // `Name` and the expression strings are Go strings of the
        // `ColumnInfo` too. The generated expression's dependency names are
        // this table's column names, replaced alike.
        go_json_text_round_trip(&mut self.name);
        if let Some(generated) = &mut self.generated {
            go_json_text_round_trip(&mut generated.expr_text);
            generated
                .dependencies
                .iter_mut()
                .for_each(go_json_text_round_trip);
        }
        if let Some(crate::column_default::ColumnDefault::Computed(computed)) =
            &mut self.default_value
        {
            go_json_text_round_trip(&mut computed.text);
        }
    }
}

/// A Go string after `encoding/json`: each byte that is not UTF-8 (carried
/// as a `tidb_hack::raw_bytes` escape) comes back as U+FFFD.
pub fn go_json_text_round_trip(text: &mut String) {
    if tidb_hack::raw_bytes::contains_escapes(text) {
        *text =
            tidb_datatype::decode_go_utf8_lossy(tidb_hack::raw_bytes::str_to_bytes(text).as_ref());
    }
}

/// A stored default after `encoding/json`: Go keeps a column default as a
/// Go string, so its bytes come back with each byte that is not UTF-8 read
/// as U+FFFD; a value of any other kind has no string to replace.
fn go_json_string_round_trip(value: Datum) -> Datum {
    let replaced = |bytes: &[u8]| tidb_datatype::decode_go_utf8_lossy(bytes).into_bytes();
    match value {
        Datum::String(string) => {
            Datum::new_collation_string(replaced(string.bytes()), string.collation())
        }
        Datum::Bytes(bytes) => Datum::Bytes(replaced(&bytes)),
        Datum::Enum(member, collation) => Datum::Enum(
            tidb_datatype::MysqlEnum::new(replaced(member.name_bytes()), member.value()),
            collation,
        ),
        Datum::Set(members, collation) => Datum::Set(
            tidb_datatype::MysqlSet::new(replaced(members.name_bytes()), members.value()),
            collation,
        ),
        other => other,
    }
}


