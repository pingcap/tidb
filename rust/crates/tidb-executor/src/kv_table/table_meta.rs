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

use crate::ddl::index_prefix::UNSPECIFIED_LENGTH;
use std::cmp::Ordering;
use std::fmt;
use tidb_codec::table_key::RecordHandle;
use tidb_datatype::{Charset, Collation, ConversionFlags, Datum, FieldType, SessionTimeZone};

/// The statement-owned facts needed while stored row bytes become Datums.
///
/// Go hands row decoding the statement's expression/type context. Keeping
/// that context intact makes defaults, generated expressions, warnings,
/// SQL-mode error levels, and temporal values one statement-owned decision.
#[derive(Clone)]
pub struct RowDecodeContext {
    origin_default_flags: ConversionFlags,
    zone: SessionTimeZone,
    expression: crate::StmtContext,
}

impl RowDecodeContext {
    /// The pre-migration row-decoder contract: caller supplies only a zone,
    /// and origin defaults use `DEFAULT_STATEMENT_FLAGS`.
    ///
    /// Kept crate-private solely for the legacy compatibility wrappers while
    /// DML/FK/server callsites await explicit authorization to select their
    /// statement class. New production code must use a semantic constructor.
    #[must_use]
    pub(crate) fn legacy_default(zone: &SessionTimeZone) -> Self {
        Self {
            origin_default_flags: tidb_datatype::DEFAULT_STATEMENT_FLAGS,
            zone: zone.clone(),
            expression: crate::StmtContext::for_query().with_time_zone(zone.clone()),
        }
    }

    /// A SELECT/read-operator context. Go's SELECT arm always tolerates a
    /// zero-in-date and treats truncation as a warning.
    #[must_use]
    pub fn for_query(ctx: &crate::StmtContext) -> Self {
        Self {
            origin_default_flags: ctx.query_default_conversion_flags(),
            zone: ctx.session_zone(),
            expression: ctx.clone(),
        }
    }

    /// A DML/FK context. Reading an old row is part of the write statement,
    /// so its SQL-mode-dependent write flags decide the origin-default cast.
    #[must_use]
    pub fn for_write(ctx: &crate::StmtContext) -> Self {
        Self {
            origin_default_flags: ctx.write_conversion_flags(),
            zone: ctx.session_zone(),
            expression: ctx.clone(),
        }
    }

    /// A DDL reorg/backfill context. It uses the same CREATE/ALTER type flags
    /// that validated the default the schema change is now reading.
    #[must_use]
    pub fn for_ddl(ctx: &crate::StmtContext) -> Self {
        Self {
            origin_default_flags: ctx.reorg_default_conversion_flags(),
            zone: ctx.session_zone(),
            expression: ctx.clone(),
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
    pub fn for_analyze(ctx: &crate::StmtContext) -> Self {
        Self {
            origin_default_flags: ctx.show_default_conversion_flags(),
            zone: ctx.session_zone(),
            expression: ctx.clone(),
        }
    }

    /// The temporal location used by row codecs, index codecs, and generated
    /// columns while this row is decoded.
    #[must_use]
    pub(crate) fn zone(&self) -> &SessionTimeZone {
        &self.zone
    }

    /// The caller-class flags used only for an absent column's origin value.
    #[must_use]
    pub(crate) fn origin_default_flags(&self) -> ConversionFlags {
        self.origin_default_flags
    }

    pub(crate) fn expression(&self) -> &crate::StmtContext {
        &self.expression
    }

    /// Query-class UTC decoding for unit fixtures with no session.
    ///
    /// This is compiled only into `tidb-executor`'s own tests, so production
    /// code cannot use it as a convenience escape from choosing a caller
    /// class.
    #[cfg(test)]
    #[must_use]
    pub fn for_test_query_utc() -> Self {
        Self::for_query(&crate::StmtContext::for_query())
    }
}

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

impl IndexRange {
    /// Go `ranger.FullRange()`: `[NULL, +inf]` over the index's leading
    /// column, the range an `IndexFullScan` reads.
    ///
    /// The low bound is NULL rather than `MinNotNull` because an index stores
    /// its NULL entries too, and a full scan reads them.
    #[must_use]
    pub fn full() -> Self {
        IndexRange {
            low: vec![Datum::Null],
            high: vec![Datum::MaxValue],
            low_exclusive: false,
            high_exclusive: false,
        }
    }

    /// Whether this range is [`IndexRange::full`], which is what makes the
    /// read an `IndexFullScan` rather than an `IndexRangeScan` in `EXPLAIN`
    /// (Go prints no `range:` for a path whose ranges the ranger never
    /// narrowed).
    #[must_use]
    pub fn is_full(&self) -> bool {
        *self == IndexRange::full()
    }

    /// Go `ranger.Range.IsPoint`, with the caller's `RegardNULLAsPoint`
    /// statement-context policy made explicit.
    #[must_use]
    pub fn is_point(&self, regard_null_as_point: bool) -> bool {
        self.low.len() == self.high.len()
            && !self.low_exclusive
            && !self.high_exclusive
            && self.low.iter().zip(&self.high).all(|(low, high)| {
                !matches!(low, Datum::MinNotNull)
                    && !matches!(high, Datum::MaxValue)
                    && (regard_null_as_point || !matches!((low, high), (Datum::Null, Datum::Null)))
                    && compare_index_bound_datums(low, high) == Ordering::Equal
            })
    }

    /// Go `ranger.Range.IsFullRange`, including its integer-handle boundary
    /// spellings and the source rule that `[NULL, NULL]` is not full.
    #[must_use]
    pub fn is_full_range(&self, unsigned_int_handle: bool) -> bool {
        if unsigned_int_handle {
            return self.low.len() == 1
                && self.high.len() == 1
                && is_range_boundary(&self.low[0], true, true)
                && is_range_boundary(&self.high[0], true, false);
        }
        self.low.len() == self.high.len()
            && self.low.iter().zip(&self.high).all(|(low, high)| {
                let left_is_null = matches!(low, Datum::Null);
                let right_is_null = matches!(high, Datum::Null);
                (is_range_boundary(low, false, true) || left_is_null)
                    && (is_range_boundary(high, false, false) || right_is_null)
                    && !(left_is_null && right_is_null)
            })
    }

    /// Rust-representation equivalent of Go `ranger.Range.MemUsage`.
    ///
    /// The struct owns both tuple vector headers inline and every datum on
    /// their heaps. Rust does not store Go's collator-interface slice on each
    /// range; collation is typed into string datums and index metadata.
    #[must_use]
    pub fn estimated_memory_usage(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .low
                .iter()
                .chain(&self.high)
                .map(Datum::estimated_mem_usage)
                .sum::<usize>()
    }

    /// Go `ranger.Range.IntersectRange` for already typed index bounds.
    ///
    /// A shorter tuple is a prefix constraint. Before comparing it with a
    /// longer tuple, Go extends the missing suffix with `-inf` or `+inf`
    /// according to whether that endpoint is a lower/upper and open/closed
    /// bound. Preserving that rule is what makes a one-column point intersect
    /// a more granular `(a, b)` interval correctly.
    #[must_use]
    pub fn intersect(&self, other: &Self) -> Option<Self> {
        let other_is_more_granular = self.low.len() <= other.low.len();

        if compare_range_bounds(
            &self.low,
            &other.high,
            self.low_exclusive,
            other.high_exclusive,
            true,
            false,
        ) == Ordering::Greater
            || compare_range_bounds(
                &other.low,
                &self.high,
                other.low_exclusive,
                self.high_exclusive,
                true,
                false,
            ) == Ordering::Greater
        {
            return None;
        }

        let low_cmp = compare_range_bounds(
            &self.low,
            &other.low,
            self.low_exclusive,
            other.low_exclusive,
            true,
            true,
        );
        let (low, low_exclusive) = if low_cmp == Ordering::Less
            || (low_cmp == Ordering::Equal && other_is_more_granular)
        {
            (other.low.clone(), other.low_exclusive)
        } else {
            (self.low.clone(), self.low_exclusive)
        };

        let high_cmp = compare_range_bounds(
            &self.high,
            &other.high,
            self.high_exclusive,
            other.high_exclusive,
            false,
            false,
        );
        let (high, high_exclusive) = if high_cmp == Ordering::Greater
            || (high_cmp == Ordering::Equal && other_is_more_granular)
        {
            (other.high.clone(), other.high_exclusive)
        } else {
            (self.high.clone(), self.high_exclusive)
        };

        Some(Self {
            low,
            high,
            low_exclusive,
            high_exclusive,
        })
    }
}

impl fmt::Display for IndexRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&crate::plan_trace::range_text(self))
    }
}

/// Go `ranger.Ranges.MemUsage`.
#[must_use]
pub fn index_ranges_estimated_memory_usage(ranges: &[IndexRange]) -> usize {
    ranges.iter().map(IndexRange::estimated_memory_usage).sum()
}

/// Go `ranger.Ranges.IntersectRanges`, preserving left-major result order.
#[must_use]
pub fn intersect_index_ranges(left: &[IndexRange], right: &[IndexRange]) -> Vec<IndexRange> {
    let mut intersections = Vec::new();
    for left_range in left {
        for right_range in right {
            if let Some(intersection) = left_range.intersect(right_range) {
                intersections.push(intersection);
            }
        }
    }
    intersections
}

fn compare_range_bounds(
    left: &[Datum],
    right: &[Datum],
    left_open: bool,
    right_open: bool,
    left_is_low: bool,
    right_is_low: bool,
) -> Ordering {
    let length = left.len().max(right.len());
    let left = extend_range_bound(left, length, left_is_low, left_open);
    let right = extend_range_bound(right, length, right_is_low, right_open);

    for (left, right) in left.iter().zip(&right) {
        let ordering = compare_index_bound_datums(left, right);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    if !left_open && !right_open {
        Ordering::Equal
    } else if left_open == right_open {
        if left_is_low == right_is_low {
            Ordering::Equal
        } else if left_is_low {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    } else if left_open {
        if left_is_low {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    } else if right_is_low {
        Ordering::Less
    } else {
        Ordering::Greater
    }
}

fn extend_range_bound(bound: &[Datum], length: usize, is_low: bool, open: bool) -> Vec<Datum> {
    let mut extended = Vec::with_capacity(length);
    extended.extend_from_slice(bound);
    let suffix = match (is_low, open) {
        (true, true) | (false, false) => Datum::MaxValue,
        (true, false) | (false, true) => Datum::MinNotNull,
    };
    extended.resize(length, suffix);
    extended
}

fn is_range_boundary(value: &Datum, unsigned_int_handle: bool, left_side: bool) -> bool {
    match value {
        Datum::MinNotNull => left_side,
        Datum::MaxValue => !left_side,
        Datum::Int(value) => {
            (*value == i64::MIN && left_side) || (*value == i64::MAX && !left_side)
        }
        Datum::UInt(value) => {
            (*value == 0 && unsigned_int_handle && left_side) || (*value == u64::MAX && !left_side)
        }
        _ => false,
    }
}

fn compare_index_bound_datums(left: &Datum, right: &Datum) -> Ordering {
    let rank = |value: &Datum| match value {
        Datum::Null => 0,
        Datum::MinNotNull => 1,
        Datum::MaxValue => 3,
        _ => 2,
    };
    match rank(left).cmp(&rank(right)) {
        Ordering::Equal => {}
        ordering => return ordering,
    }
    if rank(left) != 2 {
        return Ordering::Equal;
    }
    let collation = left
        .collation()
        .or_else(|| right.collation())
        .unwrap_or(Collation::Binary);
    tidb_expr::compare_datums_with_collation(left, right, collation).unwrap_or(Ordering::Equal)
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
    /// Go `IndexColumn.Length`, one per entry of `column_offsets`:
    /// [`crate::ddl::index_prefix::UNSPECIFIED_LENGTH`] for a key part that
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
    /// This tier does not maintain such an index differently -- it has one
    /// physical entry set per table either way -- so the flag is carried for
    /// the one decision Go makes with it that is visible on the wire:
    /// `checkIndexLookUpPushDownSupported` (`planbuilder.go:1274`) refuses
    /// `INDEX_LOOKUP_PUSHDOWN` on a global index with 1815, because a
    /// coprocessor-local lookup cannot follow a handle out of its own region's
    /// partition. Always false on an unpartitioned table: Go's DDL records
    /// `GLOBAL` only where partitioning makes it mean something.
    pub global: bool,
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
/// BOTH sides are stored as NAMES (Go `FKInfo.Cols` / `RefCols`, both
/// `CIStr`). The referencing side used to be offsets on the theory that the
/// declaring table's own schema is fixed -- it is not: `ALTER TABLE ... ADD
/// COLUMN ... FIRST` shifts every offset above it, and a constraint that kept
/// its old ones silently starts checking the wrong columns.
#[derive(Clone, Debug)]
pub struct KvForeignKey {
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
pub(crate) const NOT_NULL_FLAG: u32 = 1;

impl KvColumn {
    /// The value a row written before this column existed reads back.
    ///
    /// Go stores the written DEFAULT as given and casts it to the column's
    /// own type when a row reads it (`GetColOriginDefaultValue` ->
    /// `CastValue`), which is why `DECIMAL(6,2) DEFAULT 3.14159` reports
    /// `'3.14159'` in SHOW CREATE but reads back as `3.14`.
    pub(crate) fn origin_default_value(
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
}
