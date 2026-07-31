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

//! The range of a table's CLUSTERED INTEGER HANDLE that a `WHERE` implies:
//! what turns Go's `TableFullScan` into its `TableRangeScan`.
//!
//! Mirrors Go `pkg/planner/core/stats.go`'s `deriveTablePathStats`, which is
//! three steps this module keeps in the same order:
//!
//! 1. `ranger.BuildTableRange` builds the ranges. Go's builder is
//!    `buildColumnRange(..., tableRange: true)` -- the SAME point algebra as
//!    the index detacher, over one column -- so this module calls
//!    [`crate::index_range::detach_cond_and_build_range_for_index`] with the
//!    primary key as a one-column index rather than growing a second ranger.
//!    There is exactly one range algebra in this crate and this is a caller
//!    of it.
//! 2. `points2TableRanges` then replaces the open endpoints with the handle
//!    domain's own extremes ([`to_table_range`]), which is what makes the
//!    range encodable as a key interval.
//! 3. `cardinality.GetRowCountByColumnRanges` with `pkIsHandle` estimates it.
//!
//! # The two row counts, and which one `EXPLAIN` prints
//!
//! Go computes the handle range's row count TWICE, from ranges of different
//! shapes, and the numbers differ. `deriveTablePathStats` estimates the
//! CONVERTED ranges of step 2, whose low bound is a real `KindInt64`
//! `math.MinInt64`; that is `path.CountAfterAccess`, which costing and
//! skyline pruning read. `DeriveStats`/`Selectivity` estimates the
//! UNCONVERTED column ranges, whose open low bound is still `KindMinNotNull`;
//! that is `ds.StatsInfo().RowCount`, and it is what `EXPLAIN` prints on the
//! scan node.
//!
//! The difference is not cosmetic, because `GetRowCountByColumnRanges`
//! dispatches on exactly that kind: `KindInt64` takes the SIGNED pseudo
//! estimator, anything else takes the UNSIGNED one. Instrumenting Go's own
//! dispatch on `sbtest1(id bigint primary key, ...)` with no statistics shows
//! both calls, in this order:
//!
//! ```text
//!   explain select c from sbtest1 where id < -1
//!     ranges=[[-inf,-1)] lowKind=KindMinNotNull  -> unsigned -> 10000.00
//!     ranges=[[-inf,-1)] lowKind=KindInt64       -> signed   ->  3333.33
//!     printed: TableRangeScan_8  10000.00  range:[-inf,-1)
//! ```
//!
//! The unsigned arm reads `-1`'s bits as `u64::MAX`, so `[-inf,-1)` becomes
//! the whole unsigned domain and estimates the whole table; the signed arm
//! reads the same range as a third of it. `id < -1` and `id <= -1` are the
//! only shapes in the captured corpus where the two disagree, which is why
//! taking either one for both purposes would have looked right almost
//! everywhere. Both are computed here, from the same estimator, by handing it
//! the ranges of the step Go hands it.

use crate::access_cost::realtime_row_count;
use crate::access_cost::TableStatistics;
use crate::index_range::IndexRanges;
use crate::kv_table::{IndexRange, KvTable};
use tidb_datatype::{Datum, FieldTypeCode};
use tidb_distsql::{signed_handle_ranges_to_kv_ranges, SignedHandleRange};
use tidb_planner::cardinality::row_count_estimator::{
    get_row_count_by_column_ranges, ColumnRange, EstimatorOptions,
};
use tidb_txnkv::Key;

/// The ranges a `WHERE` implies over `table`'s clustered integer handle, or
/// `None` when this tier builds none.
///
/// `None` is Go's full range, and the caller reads the whole table for it.
/// The refusals are:
///
/// * a table with no integer primary-key handle -- a `_tidb_rowid` table has
///   no handle a `WHERE` can name, and a common (clustered non-integer)
///   handle is a multi-column key whose ranges this tier does not encode;
/// * an UNSIGNED handle. The record key encodes a handle with the SIGNED
///   integer codec, so an unsigned value above `i64::MAX` encodes as a
///   negative key and a range over it is not the interval its bounds read
///   like. Go handles this through `points2TableRanges`' unsigned domain;
///   refusing is a lost optimization on such tables, never a wrong answer,
///   because the `WHERE` above the source still filters every row;
/// * a `WHERE` that constrains the handle with nothing the ranger can use.
pub(crate) fn build_handle_ranges<'a>(
    table: &KvTable,
    where_clause: &'a tidb_ast::Expr,
) -> Option<IndexRanges<'a>> {
    let column = handle_column(table)?;
    let built = crate::index_range::detach_cond_and_build_range_for_index(
        &[(column.name.clone(), column.field_type.clone())],
        where_clause,
    )?;
    Some(built)
}

/// The primary-key column that IS the row handle, when the table has one and
/// this tier can range over it.
fn handle_column(table: &KvTable) -> Option<&crate::kv_table::KvColumn> {
    let column = table.columns.get(table.pk_handle_offset()?)?;
    if column.field_type.is_unsigned() {
        return None;
    }
    // The handle codec is integer; a clustered non-integer key reaches this
    // tier as a common handle, which `pk_handle_offset` already excludes.
    matches!(
        column.field_type.code(),
        FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
    )
    .then_some(column)
}

/// Go `points2TableRanges`' conversion: an open endpoint becomes the handle
/// domain's own extreme, so every bound is a real integer.
///
/// Go converts a NULL low bound to the minimum INCLUSIVE (`startPoint.excl =
/// false`) as well; a `NOT NULL` handle never carries one, but the rule is
/// ported rather than assumed away.
fn to_table_range(range: &IndexRange) -> (i64, i64, bool, bool) {
    let low = range.low.first();
    let high = range.high.first();
    let (low_value, low_exclusive) = match low {
        Some(Datum::Null) => (i64::MIN, false),
        Some(Datum::MinNotNull) | None => (i64::MIN, range.low_exclusive),
        Some(Datum::Int(value)) => (*value, range.low_exclusive),
        Some(Datum::UInt(value)) => (*value as i64, range.low_exclusive),
        _ => (i64::MIN, range.low_exclusive),
    };
    let (high_value, high_exclusive) = match high {
        Some(Datum::MaxValue) | None => (i64::MAX, range.high_exclusive),
        Some(Datum::Int(value)) => (*value, range.high_exclusive),
        Some(Datum::UInt(value)) => (*value as i64, range.high_exclusive),
        _ => (i64::MAX, range.high_exclusive),
    };
    (low_value, high_value, low_exclusive, high_exclusive)
}

/// One range as the estimator's column range, in the shape of whichever of
/// Go's two calls the caller is standing in for (see the module doc).
fn column_range(range: &IndexRange, converted: bool) -> ColumnRange {
    if !converted {
        return ColumnRange {
            low: range.low.first().cloned().unwrap_or(Datum::MinNotNull),
            high: range.high.first().cloned().unwrap_or(Datum::MaxValue),
            low_exclude: range.low_exclusive,
            high_exclude: range.high_exclusive,
        };
    }
    let (low, high, low_exclude, high_exclude) = to_table_range(range);
    ColumnRange {
        low: Datum::Int(low),
        high: Datum::Int(high),
        low_exclude,
        high_exclude,
    }
}

/// Go `cardinality.GetRowCountByColumnRanges` with `pkIsHandle` over the
/// handle ranges.
///
/// `converted` picks which of Go's two calls this is: `false` is
/// `Selectivity`'s, over the unconverted column ranges, which is the number
/// `EXPLAIN` prints; `true` is `deriveTablePathStats`', over the
/// table-converted ranges, which is `CountAfterAccess`. The module doc has
/// the capture that separates them.
pub(crate) fn handle_range_row_count(
    table: &KvTable,
    ranges: &[IndexRange],
    stats: Option<&TableStatistics>,
    converted: bool,
) -> f64 {
    let realtime = realtime_row_count(stats);
    let Some(column) = handle_column(table) else {
        return realtime;
    };
    let column_ranges: Vec<ColumnRange> = ranges
        .iter()
        .map(|range| column_range(range, converted))
        .collect();
    get_row_count_by_column_ranges(
        stats.and_then(|stats| stats.columns.get(&column.id)),
        &column_ranges,
        column.field_type.collation(),
        realtime as i64,
        stats.map_or(0, |stats| stats.modify_count),
        true,
        EstimatorOptions::default(),
    )
    .est
}

/// The record-key intervals `ranges` cover, as the storage seam's half-open
/// `[start, end)` pairs in ascending key order.
///
/// A partitioned table's rows live under its PARTITIONS' ids, so a handle
/// range there is one key interval PER partition; partition-major order keeps
/// the result ascending, because the ids are allocated as one ascending block
/// and every handle of a partition sorts inside it.
///
/// `None` means a bound this tier cannot encode, and the caller falls back to
/// the whole record range -- reading a superset is always correct, because
/// the `WHERE` above the source filters every row it returns.
pub(crate) fn record_key_ranges(table: &KvTable, ranges: &[IndexRange]) -> Option<Vec<(Key, Key)>> {
    let ids = table
        .partition()
        .map_or_else(|| vec![table.table_id], |p| p.physical_ids());
    let mut handle_ranges = Vec::with_capacity(ranges.len());
    for range in ranges {
        let (low, high, low_exclusive, high_exclusive) = to_table_range(range);
        // An empty range is not an error: `id > 100 AND id < 100` admits no
        // row, and Go plans a `TableDual` for it. The caller reads nothing.
        let Ok(handle_range) = SignedHandleRange::new(low, high, low_exclusive, high_exclusive)
        else {
            continue;
        };
        handle_ranges.push(handle_range);
    }
    let mut key_ranges = Vec::with_capacity(handle_ranges.len() * ids.len());
    for id in ids {
        for encoded in signed_handle_ranges_to_kv_ranges(id, &handle_ranges) {
            key_ranges.push((encoded.start_key, encoded.end_key));
        }
    }
    Some(key_ranges)
}
