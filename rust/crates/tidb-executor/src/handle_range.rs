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

//! The range of a table's CLUSTERED HANDLE that a `WHERE` implies: what
//! turns Go's `TableFullScan` into its `TableRangeScan`.
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
//! # Why the conversion of step 2 decides the number, and the one step that
//! is NOT ported
//!
//! `GetRowCountByColumnRanges` dispatches on the FIRST range's low-bound
//! KIND: `KindInt64` takes the SIGNED pseudo estimator, anything else takes
//! the UNSIGNED one, which reads the bound's BITS as a `uint64`. Step 2 is
//! what settles that kind for this call -- after `points2TableRanges` an open
//! low bound is a real `KindInt64` `math.MinInt64`, so a table path always
//! takes the signed arm. `CountAfterAccess` is that number, and it is what
//! `EXPLAIN` prints on the scan node (`convertToTableScan` gives the
//! `PhysicalTableScan` its stats from `path.CountAfterAccess`).
//!
//! Go calls the same estimator a SECOND time, from `Selectivity`, over the
//! UNCONVERTED column ranges whose open low bound is still `KindMinNotNull`
//! -- and that call takes the unsigned arm. Instrumenting Go's own dispatch
//! on `sbtest1(id bigint primary key, ...)` with no statistics shows both,
//! in this order:
//!
//! ```text
//!   explain select c from sbtest1 where id < -1
//!     ranges=[[-inf,-1)] lowKind=KindMinNotNull  -> unsigned -> 10000.00
//!     ranges=[[-inf,-1)] lowKind=KindInt64       -> signed   ->  3333.33
//!     printed: TableRangeScan_8  10000.00
//! ```
//!
//! That second number is `ds.StatsInfo().RowCount`, and it reaches the scan
//! through `adjustCountAfterAccess`, which RAISES `CountAfterAccess` to
//! `RowCount / SelectionFactor` when the path's own estimate falls below it.
//! `id < -1` is exactly that case: the unsigned arm reads `-1`'s bits as
//! `u64::MAX`, calls `[-inf,-1)` the whole domain, and the adjustment lifts
//! the printed 3333.33 to 10000.00.
//!
//! [`crate::access_cost::adjust_count_after_access`] applies that lower bound
//! after the complete predicate's data-source estimate is available. Keeping
//! the converted-range estimate and the data-source estimate separate is
//! load-bearing: the former remains 3333.33 while the physical scan prints
//! Go's adjusted 10000.00.

use crate::access_cost::realtime_row_count;
use crate::access_cost::TableStatistics;
use crate::index_range::IndexRanges;
use crate::kv_table::{IndexRange, KvIndex, KvTable};
use tidb_datatype::{Datum, FieldTypeCode};
use tidb_distsql::{signed_handle_ranges_to_kv_ranges, SignedHandleRange};
use tidb_planner::cardinality::row_count_estimator::{
    get_row_count_by_column_ranges, ColumnRange, EstimatorOptions,
};
use tidb_txnkv::Key;

/// The ranges a `WHERE` implies over `table`'s clustered handle, or `None`
/// when this tier builds none.
///
/// `None` is Go's full range, and the caller reads the whole table for it.
/// The refusals are:
///
/// * a table with no primary-key handle -- a `_tidb_rowid` table has no
///   handle a `WHERE` can name;
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
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<IndexRanges<'a>> {
    if !table.common_handle_offsets().is_empty() {
        return build_common_handle_ranges(table, where_clause, zone);
    }
    let column = handle_column(table)?;
    let mut built = crate::index_range::detach_cond_and_build_range_for_index(
        // The clustered handle stores the whole column: a row identifier has
        // no declared prefix to cut to.
        &[crate::index_range::RangeColumn::whole(
            column.name.clone(),
            column.field_type.clone(),
        )],
        where_clause,
        zone,
    )?;
    // Go `points2TableRanges` (`pkg/util/ranger/ranger.go:466`) calls
    // `convertPointsInPlace` with `skipNull = true`, and that function DROPS
    // any interval whose END point is still `KindNull` (`:102-104`) -- while
    // converting a NULL START point to the domain minimum, INCLUSIVE
    // (`:85-88`), which is what [`to_table_range`] does below.
    //
    // The asymmetry is the whole point: a row handle is never NULL, so an
    // interval that ENDS at NULL selects nothing and Go removes it, leaving
    // zero ranges and a `TableDual`. Captured:
    //
    // ```text
    // explain select * from t where id is null    TableDual_6 | 0.00 | rows:0
    // explain select * from t where id <=> null   TableDual_5 | 1.00 | rows:0
    // ```
    //
    // Without this the `[NULL, NULL]` pair became `[MinInt64, MaxInt64]` and
    // the plan was a FULL TABLE SCAN -- the right rows (the `WHERE` above
    // still filters), read the most expensive possible way.
    built
        .ranges
        .retain(|range| range.high.first() != Some(&Datum::Null));
    Some(built)
}

/// The PRIMARY index whose columns are the common row handle.
///
/// TiDB exposes this metadata as an index for range building and statistics,
/// but physically reads its ranges from the table's `_r<common_handle>`
/// record keys. It is therefore a table path, not a secondary-index path.
pub(crate) fn common_handle_primary(table: &KvTable) -> Option<&KvIndex> {
    table.plan_indexes().find(|index| {
        index.name.eq_ignore_ascii_case("PRIMARY")
            && index.column_offsets == table.common_handle_offsets()
    })
}

fn build_common_handle_ranges<'a>(
    table: &KvTable,
    where_clause: &'a tidb_ast::Expr,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<IndexRanges<'a>> {
    let index = common_handle_primary(table)?;
    let columns: Vec<crate::index_range::RangeColumn> = index
        .column_offsets
        .iter()
        .enumerate()
        .filter_map(|(position, offset)| {
            let column = table.columns.get(*offset)?;
            Some(crate::index_range::RangeColumn {
                name: column.name.clone(),
                field_type: column.field_type.clone(),
                prefix_len: index.prefix_length(position),
            })
        })
        .collect();
    if columns.len() != index.column_offsets.len() {
        return None;
    }
    crate::index_range::detach_cond_and_build_range_for_index(&columns, where_clause, zone)
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
/// A NULL LOW bound becomes the minimum INCLUSIVE (Go `startPoint.excl =
/// false`). A NULL HIGH bound cannot reach here at all -- the `skipNull` filter
/// in [`build_handle_ranges`] has already dropped that whole interval -- so the
/// catch-all high arm below is a total-match requirement, not a rule.
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

/// One range as the estimator's column range, after step 2's conversion --
/// which is what puts a real `KindInt64` in the low bound, and so decides
/// which arm of the estimator runs (see the module doc).
fn column_range(range: &IndexRange) -> ColumnRange {
    let (low, high, low_exclude, high_exclude) = to_table_range(range);
    ColumnRange {
        low: Datum::Int(low),
        high: Datum::Int(high),
        low_exclude,
        high_exclude,
    }
}

/// Go `deriveTablePathStats`' `path.CountAfterAccess`:
/// `cardinality.GetRowCountByColumnRanges` with `pkIsHandle` over the
/// table-converted handle ranges.
///
/// This is the number `EXPLAIN` prints on the scan node AND the number the
/// path is costed at; the module doc explains why they are the same here and
/// what the one un-ported step (`adjustCountAfterAccess`) would change.
pub(crate) fn handle_range_row_count(
    table: &KvTable,
    ranges: &[IndexRange],
    stats: Option<&TableStatistics>,
) -> f64 {
    let realtime = realtime_row_count(stats);
    if let Some(index) = common_handle_primary(table) {
        return crate::access_cost::index_range_row_count(index, table, ranges, stats, realtime);
    }
    let Some(column) = handle_column(table) else {
        return realtime;
    };
    let column_ranges: Vec<ColumnRange> = ranges.iter().map(column_range).collect();
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
pub(crate) fn record_key_ranges(
    table: &KvTable,
    ranges: &[IndexRange],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Option<Vec<(Key, Key)>>, tidb_codec::CodecError> {
    if common_handle_primary(table).is_some() {
        return common_handle_record_key_ranges(table, ranges, zone).map(Some);
    }
    let ids = table.record_physical_ids();
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
    Ok(Some(key_ranges))
}

/// Go `CommonHandleRangesToKVRanges`: the ranger's encoded tuple bounds
/// prefixed by each physical table's record namespace.
fn common_handle_record_key_ranges(
    table: &KvTable,
    ranges: &[IndexRange],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Vec<(Key, Key)>, tidb_codec::CodecError> {
    let ids = table.record_physical_ids();
    let mut key_ranges = Vec::with_capacity(ranges.len() * ids.len());
    for id in ids {
        for range in ranges {
            let low = tidb_codec::encode_key_in_timezone(zone, &range.low)?;
            let low = if range.low_exclusive {
                Key::from_bytes(low).prefix_next().into_bytes()
            } else {
                low
            };
            let high = tidb_codec::encode_key_in_timezone(zone, &range.high)?;
            let high = if range.high_exclusive {
                high
            } else {
                Key::from_bytes(high).prefix_next().into_bytes()
            };
            key_ranges.push((
                Key::from_bytes(tidb_codec::encode_row_key(id, &low)),
                Key::from_bytes(tidb_codec::encode_row_key(id, &high)),
            ));
        }
    }
    Ok(key_ranges)
}
