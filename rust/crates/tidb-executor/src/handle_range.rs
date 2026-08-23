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

//! The range of a table's CLUSTERED HANDLE that a `WHERE` implies:
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
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_distsql::{signed_handle_ranges_to_kv_ranges, SignedHandleRange};
use tidb_planner::cardinality::row_count_estimator::{
    get_row_count_by_column_ranges, ColumnRange, EstimatorOptions,
};
use tidb_txnkv::Key;

/// The ranges a `WHERE` implies over `table`'s clustered handle, or
/// `None` when this tier builds none.
///
/// `None` is Go's full range, and the caller reads the whole table for it.
/// The refusals are:
///
/// * a table with no declared primary-key handle -- a `_tidb_rowid` table has
///   no handle a `WHERE` can name;
/// * an UNSIGNED handle. The record key encodes a handle with the SIGNED
///   integer codec, so an unsigned value above `i64::MAX` encodes as a
///   negative key and a range over it is not the interval its bounds read
///   like. Go handles this through `points2TableRanges`' unsigned domain;
///   refusing is a lost optimization on such tables, never a wrong answer,
///   because the `WHERE` above the source still filters every row;
/// * a `WHERE` that constrains the handle with nothing the ranger can use.
///
/// An UNSIGNED handle is ranged over like any other. Its two extra steps --
/// materialising an open endpoint in the unsigned domain, and cutting the
/// ranges where the signed key codec flips sign -- both live at the point of
/// USE ([`record_key_ranges`], [`column_range`]) rather than here. Go
/// materialises earlier, inside `points2TableRanges`, so its `path.Ranges`
/// already carry `0`/`MaxUint64` where this list still carries
/// `MinNotNull`/`MaxValue`; the bounds those two spellings denote are the
/// same, every consumer here converts through [`to_table_range_in_domain`]
/// before reading them, and keeping the sentinels is what leaves the range
/// text `EXPLAIN` prints for a SIGNED handle untouched.
pub(crate) fn build_handle_ranges<'a>(
    table: &KvTable,
    where_clause: &'a tidb_ast::Expr,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<IndexRanges<'a>> {
    let common_offsets = table.common_handle_offsets();
    if !common_offsets.is_empty() {
        let columns = common_offsets
            .iter()
            .map(|offset| {
                let column = table.columns.get(*offset)?;
                Some(crate::index_range::RangeColumn::whole(
                    column.name.clone(),
                    column.field_type.clone(),
                ))
            })
            .collect::<Option<Vec<_>>>()?;
        return crate::index_range::detach_cond_and_build_range_for_index(
            &columns,
            where_clause,
            zone,
        );
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
    // Go materialises the open endpoints here, inside `points2TableRanges`,
    // and an UNSIGNED handle is where that becomes VISIBLE: its domain
    // minimum is `0`, and `formatDatum` (`ranger/types.go:371`) prints a
    // `KindUint64` low bound as the number rather than as `-inf` -- so Go's
    // `EXPLAIN` says `range:[0,9223372036854775808)` where the sentinel would
    // say `[-inf,...)`. The high end agrees either way, because `MaxUint64`
    // on the right prints `+inf` too.
    //
    // A SIGNED handle keeps the sentinels: `formatDatum` maps `MinInt64` on
    // the left and `MaxInt64` on the right to the same `-inf`/`+inf` the
    // sentinels print, every consumer here converts through
    // [`to_table_range_in_domain`] before reading a bound, and not rewriting
    // them leaves this crate's existing range text untouched.
    if handle_is_unsigned(table) {
        for range in &mut built.ranges {
            *range = materialize_open_bounds(range, true);
        }
    }
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

/// The clustered PRIMARY's index METADATA, whether or not the catalog stores
/// it as a [`KvIndex`].
///
/// Go's `TableInfo.Indices` always carries the clustered primary `IndexInfo`
/// (`Primary: true`), and `deriveCommonHandleTablePathStats` builds the table
/// path's ranges from it. This tier's `CREATE TABLE` stores no `KvIndex` for
/// a clustered key -- the record key itself enforces it, and a stored index
/// would be physically maintained as a duplicate -- so the same metadata is
/// synthesized here from the handle offsets. A clustered key part never has a
/// prefix ([`crate::ddl::index_prefix::clustered_prefix_unsupported`]), so
/// whole-column parts are the faithful reconstruction. Id `0` collides with
/// no stored index (the DDL allocates from 1), so a statistics lookup under
/// it misses and the estimate is the pseudo one -- exactly what Go computes
/// for an unanalyzed primary.
///
/// The tables that DO store a PRIMARY `KvIndex` (test scaffolding injects one
/// for statistics) keep using it, its id and its declared parts.
pub(crate) fn clustered_primary_metadata(table: &KvTable) -> Option<std::borrow::Cow<'_, KvIndex>> {
    if table.common_handle_offsets().is_empty() {
        return None;
    }
    if let Some(index) = common_handle_primary(table) {
        return Some(std::borrow::Cow::Borrowed(index));
    }
    Some(std::borrow::Cow::Owned(KvIndex {
        id: 0,
        name: "PRIMARY".to_owned(),
        comment: String::new(),
        unique: true,
        column_offsets: table.common_handle_offsets().to_vec(),
        prefix_lengths: vec![
            crate::ddl::index_prefix::UNSPECIFIED_LENGTH;
            table.common_handle_offsets().len()
        ],
        visible: true,
        global: false,
    }))
}

fn build_common_handle_ranges<'a>(
    table: &KvTable,
    where_clause: &'a tidb_ast::Expr,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<IndexRanges<'a>> {
    let index = clustered_primary_metadata(table)?;
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
fn handle_column(table: &KvTable) -> Option<HandleColumn> {
    if let Some(offset) = table.pk_handle_offset() {
        let column = table.columns.get(offset)?;
        // The handle codec is integer; a clustered non-integer key reaches
        // this tier as a common handle, which `pk_handle_offset` excludes.
        if !matches!(
            column.field_type.code(),
            FieldTypeCode::Tiny
                | FieldTypeCode::Short
                | FieldTypeCode::Int24
                | FieldTypeCode::Long
                | FieldTypeCode::LongLong
        ) {
            return None;
        }
        return Some(HandleColumn {
            name: column.name.clone(),
            field_type: column.field_type.clone(),
            id: column.id,
        });
    }
    // Go `buildDataSource`: a table with neither an integer primary key nor a
    // common handle gets `NewExtraHandleSchemaCol()` appended, and
    // `ds.handleCols` is built FROM it. So `_tidb_rowid` is that table's
    // handle for range building exactly as an integer primary key is, and
    // `WHERE _tidb_rowid > 0` reads a TABLE RANGE rather than every row.
    //
    // A common-handle table never reaches here (the caller routes it to
    // `build_common_handle_ranges`), and a table WITH an integer primary key
    // has no `_tidb_rowid` to name -- which is why TiDB answers "Unknown
    // column" for it there. Both are the same test
    // `crate::driver::from::extra_handle_column` applies to the name.
    (!table.common_handle_offsets().is_empty())
        .then_some(())
        .map_or(
            Some(HandleColumn {
                name: crate::driver::leaf_demand::EXTRA_HANDLE_NAME.to_owned(),
                // Go `NewExtraHandleSchemaCol`: `TypeLonglong`, signed, with
                // `PriKeyFlag | NotNullFlag`.
                field_type: FieldType::new(FieldTypeCode::LongLong)
                    .with_flags(tidb_datatype::FieldTypeFlags::NOT_NULL),
                id: crate::remote_scan::EXTRA_HANDLE_COLUMN_ID,
            }),
            |()| None,
        )
}

/// The column a table's INTEGER row handle IS: its integer primary key, or
/// `_tidb_rowid` when it has none.
struct HandleColumn {
    name: String,
    field_type: FieldType,
    /// The statistics column id: a real column's own, or Go's
    /// `model.ExtraHandleID` for the extra handle.
    id: i64,
}

/// Whether this table's row handle spans the UNSIGNED domain.
///
/// Go carries the same bit as `mysql.HasUnsignedFlag(newTp.GetFlag())` inside
/// `convertPointsInPlace`, and it decides two things: which extremes an open
/// endpoint becomes ([`to_table_range_in_domain`]), and whether the ranges
/// have to be cut at the point the key encoding flips sign
/// ([`split_ranges_across_int64_boundary`]).
fn handle_is_unsigned(table: &KvTable) -> bool {
    handle_column(table).is_some_and(|column| column.field_type.is_unsigned())
}

/// Go `points2TableRanges`' conversion: an open endpoint becomes the handle
/// domain's own extreme, so every bound is a real integer.
///
/// A NULL LOW bound becomes the minimum INCLUSIVE (Go `startPoint.excl =
/// false`). A NULL HIGH bound cannot reach here at all -- the `skipNull` filter
/// in [`build_handle_ranges`] has already dropped that whole interval -- so the
/// catch-all high arm below is a total-match requirement, not a rule.
///
/// `unsigned` is the handle column's own signedness, and it decides which
/// extremes an open end becomes: Go `convertPointsInPlace` sets `0` and
/// `MaxUint64` for an unsigned column against `MinInt64` and `MaxInt64` for a
/// signed one (`ranger.go:71-78`). Getting it wrong is not a lost
/// optimization: `u64::MAX` encodes as -1, so mapping an unsigned open high to
/// `i64::MAX` turns `id >= 2^63` into the key interval `[negative, i64::MAX]`,
/// which spans the whole table -- and hides the boundary from
/// [`split_ranges_across_int64_boundary`], because a sentinel is not an
/// integer it can compare.
fn to_table_range_in_domain(range: &IndexRange, unsigned: bool) -> (i64, i64, bool, bool) {
    // The domain's extremes, as the handle codec spells them. For an unsigned
    // handle the maximum is `u64::MAX`, whose signed reading is -1.
    let (domain_low, domain_high) = if unsigned {
        (0_i64, u64::MAX as i64)
    } else {
        (i64::MIN, i64::MAX)
    };
    let low = range.low.first();
    let high = range.high.first();
    let (low_value, low_exclusive) = match low {
        Some(Datum::Null) => (domain_low, false),
        Some(Datum::MinNotNull) | None => (domain_low, range.low_exclusive),
        Some(Datum::Int(value)) => (*value, range.low_exclusive),
        Some(Datum::UInt(value)) => (*value as i64, range.low_exclusive),
        _ => (domain_low, range.low_exclusive),
    };
    let (high_value, high_exclusive) = match high {
        Some(Datum::MaxValue) | None => (domain_high, range.high_exclusive),
        Some(Datum::Int(value)) => (*value, range.high_exclusive),
        Some(Datum::UInt(value)) => (*value as i64, range.high_exclusive),
        _ => (domain_high, range.high_exclusive),
    };
    (low_value, high_value, low_exclusive, high_exclusive)
}

/// A range whose open ends have been replaced by the handle domain's own
/// extremes, so every bound is a concrete integer the boundary split can read.
fn materialize_open_bounds(range: &IndexRange, unsigned: bool) -> IndexRange {
    let (low, high, low_exclusive, high_exclusive) = to_table_range_in_domain(range, unsigned);
    let datum = |value: i64| {
        if unsigned {
            Datum::UInt(value as u64)
        } else {
            Datum::Int(value)
        }
    };
    IndexRange {
        low: vec![datum(low)],
        high: vec![datum(high)],
        low_exclusive,
        high_exclusive,
    }
}

/// One range as the estimator's column range, after step 2's conversion --
/// which is what puts a real `KindInt64` in the low bound, and so decides
/// which arm of the estimator runs (see the module doc).
fn column_range(range: &IndexRange, unsigned: bool) -> ColumnRange {
    // `GetRowCountByColumnRanges` dispatches on the first range's low-bound
    // KIND, so the bound has to keep the domain's own datum kind: a `KindInt64`
    // takes the signed pseudo estimator and anything else the unsigned one.
    // Go reaches the unsigned arm on an unsigned handle for exactly this
    // reason -- `convertPointsInPlace` casts every endpoint into the column's
    // type first.
    let materialized = materialize_open_bounds(range, unsigned);
    ColumnRange {
        low: materialized.low[0].clone(),
        high: materialized.high[0].clone(),
        low_exclude: materialized.low_exclusive,
        high_exclude: materialized.high_exclusive,
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
    if let Some(index) = clustered_primary_metadata(table) {
        return crate::access_cost::index_range_row_count(&index, table, ranges, stats, realtime);
    }
    let Some(column) = handle_column(table) else {
        return realtime;
    };
    let unsigned = column.field_type.is_unsigned();
    let column_ranges: Vec<ColumnRange> = ranges
        .iter()
        .map(|range| column_range(range, unsigned))
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
/// `keep_order` is the reader's required order (`TableReaderExecutor
/// .keepOrder`, plus the descending walk this tier models by reversing the
/// one list): an ordered caller receives the halves in ascending VALUE order,
/// while an unordered caller receives Go's merged wire order.
///
/// `None` means a bound this tier cannot encode, and the caller falls back to
/// the whole record range -- reading a superset is always correct, because
/// the `WHERE` above the source filters every row it returns.
pub(crate) fn record_key_ranges(
    table: &KvTable,
    ranges: &[IndexRange],
    zone: &tidb_datatype::SessionTimeZone,
    keep_order: bool,
) -> Result<Option<Vec<(Key, Key)>>, tidb_codec::CodecError> {
    // DDL does not materialize the clustered PRIMARY as a secondary index.
    // The table path still carries the common-handle column offsets, and Go's
    // `CommonHandleRangesToKVRanges` uses those record keys directly.
    if !table.common_handle_offsets().is_empty() {
        return common_handle_record_key_ranges(table, ranges, zone).map(Some);
    }
    let ids = table.record_physical_ids();
    let unsigned = handle_is_unsigned(table);
    // Go `points2TableRanges`: replace the open endpoints with the handle
    // domain's own extremes, so every bound below is a real integer -- and,
    // for an unsigned handle, one the boundary split can recognise.
    let materialized: Vec<IndexRange> = ranges
        .iter()
        .map(|range| materialize_open_bounds(range, unsigned))
        .collect();
    // Go `table_reader.go:295`: `SplitRangesAcrossInt64Boundary(ranges,
    // e.keepOrder, e.desc, ...)`. An ORDERED read opens the halves as two
    // results consumed one after the other, and `keepOrder = true, desc =
    // false` answers signed-first; this tier hands such a caller ONE list in
    // exactly that VALUE order, and its descending callers reverse the list
    // (and each range), which yields Go's `desc = true` answer -- the
    // unsigned half first.
    //
    // An UNORDERED read differs ON THE WIRE: Go merges both halves into ONE
    // request, `append(unsignedRanges, signedRanges...)`, because values
    // above `MaxInt64` encode NEGATIVE record keys and therefore sort before
    // every ordinary handle. That list ascends in encoded-key order -- the
    // only shape an unordered request may hand the coprocessor transport,
    // whose task builder consumes ranges in list order.
    let materialized = if !unsigned {
        materialized
    } else if keep_order {
        let (first, second) = split_ranges_across_int64_boundary(materialized, true, false);
        first.into_iter().chain(second).collect()
    } else {
        let (mut merged, second) =
            split_ranges_across_int64_boundary(materialized, false, false);
        merged.extend(second);
        merged
    };
    let mut handle_ranges = Vec::with_capacity(materialized.len());
    for range in &materialized {
        let (low, high, low_exclusive, high_exclusive) = to_table_range_in_domain(range, unsigned);
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
            let low = tidb_codec::Encoder::new(table.use_new_collation())
                .encode_key_in_timezone(zone, &range.low)?;
            let low = if range.low_exclusive {
                Key::from_bytes(low).prefix_next().into_bytes()
            } else {
                low
            };
            let high = tidb_codec::Encoder::new(table.use_new_collation())
                .encode_key_in_timezone(zone, &range.high)?;
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

/// Go `SplitRangesAcrossInt64Boundary`
/// (`pkg/distsql/request_builder.go:575`): divides an UNSIGNED handle's
/// ranges at the point where the key encoding flips sign.
///
/// A row handle is encoded as a signed 64-bit integer, so an unsigned value
/// above `i64::MAX` encodes NEGATIVE and sorts, in key order, before every
/// ordinary handle. A single range spanning that point would be read as one
/// contiguous key interval and would miss everything on the far side.
///
/// The result is two range lists in the order they must be READ. When order
/// does not matter Go concatenates them into the first list and leaves the
/// second empty, which is why the signature returns a pair rather than two
/// separate calls: the caller must not reorder them itself.
///
/// Returns `(signed, unsigned)` for an ascending ordered read and
/// `(unsigned, signed)` for a descending one, because the unsigned half
/// occupies the LOWER key range.
#[must_use]
pub fn split_ranges_across_int64_boundary(
    ranges: Vec<IndexRange>,
    keep_order: bool,
    desc: bool,
) -> (Vec<IndexRange>, Vec<IndexRange>) {
    /// The unsigned reading of a bound, or `None` when it is not an integer
    /// this split understands.
    fn unsigned(datum: Option<&Datum>) -> Option<u64> {
        match datum? {
            Datum::UInt(value) => Some(*value),
            Datum::Int(value) => Some(*value as u64),
            _ => None,
        }
    }
    // Go returns the ranges untouched for a common handle, an empty set, or a
    // SIGNED leading bound -- the last being how it detects that the handle is
    // not unsigned at all.
    if ranges.is_empty() || matches!(ranges[0].low.first(), Some(Datum::Int(_))) {
        return (ranges, Vec::new());
    }
    let boundary = i64::MAX as u64;
    let Some(index) = ranges
        .iter()
        .position(|range| unsigned(range.high.first()).is_some_and(|high| high > boundary))
    else {
        // Nothing reaches past the boundary.
        return (ranges, Vec::new());
    };
    let order = |signed: Vec<IndexRange>, mut unsigned_half: Vec<IndexRange>| {
        if !keep_order {
            // Go appends the SIGNED half onto the unsigned one and returns a
            // single list: with no order to keep, one scan covers both.
            unsigned_half.extend(signed);
            return (unsigned_half, Vec::new());
        }
        if desc {
            (unsigned_half, signed)
        } else {
            (signed, unsigned_half)
        }
    };
    let straddles = unsigned(ranges[index].low.first()).is_some_and(|low| low <= boundary);
    if !straddles {
        // A clean cut: every range from `index` on is wholly above the
        // boundary.
        let mut ranges = ranges;
        let unsigned_half = ranges.split_off(index);
        return order(ranges, unsigned_half);
    }
    // The range at `index` spans the boundary and has to be cut in two.
    let mut ranges = ranges;
    let tail = ranges.split_off(index);
    let mut signed = ranges;
    let (crossing, rest) = tail.split_first().expect("the straddling range");
    // Go skips the signed piece when the cut point is EXCLUDED at exactly
    // `MaxInt64`, because that piece would then be empty.
    if !(unsigned(crossing.low.first()) == Some(boundary) && crossing.low_exclusive) {
        signed.push(IndexRange {
            low: crossing.low.clone(),
            high: vec![Datum::UInt(boundary)],
            low_exclusive: crossing.low_exclusive,
            high_exclusive: false,
        });
    }
    let mut unsigned_half = Vec::with_capacity(rest.len() + 1);
    // ... and the unsigned piece when it is excluded at exactly
    // `MaxInt64 + 1`, for the same reason.
    if !(unsigned(crossing.high.first()) == Some(boundary + 1) && crossing.high_exclusive) {
        unsigned_half.push(IndexRange {
            low: vec![Datum::UInt(boundary + 1)],
            high: crossing.high.clone(),
            low_exclusive: false,
            high_exclusive: crossing.high_exclusive,
        });
    }
    unsigned_half.extend(rest.iter().cloned());
    order(signed, unsigned_half)
}

#[cfg(test)]
mod int64_boundary_split_tests {
    use super::*;

    const MAX: u64 = i64::MAX as u64;

    fn range(low: u64, high: u64) -> IndexRange {
        IndexRange {
            low: vec![Datum::UInt(low)],
            high: vec![Datum::UInt(high)],
            low_exclusive: false,
            high_exclusive: false,
        }
    }

    fn bounds(ranges: &[IndexRange]) -> Vec<(u64, u64)> {
        ranges
            .iter()
            .map(|range| match (range.low.first(), range.high.first()) {
                (Some(Datum::UInt(low)), Some(Datum::UInt(high))) => (*low, *high),
                other => panic!("unsigned bounds, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn a_signed_leading_bound_is_left_alone() {
        // Go detects a non-unsigned handle by the leading bound's KIND and
        // returns the ranges untouched.
        let input = vec![IndexRange {
            low: vec![Datum::Int(1)],
            high: vec![Datum::Int(9)],
            low_exclusive: false,
            high_exclusive: false,
        }];
        let (first, second) = split_ranges_across_int64_boundary(input, true, false);
        assert_eq!(first.len(), 1);
        assert!(second.is_empty());
    }

    #[test]
    fn ranges_below_the_boundary_are_left_alone() {
        let (first, second) =
            split_ranges_across_int64_boundary(vec![range(1, 9), range(20, MAX)], true, false);
        assert_eq!(bounds(&first), vec![(1, 9), (20, MAX)]);
        assert!(second.is_empty(), "nothing reaches past the boundary");
    }

    #[test]
    fn a_clean_cut_splits_between_ranges() {
        // No range straddles: the split falls between them.
        let (signed, unsigned) = split_ranges_across_int64_boundary(
            vec![range(1, 9), range(MAX + 1, MAX + 5)],
            true,
            false,
        );
        assert_eq!(bounds(&signed), vec![(1, 9)]);
        assert_eq!(bounds(&unsigned), vec![(MAX + 1, MAX + 5)]);
    }

    #[test]
    fn a_straddling_range_is_cut_at_the_boundary() {
        let (signed, unsigned) =
            split_ranges_across_int64_boundary(vec![range(1, MAX + 5)], true, false);
        assert_eq!(
            bounds(&signed),
            vec![(1, MAX)],
            "the signed piece ends AT i64::MAX, inclusive"
        );
        assert_eq!(
            bounds(&unsigned),
            vec![(MAX + 1, MAX + 5)],
            "and the unsigned piece starts one past it"
        );
    }

    #[test]
    fn an_empty_signed_piece_is_not_emitted() {
        // Go skips the signed half when the low bound is EXCLUSIVE at exactly
        // i64::MAX, because `(MAX, MAX]` selects nothing.
        let mut input = range(MAX, MAX + 5);
        input.low_exclusive = true;
        let (signed, unsigned) = split_ranges_across_int64_boundary(vec![input], true, false);
        assert!(signed.is_empty(), "an empty signed piece is omitted");
        assert_eq!(bounds(&unsigned), vec![(MAX + 1, MAX + 5)]);
    }

    #[test]
    fn an_empty_unsigned_piece_is_not_emitted() {
        // The mirror image: exclusive at exactly i64::MAX + 1.
        let mut input = range(1, MAX + 1);
        input.high_exclusive = true;
        let (signed, unsigned) = split_ranges_across_int64_boundary(vec![input], true, false);
        assert_eq!(bounds(&signed), vec![(1, MAX)]);
        assert!(unsigned.is_empty(), "an empty unsigned piece is omitted");
    }

    #[test]
    fn descending_order_reads_the_unsigned_half_first() {
        // The unsigned half occupies the LOWER key range, so a descending
        // ordered read takes it first.
        let (first, second) =
            split_ranges_across_int64_boundary(vec![range(1, MAX + 5)], true, true);
        assert_eq!(bounds(&first), vec![(MAX + 1, MAX + 5)]);
        assert_eq!(bounds(&second), vec![(1, MAX)]);
    }

    #[test]
    fn an_unordered_read_gets_one_list_with_the_unsigned_half_first() {
        // Go concatenates rather than returning a pair when order is free, so
        // ONE scan covers both halves -- and it puts the unsigned half first
        // because that is where the keys start.
        let (all, second) =
            split_ranges_across_int64_boundary(vec![range(1, MAX + 5)], false, false);
        assert_eq!(bounds(&all), vec![(MAX + 1, MAX + 5), (1, MAX)]);
        assert!(second.is_empty(), "an unordered read gets a single list");
    }
}
