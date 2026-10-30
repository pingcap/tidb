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

//! Go `TableReaderExecutor`'s key ranges (`executor/table_reader.go`): the
//! planned handle ranges of a table, split at the int64 boundary for an
//! unsigned handle (`distsql.SplitRangesAcrossInt64Boundary`, whose two
//! halves are Go's `firstPartRanges` / `secondPartRanges`) and encoded for
//! each physical table (`distsql.TableHandleRangesToKVRanges`).
//!
//! An open bound becomes the handle domain's extreme first (Go
//! `points2TableRanges`, which the planner already applied to a planned
//! table path), so every bound is an integer the boundary split can read.

use tidb_table::kv_table::KvTable;
use tidb_ranger::types::Range;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_distsql::{
    signed_handle_ranges_to_kv_ranges, split_ranges_across_int64_boundary, SignedHandleRange,
};
use tidb_txnkv::Key;

/// The primary-key column that IS the row handle, when the table has one and
/// this tier can range over it.
fn handle_column(table: &KvTable) -> Option<HandleColumn> {
    if let Some(offset) = table.pk_handle_offset() {
        let column = table.columns().get(offset)?;
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
            field_type: column.field_type.clone(),
        });
    }
    // Go `buildDataSource`: a table with neither an integer primary key nor a
    // common handle gets `NewExtraHandleSchemaCol()` appended, and
    // `ds.handleCols` is built FROM it. So `_tidb_rowid` is that table's
    // handle for range building exactly as an integer primary key is, and
    // `WHERE _tidb_rowid > 0` reads a TABLE RANGE rather than every row.
    //
    // A common-handle table never reaches here (the general clustered-primary
    // range builder handles it), and a table WITH an integer primary key
    // has no `_tidb_rowid` to name -- which is why TiDB answers "Unknown
    // column" for it there. Both are the same test
    // `crate::driver::from::extra_handle_column` applies to the name.
    (!table.common_handle_offsets().is_empty())
        .then_some(())
        .map_or(
            Some(HandleColumn {
                // Go `NewExtraHandleSchemaCol`: `TypeLonglong`, signed, with
                // `PriKeyFlag | NotNullFlag`.
                field_type: FieldType::new(FieldTypeCode::LongLong)
                    .with_flags(tidb_datatype::FieldTypeFlags::NOT_NULL),
            }),
            |()| None,
        )
}

/// The column a table's INTEGER row handle IS: its integer primary key, or
/// `_tidb_rowid` when it has none.
struct HandleColumn {
    field_type: FieldType,
}

/// Whether this table's row handle spans the UNSIGNED domain.
///
/// Go carries the same bit as `mysql.HasUnsignedFlag(newTp.GetFlag())` inside
/// `convertPointsInPlace`, and it decides two things: which extremes an open
/// endpoint becomes ([`to_table_range_in_domain`]), and whether the ranges
/// have to be cut at the point the key encoding flips sign
/// ([`tidb_distsql::split_ranges_across_int64_boundary`]).
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
/// [`tidb_distsql::split_ranges_across_int64_boundary`], because a sentinel is not an
/// integer it can compare.
fn to_table_range_in_domain(range: &Range, unsigned: bool) -> (i64, i64, bool, bool) {
    // The domain's extremes, as the handle codec spells them. For an unsigned
    // handle the maximum is `u64::MAX`, whose signed reading is -1.
    let (domain_low, domain_high) = if unsigned {
        (0_i64, u64::MAX as i64)
    } else {
        (i64::MIN, i64::MAX)
    };
    let low = range.low_val.first();
    let high = range.high_val.first();
    let (low_value, low_exclusive) = match low {
        Some(Datum::Null) => (domain_low, false),
        Some(Datum::MinNotNull) | None => (domain_low, range.low_exclude),
        Some(Datum::Int(value)) => (*value, range.low_exclude),
        Some(Datum::UInt(value)) => (*value as i64, range.low_exclude),
        _ => (domain_low, range.low_exclude),
    };
    let (high_value, high_exclusive) = match high {
        Some(Datum::MaxValue) | None => (domain_high, range.high_exclude),
        Some(Datum::Int(value)) => (*value, range.high_exclude),
        Some(Datum::UInt(value)) => (*value as i64, range.high_exclude),
        _ => (domain_high, range.high_exclude),
    };
    (low_value, high_value, low_exclusive, high_exclusive)
}

/// A range whose open ends have been replaced by the handle domain's own
/// extremes, so every bound is a concrete integer the boundary split can read.
fn materialize_open_bounds(range: &Range, unsigned: bool) -> Range {
    let (low, high, low_exclusive, high_exclusive) = to_table_range_in_domain(range, unsigned);
    let datum = |value: i64| {
        if unsigned {
            Datum::UInt(value as u64)
        } else {
            Datum::Int(value)
        }
    };
    Range {
        low_val: vec![datum(low)],
        high_val: vec![datum(high)],
        collators: range.collators.clone(),
        low_exclude: low_exclusive,
        high_exclude: high_exclusive,
    }
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
pub fn record_key_ranges(
    table: &KvTable,
    ranges: &[Range],
    zone: &tidb_datatype::SessionTimeZone,
    keep_order: bool,
) -> Result<Option<Vec<(Key, Key)>>, tidb_distsql::KvRequestBuildError>
{
    match record_key_range_value_halves(table, ranges, zone)? {
        Some([signed_half, unsigned_half]) => {
            if keep_order {
                // Go's ordered answer reads the signed half first
                // (`keepOrder = true, desc = false); a descending caller
                // reverses this one list, which puts the unsigned half first.
                Ok(Some(signed_half.into_iter().chain(unsigned_half).collect()))
            } else {
                // Go's unordered wire order merges both halves into ONE
                // request: `append(unsignedRanges, signedRanges...) ascends
                // in ENCODED keys, because values above `MaxInt64 encode
                // NEGATIVE and sort before every ordinary handle.
                let mut merged = unsigned_half;
                merged.extend(signed_half);
                Ok(Some(merged))
            }
        }
        None => unreachable!("the halves are always computed"),
    }
}

/// The two VALUE-ordered halves of [`record_key_ranges]: an unsigned handle
/// whose domain wraps the int64 boundary yields `[signed_half, unsigned_half]
/// -- each half's intervals ascend in ENCODED-key order -- while every other
/// shape is one non-empty half. An ORDERED reader consumes the halves in list
/// order (a descending reader reverses the pair, Go's `firstPart /
/// `secondPart choice), because the two halves cannot form one ascending
/// range list an ordered coprocessor request could carry. An UNORDERED reader
/// never calls this: its single request carries Go's merged wire order,
/// which only [`record_key_ranges] assembles.
/// Go `TableHandleRangesToKVRanges` for a common handle
/// (`CommonHandleRangesToKVRanges(dctx, tids, ranges)`): every physical
/// table's record keys, encoded as Go's read path encodes them -- the session
/// location and the process's collation mode.
fn common_handle_record_key_ranges(
    table: &KvTable,
    ranges: &[Range],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Vec<(Key, Key)>, tidb_distsql::KvRequestBuildError> {
    let mut key_ranges = Vec::new();
    for id in table.record_physical_ids() {
        for range in tidb_distsql::common_handle_ranges_to_kv_ranges(Some(zone), id, ranges)? {
            key_ranges.push((range.start_key, range.end_key));
        }
    }
    Ok(key_ranges)
}

pub fn record_key_range_value_halves(
    table: &KvTable,
    ranges: &[Range],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<
    Option<[Vec<(Key, Key)>; 2]>,
    tidb_distsql::KvRequestBuildError,
> {
    // DDL does not materialize the clustered PRIMARY as a secondary index.
    // The table path still carries the common-handle column offsets, and Go's
    // `CommonHandleRangesToKVRanges` uses those record keys directly.
    if !table.common_handle_offsets().is_empty() {
        return common_handle_record_key_ranges(table, ranges, zone)
            .map(|list| Some([list, Vec::new()]));
    }
    let ids = table.record_physical_ids();
    let unsigned = handle_is_unsigned(table);
    // Go `points2TableRanges`: replace the open endpoints with the handle
    // domain's own extremes, so every bound below is a real integer -- and,
    // for an unsigned handle, one the boundary split can recognise.
    let materialized: Vec<Range> = ranges
        .iter()
        .map(|range| materialize_open_bounds(range, unsigned))
        .collect();
    let (signed_side, unsigned_side) = if unsigned {
        split_ranges_across_int64_boundary(materialized, true, false, false)
    } else {
        (materialized, Vec::new())
    };
    fn encode(half: &[Range], ids: &[i64], unsigned: bool) -> Vec<(Key, Key)> {
        let mut handle_ranges = Vec::with_capacity(half.len());
        for range in half {
            let (low, high, low_exclusive, high_exclusive) =
                to_table_range_in_domain(range, unsigned);
            // An empty range is not an error: `id > 100 AND id < 100 admits
            // no row, and Go plans a `TableDual` for it. The caller reads
            // nothing from this interval.
            let Ok(handle_range) = SignedHandleRange::new(low, high, low_exclusive, high_exclusive)
            else {
                continue;
            };
            handle_ranges.push(handle_range);
        }
        let mut key_ranges = Vec::with_capacity(handle_ranges.len() * ids.len());
        for &id in ids {
            for encoded in signed_handle_ranges_to_kv_ranges(id, &handle_ranges) {
                key_ranges.push((encoded.start_key, encoded.end_key));
            }
        }
        key_ranges
    }
    Ok(Some([
        encode(&signed_side, &ids, unsigned),
        encode(&unsigned_side, &ids, unsigned),
    ]))
}
