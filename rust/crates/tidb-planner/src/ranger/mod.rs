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

//! Go `pkg/util/ranger`, ported WHOLE: scan-range construction — the
//! machinery that turns predicates into index/table key ranges.
//!
//! * [`types`] — `types.go`: the `Range`/`Ranges` model, the Go-format
//!   printer (`strconv.Quote` over raw bytes), and `MemUsage` with Go's
//!   real struct sizes. `types_test.go` transcreated.
//! * [`checker`] — `checker.go`: access-condition admission, collation
//!   gates, prefix/single-scan rules.
//! * [`points`] — `points.go`: the point model, the whole builder,
//!   prefix cutting, the union/intersection sweeps.
//! * [`ranger`] — `ranger.go`: points-to-ranges assembly, `UnionRanges`,
//!   the append family with Go's memory-quota estimates,
//!   `AppendRanges2PointRanges`, `BuildTableRange`/`BuildColumnRange`.
//! * [`detacher`] — `detacher.go`: eq/in extraction (including the
//!   unconditional NULL-valued-access drop), CNF/DNF detachment with both
//!   fix-control arms, the best-CNF-item machinery, `MergeDNFItems4Col`,
//!   and the shard-index GC-column family (`AddExpr4EqAndInCondition`).
//!
//! `ranger_test.go`'s table-driven suites are transcreated in
//! [`go_cases`]: TableRange, ColumnRange, UnsignedAndOverflow, Year,
//! PrefixIndexRangeScan, IndexRange, PrefixIndexRange,
//! ShardIndexFuncSuites, MinAccessCondsForDNFCond, the three
//! RangeFallback ladders, MemUsage, BinCollation, and issues 40997/50051.
//!
//! Boundaries, by name:
//! * `RangesToString`/`RangeSingleColToString` are not ported: nothing in
//!   the production tree consumes them (only `ranger.go` itself defines
//!   them), and no upstream test covers them.
//! * A `Constant`'s plan-cache mutability (`ParamMarker`/`DeferredExpr`)
//!   is structurally absent from this expression model; `ValueInfo`'s
//!   `mutable` leg activates with the plan-cache track.
//! * `TestTableShardIndex` and `TestRangeFallback*`'s warning surface are
//!   testkit/plan-level: their ranger-observable cores are pinned here,
//!   and the plan-level halves belong to the driver's rule track.

pub mod checker;
pub mod detacher;
mod go_cases;
pub mod points;
pub mod ranger;
pub mod types;

pub use types::{HasFullRange, Range, Ranges};

/// The pseudo-statistics bridge: `ranger` ranges into the shapes
/// `cardinality::pseudo`'s Go-ported row counts consume — the
/// `CountAfterAccess` half of Go's `detachCondAndBuildRangeForPath`
/// (`core/stats.go:397`) for unanalyzed tables.
pub mod stats_bridge {
    use tidb_datatype::Datum;

    use crate::cardinality::pseudo::{
        pseudo_row_count_by_index_ranges, pseudo_row_count_by_signed_int_ranges,
        pseudo_row_count_by_unsigned_int_ranges, IndexRange, PseudoBoundKind, ScalarRange,
        SignedIntRange, UnsignedIntRange,
    };

    fn bound_kind(datum: &Datum) -> PseudoBoundKind {
        match datum {
            Datum::Null => PseudoBoundKind::Null,
            Datum::MinNotNull => PseudoBoundKind::MinNotNull,
            Datum::MaxValue => PseudoBoundKind::MaxValue,
            _ => PseudoBoundKind::Value,
        }
    }

    /// Go `statistics.convertDatumToScalar`'s NUMERIC arms; non-numeric
    /// kinds answer `None` and their column reads as unbounded (the string
    /// byte-prefix projection follows with `statistics/scalar.go`).
    fn datum_to_scalar(datum: &Datum) -> Option<f64> {
        match datum {
            Datum::Int(v) => Some(*v as f64),
            Datum::UInt(v) => Some(*v as f64),
            Datum::Real(v) | Datum::Float32(v) => Some(*v),
            Datum::Decimal(d) => Some(d.to_f64()),
            _ => None,
        }
    }

    /// Go `GetPseudoRowCountByIndexRanges` over this port's range model.
    #[must_use]
    pub fn pseudo_count_by_ranges(ranges: &super::types::Ranges, table_row_count: f64) -> f64 {
        let mut index_ranges = Vec::with_capacity(ranges.len());
        for ran in ranges {
            let equal_prefix_len = ran.prefix_equal_len().unwrap_or(0);
            let mut columns = Vec::with_capacity(ran.low_val.len());
            for i in 0..ran.low_val.len().min(ran.high_val.len()) {
                let low_kind = bound_kind(&ran.low_val[i]);
                let high_kind = bound_kind(&ran.high_val[i]);
                let low = if low_kind == PseudoBoundKind::Value {
                    match datum_to_scalar(&ran.low_val[i]) {
                        Some(v) => v,
                        None => {
                            columns.push(ScalarRange {
                                low: 0.0,
                                high: 0.0,
                                low_kind: PseudoBoundKind::MinNotNull,
                                high_kind: PseudoBoundKind::MaxValue,
                            });
                            continue;
                        }
                    }
                } else {
                    0.0
                };
                let high = if high_kind == PseudoBoundKind::Value {
                    match datum_to_scalar(&ran.high_val[i]) {
                        Some(v) => v,
                        None => {
                            columns.push(ScalarRange {
                                low: 0.0,
                                high: 0.0,
                                low_kind: PseudoBoundKind::MinNotNull,
                                high_kind: PseudoBoundKind::MaxValue,
                            });
                            continue;
                        }
                    }
                } else {
                    0.0
                };
                columns.push(ScalarRange {
                    low,
                    high,
                    low_kind,
                    high_kind,
                });
            }
            index_ranges.push(IndexRange {
                columns,
                equal_prefix_len,
                low_exclude: ran.low_exclude,
                high_exclude: ran.high_exclude,
            });
        }
        pseudo_row_count_by_index_ranges(&index_ranges, table_row_count, None)
    }

    /// Go `GetPseudoRowCountByIntRanges` for the int-handle table path.
    #[must_use]
    pub fn pseudo_count_by_int_ranges(
        ranges: &super::types::Ranges,
        table_row_count: f64,
        unsigned: bool,
    ) -> f64 {
        if unsigned {
            let mapped: Vec<UnsignedIntRange> = ranges
                .iter()
                .map(|ran| UnsignedIntRange {
                    low: match ran.low_val.first() {
                        Some(Datum::UInt(v)) => *v,
                        Some(Datum::Int(v)) => (*v).max(0) as u64,
                        _ => 0,
                    },
                    high: match ran.high_val.first() {
                        Some(Datum::UInt(v)) => *v,
                        Some(Datum::Int(v)) => (*v).max(0) as u64,
                        _ => u64::MAX,
                    },
                    low_kind: ran
                        .low_val
                        .first()
                        .map_or(PseudoBoundKind::MinNotNull, bound_kind),
                    high_kind: ran
                        .high_val
                        .first()
                        .map_or(PseudoBoundKind::MaxValue, bound_kind),
                })
                .collect();
            return pseudo_row_count_by_unsigned_int_ranges(&mapped, table_row_count);
        }
        let mapped: Vec<SignedIntRange> = ranges
            .iter()
            .map(|ran| SignedIntRange {
                low: match ran.low_val.first() {
                    Some(Datum::Int(v)) => *v,
                    Some(Datum::UInt(v)) => (*v).min(i64::MAX as u64) as i64,
                    _ => i64::MIN,
                },
                high: match ran.high_val.first() {
                    Some(Datum::Int(v)) => *v,
                    Some(Datum::UInt(v)) => (*v).min(i64::MAX as u64) as i64,
                    _ => i64::MAX,
                },
                low_kind: ran
                    .low_val
                    .first()
                    .map_or(PseudoBoundKind::MinNotNull, bound_kind),
                high_kind: ran
                    .high_val
                    .first()
                    .map_or(PseudoBoundKind::MaxValue, bound_kind),
            })
            .collect();
        pseudo_row_count_by_signed_int_ranges(&mapped, table_row_count)
    }
}
