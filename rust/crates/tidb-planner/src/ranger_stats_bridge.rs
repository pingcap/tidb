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

//! The pseudo-statistics bridge: `ranger` ranges into the shapes
//! `cardinality::pseudo`'s Go-ported row counts consume — the
//! `CountAfterAccess` half of Go's `detachCondAndBuildRangeForPath`
//! (`core/stats.go:397`) for unanalyzed tables.

use tidb_datatype::Datum;

use crate::cardinality::pseudo::{
    pseudo_row_count_by_index_ranges as estimate_pseudo_index_ranges,
    pseudo_row_count_by_scalar_ranges, pseudo_row_count_by_signed_int_ranges,
    pseudo_row_count_by_unsigned_int_ranges, Range, PseudoBoundKind, ScalarRange,
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

/// Go `getPseudoRowCountByIndexRanges` (`cardinality/pseudo.go:173`,
/// unexported) over this port's range model.
#[must_use]
pub fn pseudo_count_by_ranges(ranges: &tidb_ranger::types::Ranges, table_row_count: f64) -> f64 {
    pseudo_count_by_index_ranges(ranges, table_row_count, None)
}

/// The same Go index-range estimator with the index's declared unique
/// column count. A fully bound unique key estimates one row; passing
/// `None` preserves the ordinary secondary-index behavior.
#[must_use]
pub fn pseudo_count_by_index_ranges(
    ranges: &[tidb_ranger::types::Range],
    table_row_count: f64,
    unique_columns: Option<usize>,
) -> f64 {
    let mut index_ranges = Vec::with_capacity(ranges.len());
    for ran in ranges {
        let equal_prefix_len = ran.prefix_equal_len().unwrap_or(0);
        let mut columns = Vec::with_capacity(ran.low_val.len());
        for i in 0..ran.low_val.len().min(ran.high_val.len()) {
            let low_kind = bound_kind(&ran.low_val[i]);
            let high_kind = bound_kind(&ran.high_val[i]);
            // Go's pseudo estimator uses only bound kinds and equality,
            // never numeric distance. Preserve collation-aware comparison
            // instead of converting strings or wide integers through f64.
            let low = 0.0;
            let high = if ran.low_val[i]
                .compare(&ran.high_val[i], ran.collators[i])
                .is_ok_and(|order| order == std::cmp::Ordering::Equal)
            {
                0.0
            } else {
                1.0
            };
            columns.push(ScalarRange {
                low,
                high,
                low_kind,
                high_kind,
            });
        }
        index_ranges.push(Range {
            columns,
            equal_prefix_len,
            low_exclude: ran.low_exclude,
            high_exclude: ran.high_exclude,
        });
    }
    estimate_pseudo_index_ranges(&index_ranges, table_row_count, unique_columns)
}

/// Go `getPseudoRowCountByColumnRanges` (`cardinality/pseudo.go:210`)
/// over one ordinary (non-handle) pseudo column. Unlike an index point,
/// an equality is not unique: it estimates `RealtimeCount / 1000`.
#[must_use]
pub fn pseudo_count_by_column_ranges(
    ranges: &tidb_ranger::types::Ranges,
    table_row_count: f64,
) -> f64 {
    let mapped = ranges
        .iter()
        .filter_map(|ran| {
            // Go indexes `ran.LowVal[0]` and `ran.HighVal[0]` directly.
            let low_datum = ran.low_val.first().expect("range bound missing low value");
            let high_datum = ran
                .high_val
                .first()
                .expect("range bound missing high value");
            let low_kind = bound_kind(low_datum);
            let high_kind = bound_kind(high_datum);
            let low = datum_to_scalar(low_datum).unwrap_or(0.0);
            let high = datum_to_scalar(high_datum).unwrap_or_else(|| {
                if low_datum == high_datum {
                    low
                } else {
                    low + 1.0
                }
            });
            Some(ScalarRange {
                low,
                high,
                low_kind,
                high_kind,
            })
        })
        .collect::<Vec<_>>();
    pseudo_row_count_by_scalar_ranges(&mapped, table_row_count)
}

/// Go's int-handle pseudo estimate: `getPseudoRowCountBySignedIntRanges`
/// and `getPseudoRowCountByUnsignedIntRanges` (`cardinality/pseudo.go:101`
/// and `:137`, both unexported), chosen by the handle's signedness as Go's
/// `GetRowCountByIntColumnRanges` chooses between them.
#[must_use]
pub fn pseudo_count_by_int_ranges(
    ranges: &tidb_ranger::types::Ranges,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pseudo_index_bounds_preserve_datum_equality() {
        let text = |value: &str| {
            Datum::String(tidb_datatype::StringDatum::new(
                value.as_bytes().to_vec(),
                tidb_datatype::Collation::Binary,
            ))
        };
        for (low, high, expected) in [
            (text("a"), text("a"), 10.0),
            (text("a"), text("z"), 250.0),
            (Datum::UInt(u64::MAX - 1), Datum::UInt(u64::MAX), 250.0),
        ] {
            let ranges = vec![tidb_ranger::types::Range {
                low_val: vec![low],
                high_val: vec![high],
                collators: vec![tidb_datatype::Collation::Binary],
                low_exclude: false,
                high_exclude: false,
            }];
            assert_eq!(
                pseudo_count_by_index_ranges(&ranges, 10000.0, None),
                expected,
                "{ranges:?}"
            );
        }
    }
}
