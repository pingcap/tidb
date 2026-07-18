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

//! Dependency-closed pseudo cardinality arithmetic from
//! `pkg/planner/cardinality/pseudo.go`.
//!
//! The Go implementation also inspects planner context, expression columns,
//! histograms, indexes, and MySQL Datum comparison.  Those owners are not
//! available in the seed planner, so this module keeps only the deterministic
//! equality/less/between/range formulas.  Callers supply already-normalized
//! scalar bounds and prefix-equality lengths; no statistics/catalog/session
//! facade is invented here.

/// Source pseudo divisor for equality predicates and one value's average
/// frequency.
pub const PSEUDO_EQUAL_RATE: f64 = 1_000.0;
/// Source pseudo divisor for one-sided less/greater predicates.
pub const PSEUDO_LESS_RATE: f64 = 3.0;
/// Source pseudo divisor for a bounded between predicate.
pub const PSEUDO_BETWEEN_RATE: f64 = 40.0;

/// A normalized range-bound kind from TiDB's Datum/ranger domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PseudoBoundKind {
    /// The accompanying scalar value is a concrete bound.
    Value,
    /// SQL NULL lower bound.
    Null,
    /// The smallest non-NULL value marker.
    MinNotNull,
    /// The largest value marker.
    MaxValue,
}

/// A signed integer pseudo range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SignedIntRange {
    /// Concrete low value (ignored for `Null`/`MinNotNull`).
    pub low: i64,
    /// Concrete high value (ignored for `MaxValue`).
    pub high: i64,
    /// Kind of the low bound.
    pub low_kind: PseudoBoundKind,
    /// Kind of the high bound.
    pub high_kind: PseudoBoundKind,
}

impl SignedIntRange {
    /// Creates a normalized signed range.
    #[must_use]
    pub const fn new(
        low: i64,
        high: i64,
        low_kind: PseudoBoundKind,
        high_kind: PseudoBoundKind,
    ) -> Self {
        Self {
            low,
            high,
            low_kind,
            high_kind,
        }
    }
}

/// An unsigned integer pseudo range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnsignedIntRange {
    /// Concrete low value (ignored for `Null`/`MinNotNull`).
    pub low: u64,
    /// Concrete high value (ignored for `MaxValue`).
    pub high: u64,
    /// Kind of the low bound.
    pub low_kind: PseudoBoundKind,
    /// Kind of the high bound.
    pub high_kind: PseudoBoundKind,
}

impl UnsignedIntRange {
    /// Creates a normalized unsigned range.
    #[must_use]
    pub const fn new(
        low: u64,
        high: u64,
        low_kind: PseudoBoundKind,
        high_kind: PseudoBoundKind,
    ) -> Self {
        Self {
            low,
            high,
            low_kind,
            high_kind,
        }
    }
}

/// A scalar numeric pseudo range for the generic Datum-range formula.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScalarRange {
    /// Numeric low value supplied by the caller.
    pub low: f64,
    /// Numeric high value supplied by the caller.
    pub high: f64,
    /// Kind of the low bound.
    pub low_kind: PseudoBoundKind,
    /// Kind of the high bound.
    pub high_kind: PseudoBoundKind,
}

impl ScalarRange {
    /// Creates a normalized scalar range.
    #[must_use]
    pub const fn new(
        low: f64,
        high: f64,
        low_kind: PseudoBoundKind,
        high_kind: PseudoBoundKind,
    ) -> Self {
        Self {
            low,
            high,
            low_kind,
            high_kind,
        }
    }
}

/// A composite-index pseudo range after the source has computed its equal
/// prefix length.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexRange {
    /// Per-column normalized bounds, in index order.
    pub columns: Vec<ScalarRange>,
    /// Number of leading columns equal to a concrete value.
    pub equal_prefix_len: usize,
    /// Whether the low bound is exclusive.
    pub low_exclude: bool,
    /// Whether the high bound is exclusive.
    pub high_exclude: bool,
}

impl IndexRange {
    /// Creates a source-shaped composite index range.
    #[must_use]
    pub fn new(
        columns: Vec<ScalarRange>,
        equal_prefix_len: usize,
        low_exclude: bool,
        high_exclude: bool,
    ) -> Self {
        Self {
            columns,
            equal_prefix_len,
            low_exclude,
            high_exclude,
        }
    }
}

/// Returns the pseudo average count for one equality value.
#[must_use]
pub fn pseudo_avg_count_per_value(table_row_count: f64) -> f64 {
    table_row_count / PSEUDO_EQUAL_RATE
}

/// Returns the equality estimate for a table with no histogram.
#[must_use]
pub fn pseudo_equal_count(table_row_count: f64) -> f64 {
    pseudo_avg_count_per_value(table_row_count)
}

/// Returns the one-sided less/greater estimate for a table with no histogram.
#[must_use]
pub fn pseudo_less_count(table_row_count: f64) -> f64 {
    table_row_count / PSEUDO_LESS_RATE
}

/// Returns the bounded between estimate for a table with no histogram.
#[must_use]
pub fn pseudo_between_count(table_row_count: f64) -> f64 {
    table_row_count / PSEUDO_BETWEEN_RATE
}

fn clamp_to_table(row_count: f64, table_row_count: f64) -> f64 {
    if row_count > table_row_count {
        table_row_count
    } else {
        row_count
    }
}

/// Estimates signed integer ranges using the source pseudo rates.
#[must_use]
pub fn pseudo_row_count_by_signed_int_ranges(
    ranges: &[SignedIntRange],
    table_row_count: f64,
) -> f64 {
    let mut row_count = 0.0;
    for range in ranges {
        let low = if matches!(
            range.low_kind,
            PseudoBoundKind::Null | PseudoBoundKind::MinNotNull
        ) {
            i64::MIN
        } else {
            range.low
        };
        let high = if range.high_kind == PseudoBoundKind::MaxValue {
            i64::MAX
        } else {
            range.high
        };
        let mut count = if low == i64::MIN && high == i64::MAX {
            table_row_count
        } else if low == i64::MIN || high == i64::MAX {
            pseudo_less_count(table_row_count)
        } else if low == high {
            1.0
        } else {
            pseudo_between_count(table_row_count)
        };
        let width = high.wrapping_sub(low);
        if width > 0 && count > width as f64 {
            count = width as f64;
        }
        row_count += count;
    }
    clamp_to_table(row_count, table_row_count)
}

/// Estimates unsigned integer ranges using the source pseudo rates.
#[must_use]
pub fn pseudo_row_count_by_unsigned_int_ranges(
    ranges: &[UnsignedIntRange],
    table_row_count: f64,
) -> f64 {
    let mut row_count = 0.0;
    for range in ranges {
        let low = if matches!(
            range.low_kind,
            PseudoBoundKind::Null | PseudoBoundKind::MinNotNull
        ) {
            0
        } else {
            range.low
        };
        let high = if range.high_kind == PseudoBoundKind::MaxValue {
            u64::MAX
        } else {
            range.high
        };
        let mut count = if low == 0 && high == u64::MAX {
            table_row_count
        } else if low == 0 || high == u64::MAX {
            pseudo_less_count(table_row_count)
        } else if low == high {
            1.0
        } else {
            pseudo_between_count(table_row_count)
        };
        let width = high.wrapping_sub(low);
        if width > 0 && count > width as f64 {
            count = width as f64;
        }
        row_count += count;
    }
    clamp_to_table(row_count, table_row_count)
}

/// Estimates generic normalized scalar ranges using equality/less/between
/// markers.  The caller performs any source Datum/collation comparison before
/// constructing this numeric range; no comparison/session error is invented.
#[must_use]
pub fn pseudo_row_count_by_scalar_ranges(ranges: &[ScalarRange], table_row_count: f64) -> f64 {
    let mut row_count = 0.0;
    for range in ranges {
        let count = if range.low_kind == PseudoBoundKind::Null
            && range.high_kind == PseudoBoundKind::MaxValue
        {
            table_row_count
        } else if range.low_kind == PseudoBoundKind::MinNotNull {
            let null_count = pseudo_equal_count(table_row_count);
            if range.high_kind == PseudoBoundKind::MaxValue {
                table_row_count - null_count
            } else {
                pseudo_less_count(table_row_count) - null_count
            }
        } else if range.high_kind == PseudoBoundKind::MaxValue {
            pseudo_less_count(table_row_count)
        } else if range.low == range.high {
            pseudo_equal_count(table_row_count)
        } else {
            pseudo_between_count(table_row_count)
        };
        row_count += count;
    }
    clamp_to_table(row_count, table_row_count)
}

/// Estimates composite-index ranges after the caller supplies equal-prefix
/// lengths.  `unique_columns` corresponds to Go's `colsLen`; `None` means the
/// index is not known to be unique.
#[must_use]
pub fn pseudo_row_count_by_index_ranges(
    ranges: &[IndexRange],
    table_row_count: f64,
    unique_columns: Option<usize>,
) -> f64 {
    if table_row_count == 0.0 {
        return 0.0;
    }
    let mut total_count = 0.0;
    for range in ranges {
        if range.columns.is_empty() {
            continue;
        }
        let mut prefix_len = range.equal_prefix_len;
        if unique_columns == Some(prefix_len) && !range.low_exclude && !range.high_exclude {
            total_count += 1.0;
            continue;
        }
        prefix_len = prefix_len.min(range.columns.len() - 1);
        let row_count = pseudo_row_count_by_scalar_ranges(
            &range.columns[prefix_len..=prefix_len],
            table_row_count,
        );
        let mut count = row_count;
        for _ in 0..prefix_len {
            count /= 100.0;
        }
        total_count += count;
    }
    if total_count > table_row_count {
        table_row_count / PSEUDO_LESS_RATE
    } else {
        total_count
    }
}
