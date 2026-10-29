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

//! Dependency-closed row-count arithmetic from
//! `pkg/planner/cardinality/row_count_column.go`.
//!
//! The Go entrypoints in this source unit receive a `PlanContext`, `HistColl`,
//! `statistics.Column`, and encoded `Datum` values.  Those owners are not in
//! the seed planner yet.  This leaf therefore accepts normalized scalar
//! bounds and caller-supplied statistics estimates, then ports the arithmetic
//! that combines point, interval, boundary, and out-of-range estimates.  It
//! does not construct a histogram/session/catalog facade.

use super::pseudo::{PseudoBoundKind, ScalarRange};

/// The source three-valued row-count estimate (`Est`, `MinEst`, `MaxEst`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RowEstimate {
    /// Default estimate used by the planner.
    pub est: f64,
    /// Lower uncertainty estimate.
    pub min_est: f64,
    /// Upper uncertainty estimate.
    pub max_est: f64,
}

impl RowEstimate {
    /// Creates a row estimate with explicit default/minimum/maximum values.
    #[must_use]
    pub const fn new(est: f64, min_est: f64, max_est: f64) -> Self {
        Self {
            est,
            min_est,
            max_est,
        }
    }

    /// Creates an estimate whose three values are identical.
    #[must_use]
    pub const fn default_est(value: f64) -> Self {
        Self::new(value, value, value)
    }

    /// Adds another estimate field-by-field.
    pub fn add(&mut self, other: Self) {
        self.est += other.est;
        self.min_est += other.min_est;
        self.max_est += other.max_est;
    }

    /// Adds one value to all three estimate fields.
    pub fn add_all(&mut self, value: f64) {
        self.est += value;
        self.min_est += value;
        self.max_est += value;
    }

    /// Subtracts another estimate field-by-field.
    pub fn subtract(&mut self, other: Self) {
        self.est -= other.est;
        self.min_est -= other.min_est;
        self.max_est -= other.max_est;
    }

    /// Multiplies all three estimate fields by one scale factor.
    pub fn multiply_all(&mut self, factor: f64) {
        self.est *= factor;
        self.min_est *= factor;
        self.max_est *= factor;
    }

    /// Divides all three estimate fields by one scale factor.
    pub fn divide_all(&mut self, factor: f64) {
        self.est /= factor;
        self.min_est /= factor;
        self.max_est /= factor;
    }

    /// Clamps all fields and keeps minimum/default/maximum ordering.
    ///
    /// This preserves the source `RowEstimate.Clamp` order, including the
    /// unusual `min > max` behavior of Go's `mathutil.Clamp`.
    pub fn clamp(&mut self, lower: f64, upper: f64) {
        self.est = clamp_value(self.est, lower, upper);
        self.min_est = go_min(self.min_est, self.est);
        self.min_est = clamp_value(self.min_est, lower, upper);
        self.max_est = go_max(self.max_est, self.est);
        self.max_est = clamp_value(self.max_est, lower, upper);
    }
}

/// Point, interval, boundary, and optional out-of-range statistics supplied
/// by the future histogram owner for one normalized range.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ColumnRangeStats {
    /// Estimate for an inclusive point range.
    pub point: RowEstimate,
    /// Histogram estimate for the half-open interval `[low, high)`.
    pub interval: RowEstimate,
    /// Equality estimate at the low boundary.
    pub low_equal: RowEstimate,
    /// Equality estimate at the high boundary.
    pub high_equal: RowEstimate,
    /// Optional estimate for values outside the loaded histogram.
    pub out_of_range: Option<RowEstimate>,
}

impl ColumnRangeStats {
    /// Creates source-shaped range statistics.
    #[must_use]
    pub const fn new(
        point: RowEstimate,
        interval: RowEstimate,
        low_equal: RowEstimate,
        high_equal: RowEstimate,
        out_of_range: Option<RowEstimate>,
    ) -> Self {
        Self {
            point,
            interval,
            low_equal,
            high_equal,
            out_of_range,
        }
    }

    /// Creates statistics for a point-only caller that has no interval or
    /// out-of-range contribution.
    #[must_use]
    pub const fn point(point: RowEstimate) -> Self {
        Self::new(
            point,
            RowEstimate::default_est(0.0),
            RowEstimate::default_est(0.0),
            RowEstimate::default_est(0.0),
            None,
        )
    }
}

/// One normalized scalar range plus the statistics needed to combine it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ColumnRangeInput {
    /// Normalized scalar bounds and bound-marker kinds.
    pub range: ScalarRange,
    /// Whether the low endpoint is excluded.
    pub low_exclude: bool,
    /// Whether the high endpoint is excluded.
    pub high_exclude: bool,
    /// Caller-supplied histogram/TopN estimates for this range.
    pub stats: ColumnRangeStats,
}

impl ColumnRangeInput {
    /// Creates one normalized range input.
    #[must_use]
    pub const fn new(
        range: ScalarRange,
        low_exclude: bool,
        high_exclude: bool,
        stats: ColumnRangeStats,
    ) -> Self {
        Self {
            range,
            low_exclude,
            high_exclude,
            stats,
        }
    }
}

/// Source-shaped result of partial index statistics estimation.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PartialStatsRowCount {
    /// Product-selectivity estimate for all index ranges.
    pub total_count: f64,
    /// Correlated-column upper estimate for all index ranges.
    pub max_count: f64,
}

/// One index range represented by per-column row estimates.
#[derive(Clone, Debug, PartialEq)]
pub struct PartialStatsRange {
    /// Column estimates in index order, already produced by the column
    /// statistics owner.
    pub column_row_counts: Vec<f64>,
}

impl PartialStatsRange {
    /// Creates a source-shaped partial-statistics range.
    #[must_use]
    pub fn new(column_row_counts: Vec<f64>) -> Self {
        Self { column_row_counts }
    }
}

fn clamp_value(value: f64, lower: f64, upper: f64) -> f64 {
    if value >= upper {
        upper
    } else if value <= lower {
        lower
    } else {
        value
    }
}

// Go's built-in min/max propagate NaN and preserve a deterministic signed-zero
// result. Keep these helpers local so primitive f64::min/max cannot silently
// alter the source arithmetic.
fn go_min(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        return f64::NAN;
    }
    if left == 0.0 && left == right {
        return if left.is_sign_negative() { left } else { right };
    }
    if left < right {
        left
    } else {
        right
    }
}

fn go_max(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        return f64::NAN;
    }
    if left == 0.0 && left == right {
        return if left.is_sign_negative() { right } else { left };
    }
    if left > right {
        left
    } else {
        right
    }
}

fn same_bound(range: &ScalarRange) -> bool {
    (range.low_kind == range.high_kind && range.low_kind != PseudoBoundKind::Value)
        || (range.low_kind == PseudoBoundKind::Value
            && range.high_kind == PseudoBoundKind::Value
            && range.low == range.high)
}

/// Estimates a column's row count from normalized ranges and caller-owned
/// statistics estimates.
///
/// This is the arithmetic body of Go's `getColumnRowCount`: point ranges use
/// the primary-key-at-most-one rule, intervals adjust their low/high equality
/// boundaries and NULL endpoint, and each interval may add a supplied
/// out-of-range estimate when it does not already cover the full table. The
/// final result is clamped to `[1, realtime_row_count]`, matching the source
/// `RowEstimate.Clamp` call. `full_range_tolerance` is the caller's cost
/// tolerance (TiDB currently supplies `cost.ToleranceFactor`).
#[must_use]
pub fn estimate_column_row_count(
    ranges: &[ColumnRangeInput],
    realtime_row_count: f64,
    not_null_count: f64,
    null_count: f64,
    increase_factor: f64,
    pk_is_handle: bool,
    full_range_tolerance: f64,
) -> RowEstimate {
    let mut total = RowEstimate::default_est(0.0);

    for range in ranges {
        let is_point = same_bound(&range.range);
        if is_point && !range.low_exclude && !range.high_exclude {
            if pk_is_handle {
                total.add_all(1.0);
            } else {
                let mut count = range.stats.point;
                count.multiply_all(increase_factor);
                total.add(count);
            }
            continue;
        }
        if is_point {
            continue;
        }

        let mut count = range.stats.interval;
        if range.low_exclude
            && range.range.low_kind != PseudoBoundKind::Null
            && range.range.low_kind != PseudoBoundKind::MaxValue
            && range.range.low_kind != PseudoBoundKind::MinNotNull
        {
            count.subtract(range.stats.low_equal);
            count.clamp(0.0, not_null_count);
        }
        if !range.low_exclude && range.range.low_kind == PseudoBoundKind::Null {
            count.add_all(null_count);
        }
        if !range.high_exclude
            && range.range.high_kind != PseudoBoundKind::MaxValue
            && range.range.high_kind != PseudoBoundKind::MinNotNull
        {
            count.add(range.stats.high_equal);
        }
        count.clamp(0.0, realtime_row_count);
        count.multiply_all(increase_factor);

        let at_full_range = count.est >= realtime_row_count * (1.0 - full_range_tolerance);
        if !at_full_range {
            if let Some(out_of_range) = range.stats.out_of_range {
                count.add(out_of_range);
            }
        }
        total.add(count);
    }

    total.clamp(1.0, realtime_row_count);
    total
}

/// Combines per-column estimates when an index has no histogram but column
/// statistics are available.
///
/// This ports the arithmetic body of Go's `getPseudoRowCountWithPartialStats`.
/// Callers provide normalized per-column row counts; no expression columns,
/// histogram collection, or planner/session context is synthesized.
#[must_use]
pub fn pseudo_row_count_with_partial_stats(
    ranges: &[PartialStatsRange],
    table_row_count: f64,
    single_column_index: bool,
) -> PartialStatsRowCount {
    if table_row_count == 0.0 {
        return PartialStatsRowCount {
            total_count: 0.0,
            max_count: 0.0,
        };
    }
    if single_column_index {
        let count = ranges
            .iter()
            .flat_map(|range| range.column_row_counts.first().copied())
            .sum();
        return PartialStatsRowCount {
            total_count: count,
            max_count: 0.0,
        };
    }

    let mut total_count = 0.0;
    let mut max_count = 0.0;
    for range in ranges {
        let mut selectivity = 1.0;
        let mut correlated_selectivity = 1.0;
        for count in &range.column_row_counts {
            let temp_selectivity = *count / table_row_count;
            selectivity *= temp_selectivity;
            correlated_selectivity = go_min(correlated_selectivity, temp_selectivity);
        }
        total_count += selectivity * table_row_count;
        max_count += correlated_selectivity * table_row_count;
    }
    total_count = clamp_value(total_count, 1.0, table_row_count);
    PartialStatsRowCount {
        total_count,
        max_count,
    }
}
