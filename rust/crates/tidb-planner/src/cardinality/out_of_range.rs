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

//! Dependency-closed out-of-range cardinality arithmetic from
//! `pkg/planner/cardinality/selectivity.go`.
//!
//! These two source helpers only combine row counts, NDV, and modification
//! metadata. The surrounding histogram, TopN, session, and range owners are
//! intentionally not reconstructed here; callers provide already-normalized
//! scalar statistics and retain ownership of how the estimate is consumed.

/// Smoothing divisor used by TiDB for out-of-range estimates.
pub const OUT_OF_RANGE_BETWEEN_RATE: f64 = 100.0;

/// Estimates equality selectivity for a value outside analyzed statistics.
///
/// This is the arithmetic body of `outOfRangeEQSelectivity`. A non-positive
/// modification delta means the histogram already covers the current table,
/// while small NDV values are clamped to the source smoothing divisor.
#[must_use]
pub fn out_of_range_eq_selectivity(
    ndv: i64,
    realtime_row_count: i64,
    analyzed_row_count: i64,
) -> f64 {
    let increase_row_count = realtime_row_count - analyzed_row_count;
    if increase_row_count <= 0 {
        return 0.0;
    }
    let ndv = ndv.max(OUT_OF_RANGE_BETWEEN_RATE as i64);
    let selectivity = 1.0 / ndv as f64;
    let estimated_rows = selectivity * analyzed_row_count as f64;
    if estimated_rows > increase_row_count as f64 {
        increase_row_count as f64 / analyzed_row_count as f64
    } else {
        selectivity
    }
}

/// Estimates rows for an out-of-range value when TopN contains all analyzed
/// NDV values.
///
/// This preserves `outOfRangeFullNDV`, including deletion fallback, zero-NDV
/// square-root derivation, increase-factor scaling, smoothing, and the source
/// minimum estimate of one row.
#[must_use]
pub fn out_of_range_full_ndv(
    mut ndv: f64,
    orig_row_count: f64,
    mut not_null_count: f64,
    realtime_row_count: f64,
    increase_factor: f64,
    modify_count: i64,
) -> f64 {
    if modify_count == 0 {
        return 0.0;
    }

    let mut new_rows = realtime_row_count - orig_row_count;
    if not_null_count <= 0.0 {
        not_null_count = super::go_min(orig_row_count, realtime_row_count);
    }
    if new_rows < 0.0 {
        new_rows = super::go_min(not_null_count, realtime_row_count);
    }
    if ndv <= 0.0 {
        ndv = super::go_max(not_null_count, realtime_row_count).sqrt();
    } else {
        ndv *= increase_factor;
    }
    ndv = super::go_max(ndv, OUT_OF_RANGE_BETWEEN_RATE);
    super::go_max(1.0, new_rows / ndv)
}
