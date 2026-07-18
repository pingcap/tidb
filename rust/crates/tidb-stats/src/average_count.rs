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

//! Average rows-per-value arithmetic from `pkg/statistics/histogram.go`.
//!
//! This helper starts with caller-owned histogram counts and NDV. Histogram
//! construction, Datum comparisons, and planner range policy remain external.

/// Returns the average row count per non-null value after scaling counts to a
/// realtime row count.
#[must_use]
pub fn avg_count_per_not_null_value(
    realtime_row_count: i64,
    histogram_total_count: f64,
    histogram_not_null_count: f64,
    histogram_ndv: f64,
) -> f64 {
    let increase_factor = if histogram_total_count == 0.0 {
        1.0
    } else {
        realtime_row_count as f64 / histogram_total_count
    };
    let total_not_null = histogram_not_null_count * increase_factor;
    let current_ndv = source_max(histogram_ndv * increase_factor, 1.0);
    total_not_null / current_ndv
}

fn source_max(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        f64::NAN
    } else if left == 0.0 && right == 0.0 {
        if left.is_sign_positive() || right.is_sign_positive() {
            0.0
        } else {
            -0.0
        }
    } else if left > right {
        left
    } else {
        right
    }
}
