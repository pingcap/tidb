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

//! Histogram order-correlation arithmetic from `pkg/statistics/builder.go`.
//!
//! This leaf owns only the Pearson-correlation calculation over the caller's
//! sample count and ordinal cross-sum. Sample ordering, histogram building,
//! handle-column discovery, and persistence remain outside the crate.

/// Computes the correlation between physical row order and sorted order.
///
/// TiDB's `calcCorrelation` uses the closed-form sums for the two ordinal
/// vectors `0..sample_num` and receives only their cross-sum from the builder.
/// The single-item shortcut is source-visible; other values intentionally use
/// the direct floating-point formula, including its `NaN` result for a zero
/// sample count.
#[must_use]
pub fn calc_correlation(sample_num: i64, corr_xy_sum: f64) -> f64 {
    if sample_num == 1 {
        return 1.0;
    }

    let items_count = sample_num as f64;
    let corr_x_sum = (items_count - 1.0) * items_count / 2.0;
    let corr_x2_sum = (items_count - 1.0) * items_count * (2.0 * items_count - 1.0) / 6.0;
    (items_count * corr_xy_sum - corr_x_sum * corr_x_sum)
        / (items_count * corr_x2_sum - corr_x_sum * corr_x_sum)
}
