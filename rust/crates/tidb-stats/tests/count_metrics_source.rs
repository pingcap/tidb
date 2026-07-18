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

//! Source-backed tests for histogram count arithmetic.

use tidb_stats::HistogramCountSummary;

#[test]
fn source_nonempty_histogram_uses_last_bucket_for_nonnull_count() {
    let summary = HistogramCountSummary::new(true, 90, 10);
    assert_eq!(summary.not_null_count(), 90.0);
    assert_eq!(summary.total_row_count(), 100.0);
    assert_eq!(summary.abs_row_count_difference(125), 25.0);
    assert_eq!(summary.increase_factor(150), 1.5);
}

#[test]
fn source_empty_histogram_ignores_last_bucket_and_avoids_zero_division() {
    let summary = HistogramCountSummary::new(false, 90, 10);
    assert_eq!(summary.not_null_count(), 0.0);
    assert_eq!(summary.total_row_count(), 10.0);
    assert_eq!(summary.abs_row_count_difference(25), 15.0);

    let empty = HistogramCountSummary::new(false, 0, 0);
    assert_eq!(empty.total_row_count(), 0.0);
    assert_eq!(empty.increase_factor(100), 1.0);
}

#[test]
fn source_count_arithmetic_preserves_negative_and_fractional_boundaries() {
    let summary = HistogramCountSummary::new(true, -5, -2);
    assert_eq!(summary.not_null_count(), -5.0);
    assert_eq!(summary.total_row_count(), -7.0);
    assert_eq!(summary.abs_row_count_difference(-2), 5.0);
    assert_eq!(summary.increase_factor(14), -2.0);
}
