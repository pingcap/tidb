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

//! Dependency-closed tests for `pkg/planner/cardinality/row_count_column.go`.

use tidb_planner::cardinality::{
    pseudo::{PseudoBoundKind, ScalarRange},
    row_count_column::{
        estimate_column_row_count, pseudo_row_count_with_partial_stats, ColumnRangeInput,
        ColumnRangeStats, PartialStatsRange, RowEstimate,
    },
};

#[test]
fn point_ranges_preserve_primary_key_and_scaled_column_rules() {
    let point = ScalarRange::new(7.0, 7.0, PseudoBoundKind::Value, PseudoBoundKind::Value);
    let stats = ColumnRangeStats::point(RowEstimate::default_est(2.0));
    let input = [ColumnRangeInput::new(point, false, false, stats)];

    assert_eq!(
        estimate_column_row_count(&input, 100.0, 100.0, 0.0, 1.5, true, 0.01),
        RowEstimate::default_est(1.0)
    );
    assert_eq!(
        estimate_column_row_count(&input, 100.0, 100.0, 0.0, 1.5, false, 0.01),
        RowEstimate::default_est(3.0)
    );
}

#[test]
fn interval_ranges_adjust_exclusive_boundaries_and_null() {
    let interval = ScalarRange::new(1.0, 10.0, PseudoBoundKind::Value, PseudoBoundKind::Value);
    let stats = ColumnRangeStats::new(
        RowEstimate::default_est(0.0),
        RowEstimate::new(10.0, 8.0, 12.0),
        RowEstimate::new(2.0, 1.0, 3.0),
        RowEstimate::default_est(1.0),
        None,
    );
    let input = [ColumnRangeInput::new(interval, true, false, stats)];
    assert_eq!(
        estimate_column_row_count(&input, 100.0, 100.0, 0.0, 1.0, false, 0.01),
        RowEstimate::new(9.0, 8.0, 10.0)
    );

    let null_to_value = ScalarRange::new(0.0, 10.0, PseudoBoundKind::Null, PseudoBoundKind::Value);
    let null_stats = ColumnRangeStats::new(
        RowEstimate::default_est(0.0),
        RowEstimate::default_est(2.0),
        RowEstimate::default_est(0.0),
        RowEstimate::default_est(1.0),
        None,
    );
    let input = [ColumnRangeInput::new(
        null_to_value,
        false,
        false,
        null_stats,
    )];
    assert_eq!(
        estimate_column_row_count(&input, 100.0, 97.0, 3.0, 1.0, false, 0.01),
        RowEstimate::default_est(6.0)
    );
}

#[test]
fn partial_stats_preserve_product_and_correlated_bounds() {
    let ranges = [
        PartialStatsRange::new(vec![10.0, 20.0]),
        PartialStatsRange::new(vec![50.0, 10.0]),
    ];
    assert_eq!(
        pseudo_row_count_with_partial_stats(&ranges, 100.0, false),
        tidb_planner::cardinality::row_count_column::PartialStatsRowCount {
            total_count: 7.0,
            max_count: 20.0,
        }
    );
    assert_eq!(
        pseudo_row_count_with_partial_stats(&ranges, 100.0, true),
        tidb_planner::cardinality::row_count_column::PartialStatsRowCount {
            total_count: 60.0,
            max_count: 0.0,
        }
    );
    assert_eq!(
        pseudo_row_count_with_partial_stats(&[], 0.0, false),
        tidb_planner::cardinality::row_count_column::PartialStatsRowCount {
            total_count: 0.0,
            max_count: 0.0,
        }
    );
}
