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

//! Direct source-contract tests for out-of-range cardinality arithmetic.

use tidb_planner::cardinality::out_of_range::{
    out_of_range_eq_selectivity, out_of_range_full_ndv, OUT_OF_RANGE_BETWEEN_RATE,
};

/// Source anchor: `TestOutOfRangeEstimation` in
/// `pkg/planner/cardinality/selectivity_test.go:135` exercises the equality
/// path through column/index row estimation.
#[test]
fn equality_selectivity_preserves_modification_and_ndv_clamps() {
    assert_eq!(OUT_OF_RANGE_BETWEEN_RATE, 100.0);
    assert_eq!(out_of_range_eq_selectivity(500, 1_000, 1_000), 0.0);
    assert_eq!(out_of_range_eq_selectivity(500, 1_100, 1_000), 0.002);
    assert_eq!(out_of_range_eq_selectivity(1, 1_100, 1_000), 0.01);
    assert_eq!(out_of_range_eq_selectivity(10_000, 1_001, 1_000), 0.0001);
}

/// Source anchor: `TestIssue64137` in
/// `pkg/planner/cardinality/selectivity_test.go:2819` pins the small-NDV,
/// all-TopN out-of-range behavior through an EXPLAIN result.
#[test]
fn full_ndv_estimate_preserves_source_zero_delete_and_smoothing_paths() {
    assert_eq!(
        out_of_range_full_ndv(10.0, 100.0, 100.0, 150.0, 1.0, 0),
        0.0
    );
    assert_eq!(
        out_of_range_full_ndv(10.0, 100.0, 100.0, 20_100.0, 1.0, 1),
        200.0
    );
    assert_eq!(out_of_range_full_ndv(0.0, 100.0, 100.0, 150.0, 1.0, 1), 1.0);
    assert_eq!(
        out_of_range_full_ndv(10.0, 3_000.0, 2_000.0, 2_000.0, 1.0, 1),
        20.0
    );
    assert_eq!(
        out_of_range_full_ndv(10.0, 3_000.0, 0.0, 2_000.0, 1.0, 1),
        20.0
    );
    assert_eq!(
        out_of_range_full_ndv(10.0, 100.0, 100.0, 150.0, 2.0, 1),
        1.0
    );
}

#[test]
fn full_ndv_estimate_keeps_floating_point_special_cases_source_shaped() {
    assert!(out_of_range_full_ndv(f64::NAN, 100.0, 100.0, 150.0, 1.0, 1).is_nan());
    assert_eq!(
        out_of_range_full_ndv(10.0, 100.0, 100.0, 150.0, 1.0, -1),
        1.0
    );
}
