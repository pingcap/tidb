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

//! Dependency-closed tests for
//! `pkg/planner/cardinality/row_count_index.go:348`.
//!
//! The Go integration anchor is `TestRiskEqSkewRatio` at
//! `pkg/planner/cardinality/selectivity_test.go:2462`. These vectors retain
//! the arithmetic contract while leaving mock-store, histogram, TopN, and
//! session-variable ownership to the Go planner ring.

use tidb_planner::cardinality::{
    row_count_column::RowEstimate,
    uniform::{calculate_skew_ratio_counts, estimate_uniform_equality, UniformEqualityStats},
};

fn stats() -> UniformEqualityStats {
    UniformEqualityStats {
        histogram_ndv: 100,
        topn_len: 10,
        total_row_count: 1_000.0,
        not_null_count: 900.0,
        null_count: 100.0,
        realtime_row_count: 1_000.0,
        increase_factor: 1.0,
        modify_count: 0,
        risk_eq_skew_ratio: 0.0,
        topn_min_count: Some(20.0),
    }
}

#[test]
fn uniform_histogram_average_and_skew_bounds() {
    assert_eq!(
        estimate_uniform_equality(stats()),
        RowEstimate::default_est(10.0)
    );

    let mut skewed = stats();
    skewed.risk_eq_skew_ratio = 0.5;
    assert_eq!(
        estimate_uniform_equality(skewed),
        RowEstimate::new(15.0, 10.0, 20.0)
    );

    let mut without_topn = stats();
    without_topn.topn_len = 0;
    without_topn.topn_min_count = None;
    without_topn.risk_eq_skew_ratio = 0.5;
    assert_eq!(
        estimate_uniform_equality(without_topn),
        RowEstimate::new(405.0, 9.0, 801.0)
    );
}

#[test]
fn empty_histogram_preserves_topn_and_out_of_range_fallbacks() {
    let mut topn_only = stats();
    topn_only.histogram_ndv = 13;
    topn_only.topn_len = 10;
    topn_only.not_null_count = 0.0;
    assert_eq!(
        estimate_uniform_equality(topn_only),
        RowEstimate::default_est(19.0)
    );

    let mut modified = topn_only;
    modified.modify_count = 20;
    modified.realtime_row_count = 1_200.0;
    modified.increase_factor = 1.2;
    assert_eq!(
        estimate_uniform_equality(modified),
        RowEstimate::default_est(2.0)
    );

    let mut deleted = topn_only;
    deleted.modify_count = -20;
    deleted.realtime_row_count = 800.0;
    deleted.increase_factor = 0.8;
    assert_eq!(
        estimate_uniform_equality(deleted),
        RowEstimate::default_est(8.0)
    );
}

#[test]
fn skew_ratio_formula_matches_source_bounds() {
    assert_eq!(
        calculate_skew_ratio_counts(2.0, 10.0, 0.5),
        RowEstimate::new(6.0, 2.0, 10.0)
    );
    assert_eq!(
        calculate_skew_ratio_counts(2.0, 10.0, 1.0),
        RowEstimate::new(10.0, 2.0, 10.0)
    );
    assert_eq!(
        calculate_skew_ratio_counts(10.0, 2.0, 0.5),
        RowEstimate::new(10.0, 10.0, 2.0)
    );
}
