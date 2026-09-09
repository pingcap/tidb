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

#![allow(missing_docs)]

//! GO PORT of `pkg/planner/cardinality/ndv_test.go` (items 3-6 of the
//! pkg/planner.part1 slice).
//!
//! The two running tests drive the dependency-closed estimators from
//! `pkg/planner/cardinality/ndv.go`:
//! `ScaleNDV` (`ndv.go:215`, transcreated as
//! [`tidb_planner::cardinality::ndv::scale_ndv`]) and
//! `EstimateColsNDVWithMatchedLen` (`ndv.go:87`, transcreated as
//! [`tidb_planner::cardinality::ndv::estimate_cols_ndv_with_matched_len`]).
//! Go passes a session whose `RiskScaleNDVSkewRatio` /
//! `RiskGroupNDVSkewRatio` variable is 0 or mocked; here that value is a
//! plain argument because the Rust leaf takes caller-owned statistics.

use tidb_planner::cardinality::ndv::{estimate_cols_ndv_with_matched_len, scale_ndv, GroupNdv};

/// `pkg/planner/cardinality/property.StatsInfo` shape used by the group-NDV
/// test: row count plus per-column NDVs plus observed group NDVs.
struct StatsProfile {
    row_count: f64,
    column_ndvs: Vec<(i64, f64)>,
    group_ndvs: Vec<GroupNdv>,
}

#[test]
fn scale_ndv_with_uniform_ratio_matches_golden_pairs() {
    // pkg/planner/cardinality/ndv_test.go:31 TestScaleNDV.
    // The session sets tidb_opt_scale_ndv_skew_ratio = 0 before every case
    // (`tk.MustExec("set @@tidb_opt_scale_ndv_skew_ratio = 0")`,
    // ndv_test.go:34), so the skew blend reduces to uniform-only estimation
    // and `scale_ndv(..., skew_ratio=0.0)` is exactly the call Go makes.
    let cases: [(f64, f64, f64, f64); 9] = [
        // (original_ndv, original_rows, selected_rows, new_ndv)
        (0.0, 0.0, 0.0, 0.0),
        (10.0, 0.0, 100.0, 0.0),
        (10.0, 100.0, 100.0, 10.0),
        (10.0, 100.0, 1.0, 1.0),
        (10.0, 100.0, 2.0, 1.83),
        (10.0, 100.0, 10.0, 6.51),
        (10.0, 100.0, 50.0, 9.99),
        (10.0, 100.0, 80.0, 10.00),
        (10.0, 100.0, 90.0, 10.00),
    ];
    for (original_ndv, original_rows, selected_rows, new_ndv) in cases {
        let result = scale_ndv(original_ndv, original_rows, selected_rows, 0.0);
        assert_eq!(
            format!("{new_ndv:.2}"),
            format!("{result:.2}"),
            "case ({original_ndv}, {original_rows}, {selected_rows})"
        );
    }
}

#[test]
fn estimate_cols_ndv_with_matched_len_blends_by_group_skew_ratio() {
    // pkg/planner/cardinality/ndv_test.go:113 TestEstimateColsNDVWithExponentialBackoff.
    // Schema holds columns a(1), b(2), c(3) with NDVs 1000/500/10 and one
    // GroupNDV over (a,b,c) with NDV 5000; profile RowCount is 100000.
    let stats = StatsProfile {
        row_count: 100_000.0,
        column_ndvs: vec![(1, 1000.0), (2, 500.0), (3, 10.0)],
        group_ndvs: vec![GroupNdv {
            columns: vec![1, 2, 3],
            ndv: 5000.0,
        }],
    };

    // Test 1: individual columns return their own NDV regardless of context
    // (ndv_test.go:138-148).
    assert_eq!(
        estimate_cols_ndv_with_matched_len(
            &[1],
            &stats.column_ndvs,
            stats.row_count,
            &stats.group_ndvs,
            0.0,
        ),
        (1000.0, 1)
    );
    assert_eq!(
        estimate_cols_ndv_with_matched_len(
            &[2],
            &stats.column_ndvs,
            stats.row_count,
            &stats.group_ndvs,
            1.0,
        ),
        (500.0, 1)
    );
    assert_eq!(
        estimate_cols_ndv_with_matched_len(
            &[3],
            &stats.column_ndvs,
            stats.row_count,
            &stats.group_ndvs,
            1.0,
        ),
        (10.0, 1)
    );

    // Test 2: exact GroupNDV match returns the group's NDV and matched length
    // (ndv_test.go:155-160).
    assert_eq!(
        estimate_cols_ndv_with_matched_len(
            &[1, 2, 3],
            &stats.column_ndvs,
            stats.row_count,
            &stats.group_ndvs,
            0.0,
        ),
        (5000.0, 3)
    );
    assert_eq!(
        estimate_cols_ndv_with_matched_len(
            &[1, 2, 3],
            &stats.column_ndvs,
            stats.row_count,
            &stats.group_ndvs,
            1.0,
        ),
        (5000.0, 3)
    );

    // Test 3: two-column combinations without an exact group match are blended
    // by RiskGroupNDVSkewRatio (ndv_test.go:162-201).
    let conservative_disabled =
        estimate_cols_ndv_with_matched_len(&[1, 2], &stats.column_ndvs, stats.row_count, &[], 0.0);
    assert!((conservative_disabled.0 - 1000.0).abs() <= 0.1);
    assert_eq!(conservative_disabled.1, 1);

    let exponential_enabled =
        estimate_cols_ndv_with_matched_len(&[1, 2], &stats.column_ndvs, stats.row_count, &[], 1.0);
    let expected_exponential = 1000.0 * 500_f64.sqrt();
    assert!((exponential_enabled.0 - expected_exponential).abs() <= 0.1);
    assert_eq!(exponential_enabled.1, 1);
    assert_ne!(conservative_disabled.0, exponential_enabled.0);
    assert!(exponential_enabled.0 > conservative_disabled.0);

    let blended = estimate_cols_ndv_with_matched_len(&[1, 2], &stats.column_ndvs, stats.row_count, &[], 0.5);
    let expected_blended = 1000.0 + (expected_exponential - 1000.0) * 0.5;
    assert!((blended.0 - expected_blended).abs() <= 0.1);
    assert!(blended.0 > conservative_disabled.0);
    assert!(blended.0 < exponential_enabled.0);

    // Additional pairs under exponential backoff (ndv_test.go:202-214).
    let ac = estimate_cols_ndv_with_matched_len(&[1, 3], &stats.column_ndvs, stats.row_count, &[], 1.0);
    assert!((ac.0 - 1000.0 * 10_f64.sqrt()).abs() <= 0.1);
    assert_eq!(ac.1, 1);
    let bc = estimate_cols_ndv_with_matched_len(&[2, 3], &stats.column_ndvs, stats.row_count, &[], 1.0);
    assert!((bc.0 - 500.0 * 10_f64.sqrt()).abs() <= 0.1);
    assert_eq!(bc.1, 1);

    // Test 4: without GroupNDVs every combination falls through to the
    // exponential-backoff path over sorted-descending per-column NDVs
    // (ndv_test.go:216-257).
    let ab_no_group =
        estimate_cols_ndv_with_matched_len(&[1, 2], &stats.column_ndvs, stats.row_count, &[], 1.0);
    assert!((ab_no_group.0 - 1000.0 * 500_f64.sqrt()).abs() <= 0.1);
    assert_eq!(ab_no_group.1, 1);
    let ac_no_group =
        estimate_cols_ndv_with_matched_len(&[1, 3], &stats.column_ndvs, stats.row_count, &[], 1.0);
    assert!((ac_no_group.0 - 1000.0 * 10_f64.sqrt()).abs() <= 0.1);
    let abc_no_group = estimate_cols_ndv_with_matched_len(
        &[1, 2, 3],
        &stats.column_ndvs,
        stats.row_count,
        &[],
        1.0,
    );
    // Sorted descending: [1000, 500, 10].
    assert!((abc_no_group.0 - 1000.0 * 500_f64.sqrt() * 10_f64.sqrt().sqrt()).abs() <= 0.1);
    assert_eq!(abc_no_group.1, 1);

    // Empty columns return 1.0 without consulting any risk variable, and a
    // single column stays on the conservative path even when backoff is fully
    // enabled (ndv_test.go:259-271).
    assert_eq!(
        estimate_cols_ndv_with_matched_len(&[], &stats.column_ndvs, stats.row_count, &[], 1.0),
        (1.0, 1)
    );
    assert_eq!(
        estimate_cols_ndv_with_matched_len(&[1], &stats.column_ndvs, stats.row_count, &[], 1.0),
        (1000.0, 1)
    );
}

/// GO PORT of `pkg/planner/cardinality/ndv_test.go:58
/// TestOptScaleNDVSkewRatioSetVar`.
///
/// Inserts 100 rows `(i%20, i)` into t(a int, b int, key(a), key(b)),
/// analyzes, then re-plans `select distinct(a) from t where b<50` under three
/// hint-injected `tidb_opt_scale_ndv_skew_ratio` values and pins the HashAgg
/// estimates in decreasing order: 19.44 at ratio 0, 14.82 at 0.5, 10.20 at 1
/// (ndv_test.go:88-94). Pins that raising the scale-NDV skew ratio pulls the
/// aggregate estimate toward the skewed (row-count-proportional) extreme.
#[test]
#[ignore = "go-parity-gap: needs live EXPLAIN planning with analyze-built histograms and set_var hints"]
fn opt_scale_ndv_skew_ratio_set_var_changes_distinct_aggregate_estimates() {}

/// GO PORT of `pkg/planner/cardinality/ndv_test.go:79 TestIssue54812`.
///
/// Builds table t(a int, b int, key(a), key(b)) holding 100 rows `(i, 1)`
/// plus 10 bulk inserts of `(100, 2)` (1100 rows total), analyzes and pins the
/// `explain format='brief'` plan for `select distinct(a) from t where b=1`
/// (ndv_test.go:106-112): HashAgg/TableReader/HashAgg all estimate 65.23 above
/// Selection 100.00 over TableFullScan 1100.00 -- the selection rows scaled by
/// the distinct-group NDV instead of collapsing onto it.
#[test]
#[ignore = "go-parity-gap: needs live EXPLAIN goldens over the analyze/stats pipeline"]
fn issue_54812_distinct_hashagg_scales_selection_rows() {}
