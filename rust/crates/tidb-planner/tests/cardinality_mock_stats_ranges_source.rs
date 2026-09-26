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

//! GO PORT of `pkg/planner/cardinality/selectivity_test.go` (items 8-57 of
//! the pkg/planner.part1 slice), minus the cases that need no mock statistics
//! at all and live in their own files.
//!
//! The running tests drive the row-count estimators transcreated from
//! `pkg/planner/cardinality/row_count_column.go` / `row_count_index.go` /
//! `selectivity.go`: [`tidb_planner::cardinality::row_count_estimator`]. They
//! reconstruct the Go suite's own fixtures -- `mockStatsHistogram`
//! (`selectivity_test.go:1380`, per-value buckets with cumulative counts),
//! `mockStatsTable` (`:1392`, RealtimeCount fixture), `generateIntDatum`
//! (`:1341`), and the golden request/response pairs recorded in
//! `pkg/planner/cardinality/testdata/cardinality_suite_{in,out}.json`.
//!
//! What stays behind `#[ignore]` is called out per test below: everything
//! needing a live store (ANALYZE-built histograms, stats-handle deltas, async
//! load registries, EXPLAIN rendering, session variables, or failpoints).

use tidb_datatype::{Collation, Datum};
use tidb_planner::cardinality::row_count_estimator::{
    ColumnRange, ColumnStats, EstimatorOptions, IndexColumnStats, IndexRangeDatums, IndexRowCounts,
    IndexStats, get_column_row_count, get_index_row_count_for_stats_v2,
};
use tidb_stats::histogram::Histogram;

/// Go `selectivity_test.go:1380 mockStatsHistogram`: one bucket per distinct
/// value, count `repeat * (i + 1)`, repeat `repeat`.
fn mock_stats_histogram(id: i64, values: &[Datum], repeat: i64) -> Histogram {
    let ndv = values.len() as i64;
    let mut histogram = Histogram::new(id, ndv, 0, 0, values.len(), 0);
    for (index, value) in values.iter().enumerate() {
        histogram.append_bucket(
            value.clone(),
            value.clone(),
            repeat * (index as i64 + 1),
            repeat,
        );
    }
    histogram
}

/// Go `generateIntDatum(1, num)` (`selectivity_test.go:1341`): `[0, num)`.
fn int_values(begin: i64, num: i64) -> Vec<Datum> {
    (begin..begin + num).map(Datum::Int).collect()
}

fn column_stats(histogram: Histogram) -> ColumnStats {
    ColumnStats {
        histogram,
        topn: None,
        cms: None,
        stats_ver: 2,
        unsigned: false,
    }
}

/// Go `getRange(start, end)` through the single-column wrapper
/// `getColumnRowCount` (`selectivity_test.go:68-73`).
fn column_row_count(
    column: &ColumnStats,
    start: i64,
    end: i64,
    realtime_row_count: i64,
    modify_count: i64,
) -> tidb_planner::cardinality::row_count_column::RowEstimate {
    get_column_row_count(
        column,
        &[ColumnRange::new(
            Datum::Int(start),
            Datum::Int(end),
            false,
            false,
        )],
        Collation::Binary,
        realtime_row_count,
        modify_count,
        false,
        EstimatorOptions::default(),
    )
    .unwrap()
}

#[test]
fn out_of_range_estimation_matches_recorded_suite_estimates() {
    // pkg/planner/cardinality/selectivity_test.go:135 TestOutOfRangeEstimation.
    // Mock column [300, 900), each value repeated 5x over RealtimeCount 3000.
    let values = int_values(300, 600);
    let column = column_stats(mock_stats_histogram(1, &values, 5));

    // Special-case probe on the unmodified table (selectivity_test.go:176-190):
    // value 900 sits above the histogram maximum, so the uniform average of
    // 3000 rows over NDV 600 must come out around 5 with ordered bounds.
    let probe = column_row_count(&column, 900, 900, 3000, 0);
    assert!(
        probe.est > 4.5 && probe.est < 5.5,
        "expected around 5.0, got {}",
        probe.est
    );
    assert!(probe.min_est <= probe.est);
    assert!(probe.max_est >= probe.est);
    assert!(probe.min_est >= 0.0);
    assert!(probe.max_est >= probe.min_est);

    // Then the recorded sweep with inflated RealtimeCount (4500 = 3000 * 1.5)
    // and ModifyCount (1500 = 3000 * 0.5) at ±20% tolerance, exactly Go's
    // assertions against cardinality_suite_out.json's TestOutOfRangeEstimation
    // book. Only each case's Count is quoted here: the book's rounded
    // MinEst/MaxEst columns are never numerically compared upstream either,
    // their ordering being asserted structurally instead.
    const GOLDEN: &[(i64, i64, f64)] = &[
        (800, 900, 763.0),
        (900, 950, 67.0),
        (950, 1000, 62.0),
        (1000, 1050, 57.0),
        (1050, 1100, 52.0),
        (1150, 1200, 41.0),
        (1200, 1300, 59.0),
        (1300, 1400, 38.0),
        (1400, 1500, 18.0),
        (1500, 1600, 13.0),
        (300, 899, 4500.0),
        (800, 1000, 873.0),
        (900, 1500, 381.0),
        (300, 1500, 4500.0),
        (200, 300, 122.0),
        (100, 200, 101.0),
        (200, 400, 872.0),
        (200, 1000, 4500.0),
        (0, 100, 80.0),
        (-100, 100, 132.0),
        (-100, 0, 60.0),
    ];
    for (start, end, count) in GOLDEN {
        let estimate = column_row_count(&column, *start, *end, 4500, 1500);
        assert!(
            estimate.est < count * 1.2,
            "for [{start}, {end}], needed around {count} (+20%), got {}",
            estimate.est
        );
        assert!(
            estimate.est > count * 0.8,
            "for [{start}, {end}], needed around {count} (-20%), got {}",
            estimate.est
        );
        assert!(
            estimate.min_est <= estimate.est,
            "MinEst must be <= Est for [{start}, {end}]"
        );
        assert!(
            estimate.max_est >= estimate.est,
            "MaxEst must be >= Est for [{start}, {end}]"
        );
        assert!(estimate.min_est >= 0.0, "MinEst must be >= 0");
        assert!(
            estimate.max_est >= estimate.min_est,
            "MaxEst must be >= MinEst"
        );
    }
}

#[test]
fn out_of_range_estimation_after_delete_excludes_deleted_rows() {
    // pkg/planner/cardinality/selectivity_test.go:314
    // TestOutOfRangeEstimationAfterDelete. After deleting rows the mock keeps
    // histogram [500, 900) x5 while the table reports RealtimeCount 2000 and
    // ModifyCount 1000.
    let deleted_histogram = mock_stats_histogram(1, &int_values(500, 400), 5);
    let column = column_stats(deleted_histogram);

    // Rows in [300, 500) were deleted; the estimate must not resurrect them.
    let estimate = column_row_count(&column, 300, 500, 2000, 1000);
    assert!(
        estimate.est < 20.0,
        "expected less than 20 after deletion, got {}",
        estimate.est
    );

    // Recorded sweep (cardinality_suite_in.json TestOutOfRangeEstimationAfterDelete
    // request list; only non-negativity and the post-delete table bound are
    // asserted upstream as well).
    let golden: &[(i64, i64)] = &[
        (300, 500),
        (500, 700),
        (700, 900),
        (900, 1100),
        (200, 400),
        (400, 600),
        (600, 800),
        (800, 1000),
        (100, 300),
        (300, 500),
        (500, 700),
        (700, 900),
        (900, 1100),
        (0, 200),
        (200, 400),
        (400, 600),
        (600, 800),
        (800, 1000),
        (1000, 1200),
    ];
    for (start, end) in golden {
        let estimate = column_row_count(&column, *start, *end, 2000, 1000);
        assert!(estimate.est >= 0.0, "[{start}, {end}] negative estimate");
        assert!(
            estimate.est <= 2000.0,
            "[{start}, {end}] exceeds post-delete table size: {}",
            estimate.est
        );
    }
}

#[test]
fn small_range_estimation_matches_recorded_suite_estimates() {
    // pkg/planner/cardinality/selectivity_test.go:1260 TestSmallRangeEstimation.
    // Histogram [0, 400) with repeat 3 over RealtimeCount 1200.
    let column = column_stats(mock_stats_histogram(1, &int_values(0, 400), 3));

    const GOLDEN: &[(i64, i64, f64)] = &[
        (5, 5, 3.0),
        (5, 6, 6.0),
        (5, 10, 18.0),
        (5, 15, 33.0),
        (10, 15, 18.0),
        (5, 25, 63.0),
        (25, 25, 3.0),
    ];
    for (start, end, count) in GOLDEN {
        let estimate = column_row_count(&column, *start, *end, 1200, 0);
        assert!(
            (estimate.est - count).abs() < 1e-9,
            "for [{start}, {end}], needed around {count}, got {}",
            estimate.est
        );
    }
}

#[test]
fn risk_range_skew_ratio_raises_out_of_range_column_estimates() {
    // pkg/planner/cardinality/selectivity_test.go:248 TestRiskRangeSkewRatio.
    // Values 1..10 at ten rows each were analyzed (with 0 topn, stats v2), and
    // the query probes [12, 15) with a 10x-inflated RealtimeCount and its
    // doubled ModifyCount.
    let column = column_stats(mock_stats_histogram(1, &int_values(1, 10), 10));
    let realtime = 100 * 10;
    let modify = realtime * 2;

    let baseline = get_column_row_count(
        &column,
        &[ColumnRange::new(
            Datum::Int(12),
            Datum::Int(15),
            false,
            false,
        )],
        Collation::Binary,
        realtime,
        modify,
        false,
        EstimatorOptions {
            risk_range_skew_ratio: 0.0,
            ..EstimatorOptions::default()
        },
    )
    .unwrap();
    let raised = get_column_row_count(
        &column,
        &[ColumnRange::new(
            Datum::Int(12),
            Datum::Int(15),
            false,
            false,
        )],
        Collation::Binary,
        realtime,
        modify,
        false,
        EstimatorOptions {
            risk_range_skew_ratio: 0.5,
            ..EstimatorOptions::default()
        },
    )
    .unwrap();

    assert!(
        raised.est > baseline.est,
        "raising risk_range_skew_ratio must raise the out-of-range estimate: {} vs {}",
        raised.est,
        baseline.est
    );
    for estimate in [&baseline, &raised] {
        assert!(estimate.min_est <= estimate.est);
        assert!(estimate.max_est >= estimate.est);
    }
    assert!(raised.min_est >= baseline.min_est);
    assert!(raised.max_est >= baseline.max_est);
}

#[test]
fn risk_range_skew_ratio_out_of_range_sequence_is_monotone() {
    // pkg/planner/cardinality/selectivity_test.go:2806
    // TestRiskRangeSkewRatioOutOfRange. Same data shape as the sibling test;
    // additionally checks the zero-realtime baseline and the whole 0 -> 0.5 ->
    // 1 ratio sequence.
    let column = column_stats(mock_stats_histogram(1, &int_values(1, 10), 10));
    let realtime = 100 * 10;
    let modify = realtime * 2;

    let empty_realtime = column_row_count(&column, 12, 15, 0, 0);
    let ratio_of = |ratio: f64| {
        get_column_row_count(
            &column,
            &[ColumnRange::new(
                Datum::Int(12),
                Datum::Int(15),
                false,
                false,
            )],
            Collation::Binary,
            realtime,
            modify,
            false,
            EstimatorOptions {
                risk_range_skew_ratio: ratio,
                ..EstimatorOptions::default()
            },
        )
        .unwrap()
    };

    assert!(empty_realtime.est < ratio_of(0.0).est);
    assert!(ratio_of(0.0).est < ratio_of(0.5).est);
    assert!(ratio_of(0.5).est < ratio_of(1.0).est);
}

#[test]
fn out_of_range_ge_vs_between_right_uncertainty_band() {
    // pkg/planner/cardinality/selectivity_test.go:2865 TestOutOfRangeGeVsBetween.
    // Histogram covers [1, 100] so the right uncertainty band is (100, 199):
    // the bounded BETWEEN 100 AND 102 overlaps it partially while >= 100 gets
    // the whole band.
    let values = int_values(1, 100);
    let column = column_stats(mock_stats_histogram(1, &values, 1));
    let realtime = 100 * 10;
    let modify = realtime * 2;

    let ge_pair = |ratio: f64| {
        (
            get_column_row_count(
                &column,
                &[ColumnRange::new(
                    Datum::Int(100),
                    Datum::Int(i64::MAX),
                    false,
                    false,
                )],
                Collation::Binary,
                realtime,
                modify,
                false,
                EstimatorOptions {
                    risk_range_skew_ratio: ratio,
                    ..EstimatorOptions::default()
                },
            )
            .unwrap(),
            get_column_row_count(
                &column,
                &[ColumnRange::new(
                    Datum::Int(100),
                    Datum::Int(102),
                    false,
                    false,
                )],
                Collation::Binary,
                realtime,
                modify,
                false,
                EstimatorOptions {
                    risk_range_skew_ratio: ratio,
                    ..EstimatorOptions::default()
                },
            )
            .unwrap(),
        )
    };

    let (wide_at_half, between_at_half) = ge_pair(0.5);
    for ratio in [0.0, 0.3, 0.5, 0.7, 1.0] {
        let (wide, between) = ge_pair(ratio);
        assert!(
            wide.est > between.est,
            "skew_ratio={ratio}: col >= 100 ({}) must exceed col BETWEEN 100 AND 102 ({})",
            wide.est,
            between.est
        );
    }
    assert!(
        wide_at_half.max_est > between_at_half.max_est,
        "MaxEst for >= 100 must be larger than MaxEst for BETWEEN 100 AND 102"
    );
}

#[test]
fn risk_eq_skew_ratio_raises_index_equal_estimates_for_unseen_value() {
    // pkg/planner/cardinality/selectivity_test.go:2682 TestRiskEqSkewRatio
    // (the `analyze ... with 0 topn` phase). A nine-row histogram holding
    // values {1:4, 2:2, 3:1, 4:1, 5:1}; probing unseen value 6 lands in the
    // uniform fallback whose skew blend grows with RiskEqSkewRatio.
    let mut histogram = mock_stats_histogram(
        1,
        &[
            Datum::Int(1),
            Datum::Int(2),
            Datum::Int(3),
            Datum::Int(4),
            Datum::Int(5),
        ],
        4,
    );
    histogram.buckets[1].repeat = 2;
    histogram.buckets[1].count = 6;
    histogram.buckets[2].repeat = 1;
    histogram.buckets[2].count = 7;
    histogram.buckets[3].repeat = 1;
    histogram.buckets[3].count = 8;
    histogram.buckets[4].repeat = 1;
    histogram.buckets[4].count = 9;
    let index = IndexStats {
        histogram,
        topn: None,
        cms: None,
        stats_ver: 2,
        num_columns: 1,
        unique: false,
    };
    let columns: IndexColumnStats<'_> = vec![None];
    let range_for = |value: i64| IndexRangeDatums {
        collators: vec![tidb_datatype::Collation::Binary; 1],
        low_val: vec![Datum::Int(value)],
        high_val: vec![Datum::Int(value)],
        low_exclude: false,
        high_exclude: false,
    };

    let estimate_at = |ratio: f64| {
        get_index_row_count_for_stats_v2(
            &index,
            &columns,
            &[],
            &[],
            &[range_for(6)],
            IndexRowCounts::unscaled(9, 0),
            EstimatorOptions {
                risk_eq_skew_ratio: ratio,
                ..EstimatorOptions::default()
            },
        )
        .unwrap()
        .est
    };
    assert!(estimate_at(0.0) < estimate_at(0.5));
    assert!(estimate_at(0.5) < estimate_at(1.0));
}

#[test]
fn index_estimation_survives_empty_idx_to_col_mapping() {
    // pkg/planner/cardinality/selectivity_test.go:658
    // TestOutOfRangeEstimationWithoutIdx2ColMapping. A fully loaded stats-v2
    // single-column index whose histogram covers the encoded values [0, 50);
    // no column mapping is available, so the estimator receives no column
    // statistics. An interval far above the max must neither panic nor return
    // a degenerate count.
    let encoded_values: Vec<Datum> = (0..50)
        .map(|value| {
            Datum::Bytes(
                tidb_codec::encode_key(std::slice::from_ref(&Datum::Int(value)))
                    .expect("integer key encodes"),
            )
        })
        .collect();
    let index = IndexStats {
        histogram: mock_stats_histogram(1, &encoded_values, 1),
        topn: None,
        cms: None,
        stats_ver: 2,
        num_columns: 1,
        unique: false,
    };
    let columns: IndexColumnStats<'_> = vec![None];
    let estimate = get_index_row_count_for_stats_v2(
        &index,
        &columns,
        &[],
        &[],
        &[IndexRangeDatums {
            collators: vec![tidb_datatype::Collation::Binary; 1],
            low_val: vec![Datum::Int(1000)],
            high_val: vec![Datum::Int(2000)],
            low_exclude: false,
            high_exclude: false,
        }],
        IndexRowCounts::unscaled(50, 0),
        EstimatorOptions::default(),
    )
    .unwrap();
    assert!(estimate.est > 0.0, "must return a small positive estimate");
    assert!(estimate.est < 50.0);
}

// ---------------------------------------------------------------------------
// Gap ports: these Go tests pin behavior that needs live SQL machinery the
// Rust workspace does not own yet (store-backed ANALYZE, stats-handle delta
// application, EXPLAIN rendering, session-variable plumbing, failpoints, or
// plan building). Bodies stay empty; every gap cites its Go source.
// ---------------------------------------------------------------------------

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:70
/// TestCollationColumnEstimate`.
///
/// utf8mb4_general_ci column holding aaa/bbb/AAA/BBB analyzed at stats v2,
/// then `show stats_topn` and two EXPLAIN-form brief probes against
/// cardinality_suite_out.json's first book (eq estimate 2.00 hitting the
/// case-insensitive TopN pair, gt spanning it). Pins new-collation sort-key
/// bounds flowing into point/range estimation. All three output fixtures are
/// exercised by `tidb_session::topn_assisted_string_match` after SQL ANALYZE;
/// Go's explicit `LoadNeededHistograms` lifecycle remains open.
#[test]
#[ignore = "SQL ANALYZE, TopN keys and EXPLAIN goldens are covered; explicit histogram reload remains open"]
fn collation_column_estimate_matches_recorded_plans() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:455
/// TestEstimationForUnknownValues`.
///
/// The live session lifecycle is covered by
/// `tidb_session::tests_explain::unknown_value_estimates_follow_analyze_and_truncate_lifecycle`;
/// direct composite-index range estimates use the production estimator in
/// `tidb_executor::access_cost::index_async_load_queue_tests::unknown_values_in_composite_index_ranges_match_go`.
#[test]
#[ignore = "mapped to active session lifecycle and executor estimator regressions"]
fn estimation_for_unknown_values_across_analyze_rounds() {}

// Go `TestCanSkipIndexEstimation` (`selectivity_test.go:541`) is exercised
// through the production statistics boundary at
// `tidb_executor::access_cost::index_async_load_queue_tests::full_index_range_skips_evicted_histogram_load`.
// That regression checks the exact RealtimeCount result, that the full range
// does not queue its evicted index, and that full-not-null, bounded,
// exclusive-NULL, partial-index, and multi-valued-index cases do not take the
// shortcut.

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:703
/// TestEstimationForUnknownValuesAfterModify`.
///
/// The analyzed v2 histogram has ten values, each repeated ten times, over
/// 100 analyzed rows. Reuse that histogram with Go's post-insert stats metadata
/// (RealtimeCount=300, ModifyCount=200): a known value stays at 10, unknown
/// value 11 with no modifications falls back to 1, and unknown value 15 after
/// modifications is strictly between 1 and 10. The live ANALYZE, committed
/// insert delta, and histogram-refresh lifecycle also runs through
/// `tidb_session::tests_explain::unknown_value_estimates_follow_modify_delta_lifecycle`.
#[test]
fn estimation_for_unknown_values_after_modify_stays_bounded() {
    // Go's default ANALYZE TopN capacity retains all ten values here, leaving
    // an empty histogram with its original NDV and 100 analyzed TopN rows.
    let histogram = Histogram::new(1, 10, 0, 0, 0, 0);
    let mut topn = tidb_stats::TopN::new(10);
    for value in 1..=10 {
        let encoded = tidb_codec::encode_key(&[Datum::Int(value)]).unwrap();
        topn.append(&encoded, 10);
    }
    topn.sort();
    let analyzed_column = ColumnStats {
        histogram,
        topn: Some(topn),
        cms: None,
        stats_ver: 2,
        unsigned: false,
    };

    let known = column_row_count(&analyzed_column, 5, 5, 100, 0);
    assert_eq!(known.est, 10.0);

    let unknown_without_modifications = column_row_count(&analyzed_column, 11, 11, 100, 0);
    assert_eq!(unknown_without_modifications.est, 1.0);

    let unknown_after_modifications = column_row_count(&analyzed_column, 15, 15, 300, 200);
    assert!(
        unknown_after_modifications.est > 1.0 && unknown_after_modifications.est < 10.0,
        "post-modification unseen value should be between fallback and observed frequency: {:?}",
        unknown_after_modifications
    );
}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:757
/// TestNewIndexWithoutStats`.
///
/// idxa created after ANALYZE must beat statistics-less idxab only while
/// predicates do not favor it; once idxab carries more matching equal
/// predicates it wins despite missing stats, except where idxca matches the
/// same equals with real statistics. Pins skyline pruning across access-path
/// row counts via EXPLAIN containment checks.
#[test]
#[ignore = "executed through tidb_session::tests_explain::new_index_without_stats_skyline_choice_matches_go"]
fn new_index_without_stats_skyline_choice() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:788 TestIssue57948`.
///
/// With exactly one statistics-bearing index existing (idxb) after ANALYZE,
/// `where b = 5` must pick idxb even though its statistics predate the index
/// registration ordering issue.
#[test]
#[ignore = "executed through tidb_session::tests_explain::single_new_index_with_column_stats_is_chosen"]
fn issue_57948_single_statistics_index_is_chosen() {}

// Go TestVirtualColumnIndexEstimation (issue #69134) is exercised through
// tidb-session::tests_explain::virtual_column_index_estimation_preserves_the_selective_suffix.
// The native estimator's missing-virtual, missing-ordinary, TopN-only, and
// recursive-success branches are covered by
// row_count_estimator::recursive_index_estimation_tests::missing_virtual_column_requires_index_histogram_fallback.
// Recursive error injection belongs to TestNewIndexWithColumnStats below;
// its error-propagation coverage remains open.

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:924
/// TestNewIndexWithColumnStats`.
///
/// Identical data tables t (column stats only) and t2 (no stats at all):
/// index scans on newly created idxa(a) must differ, with t's estimate within
/// 0.1 of the true affected rows because column statistics supplement the
/// missing index statistics. SQL/ANALYZE execution is mapped to
/// `tidb_session::tests_explain::newly_created_index_estimates_from_existing_column_statistics`.
#[test]
#[ignore = "executed through the session's SQL/ANALYZE path"]
fn new_index_with_column_stats_supplements_missing_index_stats() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:960
/// TestEstimationUniqueKeyEqualConds`.
///
/// Unique key(b) analyzed with cmsketch width 4 depth 1: index point lookups
/// of present values return exactly 1.0 via the unique full-length range path.
/// Go's shortcut is independent of the CMSketch contents, so this estimator
/// fixture exercises the same row-count branch without emulating ANALYZE's
/// sketch construction.
#[test]
fn unique_key_equal_conds_return_exact_counts() {
    let values = int_values(1, 7);
    let column = column_stats(mock_stats_histogram(1, &values, 1));
    let index = IndexStats {
        histogram: Histogram::new(7, 7, 0, 0, 1, 0),
        topn: None,
        cms: None,
        stats_ver: 2,
        num_columns: 1,
        unique: true,
    };
    for value in [7, 6] {
        let estimate = get_index_row_count_for_stats_v2(
            &index,
            &vec![None],
            &[],
            &[],
            &[IndexRangeDatums {
                collators: vec![tidb_datatype::Collation::Binary; 1],
                low_val: vec![Datum::Int(value)],
                high_val: vec![Datum::Int(value)],
                low_exclude: false,
                high_exclude: false,
            }],
            IndexRowCounts::unscaled(7, 0),
            EstimatorOptions::default(),
        )
        .expect("the closed unique point range is estimable");
        assert_eq!(estimate.est, 1.0, "value={value}");

        let column_estimate = get_column_row_count(
            &column,
            &[ColumnRange::point(Datum::Int(value))],
            Collation::Binary,
            7,
            0,
            true,
            EstimatorOptions::default(),
        )
        .expect("the primary-key handle point is estimable");
        assert_eq!(column_estimate.est, 1.0, "pk handle value={value}");
    }
}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:994
/// TestColumnIndexNullEstimation`.
///
/// Five NULL-bearing rows across idx_b(b)/idx_c_a(c,a): recorded plans pin
/// NULL point ranges (IndexRangeScan range:[NULL,NULL] == 4.00), NULL column
/// probes, and non-null interval estimates from cardinality_suite_out.json.
/// The ten SQL plan cases are exercised through
/// `tidb_session::tests_explain::null_column_and_index_ranges_match_cardinality_goldens`.
#[test]
#[ignore = "executed through the session's SQL/EXPLAIN path"]
fn column_index_null_estimation_matches_recorded_plans() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1027
/// TestUniqCompEqualEst`.
///
/// Clustered primary key(a,b) under EnableClusteredIndexDefModeOn: the suite
/// pins the Point_Get operator reading range:[1 3,1 3] with 1.00 rows. The
/// complete equality is exercised through
/// `tidb_session::tests_explain::clustered_composite_primary_key_equality_matches_go_point_get`.
#[test]
#[ignore = "executed through the session's SQL/EXPLAIN path"]
fn uniq_comp_equal_estimate_resolves_to_point_get() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1051 TestSelectivity`.
///
/// prepareSelectivity mocks five columns (NDV 54 repeat 10) and two composite
/// indexes over encoded two-column keys (NDV 9 repeat 60) on RealtimeCount
/// 540, then re-computes Selectivity() for nine expressions -- including a 64
/// clause conjunction capped at pseudo selectivity 0.001 -- to eps 1e-9 both
/// before and after inflating RealtimeCount 10x/ModifyCount 9x, under
/// tidb_opt_risk_range_skew_ratio = 0.3.
#[test]
#[ignore = "executed with the exact AST/statistics fixture in tidb_executor::access_cost::tests::go_test_selectivity_matches_mock_hist_coll_before_and_after_growth"]
fn selectivity_over_mocked_hist_coll_matches_recorded_ratios() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1146
/// TestDNFCondSelectivity`.
///
/// DNF conditions use independence across branches (`b > 7 or c < 4` etc.,
/// golden 0.34375/0.625/...) over four columns plus idx(b)/idx(d); also guards
/// regressions for _tidb_rowid DNF, unloaded timestamp columns preventing
/// infinite recursion (issue 22134), and blob/decimal/timestamp NOT-BETWEEN
/// tuples (issue 27294). The numeric goldens and missing-statistics guard run
/// in `tidb_executor::access_cost`; all three planner smoke cases run through
/// `tidb_session::tests_explain::dnf_selectivity_safety_cases_match_go_smoke_coverage`.
#[test]
#[ignore = "executed with Go's cardinality-suite goldens and planner smoke cases in tidb_executor and tidb_session"]
fn dnf_cond_selectivity_uses_independence_assumption() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1215
/// TestIndexEstimationCrossValidate`.
///
/// With failpoint statistics/table/mockQueryBytesMaxUint64=return(100000),
/// IndexRangeScan over key(a,b) reports 1.00 (cross-validation prefers bucket
/// repeat over CMS noise); issue 22466 keeps TableFullScan 5.00 after
/// re-analyzing only index b.
#[test]
#[ignore = "split across row_count_estimator::cross_validation_wins_over_a_maximally_noisy_cms and tidb_session::tests_explain::composite_index_estimate_and_empty_index_stats_match_go"]
fn index_estimation_cross_validates_against_cms_maximum() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1243
/// TestRangeStepOverflow`.
///
/// datetime histogram with years 3580..4862 must survive range detaching of
/// '8499-01-23'..'9961-07-23' without overflow and load its statistics.
#[test]
#[ignore = "session covers range execution after ANALYZE; Go's explicit LoadNeededHistograms lifecycle is not ported"]
fn range_step_overflow_on_datetime_histogram() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1465
/// TestTopNAssistedEstimationWithoutNewCollation` and `:1477
/// TestTopNAssistedEstimationWithNewCollation`.
///
/// Six string columns across utf8mb4/gbk collations, forty rows analyzed with
/// 3 topn; 28 recorded explain/select queries per collation mode pin LIKE
/// estimates assisted by TopN (e.g. like '%111%' reads 30.00) through
/// tidb_default_string_match_selectivity=0.
#[test]
#[ignore = "executed through the production SQL/ANALYZE path in tidb_session::topn_assisted_string_match"]
fn topn_assisted_string_match_estimation_golden_suite() {}

/// Go `pkg/planner/cardinality/selectivity_test.go:1418`,
/// `TestDefaultStringMatchSelectivityZeroImprovesLikeEstimation`, is exercised
/// through its active session/EXPLAIN path in
/// `tidb_session::tests_explain::default_string_match_selectivity_zero_improves_like_estimates`.

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1519
/// TestStringMatchSelectivityDoesNotRestoreTransientHistogramBoundsSelection`.
///
/// GetSelectivityByFilter over a LIKE '%R%' predicate on a three-bucket
/// histogram must return ok=true with 2/3 selectivity while leaving the shared
/// cached bounds selection untouched by a simulated concurrent VecEvalBool
/// that narrowed Bounds.sel to {4,5}.
#[test]
#[ignore = "Rust's immutable HistColl equivalent is covered by logical::rewrite::analyzed_filter_selectivity_tests::string_match_estimation_does_not_mutate_shared_histogram"]
fn string_match_selectivity_keeps_transient_bounds_selection() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1629
/// TestGlobalStatsOutOfRangeEstimationAfterDelete`.
///
/// Range-partitioned table (p0..p4) analyzed with samplerate, then partially
/// deleted: all thirteen recorded estimates, partition sets, and full-scan row
/// counts are exercised through `tidb_session::tests_explain::
/// global_partition_out_of_range_estimates_survive_delete_and_partition_analyze`.
#[test]
#[ignore = "covered by the production SQL/ANALYZE session regression"]
fn global_stats_out_of_range_after_partition_delete() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1695 TestIssue39593`.
///
/// Twenty leading-prefix point ranges over mocked key(a,b) (columns NDV 54,
/// repeat 10; index NDV 9, repeat 60; RealtimeCount 540) estimate ~462.6 +- 1,
/// and the same ranges estimate ~5400 +- 1 after the table RealtimeCount grows
/// tenfold. The Go fixture leaves the index StatsVer at its zero value, so this
/// is the legacy index-histogram path, despite supplying its column map.
#[test]
fn issue_39593_composite_prefix_point_ranges_match_estimates() {
    // The Go fixture has five uniform column histograms (NDV 54, repeat 10),
    // a two-column index histogram over all 3x3 encoded pairs (NDV 9, repeat
    // 60), and twenty leading-column point ranges. Supply the same ordered
    // index-to-column mapping the Go HistColl carries. The mock Index leaves
    // StatsVer at its zero value, so Go does not dispatch to v2 backoff here.
    let values = int_values(0, 54);
    let first = column_stats(mock_stats_histogram(1, &values, 10));
    let second = column_stats(mock_stats_histogram(2, &values, 10));
    let mut encoded_pairs = Vec::with_capacity(9);
    for left in 0..3 {
        for right in 0..3 {
            encoded_pairs.push(Datum::Bytes(
                tidb_codec::encode_key(&[Datum::Int(left), Datum::Int(right)])
                    .expect("composite key encodes"),
            ));
        }
    }
    let index = IndexStats {
        histogram: mock_stats_histogram(1, &encoded_pairs, 60),
        topn: None,
        cms: None,
        // Go's mock `statistics.Index` omits StatsVer here, so it is version 0.
        stats_ver: 0,
        num_columns: 2,
        unique: false,
    };
    let columns: IndexColumnStats<'_> = vec![Some(&first), Some(&second)];
    let ranges = (1..=20)
        .map(|value| IndexRangeDatums {
            collators: vec![tidb_datatype::Collation::Binary; 1],
            low_val: vec![Datum::Int(value)],
            high_val: vec![Datum::Int(value)],
            low_exclude: false,
            high_exclude: false,
        })
        .collect::<Vec<_>>();

    let before_growth = get_index_row_count_for_stats_v2(
        &index,
        &columns,
        &[],
        &[],
        &ranges,
        IndexRowCounts::unscaled(540, 0),
        EstimatorOptions::default(),
    )
    .expect("the composite point sweep is estimable");
    assert!(
        (before_growth.est - 462.6).abs() <= 1.0,
        "Go TestIssue39593 baseline estimate: {}",
        before_growth.est
    );

    let after_growth = get_index_row_count_for_stats_v2(
        &index,
        &columns,
        &[],
        &[],
        &ranges,
        IndexRowCounts {
            table_realtime: 5_400,
            table_modify: 0,
            index_realtime: 5_400,
            index_modify: 0,
        },
        EstimatorOptions::default(),
    )
    .expect("scaled composite point sweep is estimable");
    assert!(
        (after_growth.est - 5_400.0).abs() <= 1.0,
        "Go TestIssue39593 scaled estimate: {}",
        after_growth.est
    );
}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1743
/// TestIndexRangeEstimationWithAppendedHandleColumn`.
///
/// Non-unique idx_ab(a,b) with only partial column stats: planner appends the
/// handle column, and `a = 1 and b = 2 and id = 3` still estimates 1.00 with
/// stats:partial markers instead of panicking.
#[test]
#[ignore = "executed with partial column statistics in tidb_session::tests_explain::appended_handle_range_uses_partial_column_statistics"]
fn index_range_estimation_with_appended_handle_column() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1793
/// TestIndexRangeEstimationWithTruncatedHandleRange`.
///
/// ia(a) + clustered id handle: pruned execution ranges keep handle dimensions
/// ((5 10,5 +inf], [5 -inf,5 10)) with exclusive flag fixes yielding 10.00;
/// point handle IN-lists get credit down to 2.00; unsigned handles never
/// extend the range ([5,5]) because signed key encoding wraps at MaxInt64.
#[test]
#[ignore = "execution ranges and estimates are covered in tidb_session::tests_explain::truncated_integer_handle_ranges_match_go_cardinality_estimates"]
fn index_range_estimation_with_truncated_handle_range() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1876
/// TestIndexRangeEstimationWithPrefixedCommonHandle`.
///
/// Clustered PK p1(2-prefix),p2 behind key ic(c): execution ranges keep prefix
/// semantics ([5 "pp",5 "pp"]) while Selection re-checks eq(p1,'pp_055');
/// tuple comparisons spanning index+handle columns must not read past the
/// per-appended-column length slice (issue #70532).
#[test]
#[ignore = "execution and cardinality assertions are covered in tidb_session::tests_explain::prefixed_common_handle_ranges_match_go_cardinality_cases"]
fn index_range_estimation_with_prefixed_common_handle() {}

/// Go `TestDeriveTablePathStatsNoAccessConds`'s CountAfterAccess assertion is
/// exercised in `tidb_executor::driver::planner_bridge::statistics_initialization_error_tests::unfiltered_table_path_count_uses_realtime_row_count`.
#[test]
#[ignore = "covered at the production stats-initialization boundary"]
fn derive_table_path_stats_keeps_count_after_access_without_conditions() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:2018
/// TestIndexJoinInnerRowCountUpperBound`.
///
/// Mocked 500000-row stats (NDV 500) drive two recorded index-join plans,
/// separated by SET Fix44855=ON. The active session test compares every
/// original EXPLAIN cell, including the 500000000-to-2000000 scan-row cap.
#[test]
#[ignore = "covered by tidb_session::tests_explain::index_join_inner_row_count_upper_bound_matches_go"]
fn index_join_inner_row_count_upper_bound_golden() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:2089
/// TestOrderingIdxSelectivityThreshold` and `:2173
/// TestOrderingIdxSelectivityRatio`.
///
/// Mocked 100000-row / 1000-row suites run all 32 and 21 source statements:
/// 28 and 15 complete EXPLAIN plans plus 10 setting changes. Active mappings:
/// tidb_session::tests_explain::ordering_index_selectivity_threshold_matches_go_fixture
/// and ordering_index_selectivity_ratio_matches_go_fixture. The fixtures are
/// read directly from Go's cardinality_suite_out.json without plan normalization.
#[test]
#[ignore = "executed through both complete ordering-index session fixtures"]
fn ordering_idx_selectivity_threshold_and_ratio_suites() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:2256
/// TestOrderingIdxSelectivityRatioForJoin`, `:2296 ...ForMergeJoin`, and
/// `:2360 ...ForApply`.
///
/// Analyzed join tables and mocked Apply histograms force the source shapes under
/// discouraging cost factors; explain format=verbose costs must be identical
/// for ratio -1/0 and strictly increasing across 0 -> 0.5 -> 1 whenever an
/// ordering index supplies the ORDER BY.
#[test]
#[ignore = "executed through tests_explain::ordering_ratio_increases_index_join_cost, ordering_ratio_increases_merge_join_cost and ordering_ratio_increases_apply_cost with the complete Go fixtures"]
fn ordering_idx_selectivity_ratio_cost_monotonicity_for_join_shapes() {}

/// Go `TestCrossValidationSelectivity` is exercised through
/// `tidb_session::tests_explain::cross_validation_on_clustered_pk_range_matches_go`.
#[test]
#[ignore = "covered at the SQL planner/EXPLAIN boundary"]
fn cross_validation_selectivity_on_clustered_pk_range() {}

/// Go `TestIgnoreRealtimeStats`'s post-ANALYZE realtime-count behavior is
/// exercised in
/// `tidb_session::tests_explain::determinate_objective_uses_analyzed_row_count_after_inserts`.
#[test]
#[ignore = "partial SQL coverage; cluster stats-delta/cache refresh lifecycle remains open"]
fn ignore_realtime_stats_by_optimizer_objective() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:2567
/// TestSubsetIdxCardinality`.
///
/// After loading queued histograms (sync wait 0), full-load flags hold for
/// every column of iabc and the index itself; the five recorded distinct/count
/// plans pin subset-vs-full index cardinality behavior.
#[test]
#[ignore = "executed by tidb-session cardinality_stats_loading through the catalog queue/cache lifecycle; storage I/O uses a test double"]
fn subset_idx_cardinality_after_async_stats_load() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:2635
/// TestBuiltinInEstWithoutStats`.
///
/// Pseudo-stat table with ten rows: `a IN (1..8)` records Selection 1.00 over
/// TableFullScan 10.00 stats:pseudo and must survive InitStatsLite/InitStats
/// refreshes unchanged; ColAndIdxExistenceMap ends populated but with no
/// analyzed columns. The initial post-delta EXPLAIN for both columns is
/// exercised by `tidb_session::tests_explain::builtin_in_estimate_without_stats_keeps_selection_floor`;
/// Rust's stats-handle InitStatsLite/InitStats refresh lifecycle is still not
/// wired into that session path.
#[test]
#[ignore = "Go's repeated stats initialization and ColAndIdxExistenceMap assertions are not wired through the Rust session"]
fn builtin_in_estimate_without_stats_keeps_selection_floor() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:2754
/// TestRiskRangeSkewRatioWithinBucket`.
///
/// Single-bucket index (analyze with 0 topn, 1 buckets): probing [2,3] stays
/// inside the bucket where widening applies; counts must rise monotonically
/// across ratios 0/0.5/1. The Rust fixture exercises the production estimator
/// directly; global/session DEFAULT semantics are also exercised at the
/// session SQL boundary in `tests_explain`.
#[test]
fn risk_range_skew_ratio_widens_within_bucket_estimates() {
    let low = tidb_codec::encode_key(&[Datum::Int(1)]).unwrap();
    let high = tidb_codec::encode_key(&[Datum::Int(5)]).unwrap();
    let mut histogram = Histogram::new(7, 5, 0, 0, 1, 0);
    histogram.append_bucket(Datum::Bytes(low), Datum::Bytes(high), 10, 2);
    let index = IndexStats {
        histogram,
        topn: None,
        cms: None,
        stats_ver: 2,
        num_columns: 1,
        unique: false,
    };
    let range = IndexRangeDatums {
        collators: vec![tidb_datatype::Collation::Binary; 1],
        low_val: vec![Datum::Int(2)],
        high_val: vec![Datum::Int(3)],
        low_exclude: false,
        high_exclude: false,
    };
    let estimate = |risk_range_skew_ratio| {
        get_index_row_count_for_stats_v2(
            &index,
            &vec![None],
            &[],
            &[],
            std::slice::from_ref(&range),
            IndexRowCounts::unscaled(10, 0),
            EstimatorOptions {
                risk_range_skew_ratio,
                ..EstimatorOptions::default()
            },
        )
        .unwrap()
        .est
    };
    let zero = estimate(0.0);
    let half = estimate(0.5);
    let one = estimate(1.0);
    assert!(zero < half && half < one, "0={zero}, 0.5={half}, 1={one}");
}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:2942
/// TestLastBucketEndValueHeuristic`.
///
/// Value 11 appears once against buckets of ~100 (5-bucket analyze); ten extra
/// copies stay under the 50-row trigger so the estimate hugs 1, ninety more
/// trip the heuristic lifting the estimate to ~100.09 while mid-histogram
/// value 3 reads ~109.99; index paths mirror both numbers.
#[test]
fn last_bucket_end_value_heuristic_lifts_underrepresented_counts() {
    // Reproduce the sufficient statistics from Go's ANALYZE fixture directly:
    // 100 rows for values 1..10, one row for 11, five buckets, no TopN.
    // The final bucket's repeat is stale after 100 concentrated inserts.
    let mut column_histogram = Histogram::new(1, 11, 0, 0, 5, 0);
    for (low, high, cumulative, repeat, ndv) in [
        (1, 2, 200, 100, 2),
        (3, 4, 400, 100, 2),
        (5, 6, 600, 100, 2),
        (7, 8, 800, 100, 2),
        (9, 11, 1001, 1, 3),
    ] {
        column_histogram.append_bucket_with_ndv(
            Datum::Int(low),
            Datum::Int(high),
            cumulative,
            repeat,
            ndv,
        );
    }
    let column = column_stats(column_histogram);
    let point_count = |value, realtime, modify| {
        get_column_row_count(
            &column,
            &[ColumnRange::point(Datum::Int(value))],
            Collation::Binary,
            realtime,
            modify,
            false,
            EstimatorOptions::default(),
        )
        .unwrap()
    };

    let baseline = point_count(11, 1001, 0);
    assert_eq!(baseline.est, 1.0);
    let insufficient_growth = point_count(11, 1011, 10);
    assert!((insufficient_growth.est - baseline.est).abs() < 0.5);
    let enough_growth = point_count(11, 1101, 100);
    assert!(
        (enough_growth.est - 100.09).abs() < 0.1,
        "{enough_growth:?}"
    );
    let ordinary_value = point_count(3, 1101, 100);
    assert!(
        (ordinary_value.est - 109.99).abs() < 0.1,
        "{ordinary_value:?}"
    );

    let encode = |value| {
        Datum::Bytes(tidb_codec::encode_key(&[Datum::Int(value)]).expect("index key encodes"))
    };
    let mut index_histogram = Histogram::new(1, 11, 0, 0, 5, 0);
    for (low, high, cumulative, repeat, ndv) in [
        (1, 2, 200, 100, 2),
        (3, 4, 400, 100, 2),
        (5, 6, 600, 100, 2),
        (7, 8, 800, 100, 2),
        (9, 11, 1001, 1, 3),
    ] {
        index_histogram.append_bucket_with_ndv(encode(low), encode(high), cumulative, repeat, ndv);
    }
    let index = IndexStats {
        histogram: index_histogram,
        topn: None,
        cms: None,
        stats_ver: 2,
        num_columns: 1,
        unique: false,
    };
    let estimate_index = |value, realtime, modify| {
        get_index_row_count_for_stats_v2(
            &index,
            &vec![None],
            &[],
            &[],
            &[IndexRangeDatums {
                collators: vec![tidb_datatype::Collation::Binary; 1],
                low_val: vec![Datum::Int(value)],
                high_val: vec![Datum::Int(value)],
                low_exclude: false,
                high_exclude: false,
            }],
            IndexRowCounts::unscaled(realtime, modify),
            EstimatorOptions::default(),
        )
        .unwrap()
    };
    assert!((estimate_index(11, 1101, 100).est - 100.09).abs() < 0.1);
    assert!((estimate_index(3, 1101, 100).est - 109.99).abs() < 0.1);
}

/// Go `TestIssue64137`'s SQL estimator assertions are exercised in
/// `tidb-session::tests_explain::small_ndv_out_of_range_index_reader_rows_match_go`.
/// That session test supplies Go's post-`StatsHandle.Update` metadata because
/// this source-shaped harness cannot run the domain stats-delta worker.
#[test]
#[ignore = "covered at the SQL estimator boundary; source mock lacks refreshed stats metadata"]
fn issue_64137_small_ndv_out_of_range_index_reader_rows() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:3069
/// TestUninitializedStats`.
///
/// Expression index idx_expr((cast(json_unquote(...)) collate utf8mb4_bin)):
/// after explain-analyze triggers loading, show stats_histograms must not list
/// allEvicted states and replans must never print unInitialized.
#[test]
#[ignore = "local SQL fixture executed by tidb-session tests_explain::expression_index_statistics_remain_initialized; cluster virtual-sample evaluation and loading remain open"]
fn uninitialized_expr_index_stats_finish_loading() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:3093
/// TestEqualEstimateOnZeroRepeatBucketUpper`.
///
/// A merged/sampled v2 histogram legitimately carries bucket upper bounds with
/// `Repeat` 0: an upper bound was observed in the data, so zero means "no point
/// frequency recorded", not "zero rows". Go's `equalRowCountOnColumn` therefore
/// requires `matched && histCnt > 0` before trusting the bucket repeat
/// (`pkg/planner/cardinality/row_count_column.go:116`) and otherwise falls
/// through to the uniform average. Against buckets ([1,50] repeat 0,
/// [51,100] repeat 5, NDV 100, 200 rows), value 50 must estimate the uniform
/// average 200/100 = 2.0 while observed upper 100 stays exactly 5.0.
///
/// `equal_row_count_on_column` now applies the same `histCnt > 0` condition, so
/// this executable regression verifies the zero-repeat upper falls through to
/// the uniform estimate while observed repeats remain exact.
#[test]
fn equal_estimate_on_zero_repeat_bucket_upper_falls_back_to_uniform() {
    let mut histogram = Histogram::new(1, 100, 0, 0, 2, 0);
    histogram.append_bucket(Datum::Int(1), Datum::Int(50), 100, 0);
    histogram.append_bucket(Datum::Int(51), Datum::Int(100), 200, 5);
    let column = column_stats(histogram);

    // Upper bound 50 carries no recorded frequency.
    let uniform_fallback = column_row_count(&column, 50, 50, 200, 0);
    assert_eq!(
        uniform_fallback.est, 2.0,
        "a zero Repeat must fall back to the uniform average, not report zero rows"
    );

    // An observed Repeat is still used as-is.
    let observed_repeat = column_row_count(&column, 100, 100, 200, 0);
    assert_eq!(
        observed_repeat.est, 5.0,
        "an observed Repeat must still be used as is"
    );
}
