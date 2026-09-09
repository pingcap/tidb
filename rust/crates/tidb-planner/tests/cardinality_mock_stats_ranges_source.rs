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
    get_column_row_count, get_index_row_count_for_stats_v2, ColumnRange, ColumnStats, EstimatorOptions,
    IndexColumnStats, IndexRangeDatums, IndexStats,
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
        assert!(estimate.max_est >= estimate.min_est, "MaxEst must be >= MinEst");
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
        &[ColumnRange::new(Datum::Int(12), Datum::Int(15), false, false)],
        Collation::Binary,
        realtime,
        modify,
        false,
        EstimatorOptions {
            risk_range_skew_ratio: 0.0,
            ..EstimatorOptions::default()
        },
    );
    let raised = get_column_row_count(
        &column,
        &[ColumnRange::new(Datum::Int(12), Datum::Int(15), false, false)],
        Collation::Binary,
        realtime,
        modify,
        false,
        EstimatorOptions {
            risk_range_skew_ratio: 0.5,
            ..EstimatorOptions::default()
        },
    );

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

    let empty_realtime =
        column_row_count(&column, 12, 15, 0, 0);
    let ratio_of = |ratio: f64| {
        get_column_row_count(
            &column,
            &[ColumnRange::new(Datum::Int(12), Datum::Int(15), false, false)],
            Collation::Binary,
            realtime,
            modify,
            false,
            EstimatorOptions {
                risk_range_skew_ratio: ratio,
                ..EstimatorOptions::default()
            },
        )
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
                &[ColumnRange::new(Datum::Int(100), Datum::Int(i64::MAX), false, false)],
                Collation::Binary,
                realtime,
                modify,
                false,
                EstimatorOptions {
                    risk_range_skew_ratio: ratio,
                    ..EstimatorOptions::default()
                },
            ),
            get_column_row_count(
                &column,
                &[ColumnRange::new(Datum::Int(100), Datum::Int(102), false, false)],
                Collation::Binary,
                realtime,
                modify,
                false,
                EstimatorOptions {
                    risk_range_skew_ratio: ratio,
                    ..EstimatorOptions::default()
                },
            ),
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
    let mut histogram = mock_stats_histogram(1, &[Datum::Int(1), Datum::Int(2), Datum::Int(3), Datum::Int(4), Datum::Int(5)], 4);
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
        low: vec![Datum::Int(value)],
        high: vec![Datum::Int(value)],
        low_exclude: false,
        high_exclude: false,
    };

    let estimate_at = |ratio: f64| {
        get_index_row_count_for_stats_v2(
            &index,
            &columns,
            &[range_for(6)],
            9,
            0,
            EstimatorOptions {
                risk_eq_skew_ratio: ratio,
                ..EstimatorOptions::default()
            },
        )
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
        &[IndexRangeDatums {
            low: vec![Datum::Int(1000)],
            high: vec![Datum::Int(2000)],
            low_exclude: false,
            high_exclude: false,
        }],
        50,
        0,
        EstimatorOptions::default(),
    );
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
/// bounds flowing into point/range estimation.
#[test]
#[ignore = "go-parity-gap: needs store-backed ANALYZE with new-collation collators plus EXPLAIN goldens"]
fn collation_column_estimate_matches_recorded_plans() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:455
/// TestEstimationForUnknownValues`.
///
/// Two analyze rounds around inserted rows 10..19, then pins GetRowCountByColumnRanges
/// on points/ranges (30==2.0 uniform slice of 20 rows; [9,30]==4.0 spanning the
/// post-analyze ten rows), GetRowCountByIndexRanges on the composite (1.0/2.0),
/// a single-NULL analyze giving 1.0 over [1,30], and an unanalyzed int column
/// whose out-of-range equal estimate is 0.001 while its index returns 1.0.
#[test]
#[ignore = "go-parity-gap: interleaves live INSERT/TRUNCATE/ANALYZE deltas through the stats handle"]
fn estimation_for_unknown_values_across_analyze_rounds() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:541
/// TestCanSkipIndexEstimation`.
///
/// Mocks 50 loaded column histograms + one all-evicted index histogram so a
/// full `[NULL,+inf)` range must return RealtimeCount via canSkipIndexEstimation
/// BEFORE IndexStatsIsInvalid queues the evicted item into
/// asyncload.AsyncLoadHistogramNeededItems (`:604` asserts absence); sibling
/// ranges ([MinNotNull,+inf) with 10 NULLs, bounded [1,10], `(NULL,+inf]`)
/// must bypass the fast path.
#[test]
#[ignore = "go-parity-gap: fast-path short-circuit lives above GetRowCountByIndexRanges next to the async-load registry"]
fn can_skip_index_estimation_short_circuits_before_async_load() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:703
/// TestEstimationForUnknownValuesAfterModify`.
///
/// Post-analyze equality estimates for found value 5 (exactly 10.0), unseen
/// value 11 with zero modify count (fallback 1.0), and unseen value 15 after
/// +200 modified rows (strictly between 1.0 and 10.0).
#[test]
#[ignore = "go-parity-gap: needs live modify-count accounting between ANALYZE rounds"]
fn estimation_for_unknown_values_after_modify_stays_bounded() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:757
/// TestNewIndexWithoutStats`.
///
/// idxa created after ANALYZE must beat statistics-less idxab only while
/// predicates do not favor it; once idxab carries more matching equal
/// predicates it wins despite missing stats, except where idxca matches the
/// same equals with real statistics. Pins skyline pruning across access-path
/// row counts via EXPLAIN containment checks.
#[test]
#[ignore = "go-parity-gap: skyline access-path pruning is decided in plan building, not range estimation"]
fn new_index_without_stats_skyline_choice() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:788 TestIssue57948`.
///
/// With exactly one statistics-bearing index existing (idxb) after ANALYZE,
/// `where b = 5` must pick idxb even though its statistics predate the index
/// registration ordering issue.
#[test]
#[ignore = "go-parity-gap: skyline choice needs plan-level access-path candidates"]
fn issue_57948_single_statistics_index_is_chosen() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:806
/// TestVirtualColumnIndexEstimation` (issue #69134).
///
/// Composite index iabd(a,b,virtual d): exponential backoff over a/b alone
/// would over-estimate (~45) vs actual 10, so estimation falls back to the
/// index histogram (<25); the real-column control keeps the clamped backoff
/// (>10); leading virtual-column indexes propagate recursive estimates and
/// need failpoint `...cardinality/afterRecursiveIndexEstimation` to prove the
/// fallback chain (:942-963).
#[test]
#[ignore = "go-parity-gap: needs EXPLAIN ANALYZE plus the afterRecursiveIndexEstimation failpoint"]
fn virtual_column_index_estimation_falls_back_to_index_stats() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:924
/// TestNewIndexWithColumnStats`.
///
/// Identical data tables t (column stats only) and t2 (no stats at all):
/// index scans on newly created idxa(a) must differ, with t's estimate within
/// 0.1 of the true affected rows because column statistics supplement the
/// missing index statistics.
#[test]
#[ignore = "go-parity-gap: needs cross-table EXPLAIN ANALYZE comparisons"]
fn new_index_with_column_stats_supplements_missing_index_stats() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:960
/// TestEstimationUniqueKeyEqualConds`.
///
/// Unique key(b) analyzed with cmsketch width 4 depth 1: index point lookups
/// of present values return exactly 1.0 via the unique full-length range path,
/// and pk-is-handle column probes match them at 1.0.
#[test]
#[ignore = "go-parity-gap: needs cmsketch-shaped ANALYZE output wired through GetRowCountByIndexRanges"]
fn unique_key_equal_conds_return_exact_counts() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:994
/// TestColumnIndexNullEstimation`.
///
/// Five NULL-bearing rows across idx_b(b)/idx_c_a(c,a): recorded plans pin
/// NULL point ranges (IndexRangeScan range:[NULL,NULL] == 4.00), NULL column
/// probes, and non-null interval estimates from cardinality_suite_out.json.
#[test]
#[ignore = "go-parity-gap: NULL-range plans require the executor-side index reader stack"]
fn column_index_null_estimation_matches_recorded_plans() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1027
/// TestUniqCompEqualEst`.
///
/// Clustered primary key(a,b) under EnableClusteredIndexDefModeOn: the suite
/// pins the Point_Get operator reading range:[1 3,1 3] with 1.00 rows.
#[test]
#[ignore = "go-parity-gap: clustered point-get planning is outside this crate"]
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
#[ignore = "go-parity-gap: Selectivity() needs expression parsing -> mask/range extraction"]
fn selectivity_over_mocked_hist_coll_matches_recorded_ratios() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1146
/// TestDNFCondSelectivity`.
///
/// DNF conditions use independence across branches (`b > 7 or c < 4` etc.,
/// golden 0.34375/0.625/...) over four columns plus idx(b)/idx(d); also guards
/// regressions for _tidb_rowid DNF, unloaded timestamp columns preventing
/// infinite recursion (issue 22134), and blob/decimal/timestamp NOT-BETWEEN
/// tuples (issue 27294).
#[test]
#[ignore = "go-parity-gap: DNF independence needs expression extraction over parsed conditions"]
fn dnf_cond_selectivity_uses_independence_assumption() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1215
/// TestIndexEstimationCrossValidate`.
///
/// With failpoint statistics/table/mockQueryBytesMaxUint64=return(100000),
/// IndexRangeScan over key(a,b) reports 1.00 (cross-validation prefers bucket
/// repeat over CMS noise); issue 22466 keeps TableFullScan 5.00 after
/// re-analyzing only index b.
#[test]
#[ignore = "go-parity-gap: CMS query max failpoint hook not ported"]
fn index_estimation_cross_validates_against_cms_maximum() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1243
/// TestRangeStepOverflow`.
///
/// datetime histogram with years 3580..4862 must survive range detaching of
/// '8499-01-23'..'9961-07-23' without overflow and load its statistics.
#[test]
#[ignore = "go-parity-gap: datetime range detaching needs the time-type ranger"]
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
#[ignore = "go-parity-gap: LIKE-pattern selectivity rides on evaluated constant patterns"]
fn topn_assisted_string_match_estimation_golden_suite() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1489
/// TestDefaultStringMatchSelectivityZeroImprovesLikeEstimation`.
///
/// 95/100 'other value' rows: with tidb_default_string_match_selectivity=0.8
/// the TableReader estimate is 80 while the TopN-assisted mode lands near the
/// true 5; the smaller absolute error decides.
#[test]
#[ignore = "go-parity-gap: session variable routing into string-match selectivity"]
fn default_string_match_selectivity_zero_improves_like_estimates() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1519
/// TestStringMatchSelectivityDoesNotRestoreTransientHistogramBoundsSelection`.
///
/// GetSelectivityByFilter over a LIKE '%R%' predicate on a three-bucket
/// histogram must return ok=true with 2/3 selectivity while leaving the shared
/// cached bounds selection untouched by a simulated concurrent VecEvalBool
/// that narrowed Bounds.sel to {4,5}.
#[test]
#[ignore = "go-parity-gap: filter-driven selectivity needs vectorized evaluation"]
fn string_match_selectivity_keeps_transient_bounds_selection() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1629
/// TestGlobalStatsOutOfRangeEstimationAfterDelete`.
///
/// Range-partitioned table (p0..p4) analyzed with samplerate, half deleted:
/// thirteen recorded plans verify global-stats out-of-range handling in
/// dynamic prune mode before AND after re-analyzing partition p4.
#[test]
#[ignore = "go-parity-gap: global (merged) stats and dynamic partition pruning are unported"]
fn global_stats_out_of_range_after_partition_delete() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1695 TestIssue39593`.
///
/// Twenty composite point ranges over mocked key(a,b) (columns NDV 54 repeat
/// 10, index NDV 9 repeat 60, RealtimeCount 540, maps generated from offsets)
/// damp to ~462.6 +- 1 through exponential backoff; multiplying RealtimeCount
/// by ten raises the same sweep to ~5400 +- 1.
#[test]
#[ignore = "go-parity-gap: v2 index dispatch needs Idx2ColUniqueIDs-leading-column plumbing"]
fn issue_39593_composite_point_ranges_damped_by_backoff() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1743
/// TestIndexRangeEstimationWithAppendedHandleColumn`.
///
/// Non-unique idx_ab(a,b) with only partial column stats: planner appends the
/// handle column, and `a = 1 and b = 2 and id = 3` still estimates 1.00 with
/// stats:partial markers instead of panicking.
#[test]
#[ignore = "go-parity-gap: fillIndexPath handle appending lives in core access-path construction"]
fn index_range_estimation_with_appended_handle_column() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1793
/// TestIndexRangeEstimationWithTruncatedHandleRange`.
///
/// ia(a) + clustered id handle: pruned execution ranges keep handle dimensions
/// ((5 10,5 +inf], [5 -inf,5 10)) with exclusive flag fixes yielding 10.00;
/// point handle IN-lists get credit down to 2.00; unsigned handles never
/// extend the range ([5,5]) because signed key encoding wraps at MaxInt64.
#[test]
#[ignore = "go-parity-gap: truncated/pruned index+handle range merge logic is planner-owned"]
fn index_range_estimation_with_truncated_handle_range() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1876
/// TestIndexRangeEstimationWithPrefixedCommonHandle`.
///
/// Clustered PK p1(2-prefix),p2 behind key ic(c): execution ranges keep prefix
/// semantics ([5 "pp",5 "pp"]) while Selection re-checks eq(p1,'pp_055');
/// tuple comparisons spanning index+handle columns must not read past the
/// per-appended-column length slice (issue #70532).
#[test]
#[ignore = "go-parity-gap: prefixed common-handle range building is planner-owned"]
fn index_range_estimation_with_prefixed_common_handle() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:1936
/// TestDeriveTablePathStatsNoAccessConds`.
///
/// A DataSource built from `select * from t` and optimized collects no access
/// conditions, so deriveTablePathStats leaves CountAfterAccess at the mocked
/// RealtimeCount 1000.
#[test]
#[ignore = "go-parity-gap: recursive stats derivation over logical plans is not implemented"]
fn derive_table_path_stats_keeps_count_after_access_without_conditions() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:2018
/// TestIndexJoinInnerRowCountUpperBound`.
///
/// Mocked 500000-row stats (NDV 500) drive three recorded index-join plans
/// whose inner side row counts cap at 4000/2000 via the upper-bound formula.
#[test]
#[ignore = "go-parity-gap: index-join inner row count caps apply during join task building"]
fn index_join_inner_row_count_upper_bound_golden() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:2089
/// TestOrderingIdxSelectivityThreshold` and `:2173
/// TestOrderingIdxSelectivityRatio`.
///
/// Mocked 100000-row / 1000-row stats suites run 32 and 21 recorded queries
/// exercising tidb_opt_ordering_index_selectivity_threshold/_ratio over ORDER
/// BY-matching indexes ib/ic.
#[test]
#[ignore = "go-parity-gap: ordering-index cost factors live in find-best-task/cost model"]
fn ordering_idx_selectivity_threshold_and_ratio_suites() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:2256
/// TestOrderingIdxSelectivityRatioForJoin`, `:2296 ...ForMergeJoin`, and
/// `:2360 ...ForApply`.
///
/// Live analyzed tables force index joins, merge joins, or Apply+Limit under
/// discouraging cost factors; explain format=verbose costs must be identical
/// for ratio -1/0 and strictly increasing across 0 -> 0.5 -> 1 whenever an
/// ordering index supplies the ORDER BY.
#[test]
#[ignore = "go-parity-gap: verbose-plan costing across join/apply shapes needs the optimizer loop"]
fn ordering_idx_selectivity_ratio_cost_monotonicity_for_join_shapes() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:2477
/// TestCrossValidationSelectivity`.
///
/// Clustered PK(a,b) analyzed at v2: `a = 1 and b > 0 and b < 1000 and c >
/// 1000` pins TableRangeScan range:(1 0,1 1000) at 2.00 with the residual c
/// predicate as Selection 1.00.
#[test]
#[ignore = "go-parity-gap: clustered composite-key range scan planning is outside this crate"]
fn cross_validation_selectivity_on_clustered_pk_range() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:2498
/// TestIgnoreRealtimeStats`.
///
/// tidb_opt_objective moderate/determinate switches RealtimeCount usage: an
/// unanalyzed table shows TableFullScan 11.00 vs 10000 pseudo; after ANALYZE
/// both agree (2.73/11.00); inserting four rows scales only moderate to 15.00/
/// 3.72 while determinate stays frozen.
#[test]
#[ignore = "go-parity-gap: optimizer objective mode gates realtime stats at plan time"]
fn ignore_realtime_stats_by_optimizer_objective() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:2567
/// TestSubsetIdxCardinality`.
///
/// After loading queued histograms (sync wait 0), full-load flags hold for
/// every column of iabc and the index itself; the five recorded distinct/count
/// plans pin subset-vs-full index cardinality behavior.
#[test]
#[ignore = "go-parity-gap: async stats-load queue lifecycle spans session machinery"]
fn subset_idx_cardinality_after_async_stats_load() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:2635
/// TestBuiltinInEstWithoutStats`.
///
/// Pseudo-stat table with ten rows: `a IN (1..8)` records Selection 1.00 over
/// TableFullScan 10.00 stats:pseudo and must survive InitStatsLite/InitStats
/// refreshes unchanged; ColAndIdxExistenceMap ends populated but with no
/// analyzed columns.
#[test]
#[ignore = "go-parity-gap: recorded floor comes from Selectivity()/plan composition over pseudo stats"]
fn builtin_in_estimate_without_stats_keeps_selection_floor() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:2754
/// TestRiskRangeSkewRatioWithinBucket`.
///
/// Single-bucket index (analyze with 0 topn, 1 buckets): probing [2,3] stays
/// inside the bucket where widening applies; counts must rise monotonically
/// across session ratios 0/0.5/1 with global-set and default-session values
/// behaving like the plain estimator.
#[test]
#[ignore = "go-parity-gap: within-bucket skew widening dispatch sits behind live-analyzed v1/v2 index stats"]
fn risk_range_skew_ratio_widens_within_bucket_estimates() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:2942
/// TestLastBucketEndValueHeuristic`.
///
/// Value 11 appears once against buckets of ~100 (5-bucket analyze); ten extra
/// copies stay under the 50-row trigger so the estimate hugs 1, ninety more
/// trip the heuristic lifting the estimate to ~100.09 while mid-histogram
/// value 3 reads ~109.99; index paths mirror both numbers.
#[test]
#[ignore = "go-parity-gap: needs the merged 5-bucket histogram shape produced by live ANALYZE"]
fn last_bucket_end_value_heuristic_lifts_underrepresented_counts() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:3039 TestIssue64137`.
///
/// Ten thousand rows for a=1 analyzed with single-value TopN: out-of-range
/// a=99999999 estimates 24.00 via the small-NDV out-of-range band while a=1
/// keeps the exact 12000.00.
#[test]
#[ignore = "go-parity-gap: index-reader row counts need TopN-stripped analyze output"]
fn issue_64137_small_ndv_out_of_range_index_reader_rows() {}

/// GO PORT of `pkg/planner/cardinality/selectivity_test.go:3069
/// TestUninitializedStats`.
///
/// Expression index idx_expr((cast(json_unquote(...)) collate utf8mb4_bin)):
/// after explain-analyze triggers loading, show stats_histograms must not list
/// allEvicted states and replans must never print unInitialized.
#[test]
#[ignore = "go-parity-gap: expression-index stats loading states span session/domain owners"]
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
