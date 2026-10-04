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

//! Behavioral tests retained from the Go source inventory.
//! Removed empty entries and their original contracts are indexed in
//! rust/docs/parity/current-audit/empty-test-cleanup-obligations.json.

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
        &EstimatorOptions::default(),
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
        &EstimatorOptions {
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
        &EstimatorOptions {
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
            &EstimatorOptions {
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
                &EstimatorOptions {
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
                &EstimatorOptions {
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
            &EstimatorOptions {
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
        &EstimatorOptions::default(),
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

// Go TestVirtualColumnIndexEstimation (issue #69134) is exercised through
// tidb-session::tests_explain::virtual_column_index_estimation_preserves_the_selective_suffix.
// The native estimator's missing-virtual, missing-ordinary, TopN-only, and
// recursive-success branches are covered by
// row_count_estimator::recursive_index_estimation_tests::missing_virtual_column_requires_index_histogram_fallback.
// Recursive error injection belongs to TestNewIndexWithColumnStats below;
// its error-propagation coverage remains open.

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
            &EstimatorOptions::default(),
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
            &EstimatorOptions::default(),
        )
        .expect("the primary-key handle point is estimable");
        assert_eq!(column_estimate.est, 1.0, "pk handle value={value}");
    }
}

/// Go `pkg/planner/cardinality/selectivity_test.go:1418`,
/// `TestDefaultStringMatchSelectivityZeroImprovesLikeEstimation`, is exercised
/// through its active session/EXPLAIN path in
/// `tidb_session::tests_explain::default_string_match_selectivity_zero_improves_like_estimates`.

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
        &EstimatorOptions::default(),
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
        &EstimatorOptions::default(),
    )
    .expect("scaled composite point sweep is estimable");
    assert!(
        (after_growth.est - 5_400.0).abs() <= 1.0,
        "Go TestIssue39593 scaled estimate: {}",
        after_growth.est
    );
}

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
            &EstimatorOptions {
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
            &EstimatorOptions::default(),
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
            &EstimatorOptions::default(),
        )
        .unwrap()
    };
    assert!((estimate_index(11, 1101, 100).est - 100.09).abs() < 0.1);
    assert!((estimate_index(3, 1101, 100).est - 109.99).abs() < 0.1);
}

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
