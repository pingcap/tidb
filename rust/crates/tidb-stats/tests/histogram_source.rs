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

//! Source-backed tests for `tidb_stats::histogram`.
//!
//! Fixtures below were captured by building the same histograms in Go
//! (`pkg/statistics`, via a throwaway `zz_dump_histvec_test.go` -- deleted
//! after capture, not part of this repo) with `NewHistogram` /
//! `AppendBucketWithNDV` and printing `EqualRowCount` / `LessRowCount` /
//! `GreaterRowCount` / `BetweenRowCount` at 30+ probe points across
//! BIGINT, VARCHAR, DECIMAL, and DATETIME bounds. Every expected number here
//! is the literal Go-printed float64 value (or its exact bit-identical
//! decimal literal), so equality assertions require zero drift.

use std::{collections::HashSet, fs, path::PathBuf};

use sha2::{Digest, Sha256};
use tidb_datatype::{Collation, Datum};
use tidb_stats::average_count::avg_count_per_not_null_value;
use tidb_stats::histogram::{
    deep_slice, merge_histograms, merge_partition_histograms, Bucket, Histogram,
    HistogramMergeError, OutOfRangeContext, PartitionMergeOptions, TopNMergeEntry,
    OUT_OF_RANGE_BETWEEN_RATE,
};
use tidb_stats::overlap_geometry::{left_overlap_percent, right_overlap_percent};
use tidb_stats::row_estimate::{calculate_skew_ratio_counts, default_row_est, RowEstimate};
use tidb_stats::stats_version::{
    is_analyzed, is_column_analyzed_or_synthesized, VERSION_0, VERSION_1, VERSION_2,
};
use tidb_stats::status::{StatsLoadedStatus, ALL_EVICTED, ALL_LOADED};

const EPS: f64 = 1e-9;
const INVENTORY: &str = include_str!("../src/histogram.inventory.tsv");
const DECLINE_EVIDENCE: &str = include_str!("../src/histogram.evidence.tsv");
const MUTATION_PLAN: &str = include_str!("../src/histogram.mutation-plan.tsv");
const GO_HISTOGRAM_SHA256: &str =
    "1233e0a3430067400eaee5d562772cc83541fce8ae8b3e4579895a574c8c1024";
const GO_HISTOGRAM_TEST_SHA256: &str =
    "8adb0d249a37ffa08c859ea1709426cfc0e98c4fc5a7ff689726fce0a1904a7a";
const GO_HISTOGRAM_BENCH_SHA256: &str =
    "7c7a0a4ca77720ea94afa353ed1070cce6b03f6735f87780ca4767e10ff74780";
const GO_ADJACENT_TEST_SHA256: &str =
    "2252313be49f161986afe7a94c379ca875bf79930b4f6c52a0b5e3cd9ffafe35";
const RUST_HISTOGRAM_SHA256: &str =
    "f01d5fe10a8bae9531c6588c84f31fdfa14c9c1c0dd386c5a2c1145c35cbfb95";
const INVENTORY_SHA256: &str = "377ef285f6824b12d7f89e74b3ab2f41d4b49dc0de6b7205ff6b4a51dc0353e5";
const DECLINE_EVIDENCE_SHA256: &str =
    "6a741b83456edbeaba33893451c26c006171d608f99b28a894140586132d6a21";
const MUTATION_PLAN_SHA256: &str =
    "b5a1f4e0120c3014ec80c0a0e01bef887f78978c6dc11cb618263db8aab67c2b";

const COMPILE_ANCHORED_SYMBOLS: &[&str] = &[
    "ALL_EVICTED",
    "ALL_LOADED",
    "Bucket",
    "BucketForMerging",
    "BucketForMerging::clone",
    "BucketForMerging::from_histogram",
    "BucketForMerging::from_topn",
    "Histogram",
    "Histogram::abs_row_count_difference",
    "Histogram::append_bucket",
    "Histogram::append_bucket_with_ndv",
    "Histogram::between_row_count",
    "Histogram::binary_search_remove_value",
    "Histogram::bucket_count",
    "Histogram::copy",
    "Histogram::equal_row_count",
    "Histogram::get_increase_factor",
    "Histogram::get_lower",
    "Histogram::get_upper",
    "Histogram::greater_row_count",
    "Histogram::len",
    "Histogram::less_row_count",
    "Histogram::less_row_count_with_bkt_idx",
    "Histogram::locate_bucket",
    "Histogram::lower_to_datum",
    "Histogram::merge_neighbor_buckets",
    "Histogram::new",
    "Histogram::not_null_count",
    "Histogram::out_of_range",
    "Histogram::out_of_range_row_count",
    "Histogram::pop_first_bucket",
    "Histogram::remove_values",
    "Histogram::standardize_for_v2_analyze_index",
    "Histogram::total_row_count",
    "Histogram::truncate",
    "Histogram::update_last_bucket",
    "Histogram::upper_to_datum",
    "IntBucket",
    "OUT_OF_RANGE_BETWEEN_RATE",
    "RowEstimate",
    "RowEstimate::add",
    "RowEstimate::add_all",
    "RowEstimate::clamp",
    "RowEstimate::divide_all",
    "RowEstimate::multiply_all",
    "RowEstimate::subtract",
    "StatsLoadedStatus",
    "StatsLoadedStatus::all_evicted",
    "StatsLoadedStatus::copy",
    "StatsLoadedStatus::full_load",
    "StatsLoadedStatus::is_all_evicted",
    "StatsLoadedStatus::is_essential_stats_loaded",
    "StatsLoadedStatus::is_full_load",
    "StatsLoadedStatus::is_load_needed",
    "StatsLoadedStatus::stats_initialized",
    "TopNMergeEntry",
    "VERSION_0",
    "VERSION_1",
    "VERSION_2",
    "avg_count_per_not_null_value",
    "buckets_are_sorted",
    "calculate_skew_ratio_counts",
    "consecutive_histogram",
    "deep_slice",
    "default_row_est",
    "is_analyzed",
    "is_column_analyzed_or_synthesized",
    "left_overlap_percent",
    "merge_bucket",
    "merge_bucket_ndv",
    "merge_histograms",
    "merge_partition_buckets",
    "merge_partition_histograms",
    "right_overlap_percent",
    "sort_buckets_by_upper_bound",
    "source_index_merge_histogram",
    "source_merge_bucket_ndv_matches_all_go_cases",
    "source_merge_histograms_matches_go_cases",
    "source_merge_partition_level_hist_matches_all_go_cases",
    "source_standardize_v2_index_matches_all_go_tables",
    "source_truncate_histogram_keeps_metadata_and_prefix",
];

const PORTED_SYMBOL_TESTS: &[(&str, &str)] = &[
    (
        "ALL_EVICTED",
        "status_source::source_all_evicted_status_requires_reload_and_loses_essential_stats",
    ),
    (
        "ALL_LOADED",
        "status_source::source_full_load_status_is_initialized_without_reload",
    ),
    (
        "Bucket",
        "histogram_source::source_int_histogram_basic_shape",
    ),
    (
        "BucketForMerging",
        "histogram_source::source_merge_partition_level_hist_matches_all_go_cases",
    ),
    (
        "BucketForMerging::clone",
        "histogram_source::source_merge_partition_level_hist_matches_all_go_cases",
    ),
    (
        "BucketForMerging::from_histogram",
        "histogram_source::source_merge_partition_level_hist_matches_all_go_cases",
    ),
    (
        "BucketForMerging::from_topn",
        "histogram_source::source_partition_merge_empty_error_and_topn_boundaries_match_oracle",
    ),
    (
        "Histogram",
        "histogram_source::source_int_histogram_basic_shape",
    ),
    (
        "Histogram::abs_row_count_difference",
        "histogram_source::source_out_of_range_estimation_matches_go_boundaries",
    ),
    (
        "Histogram::append_bucket",
        "histogram_source::source_merge_histograms_matches_go_cases",
    ),
    (
        "Histogram::append_bucket_with_ndv",
        "histogram_source::source_merge_partition_level_hist_matches_all_go_cases",
    ),
    (
        "Histogram::between_row_count",
        "histogram_source::source_int_histogram_between_row_count_probes",
    ),
    (
        "Histogram::binary_search_remove_value",
        "histogram_source::source_topn_removal_matches_go_boundaries",
    ),
    (
        "Histogram::bucket_count",
        "histogram_source::source_int_histogram_equal_less_greater_probes",
    ),
    (
        "Histogram::copy",
        "histogram_source::source_bound_access_copy_and_deep_slice_match_go_ownership",
    ),
    (
        "Histogram::equal_row_count",
        "histogram_source::source_int_histogram_equal_less_greater_probes",
    ),
    (
        "Histogram::get_increase_factor",
        "histogram_source::source_out_of_range_estimation_matches_go_boundaries",
    ),
    (
        "Histogram::get_lower",
        "histogram_source::source_bound_access_copy_and_deep_slice_match_go_ownership",
    ),
    (
        "Histogram::get_upper",
        "histogram_source::source_bound_access_copy_and_deep_slice_match_go_ownership",
    ),
    (
        "Histogram::greater_row_count",
        "histogram_source::source_int_histogram_equal_less_greater_probes",
    ),
    (
        "Histogram::len",
        "histogram_source::source_int_histogram_basic_shape",
    ),
    (
        "Histogram::less_row_count",
        "histogram_source::source_int_histogram_equal_less_greater_probes",
    ),
    (
        "Histogram::less_row_count_with_bkt_idx",
        "histogram_source::source_int_histogram_equal_less_greater_probes",
    ),
    (
        "Histogram::locate_bucket",
        "histogram_source::source_int_histogram_equal_less_greater_probes",
    ),
    (
        "Histogram::lower_to_datum",
        "histogram_source::source_bound_access_copy_and_deep_slice_match_go_ownership",
    ),
    (
        "Histogram::merge_neighbor_buckets",
        "histogram_source::source_merge_histograms_matches_go_cases",
    ),
    (
        "Histogram::new",
        "histogram_source::source_merge_histograms_matches_go_cases",
    ),
    (
        "Histogram::not_null_count",
        "histogram_source::source_int_histogram_basic_shape",
    ),
    (
        "Histogram::out_of_range",
        "histogram_source::source_out_of_range_estimation_matches_go_boundaries",
    ),
    (
        "Histogram::out_of_range_row_count",
        "histogram_source::source_out_of_range_estimation_matches_go_boundaries",
    ),
    (
        "Histogram::pop_first_bucket",
        "histogram_source::source_merge_histograms_matches_go_cases",
    ),
    (
        "Histogram::remove_values",
        "histogram_source::source_topn_removal_matches_go_boundaries",
    ),
    (
        "Histogram::standardize_for_v2_analyze_index",
        "histogram_source::source_standardize_v2_index_matches_all_go_tables",
    ),
    (
        "Histogram::total_row_count",
        "histogram_source::source_int_histogram_basic_shape",
    ),
    (
        "Histogram::truncate",
        "histogram_source::source_truncate_histogram_keeps_metadata_and_prefix",
    ),
    (
        "Histogram::update_last_bucket",
        "histogram_source::source_merge_histograms_matches_go_cases",
    ),
    (
        "Histogram::upper_to_datum",
        "histogram_source::source_bound_access_copy_and_deep_slice_match_go_ownership",
    ),
    (
        "IntBucket",
        "histogram_source::source_merge_partition_level_hist_matches_all_go_cases",
    ),
    (
        "OUT_OF_RANGE_BETWEEN_RATE",
        "histogram_source::source_out_of_range_estimation_matches_go_boundaries",
    ),
    (
        "RowEstimate",
        "row_estimate_source::source_default_estimate_repeats_value",
    ),
    (
        "RowEstimate::add",
        "row_estimate_source::source_arithmetic_methods_update_all_fields_in_place",
    ),
    (
        "RowEstimate::add_all",
        "row_estimate_source::source_arithmetic_methods_update_all_fields_in_place",
    ),
    (
        "RowEstimate::clamp",
        "row_estimate_source::source_clamp_keeps_default_between_min_and_max",
    ),
    (
        "RowEstimate::divide_all",
        "row_estimate_source::source_arithmetic_methods_update_all_fields_in_place",
    ),
    (
        "RowEstimate::multiply_all",
        "row_estimate_source::source_arithmetic_methods_update_all_fields_in_place",
    ),
    (
        "RowEstimate::subtract",
        "row_estimate_source::source_arithmetic_methods_update_all_fields_in_place",
    ),
    (
        "StatsLoadedStatus",
        "status_source::source_zero_value_is_uninitialized_and_does_not_reload",
    ),
    (
        "StatsLoadedStatus::all_evicted",
        "status_source::source_all_evicted_status_requires_reload_and_loses_essential_stats",
    ),
    (
        "StatsLoadedStatus::copy",
        "status_source::source_copy_is_value_independent",
    ),
    (
        "StatsLoadedStatus::full_load",
        "status_source::source_full_load_status_is_initialized_without_reload",
    ),
    (
        "StatsLoadedStatus::is_all_evicted",
        "status_source::source_all_evicted_status_requires_reload_and_loses_essential_stats",
    ),
    (
        "StatsLoadedStatus::is_essential_stats_loaded",
        "status_source::source_all_evicted_status_requires_reload_and_loses_essential_stats",
    ),
    (
        "StatsLoadedStatus::is_full_load",
        "status_source::source_full_load_status_is_initialized_without_reload",
    ),
    (
        "StatsLoadedStatus::is_load_needed",
        "status_source::source_all_evicted_status_requires_reload_and_loses_essential_stats",
    ),
    (
        "StatsLoadedStatus::stats_initialized",
        "status_source::source_full_load_status_is_initialized_without_reload",
    ),
    (
        "TopNMergeEntry",
        "histogram_source::source_topn_removal_matches_go_boundaries",
    ),
    (
        "VERSION_0",
        "stats_version_source::source_version_constants_and_analyzed_predicate_match",
    ),
    (
        "VERSION_1",
        "stats_version_source::source_version_constants_and_analyzed_predicate_match",
    ),
    (
        "VERSION_2",
        "stats_version_source::source_version_constants_and_analyzed_predicate_match",
    ),
    (
        "avg_count_per_not_null_value",
        "average_count_source::source_average_scales_nonnull_count_and_ndv_together",
    ),
    (
        "buckets_are_sorted",
        "histogram_source::source_merge_partition_level_hist_matches_all_go_cases",
    ),
    (
        "calculate_skew_ratio_counts",
        "row_estimate_source::source_skew_ratio_matches_default_min_max_formula",
    ),
    (
        "consecutive_histogram",
        "histogram_source::source_merge_histograms_matches_go_cases",
    ),
    (
        "deep_slice",
        "histogram_source::source_bound_access_copy_and_deep_slice_match_go_ownership",
    ),
    (
        "default_row_est",
        "row_estimate_source::source_default_estimate_repeats_value",
    ),
    (
        "is_analyzed",
        "stats_version_source::source_version_constants_and_analyzed_predicate_match",
    ),
    (
        "is_column_analyzed_or_synthesized",
        "stats_version_source::source_column_predicate_accepts_analyzed_or_synthesized_stats",
    ),
    (
        "left_overlap_percent",
        "overlap_geometry_source::source_left_overlap_clips_to_histogram_triangle",
    ),
    (
        "merge_bucket",
        "histogram::tests::source_merge_bucket_ndv_matches_all_go_cases",
    ),
    (
        "merge_bucket_ndv",
        "histogram::tests::source_merge_bucket_ndv_matches_all_go_cases",
    ),
    (
        "merge_histograms",
        "histogram_source::source_merge_histograms_matches_go_cases",
    ),
    (
        "merge_partition_buckets",
        "histogram_source::source_merge_partition_level_hist_matches_all_go_cases",
    ),
    (
        "merge_partition_histograms",
        "histogram_source::source_merge_partition_level_hist_matches_all_go_cases",
    ),
    (
        "right_overlap_percent",
        "overlap_geometry_source::source_right_overlap_clips_to_histogram_triangle",
    ),
    (
        "sort_buckets_by_upper_bound",
        "histogram_source::source_merge_partition_level_hist_matches_all_go_cases",
    ),
    (
        "source_index_merge_histogram",
        "histogram_source::source_merge_partition_level_hist_matches_all_go_cases",
    ),
    (
        "source_merge_bucket_ndv_matches_all_go_cases",
        "histogram::tests::source_merge_bucket_ndv_matches_all_go_cases",
    ),
    (
        "source_merge_histograms_matches_go_cases",
        "histogram_source::source_merge_histograms_matches_go_cases",
    ),
    (
        "source_merge_partition_level_hist_matches_all_go_cases",
        "histogram_source::source_merge_partition_level_hist_matches_all_go_cases",
    ),
    (
        "source_standardize_v2_index_matches_all_go_tables",
        "histogram_source::source_standardize_v2_index_matches_all_go_tables",
    ),
    (
        "source_truncate_histogram_keeps_metadata_and_prefix",
        "histogram_source::source_truncate_histogram_keeps_metadata_and_prefix",
    ),
];

const HISTOGRAM_RS: &str = include_str!("../src/histogram.rs");
const HISTOGRAM_SOURCE_RS: &str = include_str!("histogram_source.rs");
const ROW_ESTIMATE_SOURCE_RS: &str = include_str!("row_estimate_source.rs");
const STATUS_SOURCE_RS: &str = include_str!("status_source.rs");
const STATS_VERSION_SOURCE_RS: &str = include_str!("stats_version_source.rs");
const OVERLAP_GEOMETRY_SOURCE_RS: &str = include_str!("overlap_geometry_source.rs");
const AVERAGE_COUNT_SOURCE_RS: &str = include_str!("average_count_source.rs");

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .expect("repository root")
}

fn sha256_file(path: &PathBuf) -> String {
    format!(
        "{:x}",
        Sha256::digest(fs::read(path).expect("read locked source"))
    )
}

fn inventory_rows() -> Vec<Vec<&'static str>> {
    INVENTORY
        .lines()
        .filter(|line| {
            !line.is_empty() && !line.starts_with('#') && !line.starts_with("obligation_id\t")
        })
        .map(|line| line.split('\t').collect())
        .collect()
}

fn decline_evidence_rows() -> Vec<Vec<&'static str>> {
    DECLINE_EVIDENCE
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#') && !line.starts_with("id\t"))
        .map(|line| line.split('\t').collect())
        .collect()
}

fn mutation_plan_rows() -> Vec<Vec<&'static str>> {
    MUTATION_PLAN
        .lines()
        .filter(|line| {
            !line.is_empty() && !line.starts_with('#') && !line.starts_with("mutation_id\t")
        })
        .map(|line| line.split('\t').collect())
        .collect()
}

fn ported_test_source(identity: &str) -> Option<(&str, &'static str)> {
    [
        ("histogram::tests::", HISTOGRAM_RS),
        ("histogram_source::", HISTOGRAM_SOURCE_RS),
        ("row_estimate_source::", ROW_ESTIMATE_SOURCE_RS),
        ("status_source::", STATUS_SOURCE_RS),
        ("stats_version_source::", STATS_VERSION_SOURCE_RS),
        ("overlap_geometry_source::", OVERLAP_GEOMETRY_SOURCE_RS),
        ("average_count_source::", AVERAGE_COUNT_SOURCE_RS),
    ]
    .into_iter()
    .find_map(|(prefix, source)| identity.strip_prefix(prefix).map(|name| (name, source)))
}

fn source_declares_test(source: &str, test_name: &str) -> bool {
    let declaration = format!("fn {test_name}(");
    let mut previous_nonempty = "";
    for line in source.lines() {
        if line.trim_start().starts_with(&declaration) {
            return previous_nonempty.trim() == "#[test]";
        }
        if !line.trim().is_empty() {
            previous_nonempty = line;
        }
    }
    false
}

fn file_size_and_lines(path: &PathBuf) -> (usize, usize) {
    let bytes = fs::read(path).expect("read locked source");
    let lines = bytes.iter().filter(|&&byte| byte == b'\n').count();
    (bytes.len(), lines)
}

fn assert_close(actual: f64, expected: f64, label: &str) {
    assert!(
        (actual - expected).abs() < EPS,
        "{label}: expected {expected}, got {actual}"
    );
}

fn int_bucket(lower: i64, upper: i64, count: i64, repeat: i64, ndv: i64) -> Bucket {
    Bucket {
        count,
        repeat,
        ndv,
        lower_bound: Datum::new_int(lower),
        upper_bound: Datum::new_int(upper),
    }
}

fn int_histogram() -> Histogram {
    Histogram {
        id: 1,
        ndv: 40,
        null_count: 5,
        last_update_version: 0,
        tot_col_size: 0,
        correlation: 0.0,
        buckets: vec![
            int_bucket(0, 9, 10, 2, 8),
            int_bucket(10, 19, 20, 3, 9),
            int_bucket(20, 29, 35, 1, 10),
            int_bucket(40, 49, 45, 5, 5),
        ],
    }
}

#[test]
fn source_int_histogram_basic_shape() {
    let hist = int_histogram();
    assert_eq!(hist.id, 1);
    assert_eq!(hist.ndv, 40);
    assert_eq!(hist.null_count, 5);
    assert_eq!(hist.last_update_version, 0);
    assert_eq!(hist.tot_col_size, 0);
    assert_eq!(hist.correlation, 0.0);
    assert_eq!(hist.len(), 4);
    assert_eq!(
        hist.buckets
            .iter()
            .map(|bucket| (bucket.count, bucket.repeat, bucket.ndv))
            .collect::<Vec<_>>(),
        vec![(10, 2, 8), (20, 3, 9), (35, 1, 10), (45, 5, 5)]
    );
    assert_close(hist.not_null_count(), 45.0, "not_null_count");
    assert_close(hist.total_row_count(), 50.0, "total_row_count");
}

#[test]
fn source_int_histogram_equal_less_greater_probes() {
    let hist = int_histogram();
    // (probe, equal, matched, less, greater)
    let cases: &[(i64, f64, bool, f64, f64)] = &[
        (-5, 0.0, false, 0.0, 45.0),
        (0, 1.1428571428571428, true, 0.0, 43.875),
        (
            3,
            1.1428571428571428,
            true,
            2.6666666666666665,
            41.208333333333336,
        ),
        (9, 2.0, true, 8.0, 35.0),
        (10, 0.875, true, 10.0, 33.875),
        (15, 0.875, true, 13.88888888888889, 29.98611111111111),
        (19, 3.0, true, 17.0, 25.0),
        (20, 1.5555555555555556, true, 20.0, 23.875),
        (
            25,
            1.5555555555555556,
            true,
            27.77777777777778,
            16.09722222222222,
        ),
        (29, 1.0, true, 34.0, 10.0),
        (30, 0.0, false, 35.0, 10.0),
        (35, 0.0, false, 35.0, 10.0),
        (40, 1.25, true, 35.0, 8.875),
        (45, 1.25, true, 37.77777777777778, 6.097222222222221),
        (49, 5.0, true, 40.0, 0.0),
        (100, 0.0, false, 45.0, 0.0),
    ];
    for &(probe, equal, matched, less, greater) in cases {
        let value = Datum::new_int(probe);
        let (eq, m) = hist.equal_row_count(&value, true, Collation::Binary);
        assert_close(eq, equal, &format!("equal({probe})"));
        assert_eq!(m, matched, "matched({probe})");
        assert_close(
            hist.less_row_count(&value, Collation::Binary),
            less,
            &format!("less({probe})"),
        );
        assert_close(
            hist.greater_row_count(&value, Collation::Binary),
            greater,
            &format!("greater({probe})"),
        );
    }
}

#[test]
fn source_int_histogram_between_row_count_probes() {
    let hist = int_histogram();
    // (a, b, est) -- est == min_est == max_est on the version-1 path used
    // here (no sctx, matching the Go fixture which passed nil).
    let cases: &[(i64, i64, f64)] = &[
        (0, 10, 10.0),
        (5, 15, 9.444444444444445),
        (10, 30, 25.0),
        (-5, 50, 45.0),
        (21, 25, 6.222222222222221),
        (40, 49, 5.0),
    ];
    for &(a, b, est) in cases {
        let result = hist.between_row_count(
            &Datum::new_int(a),
            &Datum::new_int(b),
            Collation::Binary,
            None,
        );
        assert_close(result.est, est, &format!("between[{a},{b})"));
        assert_close(result.min_est, est, &format!("between[{a},{b}).min"));
        assert_close(result.max_est, est, &format!("between[{a},{b}).max"));
    }
}

#[test]
fn source_topn_removal_matches_go_boundaries() {
    let mut single = int_histogram();
    single
        .binary_search_remove_value(&Datum::new_int(9), 2, Collation::Binary)
        .unwrap();
    assert_int_buckets(
        &single,
        &[
            (0, 9, 8, 0, 7),
            (10, 19, 18, 3, 9),
            (20, 29, 33, 1, 10),
            (40, 49, 43, 5, 5),
        ],
    );

    let unchanged = int_histogram();
    for value in [-1, 50] {
        let mut actual = unchanged.clone();
        actual
            .binary_search_remove_value(&Datum::new_int(value), 9, Collation::Binary)
            .unwrap();
        assert_eq!(actual, unchanged);
    }

    let mut bulk = Histogram::new(1, 12, 0, 0, 3, 0);
    for (lower, upper, count, repeat, ndv) in
        [(1_u8, 3_u8, 5, 1, 3), (4, 6, 10, 2, 3), (7, 9, 15, 3, 3)]
    {
        bulk.append_bucket_with_ndv(
            Datum::Bytes(vec![lower]),
            Datum::Bytes(vec![upper]),
            count,
            repeat,
            ndv,
        );
    }
    bulk.remove_values(
        &[
            TopNMergeEntry {
                value: Datum::Bytes(vec![2]),
                count: 2,
            },
            TopNMergeEntry {
                value: Datum::Bytes(vec![6]),
                count: 3,
            },
            TopNMergeEntry {
                value: Datum::Bytes(vec![8]),
                count: 4,
            },
            TopNMergeEntry {
                value: Datum::Bytes(vec![9]),
                count: 5,
            },
        ],
        Collation::Binary,
    )
    .unwrap();
    assert_eq!(
        bulk.buckets
            .iter()
            .map(|bucket| (bucket.count, bucket.repeat, bucket.ndv))
            .collect::<Vec<_>>(),
        vec![(3, 1, 2), (5, 0, 2), (1, 0, 1)]
    );
}

#[test]
fn source_out_of_range_estimation_matches_go_boundaries() {
    let histogram = int_histogram();
    assert!(!histogram.out_of_range(&Datum::new_int(0), Collation::Binary));
    assert!(!histogram.out_of_range(&Datum::new_int(49), Collation::Binary));
    assert!(histogram.out_of_range(&Datum::new_int(-1), Collation::Binary));
    assert!(histogram.out_of_range(&Datum::new_int(50), Collation::Binary));
    assert_eq!(histogram.abs_row_count_difference(60), 10.0);
    assert_eq!(histogram.get_increase_factor(100), 2.0);

    let context = OutOfRangeContext {
        realtime_row_count: 60,
        modify_count: 10,
        hist_ndv: 40,
        unsigned: false,
        allow_use_modify_count: true,
        skew_ratio: 0.0,
    };
    let estimate =
        histogram.out_of_range_row_count(&Datum::new_int(-49), &Datum::new_int(-1), context);
    assert_close(estimate.est, 2.399000416493128, "out-of-range estimate");
    assert_close(estimate.min_est, 1.0, "out-of-range minimum");
    assert_close(estimate.max_est, 9.596001665972512, "out-of-range maximum");

    let determinate = histogram.out_of_range_row_count(
        &Datum::new_int(-49),
        &Datum::new_int(-1),
        OutOfRangeContext {
            allow_use_modify_count: false,
            ..context
        },
    );
    assert_eq!(determinate, default_row_est(1.125));

    let impossible_unsigned = histogram.out_of_range_row_count(
        &Datum::new_int(-5),
        &Datum::new_int(-1),
        OutOfRangeContext {
            unsigned: true,
            ..context
        },
    );
    assert_eq!(impossible_unsigned, default_row_est(0.0));
    assert_eq!(
        Histogram::default().out_of_range_row_count(
            &Datum::new_int(-1),
            &Datum::new_int(1),
            context,
        ),
        default_row_est(0.0)
    );
}

fn str_bucket(lower: &str, upper: &str, count: i64, repeat: i64, ndv: i64) -> Bucket {
    Bucket {
        count,
        repeat,
        ndv,
        lower_bound: Datum::new_string(lower.as_bytes().to_vec()),
        upper_bound: Datum::new_string(upper.as_bytes().to_vec()),
    }
}

fn string_histogram() -> Histogram {
    Histogram {
        id: 2,
        ndv: 30,
        null_count: 2,
        last_update_version: 0,
        tot_col_size: 100,
        correlation: 0.0,
        buckets: vec![
            str_bucket("apple", "cherry", 8, 2, 6),
            str_bucket("date", "kiwi", 18, 3, 7),
            str_bucket("lemon", "orange", 26, 4, 5),
        ],
    }
}

#[test]
fn source_string_histogram_probes() {
    let hist = string_histogram();
    // (probe, equal, matched, less, greater)
    let cases: &[(&str, f64, bool, f64, f64)] = &[
        ("aaa", 0.0, false, 0.0, 26.0),
        ("apple", 1.2, true, 0.0, 25.133333333333333),
        ("banana", 1.2, true, 2.869196710482877, 22.264136622850454),
        ("cherry", 2.0, true, 6.0, 18.0),
        ("cucumber", 0.0, false, 8.0, 18.0),
        ("date", 1.1666666666666667, true, 8.0, 17.133333333333333),
        (
            "grape",
            1.1666666666666667,
            true,
            11.052469849099014,
            14.08086348423432,
        ),
        ("kiwi", 3.0, true, 15.0, 8.0),
        ("lemon", 1.0, true, 18.0, 7.133333333333333),
        ("mango", 1.0, true, 19.290749886122345, 5.842583447210988),
        ("orange", 4.0, true, 22.0, 0.0),
        ("zzz", 0.0, false, 26.0, 0.0),
    ];
    for &(probe, equal, matched, less, greater) in cases {
        let value = Datum::new_string(probe.as_bytes().to_vec());
        let (eq, m) = hist.equal_row_count(&value, true, Collation::Binary);
        assert_close(eq, equal, &format!("str-equal({probe})"));
        assert_eq!(m, matched, "str-matched({probe})");
        assert_close(
            hist.less_row_count(&value, Collation::Binary),
            less,
            &format!("str-less({probe})"),
        );
        assert_close(
            hist.greater_row_count(&value, Collation::Binary),
            greater,
            &format!("str-greater({probe})"),
        );
    }
}

fn decimal_bucket(lower: &str, upper: &str, count: i64, repeat: i64, ndv: i64) -> Bucket {
    Bucket {
        count,
        repeat,
        ndv,
        lower_bound: Datum::new_decimal(tidb_datatype::Decimal::from_signed_literal(lower)),
        upper_bound: Datum::new_decimal(tidb_datatype::Decimal::from_signed_literal(upper)),
    }
}

fn decimal_histogram() -> Histogram {
    Histogram {
        id: 3,
        ndv: 20,
        null_count: 1,
        last_update_version: 0,
        tot_col_size: 0,
        correlation: 0.0,
        buckets: vec![
            decimal_bucket("1.50", "10.25", 12, 2, 8),
            decimal_bucket("10.25", "99.99", 22, 3, 6),
        ],
    }
}

#[test]
fn source_decimal_histogram_probes() {
    let hist = decimal_histogram();
    // (probe, equal, matched, less, greater)
    let cases: &[(&str, f64, bool, f64, f64)] = &[
        ("0.00", 0.0, false, 0.0, 22.0),
        ("1.50", 1.4285714285714286, true, 0.0, 20.9),
        ("5.00", 1.4285714285714286, true, 4.0, 16.9),
        ("10.25", 2.0, true, 10.0, 10.0),
        ("50.00", 1.4, true, 15.100624024960998, 5.799375975039002),
        ("99.99", 3.0, true, 19.0, 0.0),
        ("200.00", 0.0, false, 22.0, 0.0),
    ];
    for &(probe, equal, matched, less, greater) in cases {
        let value = Datum::new_decimal(tidb_datatype::Decimal::from_signed_literal(probe));
        let (eq, m) = hist.equal_row_count(&value, true, Collation::Binary);
        assert_close(eq, equal, &format!("dec-equal({probe})"));
        assert_eq!(m, matched, "dec-matched({probe})");
        assert_close(
            hist.less_row_count(&value, Collation::Binary),
            less,
            &format!("dec-less({probe})"),
        );
        assert_close(
            hist.greater_row_count(&value, Collation::Binary),
            greater,
            &format!("dec-greater({probe})"),
        );
    }
}

fn parse_naive_datetime(text: &str) -> tidb_datatype::Time {
    tidb_datatype::parse_datetime(text, &chrono_tz::UTC, false, false)
        .expect("valid fixture datetime literal")
        .time
}

fn time_bucket(lower: &str, upper: &str, count: i64, repeat: i64, ndv: i64) -> Bucket {
    Bucket {
        count,
        repeat,
        ndv,
        lower_bound: Datum::new_time(parse_naive_datetime(lower)),
        upper_bound: Datum::new_time(parse_naive_datetime(upper)),
    }
}

fn time_histogram() -> Histogram {
    Histogram {
        id: 4,
        ndv: 25,
        null_count: 0,
        last_update_version: 0,
        tot_col_size: 0,
        correlation: 0.0,
        buckets: vec![
            time_bucket("2020-01-01 00:00:00", "2020-06-01 00:00:00", 15, 2, 9),
            time_bucket("2020-06-01 00:00:00", "2021-01-01 00:00:00", 28, 3, 8),
        ],
    }
}

#[test]
fn source_time_histogram_probes() {
    let hist = time_histogram();
    // (probe, equal, matched, less, greater)
    let cases: &[(&str, f64, bool, f64, f64)] = &[
        ("2019-01-01 00:00:00", 0.0, false, 0.0, 28.0),
        ("2020-01-01 00:00:00", 1.625, true, 0.0, 26.88),
        (
            "2020-03-15 00:00:00",
            1.625,
            true,
            6.328947368421053,
            20.551052631578944,
        ),
        ("2020-06-01 00:00:00", 2.0, true, 13.0, 13.0),
        (
            "2020-09-01 00:00:00",
            1.4285714285714286,
            true,
            19.299065420560748,
            7.580934579439252,
        ),
        ("2021-01-01 00:00:00", 3.0, true, 25.0, 0.0),
        ("2022-01-01 00:00:00", 0.0, false, 28.0, 0.0),
    ];
    for &(probe, equal, matched, less, greater) in cases {
        let value = Datum::new_time(parse_naive_datetime(probe));
        let (eq, m) = hist.equal_row_count(&value, true, Collation::Binary);
        assert_close(eq, equal, &format!("time-equal({probe})"));
        assert_eq!(m, matched, "time-matched({probe})");
        assert_close(
            hist.less_row_count(&value, Collation::Binary),
            less,
            &format!("time-less({probe})"),
        );
        assert_close(
            hist.greater_row_count(&value, Collation::Binary),
            greater,
            &format!("time-greater({probe})"),
        );
    }
}

type IntBucket = (i64, i64, i64, i64, i64);

fn source_merge_histogram(buckets: &[IntBucket], total_column_size: i64) -> Histogram {
    let mut histogram = Histogram::new(0, 0, 0, 0, buckets.len(), total_column_size);
    for &(lower, upper, count, repeat, ndv) in buckets {
        histogram.append_bucket_with_ndv(
            Datum::new_int(lower),
            Datum::new_int(upper),
            count,
            repeat,
            ndv,
        );
    }
    histogram
}

fn encoded_int(value: i64) -> Datum {
    let comparable = (value as u64 ^ (1_u64 << 63)).to_be_bytes();
    let mut encoded = Vec::with_capacity(9);
    encoded.push(3); // Go codec.intFlag.
    encoded.extend_from_slice(&comparable);
    Datum::Bytes(encoded)
}

fn source_index_merge_histogram(buckets: &[IntBucket], total_column_size: i64) -> Histogram {
    let mut histogram = Histogram::new(0, 0, 0, 0, buckets.len(), total_column_size);
    for &(lower, upper, count, repeat, ndv) in buckets {
        histogram.append_bucket_with_ndv(
            encoded_int(lower),
            encoded_int(upper),
            count,
            repeat,
            ndv,
        );
    }
    histogram
}

fn partition_options(expected_buckets: usize) -> PartitionMergeOptions {
    PartitionMergeOptions {
        expected_buckets,
        is_index: true,
        analyze_version: 2,
    }
}

fn assert_int_buckets(actual: &Histogram, expected: &[IntBucket]) {
    assert_eq!(actual.buckets.len(), expected.len());
    for (index, (bucket, expected)) in actual.buckets.iter().zip(expected).enumerate() {
        assert_eq!(
            bucket,
            &int_bucket(expected.0, expected.1, expected.2, expected.3, expected.4),
            "bucket {index}"
        );
    }
}

fn assert_index_buckets(actual: &Histogram, expected: &[IntBucket]) {
    assert_eq!(actual.buckets.len(), expected.len());
    for (index, (bucket, expected)) in actual.buckets.iter().zip(expected).enumerate() {
        assert_eq!(
            bucket.lower_bound,
            encoded_int(expected.0),
            "lower bound {index}"
        );
        assert_eq!(
            bucket.upper_bound,
            encoded_int(expected.1),
            "upper bound {index}"
        );
        assert_eq!(bucket.count, expected.2, "count {index}");
        assert_eq!(bucket.repeat, expected.3, "repeat {index}");
        assert_eq!(bucket.ndv, expected.4, "ndv {index}");
    }
}

#[test]
fn source_merge_partition_level_hist_matches_all_go_cases() {
    const PARTITION_1: &[IntBucket] = &[
        (1, 4, 2, 1, 2),
        (6, 9, 5, 2, 2),
        (12, 12, 8, 3, 1),
        (13, 15, 11, 1, 3),
    ];
    const PARTITION_2: &[IntBucket] = &[
        (2, 5, 2, 1, 2),
        (6, 7, 5, 2, 2),
        (11, 11, 8, 3, 1),
        (13, 17, 11, 1, 3),
    ];
    const ISSUE_49023_PARTITION_1: &[IntBucket] = &[
        (1, 4, 2, 1, 2),
        (6, 9, 5, 2, 2),
        (12, 12, 5, 3, 1),
        (13, 15, 11, 1, 3),
    ];
    const ISSUE_49023_PARTITION_N: &[IntBucket] = &[
        (2, 5, 2, 1, 2),
        (6, 7, 2, 2, 2),
        (11, 11, 8, 3, 1),
        (13, 17, 11, 1, 3),
    ];

    struct Case {
        partitions: Vec<&'static [IntBucket]>,
        popped_topn: &'static [(i64, u64)],
        expected: &'static [IntBucket],
        expected_buckets: usize,
    }
    let cases = [
        Case {
            partitions: vec![PARTITION_1, PARTITION_2],
            popped_topn: &[],
            expected: &[(1, 9, 10, 2, 7), (11, 17, 22, 1, 8)],
            expected_buckets: 2,
        },
        Case {
            partitions: vec![PARTITION_1, PARTITION_2],
            popped_topn: &[(18, 5), (4, 6)],
            expected: &[(1, 5, 10, 1, 2), (6, 12, 22, 3, 6), (13, 18, 33, 5, 5)],
            expected_buckets: 3,
        },
        Case {
            partitions: vec![
                ISSUE_49023_PARTITION_1,
                ISSUE_49023_PARTITION_N,
                ISSUE_49023_PARTITION_N,
                ISSUE_49023_PARTITION_N,
            ],
            popped_topn: &[(18, 5), (4, 6)],
            expected: &[(1, 9, 17, 2, 8), (11, 11, 35, 9, 1), (13, 18, 55, 5, 6)],
            expected_buckets: 3,
        },
    ];

    for (case_index, case) in cases.into_iter().enumerate() {
        let histograms = case
            .partitions
            .into_iter()
            .map(|buckets| source_index_merge_histogram(buckets, 11))
            .collect::<Vec<_>>();
        let popped_topn = case
            .popped_topn
            .iter()
            .map(|&(value, count)| TopNMergeEntry {
                value: encoded_int(value),
                count,
            })
            .collect::<Vec<_>>();
        let merged = merge_partition_histograms(
            &histograms,
            &popped_topn,
            partition_options(case.expected_buckets),
            Collation::Binary,
        )
        .unwrap()
        .unwrap();
        assert_index_buckets(&merged, case.expected);
        assert_eq!(merged.tot_col_size, histograms.len() as i64 * 11);
        assert_eq!(merged.len(), case.expected_buckets, "case {case_index}");
    }
}

#[test]
fn source_partition_merge_empty_error_and_topn_boundaries_match_oracle() {
    let error =
        merge_partition_histograms(&[], &[], partition_options(0), Collation::Binary).unwrap_err();
    assert_eq!(error, HistogramMergeError::ZeroExpectedBuckets);
    assert_eq!(error.to_string(), "expBucketNumber can not be zero");

    assert_eq!(
        merge_partition_histograms(&[], &[], partition_options(1), Collation::Binary).unwrap(),
        None
    );

    let empty = Histogram::new(7, 0, 4, 9, 0, 11);
    let merged = merge_partition_histograms(&[empty], &[], partition_options(1), Collation::Binary)
        .unwrap()
        .unwrap();
    assert!(merged.buckets.is_empty());
    assert_eq!(merged.null_count, 4);
    assert_eq!(merged.tot_col_size, 11);

    let histograms = [
        source_index_merge_histogram(&[(1, 3, 3, 1, 3)], 3),
        source_index_merge_histogram(&[(2, 5, 4, 1, 4)], 4),
    ];
    let popped = [TopNMergeEntry {
        value: encoded_int(4),
        count: 2,
    }];
    let merged = merge_partition_histograms(
        &histograms,
        &popped,
        partition_options(2),
        Collation::Binary,
    )
    .unwrap()
    .unwrap();
    assert_index_buckets(&merged, &[(1, 2, 2, 2, 2), (2, 5, 9, 1, 5)]);
    assert_eq!(merged.tot_col_size, 7);
}

#[test]
fn source_standardize_v2_index_matches_all_go_tables() {
    let cases: &[(&[IntBucket], &[IntBucket])] = &[
        (
            &[
                (111, 111, 0, 0, 0),
                (123, 123, 0, 0, 0),
                (34567, 5, 10, 3, 2),
            ],
            &[(34567, 5, 10, 3, 0)],
        ),
        (
            &[
                (111, 111, 0, 0, 0),
                (123, 123, 0, 0, 0),
                (34567, 5, 0, 0, 0),
            ],
            &[],
        ),
        (
            &[
                (34567, 5, 10, 3, 2),
                (876, 876, 10, 0, 0),
                (990, 990, 10, 0, 0),
            ],
            &[(34567, 5, 10, 3, 0)],
        ),
        (
            &[
                (111, 111, 10, 10, 1),
                (123, 34567, 12, 4, 20),
                (5, 990, 10, 6, 2),
            ],
            &[
                (111, 111, 10, 10, 0),
                (123, 34567, 12, 4, 0),
                (5, 990, 10, 6, 0),
            ],
        ),
        (
            &[
                (111, 111, 0, 0, 0),
                (123, 123, 0, 0, 0),
                (34567, 34567, 10, 3, 2),
                (5, 5, 10, 0, 0),
                (876, 876, 10, 0, 0),
                (990, 990, 20, 3, 2),
                (95, 95, 30, 3, 2),
            ],
            &[
                (34567, 34567, 10, 3, 0),
                (990, 990, 20, 3, 0),
                (95, 95, 30, 3, 0),
            ],
        ),
        (
            &[
                (111, 111, 0, 0, 0),
                (123, 123, 0, 0, 0),
                (34567, 34567, 10, 3, 2),
                (5, 5, 10, 0, 0),
                (876, 876, 20, 3, 2),
                (990, 990, 30, 3, 2),
                (95, 95, 30, 0, 0),
            ],
            &[
                (34567, 34567, 10, 3, 0),
                (876, 876, 20, 3, 0),
                (990, 990, 30, 3, 0),
            ],
        ),
    ];
    for &(input, expected) in cases {
        let mut histogram = source_merge_histogram(input, 0);
        histogram.standardize_for_v2_analyze_index();
        assert_int_buckets(&histogram, expected);
    }
}

fn consecutive_histogram(lower: i64, count: i64) -> Histogram {
    let mut histogram = Histogram::new(0, count, 0, 0, count as usize, 0);
    for offset in 0..count {
        histogram.append_bucket(
            Datum::new_int(lower + offset),
            Datum::new_int(lower + offset),
            offset + 1,
            1,
        );
    }
    histogram
}

#[test]
fn source_merge_histograms_matches_go_cases() {
    let cases = [
        (0, 0, 0, 1, 1, 1),
        (0, 200, 200, 200, 200, 400),
        (0, 200, 199, 200, 200, 399),
    ];
    for (left_lower, left_count, right_lower, right_count, buckets, ndv) in cases {
        let merged = merge_histograms(
            consecutive_histogram(left_lower, left_count),
            consecutive_histogram(right_lower, right_count),
            256,
            2,
            Collation::Binary,
        )
        .unwrap();
        assert_eq!(merged.ndv, ndv);
        assert_eq!(merged.len(), buckets);
        assert_eq!(merged.total_row_count(), (left_count + right_count) as f64);
        assert_eq!(merged.buckets[0].lower_bound, Datum::new_int(left_lower));
        assert_eq!(
            merged.buckets[merged.len() - 1].upper_bound,
            Datum::new_int(right_lower + right_count - 1)
        );
    }
}

#[test]
fn source_truncate_histogram_keeps_metadata_and_prefix() {
    let histogram = source_merge_histogram(&[(0, 1, 0, 1, 0)], 0);
    assert_eq!(histogram.truncate(1), histogram);
    assert!(histogram.truncate(0).buckets.is_empty());
}

#[test]
fn source_bound_access_copy_and_deep_slice_match_go_ownership() {
    let histogram = source_merge_histogram(&[(1, 2, 3, 1, 2)], 7);
    assert_eq!(histogram.get_lower(0), &Datum::new_int(1));
    assert_eq!(histogram.lower_to_datum(0), Datum::new_int(1));
    assert_eq!(histogram.get_upper(0), &Datum::new_int(2));
    assert_eq!(histogram.upper_to_datum(0), Datum::new_int(2));

    let mut copied = histogram.copy();
    copied.buckets[0].lower_bound = Datum::new_int(0);
    assert_eq!(histogram.get_lower(0), &Datum::new_int(1));
    assert_eq!(deep_slice(&[1_u8, 2, 3]), vec![1, 2, 3]);
}

#[test]
fn lockdown_public_and_test_histogram_symbols_compile() {
    let _ = std::mem::size_of::<Bucket>();
    let _ = std::mem::size_of::<Histogram>();
    let _ = std::mem::size_of::<IntBucket>();
    let _ = std::mem::size_of::<RowEstimate>();
    let _ = std::mem::size_of::<StatsLoadedStatus>();
    let _ = std::mem::size_of::<TopNMergeEntry>();
    let _ = ALL_EVICTED;
    let _ = ALL_LOADED;
    let _ = OUT_OF_RANGE_BETWEEN_RATE;
    let _ = VERSION_0;
    let _ = VERSION_1;
    let _ = VERSION_2;
    let _ = Histogram::abs_row_count_difference;
    let _ = Histogram::append_bucket;
    let _ = Histogram::append_bucket_with_ndv;
    let _ = Histogram::between_row_count;
    let _ = Histogram::binary_search_remove_value;
    let _ = Histogram::bucket_count;
    let _ = Histogram::copy;
    let _ = Histogram::equal_row_count;
    let _ = Histogram::get_increase_factor;
    let _ = Histogram::get_lower;
    let _ = Histogram::get_upper;
    let _ = Histogram::greater_row_count;
    let _ = Histogram::len;
    let _ = Histogram::less_row_count;
    let _ = Histogram::less_row_count_with_bkt_idx;
    let _ = Histogram::locate_bucket;
    let _ = Histogram::lower_to_datum;
    let _ = Histogram::new;
    let _ = Histogram::not_null_count;
    let _ = Histogram::out_of_range;
    let _ = Histogram::out_of_range_row_count;
    let _ = Histogram::remove_values;
    let _ = Histogram::standardize_for_v2_analyze_index;
    let _ = Histogram::total_row_count;
    let _ = Histogram::truncate;
    let _ = Histogram::upper_to_datum;
    let _ = RowEstimate::add;
    let _ = RowEstimate::add_all;
    let _ = RowEstimate::clamp;
    let _ = RowEstimate::divide_all;
    let _ = RowEstimate::multiply_all;
    let _ = RowEstimate::subtract;
    let _ = StatsLoadedStatus::all_evicted;
    let _ = StatsLoadedStatus::copy;
    let _ = StatsLoadedStatus::full_load;
    let _ = StatsLoadedStatus::is_all_evicted;
    let _ = StatsLoadedStatus::is_essential_stats_loaded;
    let _ = StatsLoadedStatus::is_full_load;
    let _ = StatsLoadedStatus::is_load_needed;
    let _ = StatsLoadedStatus::stats_initialized;
    let _ = avg_count_per_not_null_value;
    let _ = calculate_skew_ratio_counts;
    let _ = consecutive_histogram;
    let _ = deep_slice::<u8>;
    let _ = default_row_est;
    let _ = is_analyzed;
    let _ = is_column_analyzed_or_synthesized;
    let _ = left_overlap_percent;
    let _ = merge_histograms;
    let _ = merge_partition_histograms;
    let _ = right_overlap_percent;
    let _ = source_index_merge_histogram;
    let _ = source_merge_histograms_matches_go_cases;
    let _ = source_merge_partition_level_hist_matches_all_go_cases;
    let _ = source_standardize_v2_index_matches_all_go_tables;
    let _ = source_truncate_histogram_keeps_metadata_and_prefix;
}

#[test]
fn lockdown_histogram_sources_match_pinned_sha256() {
    let root = repository_root();
    let locked_sources = [
        ("pkg/statistics/histogram.go", GO_HISTOGRAM_SHA256),
        ("pkg/statistics/histogram_test.go", GO_HISTOGRAM_TEST_SHA256),
        (
            "pkg/statistics/histogram_bench_test.go",
            GO_HISTOGRAM_BENCH_SHA256,
        ),
        ("pkg/statistics/statistics_test.go", GO_ADJACENT_TEST_SHA256),
        (
            "rust/crates/tidb-stats/src/histogram.rs",
            RUST_HISTOGRAM_SHA256,
        ),
        (
            "rust/crates/tidb-stats/src/histogram.inventory.tsv",
            INVENTORY_SHA256,
        ),
        (
            "rust/crates/tidb-stats/src/histogram.evidence.tsv",
            DECLINE_EVIDENCE_SHA256,
        ),
        (
            "rust/crates/tidb-stats/src/histogram.mutation-plan.tsv",
            MUTATION_PLAN_SHA256,
        ),
    ];
    for (path, expected) in locked_sources {
        assert_eq!(sha256_file(&root.join(path)), expected, "SHA drift: {path}");
    }
    assert!(INVENTORY.contains(&format!("# source-sha256\t{GO_HISTOGRAM_SHA256}")));
    assert!(INVENTORY.contains(&format!("# test-sha256\t{GO_HISTOGRAM_TEST_SHA256}")));
    assert!(INVENTORY.contains(&format!("# benchmark-sha256\t{GO_HISTOGRAM_BENCH_SHA256}")));
    assert!(INVENTORY.contains(&format!(
        "# adjacent-test-sha256\t{GO_ADJACENT_TEST_SHA256}"
    )));
    for (path, bytes, lines) in [
        ("pkg/statistics/histogram.go", 73_562, 1_993),
        ("pkg/statistics/histogram_test.go", 21_210, 737),
        ("pkg/statistics/histogram_bench_test.go", 3_193, 108),
        ("pkg/statistics/statistics_test.go", 24_480, 726),
    ] {
        assert_eq!(
            file_size_and_lines(&root.join(path)),
            (bytes, lines),
            "size drift: {path}"
        );
    }
    for ratchet in [
        "# source-bytes\t73562",
        "# source-lines\t1993",
        "# test-bytes\t21210",
        "# test-lines\t737",
        "# benchmark-bytes\t3193",
        "# benchmark-lines\t108",
        "# adjacent-test-bytes\t24480",
        "# adjacent-test-lines\t726",
    ] {
        assert!(
            INVENTORY.contains(ratchet),
            "missing inventory ratchet: {ratchet}"
        );
    }
}

#[test]
fn lockdown_histogram_inventory_has_complete_shape_and_allowed_statuses() {
    let rows = inventory_rows();
    assert_eq!(rows.len(), 668);
    let mut ids = HashSet::new();
    for row in &rows {
        assert_eq!(row.len(), 9, "malformed inventory row: {row:?}");
        assert!(ids.insert(row[0]), "duplicate inventory id: {}", row[0]);
        assert!(row[0].starts_with('O'), "invalid obligation id: {row:?}");
        assert!(!row[2].is_empty(), "missing source path: {row:?}");
        assert!(!row[3].is_empty(), "missing AST anchor: {row:?}");
        assert!(
            row[4].len() == 64 && row[4].bytes().all(|byte| byte.is_ascii_hexdigit()),
            "invalid node hash: {row:?}"
        );
        assert!(!row[5].is_empty(), "missing owner: {row:?}");
        assert!(
            matches!(row[6], "PORTED" | "DECLINED" | "UNREACHABLE"),
            "unsupported status: {row:?}"
        );
        assert!(!row[8].is_empty(), "missing evidence: {row:?}");
        if row[6] == "PORTED" {
            assert_ne!(row[7], "-", "PORTED row lacks a Rust symbol: {row:?}");
        } else {
            assert_eq!(row[7], "-", "non-PORTED row claims a Rust symbol: {row:?}");
        }
    }

    for (category, expected) in [
        ("benchmark", 1),
        ("branch", 344),
        ("closure", 6),
        ("const", 9),
        ("declaration", 7),
        ("field", 37),
        ("function", 84),
        ("loop", 90),
        ("short_circuit", 66),
        ("test", 8),
        ("test_helper", 6),
        ("test_support_const", 3),
        ("test_support_declaration", 3),
        ("test_support_var", 1),
    ] {
        assert_eq!(
            rows.iter().filter(|row| row[1] == category).count(),
            expected,
            "category count: {category}"
        );
    }
    for (status, expected) in [("PORTED", 498), ("DECLINED", 169), ("UNREACHABLE", 1)] {
        assert_eq!(
            rows.iter().filter(|row| row[6] == status).count(),
            expected,
            "status count: {status}"
        );
    }
    for (source, expected) in [
        ("pkg/statistics/histogram.go", 636),
        ("pkg/statistics/histogram_test.go", 22),
        ("pkg/statistics/histogram_bench_test.go", 8),
        ("pkg/statistics/statistics_test.go", 2),
    ] {
        assert_eq!(
            rows.iter().filter(|row| row[2] == source).count(),
            expected,
            "source obligation count: {source}"
        );
    }
    assert!(
        rows.windows(2).all(|pair| {
            (pair[0][2], pair[0][3], pair[0][1]) <= (pair[1][2], pair[1][3], pair[1][1])
        }),
        "inventory is not in deterministic AST order"
    );
    assert!(INVENTORY.contains("# production-obligations\t636"));
    assert!(INVENTORY.contains("# source-owned-test-support-obligations\t32"));
}

#[test]
fn lockdown_every_ported_histogram_symbol_still_exists() {
    let inventory_symbols = inventory_rows()
        .into_iter()
        .filter(|row| row[6] == "PORTED")
        .map(|row| row[7])
        .collect::<HashSet<_>>();
    let anchored_symbols = COMPILE_ANCHORED_SYMBOLS
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    assert!(
        !inventory_symbols.contains("-"),
        "PORTED row lacks a Rust symbol"
    );
    assert_eq!(anchored_symbols.len(), COMPILE_ANCHORED_SYMBOLS.len());
    assert_eq!(inventory_symbols, anchored_symbols);
}

#[test]
fn lockdown_every_ported_histogram_obligation_names_an_existing_boundary_test() {
    let symbol_tests = PORTED_SYMBOL_TESTS
        .iter()
        .copied()
        .collect::<std::collections::HashMap<_, _>>();
    assert_eq!(symbol_tests.len(), PORTED_SYMBOL_TESTS.len());
    assert_eq!(symbol_tests.len(), COMPILE_ANCHORED_SYMBOLS.len());

    let anchored_symbols = COMPILE_ANCHORED_SYMBOLS
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    assert_eq!(
        symbol_tests.keys().copied().collect::<HashSet<_>>(),
        anchored_symbols
    );

    let mut referenced_tests = HashSet::new();
    for row in inventory_rows()
        .into_iter()
        .filter(|row| row[6] == "PORTED")
    {
        let test_identity = symbol_tests
            .get(row[7])
            .unwrap_or_else(|| panic!("PORTED symbol has no boundary test mapping: {row:?}"));
        let expected_evidence = format!("rust-test:{test_identity}");
        assert_eq!(row[8], expected_evidence, "wrong PORTED evidence: {row:?}");
        referenced_tests.insert(*test_identity);
    }

    let mapped_tests = symbol_tests.values().copied().collect::<HashSet<_>>();
    assert_eq!(referenced_tests, mapped_tests);
    for identity in referenced_tests {
        let (test_name, source) = ported_test_source(identity)
            .unwrap_or_else(|| panic!("unknown Rust test module in evidence: {identity}"));
        assert!(
            source_declares_test(source, test_name),
            "evidence names a missing #[test]: {identity}"
        );
    }
}

#[test]
fn lockdown_histogram_mutation_plan_covers_independent_rule_families() {
    let rows = mutation_plan_rows();
    assert_eq!(rows.len(), 21);
    let mut ids = HashSet::new();
    let mut families = HashSet::new();
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row.len(), 7, "malformed mutation plan row: {row:?}");
        assert_eq!(row[0], format!("M{:02}", index + 1));
        assert!(ids.insert(row[0]), "duplicate mutation ID: {}", row[0]);
        assert!(
            families.insert(row[1]),
            "duplicate mutation rule family: {}",
            row[1]
        );
        assert!(!row[2].is_empty(), "missing mutation target path: {row:?}");
        assert!(
            !row[3].is_empty(),
            "missing mutation target symbol: {row:?}"
        );
        assert!(!row[4].is_empty(), "missing mutation operation: {row:?}");
        let (test_name, source) = ported_test_source(row[5])
            .unwrap_or_else(|| panic!("unknown mutation test module: {row:?}"));
        assert!(
            source_declares_test(source, test_name),
            "mutation names a missing #[test]: {row:?}"
        );
        assert!(
            matches!(row[6], "assertion_failure" | "compilation_failure"),
            "unknown mutation outcome: {row:?}"
        );
    }
    assert_eq!(ids.len(), 21);
    assert_eq!(families.len(), 21);
}

#[test]
fn lockdown_declined_histogram_evidence_is_source_backed() {
    let rows = inventory_rows();
    let evidence = decline_evidence_rows();
    assert_eq!(evidence.len(), 11);
    let mut evidence_ids = HashSet::new();
    for row in &evidence {
        assert_eq!(row.len(), 4, "malformed decline evidence: {row:?}");
        assert!(
            evidence_ids.insert(row[0]),
            "duplicate decline evidence: {}",
            row[0]
        );
        assert!(
            row[1].contains("pkg/statistics/"),
            "missing Go source evidence: {row:?}"
        );
        assert!(
            !row[2].is_empty(),
            "missing Rust boundary evidence: {row:?}"
        );
        assert!(!row[3].is_empty(), "missing verification identity: {row:?}");
    }
    let declined = rows
        .iter()
        .filter(|row| row[6] == "DECLINED")
        .collect::<Vec<_>>();
    assert_eq!(declined.len(), 169);
    assert!(declined
        .iter()
        .all(|row| row[7] == "-" && evidence_ids.contains(row[8])));
    let used_evidence = declined.iter().map(|row| row[8]).collect::<HashSet<_>>();
    assert_eq!(used_evidence, evidence_ids);
    assert!(declined
        .iter()
        .any(|row| row[3].starts_with("Histogram.SplitRange/")));
    assert!(declined
        .iter()
        .any(|row| row[3].starts_with("HistogramToProto/")));
    assert!(declined
        .iter()
        .any(|row| row[3] == "type:Histogram/field:0:Tp"));
    assert!(declined
        .iter()
        .any(|row| row[3] == "type:Histogram/field:3:Scalars"));
}

#[test]
fn lockdown_unreachable_histogram_obligation_has_measured_proof() {
    let rows = inventory_rows();
    let unreachable = rows
        .iter()
        .filter(|row| row[6] == "UNREACHABLE")
        .collect::<Vec<_>>();
    assert_eq!(unreachable.len(), 1);
    assert_eq!(unreachable[0][3], "TopNMeta.buildBucket4Merging/closure:1");
    assert_eq!(
        unreachable[0][8],
        "measured_go_oracle_topn_bucket_ndv=0_after_enable"
    );
    assert!(rows.iter().any(|row| {
        row[3].starts_with("TopNMeta.buildBucket4Merging/if:1/")
            && row[6] == "PORTED"
            && row[7] == "BucketForMerging::from_topn"
    }));
}
