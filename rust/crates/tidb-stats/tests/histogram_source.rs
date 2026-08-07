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
use tidb_stats::histogram::{
    deep_slice, merge_histograms, merge_partition_histograms, Bucket, Histogram,
    HistogramMergeError, PartitionMergeOptions, TopNMergeEntry,
};

const EPS: f64 = 1e-9;
const INVENTORY: &str = include_str!("../src/histogram.inventory.tsv");
const GO_HISTOGRAM_SHA256: &str =
    "1233e0a3430067400eaee5d562772cc83541fce8ae8b3e4579895a574c8c1024";
const GO_HISTOGRAM_TEST_SHA256: &str =
    "8adb0d249a37ffa08c859ea1709426cfc0e98c4fc5a7ff689726fce0a1904a7a";
const GO_ADJACENT_TEST_SHA256: &str =
    "2252313be49f161986afe7a94c379ca875bf79930b4f6c52a0b5e3cd9ffafe35";
const RUST_HISTOGRAM_SHA256: &str =
    "58f3cc4b6310825cd1f27950c23ffa67a09d6e5983f16a534405d782fdcb7ba8";
const INVENTORY_SHA256: &str = "95c7cdd2d211a9e5cdc31c1506468e25328f36b1bc1e9bbce70c5661ac5989dd";

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
        .filter(|line| !line.is_empty() && !line.starts_with('#') && !line.starts_with("id\t"))
        .map(|line| line.split('\t').collect())
        .collect()
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
    assert_eq!(hist.len(), 4);
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
fn lockdown_histogram_sources_match_pinned_sha256() {
    let root = repository_root();
    let locked_sources = [
        ("pkg/statistics/histogram.go", GO_HISTOGRAM_SHA256),
        ("pkg/statistics/histogram_test.go", GO_HISTOGRAM_TEST_SHA256),
        ("pkg/statistics/statistics_test.go", GO_ADJACENT_TEST_SHA256),
        (
            "rust/crates/tidb-stats/src/histogram.rs",
            RUST_HISTOGRAM_SHA256,
        ),
        (
            "rust/crates/tidb-stats/src/histogram.inventory.tsv",
            INVENTORY_SHA256,
        ),
    ];
    for (path, expected) in locked_sources {
        assert_eq!(sha256_file(&root.join(path)), expected, "SHA drift: {path}");
    }
    assert!(INVENTORY.contains(&format!("# source-sha256\t{GO_HISTOGRAM_SHA256}")));
    assert!(INVENTORY.contains(&format!("# test-sha256\t{GO_HISTOGRAM_TEST_SHA256}")));
    assert!(INVENTORY.contains(&format!(
        "# adjacent-test-sha256\t{GO_ADJACENT_TEST_SHA256}"
    )));
}

#[test]
fn lockdown_histogram_inventory_has_complete_shape_and_allowed_statuses() {
    let rows = inventory_rows();
    assert_eq!(rows.len(), 329);
    let mut ids = HashSet::new();
    for row in &rows {
        assert_eq!(row.len(), 6, "malformed inventory row: {row:?}");
        assert!(ids.insert(row[0]), "duplicate inventory id: {}", row[0]);
        assert!(
            matches!(row[3], "PORTED" | "DECLINED" | "UNREACHABLE"),
            "unsupported status: {row:?}"
        );
        assert!(!row[5].is_empty(), "missing evidence: {row:?}");
    }

    for (category, expected) in [
        ("declaration", 19),
        ("function", 84),
        ("branch", 173),
        ("loop", 45),
        ("test", 8),
    ] {
        assert_eq!(
            rows.iter().filter(|row| row[1] == category).count(),
            expected,
            "category count: {category}"
        );
    }
    for (status, expected) in [("PORTED", 242), ("DECLINED", 86), ("UNREACHABLE", 1)] {
        assert_eq!(
            rows.iter().filter(|row| row[3] == status).count(),
            expected,
            "status count: {status}"
        );
    }
}

fn ported_symbol_is_defined(symbol: &str) -> bool {
    let sources = [
        include_str!("../src/histogram.rs"),
        include_str!("../src/row_estimate.rs"),
        include_str!("../src/average_count.rs"),
        include_str!("../src/overlap_geometry.rs"),
        include_str!("../src/stats_version.rs"),
        include_str!("../src/status.rs"),
        include_str!("histogram_source.rs"),
    ];
    if let Some(owner) = symbol.strip_suffix("::clone") {
        return sources
            .iter()
            .any(|source| source.contains(&format!("struct {owner}")) && source.contains("Clone"));
    }
    let leaf = symbol.rsplit("::").next().expect("non-empty Rust symbol");
    let definitions = [
        format!("fn {leaf}("),
        format!("fn {leaf}<"),
        format!("struct {leaf}"),
        format!("enum {leaf}"),
        format!("const {leaf}:"),
    ];
    sources.iter().any(|source| {
        definitions
            .iter()
            .any(|definition| source.contains(definition))
    })
}

#[test]
fn lockdown_every_ported_histogram_symbol_still_exists() {
    for row in inventory_rows()
        .into_iter()
        .filter(|row| row[3] == "PORTED")
    {
        assert_ne!(row[4], "-", "PORTED row lacks a Rust symbol: {row:?}");
        assert!(
            ported_symbol_is_defined(row[4]),
            "PORTED symbol disappeared: {} ({})",
            row[4],
            row[2]
        );
    }
}

#[test]
fn lockdown_declined_and_unreachable_histogram_obligations_have_evidence() {
    let rows = inventory_rows();
    let declined = rows
        .iter()
        .filter(|row| row[3] == "DECLINED")
        .collect::<Vec<_>>();
    assert_eq!(declined.len(), 86);
    assert!(declined.iter().all(|row| {
        row[4] == "-"
            && row[5] == "dependency_or_runtime_contract_outside_dependency_closed_tidb_stats_owner"
    }));
    assert!(declined
        .iter()
        .any(|row| row[2].starts_with("Histogram.SplitRange:")));
    assert!(declined
        .iter()
        .any(|row| row[2].starts_with("HistogramToProto:")));

    let unreachable = rows
        .iter()
        .filter(|row| row[3] == "UNREACHABLE")
        .collect::<Vec<_>>();
    assert_eq!(unreachable.len(), 1);
    assert_eq!(
        unreachable[0][2],
        "TopNMeta.buildBucket4Merging:1604 failpoint closure enableTopNNDV"
    );
    assert_eq!(
        unreachable[0][5],
        "measured_go_oracle_topn_bucket_ndv=0_after_enable"
    );
    assert!(rows.iter().any(|row| {
        row[2].starts_with("TopNMeta.buildBucket4Merging:1601 if analyzeVer <= Version2")
            && row[3] == "PORTED"
            && row[4] == "BucketForMerging::from_topn"
    }));
}
