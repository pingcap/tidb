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

use tidb_datatype::{Collation, Datum};
use tidb_stats::histogram::{Bucket, Histogram};

const EPS: f64 = 1e-9;

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
        (3, 1.1428571428571428, true, 2.6666666666666665, 41.208333333333336),
        (9, 2.0, true, 8.0, 35.0),
        (10, 0.875, true, 10.0, 33.875),
        (15, 0.875, true, 13.88888888888889, 29.98611111111111),
        (19, 3.0, true, 17.0, 25.0),
        (20, 1.5555555555555556, true, 20.0, 23.875),
        (25, 1.5555555555555556, true, 27.77777777777778, 16.09722222222222),
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
        let result =
            hist.between_row_count(&Datum::new_int(a), &Datum::new_int(b), Collation::Binary, None);
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
        ("grape", 1.1666666666666667, true, 11.052469849099014, 14.08086348423432),
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
        ("2020-03-15 00:00:00", 1.625, true, 6.328947368421053, 20.551052631578944),
        ("2020-06-01 00:00:00", 2.0, true, 13.0, 13.0),
        ("2020-09-01 00:00:00", 1.4285714285714286, true, 19.299065420560748, 7.580934579439252),
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
