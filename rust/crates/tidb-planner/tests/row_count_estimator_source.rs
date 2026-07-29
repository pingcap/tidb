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

// The fixture literals below are Go's printed 17-significant-digit output and
// the case tables are wide on purpose; shortening either would make this file
// harder to check against the generator, which is the only thing it is for.
#![allow(clippy::excessive_precision, clippy::type_complexity)]

//! Go-fixture parity for the statistics-backed row-count estimator.
//!
//! Every number below was printed by a throwaway Go test,
//! `pkg/planner/cardinality/zz_dump_selectivity_test.go`, run as
//! `go test -tags=intest ./pkg/planner/cardinality/ -run
//! TestZZDumpSelectivityFixtures -v`. It built `test.t1` on a mock store
//! (40 distinct values of `a` with `1 + i%3` rows each, plus 20 extra rows at
//! `a = 1`, 15 at `a = 2`, 3 all-NULL rows), ran `ANALYZE TABLE t1 WITH 5
//! TOPN, 4 BUCKETS`, then inserted 10 more rows so the estimator sees
//! `RealtimeCount = 127` against an analyzed 117 with `ModifyCount = 10` --
//! which is what puts the out-of-range and increase-factor paths under test.
//! The histogram/TopN/index-key contents printed by that same run are
//! transcribed here as the estimator's input, so the assertions compare Go's
//! estimate against this port's estimate over byte-identical statistics.
//!
//! The stats-version-1 block is built by hand for the same reason the Go
//! generator builds it by hand: `tidb_analyze_version = 1` is rejected by
//! current TiDB, so the only way to exercise the CMSketch branch of
//! `equalRowCount` is to assemble the column. Its sketch was fed the exact
//! `(value, count)` pairs listed in `V1_CMS_INSERTS`, and `CMSQUERY` in the
//! generator confirms what Go's `QueryValue` returns for each probe.
//!
//! Epsilon: 1e-9 relative. Go printed 17 significant digits; every case here
//! matches to the last printed digit except where f64 addition order differs,
//! which stays inside 1e-12 relative.

use tidb_codec::encode_key;
use tidb_datatype::{Collation, Datum};
use tidb_planner::cardinality::row_count_estimator::{
    equal_row_count_on_column, get_index_row_count_for_stats_v2, get_row_count_by_column_ranges,
    ColumnRange, ColumnStats, EstimatorOptions, IndexRangeDatums, IndexStats,
};
use tidb_planner::selectivity_greedy::{
    combine_selectivity, ConditionKind, SelectivityDefaults, StatsNode, StatsNodeType,
};
use tidb_stats::cmsketch::{encode_integer_datum_value, CmsSketch, TopN};
use tidb_stats::histogram::{Bucket, Histogram};

const REALTIME: i64 = 127;
const MODIFY: i64 = 10;
const EPS: f64 = 1e-9;

fn assert_close(got: f64, want: f64, what: &str) {
    let scale = want.abs().max(1.0);
    assert!(
        (got - want).abs() <= EPS * scale,
        "{what}: got {got:.17}, want {want:.17}"
    );
}

fn int_bucket(count: i64, repeat: i64, lower: i64, upper: i64) -> Bucket {
    Bucket {
        count,
        repeat,
        ndv: 0,
        lower_bound: Datum::Int(lower),
        upper_bound: Datum::Int(upper),
    }
}

fn bytes_bucket(count: i64, repeat: i64, lower: &str, upper: &str) -> Bucket {
    Bucket {
        count,
        repeat,
        ndv: 0,
        lower_bound: Datum::Bytes(unhex(lower)),
        upper_bound: Datum::Bytes(unhex(upper)),
    }
}

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len() / 2)
        .map(|i| u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).expect("valid hex"))
        .collect()
}

fn key_of(values: &[Datum]) -> Vec<u8> {
    encode_key(values).expect("integer datums always encode")
}

fn topn_of(entries: &[(&[Datum], u64, &str)]) -> TopN {
    let mut topn = TopN::new(entries.len());
    for (values, count, expected_hex) in entries {
        let encoded = key_of(values);
        assert_eq!(
            encoded,
            unhex(expected_hex),
            "TopN key encoding must match the Go fixture"
        );
        topn.append(&encoded, *count);
    }
    topn.sort();
    topn
}

/// `test.t1.a`: `ndv = 40`, `null_count = 3`, 4 buckets, 5 TopN entries.
fn column_a() -> ColumnStats {
    ColumnStats {
        histogram: Histogram {
            id: 1,
            ndv: 40,
            null_count: 3,
            buckets: vec![
                int_bucket(17, 1, 0, 15),
                int_bucket(34, 3, 16, 23),
                int_bucket(52, 3, 24, 32),
                int_bucket(65, 1, 33, 39),
            ],
            ..Histogram::default()
        },
        topn: Some(topn_of(&[
            (&[Datum::Int(1)], 22, "038000000000000001"),
            (&[Datum::Int(2)], 18, "038000000000000002"),
            (&[Datum::Int(5)], 3, "038000000000000005"),
            (&[Datum::Int(8)], 3, "038000000000000008"),
            (&[Datum::Int(11)], 3, "03800000000000000b"),
        ])),
        cms: None,
        stats_ver: 2,
        unsigned: false,
    }
}

/// `test.t1.b`, whose values are `2 * a`.
fn column_b() -> ColumnStats {
    ColumnStats {
        histogram: Histogram {
            id: 2,
            ndv: 40,
            null_count: 3,
            buckets: vec![
                int_bucket(17, 1, 0, 30),
                int_bucket(34, 3, 32, 46),
                int_bucket(52, 3, 48, 64),
                int_bucket(65, 1, 66, 78),
            ],
            ..Histogram::default()
        },
        topn: Some(topn_of(&[
            (&[Datum::Int(2)], 22, "038000000000000002"),
            (&[Datum::Int(4)], 18, "038000000000000004"),
            (&[Datum::Int(10)], 3, "03800000000000000a"),
            (&[Datum::Int(16)], 3, "038000000000000010"),
            (&[Datum::Int(22)], 3, "038000000000000016"),
        ])),
        cms: None,
        stats_ver: 2,
        unsigned: false,
    }
}

/// `test.t1.idx(a, b)`: `ndv = 41`, no NULL count, bounds are index keys.
fn index_ab() -> IndexStats {
    IndexStats {
        histogram: Histogram {
            id: 1,
            ndv: 41,
            null_count: 0,
            buckets: vec![
                bytes_bucket(
                    19,
                    3,
                    "038000000000000000038000000000000000",
                    "03800000000000000e03800000000000001c",
                ),
                bytes_bucket(
                    37,
                    3,
                    "03800000000000000f03800000000000001e",
                    "03800000000000001703800000000000002e",
                ),
                bytes_bucket(
                    55,
                    3,
                    "038000000000000018038000000000000030",
                    "038000000000000020038000000000000040",
                ),
                bytes_bucket(
                    68,
                    1,
                    "038000000000000021038000000000000042",
                    "03800000000000002703800000000000004e",
                ),
            ],
            ..Histogram::default()
        },
        topn: Some(topn_of(&[
            (&[Datum::Null, Datum::Null], 3, "0000"),
            (
                &[Datum::Int(1), Datum::Int(2)],
                22,
                "038000000000000001038000000000000002",
            ),
            (
                &[Datum::Int(2), Datum::Int(4)],
                18,
                "038000000000000002038000000000000004",
            ),
            (
                &[Datum::Int(5), Datum::Int(10)],
                3,
                "03800000000000000503800000000000000a",
            ),
            (
                &[Datum::Int(8), Datum::Int(16)],
                3,
                "038000000000000008038000000000000010",
            ),
        ])),
        cms: None,
        stats_ver: 2,
        num_columns: 2,
        unique: false,
    }
}

fn point(value: i64) -> ColumnRange {
    ColumnRange::point(Datum::Int(value))
}

fn range_of(low: Datum, high: Datum, low_exclude: bool, high_exclude: bool) -> ColumnRange {
    ColumnRange::new(low, high, low_exclude, high_exclude)
}

fn column_estimate(column: Option<&ColumnStats>, ranges: &[ColumnRange], pk: bool) -> (f64, f64, f64) {
    let result = get_row_count_by_column_ranges(
        column,
        ranges,
        Collation::Binary,
        REALTIME,
        MODIFY,
        pk,
        EstimatorOptions::default(),
    );
    (result.est, result.min_est, result.max_est)
}

fn check(name: &str, got: (f64, f64, f64), want: (f64, f64, f64)) {
    assert_close(got.0, want.0, &format!("{name}.est"));
    assert_close(got.1, want.1, &format!("{name}.min"));
    assert_close(got.2, want.2, &format!("{name}.max"));
}

#[test]
fn source_column_point_estimates_follow_topn_then_histogram_then_uniform() {
    let a = column_a();
    let b = column_b();
    let cases: &[(&str, &ColumnStats, i64, bool, (f64, f64, f64))] = &[
        // TopN hits are exact counts, scaled by the increase factor 127/117.
        (
            "a_eq_1_topn",
            &a,
            1,
            false,
            (23.880341880341877, 23.880341880341877, 23.880341880341877),
        ),
        (
            "a_eq_2_topn",
            &a,
            2,
            false,
            (19.538461538461537, 19.538461538461537, 19.538461538461537),
        ),
        // The last bucket's upper bound: its repeat count, not underrepresented.
        (
            "a_eq_39",
            &a,
            39,
            false,
            (1.0854700854700854, 1.0854700854700854, 1.0854700854700854),
        ),
        // Inside a bucket, absent, and out of range all take uniform.
        (
            "a_eq_20",
            &a,
            20,
            false,
            (2.0158730158730158, 2.0158730158730158, 2.0158730158730158),
        ),
        (
            "a_eq_21",
            &a,
            21,
            false,
            (2.0158730158730158, 2.0158730158730158, 2.0158730158730158),
        ),
        (
            "a_eq_0",
            &a,
            0,
            false,
            (2.0158730158730158, 2.0158730158730158, 2.0158730158730158),
        ),
        (
            "a_eq_out_high",
            &a,
            500,
            false,
            (2.0158730158730158, 2.0158730158730158, 2.0158730158730158),
        ),
        (
            "a_eq_out_low",
            &a,
            -7,
            false,
            (2.0158730158730158, 2.0158730158730158, 2.0158730158730158),
        ),
        // A handle point is at most one row, whatever the statistics say.
        ("a_eq_pk", &a, 20, true, (1.0, 1.0, 1.0)),
        (
            "b_eq_40",
            &b,
            40,
            false,
            (2.0158730158730158, 2.0158730158730158, 2.0158730158730158),
        ),
        (
            "b_eq_out_high",
            &b,
            9999,
            false,
            (2.0158730158730158, 2.0158730158730158, 2.0158730158730158),
        ),
    ];
    for (name, column, value, pk, want) in cases {
        check(name, column_estimate(Some(column), &[point(*value)], *pk), *want);
    }
}

#[test]
fn source_column_null_and_range_estimates() {
    let a = column_a();
    let b = column_b();
    let cases: &[(&str, &ColumnStats, ColumnRange, (f64, f64, f64))] = &[
        (
            "a_eq_null",
            &a,
            range_of(Datum::Null, Datum::Null, false, false),
            (3.2564102564102564, 3.2564102564102564, 3.2564102564102564),
        ),
        (
            "a_null_to_10",
            &a,
            range_of(Datum::Null, Datum::Int(10), false, false),
            (66.782254782254768, 16.85063085063085, 66.782254782254768),
        ),
        (
            "a_closed_5_15",
            &a,
            range_of(Datum::Int(5), Datum::Int(15), false, false),
            (22.433048433048434, 12.663817663817664, 22.433048433048434),
        ),
        (
            "a_open_5_15",
            &a,
            range_of(Datum::Int(5), Datum::Int(15), true, true),
            (18.09116809116809, 8.3219373219373232, 18.09116809116809),
        ),
        (
            "a_lowex_5_15",
            &a,
            range_of(Datum::Int(5), Datum::Int(15), true, false),
            (19.176638176638175, 9.4074074074074083, 19.176638176638175),
        ),
        (
            "a_highex_5_15",
            &a,
            range_of(Datum::Int(5), Datum::Int(15), false, true),
            (21.347578347578349, 11.578347578347579, 21.347578347578349),
        ),
        (
            "a_ge_30",
            &a,
            range_of(Datum::Int(30), Datum::MaxValue, false, false),
            (36.188034188034187, 22.708034188034187, 80.43803418803418),
        ),
        (
            "a_gt_30",
            &a,
            range_of(Datum::Int(30), Datum::MaxValue, true, false),
            (34.172161172161168, 20.692161172161171, 78.422161172161168),
        ),
        (
            "a_lt_10",
            &a,
            range_of(Datum::MinNotNull, Datum::Int(10), false, true),
            (76.259971509971507, 12.848347578347576, 120.50997150997151),
        ),
        (
            "a_le_10",
            &a,
            range_of(Datum::MinNotNull, Datum::Int(10), false, false),
            (78.275844525844519, 14.864220594220592, 122.52584452584452),
        ),
        (
            "a_full",
            &a,
            range_of(Datum::MinNotNull, Datum::MaxValue, false, false),
            (127.0, 71.825555555555553, 127.0),
        ),
        (
            "a_out_high_range",
            &a,
            range_of(Datum::Int(200), Datum::Int(400), false, false),
            (3.2858730158730158, 3.2858730158730158, 61.015873015873012),
        ),
        (
            "a_out_low_range",
            &a,
            range_of(Datum::Int(-50), Datum::Int(-10), false, false),
            (10.17152719075796, 3.2858730158730158, 34.638489715412788),
        ),
        (
            "a_spanning_high",
            &a,
            range_of(Datum::Int(35), Datum::Int(300), false, false),
            (26.535103785103786, 13.055103785103784, 75.126984126984127),
        ),
        (
            "a_narrow_2_3",
            &a,
            range_of(Datum::Int(2), Datum::Int(3), false, false),
            (25.027838827838824, 5.4893772893772885, 25.027838827838824),
        ),
        (
            "b_range_10_50",
            &b,
            range_of(Datum::Int(10), Datum::Int(50), false, false),
            (44.93716931216931, 35.167938542938543, 44.93716931216931),
        ),
    ];
    for (name, column, range, want) in cases {
        check(
            name,
            column_estimate(Some(column), std::slice::from_ref(range), false),
            *want,
        );
    }
}

#[test]
fn source_column_multi_range_estimates() {
    let a = column_a();
    check(
        "a_in_1_2_3",
        column_estimate(Some(&a), &[point(1), point(2), point(3)], false),
        (45.434676434676426, 45.434676434676426, 45.434676434676426),
    );
    check(
        "a_in_with_missing",
        column_estimate(Some(&a), &[point(1), point(500), point(21)], false),
        (27.912087912087909, 27.912087912087909, 27.912087912087909),
    );
    check(
        "a_two_ranges",
        column_estimate(
            Some(&a),
            &[
                range_of(Datum::Int(1), Datum::Int(5), false, false),
                range_of(Datum::Int(30), Datum::Int(39), false, false),
            ],
            false,
        ),
        (72.744586894586888, 29.325783475783474, 72.744586894586888),
    );
}

#[test]
fn source_pseudo_estimates_when_the_column_has_no_statistics() {
    let cases: &[(&str, Vec<ColumnRange>, bool, f64)] = &[
        ("pseudo_eq", vec![point(3)], false, 0.127),
        (
            "pseudo_range",
            vec![range_of(Datum::Int(3), Datum::Int(20), false, false)],
            false,
            3.1749999999999998,
        ),
        (
            "pseudo_lt",
            vec![range_of(Datum::MinNotNull, Datum::Int(20), false, true)],
            false,
            42.206333333333333,
        ),
        (
            "pseudo_ge",
            vec![range_of(Datum::Int(20), Datum::MaxValue, false, false)],
            false,
            42.333333333333336,
        ),
        ("pseudo_in", vec![point(3), point(9)], false, 0.254),
        ("pseudo_pk_eq", vec![point(3)], true, 1.0),
        (
            "pseudo_pk_range",
            vec![range_of(Datum::Int(3), Datum::Int(20), false, false)],
            true,
            3.1749999999999998,
        ),
    ];
    for (name, ranges, pk, want) in cases {
        let (est, min, max) = column_estimate(None, ranges, *pk);
        assert_close(est, *want, name);
        assert_close(min, *want, &format!("{name}.min"));
        assert_close(max, *want, &format!("{name}.max"));
    }
}

fn index_range(low: &[i64], high: &[i64], low_exclude: bool, high_exclude: bool) -> IndexRangeDatums {
    IndexRangeDatums {
        low: low.iter().map(|v| Datum::Int(*v)).collect(),
        high: high.iter().map(|v| Datum::Int(*v)).collect(),
        low_exclude,
        high_exclude,
    }
}

#[test]
fn source_index_range_estimates() {
    let index = index_ab();
    // The analyzed `HistColl` the generator read has an empty
    // `Idx2ColUniqueIDs` (it printed `IDX2COL map[]`), so exponential backoff
    // finds no column for any index position and every range falls back to
    // the index histogram. `source_index_exp_backoff_estimates` covers the
    // populated-map case.
    let columns = vec![None, None];
    let cases: &[(&str, Vec<IndexRangeDatums>, (f64, f64, f64))] = &[
        (
            "idx_point_1_2",
            vec![index_range(&[1, 2], &[1, 2], false, false)],
            (23.880341880341877, 23.880341880341877, 23.880341880341877),
        ),
        (
            "idx_point_20_40",
            vec![index_range(&[20, 40], &[20, 40], false, false)],
            (2.0503323836657166, 2.0503323836657166, 2.0503323836657166),
        ),
        (
            "idx_point_absent",
            vec![index_range(&[20, 41], &[20, 41], false, false)],
            (2.0503323836657166, 2.0503323836657166, 2.0503323836657166),
        ),
        (
            "idx_prefix_20",
            vec![index_range(&[20], &[20], false, false)],
            (2.0352564102564101, 2.0352564102564101, 16.282051282051281),
        ),
        (
            "idx_prefix_1",
            vec![index_range(&[1], &[1], false, false)],
            (26.344455891330888, 26.344455891330888, 41.247863247863243),
        ),
        (
            "idx_prefix_range",
            vec![index_range(&[5], &[15], false, false)],
            (22.958457043173837, 22.958457043173837, 22.958457043173837),
        ),
        (
            "idx_composite_range",
            vec![index_range(&[20, 40], &[20, 80], false, false)],
            (3.6005836981446735, 3.6005836981446735, 3.6005836981446735),
        ),
        (
            "idx_out_high",
            vec![index_range(&[500], &[600], false, false)],
            (1.27, 1.27, 59.0),
        ),
        (
            "idx_two_points",
            vec![
                index_range(&[1, 2], &[1, 2], false, false),
                index_range(&[2, 4], &[2, 4], false, false),
            ],
            (43.418803418803414, 43.418803418803414, 43.418803418803414),
        ),
        (
            "idx_open_prefix",
            vec![index_range(&[5], &[15], true, true)],
            (16.454078907203904, 16.454078907203904, 16.454078907203904),
        ),
    ];
    for (name, ranges, want) in cases {
        let result = get_index_row_count_for_stats_v2(
            &index,
            &columns,
            ranges,
            REALTIME,
            MODIFY,
            EstimatorOptions::default(),
        );
        check(name, (result.est, result.min_est, result.max_est), *want);
    }
}


#[test]
fn source_index_exp_backoff_estimates() {
    // Same index and ranges, but with the index-to-column map populated, so
    // `expBackoffEstimation` runs against the two column histograms. Numbers
    // from the generator's CASEBO lines.
    let index = index_ab();
    let a = column_a();
    let b = column_b();
    let columns = vec![Some(&a), Some(&b)];
    let cases: &[(&str, Vec<IndexRangeDatums>, (f64, f64, f64))] = &[
        (
            "bo_point_1_2",
            vec![index_range(&[1, 2], &[1, 2], false, false)],
            (23.880341880341877, 23.880341880341877, 23.880341880341877),
        ),
        (
            "bo_prefix_20",
            vec![index_range(&[20], &[20], false, false)],
            (2.0158730158730154, 2.0158730158730154, 2.0158730158730154),
        ),
        (
            "bo_prefix_1",
            vec![index_range(&[1], &[1], false, false)],
            (23.880341880341874, 23.880341880341874, 23.880341880341874),
        ),
        (
            "bo_prefix_range",
            vec![index_range(&[5], &[15], false, false)],
            (22.958457043173837, 22.958457043173837, 22.958457043173837),
        ),
        (
            "bo_composite_range",
            vec![index_range(&[20, 40], &[20, 80], false, false)],
            (2.0158730158730154, 2.0158730158730154, 3.0975609756097562),
        ),
        (
            "bo_composite_range2",
            vec![index_range(&[2, 4], &[2, 30], false, false)],
            (11.687769168869503, 6.9915406530791131, 19.538461538461533),
        ),
        (
            "bo_out_high",
            vec![index_range(&[500], &[600], false, false)],
            (1.27, 1.27, 59.0),
        ),
        (
            "bo_open_prefix",
            vec![index_range(&[5], &[15], true, true)],
            (16.454078907203904, 16.454078907203904, 16.454078907203904),
        ),
    ];
    for (name, ranges, want) in cases {
        let result = get_index_row_count_for_stats_v2(
            &index,
            &columns,
            ranges,
            REALTIME,
            MODIFY,
            EstimatorOptions::default(),
        );
        check(name, (result.est, result.min_est, result.max_est), *want);
    }
}

/// The `(value, count)` pairs the Go generator fed its version-1 CMSketch.
const V1_CMS_INSERTS: &[(i64, u64)] = &[
    (0, 1),
    (1, 22),
    (2, 18),
    (3, 1),
    (4, 2),
    (5, 3),
    (6, 1),
    (7, 2),
    (8, 3),
    (9, 1),
    (10, 2),
    (11, 3),
    (12, 1),
    (13, 2),
    (14, 3),
    (15, 1),
    (16, 2),
    (17, 3),
    (18, 1),
    (19, 2),
    (20, 3),
    (21, 1),
    (22, 2),
    (23, 3),
    (24, 1),
    (25, 2),
    (26, 3),
    (27, 1),
    (28, 2),
    (29, 3),
    (30, 1),
    (31, 2),
    (32, 3),
    (33, 1),
    (34, 2),
    (35, 3),
    (36, 1),
    (37, 2),
    (38, 3),
    (39, 1),
];

fn column_v1() -> ColumnStats {
    let mut cms = CmsSketch::new(5, 2048);
    for (value, count) in V1_CMS_INSERTS {
        let encoded =
            encode_integer_datum_value(&Datum::Int(*value)).expect("integer datums always encode");
        cms.insert_bytes_by_count(&encoded, *count);
    }
    ColumnStats {
        histogram: column_a().histogram,
        topn: None,
        cms: Some(cms),
        stats_ver: 1,
        unsigned: false,
    }
}

#[test]
fn source_v1_cmsketch_queries_match_go() {
    let column = column_v1();
    let cms = column.cms.as_ref().expect("version-1 column has a sketch");
    // Printed by the generator's CMSQUERY lines.
    for (value, want) in [(0_i64, 1_u64), (1, 22), (2, 18), (20, 3), (39, 1), (500, 0), (-7, 0)] {
        let got = cms
            .query_integer_datum(None, &Datum::Int(value))
            .expect("integer datums always encode");
        assert_eq!(got, want, "CMSketch query for {value}");
    }
}

#[test]
fn source_v1_column_estimates_use_the_cmsketch_before_the_histogram() {
    let column = column_v1();
    let cases: &[(&str, Vec<ColumnRange>, (f64, f64, f64))] = &[
        (
            "v1_eq_1_cms",
            vec![point(1)],
            (41.088235294117645, 41.088235294117645, 41.088235294117645),
        ),
        (
            "v1_eq_20_cms",
            vec![point(20)],
            (5.6029411764705888, 5.6029411764705888, 5.6029411764705888),
        ),
        ("v1_eq_out_high", vec![point(500)], (1.27, 1.27, 1.27)),
        ("v1_eq_out_low", vec![point(-7)], (1.27, 1.27, 1.27)),
        (
            "v1_eq_null",
            vec![range_of(Datum::Null, Datum::Null, false, false)],
            (5.6029411764705888, 5.6029411764705888, 5.6029411764705888),
        ),
        (
            "v1_range_5_15",
            vec![range_of(Datum::Int(5), Datum::Int(15), false, false)],
            (21.789215686274513, 21.789215686274513, 31.75),
        ),
        (
            "v1_range_open",
            vec![range_of(Datum::Int(5), Datum::Int(15), true, true)],
            (33.617647058823529, 33.617647058823529, 33.617647058823529),
        ),
        (
            "v1_small_range",
            vec![range_of(Datum::Int(2), Datum::Int(4), false, false)],
            (39.220588235294116, 39.220588235294116, 39.220588235294116),
        ),
        (
            "v1_lt_10",
            vec![range_of(Datum::MinNotNull, Datum::Int(10), false, true)],
            (34.671568627450981, 21.19156862745098, 88.882352941176464),
        ),
        (
            "v1_ge_30",
            vec![range_of(Datum::Int(30), Datum::MaxValue, false, false)],
            (51.636029411764703, 38.156029411764706, 95.886029411764696),
        ),
        (
            "v1_out_high_range",
            vec![range_of(Datum::Int(200), Datum::Int(400), false, false)],
            (2.54, 2.54, 60.270000000000003),
        ),
    ];
    for (name, ranges, want) in cases {
        check(name, column_estimate(Some(&column), ranges, false), *want);
    }
}

#[test]
fn equal_row_count_prefers_topn_over_every_later_source() {
    // The ordering itself, stated as a test: the same value is present in
    // TopN *and* inside a bucket; the TopN count must win untouched.
    let column = column_a();
    let encoded = key_of(&[Datum::Int(1)]);
    let result = equal_row_count_on_column(
        &column,
        &Datum::Int(1),
        &encoded,
        Collation::Binary,
        REALTIME,
        MODIFY,
        EstimatorOptions::default(),
    );
    assert_close(result.est, 22.0, "TopN count is exact");
}

#[test]
fn selectivity_combines_greedy_cover_and_leftover_defaults() {
    // Two column nodes, one bit each, both fully covered: the result is the
    // plain product of their selectivities.
    let mut nodes = vec![
        StatsNode {
            selectivity: 0.25,
            ..StatsNode::new(StatsNodeType::Column, 1, 0b01, 1)
        },
        StatsNode {
            selectivity: 0.5,
            ..StatsNode::new(StatsNodeType::Column, 2, 0b10, 1)
        },
    ];
    let conditions = [ConditionKind::Other, ConditionKind::Other];
    let combined = combine_selectivity(
        &mut nodes,
        &conditions,
        1.0,
        REALTIME,
        SelectivityDefaults::default(),
    );
    assert_close(combined, 0.125, "product of covered nodes");

    // An index node covering both bits outranks the two column nodes.
    let mut nodes = vec![
        StatsNode {
            selectivity: 0.25,
            ..StatsNode::new(StatsNodeType::Column, 1, 0b01, 1)
        },
        StatsNode {
            selectivity: 0.5,
            ..StatsNode::new(StatsNodeType::Column, 2, 0b10, 1)
        },
        StatsNode {
            selectivity: 0.2,
            ..StatsNode::new(StatsNodeType::Index, 3, 0b11, 2)
        },
    ];
    let combined = combine_selectivity(
        &mut nodes,
        &conditions,
        1.0,
        REALTIME,
        SelectivityDefaults::default(),
    );
    assert_close(combined, 0.2, "index node covers both conditions");

    // A third, uncovered condition charges the default factor once.
    let mut nodes = vec![StatsNode {
        selectivity: 0.2,
        ..StatsNode::new(StatsNodeType::Index, 3, 0b11, 2)
    }];
    let conditions = [
        ConditionKind::Other,
        ConditionKind::Other,
        ConditionKind::Other,
    ];
    let combined = combine_selectivity(
        &mut nodes,
        &conditions,
        1.0,
        REALTIME,
        SelectivityDefaults::default(),
    );
    assert_close(combined, 0.2 * 0.8, "leftover condition takes 0.8");

    // A constant-false condition zeroes the result, and the floor of one row
    // still applies.
    let mut nodes = vec![StatsNode {
        selectivity: 0.2,
        ..StatsNode::new(StatsNodeType::Index, 3, 0b11, 2)
    }];
    let conditions = [
        ConditionKind::Other,
        ConditionKind::Other,
        ConditionKind::ConstantFalse,
    ];
    let combined = combine_selectivity(
        &mut nodes,
        &conditions,
        1.0,
        REALTIME,
        SelectivityDefaults::default(),
    );
    assert_close(combined, 1.0 / REALTIME as f64, "one-row floor");
}
