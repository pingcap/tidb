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

//! Source-backed tests for the exact-distinct half of
//! `pkg/executor/aggfuncs/func_count_distinct.go`.
//!
//! Go coverage for that half lives in `func_count_test.go` and
//! `func_distinct_agg_test.go`. `TestWriteTime` is ported byte for byte. The
//! rest of the Go coverage runs the whole `AggFunc` harness --
//! `buildAggTester`/`testAggFunc`/`testMultiArgsAggFunc`/`testAggMemFunc`/
//! `testParallelDistinctAggCases` -- over built expressions, `chunk.Chunk`s
//! and `mock.NewContext()`, none of which this tier owns; the behavior those
//! harnesses pin (per-type distinct cardinality, partial merge, the group-key
//! encoding) is asserted here directly against the ported states, using the
//! same 0..5 generated-value shape `buildAggTester` uses.

use tidb_datatype::{
    BinaryJSON, Collation, CoreTime, Datum, Decimal, MySqlDuration, Time, TimeType,
};
use tidb_exec::aggregate::runtime::count::{
    append_duration, append_float64, append_int64, append_time, eval_and_encode, write_time,
    CountDistinctDecimalState, CountDistinctDurationState, CountDistinctIntState,
    CountDistinctRealState, CountDistinctStringState, CountWithDistinctState,
};

fn date(year: u16, month: u8, day: u8) -> Time {
    Time::new(
        CoreTime::from_date(year, month, day, 0, 0, 0, 0),
        TimeType::Date,
        0,
    )
    .unwrap()
}

/// Go `func_count_test.go:159` `TestWriteTime`.
#[test]
fn write_time_fills_every_byte() {
    // Go: types.ParseDate(..., "2020-11-11").
    let value = date(2020, 11, 11);

    let mut buf = [255u8; 16];
    write_time(&mut buf, value);
    for byte in buf {
        assert_ne!(byte, 255);
    }
}

/// The exact bytes Go's `WriteTime` lays down, so the group key itself is
/// pinned and not just the "no byte left untouched" property `TestWriteTime`
/// checks.
#[test]
fn write_time_layout_matches_source() {
    let mut buf = [255u8; 16];
    write_time(&mut buf, date(2020, 11, 11));
    assert_eq!(
        buf,
        [
            // BigEndian year 2020.
            0x07, 0xE4, // month, day, hour, minute, second.
            11, 11, 0, 0, 0, // buf[7] is explicitly zeroed.
            0, // BigEndian microsecond.
            0, 0, 0, 0, // mysql.TypeDate, fsp.
            10, 0, // buf[14], buf[15] are explicitly zeroed.
            0, 0,
        ]
    );

    // A DATETIME with a microsecond exercises the remaining fields.
    let stamp = Time::new(
        CoreTime::from_date(2020, 11, 11, 1, 2, 3, 123_456),
        TimeType::DateTime,
        6,
    )
    .unwrap();
    let mut buf = [255u8; 16];
    write_time(&mut buf, stamp);
    assert_eq!(
        buf,
        [
            0x07, 0xE4, 11, 11, 1, 2, 3, 0, 0x00, 0x01, 0xE2, 0x40, 12, 6, 0, 0,
        ]
    );
}

/// `TestCount`'s per-type DISTINCT arm and `TestParallelDistinctCount`'s
/// `mysql.TypeLonglong` case: five generated values, five distinct.
#[test]
fn typed_distinct_sets_count_generated_values() {
    let mut ints = CountDistinctIntState::new();
    ints.update(&[Some(0), Some(1), Some(2), Some(3), Some(4), None]);
    assert_eq!(ints.result(), 5);

    let mut reals = CountDistinctRealState::new();
    reals.update(&[Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0), None]);
    assert_eq!(reals.result(), 5);

    let mut decimals = CountDistinctDecimalState::new();
    decimals
        .update(&(0..5).map(|i| Some(Decimal::from_int(i))).collect::<Vec<_>>())
        .unwrap();
    assert_eq!(decimals.result(), 5);

    let mut durations = CountDistinctDurationState::new();
    durations.update(
        &(0..5)
            .map(|i| Some(MySqlDuration::from_raw_parts(i, 0)))
            .collect::<Vec<_>>(),
    );
    assert_eq!(durations.result(), 5);

    let mut strings = CountDistinctStringState::new(Collation::Utf8Mb4Bin);
    let owned: Vec<Vec<u8>> = (0..5).map(|i| format!("{i}").into_bytes()).collect();
    strings.update(&owned.iter().map(|v| Some(v.as_slice())).collect::<Vec<_>>());
    assert_eq!(strings.result(), 5);
}

/// Repeats and NULLs collapse exactly as Go's `Exist`/`isNull` guards do.
#[test]
fn repeats_and_nulls_do_not_grow_the_sets() {
    let mut ints = CountDistinctIntState::new();
    ints.update(&[Some(7), Some(7), None, Some(7), None]);
    assert_eq!(ints.result(), 1);

    let mut durations = CountDistinctDurationState::new();
    // Go keys only on `input.Duration`, never on `Fsp`, so these are one
    // value even though their fsp differs.
    durations.update(&[
        Some(MySqlDuration::from_raw_parts(3_600_000_000_000, 0)),
        Some(MySqlDuration::from_raw_parts(3_600_000_000_000, 6)),
        None,
    ]);
    assert_eq!(durations.result(), 1);

    // `MyDecimal.ToHashKey` normalizes trailing fraction zeros.
    let mut decimals = CountDistinctDecimalState::new();
    decimals
        .update(&[
            Some(Decimal::from_literal("1.10")),
            Some(Decimal::from_literal("1.1")),
            Some(Decimal::from_literal("1.100000")),
            None,
        ])
        .unwrap();
    assert_eq!(decimals.result(), 1);
}

/// The collation-aware key is Go's `collator.Key(input)`, not the raw bytes.
#[test]
fn string_distinct_follows_the_argument_collation() {
    let values: Vec<Vec<u8>> = vec![b"abc".to_vec(), b"ABC".to_vec(), b"Abc".to_vec()];
    let slices: Vec<Option<&[u8]>> = values.iter().map(|v| Some(v.as_slice())).collect();

    let mut binary = CountDistinctStringState::new(Collation::Utf8Mb4Bin);
    binary.update(&slices);
    assert_eq!(binary.result(), 3);

    let mut case_insensitive = CountDistinctStringState::new(Collation::Utf8Mb4GeneralCi);
    case_insensitive.update(&slices);
    assert_eq!(case_insensitive.result(), 1);
}

/// Go's `map[float64]` key rules, which `set.Float64SetWithMemoryUsage`
/// inherits: `-0.0 == +0.0`, and NaN never equals itself so each insert adds
/// another counted entry.
#[test]
fn real_distinct_follows_go_map_float_key_equality() {
    let mut zeros = CountDistinctRealState::new();
    zeros.update(&[Some(0.0), Some(-0.0)]);
    assert_eq!(zeros.result(), 1);

    let mut nans = CountDistinctRealState::new();
    nans.update(&[Some(f64::NAN), Some(f64::NAN), Some(1.0)]);
    assert_eq!(nans.result(), 3);
}

/// `TestMergePartialResult4Count`'s DISTINCT shape: a source partial over
/// `0..5` merged into a destination over `2..5` yields 5 distinct values.
#[test]
fn merge_partial_result_unions_the_sets() {
    let mut source = CountDistinctIntState::new();
    source.update(&[Some(0), Some(1), Some(2), Some(3), Some(4)]);
    let mut destination = CountDistinctIntState::new();
    destination.update(&[Some(2), Some(3), Some(4)]);
    destination.merge_from(&source);
    assert_eq!(destination.result(), 5);

    let mut source = CountDistinctRealState::new();
    source.update(&[Some(f64::NAN), Some(1.0)]);
    let mut destination = CountDistinctRealState::new();
    destination.update(&[Some(f64::NAN), Some(1.0)]);
    destination.merge_from(&source);
    // Two unreachable NaN entries plus one real key, as in Go.
    assert_eq!(destination.result(), 3);

    let mut source = CountDistinctStringState::new(Collation::Utf8Mb4Bin);
    source.update(&[Some(b"a".as_slice()), Some(b"b".as_slice())]);
    let mut destination = CountDistinctStringState::new(Collation::Utf8Mb4Bin);
    destination.update(&[Some(b"b".as_slice())]);
    destination.merge_from(&source);
    assert_eq!(destination.result(), 2);
}

/// Every state's `ResetPartialResult` drops the whole set.
#[test]
fn reset_clears_every_distinct_state() {
    let mut ints = CountDistinctIntState::new();
    ints.update(&[Some(1)]);
    ints.reset();
    assert!(ints.is_empty());

    let mut reals = CountDistinctRealState::new();
    reals.update(&[Some(1.0), Some(f64::NAN)]);
    reals.reset();
    assert!(reals.is_empty());

    let mut multi = CountWithDistinctState::new();
    multi.update_row(&[Datum::Int(1)], &[]).unwrap();
    multi.reset();
    assert!(multi.is_empty());
}

/// `TestCount`'s `buildMultiArgsAggTester` arms and
/// `TestParallelDistinctCount`'s two-`VarString` case: the group-key path.
#[test]
fn multi_arg_distinct_encodes_the_whole_tuple() {
    let mut state = CountWithDistinctState::new();
    let rows: Vec<Vec<Datum>> = (0..5)
        .map(|i| vec![Datum::Int(i), Datum::Int(i)])
        .collect();
    state.update(&rows, &[Collation::Binary, Collation::Binary]).unwrap();
    assert_eq!(state.result(), 5);

    // A repeated tuple collapses; a tuple differing in only one column does
    // not.
    let mut state = CountWithDistinctState::new();
    state
        .update(
            &[
                vec![Datum::Int(1), Datum::Int(2)],
                vec![Datum::Int(1), Datum::Int(2)],
                vec![Datum::Int(2), Datum::Int(1)],
            ],
            &[Collation::Binary, Collation::Binary],
        )
        .unwrap();
    assert_eq!(state.result(), 2);
}

/// Go abandons a row at its first NULL argument (`hasNull`), so the row is
/// never inserted and a partially built key never leaks into the set.
#[test]
fn a_null_argument_skips_the_whole_row() {
    let mut state = CountWithDistinctState::new();
    assert!(!state
        .update_row(&[Datum::Int(1), Datum::Null], &[Collation::Binary; 2])
        .unwrap());
    assert!(!state
        .update_row(&[Datum::Null, Datum::Int(1)], &[Collation::Binary; 2])
        .unwrap());
    assert!(state.is_empty());

    assert!(state
        .update_row(&[Datum::Int(1), Datum::Int(1)], &[Collation::Binary; 2])
        .unwrap());
    assert_eq!(state.result(), 1);
}

/// The single-argument types Go routes through the group-key path rather
/// than a dedicated set: DATE/DATETIME/TIMESTAMP and JSON.
#[test]
fn group_key_path_covers_time_and_json_arguments() {
    let mut times = CountWithDistinctState::new();
    times
        .update(
            &[
                vec![Datum::Time(date(2020, 11, 11))],
                vec![Datum::Time(date(2020, 11, 11))],
                vec![Datum::Time(date(2020, 11, 12))],
            ],
            &[Collation::Binary],
        )
        .unwrap();
    assert_eq!(times.result(), 2);

    let json = |text: &str| Datum::Json(BinaryJSON::parse(text).unwrap());
    let mut documents = CountWithDistinctState::new();
    documents
        .update(
            &[
                vec![json(r#"{"a": 1}"#)],
                vec![json(r#"{"a": 1}"#)],
                vec![json(r#"{"a": 2}"#)],
            ],
            &[Collation::Binary],
        )
        .unwrap();
    assert_eq!(documents.result(), 2);
}

/// Go's `default:` arm: `unsupported column type for encode`.
#[test]
fn an_unencodable_argument_is_rejected() {
    let mut encoded = Vec::new();
    assert!(eval_and_encode(&mut encoded, &Datum::MaxValue, Collation::Binary).is_err());
    assert!(encoded.is_empty());
}

/// The raw-memory appenders Go writes through `unsafe.Pointer`: eight
/// native-endian bytes each, and sixteen for `types.Duration`'s
/// `{int64 Duration; int Fsp}`.
#[test]
fn raw_appenders_match_the_go_struct_layout() {
    let mut encoded = Vec::new();
    append_int64(&mut encoded, 0x0102_0304_0506_0708);
    assert_eq!(encoded, 0x0102_0304_0506_0708i64.to_ne_bytes());

    let mut encoded = Vec::new();
    append_float64(&mut encoded, 1.5);
    assert_eq!(encoded, 1.5f64.to_ne_bytes());

    let mut encoded = Vec::new();
    append_duration(&mut encoded, MySqlDuration::from_raw_parts(42, 3));
    assert_eq!(encoded.len(), 16);
    assert_eq!(&encoded[0..8], &42i64.to_ne_bytes());
    assert_eq!(&encoded[8..16], &3i64.to_ne_bytes());

    let mut encoded = Vec::new();
    append_time(&mut encoded, date(2020, 11, 11));
    assert_eq!(encoded.len(), 16);
    let mut buf = [0u8; 16];
    write_time(&mut buf, date(2020, 11, 11));
    assert_eq!(encoded, buf);
}
