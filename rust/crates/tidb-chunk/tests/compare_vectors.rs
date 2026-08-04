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

//! Go's own answers for `pkg/util/chunk/compare.go`, replayed.
//!
//! `chunk_test.go`'s `TestCompare` only ever orders null < 0 < 1 per type, so
//! it cannot see collation, `BinaryLiteral` length-first bit ordering,
//! ENUM/SET value-not-name ordering, JSON type precedence, unsigned wrap, or
//! the `sort.Search` probe sequence behind `LowerBound`'s `match` flag. Those
//! are what this fixture pins.
//!
//! The fixture is written by
//! `rust/difftests/chunk-tests/fixtures/generate_compare_vectors.go`, which
//! drives the REAL Go package; the recipes below rebuild the same cells and
//! datums from the same names.

use std::cmp::Ordering;
use tidb_chunk::chunk::Chunk;
use tidb_chunk::compare::get_compare_func;
use tidb_datatype::{
    BinaryJSON, Collation, CoreTime, Datum, FieldType, FieldTypeCode, MyDecimal, MySqlDuration,
    MysqlEnum, MysqlSet, Time, TimeType,
};

const FIXTURE: &str = include_str!("../../../difftests/chunk-tests/fixtures/compare_vectors.tsv");

const NUM_ROWS: usize = 8;

/// Fills one named column of the comparison chunk.
type ColumnFill = Box<dyn Fn(&mut Chunk, usize)>;
/// Fills the single column of a bound-search chunk.
type BoundFill = Box<dyn Fn(&mut Chunk)>;

/// The seven non-null strings shared by the three string columns.
const STRING_VALUES: [&str; 7] = ["", "A", "a", "ab", "ab ", "b", "B"];

fn datetime(y: i32, mo: i32, d: i32, h: i32, mi: i32, s: i32) -> Time {
    Time::new(
        CoreTime::from_date(y as u16, mo as u8, d as u8, h as u8, mi as u8, s as u8, 0),
        TimeType::DateTime,
        0,
    )
    .expect("a valid datetime")
}

fn decimal(text: &str) -> MyDecimal {
    let (value, err) = MyDecimal::from_string(text.as_bytes());
    assert!(err.is_none(), "decimal literal {text} must parse");
    value
}

fn duration_secs(secs: i64) -> MySqlDuration {
    MySqlDuration::from_nanoseconds(secs * 1_000_000_000, 0).expect("in TIME range")
}

/// The field type and cell recipe for each named column of the fixture.
fn build_column(name: &str) -> (FieldType, ColumnFill) {
    match name {
        "int_signed" => (
            FieldType::new(FieldTypeCode::LongLong),
            Box::new(|chk, c| {
                chk.append_null(c);
                for v in [i64::MIN, -1, 0, 1, 42, 42, i64::MAX] {
                    chk.append_int64(c, v);
                }
            }),
        ),
        "int_unsigned" => (
            FieldType::new(FieldTypeCode::LongLong).with_unsigned(true),
            Box::new(|chk, c| {
                chk.append_null(c);
                for v in [0, 1, 42, 42, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX] {
                    chk.append_uint64(c, v);
                }
            }),
        ),
        "year" => (
            FieldType::new(FieldTypeCode::Year),
            Box::new(|chk, c| {
                chk.append_null(c);
                for v in [0, 1901, 1999, 2000, 2000, 2155, 9999] {
                    chk.append_int64(c, v);
                }
            }),
        ),
        "float" => (
            FieldType::new(FieldTypeCode::Float),
            Box::new(|chk, c| {
                chk.append_null(c);
                for v in [-1.5f32, -0.0, 0.0, 1.5, 1.5, f32::MAX, f32::from_bits(1)] {
                    chk.append_float32(c, v);
                }
            }),
        ),
        "double" => (
            FieldType::new(FieldTypeCode::Double),
            Box::new(|chk, c| {
                chk.append_null(c);
                for v in [-1.5f64, -0.0, 0.0, 1.5, 1.5, f64::MAX, f64::from_bits(1)] {
                    chk.append_float64(c, v);
                }
            }),
        ),
        "varchar_bin" => (
            FieldType::new(FieldTypeCode::Varchar),
            Box::new(append_string_values),
        ),
        "varchar_ci" => (
            FieldType::new(FieldTypeCode::Varchar)
                .with_charset_name("utf8mb4")
                .with_collation_name("utf8mb4_general_ci"),
            Box::new(append_string_values),
        ),
        "blob_binary" => (
            FieldType::new(FieldTypeCode::Blob),
            Box::new(append_string_values),
        ),
        "datetime" => (
            FieldType::new(FieldTypeCode::Datetime),
            Box::new(|chk, c| {
                chk.append_null(c);
                for v in [
                    datetime(1000, 1, 1, 0, 0, 0),
                    datetime(2000, 1, 1, 0, 0, 0),
                    datetime(2000, 1, 1, 0, 0, 1),
                    datetime(2000, 1, 1, 0, 0, 1),
                    datetime(2000, 12, 31, 23, 59, 59),
                    datetime(2020, 6, 15, 12, 0, 0),
                    datetime(9999, 12, 31, 23, 59, 59),
                ] {
                    chk.append_time(c, v);
                }
            }),
        ),
        "duration" => (
            FieldType::new(FieldTypeCode::Duration),
            Box::new(|chk, c| {
                chk.append_null(c);
                for secs in [-838 * 3600, -1, 0, 0, 1, 3600, 838 * 3600] {
                    chk.append_duration(c, duration_secs(secs));
                }
            }),
        ),
        "decimal" => (
            FieldType::new(FieldTypeCode::NewDecimal),
            Box::new(|chk, c| {
                chk.append_null(c);
                for text in ["-99999.9", "-1.5", "0", "0.00", "1.50", "2", "10"] {
                    chk.append_my_decimal(c, &decimal(text));
                }
            }),
        ),
        "enum" => (
            FieldType::new(FieldTypeCode::Enum).with_elems(["a", "b", "c"]),
            Box::new(|chk, c| {
                chk.append_null(c);
                // The names deliberately disagree with the numeric order.
                for (name, value) in [
                    ("c", 0),
                    ("b", 1),
                    ("a", 2),
                    ("z", 2),
                    ("a", 3),
                    ("a", 4),
                    ("a", 5),
                ] {
                    chk.append_enum(c, &MysqlEnum::new(name, value));
                }
            }),
        ),
        "set" => (
            FieldType::new(FieldTypeCode::Set).with_elems(["a", "b", "c"]),
            Box::new(|chk, c| {
                chk.append_null(c);
                for (name, value) in [
                    ("c", 0),
                    ("b", 1),
                    ("a", 2),
                    ("z", 2),
                    ("a,b", 3),
                    ("c", 4),
                    ("a,b,c", 7),
                ] {
                    chk.append_set(c, &MysqlSet::new(name, value));
                }
            }),
        ),
        "bit" => (
            FieldType::new(FieldTypeCode::Bit),
            Box::new(|chk, c| {
                chk.append_null(c);
                for bytes in [
                    &[][..],
                    &[0x00],
                    &[0x00, 0x00],
                    &[0x01],
                    &[0x00, 0x01],
                    &[0x02],
                    &[0x01, 0x00],
                ] {
                    chk.append_bytes(c, bytes);
                }
            }),
        ),
        "json" => (
            FieldType::new(FieldTypeCode::Json),
            Box::new(|chk, c| {
                chk.append_null(c);
                for text in ["null", "false", "true", "1", "2.5", "\"abc\"", "{\"a\": 1}"] {
                    chk.append_json(c, &BinaryJSON::parse(text).expect("valid JSON"));
                }
            }),
        ),
        other => panic!("unknown fixture column {other}"),
    }
}

fn append_string_values(chk: &mut Chunk, c: usize) {
    chk.append_null(c);
    for v in STRING_VALUES {
        chk.append_string(c, v);
    }
}

/// The non-decreasing columns `LowerBound`/`UpperBound` are searched over.
fn build_bound_column(name: &str) -> (FieldType, BoundFill) {
    match name {
        "lb_int" => (
            FieldType::new(FieldTypeCode::LongLong),
            Box::new(|chk| {
                for v in [1, 3, 3, 3, 5, 7, 9, 9] {
                    chk.append_int64(0, v);
                }
            }),
        ),
        "lb_nullable_int" => (
            FieldType::new(FieldTypeCode::LongLong),
            Box::new(|chk| {
                chk.append_null(0);
                chk.append_null(0);
                for v in [1, 3, 3, 5, 7, 9] {
                    chk.append_int64(0, v);
                }
            }),
        ),
        "lb_str" => (
            FieldType::new(FieldTypeCode::Varchar),
            Box::new(|chk| {
                for v in ["a", "b", "b", "c", "e", "f", "g", "h"] {
                    chk.append_string(0, v);
                }
            }),
        ),
        other => panic!("unknown fixture bound column {other}"),
    }
}

fn probe_datum(name: &str) -> Datum {
    let string = |text: &str| {
        let mut d = Datum::Null;
        d.set_string(text.as_bytes().to_vec(), Collation::Utf8Mb4Bin);
        d
    };
    match name {
        "null" => Datum::Null,
        "min_not_null" => Datum::MinNotNull,
        "max_value" => Datum::MaxValue,
        "i0" => Datum::Int(0),
        "i1" => Datum::Int(1),
        "i2" => Datum::Int(2),
        "i3" => Datum::Int(3),
        "i4" => Datum::Int(4),
        "i9" => Datum::Int(9),
        "i10" => Datum::Int(10),
        "s_a" => string("a"),
        "s_b" => string("b"),
        "s_d" => string("d"),
        "s_z" => string("z"),
        other => panic!("unknown fixture probe {other}"),
    }
}

fn ordering_from_go(value: &str) -> Ordering {
    match value {
        "-1" => Ordering::Less,
        "0" => Ordering::Equal,
        "1" => Ordering::Greater,
        other => panic!("Go compare results are -1/0/1, got {other}"),
    }
}

/// Every `GetCompareFunc` answer Go produced for the fixture's columns.
#[test]
fn go_compare_func_answers_replay() {
    let mut checked = 0;
    for line in FIXTURE.lines().filter(|l| l.starts_with("cmp\t")) {
        let f: Vec<&str> = line.split('\t').collect();
        let (name, i, j, want) = (
            f[1],
            f[2].parse::<usize>().expect("row index"),
            f[3].parse::<usize>().expect("row index"),
            ordering_from_go(f[4]),
        );
        let (field_type, fill) = build_column(name);
        let mut chk = Chunk::new_with_capacity(std::slice::from_ref(&field_type), NUM_ROWS);
        fill(&mut chk, 0);
        assert_eq!(chk.num_rows(), NUM_ROWS, "{name} must fill all rows");
        let cmp_func = get_compare_func(&field_type).expect("a comparator for {name}");
        assert_eq!(
            cmp_func(chk.get_row(i), 0, chk.get_row(j), 0),
            want,
            "column {name}, rows {i} vs {j}"
        );
        checked += 1;
    }
    assert_eq!(checked, 15 * NUM_ROWS * NUM_ROWS, "the fixture's cmp rows");
}

/// Every `LowerBound`/`UpperBound` answer Go produced, including the `match`
/// flag -- which is a property of the bisection's probe sequence, not just of
/// the returned index.
#[test]
fn go_bound_search_answers_replay() {
    let mut checked = 0;
    for line in FIXTURE
        .lines()
        .filter(|l| l.starts_with("lb\t") || l.starts_with("ub\t"))
    {
        let f: Vec<&str> = line.split('\t').collect();
        let (kind, name, probe) = (f[0], f[1], f[2]);
        let (field_type, fill) = build_bound_column(name);
        let mut chk = Chunk::new_with_capacity(std::slice::from_ref(&field_type), NUM_ROWS);
        fill(&mut chk);
        assert_eq!(chk.num_rows(), NUM_ROWS, "{name} must fill all rows");
        let datum = probe_datum(probe);
        if kind == "lb" {
            let want = (f[3].parse::<usize>().expect("index"), f[4] == "1");
            assert_eq!(
                chk.lower_bound(0, &datum),
                want,
                "lower_bound {name}/{probe}"
            );
        } else {
            let want = f[3].parse::<usize>().expect("index");
            assert_eq!(
                chk.upper_bound(0, &datum),
                want,
                "upper_bound {name}/{probe}"
            );
        }
        checked += 1;
    }
    assert_eq!(checked, (10 + 10 + 7) * 2, "the fixture's lb/ub rows");
}
