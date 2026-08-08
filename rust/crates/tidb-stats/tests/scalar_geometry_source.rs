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

//! Source-backed tests for scalar geometry.

use tidb_datatype::{
    BinaryJSON, Collation, CoreTime, Datum, Decimal, MySqlDuration, MysqlEnum, MysqlSet, Time,
    TimeType, VectorFloat32,
};
use tidb_stats::{
    calc_fraction, calc_fraction_from_datums, common_prefix_length, convert_bytes_to_scalar,
    convert_datum_to_scalar,
};

#[test]
fn source_fraction_matches_interval_boundaries_and_fallback() {
    assert_eq!(calc_fraction(0.0, 4.0, 1.0), 0.25);
    assert_eq!(calc_fraction(0.0, 4.0, -1.0), 0.0);
    assert_eq!(calc_fraction(0.0, 4.0, 4.0), 1.0);
    assert_eq!(calc_fraction(4.0, 0.0, 2.0), 0.5);
    assert_eq!(calc_fraction(1.0, 1.0, 1.0), 0.5);
    assert_eq!(calc_fraction(0.0, 4.0, f64::NAN), 0.5);
    assert_eq!(calc_fraction(0.0, 4.0, f64::INFINITY), 1.0);
    assert_eq!(calc_fraction(0.0, f64::INFINITY, f64::INFINITY), 1.0);
}

#[test]
fn source_common_prefix_length_handles_empty_and_multiple_strings() {
    assert_eq!(common_prefix_length(&[]), 0);
    assert_eq!(common_prefix_length(&[b"abc"]), 3);
    assert_eq!(common_prefix_length(&[b"", b"abc"]), 0);
    assert_eq!(common_prefix_length(&[b"abc", b"xyz"]), 0);
    assert_eq!(common_prefix_length(&[b"abc", b"abd", b"abz"]), 2);
    assert_eq!(common_prefix_length(&[b"abc", b"abc", b"abc"]), 3);
    assert_eq!(common_prefix_length(&[b"abcdef", b"ab", b"abcd"]), 2);
}

#[test]
fn source_byte_scalar_pins_every_switch_width_and_truncates_after_eight() {
    let bytes = [1_u8, 2, 3, 4, 5, 6, 7, 8, 9];
    let expected = [
        0x0000_0000_0000_0000_u64,
        0x0100_0000_0000_0000_u64,
        0x0102_0000_0000_0000_u64,
        0x0102_0300_0000_0000_u64,
        0x0102_0304_0000_0000_u64,
        0x0102_0304_0500_0000_u64,
        0x0102_0304_0506_0000_u64,
        0x0102_0304_0506_0700_u64,
        0x0102_0304_0506_0708_u64,
        0x0102_0304_0506_0708_u64,
    ];
    for (length, expected) in expected.into_iter().enumerate() {
        assert_eq!(convert_bytes_to_scalar(&bytes[..length]), expected as f64);
    }
}

#[test]
fn source_datum_scalar_preserves_typed_cases_and_invalid_timestamp_fallback() {
    let value = Datum::new_float32_from_f64(0.1);
    assert_eq!(convert_datum_to_scalar(&value, 0), 0.100_000_001_490_116_12);
    assert_ne!(convert_datum_to_scalar(&value, 0), 0.1);

    assert_eq!(convert_datum_to_scalar(&Datum::Real(1.25), 0), 1.25);
    assert_eq!(convert_datum_to_scalar(&Datum::Int(-2), 0), -2.0);
    assert_eq!(convert_datum_to_scalar(&Datum::UInt(3), 0), 3.0);
    assert_eq!(
        convert_datum_to_scalar(
            &Datum::Duration(MySqlDuration::from_nanoseconds(4_000, 6).unwrap()),
            0,
        ),
        4_000.0
    );
    assert_eq!(
        convert_datum_to_scalar(&Datum::Decimal(Decimal::from_literal("1.25")), 0),
        1.25
    );

    for kind in [TimeType::Date, TimeType::DateTime] {
        let minimum = Time::new(CoreTime::from_date(1, 1, 1, 0, 0, 0, 0), kind, 0).unwrap();
        assert_eq!(convert_datum_to_scalar(&Datum::Time(minimum), 0), 0.0);
    }
    let minimum_timestamp = Time::new(
        CoreTime::from_date(1970, 1, 1, 0, 0, 1, 0),
        TimeType::Timestamp,
        0,
    )
    .unwrap();
    assert_eq!(
        convert_datum_to_scalar(&Datum::Time(minimum_timestamp), 0),
        0.0
    );

    let bytes = Datum::new_bytes([1_u8, 2]);
    assert_eq!(convert_datum_to_scalar(&bytes, 2), 0.0);
    assert_eq!(
        convert_datum_to_scalar(&bytes, 1),
        convert_bytes_to_scalar(&[2])
    );
    let string = Datum::new_string([1_u8, 2]);
    assert_eq!(convert_datum_to_scalar(&string, 2), 0.0);
    assert_eq!(
        convert_datum_to_scalar(&string, 1),
        convert_bytes_to_scalar(&[2])
    );
    assert_eq!(convert_datum_to_scalar(&Datum::MinNotNull, 0), -f64::MAX);
    assert_eq!(convert_datum_to_scalar(&Datum::MaxValue, 0), f64::MAX);
    assert_eq!(convert_datum_to_scalar(&Datum::Null, 0), 0.0);

    let invalid_february = Time::new(
        CoreTime::from_date(2017, 2, 31, 0, 0, 0, 0),
        TimeType::Timestamp,
        0,
    )
    .unwrap();
    let normalized_march = Time::new(
        CoreTime::from_date(2017, 3, 3, 0, 0, 0, 0),
        TimeType::Timestamp,
        0,
    )
    .unwrap();
    assert_eq!(
        convert_datum_to_scalar(&Datum::Time(invalid_february), 0),
        convert_datum_to_scalar(&Datum::Time(normalized_march), 0)
    );

    let month_zero = Time::new(
        CoreTime::from_date(2017, 0, 1, 0, 0, 0, 0),
        TimeType::Timestamp,
        0,
    )
    .unwrap();
    let previous_december = Time::new(
        CoreTime::from_date(2016, 12, 1, 0, 0, 0, 0),
        TimeType::Timestamp,
        0,
    )
    .unwrap();
    assert_eq!(
        convert_datum_to_scalar(&Datum::Time(month_zero), 0),
        convert_datum_to_scalar(&Datum::Time(previous_december), 0)
    );
}

#[test]
fn source_fraction_reads_fresh_mismatched_bounds_through_the_value_kind_getter() {
    let float32 = Datum::new_float32_from_f64(0.25);
    assert_eq!(
        calc_fraction_from_datums(&Datum::Real(0.0), &Datum::Real(1.0), &float32),
        0.25
    );
    assert_eq!(
        calc_fraction_from_datums(
            &Datum::Real(0.0),
            &Datum::Real(1.0),
            &Datum::new_float32_from_f64(0.1),
        ),
        f64::from(0.1_f32)
    );
    assert_eq!(
        calc_fraction_from_datums(
            &Datum::new_float32_from_f64(0.0),
            &Datum::new_float32_from_f64(1.0),
            &Datum::Real(0.25),
        ),
        0.25
    );

    // Go's Enum/Set setters store their numeric value in the same `i` field
    // read by GetInt64/GetUint64, irrespective of the bound's own kind.
    assert_eq!(
        calc_fraction_from_datums(
            &Datum::Enum(MysqlEnum::new("zero", 0), Collation::Binary),
            &Datum::Enum(MysqlEnum::new("four", 4), Collation::Binary),
            &Datum::Int(1),
        ),
        0.25
    );
    assert_eq!(
        calc_fraction_from_datums(
            &Datum::Set(MysqlSet::new("zero", 0), Collation::Binary),
            &Datum::Set(MysqlSet::new("four", 4), Collation::Binary),
            &Datum::UInt(1),
        ),
        0.25
    );

    let duration = Datum::Duration(MySqlDuration::from_nanoseconds(1_000, 0).unwrap());
    assert_eq!(
        calc_fraction_from_datums(&Datum::Int(0), &Datum::Int(4_000), &duration),
        0.25
    );

    let json = BinaryJSON::parse("null").unwrap();
    let json_raw_i = i64::from(json.type_code());
    assert_eq!(
        calc_fraction_from_datums(
            &Datum::Json(json),
            &Datum::Int(json_raw_i + 4),
            &Datum::Int(json_raw_i + 1),
        ),
        0.25
    );
    assert_eq!(
        calc_fraction_from_datums(
            &Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0])),
            &Datum::Int(4),
            &Datum::Int(1),
        ),
        0.25
    );

    assert_eq!(
        calc_fraction_from_datums(
            &Datum::Int(0),
            &Datum::UInt(4),
            &Datum::Decimal(Decimal::from_int(1)),
        ),
        0.25
    );
    let minimum_timestamp = Datum::Time(
        Time::new(
            CoreTime::from_date(1970, 1, 1, 0, 0, 1, 0),
            TimeType::Timestamp,
            0,
        )
        .unwrap(),
    );
    assert_eq!(
        calc_fraction_from_datums(&Datum::Int(-4), &Datum::UInt(4), &minimum_timestamp),
        0.5
    );

    // GetBytes reads the independent Go `b` field even though this lower
    // bound is KindRaw. Its byte prefix is then used while each datum is
    // converted according to its own kind.
    assert_eq!(
        calc_fraction_from_datums(
            &Datum::Raw(b"aa".to_vec()),
            &Datum::Bytes(b"ac".to_vec()),
            &Datum::String(tidb_datatype::StringDatum::new(
                b"ab".to_vec(),
                Collation::DEFAULT,
            )),
        ),
        calc_fraction(
            0.0,
            convert_bytes_to_scalar(b"c"),
            convert_bytes_to_scalar(b"b"),
        )
    );
    assert_eq!(
        calc_fraction_from_datums(&Datum::Int(0), &Datum::UInt(1), &Datum::Null),
        0.5
    );
}
