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

//! Source-backed tests for Datum-free scalar geometry.

use tidb_datatype::{CoreTime, Datum, Time, TimeType};
use tidb_stats::histogram::{calc_fraction_from_datums, convert_datum_to_scalar};
use tidb_stats::{calc_fraction, common_prefix_length, convert_bytes_to_scalar};

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
fn source_float32_narrows_before_widening() {
    let value = Datum::new_float32_from_f64(0.1);
    assert_eq!(convert_datum_to_scalar(&value, 0), 0.100_000_001_490_116_12);
    assert_ne!(convert_datum_to_scalar(&value, 0), 0.1);
}

#[test]
fn source_fraction_reads_bounds_through_the_value_kinds_raw_getter() {
    // Go switches on `value.Kind()` and then calls GetInt64 on every datum;
    // the getter returns the shared raw payload even when a bound is UInt.
    assert_eq!(
        calc_fraction_from_datums(&Datum::UInt(u64::MAX), &Datum::UInt(0), &Datum::Int(0),),
        1.0
    );

    let lower = Datum::new_float32_from_f64(0.0);
    let upper = Datum::new_float32_from_f64(1.0);
    let value = Datum::new_float32_from_f64(0.1);
    assert_eq!(
        calc_fraction_from_datums(&lower, &upper, &value),
        f64::from(0.1_f32)
    );
}

#[test]
fn source_invalid_timestamp_uses_go_time_date_normalization_after_error() {
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
