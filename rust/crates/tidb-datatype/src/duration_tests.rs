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

use super::{truncate_overflow_mysql_time, DurationOverflow, MAX_TIME_NANOS, MIN_TIME_NANOS};

use super::{
    can_fallback_to_datetime, classify_duration_datetime_fallback, parse_duration,
    parse_mysql_duration, round_duration_fsp, DurationDateTimeFallbackKind, DurationParseError,
    DurationParseEvent, DurationRoundError, MySqlDuration, TimeType,
};

#[test]
fn can_fallback_to_datetime_matches_source_shape_rows() {
    // Source: pkg/types/time.go::canFallbackToDateTime, exercised by the
    // datetime-shaped rows in pkg/types/time_test.go::TestTime.
    assert_eq!(
        classify_duration_datetime_fallback(b"201111111212"),
        Some(DurationDateTimeFallbackKind::Compact12)
    );
    assert!(can_fallback_to_datetime(b"201111111212"));
    assert_eq!(
        classify_duration_datetime_fallback(b"20111111121212.123"),
        Some(DurationDateTimeFallbackKind::Compact14)
    );
    assert_eq!(
        classify_duration_datetime_fallback(b"2011-11-11 00:00:01"),
        Some(DurationDateTimeFallbackKind::Separated)
    );
    assert_eq!(
        classify_duration_datetime_fallback(b"2011-11-11T12:12:12"),
        Some(DurationDateTimeFallbackKind::Separated)
    );
    assert_eq!(
        classify_duration_datetime_fallback(b"2011@12@13 T"),
        Some(DurationDateTimeFallbackKind::Separated)
    );

    // A date without a trailing time separator is not eligible for the
    // source fallback, and symbols are not Unicode punctuation in Go.
    for input in [
        b"2011-11-11".as_slice(),
        b"2011+12+13 00".as_slice(),
        b" 201111111212".as_slice(),
        b"2011-11-11\n".as_slice(),
    ] {
        assert_eq!(classify_duration_datetime_fallback(input), None);
        assert!(!can_fallback_to_datetime(input));
    }
}

#[test]
fn duration_methods_match_source_rows() {
    for (left, left_fsp, right, right_fsp, sum, difference) in [
        (100_000_000, 1, 100_000_000, 1, "00:00:00.2", "00:00:00.0"),
        (0, 0, 100_000_000, 1, "00:00:00.1", "-00:00:00.1"),
        (90_000_000, 2, 10_000_000, 2, "00:00:00.10", "00:00:00.08"),
    ] {
        let left = MySqlDuration::from_nanoseconds(left, left_fsp).unwrap();
        let right = MySqlDuration::from_nanoseconds(right, right_fsp).unwrap();
        assert_eq!(left.checked_add(right).unwrap().to_string(), sum);
        assert_eq!(left.checked_sub(right).unwrap().to_string(), difference);
    }
    assert!(MySqlDuration::from_nanoseconds(i64::MAX, 0)
        .unwrap()
        .checked_add(MySqlDuration::from_nanoseconds(60_000_000_000, 0).unwrap())
        .is_err());

    let duration = MySqlDuration::new(23, 12, 34, 123_456, 6).unwrap();
    assert_eq!(
        duration.duration_format("%H %k %h %I %l %i %p %r %T %s %S %f %%"),
        "23 23 11 11 11 12 PM 11:12:34 PM 23:12:34 34 34 123456 %"
    );
    assert_eq!(duration.to_number().to_string(), "231234.123456");
    assert_eq!(
        MySqlDuration::new(-11, -30, -45, -923_345, 6)
            .unwrap()
            .to_number()
            .to_string(),
        "-113045.923345"
    );
    assert_eq!(
        MySqlDuration::new(10, 10, 10, 888_888, 6)
            .unwrap()
            .round_frac(0)
            .unwrap()
            .to_string(),
        "10:10:11"
    );
    assert_eq!(
        duration.compare_string("23:12:34.123456").unwrap(),
        std::cmp::Ordering::Equal
    );
}

#[test]
fn duration_time_and_year_conversion_match_source_rows() {
    use chrono::{TimeZone, Utc};

    let now = Utc.with_ymd_and_hms(2023, 11, 13, 3, 9, 0).unwrap();
    let converted = MySqlDuration::new(1, 0, 0, 0, 0)
        .unwrap()
        .convert_to_time(now, TimeType::DateTime, false, false)
        .unwrap();
    assert_eq!(converted.to_string(), "2023-11-13 01:00:00");

    for (duration, now, through_concat, expected) in [
        (
            MySqlDuration::new(1, 0, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2023, 11, 13, 3, 9, 0).unwrap(),
            false,
            2023,
        ),
        (
            MySqlDuration::new(40, 0, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2023, 12, 31, 11, 0, 0).unwrap(),
            false,
            2024,
        ),
        (
            MySqlDuration::new(-20, 0, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2024, 1, 1, 13, 0, 0).unwrap(),
            false,
            2023,
        ),
        (
            MySqlDuration::new(0, 20, 12, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2023, 11, 13, 3, 9, 0).unwrap(),
            true,
            2012,
        ),
        (
            MySqlDuration::new(0, 0, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2023, 11, 13, 3, 9, 0).unwrap(),
            true,
            0,
        ),
    ] {
        assert_eq!(
            duration.convert_to_year(now, through_concat).unwrap(),
            expected
        );
    }
}

#[test]
fn complete_duration_parser_handles_source_datetime_fallback_rows() {
    for (input, expected) in [
        ("2011-11-11 00:00:01", "00:00:01.000000"),
        ("20111111121212.123", "12:12:12.123000"),
        ("2011-11-11T12:12:12", "12:12:12.000000"),
    ] {
        let parsed = parse_mysql_duration(input, 6, &chrono_tz::UTC, true, false).unwrap();
        let duration = MySqlDuration::from_nanoseconds(parsed.nanoseconds(), parsed.fsp()).unwrap();
        assert_eq!(duration.to_string(), expected, "{input}");
    }
}

#[test]
fn complete_duration_parser_matches_all_test_time_rows() {
    for (input, expected) in [
        ("10:11:12", "10:11:12"),
        ("101112", "10:11:12"),
        ("020005", "02:00:05"),
        ("112", "00:01:12"),
        ("10:11", "10:11:00"),
        ("101112.123456", "10:11:12"),
        ("1112", "00:11:12"),
        ("1", "00:00:01"),
        ("12", "00:00:12"),
        ("1 12", "36:00:00"),
        ("1 10:11:12", "34:11:12"),
        ("1 10:11:12.123456", "34:11:12"),
        ("10:11:12.123456", "10:11:12"),
        ("1 10:11", "34:11:00"),
        ("1 10", "34:00:00"),
        ("24 10", "586:00:00"),
        ("-24 10", "-586:00:00"),
        ("0 10", "10:00:00"),
        ("-10:10:10", "-10:10:10"),
        ("-838:59:59", "-838:59:59"),
        ("838:59:59", "838:59:59"),
        ("2011-11-11 00:00:01", "00:00:01"),
        ("20111111121212.123", "12:12:12"),
        ("2011-11-11T12:12:12", "12:12:12"),
    ] {
        let parsed = parse_mysql_duration(input, 0, &chrono_tz::UTC, true, false).unwrap();
        let duration = MySqlDuration::from_nanoseconds(parsed.nanoseconds(), parsed.fsp()).unwrap();
        assert_eq!(duration.to_string(), expected, "{input}");
        assert_eq!(parsed.event(), None, "{input}");
    }

    for (input, expected) in [
        ("101112.123456", "10:11:12.123456"),
        ("1 10:11:12.123456", "34:11:12.123456"),
        ("10:11:12.123456", "10:11:12.123456"),
    ] {
        let parsed = parse_mysql_duration(input, 6, &chrono_tz::UTC, true, false).unwrap();
        let duration = MySqlDuration::from_nanoseconds(parsed.nanoseconds(), parsed.fsp()).unwrap();
        assert_eq!(duration.to_string(), expected, "{input}");
    }

    for (input, expected) in [
        ("0x", "00:00:00.000000"),
        ("1x", "00:00:01.000000"),
        ("0000-00-00", "00:00:00.000000"),
    ] {
        let parsed = parse_mysql_duration(input, 6, &chrono_tz::UTC, true, false).unwrap();
        let duration = MySqlDuration::from_nanoseconds(parsed.nanoseconds(), parsed.fsp()).unwrap();
        assert_eq!(duration.to_string(), expected, "{input}");
        assert_eq!(
            parsed.event(),
            Some(DurationParseEvent::Truncated),
            "{input}"
        );
    }

    for input in ["2011-11-11", "232 10", "-232 10"] {
        let parsed = parse_mysql_duration(input, 0, &chrono_tz::UTC, true, false).unwrap();
        assert!(parsed.event().is_some(), "{input}");
    }
    let overflow =
        parse_mysql_duration("4294967295 0:59:59", 0, &chrono_tz::UTC, true, false).unwrap();
    assert_eq!(overflow.nanoseconds(), MAX_TIME_NANOS);
    assert_eq!(
        overflow.event(),
        Some(DurationParseEvent::Overflow(DurationOverflow::Positive))
    );
}

#[test]
fn truncate_overflow_mysql_time_matches_source_endpoints() {
    // Source: pkg/types/time.go::TruncateOverflowMySQLTime and
    // pkg/types/time_test.go::TestTruncateOverflowMySQLTime.
    let positive = truncate_overflow_mysql_time(MAX_TIME_NANOS + 1);
    assert_eq!(positive.value(), MAX_TIME_NANOS);
    assert_eq!(positive.overflow(), Some(DurationOverflow::Positive));
    assert_eq!(
        positive.event(),
        Some(DurationParseEvent::Overflow(DurationOverflow::Positive))
    );

    let negative = truncate_overflow_mysql_time(MIN_TIME_NANOS - 1);
    assert_eq!(negative.value(), MIN_TIME_NANOS);
    assert_eq!(negative.overflow(), Some(DurationOverflow::Negative));
    assert_eq!(
        negative.event(),
        Some(DurationParseEvent::Overflow(DurationOverflow::Negative))
    );

    for value in [
        MAX_TIME_NANOS,
        MIN_TIME_NANOS,
        MAX_TIME_NANOS - 1,
        MIN_TIME_NANOS + 1,
    ] {
        let result = truncate_overflow_mysql_time(value);
        assert_eq!(result.value(), value);
        assert_eq!(result.overflow(), None);
        assert_eq!(result.event(), None);
    }
}

#[test]
fn duration_parse_events_classify_source_warning_branches() {
    // Source: pkg/types/time.go::{matchDuration, ParseDuration,
    // TruncateOverflowMySQLTime}; the owner decides warning/error policy.
    let in_range = parse_duration(b"10:11:12", 0).unwrap();
    assert_eq!(in_range.event(), None);

    let positive = parse_duration(b"232 10", 0).unwrap();
    assert_eq!(
        positive.event(),
        Some(DurationParseEvent::Overflow(DurationOverflow::Positive))
    );
    let negative = parse_duration(b"-232 10", 0).unwrap();
    assert_eq!(
        negative.event(),
        Some(DurationParseEvent::Overflow(DurationOverflow::Negative))
    );

    let fallback = parse_duration(b"2011-11-11 00:00:01", 0).unwrap_err();
    assert_eq!(
        fallback.event(),
        Some(DurationParseEvent::DateTimeFallback(
            DurationDateTimeFallbackKind::Separated
        ))
    );

    let truncated = parse_duration(b"0x", 0).unwrap_err();
    assert_eq!(truncated.event(), Some(DurationParseEvent::Truncated));
    let invalid_fsp = parse_duration(b"10:11:12", -2).unwrap_err();
    assert_eq!(invalid_fsp.event(), None);
}

#[test]
fn round_duration_fsp_matches_source_half_away_from_zero_rows() {
    // Source: pkg/types/time.go::Duration.RoundFrac and
    // pkg/types/time_test.go::TestRoundFrac duration rows.
    let second = 1_000_000_000_i64;
    let value = 10 * 60 * 60 * second + 10 * 60 * second + 10 * second + 123_456_000;
    let rounded = round_duration_fsp(value, 6, 4).unwrap();
    assert_eq!(rounded.nanoseconds(), value + 44_000);
    assert_eq!(rounded.fsp(), 4);

    let rounded = round_duration_fsp(value, 6, 0).unwrap();
    assert_eq!(
        rounded.nanoseconds(),
        10 * 60 * 60 * second + 10 * 60 * second + 10 * second
    );
    assert_eq!(rounded.fsp(), 0);

    let carry = round_duration_fsp(999_999_000, 6, 4).unwrap();
    assert_eq!(carry.nanoseconds(), second);
    assert_eq!(carry.fsp(), 4);

    let negative = round_duration_fsp(-999_999_000, 6, 0).unwrap();
    assert_eq!(negative.nanoseconds(), -second);
    assert_eq!(negative.fsp(), 0);

    let unchanged = round_duration_fsp(123, 4, 4).unwrap();
    assert_eq!(unchanged.nanoseconds(), 123);
    assert_eq!(unchanged.fsp(), 4);

    let clamped_unchanged = round_duration_fsp(123, 6, 7).unwrap();
    assert_eq!(clamped_unchanged.nanoseconds(), 123);
    assert_eq!(clamped_unchanged.fsp(), 6);

    assert_eq!(
        round_duration_fsp(1, 6, -2),
        Err(DurationRoundError::InvalidFsp(super::FspError::InvalidFsp(
            -2
        )))
    );
}

#[test]
fn parse_duration_matches_source_colon_and_day_forms() {
    // Source: pkg/types/time.go::{matchDuration, ParseDuration} and the
    // valid rows in pkg/types/time_test.go::TestTime.
    let parsed = parse_duration(b"10:11:12.123456", 6).unwrap();
    assert_eq!(
        parsed.nanoseconds(),
        10 * 3_600_000_000_000 + 11 * 60_000_000_000 + 12_123_456_000
    );
    let spaced = parse_duration(b"101112 .123456", 6).unwrap();
    assert_eq!(spaced.nanoseconds(), parsed.nanoseconds());
    assert_eq!(parsed.fsp(), 6);
    assert_eq!(parsed.overflow(), None);

    let parsed = parse_duration(b"1 10:11:12.123456", 6).unwrap();
    assert_eq!(
        parsed.nanoseconds(),
        34 * 3_600_000_000_000 + 11 * 60_000_000_000 + 12_123_456_000
    );

    let parsed = parse_duration(b"-10:10:10", 0).unwrap();
    assert_eq!(
        parsed.nanoseconds(),
        -(10 * 3_600_000_000_000 + 10 * 60_000_000_000 + 10_000_000_000)
    );
    assert_eq!(parsed.fsp(), 0);

    let parsed = parse_duration(b"10:11", 0).unwrap();
    assert_eq!(
        parsed.nanoseconds(),
        10 * 3_600_000_000_000 + 11 * 60_000_000_000
    );

    let parsed = parse_duration(b"101112.123456", 6).unwrap();
    assert_eq!(
        parsed.nanoseconds(),
        10 * 3_600_000_000_000 + 11 * 60_000_000_000 + 12_123_456_000
    );
    assert_eq!(
        parse_duration(b"020005", 0).unwrap().nanoseconds(),
        2 * 3_600_000_000_000 + 5_000_000_000
    );
    assert_eq!(
        parse_duration(b"112", 0).unwrap().nanoseconds(),
        72_000_000_000
    );
    assert_eq!(
        parse_duration(b"1112", 0).unwrap().nanoseconds(),
        11 * 60_000_000_000 + 12_000_000_000
    );
    assert_eq!(
        parse_duration(b"1", 0).unwrap().nanoseconds(),
        1_000_000_000
    );
    assert_eq!(
        parse_duration(b"12", 0).unwrap().nanoseconds(),
        12_000_000_000
    );

    for (input, expected_nanos) in [
        (b"1 12".as_slice(), 36_i64 * 3_600_000_000_000),
        (
            b"1 10:11".as_slice(),
            34 * 3_600_000_000_000 + 11 * 60_000_000_000,
        ),
        (b"1 10".as_slice(), 34 * 3_600_000_000_000),
        (b"24 10".as_slice(), 586 * 3_600_000_000_000),
    ] {
        assert_eq!(
            parse_duration(input, 0).unwrap().nanoseconds(),
            expected_nanos
        );
    }
    assert_eq!(
        parse_duration(b"-24 10", 0).unwrap().nanoseconds(),
        -586 * 3_600_000_000_000
    );

    let overflow = parse_duration(b"4294967295 0:59:59", 0).unwrap();
    assert_eq!(overflow.nanoseconds(), MAX_TIME_NANOS);
    assert_eq!(overflow.overflow(), Some(DurationOverflow::Positive));
    for input in [b"232 10".as_slice(), b"4294967295 0:59:59"] {
        let parsed = parse_duration(input, 0).unwrap();
        assert_eq!(parsed.nanoseconds(), MAX_TIME_NANOS);
        assert_eq!(parsed.overflow(), Some(DurationOverflow::Positive));
    }
    let negative_overflow = parse_duration(b"-232 10", 0).unwrap();
    assert_eq!(negative_overflow.nanoseconds(), MIN_TIME_NANOS);
    assert_eq!(
        negative_overflow.overflow(),
        Some(DurationOverflow::Negative)
    );

    let parsed = parse_duration(b"10:11:12.999", 2).unwrap();
    assert_eq!(
        parsed.nanoseconds(),
        10 * 3_600_000_000_000 + 11 * 60_000_000_000 + 13_000_000_000
    );

    let parsed = parse_duration(b"838:59:59", 0).unwrap();
    assert_eq!(parsed.nanoseconds(), MAX_TIME_NANOS);

    assert_eq!(
        parse_duration(b"10:61:12", 0),
        Err(DurationParseError::InvalidFormat)
    );
    assert_eq!(
        parse_duration(b"2011-11-11", 0),
        Err(DurationParseError::InvalidFormat)
    );
    assert_eq!(
        parse_duration(b"2011-11-11 00:00:01", 0),
        Err(DurationParseError::DateTimeFallback(
            DurationDateTimeFallbackKind::Separated
        ))
    );
    assert_eq!(
        parse_duration(b"20111111121212.123", 6),
        Err(DurationParseError::DateTimeFallback(
            DurationDateTimeFallbackKind::Compact14
        ))
    );
    assert_eq!(
        parse_duration(b"1234567", 0),
        Err(DurationParseError::InvalidFormat)
    );
    assert_eq!(
        parse_duration(b"0x", 0),
        Err(DurationParseError::InvalidFormat)
    );
}
