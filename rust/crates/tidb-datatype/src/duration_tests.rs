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
    round_duration_fsp, DurationDateTimeFallbackKind, DurationParseError, DurationParseEvent,
    DurationRoundError,
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
    // valid rows in pkg/types/time_test.go::TestTime. Date/datetime fallback
    // and warning/session attachment are intentionally outside this parser.
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
