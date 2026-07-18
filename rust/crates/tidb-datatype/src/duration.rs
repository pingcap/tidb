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

//! MySQL `TIME` duration range policy from `pkg/types/time.go`.

use std::{error::Error, fmt};

use crate::{check_fsp, parse_frac, FspError};

/// The maximum SQL `TIME` hour component accepted by TiDB.
pub const TIME_MAX_HOUR: i64 = 838;
/// The maximum SQL `TIME` minute component accepted by TiDB.
pub const TIME_MAX_MINUTE: i64 = 59;
/// The maximum SQL `TIME` second component accepted by TiDB.
pub const TIME_MAX_SECOND: i64 = 59;
/// The largest representable MySQL duration in nanoseconds.
pub const MAX_TIME_NANOS: i64 =
    (TIME_MAX_HOUR * 60 * 60 + TIME_MAX_MINUTE * 60 + TIME_MAX_SECOND) * 1_000_000_000;
/// The smallest representable MySQL duration in nanoseconds.
pub const MIN_TIME_NANOS: i64 = -MAX_TIME_NANOS;

/// A duration value after source `RoundFrac` normalization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoundedDuration {
    nanoseconds: i64,
    fsp: i64,
}

/// A successfully parsed MySQL duration literal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParsedDuration {
    nanoseconds: i64,
    fsp: i64,
    overflow: Option<DurationOverflow>,
}

impl ParsedDuration {
    /// Returns the signed nanosecond value after FSP rounding and range clamp.
    pub const fn nanoseconds(self) -> i64 {
        self.nanoseconds
    }

    /// Returns the normalized fractional-seconds precision.
    pub const fn fsp(self) -> i64 {
        self.fsp
    }

    /// Returns the range overflow direction, if the source would warn/error.
    pub const fn overflow(self) -> Option<DurationOverflow> {
        self.overflow
    }

    /// Returns the pure source-side event for this parsed duration.
    pub const fn event(self) -> Option<DurationParseEvent> {
        match self.overflow {
            Some(direction) => Some(DurationParseEvent::Overflow(direction)),
            None => None,
        }
    }
}

impl RoundedDuration {
    /// Returns the rounded signed nanosecond count.
    pub const fn nanoseconds(self) -> i64 {
        self.nanoseconds
    }

    /// Returns the normalized fractional-seconds precision.
    pub const fn fsp(self) -> i64 {
        self.fsp
    }
}

/// Direction of a source `ErrTruncatedWrongVal` duration clamp.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurationOverflow {
    /// The input was above [`MAX_TIME_NANOS`].
    Positive,
    /// The input was below [`MIN_TIME_NANOS`].
    Negative,
}

/// An error from source-compatible duration FSP rounding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurationRoundError {
    /// The target precision follows `CheckFsp`'s invalid-negative path.
    InvalidFsp(FspError),
    /// The rounded nanosecond value does not fit an `i64` duration.
    Overflow,
}

impl fmt::Display for DurationRoundError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFsp(error) => error.fmt(formatter),
            Self::Overflow => formatter.write_str("rounded duration is out of range"),
        }
    }
}

impl Error for DurationRoundError {}

/// An error or routing signal produced while parsing a duration literal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurationParseError {
    /// The requested target FSP is invalid.
    InvalidFsp(FspError),
    /// The duration grammar rejected an input that the source routes to its
    /// datetime parser. The calendar conversion and session policy belong to
    /// a higher layer; this typed signal preserves that routing decision.
    DateTimeFallback(DurationDateTimeFallbackKind),
    /// The literal does not match the dependency-closed duration grammar.
    InvalidFormat,
    /// A numeric component does not fit the parser's unsigned accumulator.
    NumericOverflow,
    /// The fractional byte parser rejected its input.
    Fraction(FspError),
}

impl fmt::Display for DurationParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFsp(error) | Self::Fraction(error) => error.fmt(formatter),
            Self::DateTimeFallback(_) => {
                formatter.write_str("duration literal requires datetime fallback")
            }
            Self::InvalidFormat => formatter.write_str("invalid duration format"),
            Self::NumericOverflow => formatter.write_str("duration component is out of range"),
        }
    }
}

impl Error for DurationParseError {}

/// Shape selected by Go `canFallbackToDateTime` after duration parsing fails.
///
/// This enum intentionally carries no calendar values. A higher-level parser
/// must perform the actual date/datetime conversion and attach SQL warning or
/// session context policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurationDateTimeFallbackKind {
    /// A contiguous twelve-digit datetime literal.
    Compact12,
    /// A contiguous fourteen-digit datetime literal.
    Compact14,
    /// Three digit fields separated by source punctuation, followed by a
    /// space or `T` time separator.
    Separated,
}

/// Pure source-side event classification for duration parsing and range
/// handling.
///
/// These events intentionally contain no warning text, SQL mode, or session
/// mutation. The owning session/executor layer decides whether an event is a
/// warning, statement error, or fallback route.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurationParseEvent {
    /// The duration was clamped to a MySQL `TIME` endpoint.
    Overflow(DurationOverflow),
    /// The source selected datetime parsing after duration parsing failed.
    DateTimeFallback(DurationDateTimeFallbackKind),
    /// The source returned `ErrTruncatedWrongVal` for malformed/trailing
    /// duration input.
    Truncated,
}

impl DurationParseError {
    /// Returns the source-side event represented by this parse result.
    ///
    /// Invalid FSP is a direct parameter error, not an
    /// `ErrTruncatedWrongVal` warning. Other parse-shape and fraction errors
    /// follow the source's truncation branch.
    pub const fn event(&self) -> Option<DurationParseEvent> {
        match self {
            Self::InvalidFsp(_) => None,
            Self::DateTimeFallback(kind) => Some(DurationParseEvent::DateTimeFallback(*kind)),
            Self::InvalidFormat | Self::NumericOverflow | Self::Fraction(_) => {
                Some(DurationParseEvent::Truncated)
            }
        }
    }
}

/// Result of Go `TruncateOverflowMySQLTime`'s clamp operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurationRangeResult {
    value: i64,
    overflow: Option<DurationOverflow>,
}

impl DurationRangeResult {
    /// Returns the clamped or unchanged duration in nanoseconds.
    pub const fn value(self) -> i64 {
        self.value
    }

    /// Returns the overflow direction, if the source would return an error.
    pub const fn overflow(self) -> Option<DurationOverflow> {
        self.overflow
    }

    /// Returns the pure source-side event for this range result.
    pub const fn event(self) -> Option<DurationParseEvent> {
        match self.overflow {
            Some(direction) => Some(DurationParseEvent::Overflow(direction)),
            None => None,
        }
    }

    const fn unchanged(value: i64) -> Self {
        Self {
            value,
            overflow: None,
        }
    }
}

/// Clamps a duration to TiDB's MySQL `TIME` range.
///
/// This is the value/error part of Go `TruncateOverflowMySQLTime`: callers
/// receive the endpoint value together with an explicit overflow direction.
/// Turning that direction into a warning or statement error remains outside
/// this dependency-leaf API.
pub const fn truncate_overflow_mysql_time(value: i64) -> DurationRangeResult {
    if value > MAX_TIME_NANOS {
        return DurationRangeResult {
            value: MAX_TIME_NANOS,
            overflow: Some(DurationOverflow::Positive),
        };
    }
    if value < MIN_TIME_NANOS {
        return DurationRangeResult {
            value: MIN_TIME_NANOS,
            overflow: Some(DurationOverflow::Negative),
        };
    }
    DurationRangeResult::unchanged(value)
}

/// Parses the source's dependency-closed `[-]HH:MM[:SS][.fraction]` and
/// compact `[-]HHMMSS[.fraction]` grammars.
///
/// A day prefix (`D HH[:MM[:SS]]`) is also accepted because Go's
/// `matchDayHHMMSS` feeds it through the same duration path. Date/datetime
/// calendar conversion, statement warnings, and session context are
/// deliberately not included. Date-shaped input returns a typed
/// [`DurationParseError::DateTimeFallback`] signal. Values beyond MySQL's
/// `TIME` range are clamped and exposed via [`ParsedDuration::overflow`],
/// matching the source value/error split; callers can consume
/// [`ParsedDuration::event`] without importing SQL warning policy.
pub fn parse_duration(input: &[u8], target_fsp: i64) -> Result<ParsedDuration, DurationParseError> {
    let fsp = check_fsp(target_fsp).map_err(DurationParseError::InvalidFsp)?;
    let input = trim_ascii_space(input);
    if input.is_empty() {
        return Err(DurationParseError::InvalidFormat);
    }
    if let Some(kind) = classify_duration_datetime_fallback(input) {
        return Err(DurationParseError::DateTimeFallback(kind));
    }
    let mut index = 0;
    let negative = input.first() == Some(&b'-');
    if negative {
        index += 1;
        skip_ascii_space(input, &mut index);
    }

    let first = parse_duration_number(input, &mut index)?;
    let before_space = index;
    skip_ascii_space(input, &mut index);
    let day_form =
        index != before_space && input.get(index).is_some_and(|byte| byte.is_ascii_digit());
    let (mut hours, mut minutes, mut seconds) = if day_form {
        let day = first;
        let hour = parse_duration_number(input, &mut index)?;
        let hours = day
            .checked_mul(24)
            .and_then(|value| value.checked_add(hour))
            .ok_or(DurationParseError::NumericOverflow)?;
        (hours, 0, 0)
    } else {
        (first, 0, 0)
    };

    if consume_duration_colon(input, &mut index) {
        minutes = parse_duration_number(input, &mut index)?;
        if consume_duration_colon(input, &mut index) {
            seconds = parse_duration_number(input, &mut index)?;
        }
    } else if !day_form {
        // Source `matchHHMMSSCompact` derives HH/MM/SS from the complete
        // numeric token, preserving short forms such as `1`, `12`, and `112`.
        hours = first / 10_000;
        minutes = (first / 100) % 100;
        seconds = first % 100;
    }
    if minutes > 59 || seconds > 59 {
        return Err(DurationParseError::InvalidFormat);
    }

    let mut microseconds = 0_i64;
    if input.get(index) == Some(&b'.') {
        index += 1;
        let start = index;
        while input.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if start == index {
            return Err(DurationParseError::InvalidFormat);
        }
        let (fraction, overflow) =
            parse_frac(&input[start..index], fsp).map_err(DurationParseError::Fraction)?;
        microseconds = fraction;
        if overflow {
            seconds = seconds
                .checked_add(1)
                .ok_or(DurationParseError::NumericOverflow)?;
            if seconds == 60 {
                seconds = 0;
                minutes = minutes
                    .checked_add(1)
                    .ok_or(DurationParseError::NumericOverflow)?;
                if minutes == 60 {
                    minutes = 0;
                    // Carrying into hours is source `hhmmssAddOverflow`.
                    hours = hours
                        .checked_add(1)
                        .ok_or(DurationParseError::NumericOverflow)?;
                }
            }
        }
    }
    skip_ascii_space(input, &mut index);
    if index != input.len() {
        return Err(DurationParseError::InvalidFormat);
    }
    parsed_duration_from_parts(negative, hours, minutes, seconds, microseconds, fsp)
}

/// Classifies the exact shape accepted by Go `canFallbackToDateTime`.
///
/// The input must already have the outer whitespace removed, as it is at the
/// call site in Go `ParseDuration`. The source parser treats each byte as a
/// Unicode code point; for the 0..255 range that means ASCII digits and the
/// Latin-1 punctuation code points listed by [`is_source_punctuation`].
pub fn classify_duration_datetime_fallback(input: &[u8]) -> Option<DurationDateTimeFallbackKind> {
    let first_len = source_digit_prefix(input);
    if first_len == 0 {
        return None;
    }
    match first_len {
        12 => return Some(DurationDateTimeFallbackKind::Compact12),
        14 => return Some(DurationDateTimeFallbackKind::Compact14),
        _ => {}
    }

    let mut index = first_len;
    if !consume_source_punctuation(input, &mut index) {
        return None;
    }
    let second_len = source_digit_prefix(&input[index..]);
    if second_len == 0 {
        return None;
    }
    index += second_len;
    if !consume_source_punctuation(input, &mut index) {
        return None;
    }
    let third_len = source_digit_prefix(&input[index..]);
    if third_len == 0 {
        return None;
    }
    index += third_len;
    match input.get(index) {
        Some(b' ' | b'T') => Some(DurationDateTimeFallbackKind::Separated),
        _ => None,
    }
}

/// Boolean form of [`classify_duration_datetime_fallback`] for callers that
/// only need the source `canFallbackToDateTime` predicate.
pub fn can_fallback_to_datetime(input: &[u8]) -> bool {
    classify_duration_datetime_fallback(input).is_some()
}

fn source_digit_prefix(input: &[u8]) -> usize {
    input
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count()
}

fn consume_source_punctuation(input: &[u8], index: &mut usize) -> bool {
    if input
        .get(*index)
        .is_some_and(|byte| is_source_punctuation(*byte))
    {
        *index += 1;
        true
    } else {
        false
    }
}

fn is_source_punctuation(byte: u8) -> bool {
    matches!(
        byte,
        b'!' | b'"'
            | b'#'
            | b'%'
            | b'&'
            | b'\''
            | b'('
            | b')'
            | b'*'
            | b','
            | b'-'
            | b'.'
            | b'/'
            | b':'
            | b';'
            | b'?'
            | b'@'
            | b'['
            | b'\\'
            | b']'
            | b'_'
            | b'{'
            | b'}'
            | 0xA1
            | 0xA7
            | 0xAB
            | 0xB6
            | 0xB7
            | 0xBB
            | 0xBF
    )
}

fn parsed_duration_from_parts(
    negative: bool,
    hours: u64,
    minutes: u64,
    seconds: u64,
    microseconds: i64,
    fsp: i64,
) -> Result<ParsedDuration, DurationParseError> {
    let magnitude = i128::from(hours) * 3_600 * 1_000_000_000
        + i128::from(minutes) * 60 * 1_000_000_000
        + i128::from(seconds) * 1_000_000_000
        + i128::from(microseconds) * 1_000;
    let signed = if negative { -magnitude } else { magnitude };
    let range = if signed > i128::from(i64::MAX) {
        DurationRangeResult {
            value: MAX_TIME_NANOS,
            overflow: Some(DurationOverflow::Positive),
        }
    } else if signed < i128::from(i64::MIN) {
        DurationRangeResult {
            value: MIN_TIME_NANOS,
            overflow: Some(DurationOverflow::Negative),
        }
    } else {
        truncate_overflow_mysql_time(signed as i64)
    };
    Ok(ParsedDuration {
        nanoseconds: range.value,
        fsp,
        overflow: range.overflow,
    })
}

fn parse_duration_number(input: &[u8], index: &mut usize) -> Result<u64, DurationParseError> {
    let start = *index;
    let mut value = 0_u64;
    while let Some(byte) = input.get(*index).copied() {
        if !byte.is_ascii_digit() {
            break;
        }
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u64::from(byte - b'0')))
            .ok_or(DurationParseError::NumericOverflow)?;
        *index += 1;
    }
    if *index == start {
        return Err(DurationParseError::InvalidFormat);
    }
    Ok(value)
}

fn consume_duration_colon(input: &[u8], index: &mut usize) -> bool {
    skip_ascii_space(input, index);
    if input.get(*index) != Some(&b':') {
        return false;
    }
    *index += 1;
    skip_ascii_space(input, index);
    true
}

fn skip_ascii_space(input: &[u8], index: &mut usize) {
    while input.get(*index).is_some_and(u8::is_ascii_whitespace) {
        *index += 1;
    }
}

fn trim_ascii_space(input: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = input.len();
    while input.get(start).is_some_and(u8::is_ascii_whitespace) {
        start += 1;
    }
    while end > start && input.get(end - 1).is_some_and(u8::is_ascii_whitespace) {
        end -= 1;
    }
    &input[start..end]
}

/// Rounds duration nanoseconds using Go `Duration.RoundFrac`'s half-away-from
/// zero rule and returns normalized FSP metadata.
///
/// The target FSP is normalized first, then compared with `current_fsp`,
/// matching Go's early return. A target FSP above six is clamped by
/// [`check_fsp`]; invalid negative values are returned as a typed error. The
/// function does not apply MySQL range clamping or statement warning policy.
pub fn round_duration_fsp(
    nanoseconds: i64,
    current_fsp: i64,
    target_fsp: i64,
) -> Result<RoundedDuration, DurationRoundError> {
    let fsp = check_fsp(target_fsp).map_err(DurationRoundError::InvalidFsp)?;
    if current_fsp == fsp {
        return Ok(RoundedDuration { nanoseconds, fsp });
    }
    let unit = 10_i128.pow((9 - fsp) as u32);
    let half = unit / 2;
    let value = i128::from(nanoseconds);
    let rounded = if value >= 0 {
        (value + half) / unit * unit
    } else {
        (value - half) / unit * unit
    };
    let nanoseconds = i64::try_from(rounded).map_err(|_| DurationRoundError::Overflow)?;
    Ok(RoundedDuration { nanoseconds, fsp })
}
