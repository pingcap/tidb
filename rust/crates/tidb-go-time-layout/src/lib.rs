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

//! Go `time.Parse`'s layout engine for clock layouts, with Go's exact error
//! text.
//!
//! TiDB returns `time.ParseInLocation`'s error verbatim from several system
//! variables (`checkTimeSystemVar` with `15:04` and `15:04 -0700`), so the
//! client sees Go's `parsing time "x" as "15:04": cannot parse "x" as "15"`.
//! This port covers the layout elements those layouts use -- `15` (hour),
//! `04` (zero-padded minute), `05` (zero-padded second) and `-0700`
//! (numeric zone) -- plus literal text, with Go's `getnum`, `skip`, range
//! and extra-text rules. Any other layout element is refused.
//!
//! It also renders `time.Time.String()`, the layout `%v` prints a time with
//! (Go errors such as `ErrTxnAbortedByGC` and `ErrSnapshotTooOld` embed it).

use std::fmt::Write as _;
use std::sync::LazyLock;

use chrono::{Datelike as _, Offset as _, TimeZone as _, Timelike as _};
use chrono_tz::OffsetName as _;

/// A clock Go parsed from a layout.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ParsedClock {
    /// The hour, `0..24`.
    pub hour: u32,
    /// The minute, `0..60`.
    pub minute: u32,
    /// The second, `0..60`.
    pub second: u32,
    /// The numeric zone offset east of UTC in seconds, when the layout had
    /// one.
    pub zone_offset: Option<i32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Std {
    Hour,
    ZeroMinute,
    ZeroSecond,
    NumTz,
}

/// Go `nextStdChunk` over the supported elements: the literal prefix, the
/// element and its layout text, and the rest of the layout.
fn next_std_chunk(layout: &str) -> Result<(&str, Option<(Std, &str)>, &str), String> {
    let bytes = layout.as_bytes();
    let start = if bytes.len() >= 16 {
        literal_layout_prefix(bytes)
    } else {
        0
    };
    for (index, &byte) in bytes.iter().enumerate().skip(start) {
        let rest = &bytes[index..];
        let (std, width) = match byte {
            b'1' if rest.get(1) == Some(&b'5') => (Std::Hour, 2),
            b'0' if rest.get(1) == Some(&b'4') => (Std::ZeroMinute, 2),
            b'0' if rest.get(1) == Some(&b'5') => (Std::ZeroSecond, 2),
            b'-' if rest.starts_with(b"-0700") => (Std::NumTz, 5),
            b'0'..=b'9' | b'J' | b'M' | b'P' | b'p' | b'Z' | b'_' | b'.' | b',' => {
                return Err(format!("unsupported Go time layout element in {layout:?}"));
            }
            _ => continue,
        };
        return Ok((
            &layout[..index],
            Some((std, &layout[index..index + width])),
            &layout[index + width..],
        ));
    }
    Ok((layout, None, ""))
}

#[inline(never)]
fn literal_layout_prefix(layout: &[u8]) -> usize {
    // These bytes either begin a supported directive or need a scalar
    // check for an unsupported one. Other bytes are ordinary literals.
    layout
        .as_chunks::<16>()
        .0
        .iter()
        .take_while(|block| {
            block.map(|byte| {
                u8::from(
                    byte.is_ascii_digit()
                        | (b','..=b'.').contains(&byte)
                        | (byte == b'J')
                        | (byte == b'M')
                        | (byte == b'P')
                        | (byte == b'p')
                        | (byte == b'Z')
                        | (byte == b'_'),
                )
            }) == [0; 16]
        })
        .count()
        * 16
}

/// Go `time.quote`.
#[must_use]
pub fn go_time_quote(text: &str) -> String {
    let mut quoted = String::with_capacity(text.len() + 2);
    push_time_quote(&mut quoted, text.as_bytes());
    quoted
}

fn push_time_quote(out: &mut String, text: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push('"');
    let mut start = 0;
    if text.len() >= 32 {
        start = plain_quote_prefix(text);
        if start != 0 {
            out.push_str(std::str::from_utf8(&text[..start]).expect("plain quote prefix is ASCII"));
        }
    }
    for &byte in &text[start..] {
        match byte {
            b'"' | b'\\' => {
                out.push('\\');
                out.push(char::from(byte));
            }
            b' '..=0x7f => out.push(char::from(byte)),
            _ => {
                out.push_str("\\x");
                out.push(char::from(HEX[usize::from(byte >> 4)]));
                out.push(char::from(HEX[usize::from(byte & 15)]));
            }
        }
    }
    out.push('"');
}

fn plain_quote_prefix(text: &[u8]) -> usize {
    // Go's quote passes DEL through as well as printable ASCII.
    text.as_chunks::<16>()
        .0
        .iter()
        .take_while(|block| {
            block.map(|byte| {
                u8::from((b' '..=0x7f).contains(&byte) & (byte != b'"') & (byte != b'\\'))
            }) == [1; 16]
        })
        .count()
        * 16
}

/// Go `ParseError.Error()`.
#[cold]
fn parse_error(layout: &str, value: &str, layout_elem: &str, value_elem: &[u8]) -> String {
    const AS: &str = " as ";
    const CANNOT_PARSE: &str = ": cannot parse ";
    let suffix_len = 2 * AS.len() + CANNOT_PARSE.len() + 6;
    let mut out = parse_error_prefix(
        value,
        suffix_len + layout.len() + layout_elem.len() + value_elem.len(),
    );
    out.push_str(AS);
    push_time_quote(&mut out, layout.as_bytes());
    out.push_str(CANNOT_PARSE);
    push_time_quote(&mut out, value_elem);
    out.push_str(AS);
    push_time_quote(&mut out, layout_elem.as_bytes());
    out
}

fn parse_error_prefix(value: &str, suffix_len: usize) -> String {
    const PREFIX: &str = "parsing time ";
    let mut out = String::with_capacity(PREFIX.len() + 2 + value.len() + suffix_len);
    out.push_str(PREFIX);
    push_time_quote(&mut out, value.as_bytes());
    out
}

/// Go `getnum`: one or two leading digits; `fixed` demands two.
fn getnum(value: &str, fixed: bool) -> Option<(u32, &str)> {
    let bytes = value.as_bytes();
    if !bytes.first().is_some_and(u8::is_ascii_digit) {
        return None;
    }
    if !bytes.get(1).is_some_and(u8::is_ascii_digit) {
        if fixed {
            return None;
        }
        return Some((u32::from(bytes[0] - b'0'), &value[1..]));
    }
    Some((
        u32::from(bytes[0] - b'0') * 10 + u32::from(bytes[1] - b'0'),
        &value[2..],
    ))
}

/// Go `cutspace`.
fn cutspace(mut text: &[u8]) -> &[u8] {
    while let Some(rest) = text.strip_prefix(&[b' '; 32]) {
        text = rest;
    }
    while let [b' ', rest @ ..] = text {
        text = rest;
    }
    text
}

/// Go `skip`: consumes `prefix` from `value`, a space matching any run of
/// spaces. On a mismatch, answers the value where matching stopped.
fn skip<'a>(value: &'a str, prefix: &str) -> Result<&'a str, &'a [u8]> {
    let mut rest = value.as_bytes();
    let mut prefix = prefix.as_bytes();
    while let Some((literal, rest_prefix)) = prefix.split_first_chunk::<16>() {
        if !rest.starts_with(literal) || literal.contains(&b' ') {
            break;
        }
        prefix = rest_prefix;
        rest = &rest[16..];
    }
    while let Some(&first) = prefix.first() {
        if first == b' ' {
            if rest.first().is_some_and(|byte| *byte != b' ') {
                return Err(rest);
            }
            prefix = cutspace(prefix);
            rest = cutspace(rest);
            continue;
        }
        if rest.first() != Some(&first) {
            return Err(rest);
        }
        prefix = &prefix[1..];
        rest = &rest[1..];
    }
    // A complete UTF-8 prefix and ASCII spaces preserve the character boundary.
    Ok(&value[value.len() - rest.len()..])
}

/// Go `time.Parse(layout, value)` for a clock layout, answering the parsed
/// fields or Go's error text.
pub fn parse_clock(layout: &str, value: &str) -> Result<ParsedClock, String> {
    let mut clock = ParsedClock::default();
    let mut remaining_layout = layout;
    let mut rest = value;
    let mut range_error: Option<&str> = None;
    loop {
        let (prefix, std, suffix) = next_std_chunk(remaining_layout)?;
        rest = skip(rest, prefix).map_err(|at| parse_error(layout, value, prefix, at))?;
        let Some((std, layout_elem)) = std else {
            break;
        };
        remaining_layout = suffix;
        let hold = rest;
        let bad = || parse_error(layout, value, layout_elem, hold.as_bytes());
        match std {
            Std::Hour => {
                let (hour, after) = getnum(rest, false).ok_or_else(bad)?;
                if hour >= 24 {
                    range_error = Some("hour");
                }
                clock.hour = hour;
                rest = after;
            }
            Std::ZeroMinute => {
                let (minute, after) = getnum(rest, true).ok_or_else(bad)?;
                if minute >= 60 {
                    range_error = Some("minute");
                }
                clock.minute = minute;
                rest = after;
            }
            Std::ZeroSecond => {
                let (second, after) = getnum(rest, true).ok_or_else(bad)?;
                if second >= 60 {
                    range_error = Some("second");
                }
                clock.second = second;
                rest = after;
                // Go consumes an optional fractional second even when the
                // layout requests only seconds. ParsedClock stores whole seconds.
                if let [b'.' | b',', b'0'..=b'9', tail @ ..] = rest.as_bytes() {
                    let digits = tail.iter().take_while(|byte| byte.is_ascii_digit()).count();
                    rest = &rest[2 + digits..];
                }
            }
            Std::NumTz => {
                let &[sign, h1, h2, m1, m2, ..] = rest.as_bytes() else {
                    return Err(bad());
                };
                let hours = decimal_pair(h1, h2).ok_or_else(bad)?;
                let minutes = decimal_pair(m1, m2);
                // Go reports range errors before errors in the sign or minutes.
                if hours > 24 {
                    range_error = Some("time zone offset hour");
                }
                if minutes.is_some_and(|minutes| minutes > 60) {
                    range_error = Some("time zone offset minute");
                }
                if range_error.is_none() {
                    let minutes = minutes.ok_or_else(bad)?;
                    let offset = (hours * 60 + minutes) * 60;
                    clock.zone_offset = Some(match sign {
                        b'+' => offset,
                        b'-' => -offset,
                        _ => return Err(bad()),
                    });
                    rest = &rest[5..];
                }
            }
        }
        if let Some(element) = range_error {
            const OUT_OF_RANGE: &str = " out of range";
            let mut out = parse_error_prefix(value, 2 + element.len() + OUT_OF_RANGE.len());
            out.push_str(": ");
            out.push_str(element);
            out.push_str(OUT_OF_RANGE);
            return Err(out);
        }
    }
    if !rest.is_empty() {
        const EXTRA_TEXT: &str = ": extra text: ";
        let mut out = parse_error_prefix(value, EXTRA_TEXT.len() + rest.len() + 2);
        out.push_str(EXTRA_TEXT);
        push_time_quote(&mut out, rest.as_bytes());
        return Err(out);
    }
    Ok(clock)
}

#[inline]
fn decimal_pair(tens: u8, ones: u8) -> Option<i32> {
    let tens = tens.wrapping_sub(b'0');
    let ones = ones.wrapping_sub(b'0');
    (tens <= 9 && ones <= 9).then_some(i32::from(tens) * 10 + i32::from(ones))
}

/// Go `time.Time.String()` of a time in `location` without a monotonic
/// reading: the layout `2006-01-02 15:04:05.999999999 -0700 MST`, the
/// fraction without trailing zeros and the zone named by its abbreviation
/// (`PST`, or a numeric `-03` for a zone the database names that way).
#[must_use]
pub fn go_time_string_in(time: chrono::DateTime<chrono_tz::Tz>) -> String {
    let offset = time.offset();
    let mut out = format_time(time.naive_utc(), offset.fix());
    out.push(' ');
    if let Some(name) = offset.abbreviation() {
        out.push_str(name);
    } else {
        // chrono-tz renders numeric names such as "-02" through Display.
        write!(out, "{offset}").expect("writing to a String succeeds");
    }
    out
}

// Like Go's time.Local, resolve the process zone once, on first use.
static LOCAL_ZONE: LazyLock<Option<chrono_tz::Tz>> =
    LazyLock::new(|| tidb_timeutil::infer_system_tz().parse().ok());

/// [`go_time_string_in`] in the process's local zone, which is what Go's
/// `%v` of a `time.Local` value prints (`oracle.GetTimeFromTS`,
/// `time.Now()`).
#[must_use]
pub fn go_time_string(time: chrono::DateTime<chrono::Local>) -> String {
    if let Some(zone) = *LOCAL_ZONE {
        return go_time_string_in(time.with_timezone(&zone));
    }
    // Go repeats the numeric offset when the zone is unnamed.
    let offset = *time.offset();
    let mut out = format_time(time.naive_utc(), offset);
    out.push(' ');
    push_offset(&mut out, offset);
    out
}

/// Go `time.UnixMilli(millis).String()`: a Unix millisecond instant as a
/// `time.Local` value (`oracle.GetTimeFromTS`, `model.TSConvert2Time`).
#[must_use]
pub fn go_unix_milli_string(millis: i64) -> String {
    if let Some(zone) = *LOCAL_ZONE {
        let time = zone
            .timestamp_millis_opt(millis)
            .single()
            .expect("Unix milliseconds fit Chrono");
        return go_time_string_in(time);
    }
    let time = chrono::Local
        .timestamp_millis_opt(millis)
        .single()
        .expect("Unix milliseconds fit Chrono");
    go_time_string(time)
}

fn format_time(utc: chrono::NaiveDateTime, offset: chrono::FixedOffset) -> String {
    let mut out = String::with_capacity(40);
    if let Some(local) = utc.checked_add_offset(offset) {
        push_year(&mut out, local.year());
        out.push('-');
        push_two_digits(&mut out, local.month());
        out.push('-');
        push_two_digits(&mut out, local.day());
        out.push(' ');
        push_two_digits(&mut out, local.hour());
        out.push(':');
        push_two_digits(&mut out, local.minute());
        out.push(':');
        let second = local.second() + local.nanosecond() / 1_000_000_000;
        push_two_digits(&mut out, second);
    } else {
        // Chrono can display a local date outside the NaiveDateTime range.
        let time = chrono::DateTime::<chrono::FixedOffset>::from_naive_utc_and_offset(utc, offset);
        push_year(&mut out, time.year());
        write!(out, "{}", time.format("-%m-%d %H:%M:%S")).expect("writing to a String succeeds");
    }
    let nanos = utc.nanosecond();
    if nanos != 0 {
        write!(out, ".{nanos:09}").expect("writing to a String succeeds");
        out.truncate(out.trim_end_matches('0').len());
    }
    out.push(' ');
    push_offset(&mut out, offset);
    out
}

fn push_year(out: &mut String, year: i32) {
    if year < 0 {
        out.push('-');
    }
    let year = year.unsigned_abs();
    if year < 10_000 {
        push_two_digits(out, year / 100);
        push_two_digits(out, year % 100);
    } else {
        write!(out, "{year}").expect("writing to a String succeeds");
    }
}

fn push_offset(out: &mut String, offset: chrono::FixedOffset) {
    // Go truncates historical offsets with a sub-minute component.
    let minutes = offset.local_minus_utc() / 60;
    out.push(if minutes < 0 { '-' } else { '+' });
    let minutes = minutes.unsigned_abs();
    push_two_digits(out, minutes / 60);
    push_two_digits(out, minutes % 60);
}

fn push_two_digits(out: &mut String, value: u32) {
    out.push(char::from(b'0' + (value / 10) as u8));
    out.push(char::from(b'0' + (value % 10) as u8));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `time.Time.String()`: the same instants Go's 9006 error printed on
    /// a `America/Los_Angeles` host, in standard and in daylight time.
    #[test]
    fn time_string_matches_go_in_a_named_zone() {
        use chrono::TimeZone as _;
        let zone = chrono_tz::America::Los_Angeles;
        assert_eq!(
            go_time_string_in(zone.timestamp_millis_opt(946_713_600_000).unwrap()),
            "2000-01-01 00:00:00 -0800 PST"
        );
        assert_eq!(
            go_time_string_in(zone.timestamp_millis_opt(1_792_512_051_740).unwrap()),
            "2026-10-20 09:00:51.74 -0700 PDT"
        );
        assert_eq!(
            go_time_string_in(
                chrono_tz::UTC
                    .timestamp_millis_opt(946_684_800_001)
                    .unwrap()
            ),
            "2000-01-01 00:00:00.001 +0000 UTC"
        );
        assert_eq!(
            go_time_string_in(
                chrono_tz::America::Sao_Paulo
                    .timestamp_millis_opt(946_684_800_000)
                    .unwrap()
            ),
            "1999-12-31 22:00:00 -0200 -02"
        );
    }

    #[test]
    fn time_string_uses_go_year_and_offset_formatting() {
        use chrono::TimeZone as _;
        let dates = [
            (
                chrono_tz::UTC,
                253_402_300_800,
                "10000-01-01 00:00:00.000000001 +0000 UTC",
            ),
            (
                chrono_tz::UTC,
                -62_198_755_200,
                "-0001-01-01 00:00:00.000000001 +0000 UTC",
            ),
            (
                chrono_tz::America::Los_Angeles,
                -2_840_140_800,
                "1879-12-31 16:07:02.000000001 -0752 LMT",
            ),
        ];
        for (zone, seconds, expected) in dates {
            let time = zone.timestamp_opt(seconds, 1).unwrap();
            assert_eq!(go_time_string_in(time), expected);
        }
    }

    /// Errors captured from Go on this branch through
    /// `SET GLOBAL tidb_auto_analyze_start_time = ...`, plus Go's own rules.
    #[test]
    fn clock_layouts_parse_and_fail_like_go() {
        assert_eq!(
            parse_clock("15:04", "abc").unwrap_err(),
            r#"parsing time "abc" as "15:04": cannot parse "abc" as "15""#
        );
        assert_eq!(
            parse_clock("15:04", "1:5").unwrap_err(),
            r#"parsing time "1:5" as "15:04": cannot parse "5" as "04""#
        );
        assert_eq!(
            parse_clock("15:04", "1:05").unwrap(),
            ParsedClock {
                hour: 1,
                minute: 5,
                ..ParsedClock::default()
            }
        );
        assert_eq!(
            parse_clock("15:04", "24:00").unwrap_err(),
            r#"parsing time "24:00": hour out of range"#
        );
        assert_eq!(
            parse_clock("15:04", "1:05x").unwrap_err(),
            r#"parsing time "1:05x": extra text: "x""#
        );
        assert_eq!(
            parse_clock("15:04 -0700", "16:11 +0800").unwrap(),
            ParsedClock {
                hour: 16,
                minute: 11,
                second: 0,
                zone_offset: Some(8 * 3600),
            }
        );
        assert_eq!(
            parse_clock("15:04 -0700", "16:11 0800").unwrap_err(),
            r#"parsing time "16:11 0800" as "15:04 -0700": cannot parse "0800" as "-0700""#
        );
    }

    #[test]
    fn clock_layouts_handle_non_ascii_like_go() {
        assert_eq!(
            parse_clock("時15:04", "時1:05").unwrap(),
            ParsedClock {
                hour: 1,
                minute: 5,
                ..ParsedClock::default()
            }
        );
        assert_eq!(
            parse_clock("é15:04", "ê1:05").unwrap_err(),
            r#"parsing time "\xc3\xaa1:05" as "\xc3\xa915:04": cannot parse "\xaa1:05" as "\xc3\xa9""#,
        );
        assert_eq!(
            parse_clock("15:04 -0700", "12:30 é123").unwrap_err(),
            r#"parsing time "12:30 \xc3\xa9123" as "15:04 -0700": cannot parse "\xc3\xa9123" as "-0700""#,
        );
    }

    #[test]
    fn clock_consumes_implicit_fractional_seconds() {
        assert_eq!(
            parse_clock("15:04:05 -0700", "1:05:09,123456789012 +0800").unwrap(),
            ParsedClock {
                hour: 1,
                minute: 5,
                second: 9,
                zone_offset: Some(8 * 3600),
            }
        );
        assert_eq!(
            parse_clock("15h04m05s", "12h34m56.009s").unwrap().second,
            56
        );
        assert_eq!(
            parse_clock("15:04:05", "12:34:56.").unwrap_err(),
            r#"parsing time "12:34:56.": extra text: ".""#,
        );
    }

    #[test]
    fn clock_zone_ranges_take_precedence_over_syntax_errors() {
        assert_eq!(
            parse_clock("15:04 -0700", "12:34 +25\t61").unwrap_err(),
            r#"parsing time "12:34 +25\x0961": time zone offset hour out of range"#,
        );
        assert_eq!(
            parse_clock("15:04 -0700", "12:34 ?2561").unwrap_err(),
            r#"parsing time "12:34 ?2561": time zone offset minute out of range"#,
        );
    }
}
