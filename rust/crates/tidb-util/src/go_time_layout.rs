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
    for index in 0..bytes.len() {
        let rest = &layout[index..];
        let found = if rest.starts_with("15") {
            Some((Std::Hour, 2))
        } else if rest.starts_with("04") {
            Some((Std::ZeroMinute, 2))
        } else if rest.starts_with("05") {
            Some((Std::ZeroSecond, 2))
        } else if rest.starts_with("-0700") {
            Some((Std::NumTz, 5))
        } else if bytes[index].is_ascii_digit()
            || matches!(
                bytes[index],
                b'J' | b'M' | b'P' | b'p' | b'Z' | b'_' | b'.' | b','
            )
        {
            return Err(format!("unsupported Go time layout element in {layout:?}"));
        } else {
            None
        };
        if let Some((std, width)) = found {
            return Ok((
                &layout[..index],
                Some((std, &layout[index..index + width])),
                &layout[index + width..],
            ));
        }
    }
    Ok((layout, None, ""))
}

/// Go `time.quote`.
#[must_use]
pub fn go_time_quote(text: &str) -> String {
    let mut quoted = String::with_capacity(text.len() + 2);
    quoted.push('"');
    for (index, character) in text.char_indices() {
        if !character.is_ascii() || character < ' ' {
            for byte in &text.as_bytes()[index..index + character.len_utf8()] {
                quoted.push_str(&format!("\\x{byte:02x}"));
            }
        } else {
            if character == '"' || character == '\\' {
                quoted.push('\\');
            }
            quoted.push(character);
        }
    }
    quoted.push('"');
    quoted
}

/// Go `ParseError.Error()`.
fn parse_error(
    layout: &str,
    value: &str,
    layout_elem: &str,
    value_elem: &str,
    message: &str,
) -> String {
    if message.is_empty() {
        format!(
            "parsing time {} as {}: cannot parse {} as {}",
            go_time_quote(value),
            go_time_quote(layout),
            go_time_quote(value_elem),
            go_time_quote(layout_elem)
        )
    } else {
        format!("parsing time {}{message}", go_time_quote(value))
    }
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
fn cutspace(text: &str) -> &str {
    text.trim_start_matches(' ')
}

/// Go `skip`: consumes `prefix` from `value`, a space matching any run of
/// spaces. On a mismatch, answers the value where matching stopped.
fn skip<'a>(mut value: &'a str, mut prefix: &str) -> Result<&'a str, &'a str> {
    while let Some(first) = prefix.as_bytes().first() {
        if *first == b' ' {
            if value.as_bytes().first().is_some_and(|byte| *byte != b' ') {
                return Err(value);
            }
            prefix = cutspace(prefix);
            value = cutspace(value);
            continue;
        }
        if value.as_bytes().first() != Some(first) {
            return Err(value);
        }
        prefix = &prefix[1..];
        value = &value[1..];
    }
    Ok(value)
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
        rest = skip(rest, prefix).map_err(|at| parse_error(layout, value, prefix, at, ""))?;
        let Some((std, layout_elem)) = std else {
            break;
        };
        remaining_layout = suffix;
        let hold = rest;
        let bad = || parse_error(layout, value, layout_elem, hold, "");
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
            }
            Std::NumTz => {
                if rest.len() < 5 || !rest.is_char_boundary(5) {
                    return Err(bad());
                }
                let (sign, hour, minute) = (&rest[..1], &rest[1..3], &rest[3..5]);
                let (hours, _) = getnum(hour, true).ok_or_else(bad)?;
                let (minutes, _) = getnum(minute, true).ok_or_else(bad)?;
                // Go tests `>`, not `>=`, for offsets.
                if hours > 24 {
                    range_error = Some("time zone offset hour");
                }
                if minutes > 60 {
                    range_error = Some("time zone offset minute");
                }
                let offset = i32::try_from((hours * 60 + minutes) * 60).unwrap_or(i32::MAX);
                clock.zone_offset = Some(match sign {
                    "+" => offset,
                    "-" => -offset,
                    _ => return Err(bad()),
                });
                rest = &rest[5..];
            }
        }
        if let Some(element) = range_error {
            return Err(parse_error(
                layout,
                value,
                layout_elem,
                rest,
                &format!(": {element} out of range"),
            ));
        }
    }
    if !rest.is_empty() {
        return Err(parse_error(
            layout,
            value,
            "",
            rest,
            &format!(": extra text: {}", go_time_quote(rest)),
        ));
    }
    Ok(clock)
}

/// Go `time.Time.String()` of a time in `location` without a monotonic
/// reading: the layout `2006-01-02 15:04:05.999999999 -0700 MST`, the
/// fraction without trailing zeros and the zone named by its abbreviation
/// (`PST`, or a numeric `-03` for a zone the database names that way).
#[must_use]
pub fn go_time_string_in(time: chrono::DateTime<chrono_tz::Tz>) -> String {
    format!(
        "{}{} {}",
        time.format("%Y-%m-%d %H:%M:%S"),
        go_time_fraction(time.timestamp_subsec_nanos()),
        time.format("%z %Z")
    )
}

/// [`go_time_string_in`] in the process's local zone, which is what Go's
/// `%v` of a `time.Local` value prints (`oracle.GetTimeFromTS`,
/// `time.Now()`).
#[must_use]
pub fn go_time_string(time: chrono::DateTime<chrono::Local>) -> String {
    match crate::timeutil::infer_system_tz().parse::<chrono_tz::Tz>() {
        Ok(zone) => go_time_string_in(time.with_timezone(&zone)),
        // A zone the database cannot name keeps its numeric offset in the
        // name position, as Go does for an unnamed zone.
        Err(_) => format!(
            "{}{} {} {}",
            time.format("%Y-%m-%d %H:%M:%S"),
            go_time_fraction(time.timestamp_subsec_nanos()),
            time.format("%z"),
            time.format("%z")
        ),
    }
}

/// Go `time.UnixMilli(millis).String()`: a Unix millisecond instant as a
/// `time.Local` value (`oracle.GetTimeFromTS`, `model.TSConvert2Time`).
#[must_use]
pub fn go_unix_milli_string(millis: i64) -> String {
    use chrono::TimeZone as _;
    let time = chrono::Local
        .timestamp_millis_opt(millis)
        .single()
        .expect("Unix milliseconds fit Chrono");
    go_time_string(time)
}

/// Go's `.999999999` layout element: the nanoseconds with trailing zeros
/// removed, nothing for a whole second.
fn go_time_fraction(nanos: u32) -> String {
    let fraction = format!("{nanos:09}");
    let fraction = fraction.trim_end_matches('0');
    if fraction.is_empty() {
        String::new()
    } else {
        format!(".{fraction}")
    }
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
}
