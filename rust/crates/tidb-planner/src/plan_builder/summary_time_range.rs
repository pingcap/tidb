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

//! Go `PlanBuilder.timeRangeForSummaryTable` (`logical_plan_builder.go`):
//! the window the summary memory tables (`INSPECTION_RESULT`,
//! `INSPECTION_SUMMARY`, `METRICS_SUMMARY[_BY_LABEL]`) query, from the
//! `TIME_RANGE` hint or the last 30 minutes.
//!
//! The window's bounds are kept as Go's `util.MetricTableTimeFormat` text in
//! the process's local zone, which is what Go's `QueryTimeRange.Condition`
//! renders them as.

use chrono::{DateTime, Duration, Local, NaiveDate, NaiveDateTime, TimeZone, Timelike};

use tidb_exprctx::Columns;

use tidb_infoschema::table_source::TableSource;
use super::PlanBuilder;
use crate::logical::mem_table::QueryTimeRange;

/// Go `util.MetricTableTimeFormat`.
const METRIC_TABLE_TIME_FORMAT: &str = "2006-01-02 15:04:05.999";

/// Go's `defaultSummaryDuration`.
const DEFAULT_SUMMARY_DURATION_MINUTES: i64 = 30;

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `timeRangeForSummaryTable`.
    pub(crate) fn time_range_for_summary_table(&mut self) -> QueryTimeRange {
        let default_duration = Duration::minutes(DEFAULT_SUMMARY_DURATION_MINUTES);
        let hint = self
            .plan_hints
            .as_ref()
            .and_then(|hints| hints.borrow().time_range.clone())
            .filter(|(from, to)| !from.is_empty() || !to.is_empty());
        let Some((from_hint, to_hint)) = hint else {
            let to = Local::now();
            return query_time_range(to - default_duration, to);
        };
        let parse = |text: &str| match parse_in_local(text) {
            Ok(time) => Some(time),
            Err(message) => {
                self.ctx.append_warning(1105, &message);
                None
            }
        };
        let from = parse(&from_hint);
        let to = parse(&to_hint);
        let (from, to) = match (from, to) {
            (Some(from), Some(to)) => (from, to),
            (Some(from), None) => (from, from + default_duration),
            (None, Some(to)) => (to - default_duration, to),
            (None, None) => {
                let to = Local::now();
                (to - default_duration, to)
            }
        };
        query_time_range(from, to)
    }
}

fn query_time_range(from: DateTime<Local>, to: DateTime<Local>) -> QueryTimeRange {
    QueryTimeRange {
        from: format_metric_table_time(from),
        to: format_metric_table_time(to),
    }
}

/// Go `t.Format(util.MetricTableTimeFormat)`: up to three fractional digits,
/// truncated, with trailing zeros and a bare dot dropped.
fn format_metric_table_time(time: DateTime<Local>) -> String {
    let mut text = time.format("%Y-%m-%d %H:%M:%S").to_string();
    let millis = time.nanosecond() / 1_000_000 % 1000;
    if millis > 0 {
        text.push('.');
        text.push_str(format!("{millis:03}").trim_end_matches('0'));
    }
    text
}

/// Go `time.ParseInLocation(util.MetricTableTimeFormat, text, time.Local)`.
/// An error is Go's `*time.ParseError` text.
fn parse_in_local(text: &str) -> Result<DateTime<Local>, String> {
    let naive = parse_metric_table_time(text)?;
    Ok(Local
        .from_local_datetime(&naive)
        .earliest()
        .unwrap_or_else(|| Local.from_utc_datetime(&naive)))
}

/// Go's `time.quote`.
fn go_time_quote(text: &str) -> String {
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

/// Go `newParseError(...).Error()`.
fn parse_error(value: &str, layout_elem: &str, value_elem: &str, message: &str) -> String {
    if message.is_empty() {
        format!(
            "parsing time {} as {}: cannot parse {} as {}",
            go_time_quote(value),
            go_time_quote(METRIC_TABLE_TIME_FORMAT),
            go_time_quote(value_elem),
            go_time_quote(layout_elem)
        )
    } else {
        format!("parsing time {}{message}", go_time_quote(value))
    }
}

/// Go `time.skip`: matches a layout literal, where a space matches any run
/// of spaces. On failure, the value where matching stopped.
fn skip<'a>(mut value: &'a str, mut prefix: &str) -> Result<&'a str, &'a str> {
    while let Some(first) = prefix.bytes().next() {
        if first == b' ' {
            if value.bytes().next().is_some_and(|byte| byte != b' ') {
                return Err(value);
            }
            prefix = prefix.trim_start_matches(' ');
            value = value.trim_start_matches(' ');
            continue;
        }
        if value.bytes().next() != Some(first) {
            return Err(value);
        }
        prefix = &prefix[1..];
        value = &value[1..];
    }
    Ok(value)
}

fn is_digit(text: &str, index: usize) -> bool {
    text.as_bytes().get(index).is_some_and(u8::is_ascii_digit)
}

/// Go `time.getnum`.
fn getnum(text: &str, fixed: bool) -> Option<(u32, &str)> {
    if !is_digit(text, 0) {
        return None;
    }
    let bytes = text.as_bytes();
    if !is_digit(text, 1) {
        if fixed {
            return None;
        }
        return Some((u32::from(bytes[0] - b'0'), &text[1..]));
    }
    Some((
        u32::from(bytes[0] - b'0') * 10 + u32::from(bytes[1] - b'0'),
        &text[2..],
    ))
}

/// Go `time.Parse` over [`METRIC_TABLE_TIME_FORMAT`], element by element:
/// `2006`, `-01`, `-02`, ` 15`, `:04`, `:05`, `.999`.
fn parse_metric_table_time(original: &str) -> Result<NaiveDateTime, String> {
    #[derive(Clone, Copy)]
    enum Element {
        LongYear,
        ZeroMonth,
        ZeroDay,
        Hour,
        ZeroMinute,
        ZeroSecond,
        FracSecond9,
    }
    const CHUNKS: &[(&str, Element, &str)] = &[
        ("", Element::LongYear, "2006"),
        ("-", Element::ZeroMonth, "01"),
        ("-", Element::ZeroDay, "02"),
        (" ", Element::Hour, "15"),
        (":", Element::ZeroMinute, "04"),
        (":", Element::ZeroSecond, "05"),
        ("", Element::FracSecond9, ".999"),
    ];
    let (mut year, mut month, mut day) = (0_i32, 0_u32, 0_u32);
    let (mut hour, mut minute, mut second, mut nanosecond) = (0_u32, 0_u32, 0_u32, 0_u32);
    let mut value = original;
    for &(prefix, element, layout_elem) in CHUNKS {
        value =
            skip(value, prefix).map_err(|stopped| parse_error(original, prefix, stopped, ""))?;
        let hold = value;
        let mut range_error = "";
        let parsed: Option<()> = match element {
            Element::LongYear => {
                if value.len() < 4 || !is_digit(value, 0) {
                    None
                } else {
                    let (digits, rest) = value.split_at(4);
                    value = rest;
                    digits
                        .parse::<i32>()
                        .ok()
                        .filter(|_| digits.bytes().all(|b| b.is_ascii_digit()))
                        .map(|parsed| year = parsed)
                }
            }
            Element::ZeroMonth => getnum(value, true).map(|(parsed, rest)| {
                month = parsed;
                value = rest;
                if month == 0 || month > 12 {
                    range_error = "month";
                }
            }),
            Element::ZeroDay => getnum(value, true).map(|(parsed, rest)| {
                day = parsed;
                value = rest;
            }),
            Element::Hour => getnum(value, false).map(|(parsed, rest)| {
                hour = parsed;
                value = rest;
                if hour >= 24 {
                    range_error = "hour";
                }
            }),
            Element::ZeroMinute => getnum(value, true).map(|(parsed, rest)| {
                minute = parsed;
                value = rest;
                if minute >= 60 {
                    range_error = "minute";
                }
            }),
            Element::ZeroSecond => getnum(value, true).map(|(parsed, rest)| {
                second = parsed;
                value = rest;
                if second >= 60 {
                    range_error = "second";
                }
                // The layout's `.999` follows, so a fractional second in the
                // value is left for it.
            }),
            Element::FracSecond9 => {
                let bytes = value.as_bytes();
                if bytes.len() < 2 || !matches!(bytes[0], b'.' | b',') || !bytes[1].is_ascii_digit()
                {
                    // Fractional second omitted.
                    Some(())
                } else {
                    let mut digits = 0;
                    while digits + 1 < bytes.len() && bytes[digits + 1].is_ascii_digit() {
                        digits += 1;
                    }
                    // Go `parseNanoseconds`: at most nine digits count.
                    let counted = &value[1..(1 + digits).min(10)];
                    let scale = 9 - counted.len() as u32;
                    nanosecond = counted.parse::<u32>().unwrap_or(0) * 10_u32.pow(scale);
                    value = &value[1 + digits..];
                    Some(())
                }
            }
        };
        if !range_error.is_empty() {
            return Err(parse_error(
                original,
                layout_elem,
                value,
                &format!(": {range_error} out of range"),
            ));
        }
        if parsed.is_none() {
            return Err(parse_error(original, layout_elem, hold, ""));
        }
    }
    if !value.is_empty() {
        return Err(parse_error(
            original,
            "",
            value,
            &format!(": extra text: {}", go_time_quote(value)),
        ));
    }
    let date = NaiveDate::from_ymd_opt(year, month, day).filter(|_| day >= 1);
    let Some(date) = date else {
        return Err(parse_error(original, "", value, ": day out of range"));
    };
    date.and_hms_nano_opt(hour, minute, second, nanosecond)
        .ok_or_else(|| parse_error(original, "", value, ": day out of range"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go's answers for `time.ParseInLocation("2006-01-02 15:04:05.999", s,
    /// time.Local)`, captured from Go.
    #[test]
    fn parses_and_refuses_as_go_time_parse() {
        let cases: &[(&str, Result<&str, &str>)] = &[
            ("2020-02-29 08:09:10.1234", Ok("2020-02-29 08:09:10.123")),
            ("2020-02-29 8:09:10", Ok("2020-02-29 08:09:10")),
            (
                "2020-1-01 00:00:00",
                Err(
                    "parsing time \"2020-1-01 00:00:00\" as \"2006-01-02 15:04:05.999\": \
                     cannot parse \"1-01 00:00:00\" as \"01\"",
                ),
            ),
            (
                "2020-13-01 00:00:00",
                Err("parsing time \"2020-13-01 00:00:00\": month out of range"),
            ),
            (
                "2019-02-29 00:00:00",
                Err("parsing time \"2019-02-29 00:00:00\": day out of range"),
            ),
            (
                "2020-01-01",
                Err(
                    "parsing time \"2020-01-01\" as \"2006-01-02 15:04:05.999\": \
                     cannot parse \"\" as \"15\"",
                ),
            ),
            (
                "2020-01-01 00:00:00x",
                Err("parsing time \"2020-01-01 00:00:00x\": extra text: \"x\""),
            ),
            ("2020-01-01  00:00:00", Ok("2020-01-01 00:00:00")),
            (
                "20a0-01-01 00:00:00",
                Err(
                    "parsing time \"20a0-01-01 00:00:00\" as \"2006-01-02 15:04:05.999\": \
                     cannot parse \"20a0-01-01 00:00:00\" as \"2006\"",
                ),
            ),
            (
                "2020-01-01 24:00:00",
                Err("parsing time \"2020-01-01 24:00:00\": hour out of range"),
            ),
            (
                "2020-01-01 00:00:00.",
                Err("parsing time \"2020-01-01 00:00:00.\": extra text: \".\""),
            ),
            (
                "2020-01-01 00:00:00.1234567891234",
                Ok("2020-01-01 00:00:00.123"),
            ),
            (
                "x\"y",
                Err("parsing time \"x\\\"y\" as \"2006-01-02 15:04:05.999\": \
                     cannot parse \"x\\\"y\" as \"2006\""),
            ),
            (
                "2020-01-00 00:00:00",
                Err("parsing time \"2020-01-00 00:00:00\": day out of range"),
            ),
        ];
        for (input, expected) in cases {
            let answer = parse_in_local(input).map(format_metric_table_time);
            assert_eq!(
                answer.as_deref(),
                expected.map_err(str::to_owned).as_deref(),
                "{input}"
            );
        }
        assert_eq!(
            parse_metric_table_time("2020-01-01 00:00:00.1234567891234")
                .unwrap()
                .nanosecond(),
            123_456_789
        );
    }
}
