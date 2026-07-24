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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Time builtin family, translated from `pkg/expression/builtin_time.go`.
//!
//! This module is the single Rust ownership boundary for the Go source
//! family. It owns both pure value functions and statement-clock functions;
//! callers enter through one narrow [`dispatch`] seam instead of growing the
//! generic builtin dispatcher or splitting helpers across unrelated modules.

pub(crate) mod calendar;

use self::calendar::{civil_from_days, days_from_civil, parse_date_ymd, week_of_year};
use crate::coerce::coerce_str;
use crate::{Columns, Datum, EvalError};

/// Dispatches this family's builtins; `None` if `name` isn't one of them.
pub(crate) fn dispatch(
    name: &str,
    vals: &[Datum],
    cols: &dyn Columns,
) -> Option<Result<Datum, EvalError>> {
    Some(match name {
        "NOW" | "CURRENT_TIMESTAMP" => now(vals, cols),
        "UTC_TIMESTAMP" => utc_timestamp(vals, cols),
        "CURDATE" | "CURRENT_DATE" => current_date(vals, cols),
        "UTC_DATE" => utc_date(vals, cols),
        "CURTIME" | "CURRENT_TIME" => current_time(vals, cols),
        "UTC_TIME" => utc_time(vals, cols),
        "MONTH" => month(vals),
        "DAY" | "DAYOFMONTH" => day_of_month(vals),
        "DAYOFWEEK" => day_of_week(vals),
        "DAYOFYEAR" => day_of_year(vals),
        "WEEKDAY" => weekday(vals),
        "QUARTER" => quarter(vals),
        "WEEK" => week(vals, cols.default_week_format()),
        "WEEKOFYEAR" => week_of_year_builtin(vals),
        "YEARWEEK" => yearweek(vals),
        "MONTHNAME" => monthname(vals),
        "DAYNAME" => dayname(vals),
        "LAST_DAY" => last_day(vals),
        "TIME_TO_SEC" => time_to_sec(vals),
        "SEC_TO_TIME" => sec_to_time(vals),
        "MAKEDATE" => makedate(vals),
        "MAKETIME" => maketime(vals),
        "PERIOD_ADD" => period_add(vals),
        "PERIOD_DIFF" => period_diff(vals),
        "TIME_FORMAT" => time_format(vals),
        "STR_TO_DATE" => calendar::str_to_date(vals),
        "TIMEDIFF" => time_diff(vals),
        "TIMESTAMPDIFF" => calendar::timestamp_diff(vals),
        "TO_DAYS" => calendar::to_days(vals),
        "TO_SECONDS" => calendar::to_seconds(vals),
        _ => return None,
    })
}

/// `builtinNowWithArgSig` / `builtinNowWithoutArgSig`: local
/// (`time_zone`-adjusted) statement time, always truncating fractional
/// seconds. `CURRENT_TIMESTAMP` is the same function class.
fn now(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    let fsp = parse_fsp(vals)?.unwrap_or(0);
    let (utc_secs, nanos, tz_offset) = cols.now().ok_or(no_clock_err())?;
    Ok(Datum::new_string(format_datetime(
        utc_secs + i64::from(tz_offset),
        nanos,
        fsp,
        false,
    )))
}

/// `builtinUTCTimestampWithArgSig` / `builtinUTCTimestampWithoutArgSig`:
/// raw UTC statement time, always rounding fractional seconds half-up.
fn utc_timestamp(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    let fsp = parse_fsp(vals)?.unwrap_or(0);
    let (utc_secs, nanos, _) = cols.now().ok_or(no_clock_err())?;
    Ok(Datum::new_string(format_datetime(
        utc_secs, nanos, fsp, true,
    )))
}

/// `builtinCurrentDateSig`: local statement date. `CURDATE` and
/// `CURRENT_DATE` share this signature and accept no argument.
fn current_date(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if !vals.is_empty() {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let (utc_secs, _, tz_offset) = cols.now().ok_or(no_clock_err())?;
    Ok(Datum::new_string(format_date(
        utc_secs + i64::from(tz_offset),
    )))
}

/// `builtinUTCDateSig`: raw UTC statement date with no arguments.
fn utc_date(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if !vals.is_empty() {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let (utc_secs, _, _) = cols.now().ok_or(no_clock_err())?;
    Ok(Datum::new_string(format_date(utc_secs)))
}

/// `builtinCurrentTime0ArgSig` / `builtinCurrentTime1ArgSig`: local
/// statement time. The zero-argument signature truncates; an explicit FSP,
/// including zero, rounds half-up. `CURTIME` and `CURRENT_TIME` are aliases.
fn current_time(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    let fsp = parse_fsp(vals)?;
    let (utc_secs, nanos, tz_offset) = cols.now().ok_or(no_clock_err())?;
    // builtinCurrentTime1ArgSig first renders TimeFSPFormat (six digits,
    // truncating sub-microsecond nanoseconds) and only then ParseDuration
    // rounds to the requested FSP. Preserve that two-stage source algorithm;
    // it is observably different from UTC_TIMESTAMP's direct half-up path.
    let nanos = fsp.map_or(nanos, |_| nanos / 1_000 * 1_000);
    Ok(Datum::new_string(format_time_only(
        utc_secs + i64::from(tz_offset),
        nanos,
        fsp.unwrap_or(0),
        fsp.is_some(),
    )))
}

/// `builtinUTCTimeWithoutArgSig` / `builtinUTCTimeWithArgSig`: raw UTC
/// statement time with the same zero-argument-truncate / explicit-FSP-round
/// split as [`current_time`].
fn utc_time(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    let fsp = parse_fsp(vals)?;
    let (utc_secs, nanos, _) = cols.now().ok_or(no_clock_err())?;
    // builtinUTCTimeWithArgSig has the identical TimeFSPFormat-then-parse
    // conversion as CURRENT_TIME's explicit signature.
    let nanos = fsp.map_or(nanos, |_| nanos / 1_000 * 1_000);
    Ok(Datum::new_string(format_time_only(
        utc_secs,
        nanos,
        fsp.unwrap_or(0),
        fsp.is_some(),
    )))
}

fn no_clock_err() -> EvalError {
    EvalError::Unsupported("no session clock (SET timestamp)")
}

/// Parses the source family's optional 0-6 fractional-seconds precision.
/// `None` means the zero-argument signature and remains distinguishable from
/// an explicit zero for CURRENT_TIME/UTC_TIME rounding.
fn parse_fsp(vals: &[Datum]) -> Result<Option<u32>, EvalError> {
    match vals {
        [] => Ok(None),
        [Datum::Int(i)] if (0..=6).contains(i) => Ok(Some(*i as u32)),
        [Datum::UInt(i)] if *i <= 6 => Ok(Some(*i as u32)),
        _ => Err(EvalError::Unsupported(
            "bad fractional-seconds-precision argument",
        )),
    }
}

/// Renders an epoch second as a Gregorian `YYYY-MM-DD` date.
fn format_date(secs: i64) -> String {
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    format!("{y:04}-{m:02}-{d:02}")
}

fn format_hms(secs: i64) -> String {
    let secs_of_day = secs.rem_euclid(86_400);
    let (hour, minute, second) = (
        secs_of_day / 3_600,
        (secs_of_day % 3_600) / 60,
        secs_of_day % 60,
    );
    format!("{hour:02}:{minute:02}:{second:02}")
}

fn frac_suffix(nanos: u32, fsp: u32) -> String {
    if fsp == 0 {
        return String::new();
    }
    let fraction = nanos / 10u32.pow(9 - fsp);
    format!(".{fraction:0width$}", width = fsp as usize)
}

/// TiDB's `types.ModeHalfUp` rounding at the requested FSP.
fn round_nanos(nanos: u32, fsp: u32) -> (i64, u32) {
    let scale = 10u32.pow(9 - fsp);
    let half_up = nanos + scale / 2;
    if half_up >= 1_000_000_000 {
        (1, 0)
    } else {
        (0, (half_up / scale) * scale)
    }
}

fn format_datetime(secs: i64, nanos: u32, fsp: u32, round: bool) -> String {
    let (carry, nanos) = if round {
        round_nanos(nanos, fsp)
    } else {
        (0, nanos)
    };
    let secs = secs + carry;
    format!(
        "{} {}{}",
        format_date(secs),
        format_hms(secs),
        frac_suffix(nanos, fsp)
    )
}

fn format_time_only(secs: i64, nanos: u32, fsp: u32, round: bool) -> String {
    let (carry, nanos) = if round {
        round_nanos(nanos, fsp)
    } else {
        (0, nanos)
    };
    let secs = secs + carry;
    format!("{}{}", format_hms(secs), frac_suffix(nanos, fsp))
}

fn single_date(vals: &[Datum]) -> Result<Option<(i64, u32, u32)>, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    Ok(coerce_str(&vals[0])?.and_then(|s| parse_date_ymd(&s)))
}

/// Parses a date/datetime argument at the same value boundary as Go's
/// `EvalTime`.  [`parse_date_ymd`] intentionally ignores a trailing time
/// suffix because date-part functions only need the calendar fields; the
/// `LAST_DAY` signature still rejects a malformed suffix (for example
/// `23:59:61`) before it computes the month end.
fn single_datetime(vals: &[Datum]) -> Result<Option<(i64, u32, u32)>, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(value) = coerce_str(&vals[0])? else {
        return Ok(None);
    };
    let value = value.trim();
    let (date, time) = value
        .split_once(char::is_whitespace)
        .map_or((value, None), |(date, time)| (date, Some(time.trim())));
    let Some(ymd) = parse_date_ymd(date) else {
        return Ok(None);
    };
    if let Some(time) = time {
        if calendar::parse_time_with_fraction(time).is_none() {
            return Ok(None);
        }
    }
    Ok(Some(ymd))
}

/// `builtinMonthSig.evalInt` in `pkg/expression/builtin_time.go`.
///
/// The source returns the parsed month field directly. Whether a month-zero
/// value reaches this function is decided by EvalTime's StatementContext type
/// flags, which are not present at this value-only boundary.
fn month(vals: &[Datum]) -> Result<Datum, EvalError> {
    Ok(single_date(vals)?.map_or(Datum::Null, |(_, month, _)| Datum::Int(i64::from(month))))
}

/// `builtinDayOfMonthSig.evalInt` in `pkg/expression/builtin_time.go`.
///
/// The source returns the parsed day field directly. Its zero-date result is
/// selected earlier by EvalTime using StatementContext type flags; this
/// value-only evaluator has no such flag and therefore keeps its existing
/// strict-date boundary rather than pretending one SQL mode is universal.
fn day_of_month(vals: &[Datum]) -> Result<Datum, EvalError> {
    Ok(single_date(vals)?.map_or(Datum::Null, |(_, _, day)| Datum::Int(i64::from(day))))
}

/// `builtinDayOfWeekSig.evalInt`: Sunday is 1 through Saturday 7. Invalid
/// zero dates are NULL in Go even when EvalTime is configured to parse them.
fn day_of_week(vals: &[Datum]) -> Result<Datum, EvalError> {
    Ok(
        single_date(vals)?.map_or(Datum::Null, |(year, month, day)| {
            Datum::Int((days_from_civil(year, month, day) + 4).rem_euclid(7) + 1)
        }),
    )
}

/// `builtinDayOfYearSig.evalInt`: one-based day within the calendar year.
/// Invalid zero dates are NULL before this calculation in the Go evaluator.
fn day_of_year(vals: &[Datum]) -> Result<Datum, EvalError> {
    Ok(
        single_date(vals)?.map_or(Datum::Null, |(year, month, day)| {
            Datum::Int(days_from_civil(year, month, day) - days_from_civil(year, 1, 1) + 1)
        }),
    )
}

/// `builtinWeekDaySig.evalInt`: Monday is 0 through Sunday 6. Like
/// DAYOFWEEK, zero and invalid-zero dates are NULL in the source.
fn weekday(vals: &[Datum]) -> Result<Datum, EvalError> {
    Ok(
        single_date(vals)?.map_or(Datum::Null, |(year, month, day)| {
            Datum::Int((days_from_civil(year, month, day) + 3).rem_euclid(7))
        }),
    )
}

/// `builtinQuarterSig.evalInt`, returning 1-4 for a strict date. The source
/// can additionally return 0 for a parsed month-zero date when its
/// StatementContext permits invalid-zero components; that context-dependent
/// path remains outside this value-only boundary.
fn quarter(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(input) = coerce_str(&vals[0])? else {
        return Ok(Datum::Null);
    };
    Ok(
        calendar::parse_date_with_zero_month(&input).map_or(Datum::Null, |(_, month, _)| {
            Datum::Int(i64::from(month.div_ceil(3)))
        }),
    )
}

/// `builtinWeekWithModeSig` / `builtinWeekWithoutModeSig` in
/// `pkg/expression/builtin_time.go`. Only the no-mode branch uses the
/// supplied session `default_week_format`; a caller with no session passes
/// TiDB's default zero.
/// `WEEKOFYEAR(date)`. Port of `builtinWeekOfYearSig.evalInt`, which is
/// `date.Week(3)` — the ISO-like mode-3 week number. Zero and invalid dates are
/// NULL, matching `week`.
fn week_of_year_builtin(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(date) = coerce_str(&vals[0])?.and_then(|s| parse_date_ymd(&s)) else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Int(week_of_year(date.0, date.1, date.2, 3, false).1))
}

/// The `GET_FORMAT` lookup table. `format_type` is one of `DATE`/`DATETIME`/
/// `TIME` (the AST selector; `TIMESTAMP` collapses into `DATETIME` upstream, so
/// it shares the datetime row). Location matching is case-insensitive; an
/// unknown combination returns an empty string. Port of
/// `builtinGetFormatSig.getFormat`.
pub(crate) fn get_format(format_type: &str, location: &str) -> String {
    let location = location.to_uppercase();
    let res = match (format_type, location.as_str()) {
        ("DATE", "USA") => "%m.%d.%Y",
        ("DATE", "JIS") => "%Y-%m-%d",
        ("DATE", "ISO") => "%Y-%m-%d",
        ("DATE", "EUR") => "%d.%m.%Y",
        ("DATE", "INTERNAL") => "%Y%m%d",
        ("DATETIME", "USA") => "%Y-%m-%d %H.%i.%s",
        ("DATETIME", "JIS") => "%Y-%m-%d %H:%i:%s",
        ("DATETIME", "ISO") => "%Y-%m-%d %H:%i:%s",
        ("DATETIME", "EUR") => "%Y-%m-%d %H.%i.%s",
        ("DATETIME", "INTERNAL") => "%Y%m%d%H%i%s",
        ("TIME", "USA") => "%h:%i:%s %p",
        ("TIME", "JIS") => "%H:%i:%s",
        ("TIME", "ISO") => "%H:%i:%s",
        ("TIME", "EUR") => "%H.%i.%s",
        ("TIME", "INTERNAL") => "%H%i%s",
        _ => "",
    };
    res.to_string()
}

pub(crate) fn week(vals: &[Datum], default_week_format: i64) -> Result<Datum, EvalError> {
    if !(1..=2).contains(&vals.len()) {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(date) = coerce_str(&vals[0])?.and_then(|s| parse_date_ymd(&s)) else {
        return Ok(Datum::Null);
    };
    let mode = if vals.len() == 2 {
        int_arg(&vals[1])?.unwrap_or(0)
    } else {
        default_week_format
    };
    Ok(Datum::Int(
        week_of_year(date.0, date.1, date.2, mode, false).1,
    ))
}

/// `builtinYearWeekWithModeSig` / `builtinYearWeekWithoutModeSig` in
/// `pkg/expression/builtin_time.go`.
fn yearweek(vals: &[Datum]) -> Result<Datum, EvalError> {
    if !(1..=2).contains(&vals.len()) {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(date) = coerce_str(&vals[0])?.and_then(|s| parse_date_ymd(&s)) else {
        return Ok(Datum::Null);
    };
    let mode = if vals.len() == 2 {
        int_arg(&vals[1])?.unwrap_or(0)
    } else {
        0
    };
    let (year, number) = week_of_year(date.0, date.1, date.2, mode, true);
    let result = year * 100 + number;
    Ok(Datum::Int(if result < 0 {
        i64::from(u32::MAX)
    } else {
        result
    }))
}

/// `builtinMonthNameSig` in `pkg/expression/builtin_time.go`.
fn monthname(vals: &[Datum]) -> Result<Datum, EvalError> {
    const MONTHS: [&str; 12] = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];
    Ok(single_date(vals)?.map_or(Datum::Null, |(_, month, _)| {
        Datum::new_string(MONTHS[(month - 1) as usize].to_string())
    }))
}

/// `builtinDayNameSig` in `pkg/expression/builtin_time.go`.
fn dayname(vals: &[Datum]) -> Result<Datum, EvalError> {
    const DAYS: [&str; 7] = [
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
    ];
    Ok(single_date(vals)?.map_or(Datum::Null, |(y, m, d)| {
        Datum::new_string(DAYS[(days_from_civil(y, m, d) + 4).rem_euclid(7) as usize].to_string())
    }))
}

/// `builtinLastDaySig` in `pkg/expression/builtin_time.go`.
fn last_day(vals: &[Datum]) -> Result<Datum, EvalError> {
    Ok(single_datetime(vals)?.map_or(Datum::Null, |(y, m, _)| {
        let next_month = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
        let (last_y, last_m, last_d) =
            civil_from_days(days_from_civil(next_month.0, next_month.1, 1) - 1);
        Datum::new_string(format!("{last_y:04}-{last_m:02}-{last_d:02}"))
    }))
}

fn int_arg(value: &Datum) -> Result<Option<i64>, EvalError> {
    match value {
        Datum::Null => Ok(None),
        Datum::Int(v) => Ok(Some(*v)),
        Datum::UInt(v) => Ok(Some(*v as i64)),
        Datum::Decimal(v) => Ok(Some(v.round_to_i64().ok_or(EvalError::IntOverflow)?)),
        Datum::Real(v) => Ok(Some(*v as i64)),
        Datum::String(v) => Ok(Some(
            v.as_utf8()
                .map_err(|_| EvalError::Unsupported("invalid UTF-8 string datum"))?
                .trim()
                .parse::<f64>()
                .unwrap_or(0.0) as i64,
        )),
        Datum::Bytes(v) => Ok(Some(
            std::str::from_utf8(v)
                .map_err(|_| EvalError::Unsupported("invalid UTF-8 byte datum"))?
                .trim()
                .parse::<f64>()
                .unwrap_or(0.0) as i64,
        )),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel time argument"))
        }
        other => other
            .to_i64()
            .map(|converted| Some(converted.value))
            .map_err(|_| EvalError::Unsupported("time argument conversion")),
    }
}

fn number_arg(value: &Datum) -> Result<Option<f64>, EvalError> {
    Ok(match value {
        Datum::Null => None,
        Datum::Int(v) => Some(*v as f64),
        Datum::UInt(v) => Some(*v as f64),
        Datum::Decimal(v) => Some(v.to_f64()),
        Datum::Real(v) => Some(*v),
        Datum::String(v) => v
            .as_utf8()
            .ok()
            .map(|text| text.trim().parse().unwrap_or(0.0)),
        Datum::Bytes(v) => std::str::from_utf8(v)
            .ok()
            .map(|text| text.trim().parse().unwrap_or(0.0)),
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel numeric argument"));
        }
        other => Some(
            other
                .to_f64()
                .map_err(|_| EvalError::Unsupported("numeric argument conversion"))?
                .value,
        ),
    })
}

/// Parses TiDB's duration inputs used by `TIME_TO_SEC` and `TIME_FORMAT`.
/// This covers the accepted `H:M[:S[.fraction]]` and right-aligned numeric
/// forms exercised by `builtin_time_test.go`; the return is signed seconds
/// plus the fraction text preserved for formatting.
fn duration(value: &Datum) -> Result<Option<(i64, String)>, EvalError> {
    let Some(text) = coerce_str(value)? else {
        return Ok(None);
    };
    let text = text.trim();
    // EvalDuration accepts a datetime-shaped string and uses its time suffix.
    // This is the `1990-05-07 19:30:10` row in TestTimeFormat, not a generic
    // temporal parser: the current Datum domain still represents the input as
    // a string, and the already-owned duration conversion only needs the
    // suffix after the date separator.
    let text = match text.rsplit_once(' ') {
        Some((date, time)) if parse_date_ymd(date).is_some() => time,
        _ => text,
    };
    let (negative, text) = text.strip_prefix('-').map_or((false, text), |s| (true, s));
    let (h, m, seconds) = if text.contains(':') {
        let parts: Vec<_> = text.split(':').collect();
        if !(2..=3).contains(&parts.len()) {
            return Ok(None);
        }
        let Ok(h) = parts[0].parse::<i64>() else {
            return Ok(None);
        };
        let Ok(m) = parts[1].parse::<i64>() else {
            return Ok(None);
        };
        let s = parts.get(2).copied().unwrap_or("0");
        (h, m, s.to_string())
    } else {
        let digits: String = text.chars().take_while(char::is_ascii_digit).collect();
        if digits.is_empty() {
            return Ok(Some((0, String::new())));
        }
        let Ok(n) = digits.parse::<i64>() else {
            return Ok(None);
        };
        (n / 10_000, n / 100 % 100, (n % 100).to_string())
    };
    let (whole, fraction) = seconds
        .split_once('.')
        .map_or((seconds.as_str(), ""), |(a, b)| (a, b));
    let Ok(s) = whole.parse::<i64>() else {
        return Ok(None);
    };
    if h > 838 || !(0..60).contains(&m) || !(0..60).contains(&s) {
        return Ok(None);
    }
    let total = h * 3600 + m * 60 + s;
    Ok(Some((
        if negative { -total } else { total },
        fraction.chars().take(6).collect(),
    )))
}

enum TimeDiffValue {
    DateTime { micros: i64, fsp: usize },
    Duration { micros: i64, fsp: usize },
}

/// `TIMEDIFF(expr1, expr2)`, covering the string-valued signatures exercised
/// by `builtin_time_test.go`.  Go selects among typed Time/Duration
/// signatures before evaluation; this value-only port keeps that distinction
/// by rejecting a mixed date-time/duration pair, while returning the canonical
/// duration string for matching pairs.  Zero month/day components are
/// accepted for the same `IgnoreZeroInDate` source rows and are interpreted
/// by the source-compatible `calcDaynr` arithmetic.
fn time_diff(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 2 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(left) = parse_time_diff_value(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let Some(right) = parse_time_diff_value(&vals[1])? else {
        return Ok(Datum::Null);
    };
    let (left_micros, right_micros, fsp) = match (left, right) {
        (
            TimeDiffValue::DateTime {
                micros: left,
                fsp: left_fsp,
            },
            TimeDiffValue::DateTime {
                micros: right,
                fsp: right_fsp,
            },
        )
        | (
            TimeDiffValue::Duration {
                micros: left,
                fsp: left_fsp,
            },
            TimeDiffValue::Duration {
                micros: right,
                fsp: right_fsp,
            },
        ) => (left, right, left_fsp.max(right_fsp)),
        _ => return Ok(Datum::Null),
    };
    Ok(Datum::new_string(format_time_diff(
        truncate_time_diff(left_micros.saturating_sub(right_micros)),
        fsp,
    )))
}

fn parse_time_diff_value(value: &Datum) -> Result<Option<TimeDiffValue>, EvalError> {
    let Some(text) = coerce_str(value)? else {
        return Ok(None);
    };
    let text = text.trim();
    if text.is_empty() {
        return Ok(None);
    }
    if let Some((date, time)) = text.split_once(char::is_whitespace) {
        return Ok(parse_datetime_diff_value(date, time.trim()));
    }
    if text.contains(':') {
        return Ok(parse_duration_diff_value(text));
    }
    // A date-only value is a datetime at midnight.  Do not mistake a
    // colon-separated duration for a date (`10:9:0` was handled above).
    Ok(parse_datetime_diff_value(text, "00:00:00"))
}

fn parse_datetime_diff_value(date: &str, time: &str) -> Option<TimeDiffValue> {
    let parts = calendar::split_numeric_components_for_time_diff(date)?;
    let year = calendar::expand_year_for_time_diff(parts[0].0, parts[0].1);
    let month = parts[1].0;
    let day = parts[2].0;
    if month > 12 || day > 31 {
        return None;
    }
    if month != 0 && day > calendar::days_in_month_for_time_diff(year, month) {
        return None;
    }
    let (hour, minute, second, fraction) = calendar::parse_time_with_fraction(time)?;
    let fsp = fraction.len();
    let microsecond = fraction.parse::<u32>().ok().unwrap_or(0) * 10u32.pow(6 - fsp as u32);
    let micros = calendar::time_diff_daynr(year, month, day)
        .checked_mul(86_400_000_000)?
        .checked_add(i64::from(hour) * 3_600_000_000)?
        .checked_add(i64::from(minute) * 60_000_000)?
        .checked_add(i64::from(second) * 1_000_000)?
        .checked_add(i64::from(microsecond))?;
    Some(TimeDiffValue::DateTime { micros, fsp })
}

const MAX_TIME_DIFF_MICROS: i64 = (838 * 3_600 + 59 * 60 + 59) * 1_000_000;

fn truncate_time_diff(micros: i64) -> i64 {
    micros.clamp(-MAX_TIME_DIFF_MICROS, MAX_TIME_DIFF_MICROS)
}

fn parse_duration_diff_value(text: &str) -> Option<TimeDiffValue> {
    let (negative, text) = text
        .strip_prefix('-')
        .map_or((false, text), |text| (true, text));
    let mut fields = text.splitn(3, ':');
    let hour = fields.next()?.parse::<i64>().ok()?;
    let minute = fields.next()?.parse::<u32>().ok()?;
    let second_part = fields.next()?;
    let (second_part, fraction) = second_part
        .split_once('.')
        .map_or((second_part, ""), |pair| pair);
    let second = second_part.parse::<u32>().ok()?;
    if minute > 59 || second > 59 || fraction.len() > 6 || !fraction.is_ascii() {
        return None;
    }
    let microsecond = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<u32>().ok()? * 10u32.pow(6 - fraction.len() as u32)
    };
    let micros = hour
        .checked_mul(3_600_000_000)?
        .checked_add(i64::from(minute) * 60_000_000)?
        .checked_add(i64::from(second) * 1_000_000)?
        .checked_add(i64::from(microsecond))?;
    Some(TimeDiffValue::Duration {
        micros: if negative { -micros } else { micros },
        fsp: fraction.len(),
    })
}

fn format_time_diff(micros: i64, fsp: usize) -> String {
    let sign = if micros < 0 { "-" } else { "" };
    let absolute = micros.unsigned_abs();
    let hours = absolute / 3_600_000_000;
    let minutes = absolute / 60_000_000 % 60;
    let seconds = absolute / 1_000_000 % 60;
    if fsp == 0 {
        return format!("{sign}{hours:02}:{minutes:02}:{seconds:02}");
    }
    let divisor = 10u64.pow(6 - fsp as u32);
    let fraction = absolute / divisor % 10u64.pow(fsp as u32);
    format!(
        "{sign}{hours:02}:{minutes:02}:{seconds:02}.{fraction:0width$}",
        width = fsp
    )
}

/// `builtinTimeToSecSig` in `pkg/expression/builtin_time.go`.
fn time_to_sec(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    Ok(duration(&vals[0])?.map_or(Datum::Null, |(seconds, _)| Datum::Int(seconds)))
}

/// `builtinSecToTimeSig` in `pkg/expression/builtin_time.go`.
fn sec_to_time(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(seconds) = number_arg(&vals[0])? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::new_string(format_duration(
        seconds,
        duration_precision(&vals[0])?,
    )))
}

/// The result FSP comes from TiDB's argument type. String coercion uses the
/// duration parser's default FSP six; numeric literals preserve their own
/// fractional scale; integers have no fractional component.
fn duration_precision(value: &Datum) -> Result<usize, EvalError> {
    Ok(match value {
        Datum::Int(_) | Datum::UInt(_) => 0,
        Datum::String(_) | Datum::Bytes(_) => 6,
        Datum::Decimal(v) => v
            .to_string()
            .split_once('.')
            .map_or(0, |(_, f)| f.len().min(6)),
        Datum::Real(v) => v
            .to_string()
            .split_once('.')
            .map_or(0, |(_, f)| f.len().min(6)),
        Datum::Float32(v) => v
            .to_string()
            .split_once('.')
            .map_or(0, |(_, f)| f.len().min(6)),
        Datum::Duration(value) => usize::from(value.fsp()),
        Datum::Time(value) => usize::from(value.fsp()),
        Datum::Null => 0,
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel duration argument"));
        }
        other => other
            .sql_string()
            .ok()
            .and_then(|text| text.split_once('.').map(|(_, f)| f.len().min(6)))
            .unwrap_or(0),
    })
}

fn format_duration(seconds: f64, fsp: usize) -> String {
    let sign = if seconds < 0.0 { "-" } else { "" };
    let max = 838.0 * 3600.0 + 59.0 * 60.0 + 59.0;
    let mut seconds = seconds.abs();
    if seconds > max {
        seconds = max;
    }
    let whole = seconds.trunc() as i64;
    let hour = whole / 3600;
    let minute = whole / 60 % 60;
    let second = whole % 60;
    if fsp == 0 {
        return format!("{sign}{hour:02}:{minute:02}:{second:02}");
    }
    let divisor = 10_i64.pow((6 - fsp) as u32);
    let fraction = ((seconds.fract() * 1_000_000.0).round() as i64 / divisor)
        .clamp(0, 10_i64.pow(fsp as u32) - 1);
    format!("{sign}{hour:02}:{minute:02}:{second:02}.{fraction:0fsp$}")
}

/// `builtinMakeDateSig` in `pkg/expression/builtin_time.go`.
fn makedate(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 2 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let (Some(mut year), Some(day)) = (int_arg(&vals[0])?, int_arg(&vals[1])?) else {
        return Ok(Datum::Null);
    };
    if day <= 0 || !(0..=9999).contains(&year) {
        return Ok(Datum::Null);
    }
    if year < 70 {
        year += 2000;
    } else if year < 100 {
        year += 1900;
    }
    let (result_y, result_m, result_d) = civil_from_days(days_from_civil(year, 1, 1) + day - 1);
    if !(1..=9999).contains(&result_y) {
        return Ok(Datum::Null);
    }
    Ok(Datum::new_string(format!(
        "{result_y:04}-{result_m:02}-{result_d:02}"
    )))
}

/// `builtinMakeTimeSig` in `pkg/expression/builtin_time.go`.
fn maketime(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 3 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let (Some(hour), Some(minute), Some(second)) = (
        int_arg(&vals[0])?,
        int_arg(&vals[1])?,
        number_arg(&vals[2])?,
    ) else {
        return Ok(Datum::Null);
    };
    if !(0..60).contains(&minute) || !(0.0..60.0).contains(&second) {
        return Ok(Datum::Null);
    }
    let negative = hour < 0;
    let hour_abs = hour.unsigned_abs();
    let overflow = hour_abs > 838 || (hour_abs == 838 && minute == 59 && second > 59.0);
    let total = if overflow {
        838.0 * 3600.0 + 59.0 * 60.0 + 59.0
    } else {
        hour_abs as f64 * 3600.0 + minute as f64 * 60.0 + second
    };
    Ok(Datum::new_string(format_duration(
        if negative { -total } else { total },
        duration_precision(&vals[2])?,
    )))
}

/// `PERIOD_ADD(period, months)`, ported from `builtinPeriodAddSig.evalInt`
/// in `pkg/expression/builtin_time.go`.
///
/// A period is an integer `YYMM` or `YYYYMM`, not a date.  TiDB evaluates
/// both ETInt arguments before it validates the period: consequently an
/// invalid period paired with `NULL` months is `NULL`, not an error.  Keep
/// that ordering rather than validating the first argument eagerly.  The Go
/// helpers operate on `uint64`, so their arithmetic (including conversion
/// back through `int64`) deliberately wraps; the wrapping methods below are
/// the structural Rust translation, not overflow recovery.
fn period_add(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 2 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let (Some(period), Some(months)) = (int_arg(&vals[0])?, int_arg(&vals[1])?) else {
        return Ok(Datum::Null);
    };
    if !valid_period(period) {
        // TiDB returns ER_WRONG_ARGUMENTS (1210). EvalError intentionally has
        // no server error-code payload, so retain the error boundary without
        // inventing a diagnostic string.
        return Err(EvalError::Unsupported("invalid PERIOD_ADD period"));
    }
    let sum = (period_to_month(period as u64) as i64).wrapping_add(months);
    Ok(Datum::Int(month_to_period(sum as u64) as i64))
}

/// `PERIOD_DIFF(period1, period2)`, ported from
/// `builtinPeriodDiffSig.evalInt` in `pkg/expression/builtin_time.go`.
fn period_diff(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 2 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let (Some(period1), Some(period2)) = (int_arg(&vals[0])?, int_arg(&vals[1])?) else {
        return Ok(Datum::Null);
    };
    if !valid_period(period1) || !valid_period(period2) {
        return Err(EvalError::Unsupported("invalid PERIOD_DIFF period"));
    }
    // Go subtracts the uint64 month totals before converting to int64.
    Ok(Datum::Int(
        period_to_month(period1 as u64).wrapping_sub(period_to_month(period2 as u64)) as i64,
    ))
}

fn valid_period(period: i64) -> bool {
    period >= 0 && period % 100 != 0 && period % 100 <= 12
}

/// Exact `period2Month` from `pkg/expression/builtin_time.go`.
fn period_to_month(period: u64) -> u64 {
    if period == 0 {
        return 0;
    }
    let mut year = period / 100;
    let month = period % 100;
    if year < 70 {
        year += 2_000;
    } else if year < 100 {
        year += 1_900;
    }
    year.wrapping_mul(12).wrapping_add(month).wrapping_sub(1)
}

/// Exact `month2Period` from `pkg/expression/builtin_time.go`.
fn month_to_period(month: u64) -> u64 {
    if month == 0 {
        return 0;
    }
    let mut year = month / 12;
    if year < 70 {
        year += 2_000;
    } else if year < 100 {
        year += 1_900;
    }
    year.wrapping_mul(100)
        .wrapping_add(month % 12)
        .wrapping_add(1)
}

/// `builtinTimeFormatSig` in `pkg/expression/builtin_time.go`; shares the
/// `types.Duration.DurationFormat` specifier family with DATE_FORMAT.
fn time_format(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 2 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some((seconds, fraction)) = duration(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let Some(mask) = coerce_str(&vals[1])? else {
        return Ok(Datum::Null);
    };
    if mask.is_empty() {
        return Ok(Datum::Null);
    }
    let sign = if seconds < 0 { "-" } else { "" };
    let total = seconds.unsigned_abs();
    let hour = total / 3600;
    let minute = total / 60 % 60;
    let second = total % 60;
    let hour12 = (hour + 11) % 12 + 1;
    let mut out = String::new();
    let mut chars = mask.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        match chars.next() {
            None => out.push('%'),
            Some('H') => out.push_str(&format!("{sign}{hour:02}")),
            Some('k') => out.push_str(&format!("{sign}{hour}")),
            Some('h' | 'I') => out.push_str(&format!("{hour12:02}")),
            Some('l') => out.push_str(&hour12.to_string()),
            Some('i') => out.push_str(&format!("{minute:02}")),
            Some('S' | 's') => out.push_str(&format!("{second:02}")),
            Some('f') => out.push_str(&format!("{fraction:0<6}")),
            Some('p') => out.push_str(if hour < 12 { "AM" } else { "PM" }),
            Some('T') => out.push_str(&format!("{sign}{hour:02}:{minute:02}:{second:02}")),
            Some('r') => out.push_str(&format!(
                "{hour12:02}:{minute:02}:{second:02} {}",
                if hour < 12 { "AM" } else { "PM" }
            )),
            Some('%') => out.push('%'),
            Some(other) => out.push(other),
        }
    }
    Ok(Datum::new_string(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn string_datum(value: &str) -> Datum {
        Datum::new_string(value.to_string())
    }

    /// `builtinGetFormatSig.getFormat` table: every (type, location) pair,
    /// case-insensitive location, empty for unknown, TIMESTAMP shares DATETIME.
    #[test]
    fn get_format_table() {
        assert_eq!(get_format("DATE", "USA"), "%m.%d.%Y");
        assert_eq!(get_format("DATE", "JIS"), "%Y-%m-%d");
        assert_eq!(get_format("DATE", "ISO"), "%Y-%m-%d");
        assert_eq!(get_format("DATE", "EUR"), "%d.%m.%Y");
        assert_eq!(get_format("DATE", "INTERNAL"), "%Y%m%d");
        assert_eq!(get_format("DATETIME", "USA"), "%Y-%m-%d %H.%i.%s");
        assert_eq!(get_format("DATETIME", "JIS"), "%Y-%m-%d %H:%i:%s");
        assert_eq!(get_format("DATETIME", "ISO"), "%Y-%m-%d %H:%i:%s");
        assert_eq!(get_format("DATETIME", "EUR"), "%Y-%m-%d %H.%i.%s");
        assert_eq!(get_format("DATETIME", "INTERNAL"), "%Y%m%d%H%i%s");
        assert_eq!(get_format("TIME", "USA"), "%h:%i:%s %p");
        assert_eq!(get_format("TIME", "JIS"), "%H:%i:%s");
        assert_eq!(get_format("TIME", "ISO"), "%H:%i:%s");
        assert_eq!(get_format("TIME", "EUR"), "%H.%i.%s");
        assert_eq!(get_format("TIME", "INTERNAL"), "%H%i%s");
        // Location is case-insensitive.
        assert_eq!(get_format("TIME", "usa"), "%h:%i:%s %p");
        // Unknown location / type -> empty.
        assert_eq!(get_format("DATE", "unknown"), "");
        assert_eq!(get_format("YEAR", "USA"), "");
    }

    /// `builtinWeekOfYearSig` = `date.Week(3)`; zero/invalid dates are NULL.
    #[test]
    fn week_of_year_source_vectors() {
        assert_eq!(
            week_of_year_builtin(&[string_datum("2024-03-15")]).unwrap(),
            Datum::Int(11)
        );
        assert_eq!(
            week_of_year_builtin(&[string_datum("2024-01-01")]).unwrap(),
            Datum::Int(1)
        );
        assert_eq!(
            week_of_year_builtin(&[string_datum("2020-12-31")]).unwrap(),
            Datum::Int(53)
        );
        assert_eq!(
            week_of_year_builtin(&[string_datum("0000-00-00")]).unwrap(),
            Datum::Null
        );
        assert_eq!(week_of_year_builtin(&[Datum::Null]).unwrap(), Datum::Null);
    }

    #[test]
    fn calendar_part_source_vectors() {
        // TestDayOfWeek, TestDayOfMonth, TestDayOfYear, TestQuarter, and
        // the directly shared function classes in TestDate from
        // pkg/expression/builtin_time_test.go. The IgnoreZeroInDate-only
        // DAYOFMONTH/QUARTER rows stay outside this value-only evaluator;
        // their exact StatementContext blocker is recorded in the ledger.
        let day_of_week_cases = [
            ("2017-12-01", Datum::Int(6)),
            ("0000-00-00", Datum::Null),
            ("2018-00-00", Datum::Null),
            ("2017-00-00 12:12:12", Datum::Null),
            ("0000-00-00 12:12:12", Datum::Null),
            ("2000-01-01", Datum::Int(7)),
            ("2011-11-11", Datum::Int(6)),
            ("0000-01-01", Datum::Int(7)),
        ];
        for (input, want) in day_of_week_cases {
            assert_eq!(
                day_of_week(&[string_datum(input)]).unwrap(),
                want,
                "{input}"
            );
        }

        let day_of_year_cases = [
            ("2017-12-01", Datum::Int(335)),
            ("0000-00-00", Datum::Null),
            ("2018-00-00", Datum::Null),
            ("2017-00-00 12:12:12", Datum::Null),
            ("0000-00-00 12:12:12", Datum::Null),
            ("2000-01-01", Datum::Int(1)),
            ("2011-11-11", Datum::Int(315)),
            ("0000-01-01", Datum::Int(1)),
        ];
        for (input, want) in day_of_year_cases {
            assert_eq!(
                day_of_year(&[string_datum(input)]).unwrap(),
                want,
                "{input}"
            );
        }

        let day_of_month_cases = [
            ("2017-12-01", Datum::Int(1)),
            ("2000-01-01", Datum::Int(1)),
            ("2011-11-11", Datum::Int(11)),
            ("0000-01-01", Datum::Int(1)),
            ("2008-13-01", Datum::Null),
        ];
        for (input, want) in day_of_month_cases {
            assert_eq!(
                day_of_month(&[string_datum(input)]).unwrap(),
                want,
                "{input}"
            );
        }

        let quarter_cases = [
            ("2008-04-01", 2),
            ("2008-01-01", 1),
            ("2008-03-31", 1),
            ("2008-06-30", 2),
            ("2008-07-01", 3),
            ("2008-09-30", 3),
            ("2008-10-01", 4),
            ("2008-12-31", 4),
            ("0000-01-01", 1),
        ];
        for (input, want) in quarter_cases {
            assert_eq!(
                quarter(&[string_datum(input)]).unwrap(),
                Datum::Int(want),
                "{input}"
            );
        }
        assert_eq!(quarter(&[string_datum("2008-13-01")]).unwrap(), Datum::Null);

        let weekday_cases = [
            ("2000-01-01", Datum::Int(5)),
            ("2011-11-11", Datum::Int(4)),
            ("0000-01-01", Datum::Int(5)),
            ("0000-00-00", Datum::Null),
        ];
        for (input, want) in weekday_cases {
            assert_eq!(weekday(&[string_datum(input)]).unwrap(), want, "{input}");
        }

        assert_eq!(day_of_month(&[Datum::Null]).unwrap(), Datum::Null);
        assert_eq!(day_of_week(&[Datum::Null]).unwrap(), Datum::Null);
        assert_eq!(day_of_year(&[Datum::Null]).unwrap(), Datum::Null);
        assert_eq!(weekday(&[Datum::Null]).unwrap(), Datum::Null);
        assert_eq!(quarter(&[Datum::Null]).unwrap(), Datum::Null);
        assert_eq!(
            day_of_month(&[Datum::Int(20_240_315)]).unwrap(),
            Datum::Int(15)
        );
        assert_eq!(
            day_of_week(&[Datum::Int(20_240_315)]).unwrap(),
            Datum::Int(6)
        );
        assert_eq!(
            day_of_year(&[Datum::Int(20_240_315)]).unwrap(),
            Datum::Int(75)
        );
        assert_eq!(weekday(&[Datum::Int(20_240_315)]).unwrap(), Datum::Int(4));
        assert_eq!(quarter(&[Datum::Int(20_240_315)]).unwrap(), Datum::Int(1));
        assert!(day_of_week(&[]).is_err());
        assert!(quarter(&[string_datum("2008-01-01"), Datum::Int(1)]).is_err());
    }

    /// Exact scalar rows from `TestQuarter` at
    /// `pkg/expression/builtin_time_test.go:2781`.  The source context enables
    /// `IgnoreZeroInDate`, so the month-zero row is retained as quarter zero;
    /// typed temporal warnings and session mode state remain outside Datum.
    #[test]
    fn quarter_source_vectors() {
        for (input, want) in [
            ("2008-04-01", 2),
            ("2008-01-01", 1),
            ("2008-03-31", 1),
            ("2008-06-30", 2),
            ("2008-07-01", 3),
            ("2008-09-30", 3),
            ("2008-10-01", 4),
            ("2008-12-31", 4),
            ("2008-00-01", 0),
        ] {
            assert_eq!(
                quarter(&[string_datum(input)]).unwrap(),
                Datum::Int(want),
                "QUARTER({input:?})"
            );
        }
        assert_eq!(quarter(&[string_datum("2008-13-01")]).unwrap(), Datum::Null);
        assert_eq!(quarter(&[Datum::Null]).unwrap(), Datum::Null);
    }

    #[test]
    fn month_and_monthname_source_vectors() {
        // TestMonthName and the directly shared MONTH/MONTHNAME rows in
        // TestDate. MONTH's SQL-mode-dependent zero result and TestVecMonth's
        // vector/warning assertions remain explicit ledger gaps.
        let month_cases = [
            ("2000-01-01", Datum::Int(1)),
            ("2011-11-11", Datum::Int(11)),
            ("0000-01-01", Datum::Int(1)),
            ("2008-13-01", Datum::Null),
        ];
        for (input, want) in month_cases {
            assert_eq!(month(&[string_datum(input)]).unwrap(), want, "{input}");
        }

        let monthname_cases = [
            ("2017-12-01", Datum::new_string("December".to_string())),
            ("2017-00-01", Datum::Null),
            ("0000-00-00", Datum::Null),
            ("0000-00-00 00:00:00.000000", Datum::Null),
            ("0000-00-00 00:00:11.000000", Datum::Null),
            ("2000-01-01", Datum::new_string("January".to_string())),
            ("2011-11-11", Datum::new_string("November".to_string())),
            ("0000-01-01", Datum::new_string("January".to_string())),
            ("2008-13-01", Datum::Null),
        ];
        for (input, want) in monthname_cases {
            assert_eq!(monthname(&[string_datum(input)]).unwrap(), want, "{input}");
        }

        assert_eq!(month(&[Datum::Null]).unwrap(), Datum::Null);
        assert_eq!(monthname(&[Datum::Null]).unwrap(), Datum::Null);
        assert_eq!(month(&[Datum::Int(20_240_315)]).unwrap(), Datum::Int(3));
        assert_eq!(
            monthname(&[Datum::Int(20_240_315)]).unwrap(),
            Datum::new_string("March".to_string())
        );
        assert!(month(&[]).is_err());
        assert!(monthname(&[string_datum("2008-01-01"), Datum::Int(1)]).is_err());
    }

    struct FractionalClock;

    impl Columns for FractionalClock {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn now(&self) -> Option<(i64, u32, i32)> {
            // SET timestamp = 1700000000.654321 is a TypeFloat in Go and
            // materializes this binary-float nanosecond value.
            Some((1_700_000_000, 654_320_955, 0))
        }
    }

    #[test]
    fn current_time_truncates_to_microseconds_before_fsp_rounding() {
        let clock = FractionalClock;
        assert_eq!(
            current_time(&[Datum::Int(6)], &clock).unwrap(),
            Datum::new_string("22:13:20.654320".to_string())
        );
        assert_eq!(
            utc_time(&[Datum::Int(6)], &clock).unwrap(),
            Datum::new_string("22:13:20.654320".to_string())
        );
        assert_eq!(
            utc_timestamp(&[Datum::Int(6)], &clock).unwrap(),
            Datum::new_string("2023-11-14 22:13:20.654321".to_string()),
            "UTC_TIMESTAMP rounds raw nanoseconds instead of using the duration path"
        );
    }

    #[test]
    fn go_time_vectors_cover_duration_scale_and_clamp() {
        assert_eq!(
            sec_to_time(&[Datum::new_string("123.4".to_string())]).unwrap(),
            Datum::new_string("00:02:03.400000".to_string())
        );
        assert_eq!(
            sec_to_time(&[Datum::Real(86_401.4)]).unwrap(),
            Datum::new_string("24:00:01.4".to_string())
        );
        assert_eq!(
            maketime(&[
                Datum::Int(1_000),
                Datum::Int(1),
                Datum::Decimal(crate::Decimal::from_literal("1.0")),
            ])
            .unwrap(),
            Datum::new_string("838:59:59.0".to_string())
        );
        assert_eq!(
            time_format(&[
                Datum::new_string("1990-05-07 19:30:10".to_string()),
                Datum::new_string("%H %i %s".to_string()),
            ])
            .unwrap(),
            Datum::new_string("19 30 10".to_string())
        );
        assert_eq!(
            time_format(&[
                Datum::new_string("12:34:56".to_string()),
                Datum::new_string(String::new()),
            ])
            .unwrap(),
            Datum::Null
        );
    }

    /// Exact scalar rows from `TestTimeToSec` at
    /// `pkg/expression/builtin_time_test.go:3117`.  The source's typed
    /// duration result is represented here by its integer seconds; NULL and
    /// the accepted delimited/compact forms remain directly comparable.
    #[test]
    fn time_to_sec_source_vectors() {
        for (input, want) in [
            ("22:23:00", 80_580),
            ("00:39:38", 2_378),
            ("23:00", 82_800),
            ("00:00", 0),
            ("00:00:00", 0),
            ("23:59:59", 86_399),
            ("1:0", 3_600),
            ("1:00", 3_600),
            ("1:0:0", 3_600),
            ("-02:00", -7_200),
            ("-02:00:05", -7_205),
            ("020005", 7_205),
        ] {
            assert_eq!(
                time_to_sec(&[string_datum(input)]).unwrap(),
                Datum::Int(want),
                "TIME_TO_SEC({input:?})"
            );
        }
        assert_eq!(time_to_sec(&[Datum::Null]).unwrap(), Datum::Null);
    }

    /// Exact value-domain rows from `TestSecToTime` at
    /// `pkg/expression/builtin_time_test.go:3162`.  String FSP and natural
    /// scalar float precision are preserved; the source's explicit
    /// expression decimal override (`inputDecimal == -1`) is a typed metadata
    /// path and remains partial.
    #[test]
    fn sec_to_time_source_vectors() {
        for (input, want) in [
            (Datum::Int(2_378), "00:39:38"),
            (Datum::Int(3_864_000), "838:59:59"),
            (Datum::Int(-3_864_000), "-838:59:59"),
            (Datum::Real(86_401.4), "24:00:01.4"),
            (Datum::Real(-86_401.4), "-24:00:01.4"),
            (Datum::Real(86_401.543_21), "24:00:01.54321"),
            (string_datum("123.4"), "00:02:03.400000"),
            (string_datum("123.4567891"), "00:02:03.456789"),
            (string_datum("123"), "00:02:03.000000"),
            (string_datum("abc"), "00:00:00.000000"),
            (string_datum("1e-4"), "00:00:00.000100"),
            (string_datum("1e-5"), "00:00:00.000010"),
            (string_datum("1e-6"), "00:00:00.000001"),
            (string_datum("1e-7"), "00:00:00.000000"),
        ] {
            assert_eq!(
                sec_to_time(std::slice::from_ref(&input)).unwrap(),
                Datum::new_string(want.to_string()),
                "SEC_TO_TIME({input:?})"
            );
        }
        assert_eq!(sec_to_time(&[Datum::Null]).unwrap(), Datum::Null);
    }

    #[test]
    fn go_week_vectors_cover_year_boundaries() {
        assert_eq!(week_of_year(2008, 2, 20, 0, false), (2008, 7));
        assert_eq!(week_of_year(2008, 2, 20, 1, false), (2008, 8));
        assert_eq!(week_of_year(2020, 1, 1, 3, true), (2020, 1));
        assert_eq!(
            yearweek(&[Datum::new_string("2000-01-01".to_string()), Datum::Int(0)]).unwrap(),
            Datum::Int(199_952)
        );
        assert_eq!(
            calendar::date_format(
                &Datum::new_string("2020-01-01".to_string()),
                &Datum::new_string("%U %u %V %v %X %x".to_string()),
            )
            .unwrap(),
            Datum::new_string("00 01 52 01 2019 2020".to_string())
        );
    }

    /// Exact representable rows from `TestDayName` in
    /// `pkg/expression/builtin_time_test.go:458`.  The source evaluates an
    /// `ETDatetime` argument, so the value-only seed keeps ordinary calendar
    /// strings and NULL/arity domains while leaving zero-component handling to
    /// the StatementContext boundary documented by the evidence ledger.
    #[test]
    fn dayname_source_vectors() {
        let cases = [
            ("2017-12-01", Datum::new_string("Friday".to_string())),
            ("0000-12-01", Datum::new_string("Friday".to_string())),
            ("2017-00-01", Datum::Null),
            ("2017-01-00", Datum::Null),
            ("0000-00-00", Datum::Null),
            ("0000-00-00 00:00:00.000000", Datum::Null),
            ("0000-00-00 00:00:11.000000", Datum::Null),
        ];
        for (input, want) in cases {
            assert_eq!(dayname(&[string_datum(input)]).unwrap(), want, "{input}");
        }
        assert_eq!(dayname(&[Datum::Null]).unwrap(), Datum::Null);
        assert_eq!(
            dayname(&[Datum::Int(20_171_201)]).unwrap(),
            Datum::new_string("Friday".to_string())
        );
        assert!(dayname(&[]).is_err());
    }

    /// Full finite source table from `TestDateFormat` at line 604.  This is
    /// deliberately a string-valued temporal boundary: typed MySQL `Time`,
    /// invalid-zero SQL modes, and the warning/error path are not represented
    /// by `Datum` and remain explicit partial evidence rather than guessed.
    #[test]
    fn date_format_source_vectors() {
        let cases = [
            (
                "2010-01-07 23:12:34.12345",
                "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%",
                "Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01 01 01 Thu Thursday 4 2010 2010 2010 10 %",
            ),
            (
                "2012-12-21 23:12:34.123456",
                "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%",
                "Dec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51 51 51 51 Fri Friday 5 2012 2012 2012 12 %",
            ),
            (
                "0000-01-01 00:00:00.123456",
                "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %Y %y %%",
                "Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 52 0000 00 %",
            ),
            (
                "2016-09-3 00:59:59.123456",
                "abc%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y!123 %%xyz %z",
                "abcSep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35 35 35 35 Sat Saturday 6 2016 2016 2016 16!123 %xyz z",
            ),
            (
                "2012-10-01 00:00:00",
                "%b %M %m %c %D %d %e %j %k %H %i %p %r %T %s %f %v %x %Y %y %%",
                "Oct October 10 10 1st 01 1 275 0 00 00 AM 12:00:00 AM 00:00:00 00 000000 40 2012 2012 12 %",
            ),
        ];
        for (date, format, want) in cases {
            assert_eq!(
                calendar::date_format(&string_datum(date), &string_datum(format)).unwrap(),
                Datum::new_string(want.to_string()),
                "DATE_FORMAT({date}, {format})"
            );
        }
        assert_eq!(
            calendar::date_format(&Datum::Null, &string_datum("%Y-%M-%D")).unwrap(),
            Datum::Null
        );
    }

    /// Representable rows from `TestStrToDate` at
    /// `pkg/expression/builtin_time_test.go:1792`.  The Go function class
    /// chooses typed DATE/DATETIME/DURATION signatures from the format; this
    /// seed exposes the same parsed value as its canonical string while
    /// retaining NULL/invalid input, fractional-second, AM/PM, and skip-token
    /// behavior.
    #[test]
    fn str_to_date_source_vectors() {
        let cases = [
            (
                "10/28/2011 9:46:29 pm",
                "%m/%d/%Y %l:%i:%s %p",
                Some("2011-10-28 21:46:29"),
            ),
            (
                "10/28/2011 9:46:29 Pm",
                "%m/%d/%Y %l:%i:%s %p",
                Some("2011-10-28 21:46:29"),
            ),
            (
                "2011/10/28 9:46:29 am",
                "%Y/%m/%d %l:%i:%s %p",
                Some("2011-10-28 09:46:29"),
            ),
            (
                "20161122165022",
                "%Y%m%d%H%i%s",
                Some("2016-11-22 16:50:22"),
            ),
            (
                "2016 11 22 16 50 22",
                "%Y%m%d%H%i%s",
                Some("2016-11-22 16:50:22"),
            ),
            (
                "16-50-22 2016 11 22",
                "%H-%i-%s%Y%m%d",
                Some("2016-11-22 16:50:22"),
            ),
            ("16-50 2016 11 22", "%H-%i-%s%Y%m%d", None),
            (
                "15-01-2001 1:59:58.999",
                "%d-%m-%Y %I:%i:%s.%f",
                Some("2001-01-15 01:59:58.999000"),
            ),
            (
                "15-01-2001 1:59:58.1",
                "%d-%m-%Y %H:%i:%s.%f",
                Some("2001-01-15 01:59:58.100000"),
            ),
            (
                "15-01-2001 1:59:58.",
                "%d-%m-%Y %H:%i:%s.%f",
                Some("2001-01-15 01:59:58.000000"),
            ),
            (
                "15-01-2001 1:9:8.999",
                "%d-%m-%Y %H:%i:%s.%f",
                Some("2001-01-15 01:09:08.999000"),
            ),
            (
                "15-01-2001 1:9:8.999",
                "%d-%m-%Y %H:%i:%S.%f",
                Some("2001-01-15 01:09:08.999000"),
            ),
            (
                "2003-01-02 10:11:12.0012",
                "%Y-%m-%d %H:%i:%S.%f",
                Some("2003-01-02 10:11:12.001200"),
            ),
            ("2003-01-02 10:11:12 PM", "%Y-%m-%d %H:%i:%S %p", None),
            ("10:20:10AM", "%H:%i:%S%p", None),
            ("2020-10-10ABCD", "%Y-%m-%d%@", Some("2020-10-10")),
            ("2020-10-101234", "%Y-%m-%d%#", Some("2020-10-10")),
            ("2020-10-10....", "%Y-%m-%d%.", Some("2020-10-10")),
            ("2020-10-10.1", "%Y-%m-%d%.%#%@", Some("2020-10-10")),
            ("abcd2020-10-10.1", "%@%Y-%m-%d%.%#%@", Some("2020-10-10")),
            ("abcd-2020-10-10.1", "%@-%Y-%m-%d%.%#%@", Some("2020-10-10")),
            ("2020-10-10", "%Y-%m-%d%@", Some("2020-10-10")),
            (
                "2020-10-10abcde123abcdef",
                "%Y-%m-%d%@%#",
                Some("2020-10-10"),
            ),
            (
                "12:3:56pm  13/05/2019",
                "%r %d/%c/%Y",
                Some("2019-05-13 12:03:56"),
            ),
            ("11:13:56 am", "%r", Some("11:13:56")),
            (
                "12:13:56 13/05/2019",
                "%T %d/%c/%Y",
                Some("2019-05-13 12:13:56"),
            ),
            (
                "19:3:56  13/05/2019",
                "%T %d/%c/%Y",
                Some("2019-05-13 19:03:56"),
            ),
            ("21:13:24", "%T", Some("21:13:24")),
        ];
        for (date, format, want) in cases {
            let got = calendar::str_to_date(&[string_datum(date), string_datum(format)]).unwrap();
            let want = want.map_or(Datum::Null, |want| Datum::new_string(want.to_string()));
            assert_eq!(got, want, "STR_TO_DATE({date:?}, {format:?})");
        }
        assert_eq!(
            calendar::str_to_date(&[Datum::Null, string_datum("%Y")]).unwrap(),
            Datum::Null
        );
        assert!(calendar::str_to_date(&[string_datum("2020")]).is_err());
    }

    /// Exact source rows from `TestFromDays` at
    /// `pkg/expression/builtin_time_test.go:1864`.  The evaluator keeps the
    /// result as a date-shaped string; the Go function's typed DATE result and
    /// warning/SQL-mode state remain outside the value-only boundary.
    #[test]
    fn from_days_source_vectors() {
        for (day, want) in [
            (-140, "0000-00-00"),
            (140, "0000-00-00"),
            (735_000, "2012-05-12"),
            (735_030, "2012-06-11"),
            (735_130, "2012-09-19"),
            (734_909, "2012-02-11"),
            (734_878, "2012-01-11"),
            (734_927, "2012-02-29"),
            (734_634, "2011-05-12"),
            (734_664, "2011-06-11"),
            (734_764, "2011-09-19"),
            (734_544, "2011-02-11"),
            (734_513, "2011-01-11"),
            (3_652_424, "9999-12-31"),
        ] {
            assert_eq!(
                calendar::from_days(&[Datum::Int(day)]).unwrap(),
                Datum::new_string(want.to_string()),
                "FROM_DAYS({day})"
            );
        }
        assert_eq!(
            calendar::from_days(&[Datum::Int(3_652_425)]).unwrap(),
            Datum::Null
        );
        for (input, want) in [
            ("z550z", "0000-00-00"),
            ("6500z", "0017-10-18"),
            ("440", "0001-03-16"),
        ] {
            assert_eq!(
                calendar::from_days(&[string_datum(input)]).unwrap(),
                Datum::new_string(want.to_string()),
                "FROM_DAYS({input:?})"
            );
        }
        assert_eq!(calendar::from_days(&[Datum::Null]).unwrap(), Datum::Null);
    }

    /// Exact scalar rows from `TestDateDiff` at
    /// `pkg/expression/builtin_time_test.go:1932`.  DATE/TIME typed datum
    /// conversion and warning state are outside this value-only boundary;
    /// the source's valid and invalid string pairs remain directly
    /// representable here.
    #[test]
    fn date_diff_source_vectors() {
        for ((left, right), want) in [
            (("2004-05-21", "2004:01:02"), 140),
            (("2004-04-21", "2000:01:02"), 1_571),
            (
                ("2008-12-31 23:59:59.000001", "2008-12-30 01:01:01.000002"),
                1,
            ),
            (("1010-11-30 23:59:59", "2010-12-31"), -365_274),
            (("1010-11-30", "2210-11-01"), -438_262),
        ] {
            assert_eq!(
                calendar::date_diff(&[string_datum(left), string_datum(right)]).unwrap(),
                Datum::Int(want),
                "DATEDIFF({left:?}, {right:?})"
            );
        }
        for (left, right) in [
            ("2004-05-21", "abcdefg"),
            ("2007-12-31 23:59:59", "23:59:59"),
            ("2007-00-31 23:59:59", "2016-01-13"),
            ("2007-10-31 23:59:59", "2016-01-00"),
            ("2007-10-31 23:59:59", "99999999-01-00"),
        ] {
            assert_eq!(
                calendar::date_diff(&[string_datum(left), string_datum(right)]).unwrap(),
                Datum::Null,
                "DATEDIFF({left:?}, {right:?})"
            );
        }
        assert_eq!(
            calendar::date_diff(&[Datum::Null, string_datum("2004-01-01")]).unwrap(),
            Datum::Null
        );
    }

    /// Exact scalar rows from `TestTimestampDiff` at
    /// `pkg/expression/builtin_time_test.go:2130`.  The source evaluates
    /// typed DATETIME arguments and StatementContext zero-date flags; these
    /// string-valued rows preserve the integer results and NULL boundary
    /// without inventing warning or SQL-mode state.
    #[test]
    fn timestamp_diff_source_vectors() {
        for ((unit, left, right), want) in [
            (("MONTH", "2003-02-01", "2003-05-01"), 3),
            (("YEAR", "2002-05-01", "2001-01-01"), -1),
            (("MINUTE", "2003-02-01", "2003-05-01 12:05:55"), 128_885),
        ] {
            assert_eq!(
                calendar::timestamp_diff(&[
                    string_datum(unit),
                    string_datum(left),
                    string_datum(right),
                ])
                .unwrap(),
                Datum::Int(want),
                "TIMESTAMPDIFF({unit:?}, {left:?}, {right:?})"
            );
        }
        for (unit, left, right) in [
            ("MONTH", "2003-00-01", "2003-05-01"),
            ("MONTH", "2003-02-01", "2003-05-00"),
        ] {
            assert_eq!(
                calendar::timestamp_diff(&[
                    string_datum(unit),
                    string_datum(left),
                    string_datum(right),
                ])
                .unwrap(),
                Datum::Null,
                "TIMESTAMPDIFF({unit:?}, {left:?}, {right:?})"
            );
        }
        assert_eq!(
            calendar::timestamp_diff(&[
                string_datum("DAY"),
                Datum::Null,
                string_datum("2017-01-01"),
            ])
            .unwrap(),
            Datum::Null
        );
    }

    /// Exact scalar rows from `TestToSeconds` at
    /// `pkg/expression/builtin_time_test.go:2860`.  The source evaluates a
    /// typed DATETIME and enables `IgnoreZeroInDate`; this keeps ordinary
    /// numeric/string dates, two-digit-year expansion, invalid temporal
    /// strings, and NULL results while leaving warnings and type metadata to
    /// the explicit partial boundary.
    #[test]
    fn to_seconds_source_vectors() {
        for (input, want) in [
            (Datum::Int(950501), 62_966_505_600),
            (string_datum("2009-11-29"), 63_426_672_000),
            (string_datum("2009-11-29 13:43:32"), 63_426_721_412),
            (string_datum("09-11-29 13:43:32"), 63_426_721_412),
            (string_datum("99-11-29 13:43:32"), 63_111_102_212),
        ] {
            assert_eq!(calendar::to_seconds(&[input]).unwrap(), Datum::Int(want),);
        }
        for input in [
            "0000-00-00",
            "1992-13-00",
            "2007-10-07 23:59:61",
            "1998-10-00",
            "1998-00-11",
            "123456789",
        ] {
            assert_eq!(
                calendar::to_seconds(&[string_datum(input)]).unwrap(),
                Datum::Null,
                "TO_SECONDS({input:?})"
            );
        }
        assert_eq!(calendar::to_seconds(&[Datum::Null]).unwrap(), Datum::Null);
    }

    /// Exact scalar rows from `TestToDays` at
    /// `pkg/expression/builtin_time_test.go:2903`.  The source uses the
    /// zero-date `TimestampDiff("DAY", ...)` path, so year-zero January 1 is
    /// retained while all invalid-zero/malformed temporal inputs stay NULL.
    #[test]
    fn to_days_source_vectors() {
        for (input, want) in [
            (Datum::Int(950501), 728_779),
            (string_datum("2007-10-07"), 733_321),
            (string_datum("2008-10-07"), 733_687),
            (string_datum("08-10-07"), 733_687),
            (string_datum("0000-01-01"), 1),
            (string_datum("2007-10-07 00:00:59"), 733_321),
        ] {
            assert_eq!(calendar::to_days(&[input]).unwrap(), Datum::Int(want));
        }
        for input in [
            "0000-00-00",
            "1992-13-00",
            "2007-10-07 23:59:61",
            "1998-10-00",
            "123456789",
        ] {
            assert_eq!(
                calendar::to_days(&[string_datum(input)]).unwrap(),
                Datum::Null,
                "TO_DAYS({input:?})"
            );
        }
        assert_eq!(calendar::to_days(&[Datum::Null]).unwrap(), Datum::Null);
    }

    /// Exact scalar rows from `TestTimeDiff` at
    /// `pkg/expression/builtin_time_test.go:1985`.  The Go suite also checks
    /// typed result FSP and StatementContext warnings; those metadata paths
    /// remain explicit partial evidence rather than being guessed here.
    #[test]
    fn time_diff_source_vectors() {
        for ((left, right), want) in [
            (
                ("2000:01:01 00:00:00", "2000:01:01 00:00:00.000001"),
                "-00:00:00.000001",
            ),
            (
                ("2008-12-31 23:59:59.000001", "2008-12-30 01:01:01.000002"),
                "46:58:57.999999",
            ),
            (("2016-12-00 12:00:00", "2016-12-01 12:00:00"), "-24:00:00"),
            (("10:10:10", "10:9:0"), "00:01:10"),
            (("00:00:00.000000", "00:00:00.000001"), "-00:00:00.000001"),
        ] {
            assert_eq!(
                time_diff(&[string_datum(left), string_datum(right)]).unwrap(),
                Datum::new_string(want.to_string()),
                "TIMEDIFF({left:?}, {right:?})"
            );
        }
        for (left, right) in [
            ("2016-12-00 12:00:00", "10:9:0"),
            ("2016-12-00 12:00:00", ""),
        ] {
            assert_eq!(
                time_diff(&[string_datum(left), string_datum(right)]).unwrap(),
                Datum::Null,
                "TIMEDIFF({left:?}, {right:?})"
            );
        }
        assert_eq!(
            time_diff(&[Datum::Null, string_datum("00:00:00")]).unwrap(),
            Datum::Null
        );
    }

    /// Explicit-mode rows from `TestWeek` at line 2035, including the source
    /// NULL-mode normalization to mode zero.
    #[test]
    fn week_source_vectors() {
        for ((date, mode), want) in [
            (("2008-02-20", 0), 7),
            (("2008-02-20", 1), 8),
            (("2008-12-31", 1), 53),
        ] {
            assert_eq!(
                week(&[string_datum(date), Datum::Int(mode)], 0).unwrap(),
                Datum::Int(want),
                "WEEK({date}, {mode})"
            );
        }
        assert_eq!(
            week(&[string_datum("2023-01-01"), Datum::Null], 0,).unwrap(),
            Datum::Int(1)
        );
    }

    /// Normal string/numeric rows from `TestLastDay` at line 3371.  The
    /// source's day-zero result changes with SQLMode and therefore stays
    /// outside this value-only test; malformed time-of-day input is still
    /// representable and must not be silently accepted.
    #[test]
    fn last_day_source_vectors() {
        for (input, want) in [
            ("2003-02-05", "2003-02-28"),
            ("2004-02-05", "2004-02-29"),
            ("2004-01-01 01:01:01", "2004-01-31"),
        ] {
            assert_eq!(
                last_day(&[string_datum(input)]).unwrap(),
                Datum::new_string(want.to_string()),
                "LAST_DAY({input})"
            );
        }
        assert_eq!(
            last_day(&[Datum::Int(950501)]).unwrap(),
            Datum::new_string("1995-05-31".to_string())
        );
        for input in [
            "0000-00-00",
            "1992-13-00",
            "2007-10-07 23:59:61",
            "2005-00-00",
            "2005-00-01",
            "2243-01 00:00:00",
            "123456789",
        ] {
            assert_eq!(
                last_day(&[string_datum(input)]).unwrap(),
                Datum::Null,
                "LAST_DAY({input})"
            );
        }
        assert_eq!(last_day(&[Datum::Null]).unwrap(), Datum::Null);
    }

    #[test]
    fn period_arithmetic_matches_go_vectors_and_null_ordering() {
        // `TestPeriodAdd` and `TestPeriodDiff` in
        // `pkg/expression/builtin_time_test.go`.
        let add_cases = [
            ((201611, 2), 201701),
            ((201611, 3), 201702),
            ((201611, -13), 201510),
            ((1611, 3), 201702),
            ((7011, 3), 197102),
        ];
        for ((period, months), want) in add_cases {
            assert_eq!(
                period_add(&[Datum::Int(period), Datum::Int(months)]).unwrap(),
                Datum::Int(want)
            );
        }
        assert!(period_add(&[Datum::Int(0), Datum::Int(3)]).is_err());
        assert_eq!(
            period_add(&[Datum::Int(0), Datum::Null]).unwrap(),
            Datum::Null,
            "both arguments are evaluated before TiDB validates the period"
        );

        let diff_cases = [
            ((201611, 201611), 0),
            ((200802, 200703), 11),
            ((201701, 201611), 2),
            ((201702, 201611), 3),
            ((201510, 201611), -13),
            ((201702, 1611), 3),
            ((197102, 7011), 3),
        ];
        for ((period1, period2), want) in diff_cases {
            assert_eq!(
                period_diff(&[Datum::Int(period1), Datum::Int(period2)]).unwrap(),
                Datum::Int(want)
            );
        }
        assert!(period_diff(&[Datum::Int(0), Datum::Int(201611)]).is_err());
        assert_eq!(
            period_diff(&[Datum::Null, Datum::Int(201611)]).unwrap(),
            Datum::Null
        );
    }

    #[test]
    fn period_arithmetic_retains_go_unsigned_wrapping() {
        // Direct `goeval` probes for the Go uint64 helper / int64 conversion
        // boundary in `builtinPeriodAddSig` and `builtinPeriodDiffSig`.
        assert_eq!(
            period_add(&[Datum::Int(i64::MAX), Datum::Int(1)]).unwrap(),
            Datum::Int(i64::MIN)
        );
        assert_eq!(
            period_diff(&[Datum::Int(i64::MAX), Datum::Int(197001)]).unwrap(),
            Datum::Int(1_106_804_644_422_549_462)
        );
    }
}
