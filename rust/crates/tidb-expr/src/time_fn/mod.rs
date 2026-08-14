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
//!
//! `ADDTIME`, `SUBTIME`, `TIMESTAMP`, `TIMESTAMPADD` and `SYSDATE` -- the
//! five that Go types from the argument `FieldType`s rather than from their
//! values -- live in [`add_sub`], with the microsecond value domain they
//! need in [`duration_parse`].

pub(crate) mod add_sub;
pub(crate) mod calendar;
mod convert_tz;
pub(crate) mod duration_parse;
mod session_tz;

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
        // `pkg/expression/builtin.go:722-725` binds all four names to the SAME
        // `nowFunctionClass`, so LOCALTIME and LOCALTIMESTAMP are NOW down to
        // the optional fsp argument and the statement-timestamp clock. Go's
        // capture: `select localtime(), localtimestamp(), localtime,
        // localtimestamp, now()` prints one value five times, and
        // `localtime() = now()` is 1.
        "NOW" | "CURRENT_TIMESTAMP" | "LOCALTIME" | "LOCALTIMESTAMP" => now(vals, cols),
        "UTC_TIMESTAMP" => utc_timestamp(vals, cols),
        "CURDATE" | "CURRENT_DATE" => current_date(vals, cols),
        "UTC_DATE" => utc_date(vals, cols),
        "CURTIME" | "CURRENT_TIME" => current_time(vals, cols),
        "UTC_TIME" => utc_time(vals, cols),
        "DATE" => date(vals, cols),
        "MICROSECOND" => microsecond(vals),
        "TIME" => time(vals, cols),
        "MONTH" => month(vals),
        "DAY" | "DAYOFMONTH" => day_of_month(vals),
        "DAYOFWEEK" => day_of_week(vals),
        "DAYOFYEAR" => day_of_year(vals),
        "WEEKDAY" => weekday(vals),
        "QUARTER" => quarter(vals),
        "WEEK" => week(vals, cols.default_week_format()),
        "WEEKOFYEAR" => week_of_year_builtin(vals),
        "TIDB_PARSE_TSO_LOGICAL" => tidb_parse_tso_logical(vals),
        "TIDB_CURRENT_TSO" => current_tso(vals, cols),
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
        "STR_TO_DATE" => calendar::str_to_date(vals, cols),
        "FROM_DAYS" => calendar::from_days(vals),
        "TIMEDIFF" => time_diff(vals),
        "CONVERT_TZ" => convert_tz::convert_tz(vals),
        "FROM_UNIXTIME" => session_tz::from_unixtime(vals, cols),
        "UNIX_TIMESTAMP" => session_tz::unix_timestamp(vals, cols),
        "TIDB_PARSE_TSO" => session_tz::tidb_parse_tso(vals, cols),
        "TIMESTAMPDIFF" => calendar::timestamp_diff(vals),
        // `ADDTIME`/`SUBTIME` reach here with no static argument types, so
        // every argument takes Go's `default` branch -- which is the branch
        // Go itself selects for a string constant. The chunk tier, which
        // does have the types, enters through [`add_sub::add_sub_time`]
        // directly; see that module's doc for the row/vec split this
        // `row_path = true` selects.
        "ADDTIME" | "SUBTIME" => add_sub::add_sub_untyped(name, vals, cols),
        "TIMESTAMP" => add_sub::timestamp(vals, cols),
        "TIMESTAMPADD" => add_sub::timestamp_add(vals, cols),
        "SYSDATE" => add_sub::sysdate(vals, cols),
        "TO_DAYS" => calendar::to_days(vals),
        "TO_SECONDS" => calendar::to_seconds(vals),
        // `EXTRACT(<composite unit> FROM value)`, e.g. `HOUR_MINUTE`,
        // `DAY_SECOND`, `YEAR_MONTH` — see `calendar::extract_composite`'s
        // own doc.
        "YEAR_MONTH" | "DAY_HOUR" | "DAY_MINUTE" | "DAY_SECOND" | "DAY_MICROSECOND"
        | "HOUR_MINUTE" | "HOUR_SECOND" | "HOUR_MICROSECOND" | "MINUTE_SECOND"
        | "MINUTE_MICROSECOND" | "SECOND_MICROSECOND" => calendar::extract_composite(name, vals),
        _ => return None,
    })
}

/// `TIDB_CURRENT_TSO()`: the active transaction's start timestamp, or zero
/// when the session is not inside a transaction.
fn current_tso(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if !vals.is_empty() {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    Ok(Datum::Int(cols.current_tso()))
}

/// `DATE(expr)`, after Go's declared `ETDatetime` argument cast has produced
/// a typed temporal value. The function applies its own zero-date SQL-mode
/// checks, clears the clock, and changes the result domain to `DATE`.
fn date(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    let [value] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    let Datum::Time(mut value) = value else {
        return if matches!(value, Datum::Null) {
            Ok(Datum::Null)
        } else {
            Err(EvalError::Unsupported(
                "DATE argument reached the signature without its ETDatetime cast",
            ))
        };
    };

    let modes = cols.date_modes();
    if (value.is_zero() && modes.no_zero_date)
        || (!value.is_zero() && value.invalid_zero() && modes.no_zero_in_date)
    {
        cols.handle_truncate(&format!("Incorrect datetime value: '{value}'"))?;
        return Ok(Datum::Null);
    }

    let core = value.core_time();
    value.set_core_time(tidb_datatype::CoreTime::from_date(
        u16::try_from(core.year()).expect("a typed temporal value has a nonnegative year"),
        core.month(),
        core.day(),
        0,
        0,
        0,
        0,
    ));
    value.set_kind(tidb_datatype::TimeType::Date);
    Ok(Datum::Time(value))
}

/// `builtinNowWithArgSig` / `builtinNowWithoutArgSig`: local
/// (`time_zone`-adjusted) statement time, always truncating fractional
/// seconds. `CURRENT_TIMESTAMP` is the same function class.
fn now(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    let fsp = parse_fsp_with_null_as_zero(vals)?.unwrap_or(0);
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
    let fsp = parse_fsp_with_null_as_zero(vals)?.unwrap_or(0);
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
    let fsp = parse_fsp_with_null_as_zero(vals)?;
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
    if matches!(vals, [Datum::Null]) {
        return Ok(Datum::Null);
    }
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

/// `builtinMicroSecondSig.evalInt`: read the fractional component of the
/// ETDuration argument. Go deliberately suppresses a duration-cast error and
/// returns NULL, unlike `TIME()` which reports the same truncation through the
/// statement context.
fn microsecond(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(value) = coerce_str(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let fsp = duration_parse::get_fsp(&value);
    Ok(match duration_parse::parse_duration(&value, fsp) {
        Ok(duration) => Datum::Int(duration.micro_second()),
        Err(_) => Datum::Null,
    })
}

/// `builtinTimeSig.evalDuration`: parse the string as a TiDB duration while
/// preserving its written FSP. `ErrTruncatedWrongVal` is a statement warning
/// for a SELECT and leaves Go's zero-value duration as the result.
fn time(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(value) = coerce_str(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let fsp = duration_parse::get_fsp(&value);
    match duration_parse::parse_duration(&value, fsp) {
        Ok(duration) => Ok(Datum::new_string(duration.format())),
        Err(_) => {
            cols.handle_truncate(&format!("Truncated incorrect time value: '{value}'"))?;
            Ok(Datum::new_string("00:00:00".to_owned()))
        }
    }
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

fn parse_fsp_with_null_as_zero(vals: &[Datum]) -> Result<Option<u32>, EvalError> {
    if matches!(vals, [Datum::Null]) {
        Ok(Some(0))
    } else {
        parse_fsp(vals)
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
/// The source returns the stored month field directly with no zero
/// rejection, because `monthFunctionClass` declares its argument
/// `types.ETDatetime` (`builtin_time.go:1116`) and so receives a value
/// `EvalTime` already produced non-NULL — see [`calendar::component_date`].
fn month(vals: &[Datum]) -> Result<Datum, EvalError> {
    Ok(calendar::component_date(vals)?
        .map_or(Datum::Null, |(_, month, _)| Datum::Int(i64::from(month))))
}

/// `builtinDayOfMonthSig.evalInt` in `pkg/expression/builtin_time.go`.
///
/// The source returns the stored day field directly with no zero rejection,
/// because `dayOfMonthFunctionClass` declares its argument
/// `types.ETDatetime` (`builtin_time.go:1284`) and so receives a value
/// `EvalTime` already produced non-NULL — see [`calendar::component_date`].
fn day_of_month(vals: &[Datum]) -> Result<Datum, EvalError> {
    Ok(calendar::component_date(vals)?
        .map_or(Datum::Null, |(_, _, day)| Datum::Int(i64::from(day))))
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

/// `builtinQuarterSig.evalInt` = `(date.Month() + 2) / 3`, returning 1-4 for
/// a real date and `0` for a month-zero one — with no zero rejection, because
/// `quarterFunctionClass` declares its argument `types.ETDatetime`
/// (`builtin_time.go:5833`) and so receives a value `EvalTime` already
/// produced non-NULL. Real TiDB's recorded `QUARTER(v1)` over a
/// zero-datetime column is `0`; `gorun` confirms `QUARTER(20240000)` is `0`
/// too, by the same stored month.
///
/// The month-zero string form no longer needs a parser of its own here: the
/// ETDatetime cast ([`crate::arg_eval_type`]) is what decides a string, and
/// it is `types.ParseTime` under the READ path's `IgnoreZeroInDate`, which
/// keeps a zero month exactly as Go does.
fn quarter(vals: &[Datum]) -> Result<Datum, EvalError> {
    Ok(
        calendar::component_date(vals)?.map_or(Datum::Null, |(_, month, _)| {
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

/// The low 18 bits of a TSO carry its logical component (`oracle`'s
/// `physicalShiftBits = 18`, `logicalBits = (1<<18)-1`).
const TSO_LOGICAL_BITS: i64 = (1 << 18) - 1;

/// `TIDB_PARSE_TSO_LOGICAL(tso)`. Port of `builtinTidbParseTsoLogicalSig` =
/// `oracle.ExtractLogical`: the low 18 bits of the timestamp oracle value. A
/// non-positive or NULL argument yields NULL. Session-independent (the physical
/// half, `TIDB_PARSE_TSO`, is not, because it renders a datetime in the session
/// time zone). For a positive `tso`, masking the low 18 bits as `i64` equals
/// Go's `uint64(tso) & logicalBits`.
fn tidb_parse_tso_logical(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(tso) = int_arg(&vals[0])? else {
        return Ok(Datum::Null);
    };
    if tso <= 0 {
        return Ok(Datum::Null);
    }
    Ok(Datum::Int(tso & TSO_LOGICAL_BITS))
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
        Datum::Duration(value) => {
            usize::try_from(value.fsp()).expect("duration FSP is nonnegative")
        }
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
mod clock_source_tests {
    use std::cell::RefCell;

    use super::*;

    #[derive(Default)]
    struct WarningContext {
        warnings: RefCell<Vec<(u16, String)>>,
    }

    impl Columns for WarningContext {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
    }

    fn source_eval(name: &str, args: &[Datum], ctx: &WarningContext) -> Result<Datum, EvalError> {
        crate::func::eval_func_values_in(name, args, ctx)
            .or_else(|| dispatch(name, args, ctx))
            .expect("TestClock builtin must be dispatched")
    }

    fn string(value: &str) -> Datum {
        Datum::new_string(value.to_owned())
    }

    /// Exact Go `TestClock`: HOUR, MINUTE, SECOND, MICROSECOND and TIME over
    /// its three source values, every NULL arm, and the malformed TIME warning.
    #[test]
    fn test_clock() {
        let ctx = WarningContext::default();
        for (input, hour, minute, second, micros, time) in [
            ("10:10:10.123456", 10, 10, 10, 123_456, "10:10:10.123456"),
            ("11:11:11.11", 11, 11, 11, 110_000, "11:11:11.11"),
            ("2010-10-10 11:11:11.11", 11, 11, 11, 110_000, "11:11:11.11"),
        ] {
            let args = [string(input)];
            assert_eq!(source_eval("HOUR", &args, &ctx).unwrap(), Datum::Int(hour));
            assert_eq!(
                source_eval("MINUTE", &args, &ctx).unwrap(),
                Datum::Int(minute)
            );
            assert_eq!(
                source_eval("SECOND", &args, &ctx).unwrap(),
                Datum::Int(second)
            );
            assert_eq!(
                source_eval("MICROSECOND", &args, &ctx).unwrap(),
                Datum::Int(micros)
            );
            assert_eq!(source_eval("TIME", &args, &ctx).unwrap(), string(time));
        }

        for name in ["HOUR", "MINUTE", "SECOND", "MICROSECOND", "TIME"] {
            assert_eq!(
                source_eval(name, &[Datum::Null], &ctx).unwrap(),
                Datum::Null
            );
        }

        let malformed = [string("2011-11-11 10:10:10.11.12")];
        for name in ["HOUR", "MINUTE", "SECOND", "MICROSECOND"] {
            assert_eq!(source_eval(name, &malformed, &ctx).unwrap(), Datum::Null);
        }
        let warning_count = ctx.warnings.borrow().len();
        assert_eq!(
            source_eval("TIME", &malformed, &ctx).unwrap(),
            string("00:00:00")
        );
        assert_eq!(ctx.warnings.borrow().len(), warning_count + 1);
        assert_eq!(
            ctx.warnings.borrow().last(),
            Some(&(
                1292,
                "Truncated incorrect time value: '2011-11-11 10:10:10.11.12'".to_owned()
            ))
        );
    }
}

#[cfg(test)]
mod tests;
