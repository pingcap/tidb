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

//! Calendar arithmetic shared by the source-owned time family and the
//! remaining generic date syntax in `crate::func`.

use crate::cast::to_i64_signed;
use crate::coerce::coerce_str;
use crate::{Datum, EvalError};

/// The calendar `(year, month, day)` parsed from a single-argument
/// date-part function's argument: `NULL` if it doesn't coerce to a string
/// or doesn't parse as a valid date (see [`parse_date_ymd`]).
pub(crate) fn date_part(
    vals: &[Datum],
    f: impl Fn((i64, u32, u32)) -> i64,
) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(s) = coerce_str(&vals[0])? else {
        return Ok(Datum::Null);
    };
    match parse_date_ymd(&s) {
        Some(ymd) => Ok(Datum::Int(f(ymd))),
        None => Ok(Datum::Null),
    }
}

/// Parses a date or datetime string's calendar date into `(year, month,
/// day)`, calendar-validated (month 1-12, day valid for that specific
/// month/year, including leap years) — `None` if it doesn't look like one.
/// Mirrors MySQL's lenient separator handling (any run of non-digit
/// characters between the numeric components, confirmed via `goeval` to
/// accept `-`, `/`, and `.`) and leading/trailing whitespace tolerance. A
/// trailing time-of-day component (space-separated) is accepted but
/// ignored here — this function is deliberately scoped to the DATE part
/// only; see [`parse_hms_extended`] for `HOUR`/`MINUTE`/`SECOND`
/// extraction, which needs a GENUINELY different algorithm (real TiDB's
/// behavior on a string with no time component is non-obvious — e.g.
/// `MINUTE('2021-01-01')` is NOT `0` — so it does not simply call this
/// function and default a missing time to midnight).
///
/// A bare, separator-less digit run of EXACTLY 6 or 8 digits (e.g.
/// `20240315` — including from an integer literal argument like
/// `YEAR(20240315)`, `Datum::Int` already coerces to this same decimal
/// string form) is a SEPARATE, positional `YYMMDD`/`YYYYMMDD` reading —
/// confirmed via `goeval`, not assumed, and NOT limited to `HOUR`/
/// `MINUTE`/`SECOND`'s own colon-less path, which this function does not
/// share (that path decodes an ELAPSED-time magnitude via modulo
/// arithmetic; this one slices fixed-width calendar fields). See
/// [`expand_year`] for the 2-digit-year century pivot this shares with
/// the separator-based path below.
pub(crate) fn parse_date_ymd(s: &str) -> Option<(i64, u32, u32)> {
    let input = s.trim();
    let date = input
        .split_once(char::is_whitespace)
        .map_or(input, |(date, _)| date);
    let bare = matches!(date.len(), 6 | 8) && date.bytes().all(|byte| byte.is_ascii_digit());
    let (year, month, day) = if bare {
        let year_digits = date.len() - 4;
        let (year, rest) = date.split_at(year_digits);
        let (month, day) = rest.split_at(2);
        (
            expand_year(year.parse().ok()?, year_digits),
            month.parse().ok()?,
            day.parse().ok()?,
        )
    } else {
        let parts = split_numeric_components(date)?;
        let [(year, year_digits), (month, _), (day, _)] = parts.as_slice() else {
            return None;
        };
        (expand_year(*year, *year_digits), *month, *day)
    };
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    Some((year, month, day))
}

/// Parses the month-zero date form accepted by `QUARTER` when TiDB's
/// `IgnoreZeroInDate` flag is enabled.  Only month zero is relaxed here;
/// nonzero months continue through strict calendar validation.
pub(crate) fn parse_date_with_zero_month(s: &str) -> Option<(i64, u32, u32)> {
    if let Some(date) = parse_date_ymd(s) {
        return Some(date);
    }
    let input = s.trim();
    let date = input
        .split_once(char::is_whitespace)
        .map_or(input, |(date, _)| date);
    let parts = split_numeric_components(date)?;
    let [(year, year_digits), (month, _), (day, _)] = parts.as_slice() else {
        return None;
    };
    if *month == 0 {
        if *day == 0 || *day > 31 {
            return None;
        }
        return Some((expand_year(*year, *year_digits), 0, *day));
    }
    None
}

/// The `(hour, minute, second)` extracted from a single-argument time-part
/// function's argument (`HOUR`/`MINUTE`/`SECOND`): `NULL` if it doesn't
/// coerce to a string or doesn't parse (see [`parse_hms_extended`]).
pub(crate) fn time_part(
    vals: &[Datum],
    f: impl Fn((u32, u32, u32)) -> i64,
) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(s) = coerce_str(&vals[0])? else {
        return Ok(Datum::Null);
    };
    match parse_hms_extended(&s) {
        Some(hms) => Ok(Datum::Int(f(hms))),
        None => Ok(Datum::Null),
    }
}

/// Parses `HOUR`/`MINUTE`/`SECOND`'s single argument into `(hour, minute,
/// second)`, following real TiDB's own two-path algorithm (confirmed via
/// `goeval`, not assumed) depending on whether the value contains a `:`:
///
/// - WITH a `:`: an optional `[DATE ]` prefix (validated the SAME way
///   [`parse_date_ymd`] validates a DATE, split on the FIRST whitespace —
///   `'junk 10:30:45'`'s `junk` prefix makes the WHOLE value invalid, not
///   just ignored) followed by a REQUIRED `H:M[:S]` time-of-day (`S`
///   defaults to `0` if omitted). `H` may be MULTI-DIGIT and exceed 23 —
///   TiDB's `TIME` domain is an ELAPSED-time range, not a wall-clock
///   hour, confirmed up to its real documented maximum `838:59:59` (a
///   larger `H`, even with `M`/`S` individually valid, clamps the WHOLE
///   value to exactly `838:59:59` — not just the hour component,
///   confirmed via `goeval`: `HOUR('900:30:15')` is `838` but
///   `MINUTE('900:30:15')` is `59`, not `30`). `M`/`S` must each be
///   `0..=59` or the WHOLE value is invalid (`NULL`), regardless of `H`'s
///   own magnitude.
/// - WITHOUT a `:` (including a bare `DATE`-only value — confirmed via
///   `goeval` this is NOT `(0, 0, 0)`, a genuinely surprising real TiDB
///   behavior, not a theoretical corner case this executor invented):
///   the value's OWN leading contiguous run of ASCII digits (after
///   trimming whitespace and an optional leading `-`, sign otherwise
///   irrelevant since `HOUR`/`MINUTE`/`SECOND` always return a
///   non-negative magnitude) is parsed as a plain integer `N` and
///   reinterpreted as a right-aligned `HHMMSS`-style number: `SECOND = N
///   % 100`, `MINUTE = (N / 100) % 100`, `HOUR = N / 10000` — the SAME
///   rule an integer-literal argument like `HOUR(103045)` already uses,
///   applied UNIFORMLY regardless of how many digits `N` has (so
///   `HOUR('2024-01-15')` takes ONLY the leading `'2024'` — stopping at
///   the first non-digit `-` — decoding to `HOUR=0, MINUTE=20,
///   SECOND=24`, NOT the calendar date's own values at all). The SAME
///   `0..=59`-for-`M`/`S`-or-invalid and clamp-to-`838:59:59` rules apply
///   identically to the decoded `N`.
fn parse_hms_extended(s: &str) -> Option<(u32, u32, u32)> {
    let s = s.trim();
    let s = s.strip_prefix('-').unwrap_or(s);
    if s.contains(':') {
        let time_str = match s.split_once(char::is_whitespace) {
            Some((date_str, time_str)) => {
                parse_date_ymd(date_str)?;
                time_str.trim_start()
            }
            None => s,
        };
        let mut fields = time_str.splitn(3, ':');
        let h: i64 = fields.next()?.parse().ok()?;
        let m: u32 = fields.next()?.parse().ok()?;
        let sec: u32 = match fields.next() {
            Some(f) => f.parse().ok()?,
            None => 0,
        };
        clamp_hms(h, m, sec)
    } else {
        let digits: String = s.chars().take_while(char::is_ascii_digit).collect();
        if digits.is_empty() {
            return None;
        }
        let n: i64 = digits.parse().ok()?;
        clamp_hms(n / 10_000, ((n / 100) % 100) as u32, (n % 100) as u32)
    }
}

/// Shared by both of [`parse_hms_extended`]'s paths: rejects an
/// out-of-range `m`/`sec`, else clamps `h` (and, if clamped, `m`/`sec`
/// TOO) to TiDB's real documented `TIME` maximum, `838:59:59` (confirmed
/// via `goeval`, not assumed, that an overflowing `h` clamps the WHOLE
/// value, not just `h` alone).
fn clamp_hms(h: i64, m: u32, sec: u32) -> Option<(u32, u32, u32)> {
    if h < 0 || m > 59 || sec > 59 {
        return None;
    }
    if h > 838 {
        return Some((838, 59, 59));
    }
    Some((h as u32, m, sec))
}

/// Splits a string into the `(value, digit count)` of its maximal runs of
/// ASCII digits, treating every other character as a separator — `None`
/// if any component is empty (a leading, trailing, or doubled separator)
/// or if there are not exactly 3 components. The digit count (not just
/// the parsed value) is preserved for [`expand_year`]'s own century-pivot
/// rule, which depends on how many digits the year was actually WRITTEN
/// with, not on its numeric value alone.
fn split_numeric_components(input: &str) -> Option<Vec<(u32, usize)>> {
    let mut parts = Vec::new();
    let mut current = String::new();
    for character in input.chars() {
        if character.is_ascii_digit() {
            current.push(character);
        } else {
            if current.is_empty() {
                return None;
            }
            parts.push((current.parse().ok()?, current.len()));
            current.clear();
        }
    }
    if current.is_empty() {
        return None;
    }
    parts.push((current.parse().ok()?, current.len()));
    (parts.len() == 3).then_some(parts)
}

/// Splits numeric date components for the `TIMEDIFF` parser.
pub(crate) fn split_numeric_components_for_time_diff(input: &str) -> Option<Vec<(u32, usize)>> {
    split_numeric_components(input)
}

/// Expands a calendar-date component's own YEAR value per real MySQL/
/// TiDB's century-pivot rule, confirmed via `goeval` to depend on the
/// value's ORIGINAL WRITTEN digit count, not its numeric magnitude: a
/// 1- or 2-digit year (`'1-03-15'` and `'01-03-15'` are indistinguishable
/// once parsed to a plain integer, and both pivot identically) is
/// EXPANDED — `0..=69` becomes `2000..=2069`, `70..=99` becomes
/// `1970..=1999` — while a 3-or-more-digit year is taken LITERALLY, even
/// when its own value happens to be under 100 (`'099-03-15'` is year
/// `99`, NOT pivoted to `1999`/`2099` — confirmed via `goeval`, a real
/// asymmetry from the 2-digit case that could not be guessed from the
/// value alone).
fn expand_year(value: u32, digits: usize) -> i64 {
    if digits > 2 {
        return i64::from(value);
    }
    if value <= 69 {
        2000 + i64::from(value)
    } else {
        1900 + i64::from(value)
    }
}

/// Expands a two-digit year using TiDB's date parsing window.
pub(crate) fn expand_year_for_time_diff(value: u32, digits: usize) -> i64 {
    expand_year(value, digits)
}

/// Computes TiDB's `calcDaynr` value for `TIMEDIFF` datetime arithmetic.
/// Unlike normal calendar parsing, the source permits zero month/day
/// components when `IgnoreZeroInDate` is enabled; mirroring `calcDaynr`
/// preserves those values instead of forcing them through Gregorian
/// month normalization.
pub(crate) fn time_diff_daynr(year: i64, month: u32, day: u32) -> i64 {
    if year == 0 && month == 0 {
        return 0;
    }
    let month = i64::from(month);
    let day = i64::from(day);
    let mut daynr = 365 * year + 31 * (month - 1) + day;
    let year = if month <= 2 {
        year - 1
    } else {
        daynr -= (month * 4 + 23) / 10;
        year
    };
    let century = ((year / 100 + 1) * 3) / 4;
    daynr + year / 4 - century
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// The number of days in `month` of `year` (Gregorian, leap-year aware);
/// `0` for an out-of-range month so a range check against it always fails.
fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Returns the Gregorian month length for the `TIMEDIFF` parser.
pub(crate) fn days_in_month_for_time_diff(year: i64, month: u32) -> u32 {
    days_in_month(year, month)
}

/// TiDB's `types.CoreTime.Week` / `YearWeek` calculation, ported from
/// `pkg/types/core_time.go:calcDaynr`, `weekMode`, and `calcWeek`.
/// `mode` is masked to its low three bits exactly as the Go implementation
/// does.  `with_year` selects `YearWeek`'s always-year-numbered variant.
pub(crate) fn week_of_year(y: i64, m: u32, d: u32, mode: i64, with_year: bool) -> (i64, i64) {
    const MONDAY_FIRST: u8 = 1;
    const WEEK_YEAR: u8 = 2;
    const FIRST_WEEKDAY: u8 = 4;
    let calc_daynr = |year: i64, month: u32, day: u32| {
        if year == 0 && month == 0 {
            return 0;
        }
        let mut year = year;
        let mut sum = 365 * year + 31 * (i64::from(month) - 1) + i64::from(day);
        if month <= 2 {
            year -= 1;
        } else {
            sum -= (i64::from(month) * 4 + 23) / 10;
        }
        sum + year / 4 - ((year / 100 + 1) * 3) / 4
    };
    let days_in_year = |year: i64| {
        if year & 3 == 0 && (year % 100 != 0 || (year % 400 == 0 && year != 0)) {
            366
        } else {
            365
        }
    };
    let mut behavior = (mode as u8) & 7;
    if behavior & MONDAY_FIRST == 0 {
        behavior ^= FIRST_WEEKDAY;
    }
    if with_year {
        behavior |= WEEK_YEAR;
    }
    let monday_first = behavior & MONDAY_FIRST != 0;
    let mut week_year = behavior & WEEK_YEAR != 0;
    let first_weekday = behavior & FIRST_WEEKDAY != 0;
    let mut year = y;
    let daynr = calc_daynr(y, m, d);
    let mut first_daynr = calc_daynr(y, 1, 1);
    let mut weekday = (first_daynr + 5 + if monday_first { 0 } else { 1 }) % 7;
    if m == 1 && d <= (7 - weekday) as u32 {
        if !week_year && ((first_weekday && weekday != 0) || (!first_weekday && weekday >= 4)) {
            return (year, 0);
        }
        week_year = true;
        year -= 1;
        let days = days_in_year(year);
        first_daynr -= days;
        weekday = (weekday + 53 * 7 - days) % 7;
    }
    let days = if (first_weekday && weekday != 0) || (!first_weekday && weekday >= 4) {
        daynr - (first_daynr + 7 - weekday)
    } else {
        daynr - (first_daynr - weekday)
    };
    if week_year && days >= 52 * 7 {
        weekday = (weekday + days_in_year(year)) % 7;
        if (!first_weekday && weekday < 4) || (first_weekday && weekday == 0) {
            return (year + 1, 1);
        }
    }
    (year, days / 7 + 1)
}

/// Days since an arbitrary fixed epoch (1970-01-01) for a Gregorian
/// calendar date — Howard Hinnant's well-known `days_from_civil` algorithm
/// (<http://howardhinnant.github.io/date_algorithms.html>), correct for the
/// proleptic Gregorian calendar. Only used for the RELATIVE difference
/// `DATEDIFF` computes, so matching MySQL's own internal epoch exactly
/// doesn't matter — any consistent day-numbering gives the same difference.
pub(crate) fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (i64::from(m) + 9) % 12;
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// The inverse of [`days_from_civil`]: the Gregorian calendar date for a day
/// count `z` since the same 1970-01-01 epoch — Howard Hinnant's
/// `civil_from_days` algorithm, from the same public source. Used by
/// [`from_days`] (which converts through [`days_from_civil`]'s own epoch —
/// see `TO_DAYS`'s `719_528` offset, so the exact epoch choice is internal
/// and doesn't need to match MySQL's) and, unlike `from_days`, directly on
/// its OWN public epoch by `crate::time_fn`'s `NOW()`/`CURRENT_TIMESTAMP()`
/// (a true Unix timestamp's day count IS already `z` in this function's own
/// terms, since both anchor to 1970-01-01).
pub(crate) fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// `FROM_DAYS`: the inverse of `TO_DAYS` — an absolute day number back to a
/// `YYYY-MM-DD` date string. The source signature is `ETInt`, so strings use
/// TiDB's integer-prefix coercion (`"z550z"` becomes zero and `"6500z"`
/// becomes 6500), while decimal/float inputs round through the shared
/// `to_i64_signed` path. Outside the valid range (`366` to `3_652_424`,
/// i.e. year `0001` through `9999`), values normally return the literal
/// string `"0000-00-00"` (MySQL's "zero date"). Real TiDB also has a narrow,
/// clearly-anomalous `NULL` sub-band immediately ABOVE the valid range
/// (`3_652_425` to `3_652_499`), which is source-visible in `TestFromDays`
/// and therefore retained here before the zero-date fallback resumes beyond
/// it.
pub(crate) fn from_days(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let n = match &vals[0] {
        Datum::Null => return Ok(Datum::Null),
        value => to_i64_signed(value),
    };
    if (3_652_425..=3_652_499).contains(&n) {
        return Ok(Datum::Null);
    }
    if !(366..=3_652_424).contains(&n) {
        return Ok(Datum::new_string("0000-00-00".to_string()));
    }
    let (y, m, d) = civil_from_days(n - 719_528);
    Ok(Datum::new_string(format!("{y:04}-{m:02}-{d:02}")))
}

/// `DATEDIFF(date1, date2)`, ported from `builtinDateDiffSig.evalInt` in
/// `pkg/expression/builtin_time.go`.  Both arguments are parsed as calendar
/// dates; any time-of-day suffix is deliberately ignored, matching TiDB's
/// `types.DateDiff` contract.  The value-only boundary returns `NULL` for a
/// failed temporal conversion and has no warning/SQLMode state.
pub(crate) fn date_diff(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 2 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let (Some(left), Some(right)) = (coerce_str(&vals[0])?, coerce_str(&vals[1])?) else {
        return Ok(Datum::Null);
    };
    let (Some((ly, lm, ld)), Some((ry, rm, rd))) = (parse_date_ymd(&left), parse_date_ymd(&right))
    else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Int(
        days_from_civil(ly, lm, ld) - days_from_civil(ry, rm, rd),
    ))
}

#[derive(Clone, Copy)]
struct TimestampDiffDateTime {
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    microsecond: u32,
}

/// Parses a strict DATE/DATETIME string for `TIMESTAMPDIFF`.
fn parse_timestamp_diff_datetime(input: &str) -> Option<TimestampDiffDateTime> {
    let input = input.trim();
    let (date, time) = input
        .split_once(char::is_whitespace)
        .or_else(|| input.split_once('T'))
        .map_or((input, "00:00:00"), |(date, time)| (date, time.trim()));
    let (year, month, day) = parse_date_ymd(date)?;
    let (hour, minute, second, fraction) = parse_time_with_fraction(time)?;
    let fsp = fraction.len();
    let microsecond = fraction.parse::<u32>().ok().unwrap_or(0) * 10u32.pow(6 - fsp as u32);
    Some(TimestampDiffDateTime {
        year,
        month,
        day,
        hour,
        minute,
        second,
        microsecond,
    })
}

/// `TIMESTAMPDIFF(unit, datetime_expr1, datetime_expr2)`, ported from
/// `builtinTimestampDiffSig.evalInt` and `types.TimestampDiff`.  The Rust
/// value boundary accepts scalar DATE/DATETIME strings and returns the exact
/// integer result; typed temporal conversion, warning state, and SQL-mode
/// handling remain at the caller boundary.
pub(crate) fn timestamp_diff(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 3 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let (Some(unit), Some(left), Some(right)) = (
        coerce_str(&vals[0])?,
        coerce_str(&vals[1])?,
        coerce_str(&vals[2])?,
    ) else {
        return Ok(Datum::Null);
    };
    let Some(left) = parse_timestamp_diff_datetime(&left) else {
        return Ok(Datum::Null);
    };
    let Some(right) = parse_timestamp_diff_datetime(&right) else {
        return Ok(Datum::Null);
    };

    let left_day = days_from_civil(left.year, left.month, left.day);
    let right_day = days_from_civil(right.year, right.month, right.day);
    let left_clock = i64::from(left.hour) * 3_600_000_000
        + i64::from(left.minute) * 60_000_000
        + i64::from(left.second) * 1_000_000
        + i64::from(left.microsecond);
    let right_clock = i64::from(right.hour) * 3_600_000_000
        + i64::from(right.minute) * 60_000_000
        + i64::from(right.second) * 1_000_000
        + i64::from(right.microsecond);
    let delta = (right_day - left_day) * 86_400_000_000 + right_clock - left_clock;
    let negative = delta < 0;
    let absolute = delta.unsigned_abs();
    let seconds = absolute / 1_000_000;
    let microseconds = absolute % 1_000_000;
    let sign = if negative { -1 } else { 1 };

    let (begin, end) = if negative {
        (right, left)
    } else {
        (left, right)
    };
    let months = if matches!(
        unit.to_ascii_uppercase().as_str(),
        "YEAR" | "QUARTER" | "MONTH"
    ) {
        let mut years = end.year - begin.year;
        let date_before =
            end.month < begin.month || (end.month == begin.month && end.day < begin.day);
        if date_before {
            years -= 1;
        }
        let mut months = 12 * years;
        if date_before {
            months += 12 - (i64::from(begin.month) - i64::from(end.month));
        } else {
            months += i64::from(end.month) - i64::from(begin.month);
        }
        if end.day < begin.day
            || (end.day == begin.day
                && (end.hour * 3_600 + end.minute * 60 + end.second
                    < begin.hour * 3_600 + begin.minute * 60 + begin.second
                    || (end.hour * 3_600 + end.minute * 60 + end.second
                        == begin.hour * 3_600 + begin.minute * 60 + begin.second
                        && end.microsecond < begin.microsecond)))
        {
            months -= 1;
        }
        months
    } else {
        0
    };

    let unit = unit.to_ascii_uppercase();
    let result = match unit.as_str() {
        "YEAR" => months / 12 * sign,
        "QUARTER" => months / 3 * sign,
        "MONTH" => months * sign,
        "WEEK" => (seconds / 86_400 / 7) as i64 * sign,
        "DAY" => (seconds / 86_400) as i64 * sign,
        "HOUR" => (seconds / 3_600) as i64 * sign,
        "MINUTE" => (seconds / 60) as i64 * sign,
        "SECOND" => seconds as i64 * sign,
        "MICROSECOND" => (seconds * 1_000_000 + microseconds) as i64 * sign,
        _ => 0,
    };
    Ok(Datum::Int(result))
}

/// `TO_DAYS(date)`, implemented through the same zero-date day number used by
/// `types.TimestampDiff("DAY", types.ZeroDate, date)`.  This preserves the
/// source's year-zero `0000-01-01 -> 1` behavior while rejecting invalid
/// zero-date components and malformed time suffixes.
pub(crate) fn to_days(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(value) = coerce_str(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let Some(value) = parse_timestamp_diff_datetime(&value) else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Int(time_diff_daynr(
        value.year,
        value.month,
        value.day,
    )))
}

/// `TO_SECONDS(date)`, implemented through the same zero-date timestamp
/// arithmetic as the Go builtin.  Fractional seconds are deliberately
/// ignored because the source's `SECOND` unit returns whole seconds.
pub(crate) fn to_seconds(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(value) = coerce_str(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let Some(value) = parse_timestamp_diff_datetime(&value) else {
        return Ok(Datum::Null);
    };
    let seconds = time_diff_daynr(value.year, value.month, value.day) * 86_400
        + i64::from(value.hour) * 3_600
        + i64::from(value.minute) * 60
        + i64::from(value.second);
    Ok(Datum::Int(seconds))
}

/// `DATE_ADD(date, INTERVAL amount unit)` / `DATE_SUB(...)`: adds (or, when
/// `sign` is `-1`, subtracts) `amount` `unit`s to a date/datetime string.
/// Covers `DAY`/`WEEK`/`MONTH`/`YEAR` (which preserve an existing
/// time-of-day suffix verbatim, or omit it if the input had none — none of
/// them touch the time portion) and `HOUR`/`MINUTE`/`SECOND` (which ALWAYS
/// compute and render a time-of-day component instead — see
/// [`date_add_time`]'s doc comment for why that's a different, much
/// simpler problem than the standalone `HOUR()`/`MINUTE()`/`SECOND()`
/// extraction functions' unimplemented two-path algorithm).
///
/// `DAY` is exact day arithmetic via the same `days_from_civil`/
/// `civil_from_days` round-trip `TO_DAYS`/`FROM_DAYS` use, so month/year
/// rollover and leap days are handled correctly for free —
/// `2021-01-31 + 1 DAY` = `2021-02-01`, `2020-02-28 + 1 DAY` =
/// `2020-02-29`. `WEEK` is `DAY` with the (already-rounded) amount
/// pre-multiplied by 7, confirmed via `goeval` rather than assumed —
/// including that a fractional `WEEK` amount rounds to the nearest whole
/// WEEK first, THEN multiplies by 7, not the other way around: `INTERVAL
/// 1.5 WEEK` = `+14` days, not `round(1.5*7)=11` days. `MONTH`/`YEAR` are
/// calendar-FIELD arithmetic via [`add_months`], a genuinely different
/// algorithm — MySQL clamps the day to the target month's length rather
/// than overflowing into the next month, e.g. `2021-01-31 + 1 MONTH` =
/// `2021-02-28`, not `2021-03-03`; the clamp is computed once against the
/// FINAL target month, not iteratively re-clamped one month at a time —
/// confirmed via `goeval`: `2021-01-31 + 2 MONTH` = `2021-03-31`, the full
/// 31 days, not `2021-03-28` from clamping through February first. Every
/// other unit (`QUARTER`/`MICROSECOND`/...) parses syntactically via
/// `Expr::Interval` but is `Unsupported` here.
///
/// `amount` accepts `Int` directly or `Decimal` (rounded to the nearest
/// whole unit via [`crate::Decimal::round_to_i64`], ties away
/// from zero — confirmed via `goeval` for both a positive and a negative
/// half-unit, matching this crate's one existing decimal-to-integer
/// rounding rule rather than a newly invented one — BEFORE any per-unit
/// multiplication like `WEEK`'s `×7` or `YEAR`'s `×12`, per the note
/// above); a `Str` amount would need MySQL's general string-to-number
/// coercion, out of scope like `FROM_DAYS`'s argument.
///
/// The resulting year is validated against `DATE`'s real `0001`-`9999`
/// range (see [`format_ymd_result`]) — a real bug in an earlier increment's
/// `DAY`-only implementation never checked this at all, silently producing
/// a malformed string like `10000-01-01` instead of `NULL` for
/// `DATE_ADD('9999-12-31', INTERVAL 1 DAY)`; caught and fixed while
/// probing `MONTH`/`YEAR`'s boundary behavior and confirming (via
/// `goeval`) that `DAY` obeys the identical rule.
pub(crate) fn date_add(
    unit: &str,
    date: &Datum,
    amount: &Datum,
    sign: i64,
) -> Result<Datum, EvalError> {
    let n = match amount {
        Datum::Null => return Ok(Datum::Null),
        Datum::Int(i) => *i,
        Datum::UInt(i) => *i as i64,
        Datum::Decimal(d) => d.round_to_i64().ok_or(EvalError::IntOverflow)?,
        Datum::String(_) | Datum::Bytes(_) | Datum::Real(_) => {
            return Err(EvalError::Unsupported("INTERVAL amount"));
        }
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel INTERVAL amount"));
        }
    };
    let Some(s) = coerce_str(date)? else {
        return Ok(Datum::Null);
    };
    let trimmed = s.trim();
    let (date_str, time_suffix) = trimmed
        .split_once(char::is_whitespace)
        .map_or((trimmed, None), |(d, t)| (d, Some(t)));
    let Some((y, m, d)) = parse_date_ymd(date_str) else {
        return Ok(Datum::Null);
    };
    let unit_secs = if unit.eq_ignore_ascii_case("HOUR") {
        Some(3600)
    } else if unit.eq_ignore_ascii_case("MINUTE") {
        Some(60)
    } else if unit.eq_ignore_ascii_case("SECOND") {
        Some(1)
    } else {
        None
    };
    if let Some(unit_secs) = unit_secs {
        let (h, mi, sec) = match time_suffix {
            Some(t) => match parse_time_hms(t) {
                Some(hms) => hms,
                None => return Ok(Datum::Null),
            },
            None => (0, 0, 0),
        };
        return Ok(date_add_time(y, m, d, h, mi, sec, sign * n * unit_secs));
    }
    let (y2, m2, d2) = if unit.eq_ignore_ascii_case("DAY") {
        civil_from_days(days_from_civil(y, m, d) + sign * n)
    } else if unit.eq_ignore_ascii_case("WEEK") {
        civil_from_days(days_from_civil(y, m, d) + sign * n * 7)
    } else if unit.eq_ignore_ascii_case("MONTH") {
        add_months(y, m, d, sign * n)
    } else if unit.eq_ignore_ascii_case("YEAR") {
        add_months(y, m, d, sign * n * 12)
    } else {
        return Err(EvalError::Unsupported("INTERVAL unit"));
    };
    Ok(format_ymd_result(y2, m2, d2, time_suffix))
}

/// Parses a `HH:MM:SS` time-of-day string into `(hour, minute, second)`,
/// each range-validated (`0..=23`/`0..=59`/`0..=59`) — `None` if malformed
/// or out of range. Strict about the `:` separator, unlike
/// [`parse_date_ymd`]'s lenient date-separator handling: every date/
/// datetime value this crate itself ever produces is `HH:MM:SS` exactly,
/// and every corpus/test input uses the same well-formed shape, so
/// leniency here has not been demonstrated as necessary the way the DATE
/// separator's was (confirmed via `goeval` to matter for real inputs).
pub(crate) fn parse_time_hms(s: &str) -> Option<(u32, u32, u32)> {
    let mut parts = s.splitn(3, ':');
    let h: u32 = parts.next()?.parse().ok()?;
    let mi: u32 = parts.next()?.parse().ok()?;
    let sec: u32 = parts.next()?.parse().ok()?;
    if h > 23 || mi > 59 || sec > 59 {
        return None;
    }
    Some((h, mi, sec))
}

/// `DATE_FORMAT`'s time-of-day parser extends [`parse_time_hms`] with the
/// optional fractional seconds which `%f` renders. The Go source delegates
/// this to `types.Time.DateFormat` (`pkg/types/time.go`); retaining the
/// written fraction here is enough for the evaluator's string-only domain.
pub(crate) fn parse_time_with_fraction(s: &str) -> Option<(u32, u32, u32, String)> {
    let mut parts = s.splitn(3, ':');
    let h: u32 = parts.next()?.parse().ok()?;
    let mi: u32 = parts.next()?.parse().ok()?;
    let sec_part = parts.next()?;
    let (sec_part, fraction) = sec_part.split_once('.').map_or((sec_part, ""), |pair| pair);
    let sec: u32 = sec_part.parse().ok()?;
    if h > 23 || mi > 59 || sec > 59 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((h, mi, sec, fraction.chars().take(6).collect()))
}

#[derive(Default)]
struct ParsedDateTime {
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    microsecond: u32,
    saw_date: bool,
    saw_time: bool,
    saw_fraction: bool,
    saw_24_hour: bool,
    saw_12_hour: bool,
    am_pm: Option<bool>,
}

/// `STR_TO_DATE(date, format)`, ported from `types.Time.StrToDate` and the
/// `builtinStrToDate*Sig` family in `pkg/expression/builtin_time.go`.
///
/// The surrounding evaluator has no typed `Time`/`Duration` datum yet, so a
/// successful parse is rendered as the canonical string representation that
/// the source's typed value exposes (`YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, or
/// `HH:MM:SS`).  The parser intentionally keeps the source's useful scalar
/// grammar: numeric date/time directives, `%r`/`%T`, fractional seconds,
/// case-insensitive AM/PM, and `%@`/`%#`/`%.` skip directives.  SQL-mode
/// zero-date checks and the function-class result-type selection remain
/// outside this value-only seam.
pub(crate) fn str_to_date(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 2 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let (Some(date), Some(format)) = (coerce_str(&vals[0])?, coerce_str(&vals[1])?) else {
        return Ok(Datum::Null);
    };
    let date: Vec<char> = date.chars().collect();
    let format: Vec<char> = format.chars().collect();
    let mut value = ParsedDateTime::default();
    let mut date_pos = 0;
    let mut format_pos = 0;
    while format_pos < format.len() {
        skip_parser_whitespace(&date, &mut date_pos);
        skip_parser_whitespace(&format, &mut format_pos);
        if format_pos >= format.len() {
            break;
        }
        let token = format[format_pos];
        format_pos += 1;
        if token != '%' {
            if date.get(date_pos) != Some(&token) {
                return Ok(Datum::Null);
            }
            date_pos += 1;
            continue;
        }
        let Some(specifier) = format.get(format_pos).copied() else {
            return Ok(Datum::Null);
        };
        format_pos += 1;
        match specifier {
            'Y' => {
                let Some((raw, consumed)) = parse_ascii_digits(&date[date_pos..], 4) else {
                    return Ok(Datum::Null);
                };
                value.year = expand_year(raw, consumed);
                value.saw_date = true;
                date_pos += consumed;
            }
            'y' => {
                let Some((raw, consumed)) = parse_ascii_digits(&date[date_pos..], 2) else {
                    return Ok(Datum::Null);
                };
                value.year = expand_year(raw, consumed);
                value.saw_date = true;
                date_pos += consumed;
            }
            'm' | 'c' => {
                let Some((month, consumed)) = parse_ascii_digits(&date[date_pos..], 2) else {
                    return Ok(Datum::Null);
                };
                if month > 12 {
                    return Ok(Datum::Null);
                }
                value.month = month;
                value.saw_date = true;
                date_pos += consumed;
            }
            'd' | 'e' => {
                let Some((day, consumed)) = parse_ascii_digits(&date[date_pos..], 2) else {
                    return Ok(Datum::Null);
                };
                if day > 31 {
                    return Ok(Datum::Null);
                }
                value.day = day;
                value.saw_date = true;
                date_pos += consumed;
            }
            'H' | 'k' => {
                let Some((hour, consumed)) = parse_ascii_digits(&date[date_pos..], 2) else {
                    return Ok(Datum::Null);
                };
                if hour > 23 {
                    return Ok(Datum::Null);
                }
                value.hour = hour;
                value.saw_time = true;
                value.saw_24_hour = true;
                date_pos += consumed;
            }
            'h' | 'I' | 'l' => {
                let Some((hour, consumed)) = parse_ascii_digits(&date[date_pos..], 2) else {
                    return Ok(Datum::Null);
                };
                if hour == 0 || hour > 12 {
                    return Ok(Datum::Null);
                }
                value.hour = hour;
                value.saw_time = true;
                value.saw_12_hour = true;
                date_pos += consumed;
            }
            'i' => {
                let Some((minute, consumed)) = parse_ascii_digits(&date[date_pos..], 2) else {
                    return Ok(Datum::Null);
                };
                if minute > 59 {
                    return Ok(Datum::Null);
                }
                value.minute = minute;
                value.saw_time = true;
                date_pos += consumed;
            }
            's' | 'S' => {
                let Some((second, consumed)) = parse_ascii_digits(&date[date_pos..], 2) else {
                    return Ok(Datum::Null);
                };
                if second > 59 {
                    return Ok(Datum::Null);
                }
                value.second = second;
                value.saw_time = true;
                date_pos += consumed;
            }
            'f' => {
                let (microsecond, consumed) = parse_ascii_digits(&date[date_pos..], 6)
                    .map_or((0, 0), |(raw, consumed)| {
                        (raw * 10u32.pow(6 - consumed as u32), consumed)
                    });
                value.microsecond = microsecond;
                value.saw_fraction = true;
                value.saw_time = true;
                date_pos += consumed;
            }
            'p' => {
                let Some(am_pm) = parse_am_pm(&date[date_pos..]) else {
                    return Ok(Datum::Null);
                };
                if value.saw_24_hour {
                    return Ok(Datum::Null);
                }
                value.am_pm = Some(am_pm);
                date_pos += 2;
            }
            'r' => {
                let Some((hour, minute, second, am_pm, consumed)) =
                    parse_time_12(&date[date_pos..])
                else {
                    return Ok(Datum::Null);
                };
                value.hour = hour;
                value.minute = minute;
                value.second = second;
                value.saw_time = true;
                value.saw_12_hour = true;
                value.am_pm = am_pm;
                date_pos += consumed;
            }
            'T' => {
                let Some((hour, minute, second, consumed)) = parse_time_24(&date[date_pos..])
                else {
                    return Ok(Datum::Null);
                };
                value.hour = hour;
                value.minute = minute;
                value.second = second;
                value.saw_time = true;
                value.saw_24_hour = true;
                date_pos += consumed;
            }
            '@' => skip_parser_class(&date, &mut date_pos, |c| c.is_ascii_alphabetic()),
            '#' => skip_parser_class(&date, &mut date_pos, |c| c.is_ascii_digit()),
            '.' => skip_parser_class(&date, &mut date_pos, |c| c.is_ascii_punctuation()),
            _ => return Ok(Datum::Null),
        }
    }

    if let Some(am_pm) = value.am_pm {
        if value.saw_24_hour || !value.saw_12_hour {
            return Ok(Datum::Null);
        }
        value.hour = if value.hour == 12 {
            if am_pm {
                12
            } else {
                0
            }
        } else if am_pm {
            value.hour + 12
        } else {
            value.hour
        };
    }
    if value.saw_date {
        if !(1..=12).contains(&value.month)
            || value.day == 0
            || value.day > days_in_month(value.year, value.month)
        {
            return Ok(Datum::Null);
        }
        let date = format!("{:04}-{:02}-{:02}", value.year, value.month, value.day);
        if value.saw_time {
            return Ok(Datum::new_string(if value.saw_fraction {
                format!(
                    "{date} {:02}:{:02}:{:02}.{:06}",
                    value.hour, value.minute, value.second, value.microsecond
                )
            } else {
                format!(
                    "{date} {:02}:{:02}:{:02}",
                    value.hour, value.minute, value.second
                )
            }));
        }
        return Ok(Datum::new_string(date));
    }
    if value.saw_time {
        return Ok(Datum::new_string(if value.saw_fraction {
            format!(
                "{:02}:{:02}:{:02}.{:06}",
                value.hour, value.minute, value.second, value.microsecond
            )
        } else {
            format!("{:02}:{:02}:{:02}", value.hour, value.minute, value.second)
        }));
    }
    Ok(Datum::Null)
}

fn skip_parser_whitespace(input: &[char], position: &mut usize) {
    while input
        .get(*position)
        .is_some_and(|character| character.is_whitespace())
    {
        *position += 1;
    }
}

fn parse_ascii_digits(input: &[char], limit: usize) -> Option<(u32, usize)> {
    let mut value = 0u32;
    let mut consumed = 0;
    while consumed < limit {
        let Some(character) = input.get(consumed) else {
            break;
        };
        let Some(digit) = character.to_digit(10).filter(|_| character.is_ascii()) else {
            break;
        };
        value = value.checked_mul(10)?.checked_add(digit)?;
        consumed += 1;
    }
    (consumed > 0).then_some((value, consumed))
}

fn skip_parser_class(input: &[char], position: &mut usize, predicate: fn(char) -> bool) {
    while input
        .get(*position)
        .is_some_and(|character| predicate(*character))
    {
        *position += 1;
    }
}

fn parse_am_pm(input: &[char]) -> Option<bool> {
    let [first, second, ..] = input else {
        return None;
    };
    match (first.to_ascii_lowercase(), second.to_ascii_lowercase()) {
        ('a', 'm') => Some(false),
        ('p', 'm') => Some(true),
        _ => None,
    }
}

fn parse_time_24(input: &[char]) -> Option<(u32, u32, u32, usize)> {
    let mut position = 0;
    let (hour, consumed) = parse_ascii_digits(&input[position..], 2)?;
    if hour > 23 {
        return None;
    }
    position += consumed;
    skip_parser_whitespace(input, &mut position);
    if input.get(position) != Some(&':') {
        return None;
    }
    position += 1;
    skip_parser_whitespace(input, &mut position);
    let (minute, consumed) = parse_ascii_digits(&input[position..], 2)?;
    if minute > 59 {
        return None;
    }
    position += consumed;
    skip_parser_whitespace(input, &mut position);
    if input.get(position) != Some(&':') {
        return None;
    }
    position += 1;
    skip_parser_whitespace(input, &mut position);
    let (second, consumed) = parse_ascii_digits(&input[position..], 2)?;
    if second > 59 {
        return None;
    }
    position += consumed;
    Some((hour, minute, second, position))
}

fn parse_time_12(input: &[char]) -> Option<(u32, u32, u32, Option<bool>, usize)> {
    let mut position = 0;
    let (hour, consumed) = parse_ascii_digits(&input[position..], 2)?;
    if hour == 0 || hour > 12 {
        return None;
    }
    position += consumed;
    skip_parser_whitespace(input, &mut position);
    if input.get(position) != Some(&':') {
        return None;
    }
    position += 1;
    skip_parser_whitespace(input, &mut position);
    let (minute, consumed) = parse_ascii_digits(&input[position..], 2)?;
    if minute > 59 {
        return None;
    }
    position += consumed;
    skip_parser_whitespace(input, &mut position);
    if input.get(position) != Some(&':') {
        return None;
    }
    position += 1;
    skip_parser_whitespace(input, &mut position);
    let (second, consumed) = parse_ascii_digits(&input[position..], 2)?;
    if second > 59 {
        return None;
    }
    position += consumed;
    skip_parser_whitespace(input, &mut position);
    let am_pm = if let Some(am_pm) = parse_am_pm(&input[position..]) {
        position += 2;
        Some(am_pm)
    } else if position == input.len() {
        None
    } else {
        return None;
    };
    Some((hour, minute, second, am_pm, position))
}

/// `DATE_ADD`/`DATE_SUB` with an `INTERVAL n {HOUR,MINUTE,SECOND}`: unlike
/// `DAY`/`WEEK`/`MONTH`/`YEAR`, which preserve an existing time-of-day
/// suffix verbatim (or omit it if the input had none), these units always
/// compute AND render a time-of-day component — even for a `DATE`-only
/// input, treated as midnight (`2021-01-01 + 5 HOUR` = `2021-01-01
/// 05:00:00`, confirmed via `goeval`), since the interval itself is about
/// time-of-day granularity, not just the date. `delta_secs` (already
/// unit-scaled and sign-applied by the caller) is added to the whole
/// datetime's absolute seconds-since-epoch count (`days_from_civil` scaled
/// to seconds, plus the time-of-day's own seconds), then converted back —
/// so overflow correctly carries into the day and, via `civil_from_days`,
/// into month/year, exactly like `DAY`-unit arithmetic already does
/// (`22:00:00 + 5 HOUR` = the next day's `03:00:00`, confirmed via
/// `goeval`).
fn date_add_time(y: i64, m: u32, d: u32, h: u32, mi: u32, sec: u32, delta_secs: i64) -> Datum {
    let total = days_from_civil(y, m, d) * 86_400
        + i64::from(h) * 3600
        + i64::from(mi) * 60
        + i64::from(sec)
        + delta_secs;
    let day_count = total.div_euclid(86_400);
    let secs_of_day = total.rem_euclid(86_400);
    let (y2, m2, d2) = civil_from_days(day_count);
    format_ymdhms_result(
        y2,
        m2,
        d2,
        (secs_of_day / 3600) as u32,
        (secs_of_day / 60 % 60) as u32,
        (secs_of_day % 60) as u32,
    )
}

/// Like [`format_ymd_result`], but for `HOUR`/`MINUTE`/`SECOND` intervals,
/// which always render a time-of-day component — even when the computed
/// year hits the "zero date" case, where ONLY the date portion becomes the
/// `0000-00-00` placeholder; the time portion still shows the actual
/// computed value (e.g. `'0001-01-01 00:00:00' - 1 HOUR` =
/// `'0000-00-00 23:00:00'`, confirmed via `goeval` — not
/// `'0000-00-00 00:00:00'` or a bare `'0000-00-00'`).
pub(crate) fn format_ymdhms_result(y: i64, m: u32, d: u32, h: u32, mi: u32, sec: u32) -> Datum {
    if y == 0 {
        return Datum::new_string(format!("0000-00-00 {h:02}:{mi:02}:{sec:02}"));
    }
    if !(1..=9999).contains(&y) {
        return Datum::Null;
    }
    Datum::new_string(format!("{y:04}-{m:02}-{d:02} {h:02}:{mi:02}:{sec:02}"))
}

/// Adds `n` months to `(y, m, d)` as a calendar field increment: the
/// year/month roll over via total-months arithmetic, and the day clamps to
/// the target month's own length (e.g. `2021-01-31 + 1` = `(2021, 2, 28)`,
/// not an overflow into March) — MySQL's `MONTH`/`YEAR` interval rule,
/// confirmed via `goeval`, genuinely different from `DAY`'s exact
/// day-number arithmetic (see [`date_add`]'s doc comment). `YEAR` reuses
/// this with `n` pre-multiplied by 12, rather than a separate algorithm.
fn add_months(y: i64, m: u32, d: u32, n: i64) -> (i64, u32, u32) {
    let total = y * 12 + i64::from(m - 1) + n;
    let y2 = total.div_euclid(12);
    let m2 = (total.rem_euclid(12) + 1) as u32;
    let d2 = d.min(days_in_month(y2, m2));
    (y2, m2, d2)
}

/// Formats a computed `(y, m, d)` as `DATE_ADD`/`DATE_SUB`'s result,
/// re-attaching `time_suffix` if the input had one, after validating `y`
/// against `DATE`'s real supported range: a computed year of exactly `0`
/// is MySQL's "zero date" string (matching `FROM_DAYS`'s own out-of-range
/// convention); any other out-of-`1..=9999` year — negative, or past
/// `9999` — is `NULL` (a genuine asymmetry from `FROM_DAYS`'s all-zero-date
/// convention, both directions confirmed via `goeval` for `DAY`, `MONTH`,
/// and `YEAR` alike, not assumed symmetric).
pub(crate) fn format_ymd_result(y: i64, m: u32, d: u32, time_suffix: Option<&str>) -> Datum {
    if y == 0 {
        return Datum::new_string("0000-00-00".to_string());
    }
    if !(1..=9999).contains(&y) {
        return Datum::Null;
    }
    Datum::new_string(match time_suffix {
        Some(t) => format!("{y:04}-{m:02}-{d:02} {t}"),
        None => format!("{y:04}-{m:02}-{d:02}"),
    })
}

/// `DATE_FORMAT(date, fmt)`: renders a date/datetime string per a MySQL
/// format string. The date argument is parsed as `Y-M-D[ H:M:S]` (a missing
/// time is midnight); `NULL` if either argument is `NULL` or the date
/// doesn't parse. Supported specifiers cover the common set (verified via
/// `gorun`): `%Y`/`%y` year, `%m`/`%c` month, `%d`/`%e` day, `%H`/`%k`/
/// `%h`/`%I`/`%l` hour, `%i` minute, `%S`/`%s` second, `%p` AM/PM, `%T`/`%r`
/// time, `%W`/`%a`/`%w` weekday renderings, `%M`/`%b` month name, `%j` day-of-year,
/// `%D` day-with-ordinal-suffix, and `%%` a literal `%`. An unknown `%X`
/// emits `X` verbatim (matching MySQL).
pub(crate) fn date_format(date: &Datum, fmt: &Datum) -> Result<Datum, EvalError> {
    let (Some(s), Some(fmt)) = (coerce_str(date)?, coerce_str(fmt)?) else {
        return Ok(Datum::Null);
    };
    // Split off an optional time-of-day component.
    let (date_part, time_part) = match s.split_once(' ') {
        Some((d, t)) => (d, Some(t)),
        None => (s.as_str(), None),
    };
    let Some((y, m, d)) = parse_date_ymd(date_part) else {
        return Ok(Datum::Null);
    };
    let (h, mi, sec, fraction) =
        time_part
            .and_then(parse_time_with_fraction)
            .unwrap_or((0, 0, 0, String::new()));

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
    const WEEKDAYS: [&str; 7] = [
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
    ];
    // `days_from_civil + 4 (mod 7)` gives 0=Sunday .. 6=Saturday (the same
    // offset `DAYOFWEEK` uses, verified against real TiDB).
    let wd = (days_from_civil(y, m, d) + 4).rem_euclid(7) as usize;
    let doy = days_from_civil(y, m, d) - days_from_civil(y, 1, 1) + 1;
    let h12 = ((h + 11) % 12) + 1;
    let suffix = |n: u32| -> &'static str {
        match (n % 100, n % 10) {
            (11..=13, _) => "th",
            (_, 1) => "st",
            (_, 2) => "nd",
            (_, 3) => "rd",
            _ => "th",
        }
    };

    let mut out = String::new();
    let mut chars = fmt.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        match chars.next() {
            None => out.push('%'),
            Some('Y') => out.push_str(&format!("{y:04}")),
            Some('y') => out.push_str(&format!("{:02}", y.rem_euclid(100))),
            Some('m') => out.push_str(&format!("{m:02}")),
            Some('c') => out.push_str(&m.to_string()),
            Some('d') => out.push_str(&format!("{d:02}")),
            Some('e') => out.push_str(&d.to_string()),
            Some('H') => out.push_str(&format!("{h:02}")),
            Some('k') => out.push_str(&h.to_string()),
            Some('h' | 'I') => out.push_str(&format!("{h12:02}")),
            Some('l') => out.push_str(&h12.to_string()),
            Some('i') => out.push_str(&format!("{mi:02}")),
            Some('S' | 's') => out.push_str(&format!("{sec:02}")),
            Some('f') => out.push_str(&format!("{fraction:0<6}")),
            Some('p') => out.push_str(if h < 12 { "AM" } else { "PM" }),
            Some('T') => out.push_str(&format!("{h:02}:{mi:02}:{sec:02}")),
            Some('r') => out.push_str(&format!(
                "{h12:02}:{mi:02}:{sec:02} {}",
                if h < 12 { "AM" } else { "PM" }
            )),
            Some('W') => out.push_str(WEEKDAYS[wd]),
            Some('a') => out.push_str(&WEEKDAYS[wd][..3]),
            Some('w') => out.push_str(&wd.to_string()),
            Some('M') => out.push_str(MONTHS[(m - 1) as usize]),
            Some('b') => out.push_str(&MONTHS[(m - 1) as usize][..3]),
            Some('j') => out.push_str(&format!("{doy:03}")),
            Some('D') => out.push_str(&format!("{d}{}", suffix(d))),
            // Go's `types.Time.DateFormat` maps these to `CoreTime.Week` /
            // `YearWeek`, whose exact mode handling is ported in
            // `week_of_year` above. `%U`/`%u` retain the week number's
            // possible zero; `%V`/`%v` force the corresponding year-week
            // mode and `%X`/`%x` render its possibly adjacent calendar year.
            Some('U') => out.push_str(&format!("{:02}", week_of_year(y, m, d, 0, false).1)),
            Some('u') => out.push_str(&format!("{:02}", week_of_year(y, m, d, 1, false).1)),
            Some('V') => out.push_str(&format!("{:02}", week_of_year(y, m, d, 2, false).1)),
            Some('v') => out.push_str(&format!("{:02}", week_of_year(y, m, d, 3, false).1)),
            Some('X') => out.push_str(&format!("{:04}", week_of_year(y, m, d, 2, true).0)),
            Some('x') => out.push_str(&format!("{:04}", week_of_year(y, m, d, 3, true).0)),
            Some('%') => out.push('%'),
            // An unknown specifier emits the letter verbatim (MySQL rule).
            Some(other) => out.push(other),
        }
    }
    Ok(Datum::new_string(out))
}

#[cfg(test)]
mod week_tests {
    use super::week_of_year;

    /// `week_of_year(y, m, d, mode, with_year) -> (week_year, week)` ports TiDB
    /// `core_time.go` calcWeek/weekMode, driving DATE_FORMAT's %U/%u/%V/%v/%X/%x.
    /// Vectors are authoritative goeval `DATE_FORMAT(d,'%U %u %V %v %X %x')` on
    /// boundary dates that stress week 0/52/53 and the week-year transition.
    #[test]
    fn week_of_year_matches_go_for_boundary_dates() {
        // y, m, d, %U, %u, %V, %v, %X, %x
        for &(y, m, d, uu, ul, vu, vl, xu, xl) in &[
            (
                2000i64, 1u32, 1u32, 0i64, 0i64, 52i64, 52i64, 1999i64, 1999i64,
            ),
            (2001, 1, 1, 0, 1, 53, 1, 2000, 2001),
            (1999, 12, 31, 52, 52, 52, 52, 1999, 1999),
            (2000, 12, 31, 53, 52, 53, 52, 2000, 2000),
            (2004, 1, 1, 0, 1, 52, 1, 2003, 2004),
            (2005, 1, 1, 0, 0, 52, 53, 2004, 2004),
            (2015, 12, 31, 52, 53, 52, 53, 2015, 2015),
            (2016, 1, 1, 0, 0, 52, 53, 2015, 2015),
        ] {
            assert_eq!(week_of_year(y, m, d, 0, false).1, uu, "%U {y}-{m}-{d}");
            assert_eq!(week_of_year(y, m, d, 1, false).1, ul, "%u {y}-{m}-{d}");
            assert_eq!(week_of_year(y, m, d, 2, false).1, vu, "%V {y}-{m}-{d}");
            assert_eq!(week_of_year(y, m, d, 3, false).1, vl, "%v {y}-{m}-{d}");
            assert_eq!(week_of_year(y, m, d, 2, true).0, xu, "%X {y}-{m}-{d}");
            assert_eq!(week_of_year(y, m, d, 3, true).0, xl, "%x {y}-{m}-{d}");
        }
    }
}
