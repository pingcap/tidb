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

//! `types.ParseDuration` and the microsecond-precision datetime arithmetic the
//! `ADDTIME`/`SUBTIME`/`TIMESTAMP` family needs, translated from
//! `pkg/types/time.go` (`matchDuration`, `canFallbackToDateTime`,
//! `ParseDuration`, `Time.Add`) and `pkg/types/core_time.go` (`calcDaynr`,
//! `getDateFromDaynr`, `calcTimeDurationDiff`).
//!
//! This is a separate module from [`super`] because it is a VALUE domain, not
//! a builtin: the same duration and the same day-number arithmetic serve four
//! different function classes. The rest of `time_fn` represents a temporal
//! value as its formatted string and computes with seconds; these two types
//! carry microseconds, which is the whole point — `ADDTIME`'s result fsp is
//! decided by whether the sum's microsecond field is zero.

use std::sync::OnceLock;

use super::calendar;

/// Go `types.MaxFsp`.
pub(crate) const MAX_FSP: i32 = 6;
/// Go `types.MinFsp`.
pub(crate) const MIN_FSP: i32 = 0;
/// Go `types.TimeMaxHour`.
const TIME_MAX_HOUR: i64 = 838;
/// Go `types.MaxTime` in microseconds (`838:59:59.000000`).
const MAX_TIME_MICROS: i64 = (838 * 3600 + 59 * 60 + 59) * 1_000_000;

/// Go `types.Duration`: a signed microsecond span plus the fsp it prints at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GoDuration {
    pub(crate) micros: i64,
    pub(crate) fsp: i32,
}

impl GoDuration {
    /// Go `Duration.MicroSecond`: the fractional part alone, always positive.
    pub(crate) fn micro_second(self) -> i64 {
        self.micros.abs() % 1_000_000
    }

    /// Go `Duration.Add`/`Duration.Sub`: the sum keeps the LARGER fsp of the
    /// two operands. A `Duration{}` zero operand returns the receiver
    /// untouched, fsp included, which is why the `is_zero_value` guard is
    /// here and not at the call sites.
    pub(crate) fn combine(self, other: GoDuration, sign: i64) -> GoDuration {
        let scaled = GoDuration {
            micros: other.micros * sign,
            fsp: other.fsp,
        };
        if other.micros == 0 && other.fsp == 0 {
            return self;
        }
        GoDuration {
            micros: self.micros.saturating_add(scaled.micros),
            fsp: self.fsp.max(other.fsp),
        }
    }

    /// Go `Duration.String`.
    pub(crate) fn format(self) -> String {
        super::format_time_diff(self.micros, self.fsp.max(0) as usize)
    }
}

/// Go `types.Time`, reduced to the fields this family reads. `micros` is the
/// microsecond field, not a whole-value count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GoDateTime {
    pub(crate) year: i64,
    pub(crate) month: u32,
    pub(crate) day: u32,
    pub(crate) hour: u32,
    pub(crate) minute: u32,
    pub(crate) second: u32,
    pub(crate) micros: u32,
    pub(crate) fsp: i32,
}

impl GoDateTime {
    /// Go `Time.IsZero`.
    pub(crate) fn is_zero(self) -> bool {
        self.year == 0
            && self.month == 0
            && self.day == 0
            && self.hour == 0
            && self.minute == 0
            && self.second == 0
            && self.micros == 0
    }

    /// Go `Time.String`, truncating the microsecond field to `fsp` digits —
    /// TRUNCATING, not rounding, which is what makes `datetime(3) + time(6)`
    /// print `.579` for the exact value `.579789`.
    pub(crate) fn format(self) -> String {
        let stem = format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            self.year, self.month, self.day, self.hour, self.minute, self.second
        );
        let fsp = self.fsp.clamp(0, MAX_FSP);
        if fsp == 0 {
            return stem;
        }
        let divisor = 10u32.pow(6 - fsp as u32);
        format!(
            "{stem}.{:0width$}",
            self.micros / divisor,
            width = fsp as usize
        )
    }

    /// Go `Time.Add`: the datetime and the duration are reduced to one
    /// signed microsecond count (`calcTimeDurationDiff`), whose ABSOLUTE
    /// value is then split back into a day number and a time of day. Go
    /// discards the sign the same way (`_` on `neg`), so a sum that would
    /// fall before year 0 wraps rather than erroring; this port keeps that.
    ///
    /// The result fsp is Go's `max(d.Fsp, t.Fsp())`. The VECTORIZED
    /// signatures pass `types.Duration{Duration: arg1, Fsp: -1}`, so a
    /// caller reproducing the vectorized arm passes `fsp: -1` and the
    /// datetime's own fsp wins.
    pub(crate) fn add(self, delta: GoDuration) -> Option<GoDateTime> {
        let total = daynr(self.year, self.month, self.day)
            .checked_mul(86_400_000_000)?
            .checked_add(
                i64::from(self.hour) * 3_600_000_000
                    + i64::from(self.minute) * 60_000_000
                    + i64::from(self.second) * 1_000_000
                    + i64::from(self.micros),
            )?
            .checked_add(delta.micros)?;
        let total = total.abs();
        let seconds = total / 1_000_000;
        let micros = (total % 1_000_000) as u32;
        let (year, month, day) = date_from_daynr(seconds / 86_400);
        let rest = seconds % 86_400;
        Some(GoDateTime {
            year,
            month,
            day,
            hour: (rest / 3600) as u32,
            minute: (rest / 60 % 60) as u32,
            second: (rest % 60) as u32,
            micros,
            fsp: self.fsp.max(delta.fsp),
        })
    }

    /// Go `Time.Check` reduced to the range this family can produce: a day
    /// number outside `0001-01-01 .. 9999-12-31` is `getDateFromDaynr`'s own
    /// zero answer or a year past 9999, both of which Go reports as an
    /// invalid time rather than returning.
    pub(crate) fn in_range(self) -> bool {
        (1..=9999).contains(&self.year) && self.month >= 1 && self.day >= 1
    }
}

/// Go `calcDaynr`: days since 0000-00-00.
pub(crate) fn daynr(year: i64, month: u32, day: u32) -> i64 {
    if year == 0 && month == 0 {
        return 0;
    }
    let month = i64::from(month);
    let mut year = year;
    let mut delsum = 365 * year + 31 * (month - 1) + i64::from(day);
    if month <= 2 {
        year -= 1;
    } else {
        delsum -= (month * 4 + 23) / 10;
    }
    let temp = ((year / 100 + 1) * 3) / 4;
    delsum + year / 4 - temp
}

/// Go `calcDaysInYear`.
fn days_in_year(year: i64) -> i64 {
    if (year & 3) == 0 && (year % 100 != 0 || (year % 400 == 0 && year != 0)) {
        366
    } else {
        365
    }
}

/// Go `getDateFromDaynr`, the inverse of [`daynr`]. Out-of-range day numbers
/// answer `(0, 0, 0)`, exactly as Go's early return does.
pub(crate) fn date_from_daynr(daynr: i64) -> (i64, u32, u32) {
    if daynr <= 365 || daynr >= 3_652_500 {
        return (0, 0, 0);
    }
    let mut year = daynr * 100 / 36525;
    let temp = (((year - 1) / 100 + 1) * 3) / 4;
    let mut day_of_year = daynr - year * 365 - (year - 1) / 4 + temp;
    let mut in_year = days_in_year(year);
    while day_of_year > in_year {
        day_of_year -= in_year;
        year += 1;
        in_year = days_in_year(year);
    }
    let mut leap_day = 0;
    if in_year == 366 && day_of_year > 31 + 28 {
        day_of_year -= 1;
        if day_of_year == 31 + 28 {
            leap_day = 1;
        }
    }
    let mut month = 1;
    for length in [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31] {
        if day_of_year <= length {
            break;
        }
        day_of_year -= length;
        month += 1;
    }
    (year, month, (day_of_year + leap_day) as u32)
}

/// Go `expression.isDuration`, whose `durationPattern` decides whether
/// `ADDTIME`'s first STRING argument is read as a duration or as a datetime.
pub(crate) fn is_duration(value: &str) -> bool {
    static PATTERN: OnceLock<regex::Regex> = OnceLock::new();
    PATTERN
        .get_or_init(|| {
            regex::Regex::new(
                r"^\s*[-]?(((\d{1,2}\s+)?0*\d{0,3}(:0*\d{1,2}){0,2})|(\d{1,7}))?(\.\d*)?\s*$",
            )
            .expect("the source durationPattern is a valid regex")
        })
        .is_match(value)
}

/// Go `expression.getFsp4TimeAddSub`: `MaxFsp` when the string carries a
/// non-zero fractional part, `MinFsp` otherwise.
pub(crate) fn fsp_for_time_add_sub(value: &str) -> i32 {
    match value.find('.') {
        None => MIN_FSP,
        Some(dot) => {
            if value[dot + 1..].chars().any(|c| c != '0') {
                MAX_FSP
            } else {
                MIN_FSP
            }
        }
    }
}

/// Go `types.GetFsp`: the number of fractional digits, capped at `MaxFsp`.
pub(crate) fn get_fsp(value: &str) -> i32 {
    match value.find('.') {
        None => MIN_FSP,
        Some(dot) => (value.len() - dot - 1).min(MAX_FSP as usize) as i32,
    }
}

/// The one failure `ParseDuration` reports: `ErrTruncatedWrongVal`, which
/// every caller in this family turns into a warning plus a NULL result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Truncated;

fn space0(value: &str) -> &str {
    value.trim_start_matches(|c: char| c.is_ascii_whitespace())
}

/// Go `parser.Number`: at least one leading decimal digit.
fn number(value: &str) -> Option<(i64, &str)> {
    let digits: &str = value
        .split_at(
            value
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(value.len()),
        )
        .0;
    if digits.is_empty() {
        return None;
    }
    Some((digits.parse::<i64>().ok()?, &value[digits.len()..]))
}

/// Go `matchColon`: optional spaces, a colon, optional spaces.
fn match_colon(value: &str) -> Option<&str> {
    Some(space0(space0(value).strip_prefix(':')?))
}

/// Go `matchHHMMSSDelimited`.
fn match_hhmmss_delimited(value: &str, require_colon: bool) -> Option<([i64; 3], &str)> {
    let (hour, mut rest) = number(value)?;
    let mut hhmmss = [hour, 0, 0];
    for (index, slot) in hhmmss.iter_mut().enumerate().skip(1) {
        let Some(after_colon) = match_colon(rest) else {
            if index == 1 && require_colon {
                return None;
            }
            break;
        };
        let (num, remain) = number(after_colon)?;
        *slot = num;
        rest = remain;
    }
    Some((hhmmss, rest))
}

/// Go `matchDayHHMMSS`: `D HH:MM:SS`, the day folded into the hours.
fn match_day_hhmmss(value: &str) -> Option<([i64; 3], &str)> {
    let (day, rest) = number(value)?;
    let after_space = space0(rest);
    if after_space.len() == rest.len() {
        return None;
    }
    let (mut hhmmss, rest) = match_hhmmss_delimited(after_space, false)?;
    hhmmss[0] += 24 * day;
    Some((hhmmss, rest))
}

/// Go `matchHHMMSSCompact`: one run of digits read right-aligned as `HHMMSS`.
fn match_hhmmss_compact(value: &str) -> Option<([i64; 3], &str)> {
    let (num, rest) = number(value)?;
    Some(([num / 10000, num / 100 % 100, num % 100], rest))
}

/// Go `types.ParseFrac`, returning `(microseconds, overflow)`.
fn parse_frac(digits: &str, fsp: i32) -> Result<(i64, bool), Truncated> {
    if digits.is_empty() {
        return Ok((0, false));
    }
    let fsp = fsp.clamp(MIN_FSP, MAX_FSP);
    if fsp as usize >= digits.len() {
        let value: i64 = digits.parse().map_err(|_| Truncated)?;
        return Ok((
            value * 10i64.pow(MAX_FSP as u32 - digits.len() as u32),
            false,
        ));
    }
    let head: i64 = digits[..fsp as usize + 1].parse().map_err(|_| Truncated)?;
    let rounded = (head + 5) / 10;
    if rounded >= 10i64.pow(fsp as u32) {
        return Ok((0, true));
    }
    Ok((rounded * 10i64.pow(MAX_FSP as u32 - fsp as u32), false))
}

/// Go `matchFrac`, returning `(overflow, microseconds, rest)`.
fn match_frac(value: &str, fsp: i32) -> Result<(bool, i64, &str), Truncated> {
    let Some(after_dot) = value.strip_prefix('.') else {
        return Ok((false, 0, value));
    };
    let end = after_dot
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(after_dot.len());
    let (frac, overflow) = parse_frac(&after_dot[..end], fsp)?;
    Ok((overflow, frac, &after_dot[end..]))
}

/// Go `hhmmssAddOverflow`: carry one second into `HH:MM:SS`.
fn hhmmss_add_overflow(hms: &mut [i64; 3]) {
    let modulus = [-1, 60, 60];
    let mut overflow = true;
    for index in (0..3).rev() {
        if !overflow {
            break;
        }
        hms[index] += 1;
        if hms[index] == modulus[index] {
            hms[index] = 0;
        } else {
            overflow = false;
        }
    }
}

/// Go `matchDuration`.
fn match_duration(value: &str, fsp: i32) -> Result<GoDuration, Truncated> {
    if value.is_empty() {
        return Err(Truncated);
    }
    let (negative, rest) = value
        .strip_prefix('-')
        .map_or((false, value), |rest| (true, rest));
    let rest = space0(rest);
    let chars_len = rest.len();
    let (mut hhmmss, rest) = match_day_hhmmss(rest)
        .or_else(|| match_hhmmss_delimited(rest, true))
        .or_else(|| match_hhmmss_compact(rest))
        .ok_or(Truncated)?;
    let rest = space0(rest);
    let (overflow, mut frac, rest) = match_frac(rest, fsp)?;
    if !rest.is_empty() && chars_len >= 12 {
        return Err(Truncated);
    }
    if overflow {
        hhmmss_add_overflow(&mut hhmmss);
        frac = 0;
    }
    if hhmmss[1] >= 60 || hhmmss[2] >= 60 {
        return Err(Truncated);
    }
    // Go returns the CLAMPED value beside `ErrTruncatedWrongVal` here; every
    // caller in this family treats that error as "warn and answer NULL", so
    // the clamped value it carries is never read.
    if hhmmss[0] > TIME_MAX_HOUR {
        return Err(Truncated);
    }
    let mut micros = (hhmmss[0] * 3600 + hhmmss[1] * 60 + hhmmss[2]) * 1_000_000 + frac;
    if negative {
        micros = -micros;
    }
    if !(-MAX_TIME_MICROS..=MAX_TIME_MICROS).contains(&micros) || !rest.is_empty() {
        return Err(Truncated);
    }
    Ok(GoDuration { micros, fsp })
}

/// Go `canFallbackToDateTime`.
fn can_fall_back_to_datetime(value: &str) -> bool {
    let Some((_, rest)) = number(value) else {
        return false;
    };
    let digits = value.len() - rest.len();
    if digits == 12 || digits == 14 {
        return true;
    }
    let Some(rest) = strip_punct(rest) else {
        return false;
    };
    let Some((_, rest)) = number(rest) else {
        return false;
    };
    let Some(rest) = strip_punct(rest) else {
        return false;
    };
    let Some((_, rest)) = number(rest) else {
        return false;
    };
    rest.starts_with(' ') || rest.starts_with('T')
}

/// Go `parser.AnyPunct`, which tests one BYTE with `unicode.IsPunct`.
fn strip_punct(value: &str) -> Option<&str> {
    let first = *value.as_bytes().first()?;
    if (first as char).is_ascii_punctuation() {
        Some(&value[1..])
    } else {
        None
    }
}

/// Go `types.ParseDuration`: `matchDuration`, then the datetime fallback for
/// the shapes `canFallbackToDateTime` admits.
pub(crate) fn parse_duration(value: &str, fsp: i32) -> Result<GoDuration, Truncated> {
    let rest = value.trim();
    match match_duration(rest, fsp) {
        Ok(duration) => Ok(duration),
        Err(Truncated) => {
            if !can_fall_back_to_datetime(rest) {
                return Err(Truncated);
            }
            let datetime = parse_datetime(rest).ok_or(Truncated)?;
            // Go `Time.ConvertToDuration`: the clock fields alone.
            let micros = i64::from(datetime.hour) * 3_600_000_000
                + i64::from(datetime.minute) * 60_000_000
                + i64::from(datetime.second) * 1_000_000
                + i64::from(datetime.micros);
            Ok(round_frac(
                GoDuration {
                    micros,
                    fsp: datetime.fsp,
                },
                fsp,
            ))
        }
    }
}

/// Go `Duration.RoundFrac`, half-up on the microsecond field.
fn round_frac(duration: GoDuration, fsp: i32) -> GoDuration {
    let fsp = fsp.clamp(MIN_FSP, MAX_FSP);
    let unit = 10i64.pow(MAX_FSP as u32 - fsp as u32);
    let sign = if duration.micros < 0 { -1 } else { 1 };
    let magnitude = duration.micros.abs();
    let rounded = (magnitude + unit / 2) / unit * unit;
    GoDuration {
        micros: sign * rounded,
        fsp,
    }
}

/// `types.ParseDatetime` reduced to the spellings this crate's temporal value
/// domain produces and accepts elsewhere: `Y-M-D[ H:M:S[.frac]]`, with the
/// same component splitting and two-digit-year expansion the `TIMEDIFF`
/// signatures already use.
pub(crate) fn parse_datetime(value: &str) -> Option<GoDateTime> {
    let value = value.trim();
    let (date, time) = match value.split_once(|c: char| c.is_whitespace() || c == 'T') {
        Some((date, time)) => (date, time.trim()),
        None => (value, ""),
    };
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
    let (hour, minute, second, fraction) = if time.is_empty() {
        (0, 0, 0, String::new())
    } else {
        calendar::parse_time_with_fraction(time)?
    };
    let fsp = fraction.len() as i32;
    let micros = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<u32>().ok()? * 10u32.pow(6 - fsp as u32)
    };
    Some(GoDateTime {
        year,
        month,
        day,
        hour,
        minute,
        second,
        micros,
        fsp: fsp.min(MAX_FSP),
    })
}

#[cfg(test)]
mod source_tests {
    use super::is_duration;

    /// Exact Go `TestIsDuration` table. This predicate chooses ADDTIME's
    /// duration-vs-datetime signature before either parser is invoked.
    #[test]
    fn test_is_duration() {
        for (input, expected) in [
            ("110:00:00", true),
            ("aa:bb:cc", false),
            ("1 01:00:00", true),
            ("01:00:00.999999", true),
            ("071231235959.999999", false),
            ("20171231235959.999999", false),
            ("2017-01-01 01:01:01.11", false),
            ("07-12-31 23:59:59.999999", false),
            ("2007-12-31 23:59:59.999999", false),
        ] {
            assert_eq!(is_duration(input), expected, "{input}");
        }
    }
}
