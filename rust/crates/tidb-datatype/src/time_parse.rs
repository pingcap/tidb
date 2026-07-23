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

//! Temporal string parsing, ported from Go `pkg/types`.

use chrono::{FixedOffset, TimeZone};

use crate::{
    check_fsp, core_time_from_datetime, get_frac_index, get_timezone, parse_frac, CoreTime, Time,
    TimeError, TimeType, TimestampInterval,
};

/// Result metadata emitted while parsing a temporal literal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParsedTime {
    /// Parsed temporal value.
    pub time: Time,
    /// Whether TiDB would append a truncation warning.
    pub truncated: bool,
}

/// Parsed `INTERVAL` value before it is applied to a date or duration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParsedInterval {
    /// Calendar years.
    pub years: i64,
    /// Calendar months.
    pub months: i64,
    /// Whole days.
    pub days: i64,
    /// Signed sub-day nanoseconds.
    pub nanoseconds: i64,
    /// Fractional-seconds precision.
    pub fsp: u8,
    /// Whether TiDB returns the value together with a truncation diagnostic.
    pub truncated: bool,
}

/// Go `isDigit`.
const fn is_digit(c: u8) -> bool {
    c.is_ascii_digit()
}

/// Go `isPunctuation`: an ASCII punctuation character (printable, non-alnum).
const fn is_punctuation(c: u8) -> bool {
    matches!(c, 0x21..=0x2F | 0x3A..=0x40 | 0x5B..=0x60 | 0x7B..=0x7E)
}

/// Go `isValidSeparator`: punctuation is a valid separator anywhere; space and
/// `T` (and the other ASCII whitespace) separate only between the date and time
/// (`prevParts == 2`); after five parts any non-digit ends the field.
const fn is_valid_separator(c: u8, prev_parts: usize) -> bool {
    if is_punctuation(c) {
        return true;
    }
    if prev_parts == 2 && matches!(c, b'T' | b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r') {
        return true;
    }
    prev_parts > 4 && !is_digit(c)
}

/// Faithful port of Go `types.ParseDateFormat`: splits a date/time literal into
/// its numeric field strings, or returns `None` (Go's `nil`) when the literal
/// does not begin with a digit or contains an out-of-place non-digit.
///
/// The literal must start with a digit; punctuation separators are consumed
/// (including runs), a single space/`T` splits date from time, and the trailing
/// field is taken verbatim (so `"2011-11-11x"` yields `["2011","11","11x"]`,
/// exactly as Go leaves the last byte unexamined). Fields are lifted with
/// `from_utf8_lossy`: every valid (ASCII) literal round-trips exactly, and a
/// stray non-ASCII byte — only reachable through the `prev_parts > 4` rule —
/// fails downstream numeric parsing identically to Go's raw-byte string.
#[must_use]
pub fn parse_date_format(format: &str) -> Option<Vec<String>> {
    let format = format.trim();
    let bytes = format.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    // Date format must start with a number.
    if !is_digit(bytes[0]) {
        return None;
    }

    let mut seps: Vec<String> = Vec::with_capacity(6);
    let mut start = 0usize;
    let mut i = 1usize;
    // Go: `for i := 1; i < len(format)-1; i++` — the final byte is never
    // examined and always joins the trailing field.
    while i + 1 < bytes.len() {
        if is_valid_separator(bytes[i], seps.len()) {
            let prev_parts = seps.len();
            seps.push(String::from_utf8_lossy(&bytes[start..i]).into_owned());
            start = i + 1;
            // Consume further consecutive separators.
            let mut j = i + 1;
            while j < bytes.len() {
                if !is_valid_separator(bytes[j], prev_parts) {
                    break;
                }
                start += 1;
                i += 1;
                j += 1;
            }
            i += 1;
            continue;
        }
        if !is_digit(bytes[i]) {
            return None;
        }
        i += 1;
    }
    seps.push(String::from_utf8_lossy(&bytes[start..]).into_owned());
    Some(seps)
}

/// Returns whether the interval unit contains a clock component.
pub fn is_clock_unit(unit: &str) -> bool {
    matches!(
        unit.to_ascii_uppercase().as_str(),
        "MICROSECOND"
            | "SECOND"
            | "MINUTE"
            | "HOUR"
            | "SECOND_MICROSECOND"
            | "MINUTE_MICROSECOND"
            | "HOUR_MICROSECOND"
            | "DAY_MICROSECOND"
            | "MINUTE_SECOND"
            | "HOUR_SECOND"
            | "DAY_SECOND"
            | "HOUR_MINUTE"
            | "DAY_MINUTE"
            | "DAY_HOUR"
    )
}

/// Returns whether the interval unit contains a calendar component.
pub fn is_date_unit(unit: &str) -> bool {
    matches!(
        unit.to_ascii_uppercase().as_str(),
        "DAY"
            | "WEEK"
            | "MONTH"
            | "QUARTER"
            | "YEAR"
            | "DAY_MICROSECOND"
            | "DAY_SECOND"
            | "DAY_MINUTE"
            | "DAY_HOUR"
            | "YEAR_MONTH"
    )
}

/// Returns whether the interval unit contains microseconds.
pub fn is_microsecond_unit(unit: &str) -> bool {
    matches!(
        unit.to_ascii_uppercase().as_str(),
        "MICROSECOND"
            | "SECOND_MICROSECOND"
            | "MINUTE_MICROSECOND"
            | "HOUR_MICROSECOND"
            | "DAY_MICROSECOND"
    )
}

/// Returns whether the accepted literal shape can contain only a date.
pub fn is_date_format(format: &str) -> bool {
    let format = format.trim();
    match parse_date_format(format).map_or(0, |parts| parts.len()) {
        1 => matches!(format.len(), 5 | 6 | 8),
        3 => true,
        _ => false,
    }
}

/// Parses MySQL's one-, two-, or four-digit YEAR representation.
pub fn parse_year(input: &str) -> Result<i16, TimeError> {
    let year = input.parse::<i64>().map_err(|_| TimeError::InvalidDate)?;
    if !matches!(input.len(), 1 | 2 | 4) {
        return Err(TimeError::InvalidDate);
    }
    i16::try_from(adjust_year(year, true)?).map_err(|_| TimeError::OutOfRange("year"))
}

/// Applies MySQL's two-digit YEAR window and validates the YEAR domain.
pub fn adjust_year(year: i64, adjust_zero: bool) -> Result<i64, TimeError> {
    if year == 0 && !adjust_zero {
        return Ok(0);
    }
    let adjusted = match year {
        0..=69 => 2000 + year,
        70..=99 => 1900 + year,
        _ => year,
    };
    if !(1901..=2155).contains(&adjusted) {
        return Err(TimeError::OutOfRange("year"));
    }
    Ok(adjusted)
}

/// Converts a MySQL day number to a DATE.
#[must_use]
pub fn time_from_days(day_number: i64) -> Time {
    let core = if let Ok(day_number) = u32::try_from(day_number) {
        let (year, month, day) = crate::get_date_from_daynr(day_number);
        Time::from_date_checked(
            year as i32,
            month as i32,
            day as i32,
            0,
            0,
            0,
            0,
            TimeType::Date,
            0,
        )
        .map(Time::core_time)
        .unwrap_or_default()
    } else {
        CoreTime::default()
    };
    Time::new(core, TimeType::Date, 0).expect("zero FSP is valid")
}

/// Converts a YEAR value to TiDB's temporal representation.
pub fn parse_time_from_year(year: i64) -> Result<Time, TimeError> {
    if year == 0 {
        return Time::new(CoreTime::default(), TimeType::Date, 0);
    }
    let year = u16::try_from(year).map_err(|_| TimeError::OutOfRange("year"))?;
    Time::new(
        CoreTime::from_date(year, 0, 0, 0, 0, 0, 0),
        TimeType::DateTime,
        0,
    )
}

/// Applies TiDB's string-named `TIMESTAMPDIFF` unit.
pub fn timestamp_diff(unit: &str, start: Time, end: Time) -> Result<i64, TimeError> {
    let interval = match unit.to_ascii_uppercase().as_str() {
        "YEAR" => TimestampInterval::Year,
        "QUARTER" => TimestampInterval::Quarter,
        "MONTH" => TimestampInterval::Month,
        "WEEK" => TimestampInterval::Week,
        "DAY" => TimestampInterval::Day,
        "HOUR" => TimestampInterval::Hour,
        "MINUTE" => TimestampInterval::Minute,
        "SECOND" => TimestampInterval::Second,
        "MICROSECOND" => TimestampInterval::Microsecond,
        _ => return Err(TimeError::InvalidUnit(unit.to_owned())),
    };
    Ok(start.core_time().timestamp_diff(end.core_time(), interval))
}

/// Extracts the integer representation for a datetime interval unit.
pub fn extract_datetime_num(time: Time, unit: &str) -> Result<i64, TimeError> {
    let core = time.core_time();
    let hour = i64::from(core.hour());
    let minute = i64::from(core.minute());
    let second = i64::from(core.second());
    let day = i64::from(core.day());
    Ok(match unit.to_ascii_uppercase().as_str() {
        "DAY" => day,
        "WEEK" => i64::from(core.week(0)),
        "MONTH" => i64::from(core.month()),
        "QUARTER" => (i64::from(core.month()) + 2) / 3,
        "YEAR" => i64::from(core.year()),
        "DAY_MICROSECOND" => {
            (day * 1_000_000 + hour * 10_000 + minute * 100 + second) * 1_000_000
                + i64::from(core.microsecond())
        }
        "DAY_SECOND" => day * 1_000_000 + hour * 10_000 + minute * 100 + second,
        "DAY_MINUTE" => day * 10_000 + hour * 100 + minute,
        "DAY_HOUR" => day * 100 + hour,
        "YEAR_MONTH" => i64::from(core.year()) * 100 + i64::from(core.month()),
        _ => return Err(TimeError::InvalidUnit(unit.to_owned())),
    })
}

/// Extracts the integer representation for a duration interval unit.
pub fn extract_duration_num(duration: crate::MySqlDuration, unit: &str) -> Result<i64, TimeError> {
    let hour = duration.hour();
    let minute = duration.minute();
    let second = duration.second();
    let microsecond = duration.microsecond();
    let mut value = match unit.to_ascii_uppercase().as_str() {
        "MICROSECOND" => microsecond,
        "SECOND" => second,
        "MINUTE" => minute,
        "HOUR" => hour,
        "SECOND_MICROSECOND" => second * 1_000_000 + microsecond,
        "MINUTE_MICROSECOND" => minute * 100_000_000 + second * 1_000_000 + microsecond,
        "MINUTE_SECOND" => minute * 100 + second,
        "HOUR_MICROSECOND" => {
            hour * 10_000_000_000 + minute * 100_000_000 + second * 1_000_000 + microsecond
        }
        "HOUR_SECOND" | "DAY_SECOND" => hour * 10_000 + minute * 100 + second,
        "HOUR_MINUTE" | "DAY_MINUTE" => hour * 100 + minute,
        "DAY_MICROSECOND" => (hour * 10_000 + minute * 100 + second) * 1_000_000 + microsecond,
        "DAY_HOUR" => hour,
        _ => return Err(TimeError::InvalidUnit(unit.to_owned())),
    };
    if duration.nanoseconds() < 0 {
        value = -value;
    }
    Ok(value)
}

/// Parses a MySQL interval literal into calendar and sub-day components.
pub fn parse_duration_value(unit: &str, format: &str) -> Result<ParsedInterval, TimeError> {
    let unit = unit.to_ascii_uppercase();
    match unit.as_str() {
        "MICROSECOND" | "SECOND" | "MINUTE" | "HOUR" | "DAY" | "WEEK" | "MONTH" | "QUARTER"
        | "YEAR" => parse_single_interval(&unit, format),
        "SECOND_MICROSECOND" => parse_composite_interval(format, 6, 2),
        "MINUTE_MICROSECOND" => parse_composite_interval(format, 6, 3),
        "MINUTE_SECOND" => parse_composite_interval(format, 5, 2),
        "HOUR_MICROSECOND" => parse_composite_interval(format, 6, 4),
        "HOUR_SECOND" => parse_composite_interval(format, 5, 3),
        "HOUR_MINUTE" => parse_composite_interval(format, 4, 2),
        "DAY_MICROSECOND" => parse_composite_interval(format, 6, 5),
        "DAY_SECOND" => parse_composite_interval(format, 5, 4),
        "DAY_MINUTE" => parse_composite_interval(format, 4, 3),
        "DAY_HOUR" => parse_composite_interval(format, 3, 2),
        "YEAR_MONTH" => parse_composite_interval(format, 1, 2),
        _ => Err(TimeError::InvalidUnit(unit)),
    }
}

/// Parses a MySQL interval and validates that it fits the TIME domain.
pub fn extract_duration_value(unit: &str, format: &str) -> Result<crate::MySqlDuration, TimeError> {
    let parsed = parse_duration_value(unit, format)?;
    let unit = unit.to_ascii_uppercase();
    if parsed.truncated {
        return Err(TimeError::InvalidDate);
    }
    if parsed.years != 0 {
        return Err(TimeError::OutOfRange("time"));
    }
    let total_days = parsed
        .days
        .checked_add(parsed.months.saturating_mul(30))
        .ok_or(TimeError::OutOfRange("time"))?;
    let total = total_days
        .checked_mul(86_400_000_000_000)
        .and_then(|days| days.checked_add(parsed.nanoseconds))
        .ok_or(TimeError::OutOfRange("time"))?;
    if unit == "YEAR_MONTH" || total.unsigned_abs() > 3_020_399_999_999_999 {
        return Err(TimeError::OutOfRange("time"));
    }
    crate::MySqlDuration::from_nanoseconds(total, i64::from(parsed.fsp))
        .map_err(TimeError::InvalidFsp)
}

fn parse_single_interval(unit: &str, format: &str) -> Result<ParsedInterval, TimeError> {
    let format = format.trim();
    let (integer_text, fraction_text) = format.split_once('.').unwrap_or((format, ""));
    let integer = integer_text
        .parse::<i64>()
        .map_err(|_| TimeError::InvalidDate)?;
    let sign = if format.starts_with('-') { -1 } else { 1 };
    let fraction_digits: String = fraction_text
        .chars()
        .take(6)
        .take_while(char::is_ascii_digit)
        .collect();
    let fraction_len = fraction_digits.len();
    let mut padded = fraction_digits;
    while padded.len() < 6 {
        padded.push('0');
    }
    let fraction = padded.parse::<i64>().unwrap_or(0) * sign;
    let rounded = integer
        + if fraction.unsigned_abs() >= 500_000 {
            sign
        } else {
            0
        };
    let truncated = !fraction_text.is_empty() && unit != "SECOND";
    let mut parsed = ParsedInterval {
        years: 0,
        months: 0,
        days: 0,
        nanoseconds: 0,
        fsp: 0,
        truncated,
    };
    match unit {
        "MICROSECOND" => {
            parsed.days = rounded / 86_400_000_000;
            parsed.nanoseconds = rounded % 86_400_000_000 * 1_000;
            parsed.fsp = 6;
        }
        "SECOND" => {
            parsed.days = integer / 86_400;
            parsed.nanoseconds = integer % 86_400 * 1_000_000_000 + fraction * 1_000;
            parsed.fsp = fraction_len as u8;
        }
        "MINUTE" => {
            parsed.days = rounded / 1_440;
            parsed.nanoseconds = rounded % 1_440 * 60_000_000_000;
        }
        "HOUR" => {
            parsed.days = rounded / 24;
            parsed.nanoseconds = rounded % 24 * 3_600_000_000_000;
        }
        "DAY" => parsed.days = rounded,
        "WEEK" => parsed.days = rounded * 7,
        "MONTH" => parsed.months = rounded,
        "QUARTER" => parsed.months = rounded * 3,
        "YEAR" => parsed.years = rounded,
        _ => unreachable!("single interval unit was matched by caller"),
    }
    Ok(parsed)
}

fn parse_composite_interval(
    format: &str,
    final_index: usize,
    maximum_fields: usize,
) -> Result<ParsedInterval, TimeError> {
    let negative = format.trim_start().starts_with('-');
    let matches = numeric_fields_in_text(format);
    if matches.len() > maximum_fields {
        return Err(TimeError::InvalidDate);
    }
    let mut fields = [0_i64; 7];
    let mut index = final_index;
    for value in matches.iter().rev() {
        let parsed = value.parse::<i64>().map_err(|_| TimeError::InvalidDate)?;
        fields[index] = if negative { -parsed } else { parsed };
        if index == 0 {
            break;
        }
        index -= 1;
    }
    if final_index == 6 {
        let sign = if negative { -1 } else { 1 };
        let mut value = matches.last().copied().unwrap_or("0").to_owned();
        while value.len() < 6 {
            value.push('0');
        }
        fields[6] = value.parse::<i64>().map_err(|_| TimeError::InvalidDate)? * sign;
    }
    let seconds = fields[3] * 3_600 + fields[4] * 60 + fields[5];
    Ok(ParsedInterval {
        years: fields[0],
        months: fields[1],
        days: fields[2] + seconds / 86_400,
        nanoseconds: seconds % 86_400 * 1_000_000_000 + fields[6] * 1_000,
        fsp: if final_index == 6 { 6 } else { 0 },
        truncated: false,
    })
}

fn numeric_fields_in_text(input: &str) -> Vec<&str> {
    let mut fields = Vec::new();
    let mut start = None;
    for (index, byte) in input.bytes().enumerate() {
        if byte.is_ascii_digit() {
            start.get_or_insert(index);
        } else if let Some(start) = start.take() {
            fields.push(&input[start..index]);
        }
    }
    if let Some(start) = start {
        fields.push(&input[start..]);
    }
    fields
}

/// Parses TiDB's accepted DATE, DATETIME, and TIMESTAMP string forms.
pub fn parse_time<TZ: TimeZone>(
    input: &str,
    kind: TimeType,
    fsp: i64,
    is_float: bool,
    allow_zero_in_date: bool,
    allow_invalid_date: bool,
    timezone: &TZ,
) -> Result<ParsedTime, TimeError> {
    if is_float && input.starts_with("0.0") {
        return Ok(ParsedTime {
            time: Time::new(CoreTime::default(), kind, 0)?,
            truncated: false,
        });
    }
    let fsp = check_fsp(fsp).map_err(TimeError::InvalidFsp)?;
    let (core, truncated) = parse_datetime_core(input, fsp, is_float, timezone)?;
    let time = Time::new(core, kind, fsp)?;
    time.validate(allow_zero_in_date, allow_invalid_date, timezone)?;
    Ok(ParsedTime { time, truncated })
}

/// Parses a DATETIME using the fractional precision present in the literal.
pub fn parse_datetime<TZ: TimeZone>(
    input: &str,
    timezone: &TZ,
    allow_zero_in_date: bool,
    allow_invalid_date: bool,
) -> Result<ParsedTime, TimeError> {
    parse_time(
        input,
        TimeType::DateTime,
        i64::from(crate::get_fsp(input)),
        false,
        allow_zero_in_date,
        allow_invalid_date,
        timezone,
    )
}

fn parse_datetime_core<TZ: TimeZone>(
    input: &str,
    fsp: i64,
    is_float: bool,
    timezone: &TZ,
) -> Result<(CoreTime, bool), TimeError> {
    let (mut parts, mut fraction, mut timezone_suffix, mut truncated) = split_datetime(input);
    let no_absorb = |parts: &[String]| parts.len() > 5 || (parts.len() == 1 && parts[0].len() > 4);

    if !fraction.is_empty() && !is_float && !no_absorb(&parts) {
        parts.push(std::mem::take(&mut fraction));
    }
    if let Some(suffix) = &timezone_suffix {
        if suffix.sign.is_some()
            && !no_absorb(&parts)
            && !(suffix.minute.is_some() && !suffix.has_colon)
        {
            if let Some(hour) = &suffix.hour {
                parts.push(hour.clone());
            }
            if let Some(minute) = &suffix.minute {
                parts.push(minute.clone());
            }
            timezone_suffix = None;
        }
    }

    let mut fields = [0_i32; 6];
    let hhmmss;
    match parts.len() {
        0 => return Err(TimeError::InvalidDate),
        1 if is_float => {
            let number = parts[0]
                .parse::<i64>()
                .map_err(|_| TimeError::InvalidDate)?;
            let numeric =
                parse_time_from_num(number, TimeType::DateTime, fsp, true, false, timezone)?;
            let core = numeric.time.core_time();
            fields = [
                core.year(),
                i32::from(core.month()),
                i32::from(core.day()),
                i32::from(core.hour()),
                i32::from(core.minute()),
                i32::from(core.second()),
            ];
            let length = parts[0].len();
            hhmmss = parts[0] == "0" || (9..=14).contains(&length);
        }
        1 => {
            let compact = &parts[0];
            parse_compact(compact, &mut fields)?;
            let length = compact.len();
            hhmmss = matches!(length, 11 | 12 | 14);
            if matches!(length, 5 | 6 | 8) && !is_float {
                parse_compact_clock(&fraction, &mut fields[3..]);
            } else if matches!(length, 9 | 10) {
                fields[5] = parse_prefix(&fraction, 2).unwrap_or(0);
            }
        }
        2 => return Err(TimeError::InvalidDate),
        3..=6 => {
            for (field, part) in fields.iter_mut().zip(&parts) {
                *field = part.parse().map_err(|_| TimeError::InvalidDate)?;
            }
            hhmmss = parts.len() == 6;
        }
        _ => {
            truncated = true;
            for (field, part) in fields.iter_mut().zip(parts.iter().take(6)) {
                *field = part.parse().map_err(|_| TimeError::InvalidDate)?;
            }
            hhmmss = true;
        }
    }

    if !is_float && parts[0].len() <= 2 {
        let all_zero = fields.iter().all(|field| *field == 0) && fraction.is_empty();
        if !all_zero {
            fields[0] = adjust_two_digit_year(fields[0]);
        }
    }

    let (microsecond, overflow) = if hhmmss {
        parse_frac(fraction.as_bytes(), fsp).map_err(TimeError::InvalidFsp)?
    } else {
        (0, false)
    };
    let mut core = checked_core(fields, microsecond)?;
    if overflow {
        core = core.add_duration(1_000_000_000);
    }

    if let Some(suffix) = timezone_suffix {
        if !hhmmss {
            return Err(TimeError::InvalidDate);
        }
        let hour = suffix
            .hour
            .as_deref()
            .unwrap_or("0")
            .parse::<i32>()
            .map_err(|_| TimeError::InvalidDate)?;
        let minute = suffix
            .minute
            .as_deref()
            .unwrap_or("0")
            .parse::<i32>()
            .map_err(|_| TimeError::InvalidDate)?;
        if hour > 14
            || minute > 59
            || (hour == 14 && minute != 0)
            || (suffix.sign == Some('-') && hour == 0 && minute == 0)
        {
            return Err(TimeError::InvalidDate);
        }
        let mut offset = hour * 3_600 + minute * 60;
        if suffix.sign == Some('-') {
            offset = -offset;
        }
        let fixed = FixedOffset::east_opt(offset).ok_or(TimeError::InvalidDate)?;
        let source = core.to_datetime(&fixed)?;
        core = core_time_from_datetime(source.with_timezone(timezone));
    }
    Ok((core, truncated))
}

fn split_datetime(input: &str) -> (Vec<String>, String, Option<crate::TimezoneSuffix>, bool) {
    let mut value = input;
    let mut suffix = get_timezone(value);
    if let Some(timezone) = &mut suffix {
        if timezone.index > 0 {
            let mut index = timezone.index;
            while index > 0 && is_punctuation(value.as_bytes()[index - 1]) {
                index -= 1;
            }
            value = &value[..index];
        } else {
            suffix = None;
        }
    }

    let mut fraction = String::new();
    let mut truncated = false;
    let fraction_index = get_frac_index(value);
    if fraction_index > 0 {
        let mut end = fraction_index as usize + 1;
        while end < value.len() && value.as_bytes()[end].is_ascii_digit() {
            end += 1;
        }
        truncated = end != value.len();
        fraction.push_str(&value[fraction_index as usize + 1..end]);
        let mut start = fraction_index as usize;
        while start > 0 && is_punctuation(value.as_bytes()[start - 1]) {
            start -= 1;
        }
        value = &value[..start];
    }
    (
        parse_date_format(value).unwrap_or_default(),
        fraction,
        suffix,
        truncated,
    )
}

fn parse_compact(input: &str, fields: &mut [i32; 6]) -> Result<(), TimeError> {
    let widths: &[usize] = match input.len() {
        14 => &[4, 2, 2, 2, 2, 2],
        12 => &[2, 2, 2, 2, 2, 2],
        11 => &[2, 2, 2, 2, 2, 1],
        10 => &[2, 2, 2, 2, 2],
        9 => &[2, 2, 2, 2, 1],
        8 => &[4, 2, 2],
        7 => &[2, 2, 2, 1],
        6 => &[2, 2, 2],
        5 => &[2, 2, 1],
        _ => return Err(TimeError::InvalidDate),
    };
    let mut offset = 0;
    for (field, width) in fields.iter_mut().zip(widths) {
        *field = input[offset..offset + width]
            .parse()
            .map_err(|_| TimeError::InvalidDate)?;
        offset += width;
    }
    if !matches!(input.len(), 8 | 14) {
        fields[0] = adjust_two_digit_year(fields[0]);
    }
    Ok(())
}

fn parse_compact_clock(fraction: &str, clock: &mut [i32]) {
    match fraction.len() {
        0 => {}
        1..=2 => clock[0] = parse_prefix(fraction, 2).unwrap_or(0),
        3..=4 => {
            clock[0] = parse_prefix(fraction, 2).unwrap_or(0);
            clock[1] = fraction[2..].parse().unwrap_or(0);
        }
        _ => {
            clock[0] = parse_prefix(fraction, 2).unwrap_or(0);
            clock[1] = parse_prefix(&fraction[2..], 2).unwrap_or(0);
            clock[2] = parse_prefix(&fraction[4..], 2).unwrap_or(0);
        }
    }
}

fn parse_prefix(input: &str, width: usize) -> Option<i32> {
    input.get(..input.len().min(width))?.parse().ok()
}

fn checked_core(fields: [i32; 6], microsecond: i64) -> Result<CoreTime, TimeError> {
    Time::from_date_checked(
        fields[0],
        fields[1],
        fields[2],
        fields[3],
        fields[4],
        fields[5],
        microsecond as i32,
        TimeType::DateTime,
        0,
    )
    .map(Time::core_time)
}

const fn adjust_two_digit_year(year: i32) -> i32 {
    match year {
        0..=69 => 2000 + year,
        70..=99 => 1900 + year,
        _ => year,
    }
}

/// Parses TiDB's numeric datetime representation.
pub fn parse_time_from_num<TZ: TimeZone>(
    number: i64,
    kind: TimeType,
    fsp: i64,
    allow_zero_in_date: bool,
    allow_invalid_date: bool,
    timezone: &TZ,
) -> Result<ParsedTime, TimeError> {
    if number == 0 {
        return Ok(ParsedTime {
            time: Time::new(CoreTime::default(), kind, 0)?,
            truncated: false,
        });
    }
    let (normalized, _) = normalize_numeric_datetime(number)?;
    let fields = numeric_fields(normalized);
    let time = Time::from_date_checked(
        fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], 0, kind, fsp,
    )?;
    time.validate(allow_zero_in_date, allow_invalid_date, timezone)?;
    Ok(ParsedTime {
        time,
        truncated: false,
    })
}

/// Parses an integer using TiDB's native DATE-versus-DATETIME classification.
pub fn parse_time_from_int64<TZ: TimeZone>(
    number: i64,
    allow_zero_in_date: bool,
    allow_invalid_date: bool,
    timezone: &TZ,
) -> Result<Time, TimeError> {
    if number == 0 {
        return Time::new(CoreTime::default(), TimeType::Date, 0);
    }
    let (normalized, kind) = normalize_numeric_datetime(number)?;
    let fields = numeric_fields(normalized);
    let time = Time::from_date_checked(
        fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], 0, kind, 0,
    )?;
    time.validate(allow_zero_in_date, allow_invalid_date, timezone)?;
    Ok(time)
}

/// Parses a floating-point temporal number with Go's microsecond rounding.
pub fn parse_time_from_float64<TZ: TimeZone>(
    value: f64,
    allow_zero_in_date: bool,
    allow_invalid_date: bool,
    timezone: &TZ,
) -> Result<Time, TimeError> {
    let integer = value as i64;
    let mut time =
        parse_time_from_int64(integer, allow_zero_in_date, allow_invalid_date, timezone)?;
    if time.kind() == TimeType::DateTime {
        let microsecond = ((value - integer as f64) * 1_000_000.0).round() as u32;
        let core = time.core_time();
        time.set_core_time(CoreTime::from_date(
            core.year() as u16,
            core.month(),
            core.day(),
            core.hour(),
            core.minute(),
            core.second(),
            microsecond,
        ));
    }
    Ok(time)
}

/// Parses an exact decimal temporal number without floating-point rounding.
pub fn parse_time_from_decimal<TZ: TimeZone>(
    value: &crate::Decimal,
    allow_zero_in_date: bool,
    allow_invalid_date: bool,
    timezone: &TZ,
) -> Result<Time, TimeError> {
    let text = value.to_string();
    let (integer_text, fraction) = text.split_once('.').unwrap_or((&text, ""));
    let integer = integer_text
        .parse::<i64>()
        .map_err(|_| TimeError::InvalidDate)?;
    let mut time =
        parse_time_from_int64(integer, allow_zero_in_date, allow_invalid_date, timezone)?;
    let fsp = fraction.len().min(6) as i64;
    time.set_fsp(fsp)?;
    if fsp > 0 && time.kind() == TimeType::DateTime {
        let mut microsecond_text = fraction[..fsp as usize].to_owned();
        while microsecond_text.len() < 6 {
            microsecond_text.push('0');
        }
        let microsecond = microsecond_text
            .parse::<u32>()
            .map_err(|_| TimeError::InvalidDate)?;
        let core = time.core_time();
        time.set_core_time(CoreTime::from_date(
            core.year() as u16,
            core.month(),
            core.day(),
            core.hour(),
            core.minute(),
            core.second(),
            microsecond,
        ));
    }
    Ok(time)
}

fn normalize_numeric_datetime(mut number: i64) -> Result<(i64, TimeType), TimeError> {
    if !(0..=99_999_999_999_999).contains(&number) {
        return Err(TimeError::InvalidDate);
    }
    if number >= 10_000_101_000_000 {
        return Ok((number, TimeType::DateTime));
    }
    if number < 101 {
        return Err(TimeError::InvalidDate);
    }
    if number <= 691_231 {
        return Ok(((number + 20_000_000) * 1_000_000, TimeType::Date));
    }
    if number < 700_101 {
        return Err(TimeError::InvalidDate);
    }
    if number <= 991_231 {
        return Ok(((number + 19_000_000) * 1_000_000, TimeType::Date));
    }
    if number <= 99_991_231 {
        return Ok((number * 1_000_000, TimeType::Date));
    }
    if number < 101_000_000 {
        return Err(TimeError::InvalidDate);
    }
    if number <= 691_231_235_959 {
        number += 20_000_000_000_000;
    } else if number < 700_101_000_000 {
        return Err(TimeError::InvalidDate);
    } else if number <= 991_231_235_959 {
        number += 19_000_000_000_000;
    }
    Ok((number, TimeType::DateTime))
}

fn numeric_fields(number: i64) -> [i32; 6] {
    let mut remainder = number;
    let second = (remainder % 100) as i32;
    remainder /= 100;
    let minute = (remainder % 100) as i32;
    remainder /= 100;
    let hour = (remainder % 100) as i32;
    remainder /= 100;
    let day = (remainder % 100) as i32;
    remainder /= 100;
    let month = (remainder % 100) as i32;
    let year = (remainder / 100) as i32;
    [year, month, day, hour, minute, second]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(items: &[&str]) -> Option<Vec<String>> {
        Some(items.iter().map(|s| (*s).to_string()).collect())
    }

    /// TiDB `TestParseDateFormat` (`pkg/types/time_test.go`).
    #[test]
    fn go_parse_date_format_vectors() {
        let cases: &[(&str, Option<Vec<String>>)] = &[
            (
                "2011-11-11 10:10:10.123456",
                parts(&["2011", "11", "11", "10", "10", "10", "123456"]),
            ),
            (
                "  2011-11-11 10:10:10.123456  ",
                parts(&["2011", "11", "11", "10", "10", "10", "123456"]),
            ),
            ("2011-11-11 10", parts(&["2011", "11", "11", "10"])),
            (
                "2011-11-11T10:10:10.123456",
                parts(&["2011", "11", "11", "10", "10", "10", "123456"]),
            ),
            (
                "2011:11:11T10:10:10.123456",
                parts(&["2011", "11", "11", "10", "10", "10", "123456"]),
            ),
            (
                "2011-11-11  10:10:10",
                parts(&["2011", "11", "11", "10", "10", "10"]),
            ),
            ("xx2011-11-11 10:10:10", None),
            ("T10:10:10", None),
            ("2011-11-11x", parts(&["2011", "11", "11x"])),
            ("xxx 10:10:10", None),
            (
                "2022-02-01\n16:33:00",
                parts(&["2022", "02", "01", "16", "33", "00"]),
            ),
            (
                "2022-02-01\x0c16:33:00",
                parts(&["2022", "02", "01", "16", "33", "00"]),
            ),
            (
                "2022-02-01\x0b16:33:00",
                parts(&["2022", "02", "01", "16", "33", "00"]),
            ),
            (
                "2022-02-01\r16:33:00",
                parts(&["2022", "02", "01", "16", "33", "00"]),
            ),
            (
                "2022-02-01\t16:33:00",
                parts(&["2022", "02", "01", "16", "33", "00"]),
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(
                parse_date_format(input),
                *expected,
                "parse_date_format({input:?})"
            );
        }
    }

    #[test]
    fn test_interval_and_date_format_classifiers_source_rows() {
        for unit in [
            "MICROSECOND",
            "SECOND",
            "MINUTE",
            "HOUR",
            "SECOND_MICROSECOND",
            "MINUTE_MICROSECOND",
            "MINUTE_SECOND",
            "HOUR_MICROSECOND",
            "HOUR_SECOND",
            "HOUR_MINUTE",
            "DAY_MICROSECOND",
            "DAY_SECOND",
            "DAY_MINUTE",
            "DAY_HOUR",
        ] {
            assert!(is_clock_unit(unit), "{unit}");
        }
        for unit in ["TEST", "SOME_MICROSECOND"] {
            assert!(!is_clock_unit(unit), "{unit}");
        }
        for unit in [
            "Day",
            "Week",
            "month",
            "quarter",
            "YEAR",
            "DAY_MICROSECOND",
            "DAY_SECOND",
            "DAY_MINUTE",
            "DAY_HOUR",
            "YEAR_MONTH",
        ] {
            assert!(is_date_unit(unit), "{unit}");
        }
        for unit in [
            "MICROSECOND",
            "SECOND",
            "MINUTE",
            "HOUR",
            "TEST",
            "SOME_DAY",
        ] {
            assert!(!is_date_unit(unit), "{unit}");
        }
        for unit in [
            "Microsecond",
            "Second_microsecond",
            "minute_microsecond",
            "hour_microsecond",
            "DAY_MICROSECOND",
        ] {
            assert!(is_microsecond_unit(unit), "{unit}");
        }
        for unit in [
            "SECOND",
            "MINUTE",
            "HOUR",
            "DAY_SECOND",
            "DAY_MINUTE",
            "DAY_HOUR",
            "DAY",
            "WEEK",
            "MONTH",
            "QUARTER",
            "YEAR",
            "TEST",
            "SOME_MICROSECOND",
        ] {
            assert!(!is_microsecond_unit(unit), "{unit}");
        }
        assert!(!is_date_format("1234:321"));
        assert!(is_date_format("2019-04-01"));
        assert!(is_date_format("2019-4-1"));
        assert!(is_date_format("20129"));
    }

    #[test]
    fn test_year_source_rows() {
        for (input, expected) in [("1990", 1990), ("10", 2010), ("0", 2000), ("99", 1999)] {
            assert_eq!(parse_year(input).unwrap(), expected);
        }
        assert_eq!(adjust_year(2000, false).unwrap(), 2000);
        assert!(adjust_year(20_000, false).is_err());
        assert_eq!(adjust_year(0, false).unwrap(), 0);
        assert!(adjust_year(-1, false).is_err());
        assert_eq!(adjust_year(0, true).unwrap(), 2000);
        assert!(parse_year("100").is_err());
    }

    #[test]
    fn test_timestamp_diff_and_extract_source_rows() {
        for (unit, start, end, expected) in [
            (
                "MONTH",
                CoreTime::from_date(2002, 5, 30, 0, 0, 0, 0),
                CoreTime::from_date(2001, 1, 1, 0, 0, 0, 0),
                -16,
            ),
            (
                "YEAR",
                CoreTime::from_date(2002, 5, 1, 0, 0, 0, 0),
                CoreTime::from_date(2001, 1, 1, 0, 0, 0, 0),
                -1,
            ),
            (
                "MINUTE",
                CoreTime::from_date(2003, 2, 1, 0, 0, 0, 0),
                CoreTime::from_date(2003, 5, 1, 12, 5, 55, 0),
                128_885,
            ),
            (
                "MICROSECOND",
                CoreTime::from_date(2002, 5, 30, 0, 0, 0, 0),
                CoreTime::from_date(2002, 5, 30, 0, 13, 25, 0),
                805_000_000,
            ),
            (
                "MICROSECOND",
                CoreTime::from_date(2000, 1, 1, 0, 0, 0, 12_345),
                CoreTime::from_date(2000, 1, 1, 0, 0, 45, 32),
                44_987_687,
            ),
            (
                "QUARTER",
                CoreTime::from_date(2000, 1, 12, 0, 0, 0, 0),
                CoreTime::from_date(2016, 1, 1, 0, 0, 0, 0),
                63,
            ),
            (
                "QUARTER",
                CoreTime::from_date(2016, 1, 1, 0, 0, 0, 0),
                CoreTime::from_date(2000, 1, 12, 0, 0, 0, 0),
                -63,
            ),
        ] {
            let start = Time::new(start, TimeType::DateTime, 6).unwrap();
            let end = Time::new(end, TimeType::DateTime, 6).unwrap();
            assert_eq!(timestamp_diff(unit, start, end).unwrap(), expected);
        }

        let value = Time::new(
            CoreTime::from_date(2019, 4, 12, 14, 0, 0, 0),
            TimeType::Timestamp,
            0,
        )
        .unwrap();
        for (unit, expected) in [
            ("day", 12),
            ("week", 14),
            ("MONTH", 4),
            ("QUARTER", 2),
            ("YEAR", 2019),
            ("DAY_MICROSECOND", 12_140_000_000_000),
            ("DAY_SECOND", 12_140_000),
            ("DAY_MINUTE", 121_400),
            ("DAY_HOUR", 1_214),
            ("YEAR_MONTH", 201_904),
        ] {
            assert_eq!(
                extract_datetime_num(value, unit).unwrap(),
                expected,
                "{unit}"
            );
        }
        assert!(extract_datetime_num(value, "TEST_ERROR").is_err());

        let positive = crate::MySqlDuration::from_nanoseconds(3_600 * 24 * 365, 0).unwrap();
        for (unit, expected) in [
            ("MICROSECOND", 31_536),
            ("SECOND", 0),
            ("MINUTE", 0),
            ("HOUR", 0),
            ("SECOND_MICROSECOND", 31_536),
            ("MINUTE_MICROSECOND", 31_536),
            ("MINUTE_SECOND", 0),
            ("HOUR_MICROSECOND", 31_536),
            ("HOUR_SECOND", 0),
            ("HOUR_MINUTE", 0),
            ("DAY_MICROSECOND", 31_536),
            ("DAY_SECOND", 0),
            ("DAY_MINUTE", 0),
            ("DAY_HOUR", 0),
        ] {
            assert_eq!(
                extract_duration_num(positive, unit).unwrap(),
                expected,
                "{unit}"
            );
        }
        let negative = crate::MySqlDuration::from_nanoseconds(-39_541_000_000_000, 0).unwrap();
        for (unit, expected) in [
            ("MICROSECOND", 0),
            ("SECOND", -1),
            ("MINUTE", -59),
            ("HOUR", -10),
            ("SECOND_MICROSECOND", -1_000_000),
            ("MINUTE_MICROSECOND", -5_901_000_000),
            ("MINUTE_SECOND", -5_901),
            ("HOUR_MICROSECOND", -105_901_000_000),
            ("HOUR_SECOND", -105_901),
            ("HOUR_MINUTE", -1_059),
            ("DAY_MICROSECOND", -105_901_000_000),
            ("DAY_SECOND", -105_901),
            ("DAY_MINUTE", -1_059),
            ("DAY_HOUR", -10),
        ] {
            assert_eq!(
                extract_duration_num(negative, unit).unwrap(),
                expected,
                "{unit}"
            );
        }
        assert!(extract_duration_num(negative, "TEST_ERROR").is_err());
    }

    #[test]
    fn test_parse_duration_value_source_rows() {
        for (format, unit, expected) in [
            ("52", "WEEK", (0, 0, 364, 0, 0, false)),
            ("12", "DAY", (0, 0, 12, 0, 0, false)),
            ("04", "MONTH", (0, 4, 0, 0, 0, false)),
            ("1", "QUARTER", (0, 3, 0, 0, 0, false)),
            ("2019", "YEAR", (2019, 0, 0, 0, 0, false)),
            (
                "10567890",
                "SECOND_MICROSECOND",
                (0, 0, 0, 10_567_890_000, 6, false),
            ),
            (
                "10.567890",
                "SECOND_MICROSECOND",
                (0, 0, 0, 10_567_890_000, 6, false),
            ),
            (
                "-10.567890",
                "SECOND_MICROSECOND",
                (0, 0, 0, -10_567_890_000, 6, false),
            ),
            (
                "35:10567890",
                "MINUTE_SECOND",
                (0, 0, 122, 29_190_000_000_000, 0, false),
            ),
            (
                "3510567890",
                "MINUTE_SECOND",
                (0, 0, 40_631, 49_490_000_000_000, 0, false),
            ),
            (
                "11:35:10.567890",
                "HOUR_MICROSECOND",
                (0, 0, 0, 41_710_567_890_000, 6, false),
            ),
            (
                "567890",
                "HOUR_MICROSECOND",
                (0, 0, 0, 567_890_000, 6, false),
            ),
            (
                "14:00",
                "HOUR_MINUTE",
                (0, 0, 0, 50_400_000_000_000, 0, false),
            ),
            ("14", "HOUR_MINUTE", (0, 0, 0, 840_000_000_000, 0, false)),
            (
                "12 14:00:00.345",
                "DAY_MICROSECOND",
                (0, 0, 12, 50_400_345_000_000, 6, false),
            ),
            (
                "12 14:00:00",
                "DAY_SECOND",
                (0, 0, 12, 50_400_000_000_000, 0, false),
            ),
            (
                "12 14:00",
                "DAY_MINUTE",
                (0, 0, 12, 50_400_000_000_000, 0, false),
            ),
            (
                "12 14",
                "DAY_HOUR",
                (0, 0, 12, 50_400_000_000_000, 0, false),
            ),
            ("1:1", "DAY_HOUR", (0, 0, 1, 3_600_000_000_000, 0, false)),
            ("aa1bb1", "DAY_HOUR", (0, 0, 1, 3_600_000_000_000, 0, false)),
            ("-1:1", "DAY_HOUR", (0, 0, -1, -3_600_000_000_000, 0, false)),
            (
                "-aa1bb1",
                "DAY_HOUR",
                (0, 0, -1, -3_600_000_000_000, 0, false),
            ),
            ("2019-12", "YEAR_MONTH", (2019, 12, 0, 0, 0, false)),
            ("1 1", "YEAR_MONTH", (1, 1, 0, 0, 0, false)),
            ("aa1bb1", "YEAR_MONTH", (1, 1, 0, 0, 0, false)),
            ("-1 1", "YEAR_MONTH", (-1, -1, 0, 0, 0, false)),
            ("-aa1bb1", "YEAR_MONTH", (-1, -1, 0, 0, 0, false)),
            (
                " \t\n\r\n - aa1bb1 \t\n ",
                "YEAR_MONTH",
                (-1, -1, 0, 0, 0, false),
            ),
            ("1.111", "MICROSECOND", (0, 0, 0, 1_000, 6, true)),
            ("1.111", "DAY", (0, 0, 1, 0, 0, true)),
        ] {
            let parsed = parse_duration_value(unit, format).unwrap();
            assert_eq!(
                (
                    parsed.years,
                    parsed.months,
                    parsed.days,
                    parsed.nanoseconds,
                    parsed.fsp,
                    parsed.truncated,
                ),
                expected,
                "{unit} {format}"
            );
        }
    }

    #[test]
    fn test_extract_duration_value_source_rows() {
        for (unit, format, expected) in [
            ("MICROSECOND", "50", "00:00:00.000050"),
            ("SECOND", "50", "00:00:50"),
            ("MINUTE", "10", "00:10:00"),
            ("HOUR", "10", "10:00:00"),
            ("DAY", "1", "24:00:00"),
            ("WEEK", "2", "336:00:00"),
            ("SECOND_MICROSECOND", "61.01", "00:01:01.010000"),
            ("MINUTE_MICROSECOND", "01:61.01", "00:02:01.010000"),
            ("MINUTE_SECOND", "61:61", "01:02:01"),
            ("HOUR_MICROSECOND", "01:61:01.01", "02:01:01.010000"),
            ("HOUR_SECOND", "01:61:01", "02:01:01"),
            ("HOUr_MINUTE", "2:2", "02:02:00"),
            ("DAY_MICRoSECOND", "1 1:1:1.02", "25:01:01.020000"),
            ("DAY_SeCOND", "1 02:03:04", "26:03:04"),
            ("DAY_MINUTE", "1 1:2", "25:02:00"),
            ("DAY_HOUr", "1 1", "25:00:00"),
            ("day", "34", "816:00:00"),
            ("SECOND", "50.-2", "00:00:50"),
            ("MONTH", "1", "720:00:00"),
        ] {
            assert_eq!(
                extract_duration_value(unit, format).unwrap().to_string(),
                expected,
                "{unit} {format}"
            );
        }
        for (unit, format) in [
            ("DAY", "-35"),
            ("SECOND", "-3020400"),
            ("MONTH", "-2"),
            ("DAY_second", "34 23:59:59"),
            ("DAY_hOUR", "-34 23"),
        ] {
            assert!(
                extract_duration_value(unit, format).is_err(),
                "{unit} {format}"
            );
        }
    }

    fn datetime(input: &str) -> Result<ParsedTime, TimeError> {
        parse_datetime(input, &chrono_tz::UTC, true, false)
    }

    #[test]
    fn test_parse_datetime_source_rows() {
        for (input, expected) in [
            ("2012-12-31 11:30:45", "2012-12-31 11:30:45"),
            ("0000-00-00 00:00:00", "0000-00-00 00:00:00"),
            ("0001-01-01 00:00:00", "0001-01-01 00:00:00"),
            ("00-12-31 11:30:45", "2000-12-31 11:30:45"),
            ("12-12-31 11:30:45", "2012-12-31 11:30:45"),
            ("2012-12-31", "2012-12-31 00:00:00"),
            ("20121231", "2012-12-31 00:00:00"),
            ("121231", "2012-12-31 00:00:00"),
            ("2012^12^31 11+30+45", "2012-12-31 11:30:45"),
            ("2012^12^31T11+30+45", "2012-12-31 11:30:45"),
            ("2012-2-1 11:30:45", "2012-02-01 11:30:45"),
            ("12-2-1 11:30:45", "2012-02-01 11:30:45"),
            ("20121231113045", "2012-12-31 11:30:45"),
            ("121231113045", "2012-12-31 11:30:45"),
            ("2012-02-29", "2012-02-29 00:00:00"),
            ("00-00-00", "0000-00-00 00:00:00"),
            ("00-00-00 00:00:00.123", "2000-00-00 00:00:00.123"),
            ("11111111111", "2011-11-11 11:11:01"),
            ("1701020301.", "2017-01-02 03:01:00"),
            ("1701020304.1", "2017-01-02 03:04:01.0"),
            ("1701020302.11", "2017-01-02 03:02:11.00"),
            ("170102036", "2017-01-02 03:06:00"),
            ("170102039.", "2017-01-02 03:09:00"),
            ("170102037.11", "2017-01-02 03:07:11.00"),
            ("2018-01-01 18", "2018-01-01 18:00:00"),
            ("18-01-01 18", "2018-01-01 18:00:00"),
            ("2018.01.01", "2018-01-01 00:00:00.00"),
            ("2020.10.10 10.10.10", "2020-10-10 10:10:10.00"),
            ("2020-10-10 10-10.10", "2020-10-10 10:10:10.00"),
            ("2020-10-10 10.10", "2020-10-10 10:10:00.00"),
            ("2018.01.01 00:00:00", "2018-01-01 00:00:00"),
            ("2018/01/01-00:00:00", "2018-01-01 00:00:00"),
            ("4710072", "2047-10-07 02:00:00"),
            ("2016-06-01 00:00:00 00:00:00", "2016-06-01 00:00:00"),
            ("2020-06-01 00:00:00ads!,?*da;dsx", "2020-06-01 00:00:00"),
            ("2020-05-28 23:59:59 00:00:00", "2020-05-28 23:59:59"),
            ("2020-05-28 23:59:59-00:00:00", "2020-05-28 23:59:59"),
            ("2020-05-28 23:59:59T T00:00:00", "2020-05-28 23:59:59"),
            ("2020-10-22 10:31-10:12", "2020-10-22 10:31:10"),
            ("2018.01.01 01:00:00", "2018-01-01 01:00:00"),
            (
                "2020-01-01 12:00:00.123456+05:00",
                "2020-01-01 07:00:00.123456",
            ),
            (
                "2020-01-01 12:00:00.123456-05:00",
                "2020-01-01 17:00:00.123456",
            ),
        ] {
            assert_eq!(
                datetime(input).unwrap().time.to_string(),
                expected,
                "{input}"
            );
        }
    }

    #[test]
    fn test_parse_datetime_fsp_source_rows() {
        for (input, fsp, expected) in [
            ("20170118.123", 6, "2017-01-18 12:03:00.000000"),
            ("121231113045.123345", 6, "2012-12-31 11:30:45.123345"),
            ("20121231113045.123345", 6, "2012-12-31 11:30:45.123345"),
            ("121231113045.9999999", 6, "2012-12-31 11:30:46.000000"),
            ("170105084059.575601", 0, "2017-01-05 08:41:00"),
            ("2017-01-05 23:59:59.575601", 0, "2017-01-06 00:00:00"),
            ("2017-01-31 23:59:59.575601", 0, "2017-02-01 00:00:00"),
            ("2017-00-05 23:59:58.575601", 3, "2017-00-05 23:59:58.576"),
            ("2017.00.05 23:59:58.575601", 3, "2017-00-05 23:59:58.576"),
            ("2017/00/05 23:59:58.575601", 3, "2017-00-05 23:59:58.576"),
            ("2017/00/05-23:59:58.575601", 3, "2017-00-05 23:59:58.576"),
            ("1710-10:00", 0, "1710-10-00 00:00:00"),
            ("1710.10+00", 0, "1710-10-00 00:00:00"),
            ("2020-10:15", 0, "2020-10-15 00:00:00"),
            ("2020.09-10:15", 0, "2020-09-10 15:00:00"),
            ("2.0.8 hotfix", 6, "2002-00-08 00:00:00.000000"),
        ] {
            let parsed = parse_time(
                input,
                TimeType::DateTime,
                fsp,
                false,
                true,
                false,
                &chrono_tz::UTC,
            )
            .unwrap();
            assert_eq!(parsed.time.to_string(), expected, "{input}");
        }
    }

    #[test]
    fn test_parse_datetime_source_errors_or_warnings() {
        for input in [
            "1000-01-01 00:00:70",
            "1000-13-00 00:00:00",
            "1201012736.0000",
            "1201012736",
            "10000-01-01 00:00:00",
            "1000-09-31 00:00:00",
            "1001-02-29 00:00:00",
            "20170118.999",
            "2018-01",
            "2018.01",
            "20170118-12:34",
            "20170118-1234",
            "170118-1234",
            "170118-12",
            "1710-10",
            "1710-1000",
        ] {
            match datetime(input) {
                Err(_) => {}
                Ok(parsed) => assert!(parsed.truncated, "{input}"),
            }
        }
    }

    #[test]
    fn test_parse_time_from_num_source_rows() {
        for (input, expected) in [
            (20_101_010_111_111, "2010-10-10 11:11:11"),
            (2_010_101_011_111, "0201-01-01 01:11:11"),
            (201_010_101_111, "2020-10-10 10:11:11"),
            (20_101_010_111, "2002-01-01 01:01:11"),
            (201_010_101, "2000-02-01 01:01:01"),
            (20_101_010, "2010-10-10 00:00:00"),
            (2_010_101, "0201-01-01 00:00:00"),
            (201_010, "2020-10-10 00:00:00"),
            (20_101, "2002-01-01 00:00:00"),
            (201, "2000-02-01 00:00:00"),
            (0, "0000-00-00 00:00:00"),
            (10_000_102_000_000, "1000-01-02 00:00:00"),
            (19_690_101_000_000, "1969-01-01 00:00:00"),
            (991_231_235_959, "1999-12-31 23:59:59"),
            (691_231_235_959, "2069-12-31 23:59:59"),
            (370_119_031_407, "2037-01-19 03:14:07"),
            (380_120_031_407, "2038-01-20 03:14:07"),
            (11_111_111_111, "2001-11-11 11:11:11"),
        ] {
            let parsed =
                parse_time_from_num(input, TimeType::DateTime, 0, true, false, &chrono_tz::UTC)
                    .unwrap();
            assert_eq!(parsed.time.to_string(), expected, "{input}");
        }
        for input in [
            2_010_101_011,
            2_010,
            20,
            2,
            -1,
            99_999_999_999_999,
            100_000_000_000_000,
        ] {
            assert!(
                parse_time_from_num(input, TimeType::DateTime, 0, true, false, &chrono_tz::UTC)
                    .is_err(),
                "{input}"
            );
        }
    }

    #[test]
    fn test_parse_time_from_float_string_source_rows() {
        for (input, fsp, expected) in [
            ("20170118.123", 3, "2017-01-18 00:00:00.000"),
            ("121231113045.123345", 6, "2012-12-31 11:30:45.123345"),
            ("20121231113045.123345", 6, "2012-12-31 11:30:45.123345"),
            ("121231113045.9999999", 6, "2012-12-31 11:30:46.000000"),
            ("170105084059.575601", 6, "2017-01-05 08:40:59.575601"),
        ] {
            let parsed = parse_time(
                input,
                TimeType::DateTime,
                fsp,
                true,
                true,
                false,
                &chrono_tz::UTC,
            )
            .unwrap();
            assert_eq!(parsed.time.to_string(), expected, "{input}");
        }
        for (input, fsp) in [
            ("201705051315111.22", 2),
            ("2011110859.1111", 4),
            ("191203081.1111", 4),
            ("43128.121105", 6),
        ] {
            assert!(
                parse_time(
                    input,
                    TimeType::DateTime,
                    fsp,
                    true,
                    true,
                    false,
                    &chrono_tz::UTC
                )
                .is_err(),
                "{input}"
            );
        }
    }

    #[test]
    fn test_parse_time_from_int_float_decimal_source_rows() {
        let integer =
            parse_time_from_int64(20_190_412_140_000, true, false, &chrono_tz::UTC).unwrap();
        assert_eq!(integer.kind(), TimeType::DateTime);
        assert_eq!(integer.to_string(), "2019-04-12 14:00:00");

        for (input, kind, expected, microsecond) in [
            (20_000_102.0, TimeType::Date, "2000-01-02", 0),
            (20_000_102.9, TimeType::Date, "2000-01-02", 0),
            (0.0, TimeType::Date, "0000-00-00", 0),
            (
                20_000_102_030_405.0,
                TimeType::DateTime,
                "2000-01-02 03:04:05",
                0,
            ),
            (
                20_000_102_030_405.016,
                TimeType::DateTime,
                "2000-01-02 03:04:05",
                15_625,
            ),
            (
                20_000_102_030_405.008,
                TimeType::DateTime,
                "2000-01-02 03:04:05",
                7_813,
            ),
            (
                121_212_131_313.999_98,
                TimeType::DateTime,
                "2012-12-12 13:13:13",
                999_985,
            ),
            (
                20_000_000_000_000.0,
                TimeType::DateTime,
                "2000-00-00 00:00:00",
                0,
            ),
        ] {
            let parsed = parse_time_from_float64(input, true, false, &chrono_tz::UTC).unwrap();
            assert_eq!(parsed.kind(), kind, "{input}");
            assert_eq!(parsed.to_string(), expected, "{input}");
            assert_eq!(parsed.core_time().microsecond(), microsecond, "{input}");
        }
        assert!(parse_time_from_float64(2_000.0, true, false, &chrono_tz::UTC).is_err());

        for (input, kind, expected, microsecond, fsp) in [
            ("20000102", TimeType::Date, "2000-01-02", 0, 0),
            ("20000102.9", TimeType::Date, "2000-01-02", 0, 0),
            ("0.0", TimeType::Date, "0000-00-00", 0, 0),
            (
                "20000102030405",
                TimeType::DateTime,
                "2000-01-02 03:04:05",
                0,
                0,
            ),
            (
                "20000102030405.015625",
                TimeType::DateTime,
                "2000-01-02 03:04:05.015625",
                15_625,
                6,
            ),
            (
                "20000102030405.0078125",
                TimeType::DateTime,
                "2000-01-02 03:04:05.007812",
                7_812,
                6,
            ),
            (
                "20000000000000",
                TimeType::DateTime,
                "2000-00-00 00:00:00",
                0,
                0,
            ),
        ] {
            let decimal = crate::Decimal::from_literal(input);
            let parsed = parse_time_from_decimal(&decimal, true, false, &chrono_tz::UTC).unwrap();
            assert_eq!(parsed.kind(), kind, "{input}");
            assert_eq!(parsed.to_string(), expected, "{input}");
            assert_eq!(parsed.core_time().microsecond(), microsecond, "{input}");
            assert_eq!(parsed.fsp(), fsp, "{input}");
        }
        assert!(parse_time_from_decimal(
            &crate::Decimal::from_literal("2000"),
            true,
            false,
            &chrono_tz::UTC
        )
        .is_err());
    }

    #[test]
    fn test_parse_timestamp_source_bounds() {
        assert!(parse_time(
            "2012-12-31 11:30:45",
            TimeType::Timestamp,
            0,
            false,
            false,
            false,
            &chrono_tz::UTC
        )
        .is_ok());
        for input in ["2048-12-31 11:30:45", "1969-12-31 11:30:45"] {
            assert!(
                parse_time(
                    input,
                    TimeType::Timestamp,
                    0,
                    false,
                    false,
                    false,
                    &chrono_tz::UTC
                )
                .is_err(),
                "{input}"
            );
        }
    }

    #[test]
    fn test_parse_date_source_rows() {
        for (input, expected) in [
            ("0001-12-13", "0001-12-13"),
            ("2011-12-13", "2011-12-13"),
            ("2011-12-13 10:10:10", "2011-12-13"),
            ("2015-06-01 12:12:12", "2015-06-01"),
            ("0001-01-01 00:00:00", "0001-01-01"),
            ("00-12-31", "2000-12-31"),
            ("2011\"12\"13", "2011-12-13"),
            ("2011#12#13", "2011-12-13"),
            ("2011$12$13", "2011-12-13"),
            ("2011%12%13", "2011-12-13"),
            ("2011&12&13", "2011-12-13"),
            ("2011'12'13", "2011-12-13"),
            ("2011(12(13", "2011-12-13"),
            ("2011)12)13", "2011-12-13"),
            ("2011*12*13", "2011-12-13"),
            ("2011+12+13", "2011-12-13"),
            ("2011,12,13", "2011-12-13"),
            ("2011.12.13", "2011-12-13"),
            ("2011/12/13", "2011-12-13"),
            ("2011:12:13", "2011-12-13"),
            ("2011;12;13", "2011-12-13"),
            ("2011<12<13", "2011-12-13"),
            ("2011=12=13", "2011-12-13"),
            ("2011>12>13", "2011-12-13"),
            ("2011?12?13", "2011-12-13"),
            ("2011@12@13", "2011-12-13"),
            ("2011[12[13", "2011-12-13"),
            ("2011\\12\\13", "2011-12-13"),
            ("2011]12]13", "2011-12-13"),
            ("2011^12^13", "2011-12-13"),
            ("2011_12_13", "2011-12-13"),
            ("2011`12`13", "2011-12-13"),
            ("2011{12{13", "2011-12-13"),
            ("2011|12|13", "2011-12-13"),
            ("2011}12}13", "2011-12-13"),
            ("2011~12~13", "2011-12-13"),
            ("2011~12~13 12~12~12", "2011-12-13"),
            ("2011~12~13T12~12~12", "2011-12-13"),
            ("2011~12~13~12~12~12", "2011-12-13"),
            ("20111213", "2011-12-13"),
            ("111213", "2011-12-13"),
            (" 2011-12-13", "2011-12-13"),
            ("2011-12-13 ", "2011-12-13"),
            ("   2011-12-13    ", "2011-12-13"),
            ("2011-12--13", "2011-12-13"),
            ("2011--12-13", "2011-12-13"),
            ("2011-12..13", "2011-12-13"),
            ("2011----12----13", "2011-12-13"),
            ("2011~/.12)_#13T T.12~)12[~12", "2011-12-13"),
            ("   2011----12----13    ", "2011-12-13"),
        ] {
            let parsed = parse_time(
                input,
                TimeType::Date,
                0,
                false,
                true,
                false,
                &chrono_tz::UTC,
            )
            .unwrap();
            assert_eq!(parsed.time.to_string(), expected, "{input}");
        }
        for input in [
            "0121231",
            "1201012736.0000",
            "1201012736",
            "2019.01",
            "2019 01 02",
            "2019A01A02",
            "2019-01T02",
            "2011-12-13 10:10T10",
            "2019–01–02",
            "2019—01—02",
        ] {
            assert!(
                parse_time(
                    input,
                    TimeType::Date,
                    0,
                    false,
                    true,
                    false,
                    &chrono_tz::UTC
                )
                .is_err(),
                "{input}"
            );
        }
    }

    #[test]
    fn test_parse_with_timezone_source_rows() {
        for (literal, fsp, system_offset, expected_timestamp) in [
            ("2006-01-02T15:04:05Z", 0, 0, 1_136_214_245),
            ("2006-01-02T15:04:05Z", 0, 10 * 3_600, 1_136_214_245),
            ("2020-10-21T16:05:10.50Z", 2, -10 * 3_600, 1_603_296_310),
            ("2020-10-21T16:05:10.50+08", 2, -10 * 3_600, 1_603_267_510),
            ("2020-10-21T16:05:10.50-0700", 2, -10 * 3_600, 1_603_321_510),
            (
                "2020-10-21T16:05:10.50+09:00",
                2,
                -10 * 3_600,
                1_603_263_910,
            ),
            ("2006-01-02T15:04:05+09:00", 0, 8 * 3_600, 1_136_181_845),
            ("2006-01-02T15:04:05-02:00", 0, 3 * 3_600, 1_136_221_445),
            ("2006-01-02T15:04:05-14:00", 0, 14 * 3_600, 1_136_264_645),
        ] {
            let system = FixedOffset::east_opt(system_offset).unwrap();
            let parsed = parse_time(
                literal,
                TimeType::Timestamp,
                fsp,
                false,
                false,
                false,
                &system,
            )
            .unwrap();
            let instant = parsed.time.core_time().to_datetime(&system).unwrap();
            assert_eq!(instant.timestamp(), expected_timestamp, "{literal}");
        }
    }
}
