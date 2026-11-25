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

use chrono::TimeZone;
use unicode_general_category::{get_general_category, GeneralCategory};

use crate::{CoreTime, Time, TimeError, TimeType};

mod unicode_delta;

const MONTH_NAMES: [&str; 12] = [
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

#[derive(Default)]
struct ParsedTime {
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    microsecond: u32,
    hour12: bool,
    hour24: bool,
    meridiem: Option<Meridiem>,
}

enum Meridiem {
    Am,
    Pm,
    Missing,
}

impl Time {
    /// Parses a MySQL `STR_TO_DATE` value.
    ///
    /// The boolean reports trailing source characters, matching TiDB's warning
    /// result without coupling the datatype crate to a statement context.
    pub fn str_to_date<TZ: TimeZone>(
        date: &str,
        format: &str,
        allow_zero_in_date: bool,
        allow_invalid_date: bool,
        timezone: &TZ,
    ) -> Result<(Self, bool), TimeError> {
        Self::str_to_date_bytes(
            date.as_bytes(),
            format.as_bytes(),
            allow_zero_in_date,
            allow_invalid_date,
            timezone,
        )
        .map_err(|(_, error)| error)
    }

    /// Parses the original string bytes and returns the value Go retains on failure.
    /// The boolean reports trailing input that should produce a warning.
    #[inline]
    pub fn str_to_date_bytes<TZ: TimeZone>(
        date: &[u8],
        format: &[u8],
        allow_zero_in_date: bool,
        allow_invalid_date: bool,
        timezone: &TZ,
    ) -> Result<(Self, bool), (Self, TimeError)> {
        let mut parsed = ParsedTime::default();
        let warning = parse_format(&mut parsed, date, format)
            .and_then(|warning| fix_meridiem(&mut parsed).map(|()| warning))
            .map_err(|error| (Self::from_go_raw_like_go(0), error))?;
        let result = Self::new(
            CoreTime::from_date(
                parsed.year as u16,
                parsed.month,
                parsed.day,
                parsed.hour,
                parsed.minute,
                parsed.second,
                parsed.microsecond,
            ),
            TimeType::DateTime,
            0,
        )
        .expect("zero precision");
        result
            .validate(allow_zero_in_date, allow_invalid_date, timezone)
            .map_err(|error| (result, error))?;
        Ok((result, warning))
    }
}

/// Classifies whether a MySQL date format contains time and date fields.
#[must_use]
pub fn get_format_type(format: &str) -> (bool, bool) {
    get_format_type_bytes(format.as_bytes())
}

/// Classifies a format using the original MySQL string bytes.
#[must_use]
pub fn get_format_type_bytes(format: &[u8]) -> (bool, bool) {
    let mut format = skip_whitespace(format);
    let mut is_duration = false;
    let mut is_date = false;
    while !format.is_empty() {
        let Ok((token, remaining)) = next_token(format) else {
            break;
        };
        format = remaining;
        match token {
            b"%h" | b"%H" | b"%i" | b"%I" | b"%s" | b"%S" | b"%k" | b"%l" | b"%f" | b"%r"
            | b"%T" => is_duration = true,
            b"%y" | b"%Y" | b"%m" | b"%M" | b"%c" | b"%b" | b"%D" | b"%d" | b"%e" => {
                is_date = true;
            }
            _ => {}
        }
        if is_duration && is_date {
            break;
        }
    }
    (is_duration, is_date)
}

fn parse_format(
    parsed: &mut ParsedTime,
    mut date: &[u8],
    mut format: &[u8],
) -> Result<bool, TimeError> {
    loop {
        date = skip_whitespace(date);
        format = skip_whitespace(format);
        if format.is_empty() {
            return Ok(!date.is_empty());
        }
        let (token, remaining) = next_token(format)?;
        if date.is_empty() {
            // Go records the exact, unparsed token with a zero value.
            match token {
                b"%p" => parsed.meridiem = Some(Meridiem::Missing),
                b"%H" => parsed.hour24 = true,
                b"%h" => parsed.hour12 = true,
                _ => {}
            }
            return Ok(false);
        }
        format = remaining;
        date = parse_token(parsed, date, token)?;
    }
}

fn next_token(format: &[u8]) -> Result<(&[u8], &[u8]), TimeError> {
    let width = if format[0] == b'%' { 2 } else { 1 };
    format.split_at_checked(width).ok_or(TimeError::InvalidDate)
}

fn parse_token<'a>(
    parsed: &mut ParsedTime,
    input: &'a [u8],
    token: &[u8],
) -> Result<&'a [u8], TimeError> {
    match token {
        b"%b" => parse_month_name(parsed, input, true),
        b"%M" => parse_month_name(parsed, input, false),
        b"%c" | b"%m" => {
            let (value, remaining) = parse_digits(input, 2)?;
            if value > 12 {
                return Err(TimeError::InvalidDate);
            }
            parsed.month = value as u8;
            Ok(remaining)
        }
        b"%d" | b"%e" => {
            let (value, remaining) = parse_digits(input, 2)?;
            if value > 31 {
                return Err(TimeError::InvalidDate);
            }
            parsed.day = value as u8;
            Ok(remaining)
        }
        b"%f" => {
            let (value, digits, remaining) = parse_optional_digits(input, 6);
            parsed.microsecond = value * 10_u32.pow(6 - digits as u32);
            Ok(remaining)
        }
        b"%h" | b"%I" | b"%l" => {
            let (value, remaining) = parse_digits(input, 2)?;
            if !(1..=12).contains(&value) {
                return Err(TimeError::InvalidClock);
            }
            parsed.hour = value as u8;
            parsed.hour12 = true;
            Ok(remaining)
        }
        b"%H" | b"%k" => {
            let (value, remaining) = parse_digits(input, 2)?;
            if value > 23 {
                return Err(TimeError::InvalidClock);
            }
            parsed.hour = value as u8;
            parsed.hour24 = true;
            Ok(remaining)
        }
        b"%i" => {
            let (value, remaining) = parse_digits(input, 2)?;
            if value > 59 {
                return Err(TimeError::InvalidClock);
            }
            parsed.minute = value as u8;
            Ok(remaining)
        }
        b"%s" | b"%S" => {
            let (value, remaining) = parse_digits(input, 2)?;
            if value > 59 {
                return Err(TimeError::InvalidClock);
            }
            parsed.second = value as u8;
            Ok(remaining)
        }
        b"%p" => {
            if has_prefix(input, b"AM") {
                parsed.meridiem = Some(Meridiem::Am);
                Ok(&input[2..])
            } else if has_prefix(input, b"PM") {
                parsed.meridiem = Some(Meridiem::Pm);
                Ok(&input[2..])
            } else {
                Err(TimeError::InvalidClock)
            }
        }
        b"%r" => parse_compound_time(parsed, input, true),
        b"%T" => parse_compound_time(parsed, input, false),
        b"%Y" => parse_year(parsed, input, 4),
        b"%y" => parse_year(parsed, input, 2),
        b"%j" => {
            let (value, remaining) = parse_digits(input, 3)?;
            if value == 0 {
                Err(TimeError::InvalidDate)
            } else {
                Ok(remaining)
            }
        }
        b"%#" => Ok(skip_class(input, is_go_number)),
        b"%." => Ok(skip_class(input, is_go_punctuation)),
        b"%@" => Ok(skip_class(input, is_go_letter)),
        _ if input.starts_with(token) => Ok(&input[token.len()..]),
        _ => Err(TimeError::InvalidDate),
    }
}

fn parse_month_name<'a>(
    parsed: &mut ParsedTime,
    input: &'a [u8],
    abbreviated: bool,
) -> Result<&'a [u8], TimeError> {
    for (index, name) in MONTH_NAMES.iter().enumerate() {
        let candidate = if abbreviated { &name[..3] } else { name };
        if has_prefix(input, candidate.as_bytes()) {
            parsed.month = index as u8 + 1;
            return Ok(&input[candidate.len()..]);
        }
    }
    Err(TimeError::InvalidDate)
}

fn parse_year<'a>(
    parsed: &mut ParsedTime,
    input: &'a [u8],
    limit: usize,
) -> Result<&'a [u8], TimeError> {
    let (year, digits, remaining) = parse_digits_with_len(input, limit)?;
    parsed.year = if digits <= 2 { adjust_year(year) } else { year } as i32;
    Ok(remaining)
}

fn adjust_year(year: u32) -> u32 {
    match year {
        0..=69 => 2000 + year,
        70..=99 => 1900 + year,
        _ => year,
    }
}

fn parse_compound_time<'a>(
    parsed: &mut ParsedTime,
    input: &'a [u8],
    twelve_hour: bool,
) -> Result<&'a [u8], TimeError> {
    let (hour, _, mut remaining) = parse_digits_with_len(input, 2)?;
    if (twelve_hour && !(1..=12).contains(&hour)) || (!twelve_hour && hour > 23) {
        return Err(TimeError::InvalidClock);
    }
    parsed.hour = if twelve_hour && hour == 12 {
        0
    } else {
        hour as u8
    };
    if remaining.is_empty() {
        return Ok(remaining);
    }
    remaining = parse_separator(remaining)?;
    if remaining.is_empty() {
        return Ok(remaining);
    }
    let (minute, next) = parse_digits(remaining, 2)?;
    if minute > 59 {
        return Err(TimeError::InvalidClock);
    }
    parsed.minute = minute as u8;
    remaining = next;
    if remaining.is_empty() {
        return Ok(remaining);
    }
    remaining = parse_separator(remaining)?;
    if remaining.is_empty() {
        return Ok(remaining);
    }
    let (second, next) = parse_digits(remaining, 2)?;
    if second > 59 {
        return Err(TimeError::InvalidClock);
    }
    parsed.second = second as u8;
    remaining = skip_whitespace(next);
    if !twelve_hour || remaining.is_empty() {
        return Ok(remaining);
    }
    if has_prefix(remaining, b"AM") {
        Ok(&remaining[2..])
    } else if has_prefix(remaining, b"PM") {
        parsed.hour += 12;
        Ok(&remaining[2..])
    } else {
        Err(TimeError::InvalidClock)
    }
}

fn parse_separator(input: &[u8]) -> Result<&[u8], TimeError> {
    let input = skip_whitespace(input);
    let Some(input) = input.strip_prefix(b":") else {
        return Err(TimeError::InvalidClock);
    };
    Ok(skip_whitespace(input))
}

fn fix_meridiem(parsed: &mut ParsedTime) -> Result<(), TimeError> {
    let Some(ref meridiem) = parsed.meridiem else {
        if parsed.hour12 && parsed.hour == 12 {
            parsed.hour = 0;
        }
        return Ok(());
    };
    if parsed.hour24 || parsed.hour == 0 {
        return Err(TimeError::InvalidClock);
    }
    if parsed.hour == 12 {
        if matches!(meridiem, Meridiem::Am) {
            parsed.hour = 0;
        }
    } else if matches!(meridiem, Meridiem::Pm) {
        parsed.hour += 12;
    }
    Ok(())
}

fn parse_digits(input: &[u8], limit: usize) -> Result<(u32, &[u8]), TimeError> {
    let (value, _, remaining) = parse_digits_with_len(input, limit)?;
    Ok((value, remaining))
}

fn parse_digits_with_len(input: &[u8], limit: usize) -> Result<(u32, usize, &[u8]), TimeError> {
    let (value, digits, remaining) = parse_optional_digits(input, limit);
    if digits == 0 {
        Err(TimeError::InvalidDate)
    } else {
        Ok((value, digits, remaining))
    }
}

fn parse_optional_digits(input: &[u8], limit: usize) -> (u32, usize, &[u8]) {
    let mut value = 0;
    let mut digits = 0;
    for &byte in input.iter().take(limit) {
        if !byte.is_ascii_digit() {
            break;
        }
        value = value * 10 + u32::from(byte - b'0');
        digits += 1;
    }
    (value, digits, &input[digits..])
}

fn has_prefix(input: &[u8], prefix: &[u8]) -> bool {
    input
        .get(..prefix.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
}

#[inline]
fn skip_whitespace(mut input: &[u8]) -> &[u8] {
    while let Some(&first) = input.first() {
        match first {
            b'\t'..=b'\r' | b' ' => input = &input[1..],
            0x80.. => return skip_unicode_whitespace(input),
            _ => break,
        }
    }
    input
}

#[cold]
fn skip_unicode_whitespace(mut input: &[u8]) -> &[u8] {
    while let Some((character, width)) = first_character(input) {
        if !character.is_whitespace() {
            break;
        }
        input = &input[width..];
    }
    input
}

fn first_character(input: &[u8]) -> Option<(char, usize)> {
    let (codepoint, width) = crate::collation::decode_rune(input).ok()?;
    Some((char::from_u32(codepoint)?, width))
}

fn skip_class(input: &[u8], predicate: fn(char) -> bool) -> &[u8] {
    let mut index = 0;
    let mut end = 0;
    while let Some((character, width)) = first_character(&input[index..]) {
        if !predicate(character) {
            break;
        }
        // Go's skipAll functions keep the last rune's start byte plus one.
        end = index + 1;
        index += width;
    }
    &input[end..]
}

fn is_go_letter(character: char) -> bool {
    if character.is_ascii() {
        return character.is_ascii_alphabetic();
    }
    matches!(
        get_general_category(character),
        GeneralCategory::UppercaseLetter
            | GeneralCategory::LowercaseLetter
            | GeneralCategory::TitlecaseLetter
            | GeneralCategory::ModifierLetter
            | GeneralCategory::OtherLetter
    ) || unicode_delta::added_letters(character)
}

fn is_go_number(character: char) -> bool {
    if character.is_ascii() {
        return character.is_ascii_digit();
    }
    matches!(
        get_general_category(character),
        GeneralCategory::DecimalNumber
            | GeneralCategory::LetterNumber
            | GeneralCategory::OtherNumber
    ) || unicode_delta::added_numbers(character)
}

/// Returns whether a character belongs to Go's `unicode.IsPunct` set.
///
/// The expression-level `STR_TO_DATE` implementation shares this classifier
/// so both Rust owners consume exactly the same source-version table.
pub fn is_go_punctuation(character: char) -> bool {
    matches!(
        get_general_category(character),
        GeneralCategory::ClosePunctuation
            | GeneralCategory::ConnectorPunctuation
            | GeneralCategory::DashPunctuation
            | GeneralCategory::FinalPunctuation
            | GeneralCategory::InitialPunctuation
            | GeneralCategory::OpenPunctuation
            | GeneralCategory::OtherPunctuation
    ) || unicode_delta::added_punctuation(character)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str, format: &str, allow_invalid: bool) -> Result<CoreTime, TimeError> {
        Time::str_to_date(input, format, true, allow_invalid, &chrono_tz::UTC)
            .map(|(time, _)| time.core_time())
    }

    #[test]
    fn test_str_to_date() {
        let rows = [
            (
                "01,05,2013",
                "%d,%m,%Y",
                CoreTime::from_date(2013, 5, 1, 0, 0, 0, 0),
            ),
            (
                "5 12 2021",
                "%m%d%Y",
                CoreTime::from_date(2021, 5, 12, 0, 0, 0, 0),
            ),
            (
                "May 01, 2013",
                "%M %d,%Y",
                CoreTime::from_date(2013, 5, 1, 0, 0, 0, 0),
            ),
            (
                "a09:30:17",
                "a%h:%i:%s",
                CoreTime::from_date(0, 0, 0, 9, 30, 17, 0),
            ),
            (
                "09:30:17a",
                "%h:%i:%s",
                CoreTime::from_date(0, 0, 0, 9, 30, 17, 0),
            ),
            (
                "12:43:24",
                "%h:%i:%s",
                CoreTime::from_date(0, 0, 0, 0, 43, 24, 0),
            ),
            ("abc", "abc", CoreTime::default()),
            ("09", "%m", CoreTime::from_date(0, 9, 0, 0, 0, 0, 0)),
            ("09", "%s", CoreTime::from_date(0, 0, 0, 0, 0, 9, 0)),
            (
                "12:43:24 AM",
                "%r",
                CoreTime::from_date(0, 0, 0, 0, 43, 24, 0),
            ),
            (
                "12:43:24 PM",
                "%r",
                CoreTime::from_date(0, 0, 0, 12, 43, 24, 0),
            ),
            (
                "11:43:24 PM",
                "%r",
                CoreTime::from_date(0, 0, 0, 23, 43, 24, 0),
            ),
            ("00:12:13", "%T", CoreTime::from_date(0, 0, 0, 0, 12, 13, 0)),
            (
                "23:59:59",
                "%T",
                CoreTime::from_date(0, 0, 0, 23, 59, 59, 0),
            ),
            ("00/00/0000", "%m/%d/%Y", CoreTime::default()),
            (
                "04/30/2004",
                "%m/%d/%Y",
                CoreTime::from_date(2004, 4, 30, 0, 0, 0, 0),
            ),
            (
                "15:35:00",
                "%H:%i:%s",
                CoreTime::from_date(0, 0, 0, 15, 35, 0, 0),
            ),
            (
                "Jul 17 33",
                "%b %k %S",
                CoreTime::from_date(0, 7, 0, 17, 0, 33, 0),
            ),
            (
                "2016-January:7 432101",
                "%Y-%M:%l %f",
                CoreTime::from_date(2016, 1, 0, 7, 0, 0, 432_101),
            ),
            (
                "10:13 PM",
                "%l:%i %p",
                CoreTime::from_date(0, 0, 0, 22, 13, 0, 0),
            ),
            ("12:00:00 AM", "%h:%i:%s %p", CoreTime::default()),
            (
                "12:00:00 PM",
                "%h:%i:%s %p",
                CoreTime::from_date(0, 0, 0, 12, 0, 0, 0),
            ),
            (
                "12:00:00 PM",
                "%I:%i:%s %p",
                CoreTime::from_date(0, 0, 0, 12, 0, 0, 0),
            ),
            (
                "1:00:00 PM",
                "%h:%i:%s %p",
                CoreTime::from_date(0, 0, 0, 13, 0, 0, 0),
            ),
            (
                "18/10/22",
                "%y/%m/%d",
                CoreTime::from_date(2018, 10, 22, 0, 0, 0, 0),
            ),
            (
                "8/10/22",
                "%y/%m/%d",
                CoreTime::from_date(2008, 10, 22, 0, 0, 0, 0),
            ),
            (
                "69/10/22",
                "%y/%m/%d",
                CoreTime::from_date(2069, 10, 22, 0, 0, 0, 0),
            ),
            (
                "70/10/22",
                "%y/%m/%d",
                CoreTime::from_date(1970, 10, 22, 0, 0, 0, 0),
            ),
            (
                "18/10/22",
                "%Y/%m/%d",
                CoreTime::from_date(2018, 10, 22, 0, 0, 0, 0),
            ),
            (
                "2018/10/22",
                "%Y/%m/%d",
                CoreTime::from_date(2018, 10, 22, 0, 0, 0, 0),
            ),
            (
                "8/10/22",
                "%Y/%m/%d",
                CoreTime::from_date(2008, 10, 22, 0, 0, 0, 0),
            ),
            (
                "69/10/22",
                "%Y/%m/%d",
                CoreTime::from_date(2069, 10, 22, 0, 0, 0, 0),
            ),
            (
                "70/10/22",
                "%Y/%m/%d",
                CoreTime::from_date(1970, 10, 22, 0, 0, 0, 0),
            ),
            (
                "18/10/22",
                "%Y/%m/%d",
                CoreTime::from_date(2018, 10, 22, 0, 0, 0, 0),
            ),
            (
                "100/10/22",
                "%Y/%m/%d",
                CoreTime::from_date(100, 10, 22, 0, 0, 0, 0),
            ),
            (
                "09/10/1021",
                "%d/%m/%y",
                CoreTime::from_date(2010, 10, 9, 0, 0, 0, 0),
            ),
            (
                "09/10/1021",
                "%d/%m/%Y",
                CoreTime::from_date(1021, 10, 9, 0, 0, 0, 0),
            ),
            (
                "09/10/10",
                "%d/%m/%Y",
                CoreTime::from_date(2010, 10, 9, 0, 0, 0, 0),
            ),
            (
                "31/may/2016 12:34:56.1234",
                "%d/%b/%Y %H:%i:%S.%f",
                CoreTime::from_date(2016, 5, 31, 12, 34, 56, 123_400),
            ),
            (
                "30/april/2016 12:34:56.",
                "%d/%M/%Y %H:%i:%s.%f",
                CoreTime::from_date(2016, 4, 30, 12, 34, 56, 0),
            ),
            (
                "31/mAy/2016 12:34:56.1234",
                "%d/%b/%Y %H:%i:%S.%f",
                CoreTime::from_date(2016, 5, 31, 12, 34, 56, 123_400),
            ),
            (
                "30/apRil/2016 12:34:56.",
                "%d/%M/%Y %H:%i:%s.%f",
                CoreTime::from_date(2016, 4, 30, 12, 34, 56, 0),
            ),
            (
                " 04 :13:56 AM13/05/2019",
                "%r %d/%c/%Y",
                CoreTime::from_date(2019, 5, 13, 4, 13, 56, 0),
            ),
            (
                "12: 13:56 AM 13/05/2019",
                "%r%d/%c/%Y",
                CoreTime::from_date(2019, 5, 13, 0, 13, 56, 0),
            ),
            (
                "12:13 :56 pm 13/05/2019",
                "%r %d/%c/%Y",
                CoreTime::from_date(2019, 5, 13, 12, 13, 56, 0),
            ),
            (
                "12:3: 56pm  13/05/2019",
                "%r %d/%c/%Y",
                CoreTime::from_date(2019, 5, 13, 12, 3, 56, 0),
            ),
            (
                "11:13:56",
                "%r",
                CoreTime::from_date(0, 0, 0, 11, 13, 56, 0),
            ),
            ("11:13", "%r", CoreTime::from_date(0, 0, 0, 11, 13, 0, 0)),
            ("11:", "%r", CoreTime::from_date(0, 0, 0, 11, 0, 0, 0)),
            ("11", "%r", CoreTime::from_date(0, 0, 0, 11, 0, 0, 0)),
            ("12", "%r", CoreTime::default()),
            (
                " 4 :13:56 13/05/2019",
                "%T %d/%c/%Y",
                CoreTime::from_date(2019, 5, 13, 4, 13, 56, 0),
            ),
            (
                "23: 13:56  13/05/2019",
                "%T%d/%c/%Y",
                CoreTime::from_date(2019, 5, 13, 23, 13, 56, 0),
            ),
            (
                "12:13 :56 13/05/2019",
                "%T %d/%c/%Y",
                CoreTime::from_date(2019, 5, 13, 12, 13, 56, 0),
            ),
            (
                "19:3: 56  13/05/2019",
                "%T %d/%c/%Y",
                CoreTime::from_date(2019, 5, 13, 19, 3, 56, 0),
            ),
            ("21:13", "%T", CoreTime::from_date(0, 0, 0, 21, 13, 0, 0)),
            ("21:", "%T", CoreTime::from_date(0, 0, 0, 21, 0, 0, 0)),
            (
                " 2/Jun",
                "%d/%b/%Y",
                CoreTime::from_date(0, 6, 2, 0, 0, 0, 0),
            ),
            (" liter", "lit era l", CoreTime::default()),
            (
                "29/Feb/2020 12:34:56.",
                "%d/%b/%Y %H:%i:%s.%f",
                CoreTime::from_date(2020, 2, 29, 12, 34, 56, 0),
            ),
            (
                "31/April/2016 12:34:56.",
                "%d/%M/%Y %H:%i:%s.%f",
                CoreTime::from_date(2016, 4, 31, 12, 34, 56, 0),
            ),
            (
                "29/Feb/2021 12:34:56.",
                "%d/%b/%Y %H:%i:%s.%f",
                CoreTime::from_date(2021, 2, 29, 12, 34, 56, 0),
            ),
            (
                "30/Feb/2016 12:34:56.1234",
                "%d/%b/%Y %H:%i:%S.%f",
                CoreTime::from_date(2016, 2, 30, 12, 34, 56, 123_400),
            ),
        ];
        assert_eq!(rows.len(), 63, "one entry per Go success source row");
        for (input, format, expected) in rows {
            assert_eq!(
                parse(input, format, true).unwrap(),
                expected,
                "{input} {format}"
            );
        }
    }

    #[test]
    fn punctuation_token_matches_go_unicode_punctuation() {
        assert!(parse("2013¿5", "%Y%.%c", true).is_err());
        assert!(parse("2013+5", "%Y%.%c", true).is_err());
        assert!(parse("2013\u{1b4e}5", "%Y%.%c", true).is_err());
    }

    #[test]
    fn skip_tokens_use_go_unicode_17_classes() {
        // A final ASCII member of the class advances past the Unicode rune.
        let expected = CoreTime::from_date(2024, 2, 29, 0, 0, 0, 0);
        for (input, format) in [
            ("\u{088f}foo2024-02-29", "%@%Y-%m-%d"),
            ("\u{11de0}123 2024-02-29", "%#%Y-%m-%d"),
            ("2024\u{10ed0}-02-29", "%Y%.%m-%d"),
        ] {
            assert_eq!(parse(input, format, false), Ok(expected));
        }
    }

    #[test]
    fn exhausted_format_tokens_preserve_go_meridiem_fix_state() {
        // Go records ctx["%p"] = 0 after the clock is consumed. With `%H`,
        // mysqlTimeFix rejects the combination; with `%h`, the absent AM/PM
        // token is treated as AM and the parsed hour remains valid.
        assert!(parse("11:30:45", "%H:%i:%s %p", true).is_err());
        assert_eq!(
            parse("11:30:45", "%h:%i:%s %p", true),
            Ok(CoreTime::from_date(0, 0, 0, 11, 30, 45, 0))
        );
        assert!(parse("", "%p", true).is_err());
    }

    /// Go `Time.StrToDate` forwards `FlagIgnoreZeroInDate` to `Time.Check`.
    /// A partial format therefore keeps its zero month/day only when the
    /// caller explicitly allows zero-in-date values.
    #[test]
    fn str_to_date_zero_in_date_flag_is_not_hardcoded() {
        let refused = Time::str_to_date("2013-05", "%Y-%m", false, false, &chrono_tz::UTC);
        assert_eq!(refused, Err(TimeError::ZeroInDate));

        let accepted = Time::str_to_date("2013-05", "%Y-%m", true, false, &chrono_tz::UTC)
            .expect("IgnoreZeroInDate keeps the partial date");
        assert_eq!(accepted.0.to_string(), "2013-05-00 00:00:00");
    }

    #[test]
    fn test_str_to_date_errors() {
        let rows = [
            ("04/31/2004", "%m/%d/%Y", false),
            ("29/Feb/2021 12:34:56.", "%d/%b/%Y %H:%i:%s.%f", false),
            ("512 2021", "%m%d %Y", true),
            ("a09:30:17", "%h:%i:%s", true),
            ("12:43:24 a", "%r", true),
            ("23:60:12", "%T", true),
            ("18", "%l", true),
            ("00:21:22 AM", "%h:%i:%s %p", true),
            ("100/10/22", "%y/%m/%d", true),
            ("2010-11-12 11 am", "%Y-%m-%d %H %p", true),
            ("2010-11-12 13 am", "%Y-%m-%d %h %p", true),
            ("2010-11-12 0 am", "%Y-%m-%d %h %p", true),
            ("15 SEPTEMB 2001", "%d %M %Y", true),
            ("13:13:56 AM13/5/2019", "%r", true),
            ("00:13:56 AM13/05/2019", "%r", true),
            ("00:13:56 pM13/05/2019", "%r", true),
            ("11:13:56a", "%r", true),
        ];
        assert_eq!(rows.len(), 17, "one entry per Go error source row");
        for (input, format, allow_invalid) in rows {
            assert!(
                parse(input, format, allow_invalid).is_err(),
                "{input} {format}"
            );
        }
    }

    /// Complete translation of `pkg/types/time_test.go::TestGetFormatType`.
    #[test]
    fn test_get_format_type() {
        assert_eq!(get_format_type("TEST"), (false, false));
        assert_eq!(get_format_type("%y %m %d 2019 04 01"), (false, true));
        assert_eq!(get_format_type("%h 30"), (true, false));
    }

    #[test]
    fn get_format_type_supplemental_rows() {
        assert_eq!(get_format_type("%Y-%m-%d %H:%i:%s"), (true, true));
        assert_eq!(get_format_type("%"), (false, false));
    }
}
