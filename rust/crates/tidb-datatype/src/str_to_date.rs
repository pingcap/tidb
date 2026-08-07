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
use regex::Regex;
use std::sync::LazyLock;

use crate::{CoreTime, Time, TimeError, TimeType};

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
pub(crate) struct ParsedTime {
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    microsecond: u32,
    hour12: bool,
    hour24: bool,
    meridiem_token: bool,
    meridiem: Option<bool>,
}

impl Time {
    /// Parses a MySQL `STR_TO_DATE` value.
    ///
    /// The boolean reports trailing source characters, matching TiDB's warning
    /// result without coupling the datatype crate to a statement context.
    pub fn str_to_date<TZ: TimeZone>(
        date: &str,
        format: &str,
        allow_invalid_date: bool,
        timezone: &TZ,
    ) -> Result<(Self, bool), TimeError> {
        let mut result = Self::new(CoreTime::default(), TimeType::DateTime, 0)?;
        let warning = result.str_to_date_into(date, format, allow_invalid_date, timezone)?;
        Ok((result, warning))
    }

    /// Applies Go `(*Time).StrToDate` receiver mutation semantics exactly.
    ///
    /// A tokenization/match failure resets the receiver to zero DATETIME/FSP
    /// zero. A meridiem-fix failure leaves it untouched. A final validation
    /// failure keeps the parsed DATETIME installed. Successful parsing keeps
    /// the receiver's pre-existing FSP, as Go does.
    pub fn str_to_date_into<TZ: TimeZone>(
        &mut self,
        date: &str,
        format: &str,
        allow_invalid_date: bool,
        timezone: &TZ,
    ) -> Result<bool, TimeError> {
        let mut parsed = ParsedTime::default();
        let warning = match parse_format(&mut parsed, date, format) {
            Ok(warning) => warning,
            Err(error) => {
                self.set_core_time(CoreTime::default());
                self.set_kind(TimeType::DateTime);
                self.set_fsp(0)?;
                return Err(error);
            }
        };
        fix_meridiem(&mut parsed)?;
        self.set_core_time(CoreTime::from_date(
            parsed.year as u16,
            parsed.month,
            parsed.day,
            parsed.hour,
            parsed.minute,
            parsed.second,
            parsed.microsecond,
        ));
        self.set_kind(TimeType::DateTime);
        self.validate(true, allow_invalid_date, timezone)?;
        Ok(warning)
    }
}

/// Classifies whether a MySQL date format contains time and date fields.
#[must_use]
pub fn get_format_type(mut format: &str) -> (bool, bool) {
    let mut is_duration = false;
    let mut is_date = false;
    loop {
        format = skip_whitespace(format);
        if format.is_empty() {
            break;
        }
        let Ok((token, remaining)) = next_token(format) else {
            return (false, false);
        };
        format = remaining;
        if let Some(conversion) = token
            .strip_prefix('%')
            .and_then(|value| value.chars().next())
        {
            match conversion {
                'h' | 'H' | 'i' | 'I' | 's' | 'S' | 'k' | 'l' | 'f' | 'r' | 'T' => {
                    is_duration = true;
                }
                'y' | 'Y' | 'm' | 'M' | 'c' | 'b' | 'D' | 'd' | 'e' => is_date = true,
                _ => {}
            }
        }
        if is_duration && is_date {
            break;
        }
    }
    (is_duration, is_date)
}

pub(crate) fn parse_format(
    parsed: &mut ParsedTime,
    mut date: &str,
    mut format: &str,
) -> Result<bool, TimeError> {
    loop {
        date = skip_whitespace(date);
        format = skip_whitespace(format);
        if format.is_empty() {
            return Ok(!date.is_empty());
        }
        let (token, remaining_format) = next_token(format)?;
        format = remaining_format;
        if date.is_empty() {
            match token {
                "%p" => parsed.meridiem_token = true,
                "%h" => parsed.hour12 = true,
                "%H" => parsed.hour24 = true,
                _ => {}
            }
            return Ok(false);
        }
        date = parse_token(parsed, date, token)?;
    }
}

pub(crate) fn next_token(format: &str) -> Result<(&str, &str), TimeError> {
    if let Some(remaining) = format.strip_prefix('%') {
        let Some(character) = remaining.chars().next() else {
            return Err(TimeError::InvalidDate);
        };
        let end = 1 + character.len_utf8();
        Ok((&format[..end], &format[end..]))
    } else {
        let character = format.chars().next().expect("nonempty format");
        let end = character.len_utf8();
        Ok((&format[..end], &format[end..]))
    }
}

pub(crate) fn parse_token<'a>(
    parsed: &mut ParsedTime,
    input: &'a str,
    token: &str,
) -> Result<&'a str, TimeError> {
    match token {
        "%b" => parse_month_name(parsed, input, true),
        "%M" => parse_month_name(parsed, input, false),
        "%c" | "%m" => {
            let (value, remaining) = parse_digits(input, 2)?;
            if value > 12 {
                return Err(TimeError::InvalidDate);
            }
            parsed.month = value as u8;
            Ok(remaining)
        }
        "%d" | "%e" => {
            let (value, remaining) = parse_digits(input, 2)?;
            if value > 31 {
                return Err(TimeError::InvalidDate);
            }
            parsed.day = value as u8;
            Ok(remaining)
        }
        "%f" => {
            let (value, digits, remaining) = parse_optional_digits(input, 6);
            parsed.microsecond = value * 10_u32.pow(6 - digits as u32);
            Ok(remaining)
        }
        "%h" | "%I" | "%l" => {
            let (value, remaining) = parse_digits(input, 2)?;
            if !(1..=12).contains(&value) {
                return Err(TimeError::InvalidClock);
            }
            parsed.hour = value as u8;
            parsed.hour12 = true;
            Ok(remaining)
        }
        "%H" | "%k" => {
            let (value, remaining) = parse_digits(input, 2)?;
            if value > 23 {
                return Err(TimeError::InvalidClock);
            }
            parsed.hour = value as u8;
            parsed.hour24 = true;
            Ok(remaining)
        }
        "%i" => {
            let (value, remaining) = parse_digits(input, 2)?;
            if value > 59 {
                return Err(TimeError::InvalidClock);
            }
            parsed.minute = value as u8;
            Ok(remaining)
        }
        "%s" | "%S" => {
            let (value, remaining) = parse_digits(input, 2)?;
            if value > 59 {
                return Err(TimeError::InvalidClock);
            }
            parsed.second = value as u8;
            Ok(remaining)
        }
        "%p" => {
            parsed.meridiem_token = true;
            if has_prefix(input, "AM") {
                parsed.meridiem = Some(false);
                Ok(&input[2..])
            } else if has_prefix(input, "PM") {
                parsed.meridiem = Some(true);
                Ok(&input[2..])
            } else {
                Err(TimeError::InvalidClock)
            }
        }
        "%r" => parse_compound_time(parsed, input, true),
        "%T" => parse_compound_time(parsed, input, false),
        "%Y" => parse_year(parsed, input, 4),
        "%y" => parse_year(parsed, input, 2),
        "%j" => {
            let (value, remaining) = parse_digits(input, 3)?;
            if value == 0 {
                Err(TimeError::InvalidDate)
            } else {
                Ok(remaining)
            }
        }
        "%#" => Ok(skip_while(input, is_go_number)),
        "%." => Ok(skip_while(input, is_go_punctuation)),
        "%@" => Ok(skip_while(input, is_go_letter)),
        _ if input.starts_with(token) => Ok(&input[token.len()..]),
        _ => Err(TimeError::InvalidDate),
    }
}

pub(crate) fn parse_month_name<'a>(
    parsed: &mut ParsedTime,
    input: &'a str,
    abbreviated: bool,
) -> Result<&'a str, TimeError> {
    for (index, name) in MONTH_NAMES.iter().enumerate() {
        let candidate = if abbreviated { &name[..3] } else { name };
        if has_prefix(input, candidate) {
            parsed.month = index as u8 + 1;
            return Ok(&input[candidate.len()..]);
        }
    }
    Err(TimeError::InvalidDate)
}

pub(crate) fn parse_year<'a>(
    parsed: &mut ParsedTime,
    input: &'a str,
    limit: usize,
) -> Result<&'a str, TimeError> {
    let (year, digits, remaining) = parse_digits_with_len(input, limit)?;
    parsed.year = if digits <= 2 { adjust_year(year) } else { year } as i32;
    Ok(remaining)
}

pub(crate) fn adjust_year(year: u32) -> u32 {
    match year {
        0..=69 => 2000 + year,
        70..=99 => 1900 + year,
        _ => year,
    }
}

pub(crate) fn parse_compound_time<'a>(
    parsed: &mut ParsedTime,
    input: &'a str,
    twelve_hour: bool,
) -> Result<&'a str, TimeError> {
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
    if has_prefix(remaining, "AM") {
        Ok(&remaining[2..])
    } else if has_prefix(remaining, "PM") {
        parsed.hour += 12;
        Ok(&remaining[2..])
    } else {
        Err(TimeError::InvalidClock)
    }
}

pub(crate) fn parse_separator(input: &str) -> Result<&str, TimeError> {
    let input = skip_whitespace(input);
    let Some(input) = input.strip_prefix(':') else {
        return Err(TimeError::InvalidClock);
    };
    Ok(skip_whitespace(input))
}

pub(crate) fn fix_meridiem(parsed: &mut ParsedTime) -> Result<(), TimeError> {
    if !parsed.meridiem_token {
        if parsed.hour12 && parsed.hour == 12 {
            parsed.hour = 0;
        }
        return Ok(());
    }
    if parsed.hour24 || parsed.hour == 0 {
        return Err(TimeError::InvalidClock);
    }
    let Some(pm) = parsed.meridiem else {
        return Ok(());
    };
    if parsed.hour == 12 {
        parsed.hour = if pm { 12 } else { 0 };
    } else if pm {
        parsed.hour += 12;
    }
    Ok(())
}

pub(crate) fn parse_digits(input: &str, limit: usize) -> Result<(u32, &str), TimeError> {
    let (value, _, remaining) = parse_digits_with_len(input, limit)?;
    Ok((value, remaining))
}

pub(crate) fn parse_digits_with_len(
    input: &str,
    limit: usize,
) -> Result<(u32, usize, &str), TimeError> {
    let (value, digits, remaining) = parse_optional_digits(input, limit);
    if digits == 0 {
        Err(TimeError::InvalidDate)
    } else {
        Ok((value, digits, remaining))
    }
}

pub(crate) fn parse_optional_digits(input: &str, limit: usize) -> (u32, usize, &str) {
    let digits = input
        .as_bytes()
        .iter()
        .take(limit)
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    let value = input[..digits].parse().unwrap_or(0);
    (value, digits, &input[digits..])
}

pub(crate) fn has_prefix(input: &str, prefix: &str) -> bool {
    input
        .get(..prefix.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
}

static GO_NUMBER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\p{N}$").expect("valid Unicode Number regex"));
static GO_PUNCTUATION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\p{P}$").expect("valid Unicode Punctuation regex"));
static GO_LETTER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\p{L}$").expect("valid Unicode Letter regex"));

fn matches_unicode_category(character: char, category: &Regex) -> bool {
    let mut buffer = [0; 4];
    category.is_match(character.encode_utf8(&mut buffer))
}

pub(crate) fn is_go_number(character: char) -> bool {
    matches_unicode_category(character, &GO_NUMBER)
}

pub(crate) fn is_go_punctuation(character: char) -> bool {
    matches_unicode_category(character, &GO_PUNCTUATION)
}

pub(crate) fn is_go_letter(character: char) -> bool {
    matches_unicode_category(character, &GO_LETTER)
}

pub(crate) fn skip_while(input: &str, predicate: impl Fn(char) -> bool) -> &str {
    let consumed = input
        .char_indices()
        .take_while(|(_, character)| predicate(*character))
        .map(|(index, character)| index + character.len_utf8())
        .last()
        .unwrap_or(0);
    &input[consumed..]
}

pub(crate) fn skip_whitespace(input: &str) -> &str {
    input.trim_start_matches(char::is_whitespace)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str, format: &str, allow_invalid: bool) -> Result<CoreTime, TimeError> {
        Time::str_to_date(input, format, allow_invalid, &chrono_tz::UTC)
            .map(|(time, _)| time.core_time())
    }

    #[test]
    fn test_str_to_date_source_rows() {
        for (input, format, expected) in [
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
        ] {
            assert_eq!(
                parse(input, format, true).unwrap(),
                expected,
                "{input} {format}"
            );
        }
    }

    #[test]
    fn test_str_to_date_source_errors() {
        for (input, format, allow_invalid) in [
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
        ] {
            assert!(
                parse(input, format, allow_invalid).is_err(),
                "{input} {format}"
            );
        }
    }

    #[test]
    fn test_get_format_type_source_rows() {
        assert_eq!(get_format_type("TEST"), (false, false));
        assert_eq!(get_format_type("%y %m %d 2019 04 01"), (false, true));
        assert_eq!(get_format_type("%h 30"), (true, false));
        assert_eq!(get_format_type("%Y-%m-%d %H:%i:%s"), (true, true));
        assert_eq!(get_format_type("%"), (false, false));
    }

    #[test]
    fn exhausted_input_still_tokenizes_and_records_fix_context() {
        assert!(parse("", "%", true).is_err());
        assert!(parse("", "%p", true).is_err());
        assert!(parse("PM", "%p%H", true).is_err());
        assert_eq!(parse("12", "%H%h", true).unwrap(), CoreTime::default());
        assert_eq!(
            parse("11", "%h%p", true).unwrap(),
            CoreTime::from_date(0, 0, 0, 11, 0, 0, 0)
        );
    }

    #[test]
    fn unicode_skip_tokens_use_go_number_punctuation_and_letter_categories() {
        assert_eq!(
            parse("—2010", "%.%Y", true).unwrap(),
            CoreTime::from_date(2010, 0, 0, 0, 0, 0, 0)
        );
        assert_eq!(
            parse("Ⅷ-2010", "%#-%Y", true).unwrap(),
            CoreTime::from_date(2010, 0, 0, 0, 0, 0, 0)
        );
        assert_eq!(
            parse("\u{345}2010", "%@\u{345}%Y", true).unwrap(),
            CoreTime::from_date(2010, 0, 0, 0, 0, 0, 0)
        );
    }

    #[test]
    fn str_to_date_receiver_mutation_matches_each_go_exit_path() {
        let original = Time::new(
            CoreTime::from_date(2001, 2, 3, 4, 5, 6, 123_456),
            TimeType::Timestamp,
            6,
        )
        .unwrap();

        let mut success = original;
        assert!(success
            .str_to_date_into("2010-05-06tail", "%Y-%m-%d", false, &chrono_tz::UTC)
            .unwrap());
        assert_eq!(success.kind(), TimeType::DateTime);
        assert_eq!(success.fsp(), 6);
        assert_eq!(
            success.core_time(),
            CoreTime::from_date(2010, 5, 6, 0, 0, 0, 0)
        );

        let mut parse_failure = original;
        assert!(parse_failure
            .str_to_date_into("x", "%Y", false, &chrono_tz::UTC)
            .is_err());
        assert_eq!(parse_failure.core_time(), CoreTime::default());
        assert_eq!(parse_failure.kind(), TimeType::DateTime);
        assert_eq!(parse_failure.fsp(), 0);

        let mut fix_failure = original;
        assert!(fix_failure
            .str_to_date_into("11 AM", "%H %p", false, &chrono_tz::UTC)
            .is_err());
        assert_eq!(fix_failure, original);

        let mut validation_failure = original;
        assert!(validation_failure
            .str_to_date_into("2021-02-29", "%Y-%m-%d", false, &chrono_tz::UTC)
            .is_err());
        assert_eq!(validation_failure.kind(), TimeType::DateTime);
        assert_eq!(validation_failure.fsp(), 6);
        assert_eq!(
            validation_failure.core_time(),
            CoreTime::from_date(2021, 2, 29, 0, 0, 0, 0)
        );
    }

    #[test]
    fn str_to_date_private_helper_boundaries_match_go() {
        assert_eq!(parse_optional_digits("123", 0), (0, 0, "123"));
        assert_eq!(parse_optional_digits("123", 2), (12, 2, "3"));

        for (input, format, expected) in [
            ("999", "%j", CoreTime::default()),
            ("x", "%fx", CoreTime::default()),
            ("1", "%f", CoreTime::from_date(0, 0, 0, 0, 0, 0, 100_000)),
            (
                "1234567",
                "%f7",
                CoreTime::from_date(0, 0, 0, 0, 0, 0, 123_456),
            ),
            ("12", "%m", CoreTime::from_date(0, 12, 0, 0, 0, 0, 0)),
            ("31", "%d", CoreTime::from_date(0, 0, 31, 0, 0, 0, 0)),
        ] {
            assert_eq!(
                parse(input, format, true).unwrap(),
                expected,
                "{input} {format}"
            );
        }
        for (input, format) in [
            ("0", "%j"),
            ("13", "%m"),
            ("32", "%d"),
            ("24", "%H"),
            ("13", "%h"),
            ("60", "%i"),
            ("60", "%s"),
            ("11-12", "%r"),
            ("Ja", "%b"),
            ("Nonesuch", "%M"),
        ] {
            assert!(parse(input, format, true).is_err(), "{input} {format}");
        }
    }
}
