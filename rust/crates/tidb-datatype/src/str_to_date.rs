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
        let mut parsed = ParsedTime::default();
        let warning = parse_format(&mut parsed, date, format)?;
        fix_meridiem(&mut parsed)?;
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
        )?;
        result.validate(true, allow_invalid_date, timezone)?;
        Ok((result, warning))
    }
}

/// Classifies whether a MySQL date format contains time and date fields.
#[must_use]
pub fn get_format_type(mut format: &str) -> (bool, bool) {
    let mut is_duration = false;
    let mut is_date = false;
    loop {
        format = format.trim_start_matches(char::is_whitespace);
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

fn parse_format(
    parsed: &mut ParsedTime,
    mut date: &str,
    mut format: &str,
) -> Result<bool, TimeError> {
    loop {
        date = date.trim_start_matches(char::is_whitespace);
        format = format.trim_start_matches(char::is_whitespace);
        if format.is_empty() {
            return Ok(!date.is_empty());
        }
        if date.is_empty() {
            return Ok(false);
        }

        let (token, remaining_format) = next_token(format)?;
        format = remaining_format;
        date = parse_token(parsed, date, token)?;
    }
}

fn next_token(format: &str) -> Result<(&str, &str), TimeError> {
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

fn parse_token<'a>(
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
        "%#" => Ok(skip_while(input, char::is_numeric)),
        "%." => Ok(skip_while(input, |character| {
            character.is_ascii_punctuation()
        })),
        "%@" => Ok(skip_while(input, char::is_alphabetic)),
        _ if input.starts_with(token) => Ok(&input[token.len()..]),
        _ => Err(TimeError::InvalidDate),
    }
}

fn parse_month_name<'a>(
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

fn parse_year<'a>(
    parsed: &mut ParsedTime,
    input: &'a str,
    limit: usize,
) -> Result<&'a str, TimeError> {
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
    remaining = next.trim_start_matches(char::is_whitespace);
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

fn parse_separator(input: &str) -> Result<&str, TimeError> {
    let input = input.trim_start_matches(char::is_whitespace);
    let Some(input) = input.strip_prefix(':') else {
        return Err(TimeError::InvalidClock);
    };
    Ok(input.trim_start_matches(char::is_whitespace))
}

fn fix_meridiem(parsed: &mut ParsedTime) -> Result<(), TimeError> {
    let Some(pm) = parsed.meridiem else {
        if parsed.hour12 && parsed.hour == 12 {
            parsed.hour = 0;
        }
        return Ok(());
    };
    if parsed.hour24 || parsed.hour == 0 {
        return Err(TimeError::InvalidClock);
    }
    if parsed.hour == 12 {
        parsed.hour = if pm { 12 } else { 0 };
    } else if pm {
        parsed.hour += 12;
    }
    Ok(())
}

fn parse_digits(input: &str, limit: usize) -> Result<(u32, &str), TimeError> {
    let (value, _, remaining) = parse_digits_with_len(input, limit)?;
    Ok((value, remaining))
}

fn parse_digits_with_len(input: &str, limit: usize) -> Result<(u32, usize, &str), TimeError> {
    let (value, digits, remaining) = parse_optional_digits(input, limit);
    if digits == 0 {
        Err(TimeError::InvalidDate)
    } else {
        Ok((value, digits, remaining))
    }
}

fn parse_optional_digits(input: &str, limit: usize) -> (u32, usize, &str) {
    let digits = input
        .as_bytes()
        .iter()
        .take(limit)
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    let value = input[..digits].parse().unwrap_or(0);
    (value, digits, &input[digits..])
}

fn has_prefix(input: &str, prefix: &str) -> bool {
    input
        .get(..prefix.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
}

fn skip_while(input: &str, predicate: impl Fn(char) -> bool) -> &str {
    let consumed = input
        .char_indices()
        .take_while(|(_, character)| predicate(*character))
        .map(|(index, character)| index + character.len_utf8())
        .last()
        .unwrap_or(0);
    &input[consumed..]
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
}
