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

//! Human duration syntax from `pkg/parser/duration`.

use std::fmt;

const MINUTE_NANOS: f64 = 60_000_000_000.0;
const HOUR_NANOS: f64 = 60.0 * MINUTE_NANOS;
const DAY_NANOS: f64 = 24.0 * HOUR_NANOS;

// Go 1.26's unicode.IsDigit table (Unicode 15.0.0). Rust's `is_numeric`
// additionally accepts Number_Letter and Number_Other characters, which would
// change where the source scanner stops and therefore change its error class.
const GO_UNICODE_DIGIT_RANGES: &[(u32, u32)] = &[
    (0x0030, 0x0039),
    (0x0660, 0x0669),
    (0x06f0, 0x06f9),
    (0x07c0, 0x07c9),
    (0x0966, 0x096f),
    (0x09e6, 0x09ef),
    (0x0a66, 0x0a6f),
    (0x0ae6, 0x0aef),
    (0x0b66, 0x0b6f),
    (0x0be6, 0x0bef),
    (0x0c66, 0x0c6f),
    (0x0ce6, 0x0cef),
    (0x0d66, 0x0d6f),
    (0x0de6, 0x0def),
    (0x0e50, 0x0e59),
    (0x0ed0, 0x0ed9),
    (0x0f20, 0x0f29),
    (0x1040, 0x1049),
    (0x1090, 0x1099),
    (0x17e0, 0x17e9),
    (0x1810, 0x1819),
    (0x1946, 0x194f),
    (0x19d0, 0x19d9),
    (0x1a80, 0x1a89),
    (0x1a90, 0x1a99),
    (0x1b50, 0x1b59),
    (0x1bb0, 0x1bb9),
    (0x1c40, 0x1c49),
    (0x1c50, 0x1c59),
    (0xa620, 0xa629),
    (0xa8d0, 0xa8d9),
    (0xa900, 0xa909),
    (0xa9d0, 0xa9d9),
    (0xa9f0, 0xa9f9),
    (0xaa50, 0xaa59),
    (0xabf0, 0xabf9),
    (0xff10, 0xff19),
    (0x104a0, 0x104a9),
    (0x10d30, 0x10d39),
    (0x11066, 0x1106f),
    (0x110f0, 0x110f9),
    (0x11136, 0x1113f),
    (0x111d0, 0x111d9),
    (0x112f0, 0x112f9),
    (0x11450, 0x11459),
    (0x114d0, 0x114d9),
    (0x11650, 0x11659),
    (0x116c0, 0x116c9),
    (0x11730, 0x11739),
    (0x118e0, 0x118e9),
    (0x11950, 0x11959),
    (0x11c50, 0x11c59),
    (0x11d50, 0x11d59),
    (0x11da0, 0x11da9),
    (0x11f50, 0x11f59),
    (0x16a60, 0x16a69),
    (0x16ac0, 0x16ac9),
    (0x16b50, 0x16b59),
    (0x1d7ce, 0x1d7ff),
    (0x1e140, 0x1e149),
    (0x1e2f0, 0x1e2f9),
    (0x1e4f0, 0x1e4f9),
    (0x1e950, 0x1e959),
    (0x1fbf0, 0x1fbf9),
];

fn is_go_unicode_digit(character: char) -> bool {
    let codepoint = character as u32;
    GO_UNICODE_DIGIT_RANGES
        .binary_search_by(|(start, end)| {
            if codepoint < *start {
                std::cmp::Ordering::Greater
            } else if codepoint > *end {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .is_ok()
}

/// A source human-duration parse failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigDurationError {
    /// The next component does not start with a decimal number.
    MissingNumber,
    /// `f64` rejected the source numeric spelling.
    InvalidNumber(String),
    /// A number is followed by an unsupported unit.
    UnknownUnit(char),
}

impl fmt::Display for ConfigDurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingNumber => formatter.write_str("fail to read an integer"),
            Self::InvalidNumber(number) => write!(formatter, "invalid duration number {number:?}"),
            Self::UnknownUnit(unit) => write!(formatter, "unknown unit {unit}"),
        }
    }
}

impl std::error::Error for ConfigDurationError {}

/// Parses concatenated fractional day, hour, and minute components.
///
/// The result is nanoseconds, matching Go's `time.Duration` representation.
pub fn parse_config_duration(mut source: &str) -> Result<i64, ConfigDurationError> {
    if source == "0" {
        return Ok(0);
    }
    let mut duration = 0_i64;
    while !source.is_empty() {
        let split = source
            .char_indices()
            .find_map(|(index, character)| {
                (!is_go_unicode_digit(character) && character != '.').then_some(index)
            })
            .ok_or(ConfigDurationError::MissingNumber)?;
        if split == 0 {
            return Err(ConfigDurationError::MissingNumber);
        }
        let number = &source[..split];
        let value = number
            .parse::<f64>()
            .map_err(|_| ConfigDurationError::InvalidNumber(number.to_owned()))?;
        let unit = source[split..]
            .chars()
            .next()
            .expect("split points at a unit");
        let nanos = match unit {
            'd' => value * DAY_NANOS,
            'h' => value * HOUR_NANOS,
            'm' => value * MINUTE_NANOS,
            other => return Err(ConfigDurationError::UnknownUnit(other)),
        } as i64;
        duration = duration.wrapping_add(nanos);
        source = &source[split + unit.len_utf8()..];
    }
    Ok(duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go: pkg/parser/duration/duration_test.go TestParseDuration.
    #[test]
    fn source_test_parse_duration() {
        for (source, expected) in [
            ("1h", HOUR_NANOS as i64),
            ("1h100m", (HOUR_NANOS + 100.0 * MINUTE_NANOS) as i64),
            ("1d10000m", (DAY_NANOS + 10_000.0 * MINUTE_NANOS) as i64),
            ("1d100h", (DAY_NANOS + 100.0 * HOUR_NANOS) as i64),
            ("1.5d", (1.5 * DAY_NANOS) as i64),
            ("1d1.5h", (DAY_NANOS + 1.5 * HOUR_NANOS) as i64),
            ("1d3.555h", (DAY_NANOS + 3.555 * HOUR_NANOS) as i64),
        ] {
            assert_eq!(parse_config_duration(source).unwrap(), expected, "{source}");
        }
    }

    #[test]
    fn source_errors_and_zero_are_preserved() {
        assert_eq!(parse_config_duration("0").unwrap(), 0);
        assert_eq!(
            parse_config_duration("1s").unwrap_err(),
            ConfigDurationError::UnknownUnit('s')
        );
        assert_eq!(
            parse_config_duration("h").unwrap_err(),
            ConfigDurationError::MissingNumber
        );
        assert_eq!(
            parse_config_duration("1..2h").unwrap_err(),
            ConfigDurationError::InvalidNumber("1..2".to_owned())
        );
        assert_eq!(
            parse_config_duration("٢h").unwrap_err(),
            ConfigDurationError::InvalidNumber("٢".to_owned())
        );
        assert_eq!(
            parse_config_duration("²h").unwrap_err(),
            ConfigDurationError::MissingNumber
        );
    }
}
