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
                (!character.is_numeric() && character != '.').then_some(index)
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
    }
}
