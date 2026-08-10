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

/// A human-duration parse failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigDurationError {
    /// The next component does not start with a decimal number.
    MissingNumber,
    /// The decimal spelling is invalid.
    InvalidNumber(String),
    /// The decimal magnitude is outside `f64`'s finite range.
    NumberOutOfRange(String),
    /// A number is followed by an unsupported unit.
    UnknownUnit(char),
}

impl fmt::Display for ConfigDurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingNumber => formatter.write_str("fail to read an integer"),
            Self::InvalidNumber(number) => {
                write!(
                    formatter,
                    "strconv.ParseFloat: parsing {number:?}: invalid syntax"
                )
            }
            Self::NumberOutOfRange(number) => write!(
                formatter,
                "strconv.ParseFloat: parsing {number:?}: value out of range"
            ),
            Self::UnknownUnit(unit) => write!(formatter, "unknown unit {unit}"),
        }
    }
}

impl std::error::Error for ConfigDurationError {}

/// Parses concatenated fractional day, hour, and minute components.
///
/// The result is signed nanoseconds, matching Go's `time.Duration` value.
pub fn parse_config_duration(mut source: &str) -> Result<i64, ConfigDurationError> {
    if source == "0" {
        return Ok(0);
    }

    let mut duration = 0_i64;
    while !source.is_empty() {
        // `strconv.ParseFloat` accepts ASCII decimal syntax. Scanning that
        // syntax directly avoids copying Go's Unicode tables merely to reject
        // non-ASCII digits one step later.
        let split = source
            .find(|character: char| !(character.is_ascii_digit() || character == '.'))
            .ok_or(ConfigDurationError::MissingNumber)?;
        if split == 0 {
            return Err(ConfigDurationError::MissingNumber);
        }

        let number = &source[..split];
        let value = number
            .parse::<f64>()
            .map_err(|_| ConfigDurationError::InvalidNumber(number.to_owned()))?;
        if !value.is_finite() {
            return Err(ConfigDurationError::NumberOutOfRange(number.to_owned()));
        }

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
