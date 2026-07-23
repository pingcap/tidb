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

//! Scalar conversion primitives transcreated from `pkg/types/convert.go`.

use std::fmt;

use crate::{
    parse_mysql_duration, parse_time_from_num, round_float, BinaryJSON, BinaryLiteral,
    ConversionFlags, Decimal, FieldTypeCode, MySqlDuration, MysqlEnum, MysqlSet, Time, TimeType,
    JSON_LITERAL_FALSE, JSON_LITERAL_NULL, JSON_TYPE_CODE_ARRAY, JSON_TYPE_CODE_DATE,
    JSON_TYPE_CODE_DATETIME, JSON_TYPE_CODE_DURATION, JSON_TYPE_CODE_FLOAT64, JSON_TYPE_CODE_INT64,
    JSON_TYPE_CODE_LITERAL, JSON_TYPE_CODE_OBJECT, JSON_TYPE_CODE_OPAQUE, JSON_TYPE_CODE_STRING,
    JSON_TYPE_CODE_TIMESTAMP, JSON_TYPE_CODE_UINT64,
};

/// Failure returned with the source-compatible saturated conversion result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarConversionError {
    /// The input is outside the target MySQL integer domain.
    Overflow {
        /// The value rendered for the source `ErrOverflow` argument.
        value: String,
        /// Target MySQL field type.
        target: FieldTypeCode,
    },
    /// The exponent following `e` or `E` is not a signed decimal integer.
    InvalidScientificExponent(String),
    /// The integral part is not an unsigned decimal integer.
    InvalidUnsignedInteger(String),
}

impl fmt::Display for ScalarConversionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow { value, target } => {
                write!(formatter, "{value} is out of range for {target:?}")
            }
            Self::InvalidScientificExponent(value) => {
                write!(formatter, "invalid scientific exponent in {value:?}")
            }
            Self::InvalidUnsignedInteger(value) => {
                write!(formatter, "invalid unsigned integer {value:?}")
            }
        }
    }
}

impl std::error::Error for ScalarConversionError {}

/// Non-fatal source event normally routed through `Context.HandleTruncate`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarConversionEvent {
    /// The accepted numeric prefix did not consume the input.
    Truncated,
    /// Conversion saturated at a target boundary.
    Overflow(ScalarConversionError),
}

/// A best-effort source conversion result and its warning/error event.
#[derive(Debug, Clone, PartialEq)]
pub struct Converted<T> {
    /// Value returned by Go beside the error.
    pub value: T,
    /// Event whose final error/warning policy belongs to the caller context.
    pub event: Option<ScalarConversionEvent>,
}

impl<T> Converted<T> {
    fn exact(value: T) -> Self {
        Self { value, event: None }
    }

    fn truncated(value: T) -> Self {
        Self {
            value,
            event: Some(ScalarConversionEvent::Truncated),
        }
    }
}

/// Result of `StrToDuration`, which deliberately accepts datetime syntax.
#[derive(Debug, Clone, PartialEq)]
pub enum DurationOrTime {
    /// Ordinary MySQL TIME input.
    Duration(MySqlDuration),
    /// Twelve-or-more-digit datetime input accepted by the source fallback.
    Time(Time),
}

fn overflow(value: impl ToString, target: FieldTypeCode) -> ScalarConversionError {
    ScalarConversionError::Overflow {
        value: value.to_string(),
        target,
    }
}

/// `IntegerUnsignedUpperBound`.
pub const fn integer_unsigned_upper_bound(target: FieldTypeCode) -> u64 {
    match target {
        FieldTypeCode::Tiny => u8::MAX as u64,
        FieldTypeCode::Short => u16::MAX as u64,
        FieldTypeCode::Int24 => 0x00ff_ffff,
        FieldTypeCode::Long => u32::MAX as u64,
        FieldTypeCode::LongLong | FieldTypeCode::Bit | FieldTypeCode::Set => u64::MAX,
        FieldTypeCode::Enum => u16::MAX as u64,
        _ => panic!("input is not a MySQL integer type"),
    }
}

/// `IntegerSignedUpperBound`.
pub const fn integer_signed_upper_bound(target: FieldTypeCode) -> i64 {
    match target {
        FieldTypeCode::Tiny => i8::MAX as i64,
        FieldTypeCode::Short => i16::MAX as i64,
        FieldTypeCode::Int24 => 0x007f_ffff,
        FieldTypeCode::Long => i32::MAX as i64,
        FieldTypeCode::LongLong => i64::MAX,
        FieldTypeCode::Enum => u16::MAX as i64,
        _ => panic!("input is not a MySQL signed integer type"),
    }
}

/// `IntegerSignedLowerBound`.
pub const fn integer_signed_lower_bound(target: FieldTypeCode) -> i64 {
    match target {
        FieldTypeCode::Tiny => i8::MIN as i64,
        FieldTypeCode::Short => i16::MIN as i64,
        FieldTypeCode::Int24 => -0x0080_0000,
        FieldTypeCode::Long => i32::MIN as i64,
        FieldTypeCode::LongLong => i64::MIN,
        FieldTypeCode::Enum => 0,
        _ => panic!("input is not a MySQL integer type"),
    }
}

/// `ConvertFloatToInt`, including MySQL half-away-from-zero rounding.
pub fn convert_float_to_int(
    value: f64,
    lower_bound: i64,
    upper_bound: i64,
    target: FieldTypeCode,
) -> Result<i64, (i64, ScalarConversionError)> {
    let rounded = round_float(value);
    if rounded < lower_bound as f64 {
        return Err((lower_bound, overflow(rounded, target)));
    }
    if rounded >= upper_bound as f64 {
        if rounded == upper_bound as f64 {
            return Ok(upper_bound);
        }
        return Err((upper_bound, overflow(rounded, target)));
    }
    Ok(rounded as i64)
}

/// `ConvertIntToInt`.
pub fn convert_int_to_int(
    value: i64,
    lower_bound: i64,
    upper_bound: i64,
    target: FieldTypeCode,
) -> Result<i64, (i64, ScalarConversionError)> {
    if value < lower_bound {
        Err((lower_bound, overflow(value, target)))
    } else if value > upper_bound {
        Err((upper_bound, overflow(value, target)))
    } else {
        Ok(value)
    }
}

/// `ConvertUintToInt`.
pub fn convert_uint_to_int(
    value: u64,
    upper_bound: i64,
    target: FieldTypeCode,
) -> Result<i64, (i64, ScalarConversionError)> {
    if value > upper_bound as u64 {
        Err((upper_bound, overflow(value, target)))
    } else {
        Ok(value as i64)
    }
}

/// `ConvertIntToUint`.
pub fn convert_int_to_uint(
    flags: ConversionFlags,
    value: i64,
    upper_bound: u64,
    target: FieldTypeCode,
) -> Result<u64, (u64, ScalarConversionError)> {
    if value < 0 && !flags.allow_negative_to_unsigned() {
        return Err((0, overflow(value, target)));
    }
    let converted = value as u64;
    if converted > upper_bound {
        Err((upper_bound, overflow(value, target)))
    } else {
        Ok(converted)
    }
}

/// `ConvertUintToUint`.
pub fn convert_uint_to_uint(
    value: u64,
    upper_bound: u64,
    target: FieldTypeCode,
) -> Result<u64, (u64, ScalarConversionError)> {
    if value > upper_bound {
        Err((upper_bound, overflow(value, target)))
    } else {
        Ok(value)
    }
}

/// `ConvertFloatToUint`.
pub fn convert_float_to_uint(
    flags: ConversionFlags,
    value: f64,
    upper_bound: u64,
    target: FieldTypeCode,
) -> Result<u64, (u64, ScalarConversionError)> {
    let rounded = round_float(value);
    if rounded < 0.0 {
        if !flags.allow_negative_to_unsigned() {
            return Err((0, overflow(rounded, target)));
        }
        let converted = (rounded as i64) as u64;
        return Err((converted, overflow(rounded, target)));
    }
    if !rounded.is_finite() || rounded >= (u64::MAX as f64) {
        return Err((upper_bound, overflow(rounded, target)));
    }
    let converted = rounded as u64;
    if converted > upper_bound {
        Err((upper_bound, overflow(rounded, target)))
    } else {
        Ok(converted)
    }
}

/// Expands the scientific notation accepted by `convertScientificNotation`.
pub fn convert_scientific_notation(input: &str) -> Result<String, ScalarConversionError> {
    let Some(exponent_index) = input.find(['e', 'E']) else {
        return Ok(input.to_owned());
    };
    let exponent = input[exponent_index + 1..]
        .parse::<i64>()
        .map_err(|_| ScalarConversionError::InvalidScientificExponent(input.to_owned()))?;
    let mantissa = &input[..exponent_index];
    if exponent == 0 {
        return Ok(mantissa.to_owned());
    }

    let point = mantissa.find('.').unwrap_or(mantissa.len());
    let mut digits = mantissa.to_owned();
    if point < digits.len() {
        digits.remove(point);
    }
    let new_point = point as i128 + exponent as i128;
    if new_point <= 0 {
        return Ok(format!("0.{}{}", "0".repeat((-new_point) as usize), digits));
    }
    if new_point >= digits.len() as i128 {
        digits.push_str(&"0".repeat((new_point - digits.len() as i128) as usize));
        return Ok(digits);
    }
    digits.insert(new_point as usize, '.');
    Ok(digits)
}

/// `convertDecimalStrToUint`, kept public because decimal conversion delegates
/// through this exact string path to avoid float precision loss.
pub fn convert_decimal_str_to_uint(
    input: &str,
    upper_bound: u64,
    target: FieldTypeCode,
) -> Result<u64, (u64, ScalarConversionError)> {
    let expanded = convert_scientific_notation(input).map_err(|error| (0, error))?;
    let (mut integer, fraction) = expanded
        .split_once('.')
        .map_or((expanded.as_str(), ""), |parts| parts);
    integer = integer.trim_start_matches('0');
    if integer.is_empty() {
        integer = "0";
    }
    if integer.starts_with('-') {
        return Err((0, overflow(&expanded, target)));
    }
    let round = u64::from(
        fraction
            .as_bytes()
            .first()
            .is_some_and(|digit| *digit >= b'5'),
    );
    let largest_integer = upper_bound - round;
    let upper_text = largest_integer.to_string();
    if integer.len() > upper_text.len()
        || (integer.len() == upper_text.len() && integer > upper_text.as_str())
    {
        return Err((upper_bound, overflow(&expanded, target)));
    }
    let value = integer.parse::<u64>().map_err(|_| {
        (
            0,
            ScalarConversionError::InvalidUnsignedInteger(integer.to_owned()),
        )
    })?;
    Ok(value + round)
}

/// `ConvertDecimalToUint`.
pub fn convert_decimal_to_uint(
    value: &Decimal,
    upper_bound: u64,
    target: FieldTypeCode,
) -> Result<u64, (u64, ScalarConversionError)> {
    convert_decimal_str_to_uint(&value.to_string(), upper_bound, target)
}

/// A source numeric prefix plus the truncation event that `Context` decides
/// whether to return, ignore, or publish as a warning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumericPrefix {
    value: String,
    truncated: bool,
}

impl NumericPrefix {
    /// Prefix accepted by Go's `strconv` call.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Whether source `Context.HandleTruncate` is invoked.
    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

/// `getValidFloatPrefix` without hiding its truncation event in an error.
pub fn valid_float_prefix(input: &str, is_function_cast: bool) -> NumericPrefix {
    if is_function_cast && input.is_empty() {
        return NumericPrefix {
            value: "0".to_owned(),
            truncated: false,
        };
    }

    let bytes = input.as_bytes();
    let mut saw_dot = false;
    let mut saw_digit = false;
    let mut valid_len = 0;
    let mut exponent_index: Option<usize> = None;
    let mut effective_len = bytes.len();
    for (index, byte) in bytes.iter().copied().enumerate() {
        match byte {
            b'+' | b'-'
                if index == 0 || exponent_index.is_some_and(|exponent| index == exponent + 1) => {}
            b'+' | b'-' => break,
            b'.' if saw_dot || exponent_index.is_some_and(|exponent| exponent > 0) => break,
            b'.' => {
                saw_dot = true;
                if saw_digit {
                    valid_len = index + 1;
                }
            }
            b'e' | b'E' if !saw_digit || exponent_index.is_some() => break,
            b'e' | b'E' => {
                exponent_index = Some(index);
                if index + 1 == bytes.len() {
                    return NumericPrefix {
                        value: input[..index].to_owned(),
                        truncated: false,
                    };
                }
            }
            0 => {
                effective_len = valid_len;
                break;
            }
            b'0'..=b'9' => {
                saw_digit = true;
                valid_len = index + 1;
            }
            _ => break,
        }
    }
    NumericPrefix {
        value: if valid_len == 0 {
            "0".to_owned()
        } else {
            input[..valid_len].to_owned()
        },
        truncated: valid_len == 0 || valid_len != effective_len,
    }
}

/// `roundIntStr`.
pub fn round_integer_string(next_fraction_digit: u8, integer: &str) -> String {
    if next_fraction_digit < b'5' {
        return integer.to_owned();
    }
    let mut result = integer.as_bytes().to_vec();
    let mut index = result.len() - 1;
    while index >= 1 {
        if result[index] != b'9' {
            result[index] += 1;
            return String::from_utf8(result).expect("integer input is ASCII");
        }
        result[index] = b'0';
        index -= 1;
    }
    match result[0] {
        b'9' => {
            result[0] = b'1';
            result.push(b'0');
        }
        b'0'..=b'8' => result[0] += 1,
        b'+' | b'-' => {
            result[1] = b'1';
            result.push(b'0');
        }
        _ => unreachable!("integer input is valid"),
    }
    String::from_utf8(result).expect("integer input is ASCII")
}

/// `floatStrToIntStr`. The error carries the same saturated BIGINT text used
/// by the Go caller for an exponent too large to materialize.
pub fn float_string_to_integer_string(
    valid_float: &str,
    original: &str,
) -> Result<String, (String, ScalarConversionError)> {
    let bytes = valid_float.as_bytes();
    let dot_index = bytes.iter().position(|byte| *byte == b'.');
    let exponent_index = bytes.iter().position(|byte| matches!(byte, b'e' | b'E'));

    let Some(exponent_index) = exponent_index else {
        let Some(mut dot_index) = dot_index else {
            return Ok(valid_float.to_owned());
        };
        let signed = matches!(bytes.first(), Some(b'+' | b'-'));
        let digits = if signed {
            dot_index -= 1;
            &bytes[1..]
        } else {
            bytes
        };
        let mut integer = if dot_index == 0 {
            "0".to_owned()
        } else {
            String::from_utf8(digits[..dot_index].to_vec()).expect("numeric input is ASCII")
        };
        if digits.len() > dot_index + 1 {
            integer = round_integer_string(digits[dot_index + 1], &integer);
        }
        if (integer.len() > 1 || integer.as_bytes()[0] != b'0') && bytes.first() == Some(&b'-') {
            integer.insert(0, '-');
        }
        return Ok(integer);
    };

    let mut digits = Vec::with_capacity(valid_float.len());
    let mut integer_count;
    if let Some(dot_index) = dot_index {
        digits.extend_from_slice(&bytes[..dot_index]);
        integer_count = digits.len() as i128;
        digits.extend_from_slice(&bytes[dot_index + 1..exponent_index]);
    } else {
        digits.extend_from_slice(&bytes[..exponent_index]);
        integer_count = digits.len() as i128;
    }
    let exponent = valid_float[exponent_index + 1..]
        .parse::<i128>()
        .map_err(|_| {
            let saturated = if digits.first() == Some(&b'-') {
                i64::MIN.to_string()
            } else {
                u64::MAX.to_string()
            };
            (saturated, overflow(original, FieldTypeCode::LongLong))
        })?;
    integer_count += exponent;
    if exponent >= 0 && !(0..=21).contains(&integer_count) {
        let saturated = if digits.first() == Some(&b'-') {
            i64::MIN.to_string()
        } else {
            u64::MAX.to_string()
        };
        return Err((saturated, overflow(original, FieldTypeCode::LongLong)));
    }
    if integer_count <= 0 {
        let mut integer = "0".to_owned();
        if integer_count == 0 && digits.first().is_some_and(u8::is_ascii_digit) {
            integer = round_integer_string(digits[0], &integer);
        }
        return Ok(integer);
    }
    if integer_count == 1 && matches!(digits.first(), Some(b'+' | b'-')) {
        let mut integer = "0".to_owned();
        if digits.len() > 1 {
            integer = round_integer_string(digits[1], &integer);
        }
        if integer.starts_with('1') {
            integer.insert(0, digits[0] as char);
        }
        return Ok(integer);
    }
    if integer_count <= digits.len() as i128 {
        let count = integer_count as usize;
        let mut integer =
            String::from_utf8(digits[..count].to_vec()).expect("numeric input is ASCII");
        if count < digits.len() {
            integer = round_integer_string(digits[count], &integer);
        }
        Ok(integer)
    } else {
        let mut integer = String::from_utf8(digits).expect("numeric input is ASCII");
        integer.push_str(&"0".repeat(integer_count as usize - integer.len()));
        Ok(integer)
    }
}

/// `getValidIntPrefix`.
pub fn valid_integer_prefix(
    input: &str,
    is_function_cast: bool,
) -> Result<NumericPrefix, (NumericPrefix, ScalarConversionError)> {
    if !is_function_cast {
        let float = valid_float_prefix(input, false);
        if float.truncated {
            return Err((
                float,
                ScalarConversionError::InvalidUnsignedInteger(input.to_owned()),
            ));
        }
        return float_string_to_integer_string(float.value(), input)
            .map(|value| NumericPrefix {
                value,
                truncated: false,
            })
            .map_err(|(value, error)| {
                (
                    NumericPrefix {
                        value,
                        truncated: false,
                    },
                    error,
                )
            });
    }

    let mut valid_len = 0;
    for (index, byte) in input.bytes().enumerate() {
        if matches!(byte, b'+' | b'-') && index == 0 {
            continue;
        }
        if byte.is_ascii_digit() {
            valid_len = index + 1;
            continue;
        }
        break;
    }
    let prefix = NumericPrefix {
        value: if valid_len == 0 {
            "0".to_owned()
        } else {
            input[..valid_len].to_owned()
        },
        truncated: valid_len == 0 || valid_len != input.len(),
    };
    if prefix.truncated {
        Err((
            prefix,
            ScalarConversionError::InvalidUnsignedInteger(input.to_owned()),
        ))
    } else {
        Ok(prefix)
    }
}

/// `StrToInt`, preserving the best-effort value and truncation/overflow event.
pub fn str_to_int(input: &str, is_function_cast: bool) -> Converted<i64> {
    let input = input.trim();
    let float = valid_float_prefix(input, is_function_cast);
    let integer = if is_function_cast {
        let valid = input
            .bytes()
            .enumerate()
            .take_while(|(index, byte)| {
                byte.is_ascii_digit() || (*index == 0 && matches!(*byte, b'+' | b'-'))
            })
            .map(|(_, byte)| byte as char)
            .collect::<String>();
        if valid.is_empty() {
            "0".to_owned()
        } else {
            valid
        }
    } else {
        match float_string_to_integer_string(float.value(), input) {
            Ok(value) => value,
            Err((value, error)) => {
                return Converted {
                    value: value.parse().unwrap_or_else(|_| {
                        if value.starts_with('-') {
                            i64::MIN
                        } else {
                            i64::MAX
                        }
                    }),
                    event: Some(ScalarConversionEvent::Overflow(error)),
                };
            }
        }
    };
    match integer.parse::<i64>() {
        Ok(value) if float.truncated() || (is_function_cast && integer.len() != input.len()) => {
            Converted::truncated(value)
        }
        Ok(value) => Converted::exact(value),
        Err(_) => {
            let value = if integer.starts_with('-') {
                i64::MIN
            } else {
                i64::MAX
            };
            Converted {
                value,
                event: Some(ScalarConversionEvent::Overflow(overflow(
                    &integer,
                    FieldTypeCode::LongLong,
                ))),
            }
        }
    }
}

/// `StrToUint`, including the source rule that only negative zero is valid.
pub fn str_to_uint(input: &str, is_function_cast: bool) -> Converted<u64> {
    let input = input.trim();
    let float = valid_float_prefix(input, is_function_cast);
    let integer = if is_function_cast {
        let valid = input
            .bytes()
            .enumerate()
            .take_while(|(index, byte)| {
                byte.is_ascii_digit() || (*index == 0 && matches!(*byte, b'+' | b'-'))
            })
            .map(|(_, byte)| byte as char)
            .collect::<String>();
        if valid.is_empty() {
            "0".to_owned()
        } else {
            valid
        }
    } else {
        match float_string_to_integer_string(float.value(), input) {
            Ok(value) => value,
            Err((value, error)) => {
                return Converted {
                    value: value.parse().unwrap_or(u64::MAX),
                    event: Some(ScalarConversionEvent::Overflow(error)),
                };
            }
        }
    };
    let unsigned = integer.strip_prefix('+').unwrap_or(&integer);
    if let Some(magnitude) = unsigned.strip_prefix('-') {
        if magnitude.bytes().any(|byte| byte != b'0') {
            return Converted {
                value: 0,
                event: Some(ScalarConversionEvent::Overflow(overflow(
                    &integer,
                    FieldTypeCode::LongLong,
                ))),
            };
        }
        return if float.truncated() {
            Converted::truncated(0)
        } else {
            Converted::exact(0)
        };
    }
    match unsigned.parse::<u64>() {
        Ok(value) if float.truncated() || (is_function_cast && integer.len() != input.len()) => {
            Converted::truncated(value)
        }
        Ok(value) => Converted::exact(value),
        Err(_) => Converted {
            value: u64::MAX,
            event: Some(ScalarConversionEvent::Overflow(overflow(
                &integer,
                FieldTypeCode::LongLong,
            ))),
        },
    }
}

/// `StrToFloat`.
pub fn str_to_float(input: &str, is_function_cast: bool) -> Converted<f64> {
    let input = input.trim();
    let prefix = valid_float_prefix(input, is_function_cast);
    match prefix.value().parse::<f64>() {
        Ok(value) if value.is_infinite() => Converted::truncated(if value.is_sign_positive() {
            f64::MAX
        } else {
            -f64::MAX
        }),
        Ok(value) if prefix.truncated() => Converted::truncated(value),
        Ok(value) => Converted::exact(value),
        Err(_) => Converted::truncated(0.0),
    }
}

/// `StrToDateTime`.
pub fn str_to_datetime<TZ: chrono::TimeZone>(
    input: &str,
    fsp: i64,
    timezone: &TZ,
) -> Result<Converted<Time>, crate::TimeError> {
    crate::parse_time(input, TimeType::DateTime, fsp, false, true, false, timezone).map(|parsed| {
        if parsed.truncated {
            Converted::truncated(parsed.time)
        } else {
            Converted::exact(parsed.time)
        }
    })
}

/// `StrToDuration`.
pub fn str_to_duration<TZ: chrono::TimeZone>(
    input: &str,
    fsp: i64,
    timezone: &TZ,
) -> Result<Converted<DurationOrTime>, crate::DurationValueError> {
    let input = input.trim();
    let unsigned = input.strip_prefix('-').unwrap_or(input);
    let integer_length = unsigned.find('.').unwrap_or(unsigned.len());
    if integer_length >= 12 {
        if let Ok(parsed) = str_to_datetime(input, fsp, timezone) {
            return Ok(Converted {
                value: DurationOrTime::Time(parsed.value),
                event: parsed.event,
            });
        }
    }
    let parsed = parse_mysql_duration(input, fsp, timezone, true, false)?;
    let duration = MySqlDuration::from_nanoseconds(parsed.nanoseconds(), parsed.fsp())
        .map_err(crate::DurationParseError::InvalidFsp)
        .map_err(crate::DurationValueError::Duration)?;
    Ok(Converted {
        value: DurationOrTime::Duration(duration),
        event: parsed.event().and_then(|event| match event {
            crate::DurationParseEvent::Truncated => Some(ScalarConversionEvent::Truncated),
            crate::DurationParseEvent::Overflow(_) => Some(ScalarConversionEvent::Overflow(
                overflow(input, FieldTypeCode::Duration),
            )),
            crate::DurationParseEvent::DateTimeFallback(_) => None,
        }),
    })
}

/// `NumberToDuration`.
pub fn number_to_duration(
    mut number: i64,
    fsp: i64,
) -> Result<Converted<MySqlDuration>, crate::TimeError> {
    const TIME_MAX_VALUE: i64 = 8_385_959;
    if number.abs() > TIME_MAX_VALUE {
        if number >= 10_000_000_000 {
            if let Ok(parsed) = parse_time_from_num(
                number,
                TimeType::DateTime,
                fsp,
                true,
                false,
                &chrono_tz::UTC,
            ) {
                return parsed.time.to_duration().map(Converted::exact);
            }
        }
        let mut duration = MySqlDuration::maximum(fsp).map_err(crate::TimeError::InvalidFsp)?;
        if number < 0 {
            duration = MySqlDuration::from_nanoseconds(-duration.nanoseconds(), fsp)
                .map_err(crate::TimeError::InvalidFsp)?;
        }
        return Ok(Converted {
            value: duration,
            event: Some(ScalarConversionEvent::Overflow(overflow(
                number,
                FieldTypeCode::Duration,
            ))),
        });
    }
    let negative = number < 0;
    number = number.abs();
    let hour = number / 10_000;
    let minute = (number / 100) % 100;
    let second = number % 100;
    if hour > 838 || minute >= 60 || second >= 60 {
        return Ok(Converted::truncated(
            MySqlDuration::from_nanoseconds(0, fsp).map_err(crate::TimeError::InvalidFsp)?,
        ));
    }
    let sign = if negative { -1 } else { 1 };
    let nanoseconds = sign * (hour * 3_600 + minute * 60 + second) * 1_000_000_000;
    Ok(Converted::exact(
        MySqlDuration::from_nanoseconds(nanoseconds, fsp).map_err(crate::TimeError::InvalidFsp)?,
    ))
}

fn converted_result<T>(result: Result<T, (T, ScalarConversionError)>) -> Converted<T> {
    match result {
        Ok(value) => Converted::exact(value),
        Err((value, error)) => Converted {
            value,
            event: Some(ScalarConversionEvent::Overflow(error)),
        },
    }
}

fn json_non_numeric(type_code: u8) -> bool {
    matches!(
        type_code,
        JSON_TYPE_CODE_OBJECT
            | JSON_TYPE_CODE_ARRAY
            | JSON_TYPE_CODE_OPAQUE
            | JSON_TYPE_CODE_DATE
            | JSON_TYPE_CODE_DATETIME
            | JSON_TYPE_CODE_TIMESTAMP
            | JSON_TYPE_CODE_DURATION
    )
}

/// `ConvertJSONToInt`.
pub fn json_to_int(
    json: &BinaryJSON,
    unsigned: bool,
    target: FieldTypeCode,
    flags: ConversionFlags,
) -> Converted<i64> {
    if json_non_numeric(json.type_code()) {
        return Converted::truncated(0);
    }
    match json.type_code() {
        JSON_TYPE_CODE_LITERAL => match json.value().first().copied() {
            Some(JSON_LITERAL_FALSE) => Converted::exact(0),
            Some(JSON_LITERAL_NULL) | None => Converted::truncated(0),
            Some(_) => Converted::exact(1),
        },
        JSON_TYPE_CODE_INT64 => {
            let value = json.as_i64().expect("validated binary JSON integer");
            if unsigned {
                let converted = converted_result(convert_int_to_uint(
                    flags,
                    value,
                    integer_unsigned_upper_bound(target),
                    target,
                ));
                Converted {
                    value: converted.value as i64,
                    event: converted.event,
                }
            } else {
                converted_result(convert_int_to_int(
                    value,
                    integer_signed_lower_bound(target),
                    integer_signed_upper_bound(target),
                    target,
                ))
            }
        }
        JSON_TYPE_CODE_UINT64 => {
            let value = json.as_u64().expect("validated binary JSON integer");
            if unsigned {
                let converted = converted_result(convert_uint_to_uint(
                    value,
                    integer_unsigned_upper_bound(target),
                    target,
                ));
                Converted {
                    value: converted.value as i64,
                    event: converted.event,
                }
            } else {
                converted_result(convert_uint_to_int(
                    value,
                    integer_signed_upper_bound(target),
                    target,
                ))
            }
        }
        JSON_TYPE_CODE_FLOAT64 => {
            let value = json.as_f64().expect("validated binary JSON float");
            if unsigned {
                let converted = converted_result(convert_float_to_uint(
                    flags,
                    value,
                    integer_unsigned_upper_bound(target),
                    target,
                ));
                Converted {
                    value: converted.value as i64,
                    event: converted.event,
                }
            } else {
                converted_result(convert_float_to_int(
                    value,
                    integer_signed_lower_bound(target),
                    integer_signed_upper_bound(target),
                    target,
                ))
            }
        }
        JSON_TYPE_CODE_STRING => {
            let text = std::str::from_utf8(json.as_string().expect("validated binary JSON string"))
                .unwrap_or("");
            if text.len() > 1 && text.starts_with('-') {
                str_to_int(text, false)
            } else {
                let converted = str_to_uint(text, false);
                Converted {
                    value: converted.value as i64,
                    event: converted.event,
                }
            }
        }
        _ => Converted::truncated(0),
    }
}

/// `ConvertJSONToInt64`.
pub fn json_to_int64(json: &BinaryJSON, unsigned: bool, flags: ConversionFlags) -> Converted<i64> {
    json_to_int(json, unsigned, FieldTypeCode::LongLong, flags)
}

/// `ConvertJSONToFloat`.
pub fn json_to_float(json: &BinaryJSON) -> Converted<f64> {
    if json_non_numeric(json.type_code()) {
        return Converted::truncated(0.0);
    }
    match json.type_code() {
        JSON_TYPE_CODE_LITERAL => match json.value().first().copied() {
            Some(JSON_LITERAL_FALSE) => Converted::exact(0.0),
            Some(JSON_LITERAL_NULL) | None => Converted::truncated(0.0),
            Some(_) => Converted::exact(1.0),
        },
        JSON_TYPE_CODE_INT64 => {
            Converted::exact(json.as_i64().expect("validated binary JSON integer") as f64)
        }
        JSON_TYPE_CODE_UINT64 => {
            Converted::exact(json.as_u64().expect("validated binary JSON integer") as f64)
        }
        JSON_TYPE_CODE_FLOAT64 => {
            Converted::exact(json.as_f64().expect("validated binary JSON float"))
        }
        JSON_TYPE_CODE_STRING => {
            let text = std::str::from_utf8(json.as_string().expect("validated binary JSON string"))
                .unwrap_or("");
            str_to_float(text, false)
        }
        _ => Converted::truncated(0.0),
    }
}

pub(crate) fn decimal_text(text: &str) -> Option<Decimal> {
    let expanded = convert_scientific_notation(text).ok()?;
    let magnitude = expanded.strip_prefix(['+', '-']).unwrap_or(&expanded);
    if magnitude.is_empty()
        || magnitude.matches('.').count() > 1
        || magnitude
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && byte != b'.')
    {
        return None;
    }
    Some(Decimal::from_signed_literal(&expanded))
}

/// `ConvertJSONToDecimal`.
pub fn json_to_decimal(json: &BinaryJSON) -> Converted<Decimal> {
    if json_non_numeric(json.type_code()) {
        return Converted::truncated(Decimal::from_int(0));
    }
    match json.type_code() {
        JSON_TYPE_CODE_LITERAL => match json.value().first().copied() {
            Some(JSON_LITERAL_FALSE) => Converted::exact(Decimal::from_int(0)),
            Some(JSON_LITERAL_NULL) | None => Converted::truncated(Decimal::from_int(0)),
            Some(_) => Converted::exact(Decimal::from_int(1)),
        },
        JSON_TYPE_CODE_INT64 => Converted::exact(Decimal::from_int(
            json.as_i64().expect("validated binary JSON integer"),
        )),
        JSON_TYPE_CODE_UINT64 => Converted::exact(Decimal::from_uint(
            json.as_u64().expect("validated binary JSON integer"),
        )),
        JSON_TYPE_CODE_FLOAT64 => {
            let value = json.as_f64().expect("validated binary JSON float");
            decimal_text(&value.to_string()).map_or_else(
                || Converted::truncated(Decimal::from_int(0)),
                Converted::exact,
            )
        }
        JSON_TYPE_CODE_STRING => {
            let text = std::str::from_utf8(json.as_string().expect("validated binary JSON string"))
                .unwrap_or("");
            decimal_text(text).map_or_else(
                || Converted::truncated(Decimal::from_int(0)),
                Converted::exact,
            )
        }
        _ => Converted::truncated(Decimal::from_int(0)),
    }
}

/// Typed replacement for Go `ToString(any)`.
pub enum ScalarStringValue<'a> {
    /// Boolean renders as `1` or `0`.
    Bool(bool),
    /// Signed integer.
    Int(i64),
    /// Unsigned integer.
    Uint(u64),
    /// Source float32 formatting.
    Float32(f32),
    /// Source float64 formatting.
    Float64(f64),
    /// UTF-8 string.
    String(&'a str),
    /// Raw byte string.
    Bytes(&'a [u8]),
    /// MySQL temporal.
    Time(Time),
    /// MySQL duration.
    Duration(MySqlDuration),
    /// Exact decimal.
    Decimal(&'a Decimal),
    /// Binary literal.
    BinaryLiteral(&'a BinaryLiteral),
    /// MySQL enum.
    Enum(&'a MysqlEnum),
    /// MySQL set.
    Set(&'a MysqlSet),
    /// Binary JSON.
    Json(&'a BinaryJSON),
}

/// `ToString`.
pub fn scalar_to_string(value: ScalarStringValue<'_>) -> Result<String, std::str::Utf8Error> {
    match value {
        ScalarStringValue::Bool(value) => Ok(if value { "1" } else { "0" }.to_owned()),
        ScalarStringValue::Int(value) => Ok(value.to_string()),
        ScalarStringValue::Uint(value) => Ok(value.to_string()),
        ScalarStringValue::Float32(value) => Ok(value.to_string()),
        ScalarStringValue::Float64(value) => Ok(value.to_string()),
        ScalarStringValue::String(value) => Ok(value.to_owned()),
        ScalarStringValue::Bytes(value) => std::str::from_utf8(value).map(str::to_owned),
        ScalarStringValue::Time(value) => Ok(value.to_string()),
        ScalarStringValue::Duration(value) => Ok(value.to_string()),
        ScalarStringValue::Decimal(value) => Ok(value.to_string()),
        ScalarStringValue::BinaryLiteral(value) => {
            std::str::from_utf8(value.as_bytes()).map(str::to_owned)
        }
        ScalarStringValue::Enum(value) => Ok(value.name().to_owned()),
        ScalarStringValue::Set(value) => Ok(value.name().to_owned()),
        ScalarStringValue::Json(value) => Ok(value.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_integer_bounds_and_conversions() {
        assert_eq!(integer_unsigned_upper_bound(FieldTypeCode::Tiny), 255);
        assert_eq!(
            integer_unsigned_upper_bound(FieldTypeCode::Int24),
            16_777_215
        );
        assert_eq!(integer_signed_lower_bound(FieldTypeCode::Int24), -8_388_608);
        assert_eq!(integer_signed_upper_bound(FieldTypeCode::Int24), 8_388_607);

        assert_eq!(
            convert_float_to_int(1.5, i8::MIN.into(), i8::MAX.into(), FieldTypeCode::Tiny),
            Ok(2)
        );
        assert_eq!(
            convert_float_to_int(-1.5, i8::MIN.into(), i8::MAX.into(), FieldTypeCode::Tiny),
            Ok(-2)
        );
        assert_eq!(
            convert_int_to_int(256, 0, 255, FieldTypeCode::Tiny)
                .unwrap_err()
                .0,
            255
        );
        assert_eq!(
            convert_uint_to_int(u64::MAX, i64::MAX, FieldTypeCode::LongLong)
                .unwrap_err()
                .0,
            i64::MAX
        );
        assert_eq!(
            convert_int_to_uint(
                ConversionFlags::from_bits(0),
                -1,
                u64::MAX,
                FieldTypeCode::LongLong
            )
            .unwrap_err()
            .0,
            0
        );
    }

    #[test]
    fn source_scientific_notation_rows() {
        for (input, expected) in [
            ("123.456e0", "123.456"),
            ("123.456e1", "1234.56"),
            ("123.456e3", "123456"),
            ("123.456e4", "1234560"),
            ("123.456e5", "12345600"),
            ("123.456e6", "123456000"),
            ("123.456e7", "1234560000"),
            ("123.456e-1", "12.3456"),
            ("123.456e-2", "1.23456"),
            ("123.456e-3", "0.123456"),
            ("123.456e-4", "0.0123456"),
            ("123.456e-5", "0.00123456"),
            ("123.456e-6", "0.000123456"),
            ("123.456e-7", "0.0000123456"),
            (".12345E+5", "12345"),
            ("1E6", "1000000"),
        ] {
            assert_eq!(
                convert_scientific_notation(input).unwrap(),
                expected,
                "{input}"
            );
        }
        for input in ["123.456e-", "123.456e-7.5", "123.456e"] {
            assert!(convert_scientific_notation(input).is_err(), "{input}");
        }
    }

    #[test]
    fn source_decimal_string_to_uint_rows() {
        for (input, expected) in [
            ("0.", 0),
            ("72.40", 72),
            ("072.40", 72),
            ("123.456e2", 12_346),
            ("123.456e-2", 1),
            ("072.50000000001", 73),
            (".5757", 1),
            (".12345E+4", 1_235),
            ("9223372036854775807.5", 9_223_372_036_854_775_808),
            ("9223372036854775807.4999", 9_223_372_036_854_775_807),
            ("18446744073709551614.55", u64::MAX),
            ("18446744073709551615.344", u64::MAX),
        ] {
            assert_eq!(
                convert_decimal_str_to_uint(input, u64::MAX, FieldTypeCode::LongLong).unwrap(),
                expected,
                "{input}"
            );
        }
        for input in [
            "18446744073709551615.544",
            "-111.111",
            "-10000000000000000000.0",
        ] {
            assert!(
                convert_decimal_str_to_uint(input, u64::MAX, FieldTypeCode::LongLong).is_err(),
                "{input}"
            );
        }
    }

    #[test]
    fn source_valid_float_prefix_rows() {
        for (input, expected, cast, truncated) in [
            ("-100", "-100", false, false),
            ("1abc", "1", false, true),
            ("-1-1", "-1", false, true),
            ("+1+1", "+1", false, true),
            ("123..34", "123.", false, true),
            ("123.23E-10", "123.23E-10", false, false),
            ("1.1e1.3", "1.1e1", false, true),
            ("11e1.3", "11e1", false, true),
            ("1.1e-13a", "1.1e-13", false, true),
            ("1.", "1.", false, false),
            (".1", ".1", false, false),
            ("", "0", false, true),
            ("", "0", true, false),
            ("123e+", "123", false, true),
            ("0-123", "0", false, true),
            ("9-3", "9", false, true),
            ("1001001\0\0\0", "1001001", false, false),
            ("5e", "5", false, false),
            ("+.e", "0", false, true),
            ("1e5e", "1e5", false, true),
            ("e", "0", false, true),
            ("e123", "0", false, true),
            ("e+", "0", false, true),
        ] {
            let actual = valid_float_prefix(input, cast);
            assert_eq!(actual.value(), expected, "{input:?}");
            assert_eq!(actual.truncated(), truncated, "{input:?}");
        }
    }

    #[test]
    fn source_float_string_to_integer_rows() {
        for (input, expected) in [
            ("1e5", "100000"),
            ("-123.45678e5", "-12345678"),
            ("+0.5", "1"),
            ("-0.5", "-1"),
            (".5e0", "1"),
            ("+.5e0", "+1"),
            ("-.5e0", "-1"),
            (".5", "1"),
            ("123.456789e5", "12345679"),
            ("123.456784e5", "12345678"),
            ("+999.9999e2", "+100000"),
        ] {
            assert_eq!(
                float_string_to_integer_string(input, input).unwrap(),
                expected,
                "{input}"
            );
        }
        for (input, expected) in [
            ("1e29223372036854775807", u64::MAX.to_string()),
            ("1e9223372036854775807", u64::MAX.to_string()),
            ("125e342", u64::MAX.to_string()),
            ("1e21", u64::MAX.to_string()),
            ("-1e29223372036854775807", i64::MIN.to_string()),
            ("-1e9223372036854775807", i64::MIN.to_string()),
        ] {
            assert_eq!(
                float_string_to_integer_string(input, input).unwrap_err().0,
                expected,
                "{input}"
            );
        }
    }

    #[test]
    fn source_string_to_number_rows() {
        for (input, expected, truncated) in [
            ("0", 0, false),
            ("-1", -1, false),
            ("100", 100, false),
            ("65.0", 65, false),
            ("", 0, true),
            ("xx", 0, true),
            ("11xx", 11, true),
            ("xx11", 0, true),
        ] {
            let actual = str_to_int(input, false);
            assert_eq!(actual.value, expected, "{input:?}");
            assert_eq!(
                actual.event == Some(ScalarConversionEvent::Truncated),
                truncated,
                "{input:?}"
            );
        }

        for (input, expected, truncated) in [
            ("0", 0, false),
            ("", 0, true),
            ("100", 100, false),
            ("+100", 100, false),
            ("65.0", 65, false),
            ("xx", 0, true),
            ("11xx", 11, true),
            ("xx11", 0, true),
            ("-00", 0, false),
        ] {
            let actual = str_to_uint(input, false);
            assert_eq!(actual.value, expected, "{input:?}");
            assert_eq!(
                actual.event == Some(ScalarConversionEvent::Truncated),
                truncated,
                "{input:?}"
            );
        }

        for (input, expected, truncated) in [
            ("", 0.0, true),
            ("-1", -1.0, false),
            ("1.11", 1.11, false),
            ("1.11.00", 1.11, true),
            ("xx", 0.0, true),
            ("0x00", 0.0, true),
            ("11.xx", 11.0, true),
            ("xx.11", 0.0, true),
            ("1e649", f64::MAX, true),
            ("-1e649", -f64::MAX, true),
        ] {
            let actual = str_to_float(input, false);
            assert_eq!(actual.value, expected, "{input:?}");
            assert_eq!(
                actual.event == Some(ScalarConversionEvent::Truncated),
                truncated,
                "{input:?}"
            );
        }
    }

    #[test]
    fn source_number_and_string_to_duration_rows() {
        for (number, expected, event) in [
            (171_222, "17:12:22", false),
            (-171_222, "-17:12:22", false),
            (838_1222, "838:12:22", false),
            (1_001_222, "100:12:22", false),
            (20_171_222, "838:59:59", true),
            (176_022, "00:00:00.0", true),
            (171_260, "00:00:00.0", true),
        ] {
            let actual =
                number_to_duration(number, i64::from(number == 176_022 || number == 171_260))
                    .unwrap();
            assert_eq!(actual.value.to_string(), expected, "{number}");
            assert_eq!(actual.event.is_some(), event, "{number}");
        }
        let datetime = number_to_duration(20_171_222_020_005, 0).unwrap();
        assert_eq!(datetime.value.to_string(), "02:00:05");
        assert!(datetime.event.is_none());

        for (input, fsp, is_duration) in [
            ("20190412120000", 4, false),
            ("20190101180000", 6, false),
            ("20190101180000", 1, false),
            ("20190101181234", 3, false),
            ("00:00:00.000000", 6, true),
            ("00:00:00", 0, true),
        ] {
            let actual = str_to_duration(input, fsp, &chrono_tz::UTC).unwrap();
            assert_eq!(
                matches!(actual.value, DurationOrTime::Duration(_)),
                is_duration,
                "{input}"
            );
        }
    }

    #[test]
    fn source_json_conversion_rows() {
        for (input, expected, truncated) in [
            ("{}", 0, true),
            ("[]", 0, true),
            ("3", 3, false),
            ("-3", -3, false),
            ("4.5", 4, false),
            ("true", 1, false),
            ("false", 0, false),
            ("null", 0, true),
            ("\"hello\"", 0, true),
            ("\"123hello\"", 123, true),
            ("\"1234\"", 1234, false),
        ] {
            let json = BinaryJSON::parse(input).unwrap();
            let actual = json_to_int64(&json, false, ConversionFlags::from_bits(0));
            assert_eq!(actual.value, expected, "{input}");
            assert_eq!(actual.event.is_some(), truncated, "{input}");
        }

        for (input, expected, truncated) in [
            ("{}", 0.0, true),
            ("[]", 0.0, true),
            ("3", 3.0, false),
            ("-3", -3.0, false),
            ("4.5", 4.5, false),
            ("true", 1.0, false),
            ("false", 0.0, false),
            ("null", 0.0, true),
            ("\"hello\"", 0.0, true),
            ("\"123.456hello\"", 123.456, true),
            ("\"1234\"", 1234.0, false),
        ] {
            let json = BinaryJSON::parse(input).unwrap();
            let actual = json_to_float(&json);
            assert_eq!(actual.value, expected, "{input}");
            assert_eq!(actual.event.is_some(), truncated, "{input}");
        }

        for (input, expected, truncated) in [
            ("3", "3", false),
            ("-3", "-3", false),
            ("4.5", "4.5", false),
            ("\"1234\"", "1234", false),
            (
                "\"1234567890123456789012345678901234567890123456789012345\"",
                "1234567890123456789012345678901234567890123456789012345",
                false,
            ),
            ("true", "1", false),
            ("false", "0", false),
            ("null", "0", true),
        ] {
            let json = BinaryJSON::parse(input).unwrap();
            let actual = json_to_decimal(&json);
            assert_eq!(actual.value.to_string(), expected, "{input}");
            assert_eq!(actual.event.is_some(), truncated, "{input}");
        }
    }

    #[test]
    fn source_to_string_rows() {
        for (value, expected) in [
            (ScalarStringValue::String("0"), "0"),
            (ScalarStringValue::Bool(true), "1"),
            (ScalarStringValue::String("false"), "false"),
            (ScalarStringValue::Int(0), "0"),
            (ScalarStringValue::Uint(0), "0"),
            (ScalarStringValue::Float32(1.6), "1.6"),
            (ScalarStringValue::Float64(-0.6), "-0.6"),
        ] {
            assert_eq!(scalar_to_string(value).unwrap(), expected);
        }
        assert_eq!(
            scalar_to_string(ScalarStringValue::Bytes(&[1])).unwrap(),
            "\u{1}"
        );

        let mysql = BinaryLiteral::from_uint(0x004d_7953_514c, None);
        assert_eq!(
            scalar_to_string(ScalarStringValue::BinaryLiteral(&mysql)).unwrap(),
            "MySQL"
        );
        let time = crate::parse_time(
            "2011-11-10 11:11:11.999999",
            TimeType::Timestamp,
            6,
            false,
            true,
            false,
            &chrono_tz::UTC,
        )
        .unwrap()
        .time;
        assert_eq!(
            scalar_to_string(ScalarStringValue::Time(time)).unwrap(),
            "2011-11-10 11:11:11.999999"
        );
        let duration = MySqlDuration::new(11, 11, 11, 999_999, 6).unwrap();
        assert_eq!(
            scalar_to_string(ScalarStringValue::Duration(duration)).unwrap(),
            "11:11:11.999999"
        );
        let decimal = Decimal::from_signed_literal("3.14159");
        assert_eq!(
            scalar_to_string(ScalarStringValue::Decimal(&decimal)).unwrap(),
            "3.14159"
        );
    }
}
