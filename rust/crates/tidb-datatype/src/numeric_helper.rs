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

use std::fmt;

/// Best-effort integer parsing failure from `types.strToInt`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StringToIntError {
    /// No complete integer was present or trailing input remained.
    Truncated,
    /// The magnitude exceeded the signed/unsigned accumulator.
    BadNumber,
}

impl fmt::Display for StringToIntError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => formatter.write_str("truncated"),
            Self::BadNumber => formatter.write_str("bad number"),
        }
    }
}

impl std::error::Error for StringToIntError {}

/// Overflow returned while narrowing a floating SQL value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FloatOverflow;

impl fmt::Display for FloatOverflow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DOUBLE value is out of range")
    }
}

impl std::error::Error for FloatOverflow {}

/// Rounds to the nearest even integer, matching Go `math.RoundToEven`.
pub fn round_float(value: f64) -> f64 {
    value.round_ties_even()
}

/// Rounds `value` to `decimal` decimal places.
pub fn round(value: f64, decimal: i32) -> f64 {
    let shift = decimal_shift(decimal);
    let shifted = value * shift;
    if shifted.is_infinite() {
        return value;
    }
    let result = round_float(shifted) / shift;
    if result.is_nan() {
        0.0
    } else {
        result
    }
}

/// Truncates `value` to `decimal` decimal places.
pub fn truncate(value: f64, decimal: i32) -> f64 {
    let shift = decimal_shift(decimal);
    let shifted = value * shift;
    if shifted.is_infinite() || shifted.is_nan() {
        return value;
    }
    if shift == 0.0 {
        return if value.is_nan() { value } else { 0.0 };
    }
    shifted.trunc() / shift
}

/// Returns the largest magnitude admitted by a `(flen, decimal)` float.
pub fn get_max_float(flen: i32, decimal: i32) -> f64 {
    decimal_shift(flen - decimal) - decimal_shift(-decimal)
}

/// Rounds and clamps a float to a MySQL `(flen, decimal)` domain.
pub fn truncate_float(
    mut value: f64,
    flen: i32,
    decimal: i32,
) -> Result<f64, (f64, FloatOverflow)> {
    if value.is_nan() {
        return Err((0.0, FloatOverflow));
    }
    let maximum = get_max_float(flen, decimal);
    if !value.is_infinite() {
        value = round(value, decimal);
    }
    if value > maximum {
        Err((maximum, FloatOverflow))
    } else if value < -maximum {
        Err((-maximum, FloatOverflow))
    } else {
        Ok(value)
    }
}

/// Truncates and renders without an exponent, matching Go format `'f', -1`.
pub fn truncate_float_to_string(value: f64, decimal: i32) -> String {
    fixed_shortest(truncate(value, decimal))
}

/// Parses a signed integer in TiDB's best-effort mode.
pub fn string_to_int(value: &str) -> Result<i64, (i64, StringToIntError)> {
    let value = value.trim();
    if value.is_empty() {
        return Err((0, StringToIntError::Truncated));
    }
    let bytes = value.as_bytes();
    let (negative, mut index) = match bytes[0] {
        b'-' => (true, 1),
        b'+' => (false, 1),
        _ => (false, 0),
    };
    let mut magnitude = 0_u64;
    let mut has_number = false;
    let mut trailing = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if !byte.is_ascii_digit() {
            trailing = true;
            break;
        }
        has_number = true;
        let Some(next) = magnitude
            .checked_mul(10)
            .and_then(|number| number.checked_add(u64::from(byte - b'0')))
        else {
            return Err((
                if negative { i64::MIN } else { i64::MAX },
                StringToIntError::BadNumber,
            ));
        };
        magnitude = next;
        index += 1;
    }
    if !has_number {
        return Err((0, StringToIntError::Truncated));
    }
    let limit = i64::MAX as u64 + u64::from(negative);
    if magnitude > limit {
        return Err((
            if negative { i64::MIN } else { i64::MAX },
            StringToIntError::BadNumber,
        ));
    }
    let output = if negative {
        (0_u64.wrapping_sub(magnitude)) as i64
    } else {
        magnitude as i64
    };
    if trailing {
        Err((output, StringToIntError::Truncated))
    } else {
        Ok(output)
    }
}

/// Converts display length to decimal precision.
pub const fn decimal_length_to_precision(mut length: i32, scale: i32, unsigned: bool) -> i32 {
    if scale > 0 {
        length -= 1;
    }
    if unsigned || length > 0 {
        length -= 1;
    }
    length
}

/// Converts decimal precision to display length without truncation.
pub const fn precision_to_length_no_truncation(mut length: i32, scale: i32, unsigned: bool) -> i32 {
    if scale > 0 {
        length += 1;
    }
    if unsigned || length > 0 {
        length += 1;
    }
    length
}

fn decimal_shift(decimal: i32) -> f64 {
    if decimal > 308 {
        f64::INFINITY
    } else if decimal < -323 {
        0.0
    } else {
        10_f64.powi(decimal)
    }
}

fn fixed_shortest(value: f64) -> String {
    let shortest = value.to_string();
    let Some((mantissa, exponent)) = shortest
        .split_once('e')
        .or_else(|| shortest.split_once('E'))
    else {
        return shortest;
    };
    let exponent: i32 = exponent.parse().expect("Rust exponent is numeric");
    let negative = mantissa.starts_with('-');
    let unsigned = mantissa.trim_start_matches('-');
    let digits: String = unsigned.chars().filter(|ch| *ch != '.').collect();
    let decimal = unsigned.find('.').map_or(1_i32, |index| index as i32);
    let point = decimal + exponent;
    let mut output = String::new();
    if negative {
        output.push('-');
    }
    if point <= 0 {
        output.push_str("0.");
        output.extend(std::iter::repeat_n('0', (-point) as usize));
        output.push_str(&digits);
    } else if point as usize >= digits.len() {
        output.push_str(&digits);
        output.extend(std::iter::repeat_n('0', point as usize - digits.len()));
    } else {
        output.push_str(&digits[..point as usize]);
        output.push('.');
        output.push_str(&digits[point as usize..]);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_str_to_int() {
        for (input, output, error) in [
            ("9223372036854775806", 9_223_372_036_854_775_806, None),
            ("9223372036854775807", i64::MAX, None),
            (
                "9223372036854775808",
                i64::MAX,
                Some(StringToIntError::BadNumber),
            ),
            ("-9223372036854775807", -9_223_372_036_854_775_807, None),
            ("-9223372036854775808", i64::MIN, None),
            (
                "-9223372036854775809",
                i64::MIN,
                Some(StringToIntError::BadNumber),
            ),
        ] {
            match (string_to_int(input), error) {
                (Ok(actual), None) => assert_eq!(actual, output),
                (Err((actual, actual_error)), Some(expected_error)) => {
                    assert_eq!(actual, output);
                    assert_eq!(actual_error, expected_error);
                }
                (actual, expected) => panic!("{input}: {actual:?} != {expected:?}"),
            }
        }
    }

    #[test]
    fn test_round_float() {
        for (input, expected) in [
            (2.5, 2.0),
            (1.5, 2.0),
            (0.5, 0.0),
            (0.499_999_999_999_999_97, 0.0),
            (0.0, 0.0),
            (-0.499_999_999_999_999_97, -0.0),
            (-0.5, -0.0),
            (-2.5, -2.0),
            (-1.5, -2.0),
        ] {
            assert_eq!(round_float(input), expected);
        }
    }

    #[test]
    fn test_round() {
        for (input, decimal, expected) in [
            (-1.23, 0, -1.0),
            (-1.58, 0, -2.0),
            (1.58, 0, 2.0),
            (1.298, 1, 1.3),
            (1.298, 0, 1.0),
            (23.298, -1, 20.0),
        ] {
            assert_eq!(round(input, decimal), expected);
        }
    }

    #[test]
    fn test_truncate() {
        for (input, decimal, expected) in [
            (123.45, 0, 123.0),
            (123.45, 1, 123.4),
            (123.45, 2, 123.45),
            (123.45, 3, 123.450),
            (123.45, -400, 0.0),
            (123.45, 400, 123.45),
        ] {
            assert_eq!(truncate(input, decimal), expected);
        }
    }

    #[test]
    fn test_max_float() {
        assert_eq!(get_max_float(3, 2), 9.99);
        assert_eq!(get_max_float(5, 2), 999.99);
        assert_eq!(get_max_float(10, 1), 999_999_999.9);
        assert_eq!(get_max_float(5, 5), 0.99999);
    }

    #[test]
    fn test_truncate_float() {
        assert_eq!(truncate_float(100.114, 10, 2), Ok(100.11));
        assert_eq!(truncate_float(100.115, 10, 2), Ok(100.12));
        assert_eq!(truncate_float(100.1156, 10, 3), Ok(100.116));
        assert_eq!(truncate_float(100.1156, 3, 1), Err((99.9, FloatOverflow)));
        assert_eq!(truncate_float(1.36, 10, 2), Ok(1.36));
    }

    #[test]
    fn test_truncate_float_to_string() {
        for (input, decimal, expected) in [
            (12.13, -1, "10"),
            (13.15, 0, "13"),
            (0.0, 2, "0"),
            (0.001, 2, "0"),
            (0.539, 2, "0.53"),
            (0.9951, 2, "0.99"),
            (1.0, 2, "1"),
            (-0.456, 2, "-0.45"),
        ] {
            assert_eq!(truncate_float_to_string(input, decimal), expected);
        }
    }
}
