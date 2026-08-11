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

//! Optimizer fix-control parsing from `pkg/planner/util/fixcontrol/set.go`.
//!
//! This leaf owns only the source's text-to-map parser. Session variables,
//! warning plumbing lives in `tidb-session`; this module owns the source-shaped
//! parser, issue-number catalog, and typed getters shared by planner consumers.

use std::{collections::BTreeMap, fmt, num::IntErrorKind};

/// Disables PointGet and BatchPointGet fast paths when enabled.
pub const FIX_52592: u64 = 52592;
/// Disables plan cache for partitioned tables when enabled.
pub const FIX_33031: u64 = 33031;
/// Controls optimizer evaluation of non-correlated subqueries.
pub const FIX_43817: u64 = 43817;
/// Controls dynamic-mode access to partitioned tables without global stats.
pub const FIX_44262: u64 = 44262;
/// Controls whether non-point CNF ranges participate in range building.
pub const FIX_44389: u64 = 44389;
/// Controls caching PointGet and BatchPointGet in complex scenarios.
pub const FIX_44830: u64 = 44830;
/// Controls the plan-cache parameter-count limit.
pub const FIX_44823: u64 = 44823;
/// Controls the index-join range-scan row-count upper bound.
pub const FIX_44855: u64 = 44855;
/// Controls skyline pruning's use of access-range row counts.
pub const FIX_45132: u64 = 45132;
/// Controls elimination of Apply operators.
pub const FIX_45822: u64 = 45822;
/// Controls caching plans that access generated columns.
pub const FIX_45798: u64 = 45798;
/// Controls exploration of enforced DataSource plans.
pub const FIX_46177: u64 = 46177;
/// Deprecated control for allowing row estimates below one.
pub const FIX_47400: u64 = 47400;
/// Test-only control that forces risky plans into plan cache.
pub const FIX_49736: u64 = 49736;
/// Controls automatic index-merge generation beside range scans.
pub const FIX_52869: u64 = 52869;
/// Controls range intersection for index access.
pub const FIX_54337: u64 = 54337;
/// Controls HeavyFunctionOptimize in TopN operators.
pub const FIX_56318: u64 = 56318;

/// A session's parsed `tidb_opt_fix_control` assignments.
///
/// Values stay as strings because each consumer owns its fallback type, just
/// as Go's `map[uint64]string` does. The typed accessors distinguish an absent
/// key from a present value that does not parse; the `*_with_default` forms
/// collapse both cases to the caller's chosen default, matching `get.go`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OptimizerFixControl {
    values: BTreeMap<u64, String>,
}

impl OptimizerFixControl {
    /// Parses the system-variable text and returns its duplicate warnings.
    pub fn parse(input: &str) -> Result<(Self, Vec<String>), ParseError> {
        let ParsedFixControls { values, warnings } = parse_to_map(input)?;
        Ok((Self { values }, warnings))
    }

    /// The raw source-shaped map, in numeric-key order.
    #[must_use]
    pub fn as_map(&self) -> &BTreeMap<u64, String> {
        &self.values
    }

    /// Fetches a raw string value and preserves key absence.
    #[must_use]
    pub fn get_str(&self, key: u64) -> Option<&str> {
        self.values.get(&key).map(String::as_str)
    }

    /// Fetches a raw string value or the caller's default.
    #[must_use]
    pub fn get_str_with_default<'a>(&'a self, key: u64, default: &'a str) -> &'a str {
        self.get_str(key).unwrap_or(default)
    }

    /// Fetches a boolean value; only case-insensitive `ON` and exact `1` are true.
    #[must_use]
    pub fn get_bool(&self, key: u64) -> Option<bool> {
        self.get_str(key)
            .map(|value| value.eq_ignore_ascii_case("ON") || value == "1")
    }

    /// Fetches a boolean value or the caller's default when the key is absent.
    #[must_use]
    pub fn get_bool_with_default(&self, key: u64, default: bool) -> bool {
        self.get_bool(key).unwrap_or(default)
    }

    /// Fetches a signed decimal integer as Go's `(value, exists, parseErr)` triple.
    #[must_use]
    pub fn get_int(&self, key: u64) -> (i64, bool, Option<IntParseError>) {
        let Some(raw) = self.get_str(key) else {
            return (0, false, None);
        };
        let (value, error) = parse_go_int(raw);
        (value, true, error)
    }

    /// Fetches an integer or the caller's default on absence or parse failure.
    #[must_use]
    pub fn get_int_with_default(&self, key: u64, default: i64) -> i64 {
        let (value, exists, error) = self.get_int(key);
        if exists && error.is_none() {
            value
        } else {
            default
        }
    }

    /// Fetches a float as Go's `(value, exists, parseErr)` triple.
    #[must_use]
    pub fn get_float(&self, key: u64) -> (f64, bool, Option<FloatParseError>) {
        let Some(raw) = self.get_str(key) else {
            return (0.0, false, None);
        };
        let (value, error) = parse_go_float(raw);
        (value, true, error)
    }

    /// Fetches a float or the caller's default on absence or parse failure.
    #[must_use]
    pub fn get_float_with_default(&self, key: u64, default: f64) -> f64 {
        let (value, exists, error) = self.get_float(key);
        if exists && error.is_none() {
            value
        } else {
            default
        }
    }
}

/// An error reported with Go's `strconv.ParseInt` spelling.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntParseError {
    input: String,
    out_of_range: bool,
}

impl fmt::Display for IntParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "strconv.ParseInt: parsing {}: {}",
            tidb_error::mysql::go_quote_string(&self.input),
            if self.out_of_range {
                "value out of range"
            } else {
                "invalid syntax"
            }
        )
    }
}

impl std::error::Error for IntParseError {}

fn parse_go_int(input: &str) -> (i64, Option<IntParseError>) {
    let (negative, magnitude_text) = match input.as_bytes().first() {
        Some(b'+') => (false, &input[1..]),
        Some(b'-') => (true, &input[1..]),
        _ => (false, input),
    };
    let (magnitude, parse_error) = parse_go_uint_decimal(magnitude_text);
    if parse_error == Some(DecimalParseError::Syntax) {
        return (
            0,
            Some(IntParseError {
                input: input.to_owned(),
                out_of_range: false,
            }),
        );
    }
    if parse_error == Some(DecimalParseError::Range) {
        return (
            if negative { i64::MIN } else { i64::MAX },
            Some(IntParseError {
                input: input.to_owned(),
                out_of_range: true,
            }),
        );
    }

    let negative_cutoff = 1_u64 << 63;
    if (!negative && magnitude >= negative_cutoff) || (negative && magnitude > negative_cutoff) {
        return (
            if negative { i64::MIN } else { i64::MAX },
            Some(IntParseError {
                input: input.to_owned(),
                out_of_range: true,
            }),
        );
    }
    if negative {
        if magnitude == negative_cutoff {
            (i64::MIN, None)
        } else {
            (-(magnitude as i64), None)
        }
    } else {
        (magnitude as i64, None)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DecimalParseError {
    Syntax,
    Range,
}

/// Go `strconv.ParseUint(s, 10, 64)`'s digit loop, including its observable
/// error precedence: once the accumulator crosses uint64, ErrRange returns
/// immediately and a later non-digit is never inspected.
fn parse_go_uint_decimal(input: &str) -> (u64, Option<DecimalParseError>) {
    if input.is_empty() {
        return (0, Some(DecimalParseError::Syntax));
    }
    let cutoff = u64::MAX / 10 + 1;
    let mut value = 0_u64;
    for byte in input.bytes() {
        if !byte.is_ascii_digit() {
            return (0, Some(DecimalParseError::Syntax));
        }
        if value >= cutoff {
            return (u64::MAX, Some(DecimalParseError::Range));
        }
        value *= 10;
        let Some(next) = value.checked_add(u64::from(byte - b'0')) else {
            return (u64::MAX, Some(DecimalParseError::Range));
        };
        value = next;
    }
    (value, None)
}

/// The syntax/range distinction reported by Go's `strconv.ParseFloat`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FloatParseError {
    input: String,
    out_of_range: bool,
}

impl fmt::Display for FloatParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "strconv.ParseFloat: parsing {}: {}",
            tidb_error::mysql::go_quote_string(&self.input),
            if self.out_of_range {
                "value out of range"
            } else {
                "invalid syntax"
            }
        )
    }
}

impl std::error::Error for FloatParseError {}

fn float_error(input: &str, out_of_range: bool) -> FloatParseError {
    FloatParseError {
        input: input.to_owned(),
        out_of_range,
    }
}

/// Parses the grammar accepted by Go's `strconv.ParseFloat` for `float64`.
///
/// Rust's `f64::from_str` handles decimal rounding, while hexadecimal literals
/// need a small binary parser. Keeping the conversion here prevents values such
/// as `0x1p2` from silently taking the default in Rust-only planner consumers.
fn parse_go_float(input: &str) -> (f64, Option<FloatParseError>) {
    let (negative, unsigned) = match input.as_bytes().first() {
        Some(b'+') => (false, &input[1..]),
        Some(b'-') => (true, &input[1..]),
        _ => (false, input),
    };

    if unsigned.eq_ignore_ascii_case("inf") || unsigned.eq_ignore_ascii_case("infinity") {
        return (
            if negative {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            },
            None,
        );
    }
    if !negative && !input.starts_with('+') && unsigned.eq_ignore_ascii_case("nan") {
        return (f64::NAN, None);
    }
    if unsigned.eq_ignore_ascii_case("nan") {
        return (0.0, Some(float_error(input, false)));
    }

    if unsigned.starts_with("0x") || unsigned.starts_with("0X") {
        return parse_go_hex_float(input, negative, &unsigned[2..]);
    }

    let Some(normalized) = normalize_decimal_underscores(input) else {
        return (0.0, Some(float_error(input, false)));
    };
    match normalized.parse::<f64>() {
        Ok(value) if value.is_infinite() => (value, Some(float_error(input, true))),
        Ok(value) => (value, None),
        Err(_) => (0.0, Some(float_error(input, false))),
    }
}

fn normalize_decimal_underscores(input: &str) -> Option<String> {
    if !input.contains('_') {
        return Some(input.to_owned());
    }
    let bytes = input.as_bytes();
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'_'
            && (index == 0
                || index + 1 == bytes.len()
                || !bytes[index - 1].is_ascii_digit()
                || !bytes[index + 1].is_ascii_digit())
        {
            return None;
        }
    }
    Some(
        input
            .chars()
            .filter(|character| *character != '_')
            .collect(),
    )
}

fn parse_go_hex_float(
    input: &str,
    negative: bool,
    unsigned_body: &str,
) -> (f64, Option<FloatParseError>) {
    let Some(exponent_index) = unsigned_body.find(['p', 'P']) else {
        return (0.0, Some(float_error(input, false)));
    };
    if unsigned_body[exponent_index + 1..].contains(['p', 'P']) {
        return (0.0, Some(float_error(input, false)));
    }
    let mantissa = &unsigned_body[..exponent_index];
    let mut digits = Vec::new();
    let mut digits_before_dot = None;
    let bytes = mantissa.as_bytes();
    for (index, byte) in bytes.iter().enumerate() {
        match byte {
            b'.' if digits_before_dot.is_none() => digits_before_dot = Some(digits.len()),
            b'_' => {
                let after_prefix = index == 0;
                let between_digits = index > 0
                    && index + 1 < bytes.len()
                    && bytes[index - 1].is_ascii_hexdigit()
                    && bytes[index + 1].is_ascii_hexdigit();
                if !(after_prefix
                    && index + 1 < bytes.len()
                    && bytes[index + 1].is_ascii_hexdigit())
                    && !between_digits
                {
                    return (0.0, Some(float_error(input, false)));
                }
            }
            byte if byte.is_ascii_hexdigit() => digits.push(hex_value(*byte)),
            _ => return (0.0, Some(float_error(input, false))),
        }
    }
    let digits_before_dot = digits_before_dot.unwrap_or(digits.len());
    if digits.is_empty() {
        return (0.0, Some(float_error(input, false)));
    }

    let exponent_text = &unsigned_body[exponent_index + 1..];
    let Some(exponent_text) = normalize_decimal_underscores(exponent_text)
        .filter(|text| !text.is_empty() && text != "+" && text != "-")
    else {
        return (0.0, Some(float_error(input, false)));
    };
    enum Exponent {
        Finite(i64),
        PositiveOverflow,
        NegativeOverflow,
    }
    let exponent = match exponent_text.parse::<i64>() {
        Ok(exponent) => Exponent::Finite(exponent),
        Err(error) if matches!(error.kind(), IntErrorKind::PosOverflow) => {
            Exponent::PositiveOverflow
        }
        Err(error) if matches!(error.kind(), IntErrorKind::NegOverflow) => {
            Exponent::NegativeOverflow
        }
        Err(_) => return (0.0, Some(float_error(input, false))),
    };

    let signed_zero = if negative { -0.0 } else { 0.0 };
    let signed_infinity = if negative {
        f64::NEG_INFINITY
    } else {
        f64::INFINITY
    };
    let Some(first_nonzero) = digits.iter().position(|digit| *digit != 0) else {
        return (signed_zero, None);
    };
    let exponent = match exponent {
        Exponent::Finite(exponent) => exponent,
        Exponent::PositiveOverflow => {
            return (signed_infinity, Some(float_error(input, true)));
        }
        Exponent::NegativeOverflow => return (signed_zero, None),
    };
    let significant = &digits[first_nonzero..];
    let first_bits = u8::BITS as i64 - significant[0].leading_zeros() as i64;
    let significant_len = i64::try_from(significant.len()).unwrap_or(i64::MAX);
    let bit_len = first_bits.saturating_add(4_i64.saturating_mul(significant_len - 1));
    let integer_digits = i64::try_from(digits_before_dot).unwrap_or(i64::MAX);
    let total_digits = i64::try_from(digits.len()).unwrap_or(i64::MAX);
    let binary_scale =
        exponent.saturating_add(4_i64.saturating_mul(integer_digits.saturating_sub(total_digits)));
    let mut unbiased_exponent = bit_len.saturating_sub(1).saturating_add(binary_scale);

    let bits = if unbiased_exponent >= -1022 {
        let mut significand = round_hex_significand(significant, bit_len.saturating_sub(53));
        if significand == 1_u64 << 53 {
            significand >>= 1;
            unbiased_exponent = unbiased_exponent.saturating_add(1);
        }
        if unbiased_exponent > 1023 {
            return (signed_infinity, Some(float_error(input, true)));
        }
        (((unbiased_exponent + 1023) as u64) << 52) | (significand & ((1_u64 << 52) - 1))
    } else {
        let right_shift = binary_scale.saturating_neg().saturating_sub(1074);
        let significand = round_hex_significand(significant, right_shift);
        if significand >= 1_u64 << 52 {
            1_u64 << 52
        } else {
            significand
        }
    };
    (f64::from_bits(bits | (u64::from(negative) << 63)), None)
}

fn hex_value(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => unreachable!("the caller validates hexadecimal digits"),
    }
}

fn round_hex_significand(digits: &[u8], right_shift: i64) -> u64 {
    let bit_len =
        (u8::BITS as i64 - digits[0].leading_zeros() as i64) + 4 * (digits.len() as i64 - 1);
    if right_shift <= 0 {
        let value = top_hex_bits(digits, bit_len as usize);
        return value.checked_shl((-right_shift) as u32).unwrap_or(u64::MAX);
    }
    if right_shift >= bit_len.saturating_add(1) {
        return 0;
    }
    let retained = (bit_len - right_shift).max(0) as usize;
    let mut value = top_hex_bits(digits, retained);
    let guard_index = right_shift - 1;
    let guard = hex_bit(digits, guard_index);
    let sticky = (0..guard_index).any(|index| hex_bit(digits, index));
    if guard && (sticky || value & 1 == 1) {
        value += 1;
    }
    value
}

fn top_hex_bits(digits: &[u8], count: usize) -> u64 {
    let bit_len = (u8::BITS as usize - digits[0].leading_zeros() as usize) + 4 * (digits.len() - 1);
    (0..count).fold(0_u64, |value, offset| {
        (value << 1) | u64::from(hex_bit(digits, (bit_len - 1 - offset) as i64))
    })
}

fn hex_bit(digits: &[u8], index_from_right: i64) -> bool {
    if index_from_right < 0 {
        return false;
    }
    let nibble_from_right = (index_from_right / 4) as usize;
    if nibble_from_right >= digits.len() {
        return false;
    }
    let digit = digits[digits.len() - 1 - nibble_from_right];
    digit & (1 << (index_from_right % 4)) != 0
}

impl From<BTreeMap<u64, String>> for OptimizerFixControl {
    fn from(values: BTreeMap<u64, String>) -> Self {
        Self { values }
    }
}

/// Parsed fix-control assignments and duplicate-key warnings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedFixControls {
    /// The final value for each fix-control number.
    pub values: BTreeMap<u64, String>,
    /// Warnings emitted when a key is assigned different values repeatedly.
    pub warnings: Vec<String>,
}

/// Errors emitted by the source-shaped fix-control parser.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParseError {
    /// An assignment did not contain a colon.
    MissingColon,
    /// The text before a colon was not an unsigned decimal key.
    InvalidKey {
        /// The exact key text passed to Go's `strconv.ParseUint`.
        input: String,
        /// Whether parsing failed because the value exceeded `uint64`.
        out_of_range: bool,
    },
    /// A quoted value did not contain its closing quote.
    MissingQuote,
}

impl fmt::Display for ParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::MissingColon => "invalid fix control: expected colon not found",
            Self::InvalidKey {
                input,
                out_of_range,
            } => {
                return write!(
                    formatter,
                    "strconv.ParseUint: parsing {}: {}",
                    tidb_error::mysql::go_quote_string(input),
                    if *out_of_range {
                        "value out of range"
                    } else {
                        "invalid syntax"
                    }
                );
            }
            Self::MissingQuote => "invalid fix control: expected quote not found",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for ParseError {}

/// Parses comma-separated optimizer fix-control assignments.
///
/// This follows the source's intentionally small grammar: values may be
/// unquoted (trimmed through the next comma) or quoted with either quote
/// character, and a repeated key replaces the previous value while warning
/// only when the value changed.
pub fn parse_to_map(input: &str) -> Result<ParsedFixControls, ParseError> {
    let mut values = BTreeMap::new();
    let mut warnings = Vec::new();
    let mut remaining = input;

    while !remaining.is_empty() {
        let colon = remaining.find(':').ok_or(ParseError::MissingColon)?;
        let key_text = remaining[..colon].trim();
        let (key, error) = parse_go_uint_decimal(key_text);
        if let Some(error) = error {
            return Err(ParseError::InvalidKey {
                input: key_text.to_owned(),
                out_of_range: error == DecimalParseError::Range,
            });
        }
        remaining = remaining[colon + 1..].trim();

        let mut value = String::new();
        if let Some(quote) = remaining
            .as_bytes()
            .first()
            .copied()
            .filter(|byte| *byte == b'\'' || *byte == b'"')
        {
            let quote = char::from(quote);
            let closing = remaining[1..].find(quote).ok_or(ParseError::MissingQuote)?;
            let end = closing + 1;
            value.push_str(&remaining[1..end]);
            remaining = &remaining[end + 1..];
        }

        let end = remaining.find(',').unwrap_or(remaining.len());
        let next = remaining
            .find(',')
            .map_or(remaining.len(), |comma| comma + 1);
        if value.is_empty() {
            value.push_str(remaining[..end].trim());
        }

        if let Some(previous) = values.insert(key, value.clone()) {
            if previous != value {
                warnings.push(format!(
                    "repeated assignment for fix control: {key}. existing value: {}. new value: {}.",
                    tidb_error::mysql::go_quote_string(&previous),
                    tidb_error::mysql::go_quote_string(&value),
                ));
            }
        }
        remaining = remaining[next..].trim();
    }

    Ok(ParsedFixControls { values, warnings })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{parse_to_map, OptimizerFixControl, FIX_52592};

    #[test]
    fn empty_value_is_a_present_empty_string() {
        let parsed = parse_to_map("123:").expect("Go accepts an empty value");
        assert_eq!(parsed.values, BTreeMap::from([(123, String::new())]));
        assert!(parsed.warnings.is_empty());
    }

    #[test]
    fn float_getter_uses_go_parse_float_grammar() {
        for (source, expected) in [
            ("0x1p2", 4.0),
            ("-0X1.8p+1", -3.0),
            ("1_2.5", 12.5),
            ("+Inf", f64::INFINITY),
            ("-Inf", f64::NEG_INFINITY),
        ] {
            let (control, warnings) = OptimizerFixControl::parse(&format!("{FIX_52592}:{source}"))
                .expect("the fix-control assignment is syntactically valid");
            assert!(warnings.is_empty());
            let (value, exists, error) = control.get_float(FIX_52592);
            assert!(exists, "{source}");
            assert_eq!(error, None, "{source}");
            assert_eq!(value, expected, "{source}");
        }

        let (nan, _) = OptimizerFixControl::parse(&format!("{FIX_52592}:NaN")).unwrap();
        let (value, exists, error) = nan.get_float(FIX_52592);
        assert!(exists && error.is_none() && value.is_nan());

        for malformed in ["not-a-number", "+NaN", "0x1", "0x.p1", "0x1p"] {
            let (control, _) =
                OptimizerFixControl::parse(&format!("{FIX_52592}:{malformed}")).unwrap();
            let (value, exists, error) = control.get_float(FIX_52592);
            assert_eq!(value, 0.0, "{malformed}");
            assert!(exists && error.is_some(), "{malformed}");
            assert_eq!(control.get_float_with_default(FIX_52592, 1234.5), 1234.5);
        }

        for (source, value, has_error) in [
            ("0x.p-18446744073709551616", 0.0, true),
            ("0x0p+18446744073709551616", 0.0, false),
            ("0x1p-9223372036854775808", 0.0, false),
            ("0x1p+18446744073709551616", f64::INFINITY, true),
        ] {
            let (control, _) =
                OptimizerFixControl::parse(&format!("{FIX_52592}:{source}")).unwrap();
            let (actual, exists, error) = control.get_float(FIX_52592);
            assert!(exists, "{source}");
            assert_eq!(actual, value, "{source}");
            assert_eq!(error.is_some(), has_error, "{source}");
        }

        for (source, expected_bits, has_error) in [
            ("0x1.fffffffffffffp1023", 0x7fef_ffff_ffff_ffff, false),
            ("0x1.fffffffffffff7fffp1023", 0x7fef_ffff_ffff_ffff, false),
            ("0x1.fffffffffffff8p1023", 0x7ff0_0000_0000_0000, true),
            ("0x0.00000003456788p-1022", 0x0034_5678, false),
            ("0x0.00000003456788000000000001p-1022", 0x0034_5679, false),
            ("0x0.00000000000008p-1022", 0, false),
            ("0x0.000000000000081p-1022", 1, false),
        ] {
            let (control, _) =
                OptimizerFixControl::parse(&format!("{FIX_52592}:{source}")).unwrap();
            let (actual, exists, error) = control.get_float(FIX_52592);
            assert!(exists, "{source}");
            assert_eq!(actual.to_bits(), expected_bits, "{source}");
            assert_eq!(error.is_some(), has_error, "{source}");
        }
    }

    #[test]
    fn integer_getter_reports_go_parse_int_errors() {
        for (source, expected) in [("+7", 7), ("-8", -8), ("0", 0)] {
            let (control, _) =
                OptimizerFixControl::parse(&format!("{FIX_52592}:{source}")).unwrap();
            assert_eq!(control.get_int(FIX_52592), (expected, true, None));
        }
        for (source, message) in [
            ("55.5", "strconv.ParseInt: parsing \"55.5\": invalid syntax"),
            (
                "9223372036854775808",
                "strconv.ParseInt: parsing \"9223372036854775808\": value out of range",
            ),
            (
                "-9223372036854775809",
                "strconv.ParseInt: parsing \"-9223372036854775809\": value out of range",
            ),
        ] {
            let (control, _) =
                OptimizerFixControl::parse(&format!("{FIX_52592}:{source}")).unwrap();
            let (value, exists, error) = control.get_int(FIX_52592);
            assert!(exists);
            assert_eq!(
                error
                    .expect("the value is outside Go's signed decimal grammar")
                    .to_string(),
                message
            );
            assert_eq!(
                value,
                if source.starts_with('-') {
                    i64::MIN
                } else if source == "55.5" {
                    0
                } else {
                    i64::MAX
                }
            );
            assert_eq!(control.get_int_with_default(FIX_52592, 12345), 12345);
        }

        for (source, expected_value, expected_message) in [
            (
                "9223372036854775808x",
                0,
                "strconv.ParseInt: parsing \"9223372036854775808x\": invalid syntax",
            ),
            (
                "-9223372036854775809x",
                0,
                "strconv.ParseInt: parsing \"-9223372036854775809x\": invalid syntax",
            ),
            (
                "18446744073709551616x",
                i64::MAX,
                "strconv.ParseInt: parsing \"18446744073709551616x\": value out of range",
            ),
            (
                "-18446744073709551616x",
                i64::MIN,
                "strconv.ParseInt: parsing \"-18446744073709551616x\": value out of range",
            ),
        ] {
            let (control, _) =
                OptimizerFixControl::parse(&format!("{FIX_52592}:{source}")).unwrap();
            let (value, exists, error) = control.get_int(FIX_52592);
            assert!(exists, "{source}");
            assert_eq!(value, expected_value, "{source}");
            assert_eq!(error.unwrap().to_string(), expected_message, "{source}");
        }
    }

    #[test]
    fn fix_number_rejects_every_sign_prefix() {
        for source in ["+1:ON", "-1:ON"] {
            assert_eq!(
                OptimizerFixControl::parse(source)
                    .expect_err("strconv.ParseUint base 10 rejects sign prefixes")
                    .to_string(),
                format!(
                    "strconv.ParseUint: parsing {:?}: invalid syntax",
                    &source[..2]
                )
            );
        }
        assert_eq!(
            OptimizerFixControl::parse("18446744073709551616x:ON")
                .expect_err("Go reports range as soon as ParseUint crosses uint64")
                .to_string(),
            "strconv.ParseUint: parsing \"18446744073709551616x\": value out of range"
        );
    }

    #[test]
    fn diagnostics_use_go_string_quoting() {
        assert_eq!(
            OptimizerFixControl::parse("\0:ON").unwrap_err().to_string(),
            "strconv.ParseUint: parsing \"\\x00\": invalid syntax"
        );
        let (_, warnings) = OptimizerFixControl::parse("1:'\0',1:'\u{7f}'").unwrap();
        assert_eq!(
            warnings,
            ["repeated assignment for fix control: 1. existing value: \"\\x00\". new value: \"\\x7f\"."]
        );
        let (_, warnings) = OptimizerFixControl::parse("1:'\u{85}',1:'\u{200b}'").unwrap();
        assert_eq!(
            warnings,
            ["repeated assignment for fix control: 1. existing value: \"\\u0085\". new value: \"\\u200b\"."]
        );
    }
}
