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

//! Go `strconv.FormatFloat` / `AppendFloat` (`src/internal/strconv/ftoa.go`).
//!
//! Shortest and fixed-width conversions use Go 1.27.4's unrounded scaling.
//! Higher precisions use exact integer scaling or Rust's native formatter.

mod scale;

/// Go `strconv.FormatFloat(value, fmt, prec, bitSize)` for the formats TiDB
/// uses (`'e'`, `'E'`, `'f'`, `'g'`, `'G'`). `bit_size` 32 formats
/// `float32(value)`.
#[must_use]
pub fn format_float(value: f64, fmt: u8, prec: i32, bit_size: u32) -> String {
    let mut out = Vec::with_capacity(24);
    append_float(&mut out, value, fmt, prec, bit_size);
    String::from_utf8(out).expect("formatted floats are ASCII")
}

/// Go `strconv.AppendFloat`.
pub fn append_float(dst: &mut Vec<u8>, value: f64, fmt: u8, prec: i32, bit_size: u32) {
    let value = if bit_size == 32 {
        f64::from(value as f32)
    } else {
        value
    };
    if value.is_nan() {
        dst.extend_from_slice(b"NaN");
        return;
    }
    if value.is_infinite() {
        dst.extend_from_slice(if value > 0.0 { b"+Inf" } else { b"-Inf" });
        return;
    }
    let negative = value.is_sign_negative();
    let prec = if prec == 0 && matches!(fmt, b'g' | b'G') {
        1
    } else {
        prec
    };
    if value == 0.0 {
        let digits = Digits { digits: &[], dp: 0 };
        format_digits(dst, negative, &digits, prec, fmt);
        return;
    }
    let scaled = if prec < 0 {
        Some(scale::shortest(value, bit_size))
    } else {
        scale::convert(value, fmt, prec)
    };
    if let Some((coefficient, exponent)) = scaled {
        let mut buffer = itoa::Buffer::new();
        let digits = Digits::from_coefficient(coefficient, exponent, &mut buffer);
        format_digits(dst, negative, &digits, prec, fmt);
        return;
    }
    append_float_fallback(dst, value, negative, prec, fmt);
}

#[cold]
fn append_float_fallback(dst: &mut Vec<u8>, value: f64, negative: bool, prec: i32, fmt: u8) {
    if fmt == b'f' {
        if let Some(coefficient) = scale::exact_fixed(value, prec) {
            let mut buffer = itoa::Buffer::new();
            let digits = Digits::from_coefficient(coefficient, -prec, &mut buffer);
            fmt_f(dst, negative, &digits, prec);
            return;
        }
        // Go `bigFtoa` rounds to `prec` fraction digits.
        use std::io::Write as _;
        write!(dst, "{value:.p$}", p = prec as usize).expect("writing to Vec cannot fail");
        return;
    }
    if matches!(fmt, b'e' | b'E' | b'g' | b'G')
        && try_append_extended_decimal(dst, value, negative, prec, fmt)
    {
        return;
    }
    let mut width = match fmt {
        b'e' | b'E' => prec,
        b'g' | b'G' => prec - 1,
        _ => 0,
    };
    // Capture all exact digits once, then let fmt_e append padding in bulk.
    if width >= 64 {
        width = width.min(scale::exact_digits_bound(value) - 1);
    }
    let mut fixed = format!("{value:.p$e}", p = width as usize).into_bytes();
    let digits = Digits::parse(&mut fixed);
    format_digits(dst, negative, &digits, prec, fmt);
}

#[cold]
fn try_append_extended_decimal(
    dst: &mut Vec<u8>,
    value: f64,
    negative: bool,
    prec: i32,
    fmt: u8,
) -> bool {
    let (mantissa, exponent) = scale::split_f64(value);
    let trailing = mantissa.trailing_zeros();
    let exponent = exponent + trailing as i32;
    let keep = prec as usize + usize::from(matches!(fmt, b'e' | b'E'));
    // Rust's native formatter is cheaper when the requested prefix is
    // small and computing the full coefficient would dominate.
    if (keep <= 19 && exponent.unsigned_abs() > 64)
        || (exponent < 0 && keep < exponent.unsigned_abs() as usize / 16)
    {
        return false;
    }
    let mut decimal = [0; scale::EXACT_DECIMAL_CAPACITY];
    let (len, dp) = scale::exact_decimal(mantissa >> trailing, exponent, &mut decimal);
    let digits = Digits::round_to(&mut decimal[..len], dp, keep);
    format_digits(dst, negative, &digits, prec, fmt);
    true
}

/// Go `decimalSlice`: the significant digits without leading or trailing
/// zeros, and the decimal point position relative to them.
struct Digits<'a> {
    digits: &'a [u8],
    dp: i32,
}

impl<'a> Digits<'a> {
    fn from_coefficient(
        value: impl itoa::Integer,
        exponent: i32,
        buffer: &'a mut itoa::Buffer,
    ) -> Self {
        let text = buffer.format(value);
        let mut digits = text.as_bytes();
        while let Some((b'0', rest)) = digits.split_last() {
            digits = rest;
        }
        Self {
            digits,
            dp: if digits.is_empty() {
                0
            } else {
                text.len() as i32 + exponent
            },
        }
    }

    fn nd(&self) -> usize {
        self.digits.len()
    }

    fn round_to(exact: &'a mut [u8], mut dp: i32, keep: usize) -> Self {
        let mut len = exact.len().min(keep);
        // The coefficient is nonzero and keep is positive. A first
        // discarded digit of 5 with an otherwise zero tail is a tie.
        if let Some(&discarded) = exact.get(len) {
            let round_up = discarded > b'5'
                || (discarded == b'5'
                    && (exact[len - 1] & 1 != 0
                        || exact[len + 1..].iter().any(|&digit| digit != b'0')));
            if round_up {
                let mut index = len;
                while index != 0 && exact[index - 1] == b'9' {
                    index -= 1;
                }
                if index == 0 {
                    exact[0] = b'1';
                    len = 1;
                    dp += 1;
                } else {
                    exact[index - 1] += 1;
                    len = index;
                }
            }
        }
        while len > 1 && exact[len - 1] == b'0' {
            len -= 1;
        }
        Self {
            digits: &exact[..len],
            dp,
        }
    }

    /// Parses a decimal rendering (`[-]d[.ddd][e±x]`) into significant
    /// digits and a decimal point, trimming zeros as Go's `formatDecimal`
    /// does.
    fn parse(rendered: &'a mut [u8]) -> Self {
        let end = rendered
            .iter()
            .position(|&b| b == b'e' || b == b'E')
            .unwrap_or(rendered.len());
        let exponent = if end == rendered.len() {
            0
        } else {
            std::str::from_utf8(&rendered[end + 1..])
                .unwrap()
                .parse::<i32>()
                .unwrap()
        };
        let start = usize::from(rendered.first() == Some(&b'-'));
        let digits = &mut rendered[start..end];
        let point = digits
            .iter()
            .position(|&b| b == b'.')
            .unwrap_or(digits.len());
        let mut len = digits.len();
        if point < len {
            digits.copy_within(point + 1.., point);
            len -= 1;
        }
        let digits = &digits[..len];
        let Some(first) = digits.iter().position(|&b| b != b'0') else {
            return Self { digits: &[], dp: 0 };
        };
        let last = digits.iter().rposition(|&b| b != b'0').unwrap();
        Self {
            digits: &digits[first..=last],
            dp: point as i32 + exponent - first as i32,
        }
    }
}

/// Go `formatDigits`.
fn format_digits(dst: &mut Vec<u8>, negative: bool, digits: &Digits, mut prec: i32, fmt: u8) {
    let nd = digits.nd() as i32;
    match fmt {
        b'e' | b'E' => {
            if prec < 0 {
                prec = (nd - 1).max(0);
            }
            fmt_e(dst, negative, digits, prec, fmt);
        }
        b'f' => {
            if prec < 0 {
                prec = (nd - digits.dp).max(0);
            }
            fmt_f(dst, negative, digits, prec);
        }
        b'g' | b'G' => {
            // %e is used if the exponent from the conversion is less than -4
            // or greater than or equal to the precision; the shortest form
            // decides with precision 6.
            let eprec = if prec < 0 {
                prec = nd;
                6
            } else if prec > nd && nd >= digits.dp {
                nd
            } else {
                prec
            };
            let exponent = digits.dp - 1;
            if exponent < -4 || exponent >= eprec {
                fmt_e(dst, negative, digits, prec.min(nd) - 1, fmt + b'e' - b'g');
                return;
            }
            if prec > digits.dp {
                prec = nd;
            }
            fmt_f(dst, negative, digits, (prec - digits.dp).max(0));
        }
        _ => {
            dst.push(b'%');
            dst.push(fmt);
        }
    }
}

/// Go `fmtE`: `-d.dddde±dd`.
fn fmt_e(dst: &mut Vec<u8>, negative: bool, digits: &Digits, prec: i32, fmt: u8) {
    if negative {
        dst.push(b'-');
    }
    dst.push(digits.digits.first().copied().unwrap_or(b'0'));
    if prec > 0 {
        dst.push(b'.');
        let mut index = 1;
        let end = digits.nd().min(prec as usize + 1);
        if index < end {
            dst.extend_from_slice(&digits.digits[index..end]);
            index = end;
        }
        dst.resize(dst.len() + (prec as usize + 1 - index), b'0');
    }
    dst.push(fmt);
    let mut exponent = if digits.nd() == 0 { 0 } else { digits.dp - 1 };
    if exponent < 0 {
        dst.push(b'-');
        exponent = -exponent;
    } else {
        dst.push(b'+');
    }
    if exponent >= 100 {
        dst.push(b'0' + (exponent / 100) as u8);
    }
    dst.push(b'0' + (exponent / 10 % 10) as u8);
    dst.push(b'0' + (exponent % 10) as u8);
}

/// Go `fmtF`: `-ddd.ddd`.
fn fmt_f(dst: &mut Vec<u8>, negative: bool, digits: &Digits, prec: i32) {
    if negative {
        dst.push(b'-');
    }
    if digits.dp > 0 {
        let width = digits.dp as usize;
        let integer = digits.nd().min(width);
        dst.extend_from_slice(&digits.digits[..integer]);
        if width > integer {
            dst.resize(dst.len() + width - integer, b'0');
        }
    } else {
        dst.push(b'0');
    }
    if prec > 0 {
        dst.push(b'.');
        let zeros = digits.dp.saturating_neg().clamp(0, prec) as usize;
        if zeros > 0 {
            dst.resize(dst.len() + zeros, b'0');
        }
        let remaining = prec as usize - zeros;
        let start = (digits.dp.max(0) as usize).min(digits.nd());
        let count = remaining.min(digits.nd() - start);
        dst.extend_from_slice(&digits.digits[start..start + count]);
        if remaining > count {
            dst.resize(dst.len() + remaining - count, b'0');
        }
    }
}

/// Why Go `strconv.ParseFloat` refused its input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParseFloatError {
    /// `strconv.ErrSyntax`.
    Syntax,
    /// `strconv.ErrRange`: a well-formed value beyond the float64 range.
    Range,
}

/// Go `strconv.ParseFloat(text, 64)`, including decimal and hexadecimal
/// literals with digit separators, signed infinity, and case-insensitive NaN.
pub fn parse_float64(text: &str) -> Result<f64, ParseFloatError> {
    let (negative, body) = match text.as_bytes().first() {
        Some(b'+') => (false, &text[1..]),
        Some(b'-') => (true, &text[1..]),
        _ => (false, text),
    };
    match body.as_bytes() {
        [b'0', b'x' | b'X', digits @ ..] => parse_hex_float(digits, negative),
        [b'0'..=b'9' | b'.', ..] => parse_decimal_float(text),
        [b'i' | b'I', ..]
            if body.eq_ignore_ascii_case("inf") || body.eq_ignore_ascii_case("infinity") =>
        {
            Ok(if negative {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            })
        }
        [b'n' | b'N', ..] if body.len() == text.len() && body.eq_ignore_ascii_case("nan") => {
            Ok(f64::from_bits(0x7ff8_0000_0000_0001))
        }
        _ => Err(ParseFloatError::Syntax),
    }
}

fn parse_decimal_float(text: &str) -> Result<f64, ParseFloatError> {
    let value = if text.len() > GO_DECIMAL_DIGITS {
        parse_long_decimal(text)?
    } else {
        parse_decimal_value(text)?
    };
    if value.is_infinite() {
        return Err(ParseFloatError::Range);
    }
    Ok(value)
}

fn parse_decimal_value(text: &str) -> Result<f64, ParseFloatError> {
    text.parse::<f64>()
        .or_else(|_| parse_separated_decimal(text))
}

/// The capacity of Go's decimal buffer in the exact-rounding fallback.
const GO_DECIMAL_DIGITS: usize = 800;

#[cold]
fn parse_long_decimal(text: &str) -> Result<f64, ParseFloatError> {
    let Some(position) = text.find('e').or_else(|| text.find('E')) else {
        return parse_decimal_value(text);
    };
    let mantissa = &text[..position];
    let exponent_text = &text[position + 1..];
    let (exponent, separated) = parse_float_exponent(exponent_text.as_bytes())?;
    // Go caps the exponent at the first four or five significant digits
    // once it reaches 10,000. Only a very long mantissa can counteract it.
    let value = if text.len() >= 9_000 && exponent_text.len() > 5 {
        if separated && !valid_digit_separators(text.as_bytes(), false) {
            return Err(ParseFloatError::Syntax);
        }
        parse_decimal_value(&format!("{mantissa}e{exponent}"))?
    } else {
        parse_decimal_value(text)?
    };
    if exponent >= -400 {
        return Ok(value);
    }
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let integer = integer.trim_start_matches(['+', '-', '0', '_']);
    let integer_digits = integer.len() - integer.bytes().filter(|&byte| byte == b'_').count();
    if integer_digits <= GO_DECIMAL_DIGITS {
        return Ok(value);
    }
    let mut digits = integer
        .bytes()
        .chain(fraction.bytes())
        .filter(u8::is_ascii_digit);
    let leading = digits
        .by_ref()
        .take(19)
        .fold(0u64, |value, digit| value * 10 + u64::from(digit - b'0'));
    let power = exponent + (integer_digits - 19) as i64;
    if !(-345..=310).contains(&power) || !digits.any(|digit| digit != b'0') {
        return Ok(value);
    }
    // Go takes the slow decimal path only when the first 19 digits fail
    // to determine the rounded value. That path caps the decimal point at
    // the buffer length. Keeping the extra digits preserves its sticky bit.
    if parse_decimal_value(&format!("{leading}e{power}"))?
        == parse_decimal_value(&format!("{}e{power}", leading + 1))?
    {
        return Ok(value);
    }
    let adjusted = exponent.saturating_sub((integer_digits - GO_DECIMAL_DIGITS) as i64);
    parse_decimal_value(&format!("{mantissa}e{adjusted}"))
}

fn parse_separated_decimal(text: &str) -> Result<f64, ParseFloatError> {
    let bytes = text.as_bytes();
    let first = bytes
        .iter()
        .position(|&byte| byte == b'_')
        .ok_or(ParseFloatError::Syntax)?;
    let mut scratch = [0; 64];
    let owned;
    let normalized = if bytes.len() <= scratch.len() {
        scratch[..first].copy_from_slice(&bytes[..first]);
        let mut len = first;
        for (index, &byte) in bytes.iter().enumerate().skip(first) {
            if byte == b'_' {
                if index == 0
                    || !bytes[index - 1].is_ascii_digit()
                    || !bytes.get(index + 1).is_some_and(u8::is_ascii_digit)
                {
                    return Err(ParseFloatError::Syntax);
                }
            } else {
                scratch[len] = byte;
                len += 1;
            }
        }
        std::str::from_utf8(&scratch[..len]).expect("removing ASCII separators preserves UTF-8")
    } else {
        if !valid_digit_separators(bytes, false) {
            return Err(ParseFloatError::Syntax);
        }
        owned = text.replace('_', "");
        &owned
    };
    normalized.parse().map_err(|_| ParseFloatError::Syntax)
}

fn valid_digit_separators(bytes: &[u8], hexadecimal: bool) -> bool {
    let is_digit = |byte: &u8| {
        if hexadecimal {
            byte.is_ascii_hexdigit()
        } else {
            byte.is_ascii_digit()
        }
    };
    for (index, &byte) in bytes.iter().enumerate() {
        if byte != b'_' {
            continue;
        }
        // The base prefix also counts as a preceding digit.
        let after_digit = if index == 0 {
            hexadecimal
        } else {
            is_digit(&bytes[index - 1])
        };
        if !after_digit || !bytes.get(index + 1).is_some_and(is_digit) {
            return false;
        }
    }
    true
}

/// The hexadecimal path of Go `readFloat` and `atofHex`.
fn parse_hex_float(hex: &[u8], negative: bool) -> Result<f64, ParseFloatError> {
    let mut mantissa = 0;
    let mut digits = 0i64;
    let mut fraction_digits = 0i64;
    let mut seen_point = false;
    let mut seen_digit = false;
    let mut separator = false;
    let mut sticky = false;
    for (index, &byte) in hex.iter().enumerate() {
        let digit = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            b'.' if !seen_point => {
                seen_point = true;
                continue;
            }
            b'_' => {
                separator = true;
                continue;
            }
            b'p' | b'P' if seen_digit => {
                let (exponent, exponent_separator) = parse_float_exponent(&hex[index + 1..])?;
                if (separator || exponent_separator) && !valid_digit_separators(hex, true) {
                    return Err(ParseFloatError::Syntax);
                }
                let scale = (digits - digits.min(16) - fraction_digits)
                    .saturating_mul(4)
                    .saturating_add(exponent);
                return round_hex_float(mantissa, scale, negative, sticky);
            }
            _ => return Err(ParseFloatError::Syntax),
        };
        seen_digit = true;
        fraction_digits += i64::from(seen_point);
        if mantissa != 0 || digit != 0 {
            if digits < 16 {
                mantissa = (mantissa << 4) | u64::from(digit);
            } else {
                sticky |= digit != 0;
            }
            digits += 1;
        }
    }
    Err(ParseFloatError::Syntax)
}

fn parse_float_exponent(bytes: &[u8]) -> Result<(i64, bool), ParseFloatError> {
    let (negative, digits) = match bytes {
        [b'-', digits @ ..] => (true, digits),
        [b'+', digits @ ..] => (false, digits),
        _ => (false, bytes),
    };
    if !digits.first().is_some_and(u8::is_ascii_digit) {
        return Err(ParseFloatError::Syntax);
    }
    let mut exponent = 0;
    let mut separator = false;
    for &byte in digits {
        match byte {
            b'0'..=b'9' => {
                if exponent < 10_000 {
                    exponent = 10 * exponent + i64::from(byte - b'0');
                }
            }
            b'_' => separator = true,
            _ => return Err(ParseFloatError::Syntax),
        }
    }
    Ok((if negative { -exponent } else { exponent }, separator))
}

fn round_hex_float(
    mantissa: u64,
    scale: i64,
    negative: bool,
    sticky: bool,
) -> Result<f64, ParseFloatError> {
    const FRACTION_BITS: i64 = 52;
    const MIN_EXPONENT: i64 = -1022;
    const MAX_EXPONENT: i64 = 1023;
    const IMPLICIT_BIT: u64 = 1 << FRACTION_BITS;
    let sign = u64::from(negative) << 63;
    if mantissa == 0 {
        return Ok(f64::from_bits(sign));
    }
    let highest_bit = i64::from(63 - mantissa.leading_zeros());
    let mut exponent = scale.saturating_add(highest_bit).max(MIN_EXPONENT);
    if exponent > MAX_EXPONENT {
        return Err(ParseFloatError::Range);
    }
    let shift = (exponent - FRACTION_BITS).saturating_sub(scale);
    // Round once, even when the exponent requires a subnormal value. The
    // sticky bit represents nonzero data beyond the first 16 hex digits.
    let mut significand = if shift <= 0 {
        mantissa << -shift
    } else if shift <= 64 {
        let half = 1 << (shift - 1);
        let remainder = mantissa & (half | (half - 1));
        let kept = mantissa.checked_shr(shift as u32).unwrap_or(0);
        kept + u64::from(remainder > half || (remainder == half && (sticky || kept & 1 != 0)))
    } else {
        0
    };
    if significand == IMPLICIT_BIT << 1 {
        significand >>= 1;
        exponent += 1;
    }
    if exponent > MAX_EXPONENT {
        return Err(ParseFloatError::Range);
    }
    let biased = if significand < IMPLICIT_BIT {
        0
    } else {
        (exponent + MAX_EXPONENT) as u64
    };
    Ok(f64::from_bits(
        sign | (biased << FRACTION_BITS) | (significand & (IMPLICIT_BIT - 1)),
    ))
}

#[cfg(test)]
mod tests {
    use super::{format_float, parse_float64, ParseFloatError};

    #[test]
    fn shortest_forms_match_go() {
        // An exact tie between two 17-digit candidates rounds to even.
        assert_eq!(
            format_float(f64::from(-111.111_111_11_f32), b'f', -1, 64),
            "-111.11111450195312"
        );
        assert_eq!(format_float(123.45, b'f', -1, 64), "123.45");
        assert_eq!(format_float(1e21, b'f', -1, 64), "1000000000000000000000");
        assert_eq!(format_float(0.0, b'f', -1, 64), "0");
        assert_eq!(format_float(-0.0, b'f', -1, 64), "-0");
        assert_eq!(format_float(0.000_012_5, b'f', -1, 64), "0.0000125");
        assert_eq!(format_float(1e21, b'e', -1, 64), "1e+21");
        assert_eq!(format_float(5e-324, b'e', -1, 64), "5e-324");
        assert_eq!(format_float(123_456_789.0, b'g', -1, 64), "1.23456789e+08");
        assert_eq!(format_float(100_000.0, b'g', -1, 64), "100000");
        assert_eq!(format_float(0.0001, b'g', -1, 64), "0.0001");
        assert_eq!(format_float(0.00001, b'g', -1, 64), "1e-05");
        assert_eq!(
            format_float(3.4e38, b'f', -1, 32),
            "340000000000000000000000000000000000000"
        );
        assert_eq!(format_float(0.1, b'f', -1, 32), "0.1");
        assert_eq!(format_float(f64::NAN, b'f', -1, 64), "NaN");
        assert_eq!(format_float(f64::NEG_INFINITY, b'g', -1, 64), "-Inf");
    }

    #[test]
    fn extended_scientific_rounds_exact_halfway_cases() {
        let unit = 1.0 / 4_294_967_296.0;
        assert_eq!(
            format_float(unit, b'e', 21, 64),
            "2.328306436538696289062e-10"
        );
        assert_eq!(
            format_float(-3.0 * unit, b'E', 21, 64),
            "-6.984919309616088867188E-10"
        );

        let mut coefficient = *b"99950";
        let rounded = super::Digits::round_to(&mut coefficient, 3, 3);
        assert_eq!(rounded.digits, b"1");
        assert_eq!(rounded.dp, 4);
    }

    #[test]
    fn fixed_precision_rounds_half_to_even() {
        assert_eq!(format_float(2.5, b'f', 0, 64), "2");
        assert_eq!(format_float(0.125, b'f', 2, 64), "0.12");
        assert_eq!(format_float(1234.5, b'f', 2, 64), "1234.50");
        assert_eq!(format_float(1.0625, b'e', 3, 64), "1.062e+00");
        assert_eq!(format_float(123_456.0, b'e', 2, 64), "1.23e+05");
        assert_eq!(format_float(100.0, b'g', 3, 64), "100");
        assert_eq!(format_float(1234.0, b'g', 3, 64), "1.23e+03");
        assert_eq!(format_float(0.0, b'e', 2, 64), "0.00e+00");
        assert_eq!(format_float(-0.0, b'G', 0, 64), "-0");
        assert_eq!(format_float(1.2345e120, b'e', 3, 64), "1.235e+120");
        assert_eq!(
            format_float(f64::from_bits(1), b'e', 17, 64),
            "4.94065645841246544e-324",
        );
        assert_eq!(
            format_float(1234.5678987, b'f', 18, 64),
            "1234.567898699999886958",
        );
        assert_eq!(
            format_float(1.25, b'e', 1000, 64),
            format!("1.25{}e+00", "0".repeat(998)),
        );
    }

    #[test]
    fn parse_float64_follows_go_syntax_and_range() {
        assert_eq!(parse_float64("1.5"), Ok(1.5));
        assert_eq!(parse_float64("-.5e1"), Ok(-5.0));
        assert_eq!(parse_float64("0x1p-1"), Ok(0.5));
        assert_eq!(parse_float64("1e400"), Err(ParseFloatError::Range));
        assert_eq!(parse_float64("Inf"), Ok(f64::INFINITY));
        assert_eq!(parse_float64("1_0"), Ok(10.0));
        assert_eq!(parse_float64("1e1_0"), Ok(1e10));
        assert_eq!(parse_float64("1_e0"), Err(ParseFloatError::Syntax));
        assert_eq!(parse_float64("+NaN"), Err(ParseFloatError::Syntax));
        assert_eq!(
            parse_float64("nan").unwrap().to_bits(),
            0x7ff8_0000_0000_0001
        );
        assert_eq!(parse_float64(""), Err(ParseFloatError::Syntax));
        assert_eq!(parse_float64("1e"), Err(ParseFloatError::Syntax));
    }

    #[test]
    fn hexadecimal_float_rounds_once_and_validates_separators() {
        for (literal, bits) in [
            ("0x1p-1074", 1),
            ("-0x1p-1075", 1 << 63),
            ("0x1.00000000000000001p-1075", 1),
            ("0x1.00000000000008p0", 0x3ff0_0000_0000_0000),
            ("0x1.000000000000080000001p0", 0x3ff0_0000_0000_0001),
            ("0x0.fffffffffffff8p-1022", 0x0010_0000_0000_0000),
            ("0x_1_0p-1_0", 0x3f90_0000_0000_0000),
            ("-0x0p999999999999999999999999", 1 << 63),
        ] {
            assert_eq!(parse_float64(literal).unwrap().to_bits(), bits, "{literal}");
        }
        assert_eq!(parse_float64("0x1__2p1"), Err(ParseFloatError::Syntax));
        assert_eq!(parse_float64("0x1p_2"), Err(ParseFloatError::Syntax));
        assert_eq!(parse_float64("0x1p1_"), Err(ParseFloatError::Syntax));
        assert_eq!(
            parse_float64("0x1.fffffffffffff8p1023"),
            Err(ParseFloatError::Range)
        );
    }

    #[test]
    fn wide_decimal_uses_go_exponent_and_rounding_limits() {
        let compensated = format!("0.{}1e100000", "0".repeat(9_999));
        assert_eq!(parse_float64(&compensated), Ok(1.0));
        let underflow = format!("0.{}1e100_000", "0".repeat(99_999));
        assert_eq!(parse_float64(&underflow), Ok(0.0));

        let halfway = "100000000000000011102230246251565404236316680908203125";
        let at_limit = format!("{halfway:0<800}e-799");
        let past_limit = format!("{halfway:0<801}e-800");
        assert_eq!(parse_float64(&at_limit), Ok(1.0));
        assert_eq!(parse_float64(&past_limit), Ok(0.1));
    }
}
