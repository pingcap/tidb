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

//! Go `strconv.FormatFloat` / `AppendFloat` (`src/strconv/ftoa.go`).
//!
//! The shortest form (precision -1) takes its digits from Ryu, the algorithm
//! Go's `ryuFtoaShortest` implements. Rust's own `Display` also prints a
//! shortest round-trip form, but breaks an exact tie between two candidates
//! the other way: `float64(float32(-111.11111111))` is
//! -111.111114501953125, which Go prints `-111.11111450195312` and Rust's
//! `Display` `-111.11111450195313`. A fixed precision rounds the exact binary
//! value half to even in both languages, so those digits come from Rust's
//! exact formatter.

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
    let shortest = prec < 0;
    let mut prec = prec;
    let digits = if shortest {
        let digits = shortest_digits(value, bit_size);
        prec = match fmt {
            b'e' | b'E' => (digits.nd() as i32 - 1).max(0),
            b'f' => (digits.nd() as i32 - digits.dp).max(0),
            _ => digits.nd() as i32,
        };
        digits
    } else if fmt == b'f' {
        // Go `bigFtoa` rounds to `prec` fraction digits.
        dst.extend_from_slice(format!("{value:.p$}", p = prec as usize).as_bytes());
        return;
    } else {
        let count = match fmt {
            b'e' | b'E' => prec + 1,
            b'g' | b'G' => {
                if prec == 0 {
                    prec = 1;
                }
                prec
            }
            _ => 1,
        };
        fixed_digits(value, count.max(1) as usize)
    };
    format_digits(dst, shortest, negative, &digits, prec, fmt);
}

/// Go `decimalSlice`: the significant digits without leading or trailing
/// zeros, and the decimal point position relative to them.
struct Digits {
    digits: Vec<u8>,
    dp: i32,
}

impl Digits {
    fn nd(&self) -> usize {
        self.digits.len()
    }

    /// Parses a decimal rendering (`[-]d[.ddd][e±x]`) into significant
    /// digits and a decimal point, trimming zeros as Go's `formatDecimal`
    /// does.
    fn parse(rendered: &str) -> Self {
        let rendered = rendered.trim_start_matches('-');
        let (mantissa, exponent) = rendered
            .split_once(['e', 'E'])
            .map_or((rendered, 0), |(mantissa, exponent)| {
                (mantissa, exponent.parse::<i32>().expect("float exponent"))
            });
        let point = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
        let all: Vec<u8> = mantissa.bytes().filter(|byte| *byte != b'.').collect();
        let Some(first) = all.iter().position(|digit| *digit != b'0') else {
            return Self {
                digits: Vec::new(),
                dp: 0,
            };
        };
        let last = all
            .iter()
            .rposition(|digit| *digit != b'0')
            .expect("a nonzero digit exists");
        Self {
            digits: all[first..=last].to_vec(),
            dp: point + exponent - first as i32,
        }
    }
}

/// Go `ryuFtoaShortest`.
fn shortest_digits(value: f64, bit_size: u32) -> Digits {
    if value == 0.0 {
        return Digits {
            digits: Vec::new(),
            dp: 0,
        };
    }
    let mut buffer = ryu::Buffer::new();
    let rendered = if bit_size == 32 {
        buffer.format_finite(value as f32)
    } else {
        buffer.format_finite(value)
    };
    Digits::parse(rendered)
}

/// Go `ryuFtoaFixed64` / `bigFtoa` for `count` significant digits.
fn fixed_digits(value: f64, count: usize) -> Digits {
    Digits::parse(&format!("{value:.p$e}", p = count - 1))
}

/// Go `formatDigits`.
fn format_digits(
    dst: &mut Vec<u8>,
    shortest: bool,
    negative: bool,
    digits: &Digits,
    prec: i32,
    fmt: u8,
) {
    match fmt {
        b'e' | b'E' => fmt_e(dst, negative, digits, prec, fmt),
        b'f' => fmt_f(dst, negative, digits, prec),
        b'g' | b'G' => {
            let mut eprec = prec;
            if eprec > digits.nd() as i32 && digits.nd() as i32 >= digits.dp {
                eprec = digits.nd() as i32;
            }
            // %e is used if the exponent from the conversion is less than -4
            // or greater than or equal to the precision; the shortest form
            // decides with precision 6.
            if shortest {
                eprec = 6;
            }
            let exponent = digits.dp - 1;
            let mut prec = prec;
            if exponent < -4 || exponent >= eprec {
                if prec > digits.nd() as i32 {
                    prec = digits.nd() as i32;
                }
                fmt_e(dst, negative, digits, prec - 1, fmt + b'e' - b'g');
                return;
            }
            if prec > digits.dp {
                prec = digits.nd() as i32;
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
        while index <= prec as usize {
            dst.push(b'0');
            index += 1;
        }
    }
    dst.push(fmt);
    let mut exponent = if digits.nd() == 0 { 0 } else { digits.dp - 1 };
    if exponent < 0 {
        dst.push(b'-');
        exponent = -exponent;
    } else {
        dst.push(b'+');
    }
    if exponent < 10 {
        dst.push(b'0');
    }
    dst.extend_from_slice(exponent.to_string().as_bytes());
}

/// Go `fmtF`: `-ddd.ddd`.
fn fmt_f(dst: &mut Vec<u8>, negative: bool, digits: &Digits, prec: i32) {
    if negative {
        dst.push(b'-');
    }
    if digits.dp > 0 {
        let integer = digits.nd().min(digits.dp as usize);
        dst.extend_from_slice(&digits.digits[..integer]);
        dst.extend(std::iter::repeat_n(b'0', digits.dp as usize - integer));
    } else {
        dst.push(b'0');
    }
    if prec > 0 {
        dst.push(b'.');
        for index in 0..prec {
            let position = digits.dp + index;
            dst.push(if position >= 0 && (position as usize) < digits.nd() {
                digits.digits[position as usize]
            } else {
                b'0'
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::format_float;

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
    fn fixed_precision_rounds_half_to_even() {
        assert_eq!(format_float(2.5, b'f', 0, 64), "2");
        assert_eq!(format_float(0.125, b'f', 2, 64), "0.12");
        assert_eq!(format_float(1234.5, b'f', 2, 64), "1234.50");
        assert_eq!(format_float(1.0625, b'e', 3, 64), "1.062e+00");
        assert_eq!(format_float(123_456.0, b'e', 2, 64), "1.23e+05");
        assert_eq!(format_float(100.0, b'g', 3, 64), "100");
        assert_eq!(format_float(1234.0, b'g', 3, 64), "1.23e+03");
        assert_eq!(format_float(0.0, b'e', 2, 64), "0.00e+00");
    }
}
