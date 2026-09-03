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

//! Transcreation of Go `pkg/config/configtypes`: the `ByteSize` and
//! `Duration` wrappers used by TOML/JSON config fields.
//!
//! `ByteSize` keeps `docker/go-units` semantics — `BytesSize` renders with
//! binary units and 4 significant digits, `RAMInBytes` parses both `KB` and
//! `KiB` style suffixes as powers of 1024. `Duration` keeps Go
//! `time.ParseDuration`/`Duration.String()` semantics exactly.

use std::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A byte size for TOML and JSON (Go `ByteSize`, a `uint64`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, PartialOrd, Ord)]
pub struct ByteSize(pub u64);

const BINARY_ABBRS: [&str; 9] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"];

/// go-units `BytesSize`: binary units, `%.4g` significand.
fn bytes_size(mut size: f64) -> String {
    let mut i = 0;
    while size >= 1024.0 && i < BINARY_ABBRS.len() - 1 {
        size /= 1024.0;
        i += 1;
    }
    format!("{}{}", format_g4(size), BINARY_ABBRS[i])
}

// Go `%.4g`: 4 significant digits, trailing zeros trimmed (the post-division
// value is always in [0, 1024), so %e-style output never triggers).
fn format_g4(v: f64) -> String {
    let exp = if v == 0.0 {
        0
    } else {
        v.abs().log10().floor() as i32
    };
    let decimals = (4 - 1 - exp).max(0) as usize;
    let mut s = format!("{v:.decimals$}");
    if s.contains('.') {
        s = s.trim_end_matches('0').trim_end_matches('.').to_string();
    }
    s
}

/// go-units `RAMInBytes`: parses a human size with binary units (both `KB`
/// and `KiB` mean 1024).
pub fn ram_in_bytes(size: &str) -> Result<i64, String> {
    // docker/go-units v0.5.0 `parseSize`: the numeric prefix is everything
    // through the last digit, period, or separating space. In particular,
    // this deliberately accepts the exponent and leading-plus forms accepted
    // by strconv.ParseFloat.
    let bytes = size.as_bytes();
    let Some(separator) = bytes
        .iter()
        .rposition(|byte| byte.is_ascii_digit() || *byte == b'.' || *byte == b' ')
    else {
        return Err(format!("invalid size: '{size}'"));
    };
    let (number, suffix) = if bytes[separator] == b' ' {
        (&size[..separator], &size[separator + 1..])
    } else {
        (&size[..=separator], &size[separator + 1..])
    };
    let mut value = parse_go_float(number).map_err(|_| format!("invalid size: '{size}'"))?;
    if !value.is_finite() || value < 0.0 {
        return Err(format!("invalid size: '{size}'"));
    }
    if suffix.is_empty() {
        return Ok(go_float_to_i64(value));
    }

    let suffix = suffix.to_ascii_lowercase();
    let suffix = suffix.as_bytes();
    if suffix.len() > 3 {
        return Err(format!(
            "invalid suffix: '{}'",
            String::from_utf8_lossy(suffix)
        ));
    }
    if suffix[0] == b'b' {
        if suffix.len() == 1 {
            return Ok(go_float_to_i64(value));
        }
        return Err(format!(
            "invalid suffix: '{}'",
            String::from_utf8_lossy(suffix)
        ));
    }
    let multiplier = match suffix[0] {
        b'k' => 1u64 << 10,
        b'm' => 1u64 << 20,
        b'g' => 1u64 << 30,
        b't' => 1u64 << 40,
        b'p' => 1u64 << 50,
        _ => {
            return Err(format!(
                "invalid suffix: '{}'",
                String::from_utf8_lossy(suffix)
            ))
        }
    };
    if (suffix.len() == 2 && suffix[1] != b'b') || (suffix.len() == 3 && &suffix[1..] != b"ib") {
        return Err(format!(
            "invalid suffix: '{}'",
            String::from_utf8_lossy(suffix)
        ));
    }
    value *= multiplier as f64;
    Ok(go_float_to_i64(value))
}

/// Parses the decimal and hexadecimal floating-point forms accepted by Go's
/// `strconv.ParseFloat`. Rust's standard `f64` parser does not accept the
/// hexadecimal or digit-separator forms that `docker/go-units` inherits.
fn parse_go_float(input: &str) -> Result<f64, ()> {
    let input = strip_go_float_underscores(input)?;
    let body_start = usize::from(input.starts_with(['+', '-']));
    let body = &input[body_start..];
    if body.len() >= 2 && body.as_bytes()[0] == b'0' && matches!(body.as_bytes()[1], b'x' | b'X') {
        parse_hex_float(&input)
    } else {
        input.parse::<f64>().map_err(|_| ())
    }
}

/// Removes Go numeric separators while enforcing their placement rules.
fn strip_go_float_underscores(input: &str) -> Result<String, ()> {
    if !input.contains('_') {
        return Ok(input.to_owned());
    }
    let bytes = input.as_bytes();
    let sign_len = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let is_hex = bytes.len() >= sign_len + 2
        && bytes[sign_len] == b'0'
        && matches!(bytes[sign_len + 1], b'x' | b'X');
    let prefix_end = sign_len + 2;
    let mut output = String::with_capacity(input.len());
    for (index, &byte) in bytes.iter().enumerate() {
        if byte != b'_' {
            output.push(byte as char);
            continue;
        }
        let Some(&next) = bytes.get(index + 1) else {
            return Err(());
        };
        let previous = bytes[index.saturating_sub(1)];
        let valid = if is_hex && index == prefix_end {
            next.is_ascii_hexdigit()
        } else if is_hex {
            previous.is_ascii_hexdigit() && next.is_ascii_hexdigit()
        } else {
            previous.is_ascii_digit() && next.is_ascii_digit()
        };
        if !valid {
            return Err(());
        }
    }
    Ok(output)
}

/// Parses a Go hexadecimal floating-point literal (`0x1.8p+1`).
fn parse_hex_float(input: &str) -> Result<f64, ()> {
    let sign_len = usize::from(input.starts_with(['+', '-']));
    let negative = input.as_bytes().first() == Some(&b'-');
    let body = &input[sign_len + 2..];
    let exponent_start = body.find(['p', 'P']).ok_or(())?;
    let (mantissa, exponent) = body.split_at(exponent_start);
    let exponent = exponent
        .get(1..)
        .ok_or(())?
        .parse::<i32>()
        .map_err(|_| ())?;
    if mantissa.is_empty() {
        return Err(());
    }

    let mut value = 0.0;
    let mut fractional_digits = 0i32;
    let mut after_dot = false;
    let mut digit_count = 0;
    for byte in mantissa.bytes() {
        if byte == b'.' {
            if after_dot {
                return Err(());
            }
            after_dot = true;
            continue;
        }
        let digit = match byte {
            b'0'..=b'9' => f64::from(byte - b'0'),
            b'a'..=b'f' => f64::from(byte - b'a' + 10),
            b'A'..=b'F' => f64::from(byte - b'A' + 10),
            _ => return Err(()),
        };
        value = value * 16.0 + digit;
        digit_count += 1;
        if after_dot {
            fractional_digits += 1;
        }
    }
    if digit_count == 0 {
        return Err(());
    }
    value *= 16.0f64.powi(-fractional_digits);
    value *= 2.0f64.powi(exponent);
    if !value.is_finite() {
        return Err(());
    }
    Ok(if negative { -value } else { value })
}

fn go_float_to_i64(value: f64) -> i64 {
    if value >= 9_223_372_036_854_775_808.0 {
        i64::MIN
    } else {
        value as i64
    }
}

impl Serialize for ByteSize {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&bytes_size(self.0 as f64))
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<ByteSize, D::Error> {
        let s = String::deserialize(deserializer)?;
        let v = ram_in_bytes(&s).map_err(D::Error::custom)?;
        Ok(ByteSize(v as u64))
    }
}

/// A duration wrapper for TOML and JSON (Go `Duration` wrapping
/// `time.Duration`); the inner value is nanoseconds.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, PartialOrd, Ord)]
pub struct Duration(pub i64);

/// Go `time.Duration.String()`.
fn format_go_duration(d: i64) -> String {
    if d == 0 {
        return "0s".to_string();
    }
    let neg = d < 0;
    let u = d.unsigned_abs();
    let mut out = String::new();

    if u < 1_000_000_000 {
        // Sub-second: ns, µs, or ms with fractional part.
        let (unit, prec): (&str, u32) = if u < 1_000 {
            ("ns", 0)
        } else if u < 1_000_000 {
            ("µs", 3)
        } else {
            ("ms", 6)
        };
        let scale = 10u64.pow(prec);
        let int = u / scale;
        let frac = u % scale;
        out.push_str(&int.to_string());
        if frac != 0 {
            let f = format!("{frac:0width$}", width = prec as usize);
            let f = f.trim_end_matches('0');
            out.push('.');
            out.push_str(f);
        }
        out.push_str(unit);
    } else {
        let frac = u % 1_000_000_000;
        let mut secs = u / 1_000_000_000;
        let mut sec_part = (secs % 60).to_string();
        if frac != 0 {
            let f = format!("{frac:09}");
            sec_part.push('.');
            sec_part.push_str(f.trim_end_matches('0'));
        }
        secs /= 60; // now minutes
        let mins = secs % 60;
        let hours = secs / 60;
        if hours != 0 {
            out.push_str(&hours.to_string());
            out.push('h');
        }
        if hours != 0 || mins != 0 {
            out.push_str(&mins.to_string());
            out.push('m');
        }
        out.push_str(&sec_part);
        out.push('s');
    }
    if neg {
        format!("-{out}")
    } else {
        out
    }
}

/// Go `time.ParseDuration`.
fn parse_go_duration(s: &str) -> Result<i64, String> {
    let orig = s;
    let mut s = s;
    let mut neg = false;
    if let Some(rest) = s.strip_prefix(['-', '+']) {
        neg = orig.starts_with('-');
        s = rest;
    }
    if s == "0" {
        return Ok(0);
    }
    if s.is_empty() {
        return Err(format!("time: invalid duration {orig:?}"));
    }
    let mut duration = 0u64;
    while !s.is_empty() {
        if !matches!(s.as_bytes()[0], b'.' | b'0'..=b'9') {
            return Err(format!("time: invalid duration {orig:?}"));
        }
        let (integer, rest) =
            leading_int(s).ok_or_else(|| format!("time: invalid duration {orig:?}"))?;
        let has_integer = rest.len() != s.len();
        s = rest;

        let mut fraction = 0u64;
        let mut scale = 1f64;
        let mut has_fraction = false;
        if let Some(rest) = s.strip_prefix('.') {
            let (value, value_scale, remaining) = leading_fraction(rest);
            fraction = value;
            scale = value_scale;
            has_fraction = remaining.len() != rest.len();
            s = remaining;
        }
        if !has_integer && !has_fraction {
            return Err(format!("time: invalid duration {orig:?}"));
        }

        let unit_len = s
            .char_indices()
            .find(|(_, c)| c.is_ascii_digit() || *c == '.')
            .map(|(i, _)| i)
            .unwrap_or(s.len());
        let (unit_str, rest) = s.split_at(unit_len);
        s = rest;
        let unit: u64 = match unit_str {
            "ns" => 1,
            "us" | "µs" | "μs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            "" => return Err(format!("time: missing unit in duration {orig:?}")),
            _ => {
                return Err(format!(
                    "time: unknown unit {unit_str:?} in duration {orig:?}"
                ))
            }
        };
        if integer > (1u64 << 63) / unit {
            return Err(format!("time: invalid duration {orig:?}"));
        }
        let mut value = integer * unit;
        if fraction > 0 {
            value += (fraction as f64 * (unit as f64 / scale)) as u64;
            if value > 1u64 << 63 {
                return Err(format!("time: invalid duration {orig:?}"));
            }
        }
        duration = duration
            .checked_add(value)
            .filter(|value| *value <= 1u64 << 63)
            .ok_or_else(|| format!("time: invalid duration {orig:?}"))?;
    }
    if neg {
        Ok(duration.wrapping_neg() as i64)
    } else if duration > i64::MAX as u64 {
        Err(format!("time: invalid duration {orig:?}"))
    } else {
        Ok(duration as i64)
    }
}

fn leading_int(value: &str) -> Option<(u64, &str)> {
    let mut parsed = 0u64;
    let mut length = 0;
    for byte in value.bytes() {
        if !byte.is_ascii_digit() {
            break;
        }
        if parsed > (1u64 << 63) / 10 {
            return None;
        }
        parsed = parsed * 10 + u64::from(byte - b'0');
        if parsed > 1u64 << 63 {
            return None;
        }
        length += 1;
    }
    Some((parsed, &value[length..]))
}

fn leading_fraction(value: &str) -> (u64, f64, &str) {
    let mut parsed = 0u64;
    let mut scale = 1f64;
    let mut overflow = false;
    let mut length = 0;
    for byte in value.bytes() {
        if !byte.is_ascii_digit() {
            break;
        }
        length += 1;
        if overflow {
            continue;
        }
        if parsed > (i64::MAX as u64) / 10 {
            overflow = true;
            continue;
        }
        let next = parsed * 10 + u64::from(byte - b'0');
        if next > 1u64 << 63 {
            overflow = true;
            continue;
        }
        parsed = next;
        scale *= 10.0;
    }
    (parsed, scale, &value[length..])
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&format_go_duration(self.0))
    }
}

impl Serialize for Duration {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format_go_duration(self.0))
    }
}

impl<'de> Deserialize<'de> for Duration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let s = String::deserialize(deserializer)?;
        let v = parse_go_duration(&s).map_err(D::Error::custom)?;
        Ok(Duration(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize)]
    struct ByteSizeConfig {
        size: ByteSize,
    }

    #[derive(Serialize, Deserialize)]
    struct DurationConfig {
        duration: Duration,
    }

    #[test]
    fn byte_size() {
        // json
        let cfg: ByteSizeConfig = serde_json::from_str(r#"{"size":"1MiB"}"#).unwrap();
        assert_eq!(cfg.size, ByteSize(1024 * 1024));
        let data = serde_json::to_string(&cfg).unwrap();
        assert_eq!(data, r#"{"size":"1MiB"}"#);

        // toml
        let cfg: ByteSizeConfig = toml::from_str(r#"size = "512KiB""#).unwrap();
        assert_eq!(cfg.size, ByteSize(512 * 1024));
        let out = toml::to_string(&cfg).unwrap();
        assert_eq!(out, "size = \"512KiB\"\n");
    }

    #[test]
    fn duration() {
        // json
        let duration = Duration(3_600_000_000_000 + 2 * 60_000_000_000 + 3 * 1_000_000_000);
        let cfg = DurationConfig { duration };
        let data = serde_json::to_string(&cfg).unwrap();
        assert_eq!(data, r#"{"duration":"1h2m3s"}"#);
        let decoded: DurationConfig = serde_json::from_str(&data).unwrap();
        assert_eq!(decoded.duration, duration);

        // toml
        let cfg: DurationConfig = toml::from_str(r#"duration = "2m3s""#).unwrap();
        assert_eq!(
            cfg.duration,
            Duration(2 * 60_000_000_000 + 3 * 1_000_000_000)
        );
        let out = toml::to_string(&cfg).unwrap();
        assert_eq!(out, "duration = \"2m3s\"\n");
    }

    // Supplementary coverage of the go-units / stdlib semantics the wrappers
    // delegate to in the source.
    #[test]
    fn ram_in_bytes_semantics() {
        for (s, v) in [
            ("32", 32),
            ("32b", 32),
            ("32B", 32),
            ("32k", 32 << 10),
            ("32K", 32 << 10),
            ("32kb", 32 << 10),
            ("32Kb", 32 << 10),
            ("32Mb", 32 << 20),
            ("32Gb", 32i64 << 30),
            ("32Tb", 32i64 << 40),
            ("32Pb", 32i64 << 50),
            ("32PB", 32i64 << 50),
            ("32P", 32i64 << 50),
            ("32.3", 32),
            ("32.5KiB", (32.5 * 1024.0) as i64),
            (".3kB", (0.3 * 1024.0) as i64),
            ("32.KiB", 32 << 10),
            ("+32MiB", 32 << 20),
            ("1e2KiB", 100 << 10),
        ] {
            assert_eq!(ram_in_bytes(s).unwrap(), v, "case {s}");
        }
        for s in ["", "hello", "-32", "32.5x"] {
            assert!(ram_in_bytes(s).is_err(), "case {s}");
        }
    }

    #[test]
    fn ram_in_bytes_accepts_go_float_literals() {
        for (s, v) in [
            // strconv.ParseFloat accepts hexadecimal mantissas with a binary
            // exponent and valid Go digit separators.
            ("0x1p10KiB", 1024 * 1024),
            ("0x1.8p1KiB", 3 * 1024),
            ("0x_1p10KiB", 1024 * 1024),
            ("1_000KiB", 1_000 * 1024),
        ] {
            assert_eq!(ram_in_bytes(s).unwrap(), v, "case {s}");
        }
        for s in ["1_e2KiB", "0x1_p10KiB"] {
            assert!(ram_in_bytes(s).is_err(), "case {s}");
        }
    }

    #[test]
    fn go_duration_semantics() {
        for (s, v) in [
            ("0", 0i64),
            ("5s", 5_000_000_000),
            ("30s", 30_000_000_000),
            ("1478s", 1_478_000_000_000),
            ("-5s", -5_000_000_000),
            ("+5s", 5_000_000_000),
            ("5.0s", 5_000_000_000),
            ("5.6s", 5_600_000_000),
            ("5.s", 5_000_000_000),
            (".5s", 500_000_000),
            ("1.0s", 1_000_000_000),
            ("1.00s", 1_000_000_000),
            ("1.004s", 1_004_000_000),
            ("100.00100s", 100_001_000_000),
            ("10ns", 10),
            ("11us", 11_000),
            ("12µs", 12_000),
            ("13ms", 13_000_000),
            ("14s", 14_000_000_000),
            ("15m", 15 * 60_000_000_000),
            ("16h", 16 * 3_600_000_000_000),
            ("3h30m", 3 * 3_600_000_000_000 + 30 * 60_000_000_000),
            ("10.5s4m", 4 * 60_000_000_000 + 10_500_000_000),
            ("-2m3.4s", -(2 * 60_000_000_000 + 3_400_000_000)),
            (
                "1h2m3s4ms5us6ns",
                3_600_000_000_000 + 2 * 60_000_000_000 + 3_000_000_000 + 4_000_000 + 5_000 + 6,
            ),
        ] {
            assert_eq!(parse_go_duration(s).unwrap(), v, "case {s}");
        }
        for s in ["", "3", "-", "s", ".", "-.", ".s", "+.s", "3x", "1d"] {
            assert!(parse_go_duration(s).is_err(), "case {s}");
        }

        for (v, s) in [
            (0i64, "0s"),
            (1, "1ns"),
            (1_100, "1.1µs"),
            (2_200_000, "2.2ms"),
            (3_300_000_000, "3.3s"),
            (4 * 60_000_000_000 + 5_000_000_000, "4m5s"),
            (4 * 60_000_000_000 + 1_000_000, "4m0.001s"),
            (
                5 * 3_600_000_000_000 + 6 * 60_000_000_000 + 7_001_000_000,
                "5h6m7.001s",
            ),
            (8 * 60_000_000_000 + 1, "8m0.000000001s"),
            (-(8 * 60_000_000_000 + 1), "-8m0.000000001s"),
        ] {
            assert_eq!(format_go_duration(v), s, "case {v}");
        }
    }

    #[test]
    fn bytes_size_semantics() {
        for (v, s) in [
            (1024.0 * 1024.0, "1MiB"),
            (512.0 * 1024.0, "512KiB"),
            (0.0, "0B"),
            (1023.0, "1023B"),
            (1024.0, "1KiB"),
            (1536.0, "1.5KiB"),
            (1024.0 * 1024.0 * 1.5, "1.5MiB"),
        ] {
            assert_eq!(bytes_size(v), s, "case {v}");
        }
    }
}
