// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `util.go`: the TOML/JSON duration wrapper, with Go's
//! `time.Duration.String` and `time.ParseDuration` text forms.

use std::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A wrapper of `time.Duration` for TOML and JSON. The text form is Go's
/// duration syntax, such as `"1h1m1s"`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Duration {
    pub duration: std::time::Duration,
}

/// Go `NewDuration`.
pub fn new_duration(duration: std::time::Duration) -> Duration {
    Duration { duration }
}

impl Duration {
    /// Go `NewDuration`.
    pub fn new(duration: std::time::Duration) -> Self {
        new_duration(duration)
    }

    /// Go's promoted `Duration.Seconds`.
    pub fn seconds(&self) -> f64 {
        self.duration.as_secs_f64()
    }

    /// Go `MarshalJSON`: the duration as a JSON string.
    pub fn marshal_json(&self) -> String {
        format!("\"{self}\"")
    }

    /// Go `UnmarshalJSON`: the input must be one quoted Go duration.
    pub fn unmarshal_json(&mut self, text: &[u8]) -> Result<(), String> {
        let unquoted: String = serde_json::from_slice::<serde_json::Value>(text)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .ok_or_else(|| "invalid syntax".to_owned())?;
        self.duration = parse_duration(&unquoted)?;
        Ok(())
    }

    /// Go `UnmarshalText`: parses a TOML string into the duration.
    pub fn unmarshal_text(&mut self, text: &[u8]) -> Result<(), String> {
        let text = std::str::from_utf8(text).map_err(|error| error.to_string())?;
        self.duration = parse_duration(text)?;
        Ok(())
    }

    /// Go `MarshalText`.
    pub fn marshal_text(&self) -> String {
        self.to_string()
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&go_duration_string(duration_nanos(self.duration)))
    }
}

impl Serialize for Duration {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Duration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        parse_duration(&text)
            .map(new_duration)
            .map_err(D::Error::custom)
    }
}

/// Go's `time.Duration` is a signed int64 nanosecond count.
pub(crate) fn duration_nanos(duration: std::time::Duration) -> i64 {
    duration.as_nanos().min(i64::MAX as u128) as i64
}

/// Parses a Go duration into a native, non-negative duration.
pub(crate) fn parse_duration(text: &str) -> Result<std::time::Duration, String> {
    let nanos = parse_go_duration(text)?;
    if nanos < 0 {
        return Err(format!("negative duration {:?} is not representable", text));
    }
    Ok(std::time::Duration::from_nanos(nanos as u64))
}

/// Go `time.Duration.String`.
pub(crate) fn go_duration_string(duration: i64) -> String {
    if duration == 0 {
        return "0s".to_owned();
    }
    let negative = duration < 0;
    let mut u = duration.unsigned_abs();
    let mut text = if u < 1_000_000_000 {
        let (precision, unit) = if u < 1_000 {
            (0, "ns")
        } else if u < 1_000_000 {
            (3, "µs")
        } else {
            (6, "ms")
        };
        let (fraction, integer) = format_fraction(u, precision);
        format!("{integer}{fraction}{unit}")
    } else {
        let (fraction, integer) = format_fraction(u, 9);
        u = integer;
        let mut text = format!("{}{fraction}s", u % 60);
        u /= 60;
        if u > 0 {
            text = format!("{}m{text}", u % 60);
            u /= 60;
            if u > 0 {
                text = format!("{u}h{text}");
            }
        }
        text
    };
    if negative {
        text.insert(0, '-');
    }
    text
}

/// Go `fmtFrac`: the fraction of `value / 10^precision` without trailing
/// zeros, and the remaining integer part.
fn format_fraction(mut value: u64, precision: usize) -> (String, u64) {
    let mut digits = Vec::with_capacity(precision);
    let mut print = false;
    for _ in 0..precision {
        let digit = value % 10;
        print = print || digit != 0;
        if print {
            digits.push(b'0' + digit as u8);
        }
        value /= 10;
    }
    if !print {
        return (String::new(), value);
    }
    digits.push(b'.');
    digits.reverse();
    (String::from_utf8(digits).expect("ASCII digits"), value)
}

/// Go `time.ParseDuration`, returning signed nanoseconds.
pub(crate) fn parse_go_duration(original: &str) -> Result<i64, String> {
    const LIMIT: u64 = 1 << 63;
    let invalid = || format!("time: invalid duration {}", quote(original));
    let mut s = original.as_bytes();
    let mut total: u64 = 0;
    let mut negative = false;
    if let Some(&c) = s.first() {
        if c == b'-' || c == b'+' {
            negative = c == b'-';
            s = &s[1..];
        }
    }
    if s == b"0" {
        return Ok(0);
    }
    if s.is_empty() {
        return Err(invalid());
    }
    while !s.is_empty() {
        let mut fraction: u64 = 0;
        let mut scale: f64 = 1.0;
        if !(s[0] == b'.' || s[0].is_ascii_digit()) {
            return Err(invalid());
        }
        // Consume [0-9]*.
        let before = s.len();
        let (mut value, rest) = leading_int(s).ok_or_else(invalid)?;
        s = rest;
        let pre = before != s.len();
        // Consume (\.[0-9]*)?.
        let mut post = false;
        if s.first() == Some(&b'.') {
            s = &s[1..];
            let before = s.len();
            let (f, sc, rest) = leading_fraction(s);
            fraction = f;
            scale = sc;
            s = rest;
            post = before != s.len();
        }
        if !pre && !post {
            return Err(invalid());
        }
        // Consume the unit.
        let mut i = 0;
        while i < s.len() {
            let c = s[i];
            if c == b'.' || c.is_ascii_digit() {
                break;
            }
            i += 1;
        }
        if i == 0 {
            return Err(format!(
                "time: missing unit in duration {}",
                quote(original)
            ));
        }
        let unit_text = &s[..i];
        s = &s[i..];
        let unit: u64 = match unit_text {
            b"ns" => 1,
            b"us" => 1_000,
            // U+00B5 micro sign and U+03BC Greek mu.
            [0xc2, 0xb5, b's'] | [0xce, 0xbc, b's'] => 1_000,
            b"ms" => 1_000_000,
            b"s" => 1_000_000_000,
            b"m" => 60 * 1_000_000_000,
            b"h" => 3_600 * 1_000_000_000,
            _ => {
                return Err(format!(
                    "time: unknown unit {} in duration {}",
                    quote(&String::from_utf8_lossy(unit_text)),
                    quote(original)
                ))
            }
        };
        if value > LIMIT / unit {
            return Err(invalid());
        }
        value *= unit;
        if fraction > 0 {
            // float64 is needed to be nanosecond accurate for fractions of hours.
            value = value.wrapping_add((fraction as f64 * (unit as f64 / scale)) as u64);
            if value > LIMIT {
                return Err(invalid());
            }
        }
        total = total.wrapping_add(value);
        if total > LIMIT {
            return Err(invalid());
        }
    }
    if negative {
        return Ok((total as i64).wrapping_neg());
    }
    if total > LIMIT - 1 {
        return Err(invalid());
    }
    Ok(total as i64)
}

/// Go `leadingInt`; `None` reports overflow.
fn leading_int(s: &[u8]) -> Option<(u64, &[u8])> {
    let mut i = 0;
    let mut x: u64 = 0;
    while i < s.len() {
        let c = s[i];
        if !c.is_ascii_digit() {
            break;
        }
        if x > (1 << 63) / 10 {
            return None;
        }
        x = x * 10 + u64::from(c - b'0');
        if x > 1 << 63 {
            return None;
        }
        i += 1;
    }
    Some((x, &s[i..]))
}

/// Go `leadingFraction`: overflowing digits are consumed but ignored.
fn leading_fraction(s: &[u8]) -> (u64, f64, &[u8]) {
    let mut i = 0;
    let mut x: u64 = 0;
    let mut scale = 1.0;
    let mut overflow = false;
    while i < s.len() {
        let c = s[i];
        if !c.is_ascii_digit() {
            break;
        }
        i += 1;
        if overflow {
            continue;
        }
        if x > ((1u64 << 63) - 1) / 10 {
            overflow = true;
            continue;
        }
        let y = x * 10 + u64::from(c - b'0');
        if y > 1 << 63 {
            overflow = true;
            continue;
        }
        x = y;
        scale *= 10.0;
    }
    (x, scale, &s[i..])
}

/// Go's `quote` helper in the time package.
fn quote(s: &str) -> String {
    format!("\"{s}\"")
}
