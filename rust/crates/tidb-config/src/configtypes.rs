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
pub fn bytes_size(mut size: f64) -> String {
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
    // Regex in the source: `^(\d+(\.\d+)*) ?([kKmMgGtTpP])?([iI])?[bB]?$`
    let s = size.trim_end();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
        i += 1;
    }
    let (num, rest) = s.split_at(i);
    if num.is_empty() || num.starts_with('.') || num.ends_with('.') || num.matches('.').count() > 1
    {
        return Err(format!("invalid size: '{size}'"));
    }
    let value: f64 = num.parse().map_err(|_| format!("invalid size: '{size}'"))?;

    let mut rest = rest.strip_prefix(' ').unwrap_or(rest).to_string();
    // strip optional trailing 'b'/'B'
    if rest.len() > 1 || rest.eq_ignore_ascii_case("b") {
        if let Some(r) = rest.strip_suffix(['b', 'B']) {
            rest = r.to_string();
        }
    }
    // strip optional 'i'/'I'
    if rest.len() == 2 {
        if let Some(r) = rest.strip_suffix(['i', 'I']) {
            rest = r.to_string();
        }
    }
    let mul: i64 = match rest.to_lowercase().as_str() {
        "" => 1,
        "k" => 1 << 10,
        "m" => 1 << 20,
        "g" => 1 << 30,
        "t" => 1 << 40,
        "p" => 1 << 50,
        _ => return Err(format!("invalid size: '{size}'")),
    };
    Ok((value * mul as f64) as i64)
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

impl Duration {
    /// The wrapped duration as a std `Duration` (panics on negative, like
    /// converting a negative Go duration would).
    pub fn as_std(&self) -> std::time::Duration {
        std::time::Duration::from_nanos(self.0 as u64)
    }

    /// Builds from a std `Duration`.
    pub fn from_std(d: std::time::Duration) -> Duration {
        Duration(d.as_nanos() as i64)
    }
}

/// Go `time.Duration.String()`.
pub fn format_go_duration(d: i64) -> String {
    if d == 0 {
        return "0s".to_string();
    }
    let neg = d < 0;
    let mut u = d.unsigned_abs();
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
        u = 0;
        let _ = u;
    }
    if neg {
        format!("-{out}")
    } else {
        out
    }
}

/// Go `time.ParseDuration`.
pub fn parse_go_duration(s: &str) -> Result<i64, String> {
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
    let mut d: i64 = 0;
    while !s.is_empty() {
        // integer part
        let int_len = s.bytes().take_while(u8::is_ascii_digit).count();
        let (int_str, rest) = s.split_at(int_len);
        s = rest;
        // fraction part
        let mut frac: f64 = 0.0;
        let mut has_frac = false;
        if let Some(rest) = s.strip_prefix('.') {
            let frac_len = rest.bytes().take_while(u8::is_ascii_digit).count();
            if frac_len > 0 {
                has_frac = true;
                frac = format!("0.{}", &rest[..frac_len])
                    .parse()
                    .map_err(|_| format!("time: invalid duration {orig:?}"))?;
            }
            s = &rest[frac_len..];
        }
        if int_str.is_empty() && !has_frac {
            return Err(format!("time: invalid duration {orig:?}"));
        }
        let int_val: u64 = if int_str.is_empty() {
            0
        } else {
            int_str
                .parse()
                .map_err(|_| format!("time: invalid duration {orig:?}"))?
        };
        // unit
        let unit_len = s
            .char_indices()
            .find(|(_, c)| c.is_ascii_digit() || *c == '.')
            .map(|(i, _)| i)
            .unwrap_or(s.len());
        let (unit_str, rest) = s.split_at(unit_len);
        s = rest;
        let unit: i64 = match unit_str {
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
        let v = int_val as i64 * unit + (frac * unit as f64) as i64;
        d += v;
        if d < 0 {
            return Err(format!("time: invalid duration {orig:?}"));
        }
    }
    Ok(if neg { -d } else { d })
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
        ] {
            assert_eq!(ram_in_bytes(s).unwrap(), v, "case {s}");
        }
        for s in ["", "hello", "-32", ".3kB", "32.KiB", "32.5x"] {
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
