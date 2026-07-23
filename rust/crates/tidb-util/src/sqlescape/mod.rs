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

//! Complete transcreation of `pkg/util/sqlescape`.
//!
//! `utils.go` maps to this module, `utils_test.go` maps to the source-named
//! tests below and `benches/sqlescape.rs`, and `BUILD.bazel` maps to the
//! `tidb-util` manifest. The Go package has no `TestMain`, fixtures, generated
//! files, build-tag variants, fuzz targets, or examples.
//!
//! Go strings are arbitrary bytes. Therefore [`escape_sql`] returns `Vec<u8>`
//! rather than silently rejecting or replacing invalid UTF-8 originating in a
//! binary argument. [`format_sql`] preserves the source's single writer call.

use chrono::{Datelike, NaiveDateTime, Timelike};
use std::fmt;
use std::io::Write;

/// One dynamically typed `%?` or `%n` argument.
#[derive(Clone, Debug)]
pub enum SqlArg<'a> {
    /// Go's nil interface.
    Null,
    /// Any signed integer kind, including named signed types.
    Signed(i64),
    /// Any unsigned integer kind, including named unsigned types.
    Unsigned(u64),
    /// A float32 value, formatted with its 32-bit domain.
    Float32(f32),
    /// A float64 value.
    Float64(f64),
    /// A boolean rendered as `1` or `0`.
    Bool(bool),
    /// Go's zero `time.Time` when `None`, otherwise wall-clock fields.
    Time(Option<NaiveDateTime>),
    /// `json.RawMessage`, quoted without the `_binary` introducer.
    RawJson(&'a [u8]),
    /// A byte slice; `None` distinguishes nil from a present empty slice.
    Bytes(Option<&'a [u8]>),
    /// A string or named string type.
    String(&'a str),
    /// A comma-separated sequence of quoted strings.
    Strings(&'a [&'a str]),
    /// A comma-separated sequence of float32 values.
    Float32s(&'a [f32]),
    /// A comma-separated sequence of float64 values.
    Float64s(&'a [f64]),
    /// An unsupported dynamic value with its Go-like diagnostic spelling.
    Unsupported(&'a str),
}

macro_rules! signed_arg {
    ($($type:ty),+ $(,)?) => {
        $(
            impl From<$type> for SqlArg<'_> {
                fn from(value: $type) -> Self {
                    Self::Signed(value as i64)
                }
            }
        )+
    };
}

macro_rules! unsigned_arg {
    ($($type:ty),+ $(,)?) => {
        $(
            impl From<$type> for SqlArg<'_> {
                fn from(value: $type) -> Self {
                    Self::Unsigned(value as u64)
                }
            }
        )+
    };
}

signed_arg!(i8, i16, i32, i64, isize);
unsigned_arg!(u8, u16, u32, u64, usize);

impl From<bool> for SqlArg<'_> {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<f32> for SqlArg<'_> {
    fn from(value: f32) -> Self {
        Self::Float32(value)
    }
}

impl From<f64> for SqlArg<'_> {
    fn from(value: f64) -> Self {
        Self::Float64(value)
    }
}

impl<'a> From<&'a str> for SqlArg<'a> {
    fn from(value: &'a str) -> Self {
        Self::String(value)
    }
}

/// Formatting failures from [`escape_sql`] and [`format_sql`].
#[derive(Debug)]
pub enum EscapeError {
    /// A format specifier did not have a corresponding argument.
    MissingArgument {
        /// One-based position of the required argument.
        position: usize,
        /// Total number supplied.
        supplied: usize,
    },
    /// `%n` received something other than a string.
    IdentifierType(String),
    /// `%?` received an unsupported dynamic kind.
    UnsupportedArgument {
        /// One-based argument position.
        position: usize,
        /// Diagnostic representation of the value.
        value: String,
    },
    /// The destination writer failed.
    Write(std::io::Error),
}

impl fmt::Display for EscapeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingArgument { position, supplied } => write!(
                formatter,
                "missing arguments, need {position}-th arg, but only got {supplied} args"
            ),
            Self::IdentifierType(value) => {
                write!(formatter, "expect a string identifier, got {value}")
            }
            Self::UnsupportedArgument { position, value } => {
                write!(formatter, "unsupported {position}-th argument: {value}")
            }
            Self::Write(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for EscapeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Write(error) => Some(error),
            _ => None,
        }
    }
}

fn reserve_buffer(mut buffer: Vec<u8>, append_size: usize) -> Vec<u8> {
    let new_size = buffer
        .len()
        .checked_add(append_size)
        .expect("SQL escape buffer length overflow");
    if buffer.capacity() < new_size {
        let allocation_size = buffer
            .len()
            .checked_mul(2)
            .and_then(|size| size.checked_add(append_size))
            .expect("SQL escape buffer capacity overflow");
        let mut replacement = vec![0; allocation_size];
        replacement[..buffer.len()].copy_from_slice(&buffer);
        buffer = replacement;
    }
    buffer.resize(new_size, 0);
    buffer
}

fn escape_bytes_backslash(mut buffer: Vec<u8>, value: &[u8]) -> Vec<u8> {
    let start = buffer.len();
    let escaped_capacity = value
        .len()
        .checked_mul(2)
        .expect("SQL escape input length overflow");
    buffer = reserve_buffer(buffer, escaped_capacity);
    let mut position = start;
    for &byte in value {
        let escaped = match byte {
            0 => Some(b'0'),
            b'\n' => Some(b'n'),
            b'\r' => Some(b'r'),
            0x1a => Some(b'Z'),
            b'\'' => Some(b'\''),
            b'"' => Some(b'"'),
            b'\\' => Some(b'\\'),
            _ => None,
        };
        if let Some(escaped) = escaped {
            buffer[position] = b'\\';
            buffer[position + 1] = escaped;
            position += 2;
        } else {
            buffer[position] = byte;
            position += 1;
        }
    }
    buffer.truncate(position);
    buffer
}

fn escape_string_backslash(buffer: Vec<u8>, value: &str) -> Vec<u8> {
    escape_bytes_backslash(buffer, tidb_hack::slice(value))
}

/// Escapes one string using MySQL backslash sequences.
#[must_use]
pub fn escape_string(value: &str) -> String {
    String::from_utf8(escape_string_backslash(
        Vec::with_capacity(value.len()),
        value,
    ))
    .expect("escaping a UTF-8 string preserves UTF-8")
}

fn normalize_exponent(mantissa: &str, exponent: i32) -> String {
    let sign = if exponent < 0 { '-' } else { '+' };
    format!("{mantissa}e{sign}{:02}", exponent.unsigned_abs())
}

fn format_go_float64(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_owned();
    }
    if value == f64::INFINITY {
        return "+Inf".to_owned();
    }
    if value == f64::NEG_INFINITY {
        return "-Inf".to_owned();
    }
    if value == 0.0 {
        return if value.is_sign_negative() { "-0" } else { "0" }.to_owned();
    }
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("Rust scientific float contains exponent");
    let exponent: i32 = exponent.parse().expect("Rust float exponent is numeric");
    if !(-4..6).contains(&exponent) {
        normalize_exponent(mantissa, exponent)
    } else {
        value.to_string()
    }
}

fn format_go_float32(value: f32) -> String {
    if value.is_nan() {
        return "NaN".to_owned();
    }
    if value == f32::INFINITY {
        return "+Inf".to_owned();
    }
    if value == f32::NEG_INFINITY {
        return "-Inf".to_owned();
    }
    if value == 0.0 {
        return if value.is_sign_negative() { "-0" } else { "0" }.to_owned();
    }
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("Rust scientific float contains exponent");
    let exponent: i32 = exponent.parse().expect("Rust float exponent is numeric");
    if !(-4..6).contains(&exponent) {
        normalize_exponent(mantissa, exponent)
    } else {
        value.to_string()
    }
}

fn append_time(buffer: &mut Vec<u8>, value: Option<NaiveDateTime>) {
    if let Some(value) = value {
        buffer.extend_from_slice(
            format!(
                "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                value.year(),
                value.month(),
                value.day(),
                value.hour(),
                value.minute(),
                value.second()
            )
            .as_bytes(),
        );
        let micros = value.nanosecond() / 1_000;
        if micros != 0 {
            let fraction = format!("{micros:06}");
            buffer.push(b'.');
            buffer.extend_from_slice(fraction.trim_end_matches('0').as_bytes());
        }
        buffer.push(b'\'');
    } else {
        buffer.extend_from_slice(b"'0000-00-00'");
    }
}

fn append_string_argument(mut buffer: Vec<u8>, value: &str) -> Vec<u8> {
    buffer.push(b'\'');
    buffer = escape_string_backslash(buffer, value);
    buffer.push(b'\'');
    buffer
}

fn append_argument(mut buffer: Vec<u8>, argument: &SqlArg<'_>) -> Result<Vec<u8>, EscapeError> {
    match argument {
        SqlArg::Null => buffer.extend_from_slice(b"NULL"),
        SqlArg::Signed(value) => buffer.extend_from_slice(value.to_string().as_bytes()),
        SqlArg::Unsigned(value) => buffer.extend_from_slice(value.to_string().as_bytes()),
        SqlArg::Float32(value) => {
            buffer.extend_from_slice(format_go_float32(*value).as_bytes());
        }
        SqlArg::Float64(value) => {
            buffer.extend_from_slice(format_go_float64(*value).as_bytes());
        }
        SqlArg::Bool(value) => buffer.push(if *value { b'1' } else { b'0' }),
        SqlArg::Time(value) => append_time(&mut buffer, *value),
        SqlArg::RawJson(value) => {
            buffer.push(b'\'');
            buffer = escape_bytes_backslash(buffer, value);
            buffer.push(b'\'');
        }
        SqlArg::Bytes(None) => buffer.extend_from_slice(b"NULL"),
        SqlArg::Bytes(Some(value)) => {
            buffer.extend_from_slice(b"_binary'");
            buffer = escape_bytes_backslash(buffer, value);
            buffer.push(b'\'');
        }
        SqlArg::String(value) => buffer = append_string_argument(buffer, value),
        SqlArg::Strings(values) => {
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    buffer.push(b',');
                }
                buffer = append_string_argument(buffer, value);
            }
        }
        SqlArg::Float32s(values) => {
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    buffer.push(b',');
                }
                buffer.extend_from_slice(format_go_float32(*value).as_bytes());
            }
        }
        SqlArg::Float64s(values) => {
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    buffer.push(b',');
                }
                buffer.extend_from_slice(format_go_float64(*value).as_bytes());
            }
        }
        SqlArg::Unsupported(value) => {
            return Err(EscapeError::UnsupportedArgument {
                position: 0,
                value: (*value).to_owned(),
            });
        }
    }
    Ok(buffer)
}

fn escape_sql_impl(sql: &str, arguments: &[SqlArg<'_>]) -> Result<Vec<u8>, EscapeError> {
    let mut buffer = Vec::with_capacity(sql.len());
    let mut argument_position = 0;
    let bytes = sql.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let Some(relative) = bytes[index..].iter().position(|byte| *byte == b'%') else {
            buffer.extend_from_slice(&bytes[index..]);
            break;
        };
        let marker = index + relative;
        buffer.extend_from_slice(&bytes[index..marker]);
        let specifier = bytes.get(marker + 1).copied().unwrap_or_default();
        match specifier {
            b'n' => {
                let argument =
                    arguments
                        .get(argument_position)
                        .ok_or(EscapeError::MissingArgument {
                            position: argument_position + 1,
                            supplied: arguments.len(),
                        })?;
                argument_position += 1;
                let SqlArg::String(identifier) = argument else {
                    return Err(EscapeError::IdentifierType(format!("{argument:?}")));
                };
                buffer.push(b'`');
                for byte in identifier.bytes() {
                    buffer.push(byte);
                    if byte == b'`' {
                        buffer.push(b'`');
                    }
                }
                buffer.push(b'`');
                index = marker + 2;
            }
            b'?' => {
                let argument =
                    arguments
                        .get(argument_position)
                        .ok_or(EscapeError::MissingArgument {
                            position: argument_position + 1,
                            supplied: arguments.len(),
                        })?;
                argument_position += 1;
                match append_argument(buffer, argument) {
                    Ok(next) => buffer = next,
                    Err(EscapeError::UnsupportedArgument { value, .. }) => {
                        return Err(EscapeError::UnsupportedArgument {
                            position: argument_position,
                            value,
                        });
                    }
                    Err(error) => return Err(error),
                }
                index = marker + 2;
            }
            b'%' => {
                buffer.push(b'%');
                index = marker + 2;
            }
            _ => {
                buffer.push(b'%');
                index = marker + 1;
            }
        }
    }
    Ok(buffer)
}

/// Escapes arguments into SQL using `%?`, `%n`, and `%%`.
pub fn escape_sql(sql: &str, arguments: &[SqlArg<'_>]) -> Result<Vec<u8>, EscapeError> {
    escape_sql_impl(sql, arguments)
}

/// Escapes SQL and panics on a statically avoidable argument error.
///
/// # Panics
///
/// Panics when [`escape_sql`] returns an error.
#[must_use]
pub fn must_escape_sql(sql: &str, arguments: &[SqlArg<'_>]) -> Vec<u8> {
    escape_sql(sql, arguments).unwrap_or_else(|error| panic!("{error}"))
}

/// Writes one escaped SQL byte string with one call to the destination.
pub fn format_sql(
    writer: &mut impl Write,
    sql: &str,
    arguments: &[SqlArg<'_>],
) -> Result<(), EscapeError> {
    let buffer = escape_sql_impl(sql, arguments)?;
    writer.write(&buffer).map_err(EscapeError::Write)?;
    Ok(())
}

/// Formats SQL and panics on an argument or writer error.
///
/// # Panics
///
/// Panics when [`format_sql`] returns an error.
pub fn must_format_sql(writer: &mut impl Write, sql: &str, arguments: &[SqlArg<'_>]) {
    format_sql(writer, sql, arguments).unwrap_or_else(|error| panic!("{error}"));
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn TestReserveBuffer() {
        let result0 = reserve_buffer(Vec::new(), 0);
        assert_eq!(result0.len(), 0);

        let mut result1 = reserve_buffer(result0, 3);
        assert_eq!(result1.len(), 3);
        result1[1] = 3;

        let result2 = reserve_buffer(result1.clone(), 9);
        assert_eq!(result2.len(), 12);
        assert_eq!(result2.capacity(), 15);
        assert_eq!(&result2[..3], result1.as_slice());
    }

    #[test]
    fn TestEscapeBackslash() {
        let tests: &[(&str, &[u8], &[u8])] = &[
            ("normal", b"hello", b"hello"),
            ("0", b"he\0lo", b"he\\0lo"),
            ("break line", b"he\nlo", b"he\\nlo"),
            ("carry", b"he\rlo", b"he\\rlo"),
            ("substitute", b"he\x1alo", b"he\\Zlo"),
            ("single quote", b"he'lo", b"he\\'lo"),
            ("double quote", b"he\"lo", b"he\\\"lo"),
            ("back slash", b"he\\lo", b"he\\\\lo"),
            ("double escape", b"he\0lo\"", b"he\\0lo\\\""),
            ("chinese", "中文?".as_bytes(), "中文?".as_bytes()),
        ];
        for (name, input, expected) in tests {
            assert_eq!(
                escape_bytes_backslash(Vec::new(), input),
                *expected,
                "{name}"
            );
            let text = std::str::from_utf8(input).expect("source string row");
            assert_eq!(
                escape_string_backslash(Vec::new(), text),
                *expected,
                "{name}"
            );
        }
    }

    struct EscapeCase<'a> {
        name: &'a str,
        input: &'a str,
        expected: Option<&'a [u8]>,
        error_prefix: Option<&'a str>,
        arguments: Vec<SqlArg<'a>>,
    }

    #[test]
    fn TestEscapeSQL() {
        let time1 = NaiveDate::from_ymd_opt(2019, 1, 1)
            .expect("date")
            .and_hms_opt(0, 0, 0)
            .expect("time");
        let time2 = NaiveDate::from_ymd_opt(2018, 1, 23)
            .expect("date")
            .and_hms_opt(4, 3, 5)
            .expect("time");
        let time3 = NaiveDate::from_ymd_opt(1970, 1, 1)
            .expect("date")
            .and_hms_nano_opt(0, 0, 0, 888_888_888)
            .expect("time");
        let strings = ["33", "44"];
        let float32s = [33.1_f32, 0.44];
        let float64s = [55.2_f64, 0.66];
        let cases = vec![
            EscapeCase {
                name: "normal 1",
                input: "select * from 1",
                expected: Some(b"select * from 1"),
                error_prefix: None,
                arguments: vec![],
            },
            EscapeCase {
                name: "normal 2",
                input: "WHERE source != 'builtin'",
                expected: Some(b"WHERE source != 'builtin'"),
                error_prefix: None,
                arguments: vec![],
            },
            EscapeCase {
                name: "discard extra arguments",
                input: "select * from 1",
                expected: Some(b"select * from 1"),
                error_prefix: None,
                arguments: vec![4_i64.into(), 5_i64.into(), "rt".into()],
            },
            EscapeCase {
                name: "%? missing arguments",
                input: "select %? from %?",
                expected: None,
                error_prefix: Some("missing arguments"),
                arguments: vec![4_i64.into()],
            },
            EscapeCase {
                name: "nil",
                input: "select %?",
                expected: Some(b"select NULL"),
                error_prefix: None,
                arguments: vec![SqlArg::Null],
            },
            EscapeCase {
                name: "int",
                input: "select %?",
                expected: Some(b"select 3"),
                error_prefix: None,
                arguments: vec![3_isize.into()],
            },
            EscapeCase {
                name: "int8",
                input: "select %?",
                expected: Some(b"select 4"),
                error_prefix: None,
                arguments: vec![4_i8.into()],
            },
            EscapeCase {
                name: "int16",
                input: "select %?",
                expected: Some(b"select 5"),
                error_prefix: None,
                arguments: vec![5_i16.into()],
            },
            EscapeCase {
                name: "int32",
                input: "select %?",
                expected: Some(b"select 6"),
                error_prefix: None,
                arguments: vec![6_i32.into()],
            },
            EscapeCase {
                name: "int64",
                input: "select %?",
                expected: Some(b"select 7"),
                error_prefix: None,
                arguments: vec![7_i64.into()],
            },
            EscapeCase {
                name: "uint",
                input: "select %?",
                expected: Some(b"select 8"),
                error_prefix: None,
                arguments: vec![8_usize.into()],
            },
            EscapeCase {
                name: "uint8",
                input: "select %?",
                expected: Some(b"select 9"),
                error_prefix: None,
                arguments: vec![9_u8.into()],
            },
            EscapeCase {
                name: "uint16",
                input: "select %?",
                expected: Some(b"select 10"),
                error_prefix: None,
                arguments: vec![10_u16.into()],
            },
            EscapeCase {
                name: "uint32",
                input: "select %?",
                expected: Some(b"select 11"),
                error_prefix: None,
                arguments: vec![11_u32.into()],
            },
            EscapeCase {
                name: "uint64",
                input: "select %?",
                expected: Some(b"select 12"),
                error_prefix: None,
                arguments: vec![12_u64.into()],
            },
            EscapeCase {
                name: "float32",
                input: "select %?",
                expected: Some(b"select 0.13"),
                error_prefix: None,
                arguments: vec![0.13_f32.into()],
            },
            EscapeCase {
                name: "float64",
                input: "select %?",
                expected: Some(b"select 0.14"),
                error_prefix: None,
                arguments: vec![0.14_f64.into()],
            },
            EscapeCase {
                name: "bool on",
                input: "select %?",
                expected: Some(b"select 1"),
                error_prefix: None,
                arguments: vec![true.into()],
            },
            EscapeCase {
                name: "bool off",
                input: "select %?",
                expected: Some(b"select 0"),
                error_prefix: None,
                arguments: vec![false.into()],
            },
            EscapeCase {
                name: "time 0",
                input: "select %?",
                expected: Some(b"select '0000-00-00'"),
                error_prefix: None,
                arguments: vec![SqlArg::Time(None)],
            },
            EscapeCase {
                name: "time 1",
                input: "select %?",
                expected: Some(b"select '2019-01-01 00:00:00'"),
                error_prefix: None,
                arguments: vec![SqlArg::Time(Some(time1))],
            },
            EscapeCase {
                name: "time 2",
                input: "select %?",
                expected: Some(b"select '2018-01-23 04:03:05'"),
                error_prefix: None,
                arguments: vec![SqlArg::Time(Some(time2))],
            },
            EscapeCase {
                name: "time 3",
                input: "select %?",
                expected: Some(b"select '1970-01-01 00:00:00.888888'"),
                error_prefix: None,
                arguments: vec![SqlArg::Time(Some(time3))],
            },
            EscapeCase {
                name: "empty byte slice1",
                input: "select %?",
                expected: Some(b"select NULL"),
                error_prefix: None,
                arguments: vec![SqlArg::Bytes(None)],
            },
            EscapeCase {
                name: "empty byte slice2",
                input: "select %?",
                expected: Some(b"select _binary''"),
                error_prefix: None,
                arguments: vec![SqlArg::Bytes(Some(b""))],
            },
            EscapeCase {
                name: "byte slice",
                input: "select %?",
                expected: Some(b"select _binary'\x02\x03'"),
                error_prefix: None,
                arguments: vec![SqlArg::Bytes(Some(&[2, 3]))],
            },
            EscapeCase {
                name: "string",
                input: "select %?",
                expected: Some(b"select '33'"),
                error_prefix: None,
                arguments: vec!["33".into()],
            },
            EscapeCase {
                name: "string slice",
                input: "select %?",
                expected: Some(b"select '33','44'"),
                error_prefix: None,
                arguments: vec![SqlArg::Strings(&strings)],
            },
            EscapeCase {
                name: "raw json",
                input: "select %?",
                expected: Some(b"select '{\\\"h\\\": \\\"hello\\\"}'"),
                error_prefix: None,
                arguments: vec![SqlArg::RawJson(br#"{"h": "hello"}"#)],
            },
            EscapeCase {
                name: "unsupported args",
                input: "select %?",
                expected: None,
                error_prefix: Some("unsupported 1-th argument"),
                arguments: vec![SqlArg::Unsupported("channel")],
            },
            EscapeCase {
                name: "mixed arguments",
                input: "select %?, %?, %?",
                expected: Some(b"select '33', 44, '0000-00-00'"),
                error_prefix: None,
                arguments: vec!["33".into(), 44_i64.into(), SqlArg::Time(None)],
            },
            EscapeCase {
                name: "simple injection",
                input: "select %?",
                expected: Some(b"select '0; drop database'"),
                error_prefix: None,
                arguments: vec!["0; drop database".into()],
            },
            EscapeCase {
                name: "identifier, wrong arg",
                input: "use %n",
                expected: None,
                error_prefix: Some("expect a string identifier"),
                arguments: vec![3_i64.into()],
            },
            EscapeCase {
                name: "identifier",
                input: "use %n",
                expected: Some(b"use `table```"),
                error_prefix: None,
                arguments: vec!["table`".into()],
            },
            EscapeCase {
                name: "%n missing arguments",
                input: "use %n",
                expected: None,
                error_prefix: Some("missing arguments"),
                arguments: vec![],
            },
            EscapeCase {
                name: "% escape",
                input: "select * from t where val = '%%?'",
                expected: Some(b"select * from t where val = '%?'"),
                error_prefix: None,
                arguments: vec![],
            },
            EscapeCase {
                name: "unknown specifier",
                input: "%v",
                expected: Some(b"%v"),
                error_prefix: None,
                arguments: vec![],
            },
            EscapeCase {
                name: "truncated specifier",
                input: "rv %",
                expected: Some(b"rv %"),
                error_prefix: None,
                arguments: vec![],
            },
            EscapeCase {
                name: "float32 slice",
                input: "select %?",
                expected: Some(b"select 33.1,0.44"),
                error_prefix: None,
                arguments: vec![SqlArg::Float32s(&float32s)],
            },
            EscapeCase {
                name: "float64 slice",
                input: "select %?",
                expected: Some(b"select 55.2,0.66"),
                error_prefix: None,
                arguments: vec![SqlArg::Float64s(&float64s)],
            },
            EscapeCase {
                name: "myInt",
                input: "select %?",
                expected: Some(b"select 3"),
                error_prefix: None,
                arguments: vec![SqlArg::Signed(3)],
            },
            EscapeCase {
                name: "myStr",
                input: "select %?",
                expected: Some(b"select '3'"),
                error_prefix: None,
                arguments: vec![SqlArg::String("3")],
            },
        ];

        for case in cases {
            let direct = escape_sql_impl(case.input, &case.arguments);
            let public = escape_sql(case.input, &case.arguments);
            let mut writer = Vec::new();
            let formatted = format_sql(&mut writer, case.input, &case.arguments);
            if let Some(expected) = case.expected {
                assert_eq!(direct.expect(case.name), expected, "{}", case.name);
                assert_eq!(public.expect(case.name), expected, "{}", case.name);
                formatted.expect(case.name);
                assert_eq!(writer, expected, "{}", case.name);
            } else {
                let prefix = case.error_prefix.expect("error prefix");
                assert!(
                    direct.unwrap_err().to_string().starts_with(prefix),
                    "{}",
                    case.name
                );
                assert!(
                    public.unwrap_err().to_string().starts_with(prefix),
                    "{}",
                    case.name
                );
                assert!(
                    formatted.unwrap_err().to_string().starts_with(prefix),
                    "{}",
                    case.name
                );
            }
        }
    }

    #[test]
    fn TestMustUtils() {
        let panic = std::panic::catch_unwind(|| must_escape_sql("%?", &[]));
        let message = panic.expect_err("must escape must panic");
        let message = message
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| message.downcast_ref::<&str>().copied())
            .expect("panic message");
        assert_eq!(
            message,
            "missing arguments, need 1-th arg, but only got 0 args"
        );

        let mut output = Vec::new();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            must_format_sql(&mut output, "%?", &[]);
        }));
        assert!(panic.is_err());
        must_format_sql(&mut output, "t", &[]);
        assert_eq!(must_escape_sql("tt", &[]), b"tt");
    }

    #[test]
    fn TestEscapeString() {
        for (input, expected) in [
            ("testData", "testData"),
            ("it's all good", "it\\'s all good"),
            ("+ -><()~*:\"\"&|", "+ -><()~*:\\\"\\\"&|"),
        ] {
            assert_eq!(escape_string(input), expected);
        }
    }

    #[test]
    fn go_shortest_float_boundaries_are_preserved() {
        for (value, expected) in [
            (1e-4, "0.0001"),
            (1e-5, "1e-05"),
            (1e5, "100000"),
            (1e6, "1e+06"),
            (f64::INFINITY, "+Inf"),
            (f64::NEG_INFINITY, "-Inf"),
        ] {
            assert_eq!(format_go_float64(value), expected);
        }
    }
}
