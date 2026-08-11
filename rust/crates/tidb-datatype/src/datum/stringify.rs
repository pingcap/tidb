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

//! Datum rendering: diagnostic labels, result-set text, and row text.
//!
//! Mirrors `pkg/types/datum.go`'s `Datum.ToString`, `TruncatedStringify`,
//! `DatumsToString`, `DatumsToStrNoErr`, and `DatumsToStrNoErrSmart`, together
//! with the printable-string predicate those row renderers consult.

use std::fmt;

use super::{Datum, DatumKind, DatumStringError, DatumValueError};

/// Escapes backslashes and single quotes, then surrounds the bytes with
/// single quotes. This is the byte-preserving form of parser driver's
/// `WrapInSingleQuotes`.
#[must_use]
pub fn wrap_in_single_quotes(value: &[u8]) -> Vec<u8> {
    quote_value_expr(value)
}

/// Reverses [`wrap_in_single_quotes`]. Bytes without surrounding single
/// quotes are returned unchanged.
#[must_use]
pub fn unwrap_from_single_quotes(value: &[u8]) -> Vec<u8> {
    if value.len() < 2 || value.first() != Some(&b'\'') || value.last() != Some(&b'\'') {
        return value.to_vec();
    }
    let unescaped_backslashes = collapse_byte_pair(&value[1..value.len() - 1], b'\\');
    collapse_byte_pair(&unescaped_backslashes, b'\'')
}

fn collapse_byte_pair(value: &[u8], byte: u8) -> Vec<u8> {
    let mut output = Vec::with_capacity(value.len());
    let mut index = 0;
    while index < value.len() {
        if value[index] == byte && value.get(index + 1) == Some(&byte) {
            output.push(byte);
            index += 2;
        } else {
            output.push(value[index]);
            index += 1;
        }
    }
    output
}

impl Datum {
    /// Renders the scalar in the existing Go-oracle label format.
    ///
    /// Every valid UTF-8 payload keeps the historical `STR:<text>` bytes,
    /// including embedded NUL and other control characters used by the Go
    /// differential oracle. Only invalid UTF-8 uses an uppercase hexadecimal
    /// suffix because it cannot be represented by a Rust [`String`].
    pub fn label(&self) -> String {
        match self {
            Self::Int(value) => format!("INT:{value}"),
            Self::UInt(value) => format!("UINT:{value}"),
            Self::Decimal(value) => format!("DEC:{value}"),
            Self::Real(value) => format!("FLOAT:{value}"),
            Self::Float32(value) => format!("FLOAT:{value}"),
            Self::String(value) => label_bytes("STR", value.bytes()),
            Self::Bytes(value) => label_bytes("STR", value),
            Self::BinaryLiteral(value) | Self::Bit(value) => label_bytes("STR", value.as_bytes()),
            Self::Duration(value) => format!("DUR:{value}"),
            Self::Enum(value, _) => label_bytes("ENUM", value.name_bytes()),
            Self::Set(value, _) => label_bytes("SET", value.name_bytes()),
            Self::Time(value) => format!("TIME:{value}"),
            Self::Json(value) => format!("JSON:{value}"),
            Self::Raw(value) => label_bytes("RAW", value),
            Self::VectorFloat32(value) => format!("VECTOR:{value}"),
            Self::Null => "NULL".to_string(),
            Self::MinNotNull => "SKIP:15".to_string(),
            Self::MaxValue => "SKIP:16".to_string(),
        }
    }

    /// Byte-authoritative Go `Datum.ToString`. Go strings, ENUMs, SETs, and
    /// binary literals may contain arbitrary bytes and remain unchanged.
    pub fn sql_bytes(&self) -> Result<Vec<u8>, DatumStringError> {
        let value = match self {
            Self::Int(value) => value.to_string().into_bytes(),
            Self::UInt(value) => value.to_string().into_bytes(),
            Self::Decimal(value) => value.to_string().into_bytes(),
            Self::Real(value) => format_go_float_f(*value).into_bytes(),
            Self::Float32(value) => format_go_float_f(*value as f32).into_bytes(),
            Self::String(value) => value.bytes().to_vec(),
            Self::Bytes(value) => value.clone(),
            Self::BinaryLiteral(value) | Self::Bit(value) => value.as_bytes().to_vec(),
            Self::Duration(value) => value.to_string().into_bytes(),
            Self::Enum(value, _) => value.name_bytes().to_vec(),
            Self::Set(value, _) => value.name_bytes().to_vec(),
            Self::Time(value) => value.to_string().into_bytes(),
            Self::Json(value) => value.to_string().into_bytes(),
            Self::Raw(value) => {
                decode_bytes(value)?;
                value.clone()
            }
            Self::VectorFloat32(value) => value.to_string().into_bytes(),
            Self::Null => Vec::new(),
            Self::MinNotNull => {
                return Err(DatumStringError::RangeSentinel(DatumKind::MinNotNull));
            }
            Self::MaxValue => {
                return Err(DatumStringError::RangeSentinel(DatumKind::MaxValue));
            }
        };
        Ok(value)
    }

    /// UTF-8 convenience projection of [`Self::sql_bytes`]. Invalid source
    /// bytes return an error rather than being silently replaced.
    pub fn sql_string(&self) -> Result<String, DatumStringError> {
        let bytes = self.sql_bytes()?;
        decode_bytes(&bytes)
    }

    /// Source `Datum.TruncatedStringify` used by EXPLAIN and diagnostics.
    pub fn truncated_stringify(&self) -> Result<Vec<u8>, DatumStringError> {
        let bytes = match self {
            Self::String(value) => value.bytes().to_vec(),
            Self::Bytes(value) => value.clone(),
            Self::Json(value) => value.to_string().into_bytes(),
            Self::VectorFloat32(value) => return Ok(value.truncated_string().into_bytes()),
            Self::Int(value) => return Ok(value.to_string().into_bytes()),
            Self::UInt(value) => return Ok(value.to_string().into_bytes()),
            other => other.sql_bytes()?,
        };
        Ok(truncate_diagnostic_bytes(bytes))
    }

    /// Restores the default-field-type rows from Go parser driver's
    /// `ValueExpr.Restore`.
    ///
    /// The parser driver also consults field-type flags for boolean integers,
    /// charset introducers, and unsigned binary literals. A [`Datum`] does not
    /// carry that metadata, so this method deliberately models the default
    /// field types constructed by `types.New*Datum`, which are the source
    /// contract exercised by `value_expr_test.go`.
    pub fn restore_value_expr(&self) -> Result<Vec<u8>, DatumValueError> {
        match self {
            Self::String(value) => Ok(quote_value_expr(value.bytes())),
            Self::Bytes(value) => Ok(quote_value_expr_bytes(value)),
            Self::Duration(value) => Ok(quote_value_expr_bytes(value.to_string().as_bytes())),
            Self::Time(value) => Ok(quote_value_expr_bytes(value.to_string().as_bytes())),
            other => other.value_expr_scalar("value expression restore"),
        }
    }

    /// Formats the default-field-type rows from Go parser driver's
    /// `ValueExpr.Format` without assuming the source bytes are UTF-8.
    pub fn format_value_expr(&self) -> Result<Vec<u8>, DatumValueError> {
        self.value_expr_scalar("value expression format")
    }

    fn value_expr_scalar(&self, target: &'static str) -> Result<Vec<u8>, DatumValueError> {
        let value = match self {
            Self::Null => b"NULL".to_vec(),
            Self::Int(value) => value.to_string().into_bytes(),
            Self::UInt(value) => value.to_string().into_bytes(),
            Self::Float32(value) => format_go_float_e(*value as f32).into_bytes(),
            Self::Real(value) => format_go_float_e(*value).into_bytes(),
            Self::String(value) => quote_value_expr(value.bytes()),
            Self::Bytes(value) => quote_value_expr(value),
            Self::BinaryLiteral(value) => value.to_bit_literal_string(true).into_bytes(),
            Self::Decimal(value) => value.to_string().into_bytes(),
            other => return Err(DatumValueError::Unsupported(other.kind(), target)),
        };
        Ok(value)
    }
}

/// Source `DatumsToString`.
pub fn datums_to_string(
    datums: &[Datum],
    handle_special_values: bool,
    binary_as_hex: bool,
) -> Result<String, DatumStringError> {
    use fmt::Write;

    let mut output = String::new();
    if datums.len() > 1 {
        output.push('(');
    }
    for (index, datum) in datums.iter().enumerate() {
        if index != 0 {
            output.push_str(", ");
        }
        if handle_special_values {
            match datum {
                Datum::Null => {
                    output.push_str("NULL");
                    continue;
                }
                Datum::MinNotNull => {
                    output.push_str("-inf");
                    continue;
                }
                Datum::MaxValue => {
                    output.push_str("+inf");
                    continue;
                }
                _ => {}
            }
        }
        let mut text = datum.sql_string()?;
        let original_length = (text.len() > 2048).then_some(text.len());
        if original_length.is_some() {
            text.truncate(2048);
        }
        if matches!(datum, Datum::String(_)) {
            if binary_as_hex && !is_printable(text.as_bytes()) {
                write!(output, "0x{}", encode_hex(text.as_bytes()))
                    .expect("writing to String cannot fail");
            } else {
                write!(output, "\"{text}\"").expect("writing to String cannot fail");
            }
        } else {
            output.push_str(&text);
        }
        if let Some(length) = original_length {
            write!(output, " len({length})").expect("writing to String cannot fail");
        }
    }
    if datums.len() > 1 {
        output.push(')');
    }
    Ok(output)
}

/// Source `DatumsToStrNoErr`.
pub fn datums_to_string_no_error(datums: &[Datum]) -> String {
    datums_to_string(datums, true, false).unwrap_or_default()
}

/// Source `DatumsToStrNoErrSmart`.
pub fn datums_to_string_no_error_smart(datums: &[Datum]) -> String {
    datums_to_string(datums, true, true).unwrap_or_default()
}

/// Source printable-string predicate.
pub fn is_printable(value: &[u8]) -> bool {
    std::str::from_utf8(value).is_ok_and(|text| !text.chars().any(char::is_control))
}

fn truncate_diagnostic_bytes(mut value: Vec<u8>) -> Vec<u8> {
    const MAX_LEN: usize = 64;
    if value.len() <= MAX_LEN {
        return value;
    }
    let original_len = value.len();
    value.truncate(MAX_LEN);
    value.extend_from_slice(b"...(len:");
    value.extend_from_slice(original_len.to_string().as_bytes());
    value.push(b')');
    value
}

fn label_bytes(kind: &str, bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => format!("{kind}:{text}"),
        Err(_) => format!("{kind}_HEX:{}", encode_hex(bytes)),
    }
}

fn decode_bytes(bytes: &[u8]) -> Result<String, DatumStringError> {
    std::str::from_utf8(bytes)
        .map(str::to_string)
        .map_err(DatumStringError::InvalidUtf8)
}

fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(encoded, "{byte:02X}").expect("writing to String cannot fail");
    }
    encoded
}

fn quote_value_expr(value: &[u8]) -> Vec<u8> {
    let mut escaped = Vec::with_capacity(value.len());
    for byte in value {
        escaped.push(*byte);
        if *byte == b'\\' {
            escaped.push(b'\\');
        }
    }
    quote_value_expr_bytes(&escaped)
}

fn quote_value_expr_bytes(value: &[u8]) -> Vec<u8> {
    let mut quoted = Vec::with_capacity(value.len() + 2);
    quoted.push(b'\'');
    for byte in value {
        match byte {
            b'\'' => quoted.extend_from_slice(b"''"),
            other => quoted.push(*other),
        }
    }
    quoted.push(b'\'');
    quoted
}

trait GoScientificFloat: fmt::Display + fmt::LowerExp + Copy {
    fn special(self) -> Option<&'static str>;
}

impl GoScientificFloat for f32 {
    fn special(self) -> Option<&'static str> {
        if self.is_nan() {
            Some("NaN")
        } else if self == Self::INFINITY {
            Some("+Inf")
        } else if self == Self::NEG_INFINITY {
            Some("-Inf")
        } else {
            None
        }
    }
}

impl GoScientificFloat for f64 {
    fn special(self) -> Option<&'static str> {
        if self.is_nan() {
            Some("NaN")
        } else if self == Self::INFINITY {
            Some("+Inf")
        } else if self == Self::NEG_INFINITY {
            Some("-Inf")
        } else {
            None
        }
    }
}

/// Go `strconv.FormatFloat(value, 'f', -1, bitSize)` special-value spelling
/// plus Rust's equivalent shortest fixed rendering for finite values.
fn format_go_float_f<T: GoScientificFloat>(value: T) -> String {
    value
        .special()
        .map_or_else(|| value.to_string(), str::to_owned)
}

/// Go `strconv.FormatFloat(value, 'e', -1, bitSize)` differs from Rust's
/// lower-exponent display only in special values and exponent normalization.
fn format_go_float_e<T: GoScientificFloat>(value: T) -> String {
    if let Some(special) = value.special() {
        return special.to_owned();
    }
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("Rust scientific float contains an exponent");
    let exponent: i32 = exponent.parse().expect("Rust float exponent is numeric");
    let sign = if exponent < 0 { '-' } else { '+' };
    format!("{mantissa}e{sign}{:02}", exponent.unsigned_abs())
}

#[cfg(test)]
mod tests {
    use super::Datum;

    #[test]
    fn sql_float_stringification_matches_go_special_values() {
        assert_eq!(Datum::Real(f64::INFINITY).sql_string().unwrap(), "+Inf");
        assert_eq!(Datum::Real(f64::NEG_INFINITY).sql_string().unwrap(), "-Inf");
        assert_eq!(Datum::Real(f64::NAN).sql_string().unwrap(), "NaN");
        assert_eq!(Datum::Float32(f64::INFINITY).sql_string().unwrap(), "+Inf");
        assert_eq!(
            Datum::Float32(-3.1111111).sql_string().unwrap(),
            "-3.1111112"
        );
    }

    /// Source: `pkg/types/datum.go::GetString` / `GetBytes`. Diagnostics may
    /// encode arbitrary octets, but semantic stringification must reject
    /// invalid UTF-8 instead of replacing or reinterpreting it.
    #[test]
    fn diagnostic_labels_are_lossless_but_sql_stringification_is_checked() {
        assert_eq!(Datum::new_string("TiDB").label(), "STR:TiDB");

        let invalid = Datum::new_bytes(vec![0xff, 0, b'A']);
        assert_eq!(invalid.label(), "STR_HEX:FF0041");
        assert!(invalid.sql_string().is_err());

        let embedded_nul = Datum::new_string(vec![b'a', 0, b'b']);
        assert_eq!(embedded_nul.label().as_bytes(), b"STR:a\0b");
        assert_eq!(embedded_nul.sql_string().unwrap().as_bytes(), b"a\0b");

        let raw_enum = Datum::new_enum(crate::MysqlEnum::new([0xff], 1), crate::Collation::Binary);
        assert_eq!(raw_enum.sql_bytes().unwrap(), [0xff]);
        assert_eq!(raw_enum.to_bytes().unwrap(), [0xff]);
        assert!(raw_enum.sql_string().is_err());
        assert_eq!(raw_enum.label(), "ENUM_HEX:FF");

        let raw_set = Datum::new_set(crate::MysqlSet::new([0xfe], 1), crate::Collation::Binary);
        assert_eq!(raw_set.sql_bytes().unwrap(), [0xfe]);
        assert_eq!(raw_set.to_bytes().unwrap(), [0xfe]);
        assert!(raw_set.sql_string().is_err());
        assert_eq!(raw_set.label(), "SET_HEX:FE");
    }

    /// Source `BenchmarkDatumTruncatedStringify` inputs plus the byte-boundary
    /// contract exercised by the implementation.
    #[test]
    fn source_datum_truncated_stringify_rows() {
        let long = Datum::new_string("1".repeat(128));
        assert_eq!(
            long.truncated_stringify().unwrap(),
            format!("{}...(len:128)", "1".repeat(64)).into_bytes()
        );
        assert_eq!(
            Datum::new_int(2).truncated_stringify().unwrap(),
            b"2".to_vec()
        );
        let split_utf8 = Datum::new_string(format!("{}é", "a".repeat(63)));
        assert_eq!(
            split_utf8.truncated_stringify().unwrap(),
            [
                "a".repeat(63).into_bytes(),
                vec![0xc3],
                b"...(len:65)".to_vec()
            ]
            .concat()
        );
    }
}
