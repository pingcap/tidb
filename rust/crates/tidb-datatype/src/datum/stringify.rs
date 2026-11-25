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

use std::borrow::Cow;

use memchr::{memchr2, memchr2_iter, memchr_iter, memmem};
use tidb_goruntime::go_hex::{append_uppercase, encode_uppercase_to_string as encode_hex};

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
    unwrap_from_single_quotes_ref(value).into_owned()
}

/// Removes the outer quotes and collapses doubled backslashes and quotes.
/// Borrows the original bytes when the remaining text has no doubled escapes.
#[must_use]
#[inline]
pub fn unwrap_from_single_quotes_ref(value: &[u8]) -> Cow<'_, [u8]> {
    let [b'\'', body @ .., b'\''] = value else {
        return Cow::Borrowed(value);
    };
    let Some(first) = memchr2(b'\'', b'\\', body) else {
        return Cow::Borrowed(body);
    };
    unescape_single_quoted_body(body, first)
}

fn unescape_single_quoted_body(body: &[u8], first: usize) -> Cow<'_, [u8]> {
    let tail = &body[first..];
    if tail.len() <= 64 {
        let Some(index) = tail
            .windows(2)
            .position(|pair| is_escape(pair[0]) && pair[0] == pair[1])
        else {
            return Cow::Borrowed(body);
        };
        let mut output = Vec::with_capacity(body.len() - 1);
        output.extend_from_slice(&body[..first + index + 1]);
        let mut read = index + 2;
        while read < tail.len() {
            let byte = tail[read];
            output.push(byte);
            if is_escape(byte) && tail.get(read + 1) == Some(&byte) {
                read += 2;
            } else {
                read += 1;
            }
        }
        return Cow::Owned(output);
    }

    // With frequent escape characters, searching for each individual match
    // costs more than collapsing the two kinds in separate passes.
    if tail[..32].iter().filter(|&&byte| is_escape(byte)).count() >= 4 {
        return match (memmem::find(tail, b"\\\\").is_some(), memmem::find(tail, b"''").is_some()) {
            (true, true) => Cow::Owned(collapse_byte_pair::<b'\''>(&collapse_byte_pair::<b'\\'>(body))),
            (true, false) => Cow::Owned(collapse_byte_pair::<b'\\'>(body)),
            (false, true) => Cow::Owned(collapse_byte_pair::<b'\''>(body)),
            (false, false) => Cow::Borrowed(body),
        };
    }

    let mut start = 0;
    let mut output = Vec::new();
    for offset in memchr2_iter(b'\'', b'\\', tail) {
        let index = first + offset;
        if index >= start && body.get(index + 1) == Some(&body[index]) {
            if start == 0 {
                output.reserve_exact(body.len() - 1);
            }
            output.extend_from_slice(&body[start..=index]);
            start = index + 2;
        }
    }
    if start == 0 {
        Cow::Borrowed(body)
    } else {
        output.extend_from_slice(&body[start..]);
        Cow::Owned(output)
    }
}

fn is_escape(byte: u8) -> bool {
    matches!(byte, b'\'' | b'\\')
}

fn collapse_byte_pair<const BYTE: u8>(value: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(value.len());
    let mut index = 0;
    while index < value.len() {
        if value[index] == BYTE && value.get(index + 1) == Some(&BYTE) {
            output.push(BYTE);
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
            // Go's oracle labels `KindMysqlTime` through `labelString`, the
            // same `STR:`/`STR_HEX:` contract a string takes
            // (`difftests/goeval/main.go`). A `TIME:` label of its own read
            // as a divergence for every `CAST(... AS DATE)` in the corpus
            // when only the two labellers disagreed -- the values were
            // identical -- and a unit test papered over it by rewriting the
            // oracle's `STR:` into `TIME:` before comparing.
            //
            // `DUR:`, `ENUM:`, `SET:`, `JSON:` and the rest below stay
            // richer than the oracle on purpose: Go's `labelDatum` has no
            // case for those kinds, so it answers `SKIP:<kind>` and no
            // golden ever asserts them.
            Self::Time(value) => label_bytes("STR", value.to_string().as_bytes()),
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
        Ok(self.sql_text()?.into_owned())
    }

    fn sql_text(&self) -> Result<Cow<'_, [u8]>, DatumStringError> {
        let value = match self {
            Self::Int(value) => value.to_string().into_bytes(),
            Self::UInt(value) => value.to_string().into_bytes(),
            Self::Decimal(value) => value.to_string().into_bytes(),
            Self::Real(value) => crate::go_strconv::format_float(*value, b'f', -1, 64).into_bytes(),
            Self::Float32(value) => {
                crate::go_strconv::format_float(f64::from(*value as f32), b'f', -1, 32).into_bytes()
            }
            Self::String(value) => return Ok(Cow::Borrowed(value.bytes())),
            Self::Bytes(value) => return Ok(Cow::Borrowed(value)),
            Self::BinaryLiteral(value) | Self::Bit(value) => {
                return Ok(Cow::Borrowed(value.as_bytes()));
            }
            Self::Duration(value) => value.to_string().into_bytes(),
            Self::Enum(value, _) => return Ok(Cow::Borrowed(value.name_bytes())),
            Self::Set(value, _) => return Ok(Cow::Borrowed(value.name_bytes())),
            Self::Time(value) => value.to_string().into_bytes(),
            Self::Json(value) => value.to_string().into_bytes(),
            Self::Raw(value) => {
                std::str::from_utf8(value).map_err(DatumStringError::InvalidUtf8)?;
                return Ok(Cow::Borrowed(value));
            }
            Self::VectorFloat32(value) => value.to_string().into_bytes(),
            Self::Null => return Ok(Cow::Borrowed(&[])),
            Self::MinNotNull => {
                return Err(DatumStringError::RangeSentinel(DatumKind::MinNotNull));
            }
            Self::MaxValue => {
                return Err(DatumStringError::RangeSentinel(DatumKind::MaxValue));
            }
        };
        Ok(Cow::Owned(value))
    }

    /// UTF-8 convenience projection of [`Self::sql_bytes`]. Invalid source
    /// bytes return an error rather than being silently replaced.
    pub fn sql_string(&self) -> Result<String, DatumStringError> {
        String::from_utf8(self.sql_bytes()?)
            .map_err(|error| DatumStringError::InvalidUtf8(error.utf8_error()))
    }

    /// Source `Datum.TruncatedStringify` used by EXPLAIN and diagnostics:
    /// only a string, bytes or JSON value is truncated; every other kind
    /// prints as Go's `%v` of `Datum.GetValue()`.
    #[inline]
    pub fn truncated_stringify(&self) -> Result<Vec<u8>, DatumStringError> {
        self.truncated_stringify_ref().map(Cow::into_owned)
    }

    /// Renders short diagnostic text, borrowing the bytes when no formatting
    /// or truncation is needed.
    #[inline]
    pub fn truncated_stringify_ref(&self) -> Result<Cow<'_, [u8]>, DatumStringError> {
        match self {
            Self::String(value) => Ok(truncate_diagnostic_bytes(Cow::Borrowed(value.bytes()))),
            Self::Bytes(value) => Ok(truncate_diagnostic_bytes(Cow::Borrowed(value))),
            Self::Null | Self::MinNotNull | Self::MaxValue => Ok(Cow::Borrowed(b"<nil>")),
            other => other.format_diagnostic_text(),
        }
    }

    fn format_diagnostic_text(&self) -> Result<Cow<'_, [u8]>, DatumStringError> {
        let value = match self {
            Self::Json(value) => {
                return Ok(truncate_diagnostic_bytes(Cow::Owned(value.to_string().into_bytes())));
            }
            Self::VectorFloat32(value) => value.truncated_string().into_bytes(),
            Self::Int(value) => value.to_string().into_bytes(),
            Self::UInt(value) => value.to_string().into_bytes(),
            Self::Real(value) => crate::go_strconv::format_float(*value, b'g', -1, 64).into_bytes(),
            Self::Float32(value) => {
                crate::go_strconv::format_float(f64::from(*value as f32), b'g', -1, 32).into_bytes()
            }
            Self::BinaryLiteral(value) | Self::Bit(value) => value.to_hex_bytes(),
            other => return other.sql_text(),
        };
        Ok(Cow::Owned(value))
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
            Self::Float32(value) => {
                crate::go_strconv::format_float(f64::from(*value as f32), b'e', -1, 32).into_bytes()
            }
            Self::Real(value) => crate::go_strconv::format_float(*value, b'e', -1, 64).into_bytes(),
            Self::String(value) => quote_value_expr(value.bytes()),
            Self::Bytes(value) => quote_value_expr(value),
            Self::BinaryLiteral(value) => value.to_bit_literal_string(true).into_bytes(),
            Self::Decimal(value) => value.to_string().into_bytes(),
            other => return Err(DatumValueError::Unsupported(other.kind(), target)),
        };
        Ok(value)
    }
}

/// Source `DatumsToString`, byte-authoritative: a Go string holds any
/// bytes, so a string datum's bytes pass through unchanged.
pub fn datums_to_bytes(
    datums: &[Datum],
    handle_special_values: bool,
    binary_as_hex: bool,
) -> Result<Vec<u8>, DatumStringError> {
    const LOG_DATUM_LEN: usize = 2048;
    let mut output = Vec::new();
    if datums.len() > 1 {
        output.reserve(8 * datums.len());
        output.push(b'(');
    }
    for (index, datum) in datums.iter().enumerate() {
        if index != 0 {
            output.extend_from_slice(b", ");
        }
        if handle_special_values {
            match datum {
                Datum::Null => {
                    output.extend_from_slice(b"NULL");
                    continue;
                }
                Datum::MinNotNull => {
                    output.extend_from_slice(b"-inf");
                    continue;
                }
                Datum::MaxValue => {
                    output.extend_from_slice(b"+inf");
                    continue;
                }
                _ => {}
            }
        }
        let text = datum.sql_text()?;
        let length = text.len();
        let text = &text[..length.min(LOG_DATUM_LEN)];
        let mut number = itoa::Buffer::new();
        let length_suffix = if length > LOG_DATUM_LEN {
            number.format(length).as_bytes()
        } else {
            b""
        };
        let quoted = matches!(datum, Datum::String(_));
        let hex = quoted && binary_as_hex && !is_printable(text);
        let body_len = if hex {
            2 * text.len() + 2
        } else {
            text.len() + 2 * usize::from(quoted)
        };
        let suffix_len = length_suffix.len() + 6 * usize::from(!length_suffix.is_empty());
        output.reserve(body_len + suffix_len);
        if hex {
            output.extend_from_slice(b"0x");
            append_uppercase(&mut output, text);
        } else if quoted {
            output.push(b'"');
            output.extend_from_slice(text);
            output.push(b'"');
        } else {
            output.extend_from_slice(text);
        }
        if !length_suffix.is_empty() {
            output.extend_from_slice(b" len(");
            output.extend_from_slice(length_suffix);
            output.push(b')');
        }
    }
    if datums.len() > 1 {
        output.push(b')');
    }
    Ok(output)
}

/// UTF-8 convenience projection of [`datums_to_bytes`]. Invalid source
/// bytes return an error rather than being silently replaced.
pub fn datums_to_string(
    datums: &[Datum],
    handle_special_values: bool,
    binary_as_hex: bool,
) -> Result<String, DatumStringError> {
    String::from_utf8(datums_to_bytes(
        datums,
        handle_special_values,
        binary_as_hex,
    )?)
    .map_err(|error| DatumStringError::InvalidUtf8(error.utf8_error()))
}

/// Source `DatumsToStrNoErr`.
pub fn datums_to_str_no_err(datums: &[Datum]) -> String {
    datums_to_string(datums, true, false).unwrap_or_default()
}

/// Source `DatumsToStrNoErrSmart`.
pub fn datums_to_str_no_err_smart(datums: &[Datum]) -> String {
    datums_to_string(datums, true, true).unwrap_or_default()
}

/// Source printable-string predicate.
pub fn is_printable(value: &[u8]) -> bool {
    const ONES: u128 = u128::from_ne_bytes([1; 16]);
    const HIGH: u128 = u128::from_ne_bytes([0x80; 16]);
    let (blocks, tail) = value.as_chunks::<16>();
    let mut high_bits = 0;
    for block in blocks {
        let bytes = u128::from_ne_bytes(*block);
        let del = bytes ^ (ONES * 0x7f);
        // These lane tests report whether any byte is an ASCII control.
        // A borrow into the next lane can only occur after a matching byte.
        if (bytes.wrapping_sub(ONES * 0x20) & !bytes & HIGH)
            | (del.wrapping_sub(ONES) & !del & HIGH)
            != 0
        {
            return false;
        }
        high_bits |= bytes & HIGH;
    }
    if tail.iter().any(u8::is_ascii_control) {
        return false;
    }
    (high_bits == 0 && tail.is_ascii())
        || simdutf8::basic::from_utf8(value).is_ok_and(|text| !text.chars().any(char::is_control))
}

#[inline]
fn truncate_diagnostic_bytes(value: Cow<'_, [u8]>) -> Cow<'_, [u8]> {
    const MAX_LEN: usize = 64;
    if value.len() <= MAX_LEN {
        return value;
    }
    const SUFFIX: &[u8] = b"...(len:";
    let mut buffer = itoa::Buffer::new();
    let length = buffer.format(value.len()).as_bytes();
    let mut output = match value {
        Cow::Borrowed(bytes) => {
            let mut output = Vec::with_capacity(MAX_LEN + SUFFIX.len() + length.len() + 1);
            output.extend_from_slice(&bytes[..MAX_LEN]);
            output
        }
        Cow::Owned(mut bytes) => {
            bytes.truncate(MAX_LEN);
            bytes
        }
    };
    output.extend_from_slice(SUFFIX);
    output.extend_from_slice(length);
    output.push(b')');
    Cow::Owned(output)
}

fn label_bytes(kind: &str, bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => format!("{kind}:{text}"),
        Err(_) => format!("{kind}_HEX:{}", encode_hex(bytes)),
    }
}

fn quote_value_expr(value: &[u8]) -> Vec<u8> {
    quote_literal::<true>(value)
}

fn quote_value_expr_bytes(value: &[u8]) -> Vec<u8> {
    quote_literal::<false>(value)
}

fn quote_literal<const ESCAPE_BACKSLASH: bool>(value: &[u8]) -> Vec<u8> {
    let other = if ESCAPE_BACKSLASH { b'\\' } else { b'\'' };
    let should_escape = |byte| byte == b'\'' || byte == other;
    let mut matches = memchr2_iter(b'\'', other, value);
    let Some(first) = matches.next() else {
        let mut quoted = Vec::with_capacity(value.len() + 2);
        quoted.push(b'\'');
        quoted.extend_from_slice(value);
        quoted.push(b'\'');
        return quoted;
    };
    let escaped = &value[first..];
    let count = if escaped.len() < 128 {
        escaped.iter().filter(|&&byte| should_escape(byte)).count()
    } else {
        memchr_iter(b'\'', escaped).count()
            + if ESCAPE_BACKSLASH {
                memchr_iter(b'\\', escaped).count()
            } else {
                0
            }
    };
    let mut quoted = Vec::with_capacity(value.len() + count + 2);
    quoted.push(b'\'');
    quoted.extend_from_slice(&value[..first]);
    if count > (value.len() - first) / 8 {
        for &byte in &value[first..] {
            quoted.push(byte);
            if should_escape(byte) {
                quoted.push(byte);
            }
        }
    } else {
        let mut start = first;
        for index in std::iter::once(first).chain(matches) {
            quoted.extend_from_slice(&value[start..=index]);
            quoted.push(value[index]);
            start = index + 1;
        }
        quoted.extend_from_slice(&value[start..]);
    }
    quoted.push(b'\'');
    quoted
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::Datum;

    #[test]
    fn diagnostics_limit_bytes_before_checking_printability() {
        use super::{datums_to_bytes, datums_to_string};

        let mut text = vec![b'x'; 2048];
        text.push(0);
        let mut expected = vec![b'"'];
        expected.extend_from_slice(&text[..2048]);
        expected.extend_from_slice(b"\" len(2049)");
        assert_eq!(
            datums_to_bytes(&[Datum::new_string_datum(text)], true, true).unwrap(),
            expected
        );

        let mut text = vec![b'x'; 2047];
        text.extend_from_slice("é".as_bytes());
        let values = [Datum::new_string_datum(text)];
        let mut expected = b"0x".to_vec();
        expected.extend(b"78".repeat(2047));
        expected.extend_from_slice(b"C3 len(2049)");
        assert_eq!(datums_to_bytes(&values, true, true).unwrap(), expected);
        assert!(datums_to_string(&values, true, false).is_err());
    }

    #[test]
    fn printable_prefix_and_tail_match_unicode_control_test() {
        use super::is_printable;

        for offset in 0..34 {
            for byte in u8::MIN..=u8::MAX {
                let mut bytes = [b'x'; 34];
                bytes[offset] = byte;
                let expected = byte.is_ascii() && !byte.is_ascii_control();
                assert_eq!(is_printable(&bytes), expected, "{offset} {byte}");
            }
        }
        assert!(is_printable("界\u{fffd}".repeat(9).as_bytes()));
        assert!(!is_printable("界\u{0085}".repeat(9).as_bytes()));
    }

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
        assert_eq!(Datum::new_string_datum("TiDB").label(), "STR:TiDB");

        let invalid = Datum::new_bytes_datum(vec![0xff, 0, b'A']);
        assert_eq!(invalid.label(), "STR_HEX:FF0041");
        assert!(invalid.sql_string().is_err());

        let embedded_nul = Datum::new_string_datum(vec![b'a', 0, b'b']);
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
        let short = Datum::new_bytes_datum([0xff, b'x']);
        let Cow::Borrowed(bytes) = short.truncated_stringify_ref().unwrap() else {
            panic!("short diagnostic bytes should borrow from the datum");
        };
        assert_eq!(bytes, [0xff, b'x']);
        assert_eq!(bytes.as_ptr(), short.go_bytes().as_ptr());

        let long = Datum::new_string_datum("1".repeat(128));
        assert_eq!(
            long.truncated_stringify().unwrap(),
            format!("{}...(len:128)", "1".repeat(64)).into_bytes()
        );
        assert_eq!(
            Datum::new_int(2).truncated_stringify().unwrap(),
            b"2".to_vec()
        );
        // Go `TruncatedStringify` of each remaining kind (`zzprobe/truncstr`):
        // nothing but a string, bytes or JSON value is truncated.
        let duration = crate::MySqlDuration::new(12, 34, 56, 500_000, 1).unwrap();
        for (datum, expected) in [
            (Datum::Real(1e300), "1e+300"),
            (Datum::Real(1_000_000.0), "1e+06"),
            (Datum::Real(123456.5), "123456.5"),
            (Datum::Real(-0.00001), "-1e-05"),
            (Datum::Real(1.8446744073709552e19), "1.8446744073709552e+19"),
            (Datum::Float32(1.5e10), "1.5e+10"),
            (Datum::Float32(f64::from(0.1_f32)), "0.1"),
            (
                Datum::new_decimal(crate::Decimal::from_f64(8.1e80).unwrap()),
                "810000000000000000000000000000000000000000000000000000000000000000000000000000000",
            ),
            (Datum::Duration(duration), "12:34:56.5"),
            (
                Datum::Enum(crate::MysqlEnum::new("bb", 2), tidb_charset::Collation::Utf8Mb4Bin),
                "bb",
            ),
            (
                Datum::Set(crate::MysqlSet::new("a,bb", 3), tidb_charset::Collation::Utf8Mb4Bin),
                "a,bb",
            ),
            (
                Datum::BinaryLiteral(crate::BinaryLiteral::from(vec![0xff, 0x01])),
                "0xff01",
            ),
            (Datum::Bit(crate::BinaryLiteral::from(vec![0x80])), "0x80"),
            (Datum::BinaryLiteral(crate::BinaryLiteral::ZERO), ""),
            (Datum::Null, "<nil>"),
            (Datum::MinNotNull, "<nil>"),
            (Datum::MaxValue, "<nil>"),
            (
                Datum::Bytes(vec![b'x'; 70]),
                "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx...(len:70)",
            ),
            (Datum::UInt(u64::MAX), "18446744073709551615"),
        ] {
            assert_eq!(
                datum.truncated_stringify().unwrap(),
                expected.as_bytes(),
                "{datum:?}"
            );
        }
        let split_utf8 = Datum::new_string_datum(format!("{}é", "a".repeat(63)));
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
