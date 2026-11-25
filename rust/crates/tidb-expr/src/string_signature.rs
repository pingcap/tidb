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
// See the License for the specific language governing permissions and
// limitations under the License.

//! The binary-vs-UTF-8 signature split shared by the string builtins.
//!
//! Mirrors `pkg/expression/builtin_string.go`, where every position- or
//! length-sensitive string function is built as ONE of a signature PAIR:
//! `substringFunctionClass.getFunction` branches on
//! `types.IsBinaryStr(args[0].GetType(...))` to install
//! `builtinSubstring3ArgsSig` (bytes) or `builtinSubstring3ArgsUTF8Sig`
//! (characters), and `LEFT`, `RIGHT`, `REVERSE`, `INSERT`, `LPAD`/`RPAD`,
//! `LOCATE`/`INSTR`, `UPPER`/`LOWER` and `CHAR_LENGTH` follow the same shape.
//! The choice is made ONCE, from the argument's charset, and is never
//! re-decided inside the per-row arithmetic.
//!
//! Go reads the charset off the argument's `FieldType`. This evaluator's
//! [`Datum`] carries the same fact — a binary-charset value is a
//! `Datum::Bytes`, a bit/hex literal, or a `Datum::String` whose registered
//! collation is `binary` — so [`is_binary_str`] is the single place that
//! reading happens. `CHAR_LENGTH`, whose argument may be a bare
//! `Expr::Column` with no value yet, additionally has the AST-typed seam in
//! [`crate::build`]; both answer the same question with the same rule.
//!
//! [`StrUnits`] then makes the two signatures ONE implementation: the
//! selected charset decides what a "unit" is (one byte, or one character's
//! bytes), and each builtin indexes units without knowing which it got.

use std::borrow::Cow;

use tidb_datatype::Datum;
use tidb_charset::Collation;
use tidb_goruntime::decode_go_utf8_lossy;

use crate::coerce::coerce_str_bytes;
use crate::EvalError;

/// Ports `types.IsBinaryStr` to the datum seam: is this argument a
/// binary-charset string, so its builtin gets the byte-slicing signature?
///
/// Non-string arguments answer `false`, matching Go: an integer argument is
/// converted through `ETString` and carries the connection charset, not
/// `binary`.
pub(crate) fn is_binary_str(value: &Datum) -> bool {
    match value {
        // Go's `DefaultTypeForValue` gives bit and hex literals a binary
        // string type, and `builtinReverseSig` is additionally selected for
        // `IsTypeBit`.
        Datum::Bytes(_) | Datum::BinaryLiteral(_) | Datum::Bit(_) => true,
        Datum::String(value) => value.collation() == Collation::Binary,
        Datum::Enum(_, collation) | Datum::Set(_, collation) => *collation == Collation::Binary,
        _ => false,
    }
}

/// A string argument viewed as the unit sequence its selected signature
/// slices: raw bytes for a binary signature, characters for a UTF-8 one.
///
/// Holding the byte payload (rather than a `Vec<char>`) is what lets the
/// binary signature preserve arbitrary octets the way Go strings do, while
/// the UTF-8 signature still counts and cuts on character boundaries.
pub(crate) enum StrUnits<'a> {
    Binary(Cow<'a, [u8]>),
    Text(Cow<'a, str>),
}

impl<'a> StrUnits<'a> {
    /// Selects the signature for `value` and produces its unit view, or
    /// `None` when the argument is `NULL`.
    ///
    /// A UTF-8 signature may still meet invalid UTF-8 (a `CONCAT` of a binary
    /// tail, for one). Such bytes become one unit each, matching Go's
    /// `[]rune` conversion, which yields one `RuneError` per malformed byte
    /// rather than failing.
    pub(crate) fn of(value: &'a Datum) -> Result<Option<Self>, EvalError> {
        Self::of_with_signature(value, is_binary_str(value))
    }

    /// Views `value` under an explicitly chosen signature, for the functions
    /// whose signature comes from the RESULT charset rather than from the
    /// sliced argument alone — `insertFunctionClass.getFunction` tests
    /// `types.IsBinaryStr(bf.tp)` after `addBinFlag`, so a binary
    /// replacement string makes `INSERT` slice `str` by bytes too.
    pub(crate) fn of_with_signature(
        value: &'a Datum,
        binary: bool,
    ) -> Result<Option<Self>, EvalError> {
        let bytes = match value.as_raw_bytes() {
            Some(bytes) => Cow::Borrowed(bytes),
            None => match coerce_str_bytes(value)? {
                Some(bytes) => Cow::Owned(bytes),
                None => return Ok(None),
            },
        };
        Ok(Some(Self::from_bytes(bytes, binary)))
    }

    pub(crate) fn from_bytes(bytes: Cow<'a, [u8]>, binary: bool) -> Self {
        if binary {
            return Self::Binary(bytes);
        }

        Self::Text(match bytes {
            Cow::Borrowed(bytes) => go_utf8_text(bytes),
            Cow::Owned(bytes) => Cow::Owned(
                String::from_utf8(bytes).unwrap_or_else(|error| decode_go_utf8_lossy(error.as_bytes())),
            ),
        })
    }

    /// The number of units used for negative positions and length clamps.
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Binary(bytes) => bytes.len(),
            Self::Text(text) => text.chars().count(),
        }
    }

    /// The bytes of units `start..end`; both indices are clamped into range,
    /// and an inverted range yields the empty string.
    pub(crate) fn slice(&self, start: usize, end: usize) -> &[u8] {
        let width = end.saturating_sub(start);
        match self {
            Self::Binary(bytes) => {
                let suffix = &bytes[start.min(bytes.len())..];
                &suffix[..width.min(suffix.len())]
            }
            Self::Text(text) => {
                let suffix = &text[byte_offset(text, start)..];
                &suffix.as_bytes()[..byte_offset(suffix, width)]
            }
        }
    }

    /// The last `count` units, clamped to the length of the input.
    pub(crate) fn tail(&self, count: usize) -> &[u8] {
        if count == 0 {
            return b"";
        }
        match self {
            Self::Binary(bytes) => &bytes[bytes.len().saturating_sub(count)..],
            Self::Text(text) => {
                let start = text
                    .char_indices()
                    .nth_back(count - 1)
                    .map_or(0, |(offset, _)| offset);
                &text.as_bytes()[start..]
            }
        }
    }

    /// The whole payload, for the signatures that only reorder or search it.
    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            Self::Binary(bytes) => bytes,
            Self::Text(text) => text.as_bytes(),
        }
    }

    /// Reverses bytes or whole runes according to the selected signature.
    pub(crate) fn reversed(&self) -> Datum {
        let bytes = match self {
            Self::Text(text) if !text.is_ascii() => {
                let mut reversed = String::with_capacity(text.len());
                reversed.extend(text.chars().rev());
                reversed.into_bytes()
            }
            _ => {
                let mut reversed = self.bytes().to_vec();
                reversed.reverse();
                reversed
            }
        };
        self.pack(bytes)
    }

    /// Rebuilds a result value in the charset this signature was selected
    /// for, so a binary argument keeps producing a binary result the way
    /// `SetBinFlagOrBinStr` arranges in Go.
    pub(crate) fn pack(&self, bytes: Vec<u8>) -> Datum {
        match self {
            Self::Binary(_) => Datum::new_bytes_datum(bytes),
            Self::Text(_) => Datum::new_string_datum(bytes),
        }
    }
}

/// Go's `string([]rune(bytes))`, borrowing memory when UTF-8 is already valid.
pub(crate) fn go_utf8_text(bytes: &[u8]) -> Cow<'_, str> {
    match simdutf8::basic::from_utf8(bytes) {
        Ok(text) => Cow::Borrowed(text),
        Err(_) => Cow::Owned(decode_go_utf8_lossy(bytes)),
    }
}

fn byte_offset(text: &str, units: usize) -> usize {
    if units >= text.len() {
        return text.len();
    }
    text.char_indices()
        .nth(units)
        .map_or(text.len(), |(offset, _)| offset)
}

#[cfg(test)]
mod tests {
    use super::{is_binary_str, StrUnits};
    use tidb_datatype::{Datum, StringDatum};
use tidb_charset::Collation;

    #[test]
    fn binary_charset_selects_the_byte_signature() {
        // `aéb` is four bytes and three characters, so byte and character
        // answers cannot coincide.
        let binary = Datum::new_bytes_datum("aéb".as_bytes().to_vec());
        let text = Datum::new_string_datum("aéb".to_string());
        assert!(is_binary_str(&binary));
        assert!(!is_binary_str(&text));
        assert_eq!(StrUnits::of(&binary).unwrap().unwrap().len(), 4);
        assert_eq!(StrUnits::of(&text).unwrap().unwrap().len(), 3);
    }

    #[test]
    fn binary_collation_string_is_a_binary_signature() {
        let value = Datum::String(StringDatum::new(
            "aéb".as_bytes().to_vec(),
            Collation::Binary,
        ));
        assert!(is_binary_str(&value));
        assert_eq!(StrUnits::of(&value).unwrap().unwrap().len(), 4);
    }

    #[test]
    fn slicing_clamps_and_keeps_charset() {
        let text = Datum::new_string_datum("aéb".to_string());
        let units = StrUnits::of(&text).unwrap().unwrap();
        assert_eq!(units.slice(1, 2), "é".as_bytes());
        assert_eq!(units.slice(2, 99), b"b");
        assert_eq!(units.slice(3, 1), b"");
        assert_eq!(
            units.pack(units.slice(0, 2).to_vec()),
            Datum::new_string_datum("aé".to_string())
        );

        let binary = Datum::new_bytes_datum("aéb".as_bytes().to_vec());
        let units = StrUnits::of(&binary).unwrap().unwrap();
        assert_eq!(units.slice(1, 3), &[0xC3, 0xA9]);
        assert_eq!(
            units.pack(units.slice(1, 3).to_vec()),
            Datum::new_bytes_datum(vec![0xC3, 0xA9])
        );
    }

    #[test]
    fn malformed_utf8_groups_one_unit_per_bad_byte() {
        // Go's `[]rune("a\xffb")` is three runes: 'a', RuneError, 'b'.
        let value = Datum::new_string_datum(vec![b'a', 0xFF, b'b']);
        let units = StrUnits::of(&value).unwrap().unwrap();
        assert_eq!(units.len(), 3);
        assert_eq!(units.slice(1, 2), "\u{fffd}".as_bytes());
        assert_eq!(
            units.pack(units.slice(0, 2).to_vec()),
            Datum::new_string_datum("a\u{fffd}")
        );
    }

    #[test]
    fn enum_and_set_use_their_stored_charset_for_signature_selection() {
        use tidb_datatype::{MysqlEnum, MysqlSet};

        let binary_enum = Datum::new_enum(MysqlEnum::new([0xff], 1), Collation::Binary);
        let binary_set = Datum::new_set(MysqlSet::new([0xfe], 1), Collation::Binary);
        assert!(is_binary_str(&binary_enum));
        assert!(is_binary_str(&binary_set));
        assert_eq!(
            StrUnits::of(&binary_enum).unwrap().unwrap().slice(0, 1),
            [0xff]
        );

        let text_enum = Datum::new_enum(MysqlEnum::new([0xff], 1), Collation::Utf8Mb4Bin);
        assert!(!is_binary_str(&text_enum));
        assert_eq!(
            StrUnits::of(&text_enum).unwrap().unwrap().slice(0, 1),
            "\u{fffd}".as_bytes()
        );
    }

    #[test]
    fn null_has_no_units() {
        assert!(StrUnits::of(&Datum::Null).unwrap().is_none());
    }
}
