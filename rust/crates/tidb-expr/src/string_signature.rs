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

use tidb_datatype::{Collation, Datum};

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
        _ => false,
    }
}

/// A string argument viewed as the unit sequence its selected signature
/// slices: raw bytes for a binary signature, characters for a UTF-8 one.
///
/// Holding the byte payload (rather than a `Vec<char>`) is what lets the
/// binary signature preserve arbitrary octets the way Go strings do, while
/// the UTF-8 signature still counts and cuts on character boundaries.
pub(crate) struct StrUnits {
    bytes: Vec<u8>,
    /// Byte offset of every unit start, plus the total length as terminator,
    /// so `bounds.len() == len() + 1`.
    bounds: Vec<usize>,
    binary: bool,
}

impl StrUnits {
    /// Selects the signature for `value` and produces its unit view, or
    /// `None` when the argument is `NULL`.
    ///
    /// A UTF-8 signature may still meet invalid UTF-8 (a `CONCAT` of a binary
    /// tail, for one). Such bytes become one unit each, matching Go's
    /// `[]rune` conversion, which yields one `RuneError` per malformed byte
    /// rather than failing.
    pub(crate) fn of(value: &Datum) -> Result<Option<Self>, EvalError> {
        let Some(bytes) = coerce_str_bytes(value)? else {
            return Ok(None);
        };
        Ok(Some(Self::from_bytes(bytes, is_binary_str(value))))
    }

    /// Views `value` under an explicitly chosen signature, for the functions
    /// whose signature comes from the RESULT charset rather than from the
    /// sliced argument alone — `insertFunctionClass.getFunction` tests
    /// `types.IsBinaryStr(bf.tp)` after `addBinFlag`, so a binary
    /// replacement string makes `INSERT` slice `str` by bytes too.
    pub(crate) fn of_with_signature(
        value: &Datum,
        binary: bool,
    ) -> Result<Option<Self>, EvalError> {
        let Some(bytes) = coerce_str_bytes(value)? else {
            return Ok(None);
        };
        Ok(Some(Self::from_bytes(bytes, binary)))
    }

    fn from_bytes(bytes: Vec<u8>, binary: bool) -> Self {
        let mut bounds = Vec::with_capacity(bytes.len() + 1);
        if binary {
            bounds.extend(0..=bytes.len());
        } else {
            let mut offset = 0;
            while offset < bytes.len() {
                bounds.push(offset);
                offset += char_width(&bytes[offset..]);
            }
            bounds.push(bytes.len());
        }
        Self {
            bytes,
            bounds,
            binary,
        }
    }

    /// The number of units, which is what `CHAR_LENGTH`, `LEFT`'s clamp and
    /// `SUBSTRING`'s position arithmetic all count in.
    pub(crate) fn len(&self) -> usize {
        self.bounds.len() - 1
    }

    /// The bytes of units `start..end`; both indices are clamped into range,
    /// and an inverted range yields the empty string.
    pub(crate) fn slice(&self, start: usize, end: usize) -> &[u8] {
        let start = start.min(self.len());
        let end = end.clamp(start, self.len());
        &self.bytes[self.bounds[start]..self.bounds[end]]
    }

    /// The whole payload, for the signatures that only reorder or search it.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The bytes of each unit, in order.
    pub(crate) fn units(&self) -> impl DoubleEndedIterator<Item = &[u8]> {
        self.bounds
            .windows(2)
            .map(move |window| &self.bytes[window[0]..window[1]])
    }

    /// Rebuilds a result value in the charset this signature was selected
    /// for, so a binary argument keeps producing a binary result the way
    /// `SetBinFlagOrBinStr` arranges in Go.
    pub(crate) fn pack(&self, bytes: Vec<u8>) -> Datum {
        if self.binary {
            Datum::new_bytes(bytes)
        } else {
            Datum::new_string(bytes)
        }
    }
}

/// The byte width of the leading character, with Go's `utf8.DecodeRune`
/// fallback of one byte per malformed encoding.
fn char_width(bytes: &[u8]) -> usize {
    let leading = match std::str::from_utf8(bytes) {
        Ok(text) => text,
        Err(error) if error.valid_up_to() > 0 => {
            std::str::from_utf8(&bytes[..error.valid_up_to()]).expect("prefix is valid UTF-8")
        }
        Err(_) => return 1,
    };
    leading
        .chars()
        .next()
        .map_or(1, |character| character.len_utf8())
}

#[cfg(test)]
mod tests {
    use super::{is_binary_str, StrUnits};
    use tidb_datatype::{Collation, Datum, StringDatum};

    #[test]
    fn binary_charset_selects_the_byte_signature() {
        // `aéb` is four bytes and three characters, so byte and character
        // answers cannot coincide.
        let binary = Datum::new_bytes("aéb".as_bytes().to_vec());
        let text = Datum::new_string("aéb".to_string());
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
        let text = Datum::new_string("aéb".to_string());
        let units = StrUnits::of(&text).unwrap().unwrap();
        assert_eq!(units.slice(1, 2), "é".as_bytes());
        assert_eq!(units.slice(2, 99), b"b");
        assert_eq!(units.slice(3, 1), b"");
        assert_eq!(
            units.pack(units.slice(0, 2).to_vec()),
            Datum::new_string("aé".to_string())
        );

        let binary = Datum::new_bytes("aéb".as_bytes().to_vec());
        let units = StrUnits::of(&binary).unwrap().unwrap();
        assert_eq!(units.slice(1, 3), &[0xC3, 0xA9]);
        assert_eq!(
            units.pack(units.slice(1, 3).to_vec()),
            Datum::new_bytes(vec![0xC3, 0xA9])
        );
    }

    #[test]
    fn malformed_utf8_groups_one_unit_per_bad_byte() {
        // Go's `[]rune("a\xffb")` is three runes: 'a', RuneError, 'b'.
        let value = Datum::new_string(vec![b'a', 0xFF, b'b']);
        let units = StrUnits::of(&value).unwrap().unwrap();
        assert_eq!(units.len(), 3);
        assert_eq!(units.slice(1, 2), &[0xFF]);
    }

    #[test]
    fn null_has_no_units() {
        assert!(StrUnits::of(&Datum::Null).unwrap().is_none());
    }
}
