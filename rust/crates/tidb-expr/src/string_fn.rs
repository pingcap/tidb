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

//! String builtin functions (`CONCAT`, `LENGTH`, `UPPER`/`LOWER`,
//! `LEFT`/`RIGHT`, `SUBSTRING`), plus `position_with_collation`/`trim_value`
//! below. `POSITION(a IN b)` and `TRIM(...)` have dedicated AST grammars; the
//! rewriter lowers them to scalar functions.

use std::borrow::Cow;

use memchr::memmem;
use tidb_datatype::{CollationCollator, Collator};
use crate::coerce::{borrow_str_bytes, coerce_str, coerce_str_bytes};
use crate::ops::to_f64_with_mysql_string;
use crate::string_signature::{go_utf8_text, StrUnits};
use crate::{Datum, EvalError};
use tidb_datatype::FieldType;
use tidb_charset::find_encoding;
use tidb_charset::get_default_collation;
use tidb_charset::Collation;
use tidb_charset::Encoding;
use tidb_goruntime::{go_chars, go_hex, GoString};
use tidb_charset::TransformOp;

/// CONCAT: `NULL` if any argument is `NULL`, else the concatenation.
pub(crate) fn concat(vals: &[Datum]) -> Result<Datum, EvalError> {
    concat_with_context(vals, &tidb_exprctx::context::NoColumns)
}

/// Value-evaluated CONCAT with the statement packet limit applied.
pub(crate) fn concat_with_context(
    vals: &[Datum],
    ctx: &dyn tidb_exprctx::context::Columns,
) -> Result<Datum, EvalError> {
    if vals.is_empty() {
        return Err(EvalError::Unsupported(
            "CONCAT requires at least one argument",
        ));
    }
    let mut out = Vec::new();
    for v in vals {
        match coerce_str_bytes(v)? {
            Some(s) => {
                if out.len().saturating_add(s.len()) as u64 > ctx.max_allowed_packet() {
                    crate::expression_errors::handle_allowed_packet_overflowed(ctx, "concat")?;
                    return Ok(Datum::Null);
                }
                out.extend_from_slice(&s);
            }
            None => return Ok(Datum::Null),
        }
    }
    Ok(Datum::new_string_datum(out))
}

/// Go chooses the binary signature before evaluation, then the text signature
/// selects an encoding from the argument FieldType (not the result value).
pub(crate) fn case_convert_with_type(
    vals: &[Datum],
    upper: bool,
    source: Option<&FieldType>,
    binary: bool,
) -> Result<Datum, EvalError> {
    let [value] = vals else {
        return Err(EvalError::Unsupported("bad case-conversion arity"));
    };
    let Some(bytes) = coerce_str_bytes(value)? else {
        return Ok(Datum::Null);
    };
    if binary {
        return Ok(Datum::new_bytes_datum(bytes));
    }
    let encoding = find_encoding(source.map_or("utf8mb4", FieldType::charset_name));
    let text = GoString::from(bytes).to_utf8_lossy_go();
    Ok(Datum::new_string_datum(if upper {
        encoding.to_upper(&text)
    } else {
        encoding.to_lower(&text)
    }))
}

/// Go `soundexMinLen`: the minimum length of a SOUNDEX result; shorter codes
/// are padded with '0'.
const SOUNDEX_MIN_LEN: usize = 4;

/// Go `soundexCodes`: maps 'A'..'Z' to their SOUNDEX code. MySQL gives H and
/// W the code '0' of the vowels.
const SOUNDEX_CODES: &[u8; 26] = b"01230120022455012623010202";

/// Go `soundexMode`: how SOUNDEX reads its argument.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SoundexMode {
    /// Go `soundexModeRunes`: reads UTF-8 characters; a byte that is not
    /// valid UTF-8 is a single-byte character.
    Runes,
    /// Go `soundexModeLatin1`: reads every byte as a Latin-1 character.
    Latin1,
    /// Go `soundexModeBinary`: reads every byte, and only ASCII letters are
    /// letters.
    Binary,
}

/// Go `soundexModeOf`.
pub(crate) fn soundex_mode_of(tp: &tidb_datatype::FieldType) -> SoundexMode {
    if crate::builtin_control::is_binary_str(tp) {
        SoundexMode::Binary
    } else if tp.charset_name() == "latin1" {
        SoundexMode::Latin1
    } else {
        SoundexMode::Runes
    }
}

/// Go `soundex`: the MySQL SOUNDEX string of `s`. Characters that are not
/// letters are ignored. The first letter is kept (upper cased if it is an
/// ASCII letter) and every later letter adds its code, unless the code is
/// '0' or the same as the last added one; a vowel does not separate two
/// equal codes. The result is padded with '0' to at least four characters,
/// and is empty when `s` has no letter.
pub(crate) fn soundex(s: &[u8], mode: SoundexMode) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut n = 0;
    let mut last = 0u8;
    // `add` takes one character: `r` is its code point and `b` the byte that
    // stands for it when it is a single byte of the input (None for a
    // multibyte UTF-8 character).
    let mut add = |r: u32, b: Option<u8>| {
        if !is_soundex_letter(r, mode) {
            return;
        }
        let code = soundex_code(r);
        if n == 0 {
            match b {
                Some(b) => buf.push(b.to_ascii_uppercase()),
                None => {
                    let c = char::from_u32(r).expect("a decoded UTF-8 character");
                    buf.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes());
                }
            }
            n += 1;
            last = code;
        } else if code != b'0' && code != last {
            buf.push(code);
            n += 1;
            last = code;
        }
    };
    if mode == SoundexMode::Runes {
        let mut i = 0;
        while i < s.len() {
            let rest = &s[i..];
            let window = &rest[..rest.len().min(4)];
            let valid = match std::str::from_utf8(window) {
                Ok(text) => text,
                Err(error) => std::str::from_utf8(&window[..error.valid_up_to()]).expect("a valid prefix"),
            };
            match valid.chars().next() {
                // Not valid UTF-8: the byte is a character of its own and is
                // kept as it is.
                None => {
                    add(u32::from(s[i]), Some(s[i]));
                    i += 1;
                }
                Some(c) if c.len_utf8() == 1 => {
                    add(u32::from(c), Some(s[i]));
                    i += 1;
                }
                Some(c) => {
                    add(u32::from(c), None);
                    i += c.len_utf8();
                }
            }
        }
    } else {
        for &b in s {
            add(u32::from(b), Some(b));
        }
    }
    if n == 0 {
        return Vec::new();
    }
    if n < SOUNDEX_MIN_LEN {
        buf.resize(buf.len() + SOUNDEX_MIN_LEN - n, b'0');
    }
    buf
}

/// Go `isSoundexLetter`: for a UTF-8 string MySQL 8.0 takes every non-ASCII
/// character from U+00C0 up as a letter (including U+00D7 and U+00F7) and
/// none below it. For a Latin-1 string it classifies each byte with the
/// character set's own table ([`is_latin1_letter`]). A byte of a binary
/// string is a letter only when it is an ASCII letter.
fn is_soundex_letter(r: u32, mode: SoundexMode) -> bool {
    if u8::try_from(r).is_ok_and(|b| b.is_ascii_alphabetic()) {
        return true;
    }
    match mode {
        SoundexMode::Binary => false,
        SoundexMode::Latin1 => is_latin1_letter(r as u8),
        SoundexMode::Runes => r >= 0xC0,
    }
}

/// Go `isLatin1Letter`: whether `b` is a letter in MySQL's latin1 (cp1252)
/// character class table, strings/ctype-latin1.cc: the ASCII letters, the
/// letters 0x83, 0x8A, 0x8C, 0x8E, 0x9A, 0x9C, 0x9E and 0x9F of the 0x80 to
/// 0x9F block, and 0xC0 to 0xFF except the signs 0xD7 and 0xF7.
fn is_latin1_letter(b: u8) -> bool {
    if b.is_ascii_alphabetic() {
        return true;
    }
    if b >= 0xC0 {
        return b != 0xD7 && b != 0xF7;
    }
    matches!(b, 0x83 | 0x8A | 0x8C | 0x8E | 0x9A | 0x9C | 0x9E | 0x9F)
}

/// Go `soundexCode`.
fn soundex_code(r: u32) -> u8 {
    match u8::try_from(r) {
        Ok(b @ b'a'..=b'z') => SOUNDEX_CODES[usize::from(b - b'a')],
        Ok(b @ b'A'..=b'Z') => SOUNDEX_CODES[usize::from(b - b'A')],
        _ => b'0',
    }
}

#[cfg(test)]
mod case_convert_tests {
    use crate::tests::builtin;
    use crate::Datum;
    use tidb_datatype::MysqlEnum;
use tidb_charset::Collation;

    #[test]
    fn enum_names_follow_the_selected_binary_or_unicode_signature() {
        let binary = Datum::new_enum(MysqlEnum::new([0xff], 1), Collation::Binary);
        assert_eq!(
            builtin("upper", &[binary]),
            // Go's door answers a string datum holding the replacement
            // character's bytes (EF BF BD).
            Ok(Datum::new_string_datum("\u{fffd}"))
        );

        let text = Datum::new_enum(MysqlEnum::new([0xe2, 0x82], 1), Collation::Utf8Mb4Bin);
        assert_eq!(
            builtin("upper", &[text]),
            Ok(Datum::new_string_datum("\u{fffd}\u{fffd}"))
        );
    }
}

/// `ASCII(s)`: return the first byte of the evaluated string, not the first
/// Unicode scalar value. Go's `EvalString` preserves binary arguments, so
/// this uses byte-preserving coercion instead of a UTF-8-checked one.
pub(crate) fn ascii(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(bytes) = borrow_str_bytes(&vals[0])? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Int(i64::from(bytes.first().copied().unwrap_or(0))))
}

/// `BIT_LENGTH(s)`: count evaluated bytes and multiply by eight. This follows
/// Go's `len(val)` contract for ordinary UTF-8 and binary values alike.
pub(crate) fn bit_length(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(bytes) = borrow_str_bytes(&vals[0])? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Int((bytes.len() as i64) * 8))
}

/// `LEFT`/`RIGHT`: the first or last `n` units, where `builtinLeftSig` and
/// `builtinRightSig` count BYTES for a binary argument (preserving invalid
/// UTF-8) and their `...UTF8Sig` twins count CHARACTERS. `NULL` if either
/// argument is `NULL`.
pub(crate) fn str_take(vals: &[Datum], from_left: bool) -> Result<Datum, EvalError> {
    if vals.len() != 2 {
        return Err(EvalError::Unsupported("bad LEFT/RIGHT arguments"));
    }
    let n = match &vals[1] {
        Datum::Null => return Ok(Datum::Null),
        value => crate::cast::to_i64_signed(value).max(0) as usize,
    };
    let Some(units) = StrUnits::of(&vals[0])? else {
        return Ok(Datum::Null);
    };
    Ok(take_string_units(&units, n, from_left))
}

pub(crate) fn take_string_units(units: &StrUnits<'_>, n: usize, from_left: bool) -> Datum {
    units.pack(take_string_unit_slice(units, n, from_left).to_vec())
}

pub(crate) fn take_string_unit_slice<'a>(
    units: &'a StrUnits<'_>,
    n: usize,
    from_left: bool,
) -> &'a [u8] {
    if from_left {
        units.slice(0, n)
    } else {
        units.tail(n)
    }
}

/// `SUBSTRING(s, pos[, len])`: 1-indexed, counting in the units of the
/// signature Go selected for the argument's charset — bytes for
/// `builtinSubstring2ArgsSig`/`builtinSubstring3ArgsSig`, characters for their
/// `...UTF8Sig` twins (`pkg/expression/builtin_string.go`). Those four bodies
/// are the same arithmetic over a different unit, so [`StrUnits`] carries the
/// difference and this is written once: negative positions count back from the
/// end, while position zero and every out-of-range position produce the empty
/// string.
pub(crate) fn substring(
    vals: &[Datum],
    cols: &dyn tidb_exprctx::context::Columns,
    binary: bool,
) -> Result<Datum, EvalError> {
    if vals.contains(&Datum::Null) {
        return Ok(Datum::Null);
    }
    let [value, args @ ..] = vals else {
        return Err(EvalError::Unsupported("bad SUBSTRING arguments"));
    };
    let Some(bytes) = crate::arg_eval_type::eval_string_bytes(value)? else {
        return Ok(Datum::Null);
    };
    let (pos, length) = match args {
        [pos] => (pos, None),
        [pos, length] => (pos, Some(length)),
        _ => return Err(EvalError::Unsupported("bad SUBSTRING arguments")),
    };
    substring_bytes(bytes, pos, length, cols, binary)
}

pub(crate) fn substring_bytes(
    bytes: &[u8],
    pos: &Datum,
    length: Option<&Datum>,
    cols: &dyn tidb_exprctx::context::Columns,
    binary: bool,
) -> Result<Datum, EvalError> {
    let Some((pos, length)) = substring_arguments(pos, length, cols)? else {
        return Ok(Datum::Null);
    };
    let units = StrUnits::from_bytes(Cow::Borrowed(bytes), binary);
    Ok(units.pack(substring_slice(&units, pos, length).to_vec()))
}

pub(crate) fn substring_arguments(
    pos: &Datum,
    length: Option<&Datum>,
    cols: &dyn tidb_exprctx::context::Columns,
) -> Result<Option<(i64, Option<i64>)>, EvalError> {
    if pos.is_null() || length.is_some_and(Datum::is_null) {
        return Ok(None);
    }
    // Go builds every `substring` signature through
    // `newBaseBuiltinFuncWithTp(..., types.ETInt, ...)` for the position and
    // length arguments, so a non-integer argument is CAST to an integer before
    // `evalString` runs -- exactly the coercion `LEFT`/`RIGHT` already use here.
    // Matching only `Datum::Int` refused `SUBSTRING('hello', '2')` (Go: `ello`)
    // and every other argument Go silently casts.
    // go's position/length arguments are WrapWithCastAsInt-wrapped: the
    // conversion warns `Truncated incorrect INTEGER value` exactly like an
    // explicit CAST (`SUBSTRING('abc', 1, 'x')` warns 1292 for 'x').
    let pos = crate::cast::to_i64_signed_with_warnings(pos, cols)?;
    let length = length
        .map(|length| crate::cast::to_i64_signed_with_warnings(length, cols))
        .transpose()?;
    Ok(Some((pos, length)))
}

pub(crate) fn substring_slice<'a>(
    units: &'a StrUnits<'_>,
    pos: i64,
    length: Option<i64>,
) -> &'a [u8] {
    let range = substring_range(pos, length, || units.len());
    units.slice(range.start, range.end)
}

#[inline]
pub(crate) fn substring_range(
    pos: i64,
    length: Option<i64>,
    units: impl FnOnce() -> usize,
) -> std::ops::Range<usize> {
    let start = if pos < 0 {
        pos + units() as i64
    } else {
        pos - 1
    };
    if start < 0 {
        return 0..0;
    }
    let end = match length {
        None => usize::MAX,
        Some(length) if length <= 0 => return 0..0,
        Some(length) => {
            // Go's source computes `end := pos + length` in int64.  A
            // positive length can therefore wrap when `pos > 0`, and the
            // following `end < pos` branch returns the empty string.  Do
            // not use saturating_add here: it would silently turn that
            // source-visible overflow into an unexpectedly long tail.
            let Some(end) = start.checked_add(length) else {
                return 0..0;
            };
            usize::try_from(end).unwrap_or(usize::MAX)
        }
    };
    usize::try_from(start).unwrap_or(usize::MAX)..end
}

/// `REVERSE(s)`: the units of `s` in the opposite order. Go's
/// `reverseFunctionClass.getFunction` selects `builtinReverseSig`
/// (`reverseBytes`) for a binary or BIT argument and `builtinReverseUTF8Sig`
/// (`reverseRunes`) otherwise, so reversing a binary value must NOT permute
/// the bytes of a multi-byte character back into a valid one.
pub(crate) fn reverse(vals: &[Datum]) -> Result<Datum, EvalError> {
    let [value] = vals else {
        return Err(EvalError::Unsupported("bad REVERSE arity"));
    };
    let Some(units) = StrUnits::of(value)? else {
        return Ok(Datum::Null);
    };
    Ok(units.reversed())
}

/// `LOCATE(substr, str)` / `INSTR(str, substr)` / `POSITION(substr IN str)`
/// over the raw arguments, which is what selects the signature.
///
/// `locateFunctionClass.getFunction` picks `builtinLocate2ArgsSig` over
/// `builtinLocate2ArgsUTF8Sig` when the function's DERIVED collation is
/// `binary` (`bf.collation == charset.CollationBin`), and that signature is a
/// plain `strings.Index`: a 1-based BYTE offset, with no collation folding.
/// The UTF-8 signature reports a 1-based CHARACTER offset instead, so the two
/// disagree for any haystack with a multi-byte prefix.
///
/// `collation` is the derived collation where the caller has one; the AST
/// evaluator has no derivation pass and passes `binary` exactly when
/// [`crate::string_signature::is_binary_str`] holds for an argument, which is
/// the same condition that makes Go's aggregate `binary`.
pub(crate) fn locate(
    substr: &Datum,
    str: &Datum,
    collation: tidb_charset::Collation,
) -> Result<Datum, EvalError> {
    if collation != tidb_charset::Collation::Binary {
        return Ok(position_with_collation(
            coerce_str(substr)?,
            coerce_str(str)?,
            collation,
        ));
    }
    let (Some(needle), Some(haystack)) = (borrow_str_bytes(substr)?, borrow_str_bytes(str)?) else {
        return Ok(Datum::Null);
    };
    if needle.is_empty() {
        return Ok(Datum::Int(1));
    }
    let found = memmem::find(&haystack, &needle);
    Ok(Datum::Int(found.map_or(0, |index| index as i64 + 1)))
}

/// `LOCATE(substr, str, pos)` — the three-argument signatures
/// `builtinLocate3ArgsSig` (bytes, under a binary collation) and
/// `builtinLocate3ArgsUTF8Sig` (characters, under any other collation)
/// (`pkg/expression/builtin_string.go:1615/:1660`).
///
/// Go converts the 1-based `pos` to a 0-based index BEFORE the bounds check;
/// `pos < 1` (0-based negative) or `pos` beyond `len - needle_len` answers 0,
/// and an empty needle answers `pos` itself (reported 1-based). Under a
/// case-insensitive collation Go lowers BOTH strings with `strings.ToLower`
/// before the rune-count bounds, so the bounds apply to the lowered lengths.
/// Both UTF-8 signatures search the collation keys, so expansions are matched
/// consistently with or without a start position.
pub(crate) fn locate_with_position(
    vals: &[Datum],
    collation: tidb_charset::Collation,
) -> Result<Datum, EvalError> {
    let [substr, str, pos] = vals else {
        return Err(EvalError::Unsupported("bad LOCATE arity"));
    };
    // Go evaluates substr, str, then pos, and isNull on ANY of them is the
    // NULL result (the Int signatures return `0, true, nil`).
    let binary = collation == tidb_charset::Collation::Binary;
    let (Some(needle), Some(hay)) = (borrow_str_bytes(substr)?, borrow_str_bytes(str)?) else {
        return Ok(Datum::Null);
    };
    let Some(position) = crate::arg_eval_type::eval_int(pos)? else {
        return Ok(Datum::Null);
    };
    // Transfer the 1-based argument to a 0-based index.
    let start = position.wrapping_sub(1);

    if binary {
        if start < 0 || start > hay.len() as i64 - needle.len() as i64 {
            return Ok(Datum::Int(0));
        }
        if needle.is_empty() {
            return Ok(Datum::Int(start + 1));
        }
        let found = memmem::find(&hay[start as usize..], &needle);
        return Ok(Datum::Int(
            found.map_or(0, |index| start + index as i64 + 1),
        ));
    }

    // Under a case-insensitive collation Go lowers both strings BEFORE the
    // rune-count bounds, using `strings.ToLower` (the same simple mapping
    // `tidb_mysql::to_lowercase` ports).
    let hay = go_utf8_text(&hay);
    let (needle, hay) = if tidb_datatype::is_ci_collation(collation.name()) {
        (
            Cow::Owned(tidb_mysql::to_lowercase(&go_utf8_text(&needle)).into_bytes()),
            Cow::Owned(tidb_mysql::to_lowercase(&hay)),
        )
    } else {
        (needle, hay)
    };
    let needle_len = go_chars(needle.as_ref()).count();
    if start < 0 || start > hay.chars().count() as i64 - needle_len as i64 {
        return Ok(Datum::Int(0));
    }
    if needle.is_empty() {
        return Ok(Datum::Int(start + 1));
    }
    let slice_start = hay.char_indices().nth(start as usize).unwrap().0;
    let found = locate_string_with_collation(&hay[slice_start..], &needle, collation);
    Ok(Datum::Int(if found == 0 { 0 } else { start + found }))
}

/// `LOCATE`/`INSTR`/`POSITION` under an explicit collation.
///
/// Go's `builtinLocate2ArgsUTF8Sig`/`builtinInstrUTF8Sig` search with the
/// function's own collator, so a case-insensitive collation finds a
/// case-folded occurrence: captured from TiDB,
/// `INSTR('ABC' COLLATE utf8mb4_general_ci, 'b')` is 2 where the
/// `utf8mb4_bin` form is 0. The position reported is a 1-based CHARACTER
/// index into the haystack either way.
///
/// The window is compared by the collation rather than by bytes, which is
/// what makes a folding collation match; a collation whose folding changes
/// character COUNT (none this tier registers) would need a different scan.
///
/// A `binary` collation selects Go's OTHER signature -- byte offsets, not
/// character ones -- and never reaches here: [`locate`] branches to it first.
pub(crate) fn position_with_collation(
    substr: Option<String>,
    str: Option<String>,
    collation: tidb_charset::Collation,
) -> Datum {
    let (Some(substr), Some(str)) = (substr, str) else {
        return Datum::Null;
    };
    if substr.is_empty() {
        return Datum::Int(1);
    }
    Datum::Int(locate_string_with_collation(&str, substr.as_bytes(), collation))
}

/// Go `locateStringWithCollation` (`util.go:821`): the needle's collation
/// key is searched inside the haystack's, and the byte offset found is
/// walked back to a character count -- so under `utf8mb4_unicode_ci` the
/// one character 'ß' is found at the two characters 'ss'.
fn locate_string_with_collation(
    str: &str,
    substr: &[u8],
    collation: tidb_charset::Collation,
) -> i64 {
    let collator = tidb_datatype::get_collator(collation.name());
    if matches!(
        collator,
        Collator::DerivedBinary
            | Collator::New(
                Collation::Binary
                    | Collation::AsciiBin
                    | Collation::Latin1Bin
                    | Collation::Utf8Bin
                    | Collation::Utf8Mb4Bin
                    | Collation::Utf8Mb40900Bin
            )
    ) {
        return memmem::find(str.as_bytes(), substr).map_or(0, |found| {
            str[..str.ceil_char_boundary(found)].chars().count() as i64 + 1
        });
    }
    let str_key = collator.key_without_trim_right_space(str.as_bytes());
    let substr_key = collator.key_without_trim_right_space(substr);
    let Some(found) = memmem::find(&str_key, &substr_key) else {
        return 0;
    };
    if found == 0 {
        return 1;
    }
    let mut index = found as i64;
    let mut count = 0_i64;
    let mut buffer = [0_u8; 4];
    for character in str.chars() {
        count += 1;
        index -= collator
            .key_without_trim_right_space(character.encode_utf8(&mut buffer).as_bytes())
            .len() as i64;
        if index <= 0 {
            return count + 1;
        }
    }
    count + 1
}

/// Go `INSTR(str, substr)`: `builtinInstrSig` matches bytes under a binary
/// collation; `builtinInstrUTF8Sig` lowercases both strings under a
/// case-insensitive collation (`strings.ToLower`) and reports the character
/// offset of the first byte match -- not the collation-key search LOCATE
/// uses.
pub(crate) fn instr(
    str: &Datum,
    substr: &Datum,
    collation: tidb_charset::Collation,
) -> Result<Datum, EvalError> {
    if collation == tidb_charset::Collation::Binary {
        return locate(substr, str, collation);
    }
    let (Some(mut str), Some(mut substr)) = (coerce_str(str)?, coerce_str(substr)?) else {
        return Ok(Datum::Null);
    };
    if tidb_datatype::is_ci_collation(collation.name()) {
        str = tidb_hack::go_to_lower(&str);
        substr = tidb_hack::go_to_lower(&substr);
    }
    Ok(Datum::Int(str.find(substr.as_str()).map_or(0, |index| {
        str[..index].chars().count() as i64 + 1
    })))
}

/// `REPLACE(str, from, to)`: every non-overlapping occurrence of `from` in
/// `str` replaced by `to`. An empty `from` leaves `str` unchanged (matching
/// MySQL, and avoiding a pathological empty-pattern replace). `NULL` if any
/// argument is `NULL`.
pub(crate) fn replace(vals: &[Datum]) -> Result<Datum, EvalError> {
    let [value, from, to] = vals else {
        return Err(EvalError::Unsupported("bad REPLACE arity"));
    };
    let (Some(s), Some(from), Some(to)) = (
        borrow_str_bytes(value)?,
        borrow_str_bytes(from)?,
        borrow_str_bytes(to)?,
    ) else {
        return Ok(Datum::Null);
    };
    let mut out = Vec::new();
    if !replace_bytes_into(&s, &from, &to, &mut out) {
        out = s.into_owned();
    }
    Ok(string_result(value, out))
}

/// Replaces non-overlapping byte occurrences, matching Go's
/// `strings.ReplaceAll` over an arbitrary Go string. A false return indicates
/// the caller can reuse `value` directly.
pub(crate) fn replace_bytes_into(
    value: &[u8],
    from: &[u8],
    to: &[u8],
    out: &mut Vec<u8>,
) -> bool {
    if from.is_empty() || from == to {
        return false;
    }
    let finder = memmem::Finder::new(from);
    let Some(mut start) = finder.find(value) else {
        return false;
    };
    out.clear();
    out.reserve(value.len());
    let mut cursor = 0;
    loop {
        out.extend_from_slice(&value[cursor..start]);
        out.extend_from_slice(to);
        cursor = start + from.len();
        let rest = &value[cursor..];
        if rest.starts_with(from) {
            start = cursor;
        } else if let Some(relative) = finder.find(rest) {
            start = cursor + relative;
        } else {
            out.extend_from_slice(rest);
            return true;
        }
    }
}

/// Preserve binary result semantics when the source string is binary; normal
/// SQL strings retain the ordinary String datum/collation boundary.
fn string_result(source: &Datum, bytes: Vec<u8>) -> Datum {
    if matches!(source, Datum::Bytes(_)) {
        Datum::new_bytes_datum(bytes)
    } else {
        Datum::new_string_datum(bytes)
    }
}

/// `STRCMP` under the collation the expression derivation aggregated
/// (Go `builtinStrcmpSig`, which compares with `b.collator`). Captured from
/// TiDB: `STRCMP('a' COLLATE utf8mb4_general_ci, 'A' COLLATE
/// utf8mb4_general_ci)` is 0 where the `utf8mb4_bin` form is 1.
pub(crate) fn strcmp_with_collation(
    vals: &[Datum],
    collation: tidb_charset::Collation,
) -> Result<Datum, EvalError> {
    let [left, right] = vals else {
        return Err(EvalError::Unsupported("bad STRCMP arity"));
    };
    let (Some(a), Some(b)) = (coerce_str_bytes(left)?, coerce_str_bytes(right)?) else {
        return Ok(Datum::Null);
    };
    strcmp_under(&a, &b, collation)
}

fn strcmp_under(
    a: &[u8],
    b: &[u8],
    collation: tidb_charset::Collation,
) -> Result<Datum, EvalError> {
    Ok(Datum::Int(match collation.compare(a, b) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }))
}

/// `HEX(x)`: renders a numeric argument's implicit-integer bits as uppercase
/// base-16 (`HEX(12.8)` = `D`, `HEX(-1)` = `FFFFFFFFFFFFFFFF`), or renders a
/// string/binary-literal argument's bytes (`HEX('abc')` = `616263`).
///
/// This is deliberately a type-directed split, rather than formatting every
/// value through `coerce_str`: TiDB's `TestHexFunc` proves that decimal and
/// real values use the integer signature while ETString values retain their
/// original bytes. Reusing `radix_integer_bits` keeps every numeric rounding
/// and two's-complement edge on one source-defined conversion path.
///
/// # A `BIT` COLUMN is an integer argument, a bit LITERAL is not
///
/// `hexFunctionClass.getFunction` switches on `args[0].GetType().EvalType()`,
/// and `mysql.TypeBit`'s eval type is `types.ETInt` -- so a `BIT(48)` column
/// is hexed through `builtinHexIntArgSig`, which formats the VALUE and
/// therefore drops the storage width's leading zero bytes. A bit LITERAL
/// (`b'01000001'`) is a different type: Go stores it as `KindBinaryLiteral`
/// typed `mysql.TypeVarString`, so it takes the string branch, and it only
/// LOOKS like the integer answer because a literal carries no padding.
/// Measured through `gorun` on `bit(48)` holding `0x00080A0D091A`:
///
/// ```text
/// hex(b)          -> 80A0D091A     (int branch: value, no leading zeros)
/// hex(concat(b))  -> 000000000041  (string branch on bit(48) x'0041')
/// hex(x'0041')    -> 0041          (hex literal keeps both bytes)
/// hex(b'01000001')-> 41            (bit literal is one byte to begin with)
/// ```
pub(crate) fn hex(vals: &[Datum]) -> Result<Datum, EvalError> {
    match &vals[0] {
        Datum::Null => Ok(Datum::Null),
        Datum::Int(_)
        | Datum::UInt(_)
        | Datum::Decimal(_)
        | Datum::Real(_)
        | Datum::Float32(_)
        | Datum::Bit(_) => {
            let bits = radix_integer_bits(&vals[0])?.expect("non-NULL numeric HEX input");
            Ok(Datum::new_string_datum(format!("{bits:X}")))
        }
        Datum::String(_)
        | Datum::Bytes(_)
        | Datum::Raw(_)
        | Datum::BinaryLiteral(_)
        | Datum::Duration(_)
        | Datum::Enum(_, _)
        | Datum::Set(_, _)
        | Datum::Time(_)
        | Datum::Json(_) => hex_string_value(&vals[0]),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel HEX argument"))
        }
        other => {
            let bytes = other
                .to_bytes()
                .map_err(|_| EvalError::Unsupported("HEX argument conversion"))?;
            Ok(Datum::new_string_datum(go_hex::encode_uppercase_to_string(
                &bytes,
            )))
        }
    }
}

fn hex_string_value(value: &Datum) -> Result<Datum, EvalError> {
    let bytes = match value {
        Datum::String(_) | Datum::Bytes(_) | Datum::Raw(_) | Datum::BinaryLiteral(_) => {
            value.go_bytes()
        }
        Datum::Enum(value, _) => value.name_bytes(),
        Datum::Set(value, _) => value.name_bytes(),
        _ => {
            return Ok(coerce_str_bytes(value)?.map_or(Datum::Null, |bytes| {
                Datum::new_string_datum(go_hex::encode_uppercase_to_string(&bytes))
            }));
        }
    };
    Ok(Datum::new_string_datum(go_hex::encode_uppercase_to_string(bytes)))
}

/// Go `hexFunctionClass.getFunction`: select the string or integer signature
/// from the argument's declared eval type, then apply the corresponding
/// build-time argument cast before evaluating the body.
pub(crate) fn hex_with_type(
    vals: &[Datum],
    arg_type: Option<&FieldType>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    use tidb_datatype::EvalType;

    let Some(arg_type) = arg_type else {
        return hex(vals);
    };
    let [value] = vals else {
        return Err(EvalError::Unsupported("bad HEX arity"));
    };
    match arg_type.eval_type() {
        EvalType::String
        | EvalType::Datetime
        | EvalType::Timestamp
        | EvalType::Duration
        | EvalType::Json => {
            let value = crate::cast::cast_arg_as_string(value, Some(arg_type), ctx)?;
            hex_string_value(&value)
        }
        EvalType::Int | EvalType::Real | EvalType::Decimal => {
            let value = crate::cast::cast_arg_as_int(value, Some(arg_type), ctx)?;
            hex(&[value])
        }
        _ => Err(EvalError::Unsupported("HEX argument eval type")),
    }
}

/// `UNHEX(s)`: the inverse of `HEX` for a string — each hex pair becomes one
/// byte in a binary datum (`UNHEX('4D7953514C')` carries `MySQL` bytes). An odd number of
/// digits gets an implicit leading zero (`UNHEX('126')` = bytes `01 26`), as
/// `builtinUnHexSig` does. `NULL` only when a digit is invalid; arbitrary
/// decoded octets remain representable. The source signature is `ETString`,
/// which is a byte-preserving Go string boundary, so malformed input bytes
/// also follow the normal invalid-hex `NULL` path rather than becoming a Rust
/// UTF-8 decoding error.
pub(crate) fn unhex(vals: &[Datum]) -> Result<Datum, EvalError> {
    let Some(s) = borrow_str_bytes(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let mut bytes = Vec::new();
    if !decode_sql_hex(&s, &mut bytes) {
        return Ok(Datum::Null);
    }
    Ok(Datum::new_bytes_datum(bytes))
}

pub(crate) fn decode_sql_hex(input: &[u8], output: &mut Vec<u8>) -> bool {
    let odd = input.len() % 2;
    let (head, digits) = input.split_at(odd);
    output.resize(input.len().div_ceil(2), 0);
    if let [digit] = *head {
        if go_hex::decode_pairs(&mut output[..1], &[[b'0', digit]]).is_err() {
            return false;
        }
    }
    go_hex::decode_pairs(&mut output[odd..], digits.as_chunks::<2>().0).is_ok()
}

/// `BIN(n)`: the base-2 form of an implicitly-cast integer as an unsigned
/// 64-bit value.  TiDB's `binFunctionClass` requests `ETInt`, so strings use
/// their leading integer run, decimals round half-up, and reals round
/// ties-to-even before their resulting two's-complement bits are formatted.
pub(crate) fn bin(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad BIN arity"));
    }
    let raw: Option<&[u8]> = match &vals[0] {
        Datum::String(text) => Some(text.bytes()),
        Datum::Bytes(text) => Some(text),
        _ => None,
    };
    if let Some(text) = raw {
        if radix_truncated(text) {
            ctx.append_go_warning(
                &tidb_datatype::types_errors::ERR_TRUNCATED_WRONG_VAL,
                &format!(
                    "Truncated incorrect INTEGER value: '{}'",
                    String::from_utf8_lossy(text)
                ),
            );
        }
    }
    Ok(radix_integer_bits(&vals[0])?
        .map_or(Datum::Null, |bits| Datum::new_string_datum(format!("{bits:b}"))))
}

/// `OCT(n)`: the base-8 form of an integer as an unsigned 64-bit value.
/// Go has two signatures here: the integer path renders the raw `u64` bits,
/// while every non-integer source first goes through `builtinOctStringSig`.
/// That second path is deliberately *not* the `BIN` integer cast: an empty
/// original string is `NULL`, a whitespace-only/invalid nonempty string is
/// zero, and a decimal-prefix overflow stays at `u64::MAX` even if it had a
/// leading minus sign.  Keeping that conversion local makes those cases the
/// normal signature split rather than test-specific exceptions.
///
/// The split is Go's own build-time test, and it is a SIGNATURE choice rather
/// than an argument cast, which is why `OCT` is not a member of
/// `crate::arg_eval_type`'s `types.ETInt` layer:
///
/// ```go
/// if IsBinaryLiteral(args[0]) || args[0].GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
/// ```
///
/// (`builtin_string.go:3005`). `IsBinaryLiteral` is `con.Value.Kind() ==
/// types.KindBinaryLiteral`, this tier's [`Datum::BinaryLiteral`], and
/// `mysql.TypeBit`'s eval type is `types.ETInt`, this tier's [`Datum::Bit`];
/// both therefore take the INTEGER signature and render their big-endian
/// value. ENUM and SET do NOT -- their eval type is `types.ETString` until
/// something adds `EnumSetAsIntFlag`, and `OCT` never does. Captured from
/// real TiDB (`gorun`) over `enum('x','y','z')` holding `'y'`,
/// `set('a','b','c')` holding `'a,c'` and `bit(8)` holding `b'00000011'`:
/// `oct(e)` is `0` and `oct(s)` is `0` (the STRINGS parsed), while `oct(b)`
/// is `3` and `oct(b'01000001')` is `101`.
pub(crate) fn oct(vals: &[Datum]) -> Result<Datum, EvalError> {
    match &vals[0] {
        Datum::Null => Ok(Datum::Null),
        Datum::Int(value) => Ok(Datum::new_string_datum(format!("{:o}", *value as u64))),
        Datum::UInt(value) => Ok(Datum::new_string_datum(format!("{value:o}"))),
        Datum::Bit(bits) | Datum::BinaryLiteral(bits) => {
            Ok(Datum::new_string_datum(format!("{:o}", bits.to_int().value())))
        }
        value => Ok(oct_string_bits(value)?
            .map_or(Datum::Null, |bits| Datum::new_string_datum(format!("{bits:o}")))),
    }
}

/// The `ETString` conversion inside Go's `builtinOctStringSig`.
/// `getValidPrefix(..., 10)` retains only a leading sign and decimal digits;
/// `strconv.ParseUint` returns `MaxUint64` together with `ErrRange` on
/// overflow.  A negative in-range magnitude is negated in the `u64` domain,
/// but a negative overflow is intentionally left as `MaxUint64` by the Go
/// implementation.
fn oct_string_bits(value: &Datum) -> Result<Option<u64>, EvalError> {
    let Some(value) = coerce_str_bytes(value)? else {
        return Ok(None);
    };
    if value.is_empty() {
        return Ok(None);
    }
    // Go's `EvalString` and `strings.TrimSpace` operate on byte strings. A
    // valid UTF-8 value can use Rust's Unicode-equivalent `trim`; malformed
    // bytes cannot be decoded, so trim the ASCII whitespace that Go also
    // recognizes around such a payload and leave every other octet intact.
    let value = trim_go_space(&value);
    let prefix_len = value
        .iter()
        .enumerate()
        .take_while(|(index, byte)| {
            byte.is_ascii_digit() || (*index == 0 && matches!(byte, b'+' | b'-'))
        })
        .map(|(index, _)| index + 1)
        .last()
        .unwrap_or(0);
    let prefix = &value[..prefix_len];
    let prefix = prefix.strip_prefix(b"+").unwrap_or(prefix);
    let (negative, digits) = match prefix.strip_prefix(b"-") {
        Some(digits) => (true, digits),
        None => (false, prefix),
    };
    if digits.is_empty() {
        return Ok(Some(0));
    }
    let digits = std::str::from_utf8(digits).expect("OCT decimal prefix is ASCII");
    let (bits, overflow) = match digits.parse::<u64>() {
        Ok(bits) => (bits, false),
        Err(_) => (u64::MAX, true),
    };
    Ok(Some(if negative && !overflow {
        bits.wrapping_neg()
    } else {
        bits
    }))
}

/// Trims the same source-visible whitespace around an `ETString` value that
/// `strings.TrimSpace` sees. Keeping this byte-preserving matters for values
/// such as `OCT(UNHEX('FF'))`: Go treats the invalid leading byte as an empty
/// numeric prefix and returns zero, whereas a checked UTF-8 conversion would
/// incorrectly raise an evaluator error before `getValidPrefix` runs.
fn trim_go_space(bytes: &[u8]) -> &[u8] {
    if let Ok(text) = std::str::from_utf8(bytes) {
        return text.trim().as_bytes();
    }
    let mut start = 0;
    let mut end = bytes.len();
    while start < end && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &bytes[start..end]
}

/// `BIN`'s argument path.  This mirrors
/// `newBaseBuiltinFuncWithTp(..., ETInt)` in Go: a non-negative string is
/// parsed through `StrToUint` and its low 64 bits are retained, whereas a
/// negative string follows the signed path.  The seed has no warning surface,
/// so truncation/range warnings become the same clamped result TiDB produces
/// with `IgnoreTruncateErr` in `TestBin`.
fn radix_integer_bits(value: &Datum) -> Result<Option<u64>, EvalError> {
    match value {
        Datum::Null => Ok(None),
        Datum::Int(value) => Ok(Some(*value as u64)),
        Datum::UInt(value) => Ok(Some(*value)),
        Datum::Decimal(value) => value
            .round_to_i64()
            .map(|value| Some(value as u64))
            .ok_or(EvalError::IntOverflow),
        Datum::Real(value) => Ok(Some(round_float_to_i64_saturating(*value) as u64)),
        Datum::String(value) => Ok(Some(radix_string_bits_bytes(value.bytes()))),
        Datum::Bytes(value) => Ok(Some(radix_string_bits_bytes(value))),
        // A `BIT` column reaches the integer signatures through
        // `WrapWithCastAsInt` over `mysql.TypeBit`, which is `BinaryLiteral.
        // ToInt`: the big-endian payload read as an UNSIGNED 64-bit value.
        // Going through `to_i64` instead would refuse `bit(64)` values with
        // the high bit set, which Go answers as `FFFFFFFFFFFFFFFF`.
        Datum::Bit(value) => Ok(Some(value.to_int().value())),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel integer argument"))
        }
        other => other
            .to_i64()
            .map(|converted| Some(converted.value as u64))
            .map_err(|_| EvalError::Unsupported("integer argument conversion")),
    }
}

/// `builtinCastStringAsIntSig` selects its signed path only when the trimmed
/// input has a `-` followed by at least one byte.  The other path accepts an
/// optional `+`, parses the leading ASCII digits, and preserves all `u64`
/// bits.  Invalid/trailing text only emits a warning in TiDB's test context,
/// so it leaves the valid prefix (or zero) intact.
/// go `StrToInt`/`StrToUint`: the cast warns `Truncated incorrect INTEGER
/// value` when the text carries anything the numeric parse could not
/// consume — a remainder after the digit run, or no digits at all.
fn radix_truncated(value: &[u8]) -> bool {
    let value = trim_go_space(value);
    let digits = if value.len() > 1 && value.starts_with(b"-") {
        &value[1..]
    } else {
        value.strip_prefix(b"+").unwrap_or(value)
    };
    let digit_len = digits
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    digit_len < digits.len()
}

fn radix_string_bits_bytes(value: &[u8]) -> u64 {
    let value = trim_go_space(value);
    let negative = value.len() > 1 && value.starts_with(b"-");
    let digits = if negative {
        &value[1..]
    } else {
        value.strip_prefix(b"+").unwrap_or(value)
    };
    let digit_len = digits
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    let digits = &digits[..digit_len];
    if digits.is_empty() {
        return 0;
    }
    let magnitude = digits.iter().try_fold(0_u64, |value, digit| {
        value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u64::from(*digit - b'0')))
    });
    let magnitude = magnitude.unwrap_or(u64::MAX);
    if negative {
        if magnitude >= 1_u64 << 63 {
            i64::MIN as u64
        } else {
            (-(magnitude as i64)) as u64
        }
    } else {
        magnitude
    }
}

/// [`field`] under the collation the expression derivation aggregated over
/// ALL of `FIELD`'s arguments (Go `deriveCollation`'s `ast.Field` arm, taken
/// when the argument list is all-string).
///
/// Go's `builtinFieldStringSig.evalInt` tests `b.ctor.Compare(str, stri) == 0`
/// -- the function's own collator, not a fixed byte comparison -- so a
/// case-folding collation matches a differently-cased candidate. Captured from
/// TiDB: `FIELD('ABC' COLLATE utf8mb4_general_ci, 'abc')` is 1 where the
/// `utf8mb4_bin` form is 0.
pub(crate) fn field_with_collation(
    vals: &[Datum],
    collation: tidb_charset::Collation,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    if vals[0] == Datum::Null {
        return Ok(Datum::Int(0));
    }
    // Go's two flags, verbatim (`builtin_string.go:2774-2776`):
    //
    // ```go
    // argTp := args[i].GetType(ctx.GetEvalCtx()).EvalType()
    // isAllString = isAllString && (argTp == types.ETString)
    // isAllNumber = isAllNumber && (argTp == types.ETInt)
    // ```
    //
    // so the membership question is `FieldType.EvalType()`, not the datum's
    // kind -- and that switch (`pkg/parser/types/field_type.go:417-441`) puts
    // ENUM and SET under `types.ETString` (they reach the signature as their
    // NAME) and `mysql.TypeBit` under `types.ETInt`. Captured from real TiDB
    // (`gorun`) over `enum('a','b','c')` holding `'b'` and `set('a','b')`
    // holding `'a,b'`: `field(e,'b')` and `field(s,'a,b')` are both `1`, and
    // `field(x'61','a')` is `1`; this tier answered `0` to all three while
    // the hybrids fell through to the REAL signature, which compares an
    // enum's ORDINAL. The mixed lists still do exactly that, and correctly:
    // `field(e,2)` is `1` and `field(e,b)` is `0` in both engines.
    //
    // `Datum::Null` stays a member of BOTH arms. Go's `mysql.TypeNull`
    // answers `types.ETString`, so a NULL LITERAL would be string-only there
    // -- but this tier cannot tell a NULL literal from a NULL-valued INT
    // column, and reading every NULL as string-typed would push
    // `field(int_col, null_int_col, ...)` onto the REAL signature, which
    // loses integers past 2^53. Neutral is the reading that is right whenever
    // the argument's own type is what Go looked at.
    let mode = if vals.iter().all(|value| {
        matches!(
            value,
            Datum::Null
                | Datum::String(_)
                | Datum::Bytes(_)
                | Datum::Enum(..)
                | Datum::Set(..)
                | Datum::BinaryLiteral(_)
        )
    }) {
        FieldComparisonMode::String
    } else if vals.iter().all(|value| {
        matches!(
            value,
            Datum::Null | Datum::Int(_) | Datum::UInt(_) | Datum::Bit(_)
        )
    }) {
        FieldComparisonMode::Integer
    } else {
        FieldComparisonMode::Real
    };
    // The real signature coerces the needle ONCE, before the scan: Go's
    // `builtinFieldRealSig.evalInt` evaluates `args[0]` a single time, so
    // `FIELD('12abc', 1, 2)` records exactly ONE 1292 (captured) no matter how
    // many candidates follow. Coercing it inside the loop repeated the
    // warning per candidate.
    let needle = match mode {
        FieldComparisonMode::Real => Some(Datum::Real(to_f64_with_mysql_string(&vals[0], ctx)?)),
        FieldComparisonMode::String | FieldComparisonMode::Integer => None,
    };
    for (i, v) in vals[1..].iter().enumerate() {
        if *v == Datum::Null {
            continue;
        }
        let equal = match mode {
            // Go compares the two evaluated strings through the signature's
            // own collator, which is where a `_ci` collation folds case and a
            // PAD SPACE one ignores trailing blanks.
            FieldComparisonMode::String => {
                // `builtinFieldStringSig.evalInt` is `b.args[i].EvalString`,
                // which is `crate::arg_eval_type::eval_string` -- the same
                // reader that gives an ENUM its NAME, and the reason this arm
                // admits the hybrids the mode test above just let in.
                let (Some(needle), Some(candidate)) = (
                    crate::arg_eval_type::eval_string(&vals[0])?,
                    crate::arg_eval_type::eval_string(v)?,
                ) else {
                    return Err(EvalError::Unsupported("non-string FIELD string operand"));
                };
                collation.compare(&needle, &candidate) == std::cmp::Ordering::Equal
            }
            FieldComparisonMode::Integer
                if matches!(vals[0], Datum::Int(_) | Datum::UInt(_))
                    && matches!(v, Datum::Int(_) | Datum::UInt(_)) =>
            {
                // Go EvalInt returns the same int64 carrier for both types.
                vals[0].get_int64() == v.get_int64()
            }
            FieldComparisonMode::Integer => {
                crate::eval_binary(tidb_ast::BinaryOp::Eq, vals[0].clone(), v.clone())?
                    == Datum::Int(1)
            }
            FieldComparisonMode::Real => {
                let needle = needle.clone().expect("coerced above for this mode");
                let candidate = Datum::Real(to_f64_with_mysql_string(v, ctx)?);
                crate::eval_binary(tidb_ast::BinaryOp::Eq, needle, candidate)? == Datum::Int(1)
            }
        };
        if equal {
            return Ok(Datum::Int(i as i64 + 1));
        }
    }
    Ok(Datum::Int(0))
}

#[derive(Clone, Copy)]
enum FieldComparisonMode {
    String,
    Integer,
    Real,
}

/// `ELT(n, a, b, c, ...)`: the `n`-th (1-based) following argument, or
/// `NULL` if `n` is out of range or `NULL`.
///
/// Go declares `argTps[0] = types.ETInt` and `types.ETString` for every
/// following position (`builtin_string.go:3305-3309`), so both readings below
/// are `crate::arg_eval_type`'s, not this body's -- `builtinEltSig.evalString`
/// is `b.args[0].EvalInt` then `b.args[idx].EvalString` and nothing else.
/// Reading the selected argument as BYTES is what the routing bought:
/// captured from real TiDB (`gorun`), `hex(elt(1,v))` over a `varbinary`
/// holding `0xFF` is `FF`, where the previous UTF-8 coercion here raised a
/// hard error.
///
/// The result charset is Go's `if types.IsBinaryStr(argType) {
/// types.SetBinChsClnFlag(bf.tp) }` over `args[1:]` (`:3314-3318`): ANY
/// binary candidate makes the whole function binary, not just the selected
/// one.
pub(crate) fn elt(vals: &[Datum]) -> Result<Datum, EvalError> {
    let Some(index) = crate::arg_eval_type::eval_int(&vals[0])? else {
        return Ok(Datum::Null);
    };
    if index < 1 || index as usize >= vals.len() {
        return Ok(Datum::Null);
    }
    let Some(selected) = crate::arg_eval_type::eval_string(&vals[index as usize])? else {
        return Ok(Datum::Null);
    };
    Ok(
        if vals[1..].iter().any(crate::string_signature::is_binary_str) {
            Datum::new_bytes_datum(selected)
        } else {
            Datum::new_string_datum(selected)
        },
    )
}

/// `CONCAT_WS(sep, a, b, ...)`: joins the non-`NULL` arguments with `sep`.
/// Unlike `CONCAT`, a `NULL` argument is SKIPPED (not propagated); only a
/// `NULL` separator yields `NULL` (confirmed via `gorun`).
#[cfg(test)]
pub(crate) fn concat_ws(vals: &[Datum]) -> Result<Datum, EvalError> {
    concat_ws_with_context(vals, &tidb_exprctx::context::NoColumns)
}

/// Value-evaluated CONCAT_WS with the statement packet limit applied.
pub(crate) fn concat_ws_with_context(
    vals: &[Datum],
    ctx: &dyn tidb_exprctx::context::Columns,
) -> Result<Datum, EvalError> {
    if vals.len() < 2 {
        return Err(EvalError::Unsupported(
            "CONCAT_WS requires at least two arguments",
        ));
    }
    let Some(sep) = coerce_str_bytes(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let mut out = Vec::new();
    let mut have_part = false;
    let mut target_len = 0_u64;
    for (index, value) in vals[1..].iter().enumerate() {
        let Some(value) = coerce_str_bytes(value)? else {
            continue;
        };
        target_len = target_len.saturating_add(value.len() as u64);
        if index > 0 {
            target_len = target_len.saturating_add(sep.len() as u64);
        }
        if target_len > ctx.max_allowed_packet() {
            crate::expression_errors::handle_allowed_packet_overflowed(ctx, "concat_ws")?;
            return Ok(Datum::Null);
        }
        if have_part {
            out.extend_from_slice(&sep);
        }
        out.extend_from_slice(&value);
        have_part = true;
    }
    Ok(Datum::new_string_datum(out))
}

/// `SUBSTRING_INDEX(str, delim, count)`: the substring before the `count`-th
/// occurrence of `delim` — from the left for `count > 0`, from the right for
/// `count < 0` (`SUBSTRING_INDEX('a.b.c.d', '.', -2)` = `c.d`). `count = 0`
/// yields the empty string; `|count|` past the number of parts yields the
/// whole string. `NULL` if any argument is `NULL`.
pub(crate) fn substring_index(vals: &[Datum]) -> Result<Datum, EvalError> {
    let [value, delim_value, count_value] = vals else {
        return Err(EvalError::Unsupported("bad SUBSTRING_INDEX arity"));
    };
    let (Some(s), Some(delim)) = (borrow_str_bytes(value)?, borrow_str_bytes(delim_value)?) else {
        return Ok(Datum::Null);
    };
    Ok(substring_index_bytes(
        &s,
        &delim,
        count_value,
        matches!(value, Datum::Bytes(_)),
    ))
}

pub(crate) fn substring_index_bytes(
    value: &[u8],
    delim: &[u8],
    count: &Datum,
    bytes_result: bool,
) -> Datum {
    if count.is_null() {
        return Datum::Null;
    }
    let result = if delim.is_empty() {
        &[]
    } else {
        substring_index_byte_slice(
            value,
            delim,
            crate::cast::to_i64_signed(count),
            matches!(count, Datum::UInt(_)),
        )
    };
    if bytes_result {
        Datum::new_bytes_datum(result)
    } else {
        Datum::new_string_datum(result)
    }
}

pub(crate) fn substring_index_byte_slice<'a>(
    value: &'a [u8],
    delim: &[u8],
    count: i64,
    unsigned: bool,
) -> &'a [u8] {
    if delim.is_empty() || count == 0 {
        return &[];
    }
    if unsigned && count < 0 {
        return value;
    }
    match usize::try_from(count.unsigned_abs()) {
        Ok(n) if n <= value.len() / delim.len() => substring_index_slice(value, delim, n, count < 0),
        _ => value,
    }
}

fn substring_index_slice<'a>(
    value: &'a [u8],
    delim: &[u8],
    n: usize,
    from_right: bool,
) -> &'a [u8] {
    let index = if let [byte] = delim {
        let mut matches = memchr::memchr_iter(*byte, value);
        let mut skip = n - 1;
        let mut from_back = from_right;
        // Bulk counting is cheaper than visiting every match for a deep seek.
        if n >= value.len().div_ceil(4).max(64) {
            let Some(other_side) = matches.clone().count().checked_sub(n) else {
                return value;
            };
            if other_side < skip {
                skip = other_side;
                from_back = !from_back;
            }
        }
        if from_back {
            matches.nth_back(skip)
        } else {
            matches.nth(skip)
        }
    } else if from_right {
        nth_delimiter_from_right(value, delim, n)
    } else if n == 1 {
        memmem::find(value, delim)
    } else {
        memmem::find_iter(value, delim).nth(n - 1)
    };
    match index {
        Some(index) if from_right => &value[index + delim.len()..],
        Some(index) => &value[..index],
        None => value,
    }
}

fn nth_delimiter_from_right(value: &[u8], delim: &[u8], n: usize) -> Option<usize> {
    // Go's split consumes the leftmost non-overlapping match first. A
    // rightmost match is safe if no earlier match overlaps its beginning.
    if n == 1 {
        let last = memmem::rfind(value, delim)?;
        let overlap = delim.len() - 1;
        let preceding = &value[last.saturating_sub(overlap)..last + overlap];
        if memmem::find(preceding, delim).is_none() {
            return Some(last);
        }
        return memmem::find_iter(value, delim).last();
    }

    let mut recent = [0; 8];
    if n <= recent.len() {
        let finder = memmem::Finder::new(delim);
        let mut offset = 0;
        let mut next = 0;
        let mut full = false;
        loop {
            let remaining = &value[offset..];
            if !remaining.starts_with(delim) {
                let Some(index) = finder.find(remaining) else {
                    break;
                };
                offset += index;
            }
            recent[next] = offset;
            offset += delim.len();
            next += 1;
            if next == n {
                full = true;
                next = 0;
            }
        }
        return full.then_some(recent[next]);
    }
    let mut matches = memmem::find_iter(value, delim);
    let skip = matches.clone().count().checked_sub(n)?;
    matches.nth(skip)
}

/// `INSERT(str, pos, len, newstr)`: replaces the `len` characters of `str`
/// starting at 1-based character `pos` with `newstr`. A `pos` outside
/// `1..=len(str)` leaves `str` unchanged (matching MySQL). `NULL` if any
/// argument is `NULL`.
///
/// A NEGATIVE `len` means "through the end of the string", exactly as an
/// oversized one does -- `builtinInsertUTF8Sig.evalString` clamps both with the
/// single condition `length > runeLength-pos+1 || length < 0`. Reading it as
/// zero instead would splice `newstr` in without removing anything.
pub(crate) fn str_insert(vals: &[Datum]) -> Result<Datum, EvalError> {
    // `insertFunctionClass.getFunction` selects `builtinInsertSig` (bytes) or
    // `builtinInsertUTF8Sig` (characters) from the RESULT type's charset,
    // which `addBinFlag` makes binary when either string argument is, so
    // `pos` and `len` count bytes as soon as the replacement is binary.
    let binary = crate::string_signature::is_binary_str(&vals[0])
        || crate::string_signature::is_binary_str(&vals[3]);
    // `pos` and `len` are Go's `types.ETInt` arguments, cast by
    // `crate::arg_eval_type`; each body below reads the `int64` carrier
    // `b.args[i].EvalInt` returns, so an UNSIGNED position keeps its bits
    // rather than turning the whole call NULL.
    let (Some(units), Some(pos), Some(len), Some(new)) = (
        StrUnits::of_with_signature(&vals[0], binary)?,
        crate::arg_eval_type::eval_int(&vals[1])?,
        crate::arg_eval_type::eval_int(&vals[2])?,
        coerce_str_bytes(&vals[3])?,
    ) else {
        return Ok(Datum::Null);
    };
    let n = units.len();
    if pos < 1 || pos as usize > n {
        return Ok(units.pack(units.bytes().to_vec()));
    }
    let start = (pos - 1) as usize;
    let remaining = n - start;
    let take = if len < 0 || len as u64 > remaining as u64 {
        remaining
    } else {
        len as usize
    };
    let end = start + take;
    let mut out = units.slice(0, start).to_vec();
    out.extend_from_slice(&new);
    out.extend_from_slice(units.slice(end, n));
    Ok(units.pack(out))
}

/// `MAKE_SET(bits, a, b, c, ...)`: a comma-joined set of the arguments whose
/// 1-based position has the corresponding bit set in `bits` (arg `i` is
/// included when bit `i-1` of `bits` is `1`). `NULL` arguments are excluded
/// even when their bit is set (matching MySQL). `NULL` if `bits` is `NULL`.
/// Go `builtinInsertSig` / `builtinInsertUTF8Sig`: a result longer than
/// `max_allowed_packet` is NULL with the packet-overflow warning.
pub(crate) fn insert_with_context(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    let result = str_insert(vals)?;
    let result_len = match &result {
        Datum::String(value) => value.bytes().len(),
        Datum::Bytes(value) => value.len(),
        Datum::Null => return Ok(Datum::Null),
        _ => return Err(EvalError::Unsupported("INSERT result type")),
    };
    if result_len as u64 > ctx.max_allowed_packet() {
        crate::expression_errors::handle_allowed_packet_overflowed(ctx, "insert")?;
        Ok(Datum::Null)
    } else {
        Ok(result)
    }
}

pub(crate) fn make_set(vals: &[Datum]) -> Result<Datum, EvalError> {
    // `bits` is Go's `types.ETInt` argument, cast by `crate::arg_eval_type`
    // before this body runs, so a SET/ENUM/BIT operand reaches here as the
    // ordinal integer Go's `builtinCastIntAsIntSig` reads out of it -- and
    // `1|4`'s unsigned bitwise OR keeps its raw 64-bit pattern, the same way
    // `bit_count` above does.
    let Some(bits) = crate::arg_eval_type::eval_int(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let bits = bits as u64;
    let mut output = Vec::new();
    let mut first = true;
    for (i, v) in vals[1..].iter().take(64).enumerate() {
        if bits & (1 << i) != 0 {
            if let Some(bytes) = borrow_str_bytes(v)? {
                if !first {
                    output.push(b',');
                }
                first = false;
                output.extend_from_slice(&bytes);
            }
        }
    }
    Ok(Datum::new_string_datum(output))
}
/// Go `EXPORT_SET(bits, on, off[, separator[, number_of_bits]])`
/// (`builtin_string.go:3403`/`:3434-3542`): bit 0 first, `separator`
/// between entries. The three-argument form uses `,` and 64 bits; the
/// four-argument form uses the given separator with 64 bits; the
/// five-argument form clamps a `number_of_bits` outside 0..=64 to 64. Any
/// `NULL` argument yields `NULL`.
pub(crate) fn export_set(vals: &[Datum]) -> Result<Datum, EvalError> {
    if !(3..=5).contains(&vals.len()) {
        return Err(EvalError::Unsupported("bad EXPORT_SET arguments"));
    }
    if vals.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    let bits = crate::arg_eval_type::eval_int(&vals[0])?.expect("non-NULL mask");
    let on = crate::arg_eval_type::eval_string_bytes(&vals[1])?.expect("non-NULL on value");
    let off = crate::arg_eval_type::eval_string_bytes(&vals[2])?.expect("non-NULL off value");
    let separator = match vals.get(3) {
        Some(value) => crate::arg_eval_type::eval_string_bytes(value)?.expect("non-NULL separator"),
        None => b",",
    };
    let count = match vals.get(4) {
        Some(value) => crate::arg_eval_type::eval_int(value)?.expect("non-NULL bit count"),
        None => 64,
    };
    let (width, fragments) = export_set_fragments(bits, on, off, separator, count);
    let mut result = Vec::with_capacity(width);
    for fragment in fragments {
        result.extend_from_slice(fragment);
    }
    Ok(Datum::new_string_datum(result))
}

/// Returns the byte width and the pieces of a Go EXPORT_SET result.
pub(crate) fn export_set_fragments<'a>(
    bits: i64,
    on: &'a [u8],
    off: &'a [u8],
    separator: &'a [u8],
    count: i64,
) -> (usize, impl ExactSizeIterator<Item = &'a [u8]>) {
    let count = if (0..=64).contains(&count) {
        count as usize
    } else {
        64
    };
    // Go uses signed `bits & (1 << i) > 0`, so bit 63 is always off.
    let bits =
        (bits as u64 & i64::MAX as u64) & u64::MAX.checked_shr((64 - count) as u32).unwrap_or(0);
    let on_count = bits.count_ones() as usize;
    let width = on_count * on.len()
        + (count - on_count) * off.len()
        + count.saturating_sub(1) * separator.len();
    let fragment_count = if width == 0 { 0 } else { count * 2 - 1 };
    let fragments = (0..fragment_count).map(move |index| {
        if index & 1 != 0 {
            separator
        } else if bits & (1 << (index / 2)) != 0 {
            on
        } else {
            off
        }
    });
    (width, fragments)
}

/// Evaluates `FROM_BASE64` with Go's signature-level packet bound. The Go
/// builtin checks the decoded-size estimate before stripping spaces/tabs and
/// invoking the decoder; keeping that check in the context-aware entry point
/// lets both the AST and chunk evaluators share the same warning/NULL policy
/// while the value-only helper remains useful for datatype-style tests.
pub(crate) fn from_base64_with_packet_limit(
    vals: &[Datum],
    ctx: Option<&dyn crate::Columns>,
) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("FROM_BASE64 arity"));
    }
    let Some(input) = borrow_str_bytes(&vals[0])? else {
        return Ok(Datum::Null);
    };
    if let Some(ctx) = ctx {
        if input.len() > (isize::MAX as usize) / 3 {
            return Ok(Datum::Null);
        }
        let estimated_len = input.len() * 3 / 4;
        if estimated_len as u64 > ctx.max_allowed_packet() {
            crate::expression_errors::handle_allowed_packet_overflowed(ctx, "from_base64")?;
            return Ok(Datum::Null);
        }
    }
    Ok(decode_base64(&input).map_or(Datum::Null, Datum::new_bytes_datum))
}

fn decode_base64(input: &[u8]) -> Option<Vec<u8>> {
    if let Ok(decoded) = base64_simd::STANDARD.decode_type(input) {
        return Some(decoded);
    }

    // Go discards spaces, tabs, CR and LF. It requires padding for short
    // final groups and ignores their unused bits. Check the cleaned length
    // and reject form feed before using the forgiving decoder for tail bits.
    let cleaned = strip_base64_whitespace(input);
    if !cleaned.len().is_multiple_of(4) || memchr::memchr(b'\x0c', &cleaned).is_some() {
        return None;
    }
    base64_simd::forgiving_decode_to_vec(&cleaned).ok()
}

fn strip_base64_whitespace(input: &[u8]) -> Cow<'_, [u8]> {
    let mut lines = memchr::memchr2_iter(b'\r', b'\n', input).peekable();
    let mut spaces = memchr::memchr2_iter(b' ', b'\t', input).peekable();
    let mut delimiters = std::iter::from_fn(|| match (lines.peek(), spaces.peek()) {
        (Some(line), Some(space)) if line < space => lines.next(),
        (_, Some(_)) => spaces.next(),
        _ => lines.next(),
    });
    let Some(first) = delimiters.next() else {
        return Cow::Borrowed(input);
    };
    let mut cleaned = Vec::with_capacity(input.len());
    cleaned.extend_from_slice(&input[..first]);
    let mut start = first + 1;
    for (count, index) in delimiters.enumerate() {
        // Copying whole runs pays off for wrapped lines. Switch to a byte
        // filter when separators are too dense to benefit from run copying.
        if count >= 16 && count > index / 8 {
            cleaned.extend(
                input[start..]
                    .iter()
                    .copied()
                    .filter(|byte| !matches!(byte, b' ' | b'\t' | b'\r' | b'\n')),
            );
            return Cow::Owned(cleaned);
        }
        cleaned.extend_from_slice(&input[start..index]);
        start = index + 1;
    }
    cleaned.extend_from_slice(&input[start..]);
    Cow::Owned(cleaned)
}

#[cfg(test)]
mod from_base64_tests {
    use crate::tests::builtin;
    use crate::Datum;

    fn s(value: &str) -> Datum {
        Datum::new_string_datum(value.to_string())
    }

    #[test]
    fn source_vectors_and_binary_boundaries() {
        for (input, expected) in [
            ("", b"".as_slice()),
            ("YWJj", b"abc".as_slice()),
            ("YWIgYw==", b"ab c".as_slice()),
            ("YWIKYw==", b"ab\nc".as_slice()),
            ("YWIJYw==", b"ab\tc".as_slice()),
            ("cXdlcnR5MTIzNDU2", b"qwerty123456".as_slice()),
            (
                "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrL0FCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4\neXowMTIzNDU2Nzg5Ky9BQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWmFiY2RlZmdoaWprbG1ub3Bx\ncnN0dXZ3eHl6MDEyMzQ1Njc4OSsv",
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".as_slice(),
            ),
            (
                "QUJDREVGR0hJSkt\tMTU5PUFFSU1RVVld\nYWVphYmNkZ\rWZnaGlqa2xt   bm9wcXJzdHV2d3h5ejAxMjM0NTY3ODkrLw==",
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".as_slice(),
            ),
        ] {
            assert_eq!(
                builtin("from_base64", &[s(input)]).unwrap(),
                Datum::new_bytes_datum(expected.to_vec())
            );
        }
        assert_eq!(
            builtin("from_base64", &[Datum::new_bytes_datum(b"/wA=".to_vec())]).unwrap(),
            Datum::new_bytes_datum(vec![0xff, 0x00])
        );
        assert_eq!(builtin("from_base64", &[Datum::Null]).unwrap(), Datum::Null);
    }

    #[test]
    fn malformed_padding_and_arity_return_null_or_error() {
        for input in ["asc", "YWJj=", "YWI", "YQ===", "YQ=A", "Y!Jj", "YW\x0cJj"] {
            assert_eq!(builtin("from_base64", &[s(input)]).unwrap(), Datum::Null, "{input:?}");
        }
        assert_eq!(
            builtin("from_base64", &[s("YQ==")]).unwrap(),
            Datum::new_bytes_datum(b"a".to_vec())
        );
        assert_eq!(
            builtin("from_base64", &[s("YWI=")]).unwrap(),
            Datum::new_bytes_datum(b"ab".to_vec())
        );
    }

    #[test]
    fn ignores_unused_bits_in_the_padded_final_group() {
        for (input, expected) in [("YR==", b"a".as_slice()), ("YWJ=", b"ab".as_slice())] {
            assert_eq!(
                builtin("from_base64", &[s(input)]).unwrap(),
                Datum::new_bytes_datum(expected.to_vec())
            );
        }
    }
}

/// Go `builtinOrdSig.evalInt` with the argument's declared charset. The
/// signature reads the first UTF-8 character from the in-memory value,
/// encodes it into that charset, then folds the first encoded character as a
/// base-256 integer. An unrepresentable character falls back to its first raw
/// byte.
pub(crate) fn ord_with_type(
    vals: &[Datum],
    arg_type: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    let [value] = vals else {
        return Err(EvalError::Unsupported("bad ORD arity"));
    };
    let Some(bytes) = borrow_str_bytes(value)? else {
        return Ok(Datum::Null);
    };
    let charset = arg_type.map_or_else(
        || {
            if crate::string_signature::is_binary_str(value) {
                "binary"
            } else {
                "utf8mb4"
            }
        },
        FieldType::charset_name,
    );
    Ok(Datum::Int(ord_from_bytes(&bytes, charset)))
}

pub(crate) fn ord_from_bytes(bytes: &[u8], charset: &str) -> i64 {
    ord_prefix(bytes, || find_encoding(charset))
}

pub(crate) fn ord_in_encoding(bytes: &[u8], encoding: Encoding) -> i64 {
    ord_prefix(bytes, || encoding)
}

fn ord_prefix(bytes: &[u8], encoding: impl FnOnce() -> Encoding) -> i64 {
    let Some(&first_byte) = bytes.first() else {
        return 0;
    };
    if first_byte.is_ascii() {
        return i64::from(first_byte);
    }
    let encoding = encoding();
    if matches!(encoding, Encoding::Binary | Encoding::Latin1) {
        return i64::from(first_byte);
    }
    let utf8_first = Encoding::Utf8.peek(bytes);
    let fold = |bytes: &[u8]| {
        bytes.iter().fold(0_i64, |acc, &byte| acc * 256 + i64::from(byte))
    };
    if encoding == Encoding::Utf8 {
        return if encoding.is_valid(utf8_first) {
            fold(utf8_first)
        } else {
            i64::from(first_byte)
        };
    }
    let (encoded, error) = encoding.transform_cow(utf8_first, TransformOp::ENCODE);
    let first = if error.is_some() {
        std::slice::from_ref(&first_byte)
    } else {
        encoding.peek(&encoded)
    };
    fold(first)
}

/// Go `expression.Quote`: `str` wrapped in single quotes with `'`, `\`, NUL
/// and Ctrl-Z backslash-escaped.
pub fn quote_str(str: &str) -> String {
    let mut out = String::with_capacity(str.len() + 2);
    out.push('\'');
    append_quoted_text(&mut out, str);
    out.push('\'');
    out
}

pub(crate) fn append_quoted(out: &mut String, input: &[u8]) {
    out.push('\'');
    if let Ok(valid) = simdutf8::basic::from_utf8(input) {
        append_quoted_text(out, valid);
    } else {
        for chunk in input.utf8_chunks() {
            append_quoted_text(out, chunk.valid());
            for _ in chunk.invalid() {
                out.push(char::REPLACEMENT_CHARACTER);
            }
        }
    }
    out.push('\'');
}

fn append_quoted_text(out: &mut String, input: &str) {
    let mut start = 0;
    for (index, byte) in input.bytes().enumerate() {
        let escaped = match byte {
            b'\'' => "\\'",
            b'\\' => "\\\\",
            0 => "\\0",
            0x1a => "\\Z",
            _ => continue,
        };
        out.push_str(&input[start..index]);
        out.push_str(escaped);
        start = index + 1;
    }
    out.push_str(&input[start..]);
}

/// `QUOTE(s)`: `s` wrapped in single quotes with `'`, `\`, NUL and Ctrl-Z
/// backslash-escaped — a value safe to paste into SQL. A `NULL` argument
/// yields the four-character string `NULL` (NOT SQL `NULL`), matching MySQL.
///
/// Go's `Quote` (`builtin_string.go:3228-3229`) opens with `runes :=
/// []rune(str)` and writes the runes back out, so the argument's BYTES are
/// decoded as UTF-8 with one U+FFFD substituted per malformed byte -- a
/// LOSSY step that is part of the answer, not an error. Captured from real
/// TiDB (`gorun`): `hex(quote(v))` over a `varbinary` holding `0xFF` is
/// `27EFBFBD27`, i.e. `'` U+FFFD `'`, and the same three bytes come back for
/// a `bit(8)` holding `b'11111111'`. The argument itself is
/// `crate::arg_eval_type`'s `types.ETString` cast
/// (`builtin_string.go:3180`).
pub(crate) fn quote(vals: &[Datum]) -> Result<Datum, EvalError> {
    let Some(bytes) = crate::arg_eval_type::eval_string_bytes(&vals[0])? else {
        return Ok(Datum::new_string_datum("NULL".to_string()));
    };
    let mut out = String::with_capacity(bytes.len() + 2);
    append_quoted(&mut out, bytes);
    // Go's `SetBinFlagOrBinStr(args[0].GetType(...), bf.tp)` (`:3184`): a
    // binary argument makes the quoted result binary too.
    Ok(if crate::string_signature::is_binary_str(&vals[0]) {
        Datum::new_bytes_datum(out.into_bytes())
    } else {
        Datum::new_string_datum(out)
    })
}

/// `BIT_COUNT(n)`: the number of set bits in `n` (as an unsigned 64-bit
/// value); `NULL` propagates. This follows the function's ETInt input
/// signature: decimal/real/string values first take their MySQL integer
/// coercion, whose statement warnings are outside this value-only domain.
pub(crate) fn bit_count(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    let raw: Option<&[u8]> = match &vals[0] {
        Datum::String(text) => Some(text.bytes()),
        Datum::Bytes(text) => Some(text),
        _ => None,
    };
    if let Some(text) = raw {
        if radix_truncated(text) {
            ctx.append_go_warning(
                &tidb_datatype::types_errors::ERR_TRUNCATED_WRONG_VAL,
                &format!(
                    "Truncated incorrect INTEGER value: '{}'",
                    String::from_utf8_lossy(text)
                ),
            );
        }
    }
    let bits = match &vals[0] {
        Datum::Null => return Ok(Datum::Null),
        Datum::Int(n) => *n as u64,
        Datum::UInt(n) => *n,
        Datum::Decimal(n) => n.round_to_i64_saturating() as u64,
        Datum::Real(n) => round_float_to_i64_saturating(*n) as u64,
        // BIT_COUNT's ETInt argument is built through Go's
        // `builtinCastStringAsIntSig`, which parses non-negative strings as
        // UINT64 before reinterpreting the result as the signed ETInt carrier.
        // A plain signed saturation would therefore be wrong for values in
        // `2^63..=u64::MAX` (for example, `2^63` has one set bit, not 63).
        // Keep the scan byte-oriented too: Go strings/bytes may contain
        // malformed UTF-8 after an otherwise valid numeric prefix, and the
        // source conversion still consumes that prefix.
        Datum::String(n) => bit_count_string_bytes(n.bytes()),
        Datum::Bytes(n) => bit_count_string_bytes(n),
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel BIT_COUNT argument"));
        }
        other => {
            other
                .to_i64()
                .map_err(|_| EvalError::Unsupported("BIT_COUNT argument conversion"))?
                .value as u64
        }
    };
    Ok(Datum::Int(i64::from(bits.count_ones())))
}

/// Coerces a raw string/bytes payload through the integer cast used by
/// `builtinBitCountSig`. The source's cast selects `StrToUint` for a
/// non-negative value, retaining all 64 bits, while a negative value uses
/// `StrToInt` and clamps below `i64::MIN` to that endpoint. A positive value
/// beyond `u64::MAX` likewise clamps to `u64::MAX` after the warning is
/// handled, so all three source boundaries are represented before the
/// population count runs.
fn bit_count_string_bytes(raw: &[u8]) -> u64 {
    // `strings.TrimSpace` runs before the source's sign check. Preserve its
    // Unicode behavior for valid strings, and retain a byte-safe ASCII trim
    // path for malformed Go string payloads.
    let raw = match std::str::from_utf8(raw) {
        Ok(s) => s.trim_start().as_bytes(),
        Err(_) => raw
            .iter()
            .position(|byte| !byte.is_ascii_whitespace())
            .map_or(&[][..], |start| &raw[start..]),
    };
    let negative = raw.len() > 1 && raw[0] == b'-';
    let digits = if negative || raw.first() == Some(&b'+') {
        &raw[1..]
    } else {
        raw
    };
    let digits = &digits[..digits
        .iter()
        .position(|byte| !byte.is_ascii_digit())
        .unwrap_or(digits.len())];
    if digits.is_empty() {
        return 0;
    }

    let mut magnitude = 0u64;
    let mut overflow = false;
    for &digit in digits {
        let next = magnitude
            .checked_mul(10)
            .and_then(|value| value.checked_add(u64::from(digit - b'0')));
        let Some(next) = next else {
            overflow = true;
            break;
        };
        magnitude = next;
    }

    if negative {
        if overflow || magnitude > i64::MAX as u64 {
            i64::MIN as u64
        } else {
            (-(magnitude as i64)) as u64
        }
    } else if overflow {
        u64::MAX
    } else {
        magnitude
    }
}

/// `FORMAT(x, d)`: the two-argument English-locale spelling.  See
/// [`format_num_locale`] for the shared port of TiDB's `FORMAT` evaluator.
pub(crate) fn format_num(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    format_num_locale(vals, Some("en_US"), ctx)
}

/// Shared `FORMAT(x, d[, locale])` evaluator, ported from
/// `evalNumDecArgsForFormat` and the two `builtinFormat*Sig.evalString`
/// bodies in `pkg/expression/builtin_string.go`.
///
/// ```text
/// x, d, isNull, err := evalNumDecArgsForFormat(ctx, b, row)   // x already rounded
/// formatString, found, err := mysql.FormatByLocale(x, d, locale)
/// if !isNull && !found { tc.AppendWarning(errUnknownLocale.FastGenByArgs(locale)) }
/// ```
///
/// Look up the grouping rules and the `found` flag through
/// `tidb_mysql::locale`, which ports `pkg/parser/mysql/locale_format.go`.
///
/// `locale` is `None` for the NULL a three-argument `FORMAT` evaluated to,
/// which Go warns about with the literal text `NULL` BEFORE it falls back to
/// `en_US`, and `Some("en_US")` for the two-argument form, which Go's
/// `builtinFormatSig` discards `found` for and never warns about. The two
/// are different states, not one default.
///
/// `ctx` is the statement warning sink, which `FORMAT`'s ETReal coercion of
/// a string argument raises 1292 on as well.
pub(crate) fn format_num_locale(
    vals: &[Datum],
    locale: Option<&str>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    let [number, precision, ..] = vals else {
        return Err(EvalError::Unsupported("bad FORMAT arguments"));
    };
    let Some(mut number) = format_number_text(number, ctx)? else {
        return Ok(Datum::Null);
    };
    let Some(precision) = format_precision(precision, ctx)? else {
        return Ok(Datum::Null);
    };
    let style = format_locale(locale, ctx);
    let mut formatted = Vec::new();
    append_format_number(&mut number, precision, style, &mut formatted);
    Ok(Datum::new_bytes_datum(formatted))
}

pub(crate) fn format_locale(
    locale: Option<&str>,
    ctx: &dyn crate::Columns,
) -> tidb_mysql::locale::LocaleFormatStyle {
    let (style, found) = tidb_mysql::locale::locale_format_style(locale.unwrap_or("en_US"));
    if locale.is_none() || !found {
        append_unknown_locale_warning(ctx, locale.unwrap_or("NULL"));
    }
    style
}

/// The shortest fixed-point spelling Go passes to roundFormatArgs.
pub(crate) fn append_format_real(number: f64, output: &mut Vec<u8>) {
    tidb_goruntime::go_strconv::append_float(output, number, b'f', -1, 64);
}

/// Go `formatMaxDecimals` (`pkg/expression/builtin_string.go`).
const FORMAT_MAX_DECIMALS: i64 = 30;

/// Go `errUnknownLocale` (`ErrUnknownLocale`, 1649).
fn append_unknown_locale_warning(ctx: &dyn crate::Columns, locale: &str) {
    ctx.append_go_warning(
        &crate::expression_errors::ERR_UNKNOWN_LOCALE,
        &format!("Unknown locale: '{locale}'"),
    );
}

fn format_number_text(
    value: &Datum,
    ctx: &dyn crate::Columns,
) -> Result<Option<Vec<u8>>, EvalError> {
    Ok(match value {
        Datum::Null => None,
        Datum::Int(n) => Some(n.to_string().into_bytes()),
        Datum::UInt(n) => Some(n.to_string().into_bytes()),
        Datum::Decimal(n) => Some(n.to_string().into_bytes()),
        Datum::Real(n) => {
            let mut output = Vec::new();
            append_format_real(*n, &mut output);
            Some(output)
        }
        // `FORMAT` requests ETReal for everything else, and that is the
        // NUMERIC reading of the argument, never its text: captured from
        // TiDB, `FORMAT(d,2)` on `DATE'2021-01-01'` is `20,210,101.00`,
        // `FORMAT(t,2)` on `TIME'10:20:30'` is `102,030.00`, an `enum` gives
        // its ordinal and a `json` string its numeric prefix. Rendering
        // `sql_string()` here instead formatted `'2021-01-01'` -- the text --
        // and answered `2.00`.
        other => {
            let mut output = Vec::new();
            append_format_real(crate::ops::to_f64_with_mysql_string(other, ctx)?, &mut output);
            Some(output)
        }
    })
}

fn format_precision(value: &Datum, ctx: &dyn crate::Columns) -> Result<Option<i64>, EvalError> {
    Ok(match value {
        Datum::Null => None,
        Datum::Int(n) => Some(*n),
        Datum::UInt(n) => Some(*n as i64),
        Datum::Decimal(n) => Some(n.round_to_i64_saturating()),
        Datum::Real(n) => Some(round_float_to_i64_saturating(*n)),
        Datum::String(_) | Datum::Bytes(_) => {
            Some(crate::cast::to_i64_signed_with_warnings(value, ctx)?)
        }
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel FORMAT precision"));
        }
        other => Some(
            other
                .to_i64()
                .map_err(|_| EvalError::Unsupported("FORMAT precision conversion"))?
                .value,
        ),
    })
}

fn round_float_to_i64_saturating(value: f64) -> i64 {
    let rounded = value.round_ties_even();
    if rounded < i64::MIN as f64 {
        i64::MIN
    } else if rounded >= i64::MAX as f64 {
        i64::MAX
    } else {
        rounded as i64
    }
}

/// Rounds a typed numeric spelling half up, then adds the locale separators.
/// Only the prefix retained at the clamped precision participates in the carry.
pub(crate) fn append_format_number(
    number: &mut Vec<u8>,
    precision: i64,
    style: tidb_mysql::locale::LocaleFormatStyle,
    output: &mut Vec<u8>,
) {
    let precision = precision.clamp(0, FORMAT_MAX_DECIMALS) as usize;
    let sign = usize::from(number.first() == Some(&b'-'));
    let fraction = if number.get(sign).is_some_and(u8::is_ascii_digit) {
        let mut point = memchr::memchr(b'.', number).unwrap_or(number.len());
        let mut end = number.len().min(point + 1 + precision);
        if number.get(end).is_some_and(|&digit| digit >= b'5') {
            let mut carry = true;
            for digit in number[sign..end].iter_mut().rev() {
                match *digit {
                    b'.' => {}
                    b'9' => *digit = b'0',
                    _ => {
                        *digit += 1;
                        carry = false;
                        break;
                    }
                }
            }
            if carry {
                number.insert(sign, b'1');
                point += 1;
                end += 1;
            }
        }
        if sign != 0 {
            output.push(b'-');
        }
        style.append_grouped_integer(&number[sign..point], output);
        &number[(point + 1).min(end)..end]
    } else {
        // Go FormatByLocale treats NaN and infinities as unsigned zero.
        output.push(b'0');
        &[]
    };
    if precision != 0 {
        output.extend_from_slice(style.decimal_point.as_bytes());
        output.extend_from_slice(fraction);
        output.resize(output.len() + precision - fraction.len(), b'0');
    }
}

/// `CHAR(n1, n2, ...)` (parser-renamed `CHAR_FUNC`) ported from
/// `builtinCharSig.convertToBytes` in `pkg/expression/builtin_string.go`.
/// No-`USING` CHAR returns `Datum::Bytes` exactly as TiDB's binary signature
/// does, including invalid UTF-8 and embedded NUL.
#[cfg(test)]
pub(crate) fn char_func(vals: &[Datum]) -> Result<Datum, EvalError> {
    char_func_with_context(vals, &tidb_exprctx::context::NoColumns)
}

pub(crate) fn char_func_with_context(
    vals: &[Datum],
    ctx: &dyn tidb_exprctx::context::Columns,
) -> Result<Datum, EvalError> {
    // The last argument is the charset sentinel appended by the parser.
    let Some((charset, nums)) = vals.split_last() else {
        return Err(EvalError::Unsupported("CHAR requires arguments"));
    };
    let mut bytes = Vec::new();
    for v in nums {
        match v {
            Datum::Null => {} // skipped, matching TiDB's EvalInt NULL path
            // Go's `WrapWithCastAsInt` over a JSON operand re-reads the
            // document's text as an integer (StrToInt), raising go's 1292
            // truncation warning when the text is not a clean integer —
            // captured: `CHAR(65, j)` over `{}` warns
            // "Truncated incorrect INTEGER value: '{}'".
            Datum::Json(value) => {
                let as_text = Datum::new_string_datum(value.to_string());
                crate::cast::report_int_truncation(&as_text, ctx)?;
                append_char_integer(&mut bytes, crate::cast::to_i64_signed(&as_text));
            }
            // go `WrapWithCastAsInt` over a STRING source reads the text as
            // an integer and warns `Truncated incorrect INTEGER value`
            // (captured: CHAR('中文测试', 'ünïcödé') warns against '中文测试'
            // before the charset error, so the warning survives it).
            Datum::String(_) | Datum::Bytes(_) => {
                crate::cast::report_int_truncation(v, ctx)?;
                append_char_integer(&mut bytes, crate::cast::to_i64_signed(v));
            }
            _ => append_char_integer(&mut bytes, crate::cast::to_i64_signed(v)),
        }
    }
    if *charset == Datum::Null {
        return Ok(Datum::new_bytes_datum(bytes));
    }

    // go evaluates the charset argument as a STRING (a numeric source
    // spells its text out: `-3.75` IS the charset name), then
    // `GetCharsetInfo` answers the bare 1105 `Unknown charset <name>`
    // BEFORE any encoding work happens.
    let charset_text = crate::coerce::coerce_str(charset)?
        .ok_or_else(|| EvalError::Unsupported("CHAR charset argument"))?;
    let charset = charset_text.to_ascii_lowercase();
    let collation_name = get_default_collation(&charset).map_err(|_| {
        EvalError::Unsupported(Box::leak(
            format!("Unknown charset {charset}").into_boxed_str(),
        ))
    })?;
    let (decoded, error) = find_encoding(&charset)
        .transform(&bytes, TransformOp::DECODE)
        .into_parts();
    if let Some(error) = error {
        // Go `charset.ErrInvalidCharacterString`.
        ctx.append_go_warning(
            &tidb_charset::charset_errors::ERR_INVALID_CHARACTER_STRING,
            &error.to_string(),
        );
        if ctx.strict_sql_mode() {
            return Ok(Datum::Null);
        }
    }
    let collation = Collation::from_name(&collation_name).ok_or_else(|| {
        EvalError::Unsupported(Box::leak(
            format!("Unknown charset {charset}").into_boxed_str(),
        ))
    })?;
    Ok(Datum::new_collation_string_datum(decoded, collation))
}

fn append_char_integer(bytes: &mut Vec<u8>, mut value: i64) {
    let mut current = Vec::with_capacity(4);
    for _ in 0..4 {
        current.push((value & 0xff) as u8);
        value >>= 8;
        if value == 0 {
            break;
        }
    }
    current.reverse();
    bytes.extend(current);
}

/// `TRIM([{BOTH|LEADING|TRAILING} [remstr]] FROM str)` / `TRIM(str)` /
/// `TRIM(remstr FROM str)`: repeatedly strips WHOLE occurrences of
/// `remstr` (never per-character) from the requested end(s) of `str` —
/// confirmed via `gorun`: `TRIM('xx' FROM 'xxhixx')` is `'hi'` (the
/// 2-character `remstr` removed as a unit, not char-by-char). An empty
/// `remstr` is a no-op (confirmed via `gorun`), guarded explicitly here
/// since `str::trim_start_matches`/`trim_end_matches` would otherwise
/// loop forever matching a zero-length pattern at every position.
/// `direction` defaults to `Both` when omitted (bare `TRIM(remstr FROM
/// str)` with no direction keyword, or bare `TRIM(str)` with an
/// implicit single-space `remstr`) — the caller already resolves BOTH
/// of those defaults before calling this (see `tidb_ast::Expr::Trim`'s
/// own doc for the exact `None`/`Some` shape). `NULL` if either operand
/// is `NULL`.
pub(crate) fn trim_value(
    str: Option<Vec<u8>>,
    remstr: Option<Vec<u8>>,
    direction: tidb_ast::TrimDirection,
    binary: bool,
) -> Datum {
    let (Some(mut str), Some(remstr)) = (str, remstr) else {
        return Datum::Null;
    };
    let range = trim_range(&str, &remstr, direction);
    str.truncate(range.end);
    str.drain(..range.start);
    if binary {
        Datum::new_bytes_datum(str)
    } else {
        Datum::new_string_datum(str)
    }
}

pub(crate) fn trim_range(
    value: &[u8],
    remove: &[u8],
    direction: tidb_ast::TrimDirection,
) -> std::ops::Range<usize> {
    use tidb_ast::TrimDirection::*;
    if remove == b" " {
        return trim_space_range(value, direction);
    }
    if remove.is_empty() {
        return 0..value.len();
    }
    let mut repeated = [0; 64];
    // Skip several complete repetitions per comparison for long runs.
    let block = if value.len() >= repeated.len() && remove.len() <= 16 {
        if let [byte] = remove {
            repeated.fill(*byte);
            Some(repeated.as_slice())
        } else {
            let len = repeated.len() / remove.len() * remove.len();
            for part in repeated[..len].chunks_exact_mut(remove.len()) {
                part.copy_from_slice(remove);
            }
            Some(&repeated[..len])
        }
    } else {
        None
    };
    let mut trimmed = value;
    if matches!(direction, Leading | Both) {
        if let Some(block) = block {
            while let Some(rest) = trimmed.strip_prefix(block) {
                trimmed = rest;
            }
        }
        while let Some(rest) = trimmed.strip_prefix(remove) {
            trimmed = rest;
        }
    }
    let start = value.len() - trimmed.len();
    if matches!(direction, Trailing | Both) {
        if let Some(block) = block {
            while let Some(rest) = trimmed.strip_suffix(block) {
                trimmed = rest;
            }
        }
        while let Some(rest) = trimmed.strip_suffix(remove) {
            trimmed = rest;
        }
    }
    start..start + trimmed.len()
}

/// Go's space cutset consists of one ASCII byte.
#[inline]
pub(crate) fn trim_space_range(
    value: &[u8],
    direction: tidb_ast::TrimDirection,
) -> std::ops::Range<usize> {
    use tidb_ast::TrimDirection::*;
    const SPACES: &[u8; 32] = &[b' '; 32];
    let mut trimmed = value;
    if matches!(direction, Leading | Both) {
        if trimmed.first() == Some(&b' ') {
            while let Some(rest) = trimmed.strip_prefix(SPACES) {
                trimmed = rest;
            }
            let prefix = trimmed.iter().take_while(|&&byte| byte == b' ').count();
            trimmed = &trimmed[prefix..];
        }
    }
    let start = value.len() - trimmed.len();
    if matches!(direction, Trailing | Both) {
        if trimmed.last() == Some(&b' ') {
            while let Some(rest) = trimmed.strip_suffix(SPACES) {
                trimmed = rest;
            }
            let suffix = trimmed.iter().rev().take_while(|&&byte| byte == b' ').count();
            trimmed = &trimmed[..trimmed.len() - suffix];
        }
    }
    start..start + trimmed.len()
}

#[cfg(test)]
mod bit_count_tests {
    use super::bit_count;
    use crate::{Datum, Decimal};

    /// Full value vector from `pkg/expression/builtin_other_test.go`'s
    /// `TestBitCount`, including its UInt64 regression case.
    #[test]
    fn bit_count_matches_go_source_vectors() {
        let cases = [
            (Datum::Int(8), 1),
            (Datum::Int(29), 4),
            (Datum::Int(0), 0),
            (Datum::Int(-1), 64),
            (Datum::Int(-11), 62),
            (Datum::Int(-1000), 56),
            (Datum::Real(1.1), 1),
            (Datum::Real(3.1), 2),
            (Datum::Real(-1.1), 64),
            (Datum::Real(-3.1), 63),
            (Datum::UInt(u64::MAX), 64),
            // The Go ETInt cast preserves the low 64 bits for a positive
            // string up to UINT64_MAX; it does not saturate at INT64_MAX.
            (Datum::new_string_datum("9223372036854775808"), 1),
            (Datum::new_string_datum("18446744073709551616"), 64),
            (Datum::new_string_datum("xxx".to_string()), 0),
            (Datum::Decimal(Decimal::from_literal("3.1")), 2),
        ];
        for (input, want) in cases {
            assert_eq!(
                bit_count(&[input], &tidb_exprctx::context::NoColumns),
                Ok(Datum::Int(want))
            );
        }
        assert_eq!(
            bit_count(&[Datum::Null], &tidb_exprctx::context::NoColumns),
            Ok(Datum::Null)
        );
        // Go's string conversion is byte-prefix based, so malformed UTF-8
        // after an ASCII number must not erase the numeric prefix.
        assert_eq!(
            bit_count(
                &[Datum::new_bytes_datum(vec![b'1', 0xff])],
                &tidb_exprctx::context::NoColumns
            ),
            Ok(Datum::Int(1))
        );
    }
}

#[cfg(test)]
mod concat_source_tests {
    use super::{concat, concat_ws};
    use crate::{Datum, Decimal, EvalError};

    fn string(value: &str) -> Datum {
        Datum::new_string_datum(value.to_string())
    }

    /// Representable scalar rows from `TestConcat` in
    /// `pkg/expression/builtin_string_test.go`.  The Go table's typed
    /// datetime/duration values have already crossed their `EvalString`
    /// boundary here as their exact rendered text; injected Go errors and
    /// packet-warning/session metadata remain outside the value-only API.
    #[test]
    fn concat_matches_go_source_scalar_rows() {
        assert_eq!(concat(&[Datum::Null]), Ok(Datum::Null));
        assert_eq!(
            concat(&[
                string("a"),
                string("b"),
                Datum::Int(1),
                Datum::Int(2),
                Datum::Real(1.1),
                Datum::Real(1.2),
                Datum::Decimal(Decimal::from_literal("1.1")),
                string("2000-01-01 12:01:01"),
                string("12:01:01"),
            ]),
            Ok(string("ab121.11.21.12000-01-01 12:01:0112:01:01"))
        );
        assert_eq!(
            concat(&[string("a"), string("b"), Datum::Null, string("c")]),
            Ok(Datum::Null)
        );

        // Go's ETString is a byte boundary.  Invalid UTF-8 from a binary
        // argument must survive CONCAT rather than becoming an evaluator
        // error or replacement character.
        assert_eq!(
            concat(&[Datum::new_bytes_datum(vec![0xff]), string("a")]),
            Ok(Datum::new_string_datum(vec![0xff, b'a']))
        );
        assert_eq!(
            concat(&[]),
            Err(EvalError::Unsupported(
                "CONCAT requires at least one argument"
            ))
        );
    }

    /// Representable scalar rows from `TestConcatWS`: a NULL separator
    /// propagates, later NULL fields are skipped, and empty fields still
    /// contribute separators.  Numeric and already-rendered temporal values
    /// use the same byte-preserving coercion as CONCAT.
    #[test]
    fn concat_ws_matches_go_source_scalar_rows() {
        assert_eq!(concat_ws(&[Datum::Null, Datum::Null]), Ok(Datum::Null));
        assert_eq!(
            concat_ws(&[Datum::Null, string("a"), string("b")]),
            Ok(Datum::Null)
        );
        assert_eq!(
            concat_ws(&[
                string(","),
                string("a"),
                string("b"),
                string("hello"),
                string("$^%"),
            ]),
            Ok(string("a,b,hello,$^%"))
        );
        assert_eq!(
            concat_ws(&[
                string("|"),
                string("a"),
                Datum::Null,
                string("b"),
                string("c")
            ]),
            Ok(string("a|b|c"))
        );
        assert_eq!(
            concat_ws(&[
                string(","),
                string("a"),
                string(","),
                string("b"),
                string("c"),
            ]),
            Ok(string("a,,,b,c"))
        );
        assert_eq!(
            concat_ws(&[
                string(","),
                string("a"),
                string("b"),
                Datum::Int(1),
                Datum::Int(2),
                Datum::Real(1.1),
                Datum::Real(0.11),
                Datum::Decimal(Decimal::from_literal("1.1")),
                string("2000-01-01 12:01:01"),
                string("12:01:01"),
            ]),
            Ok(string("a,b,1,2,1.1,0.11,1.1,2000-01-01 12:01:01,12:01:01"))
        );
        assert_eq!(
            concat_ws(&[string(","), string("a"), string("")]),
            Ok(string("a,"))
        );
        assert_eq!(
            concat_ws(&[
                Datum::new_bytes_datum(vec![b'|']),
                Datum::new_bytes_datum(vec![0xff]),
                Datum::Null,
                string("b")
            ]),
            Ok(Datum::new_string_datum(vec![0xff, b'|', b'b']))
        );
        assert_eq!(
            concat_ws(&[string(",")]),
            Err(EvalError::Unsupported(
                "CONCAT_WS requires at least two arguments"
            ))
        );
    }
}

#[cfg(test)]
mod format_tests {
    use super::format_num;
    use crate::{Datum, Decimal};

    fn fmt(number: &str, precision: i64) -> String {
        let n = Datum::Decimal(if let Some(mag) = number.strip_prefix('-') {
            Decimal::from_literal(mag).negate()
        } else {
            Decimal::from_literal(number)
        });
        // `FormatByLocale` counts and groups the integer part in BYTES, so
        // its result is bytes rather than a UTF-8 string; every row here is
        // ASCII, so reading them back as text is exact.
        match format_num(&[n, Datum::Int(precision)], &crate::NoColumns).unwrap() {
            Datum::Bytes(bytes) => String::from_utf8(bytes).unwrap(),
            other => panic!("expected bytes, got {other:?}"),
        }
    }

    /// FORMAT rounds HALF AWAY FROM ZERO (2.5 -> 3, -2.5 -> -3), groups the
    /// integer part with `,` every three digits, pads to `precision` decimals,
    /// and clamps a negative precision to 0. Authoritative goeval values.
    #[test]
    fn format_rounds_half_away_from_zero_and_groups_thousands() {
        assert_eq!(fmt("2.5", 0), "3");
        assert_eq!(fmt("2.4", 0), "2");
        assert_eq!(fmt("-2.5", 0), "-3");
        assert_eq!(fmt("1234567.891", 2), "1,234,567.89");
        assert_eq!(fmt("1234.5678", 2), "1,234.57");
        assert_eq!(fmt("-1234.5", 0), "-1,235");
        assert_eq!(fmt("1.9999", 2), "2.00");
        assert_eq!(fmt("123.456", -1), "123");
    }
}

#[cfg(test)]
mod hex_bit_column_tests {
    use super::hex;
    use crate::Datum;
    use tidb_datatype::BinaryLiteral;

    fn bit(bytes: &[u8]) -> Datum {
        Datum::Bit(BinaryLiteral::from(bytes.to_vec()))
    }

    /// `hexFunctionClass.getFunction` reads `args[0].GetType().EvalType()`,
    /// and `mysql.TypeBit` is `types.ETInt` -- so a stored `BIT` value is
    /// hexed as a NUMBER and its storage padding disappears, where a
    /// `BINARY`/`BLOB` of the same bytes keeps every octet.
    ///
    /// Captured from real TiDB through `gorun`, on `bit(48)`, `bit(64)` and
    /// `bit(1)` columns:
    ///
    /// ```text
    /// bit(48) = _binary '\0\b\n\r\t\Z'  -> 80A0D091A
    /// bit(48) = x'0041'                 -> 41
    /// bit(64) = all ones                -> FFFFFFFFFFFFFFFF
    /// bit(1)  = b'1'                    -> 1
    /// bit(48) = 0                       -> 0
    /// blob    = unhex('00080A0D091A')   -> 00080A0D091A
    /// ```
    #[test]
    fn a_bit_value_is_hexed_as_a_number_and_a_blob_is_hexed_as_bytes() {
        let payload = [0x00, 0x08, 0x0A, 0x0D, 0x09, 0x1A];
        assert_eq!(
            hex(&[bit(&payload)]).unwrap(),
            Datum::new_string_datum("80A0D091A".to_owned())
        );
        // The same six bytes as a blob keep their leading zero byte: this is
        // the pair the recording compares, and the whole difference is which
        // signature the argument's TYPE selects.
        assert_eq!(
            hex(&[Datum::new_bytes_datum(payload.to_vec())]).unwrap(),
            Datum::new_string_datum("00080A0D091A".to_owned())
        );
        assert_eq!(
            hex(&[bit(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x41])]).unwrap(),
            Datum::new_string_datum("41".to_owned())
        );
        // A `bit(64)` with the high bit set is why this reads the payload as
        // UNSIGNED: the signed conversion would refuse it outright.
        assert_eq!(
            hex(&[bit(&[0xFF; 8])]).unwrap(),
            Datum::new_string_datum("FFFFFFFFFFFFFFFF".to_owned())
        );
        assert_eq!(
            hex(&[bit(&[0x01])]).unwrap(),
            Datum::new_string_datum("1".to_owned())
        );
        assert_eq!(
            hex(&[bit(&[0x00; 6])]).unwrap(),
            Datum::new_string_datum("0".to_owned())
        );
    }

    /// A hex LITERAL is not a `BIT` value: Go types `x'0041'` as
    /// `mysql.TypeVarString`, so it takes the string signature and keeps its
    /// leading zero byte. `hex(x'0041')` is `0041` in TiDB, and this is the
    /// datum that carries it.
    #[test]
    fn a_binary_literal_keeps_the_string_signature() {
        assert_eq!(
            hex(&[Datum::BinaryLiteral(BinaryLiteral::from(vec![0x00, 0x41]))]).unwrap(),
            Datum::new_string_datum("0041".to_owned())
        );
    }
}

#[cfg(test)]
mod export_set_tests {
    use super::*;

    fn d(s: &str) -> Datum {
        Datum::new_string_datum(s.to_string())
    }

    #[test]
    fn export_set_matches_go_bit_order_and_arity_forms() {
        // MySQL doc example: bits 0 and 2 set within 4 bits → Y,N,Y,N.
        assert_eq!(
            export_set(&[Datum::Int(5), d("Y"), d("N"), d(","), Datum::Int(4)]).unwrap(),
            d("Y,N,Y,N")
        );
        // Bit 0 first: 6 = 0b110 within 10 bits.
        assert_eq!(
            export_set(&[Datum::Int(6), d("1"), d("0"), d(","), Datum::Int(10)]).unwrap(),
            d("0,1,1,0,0,0,0,0,0,0")
        );
        // Three-argument form: `,` separator, 64 bits (bits 0 and 3 set).
        let three = export_set(&[Datum::Int(9), d("Y"), d("N")]).unwrap();
        eprintln!("PROBE three: {:?}", three);
        assert_eq!(three, d(&format!("Y,N,N,Y{}", ",N".repeat(60))));
        // Four-argument form: the custom separator with 64 bits.
        let expected_four = format!("Y@N@N@Y{}", "@N".repeat(60));
        assert_eq!(
            export_set(&[Datum::Int(9), d("Y"), d("N"), d("@")]).unwrap(),
            d(&expected_four)
        );
        // Five-argument form: a negative or oversized number_of_bits clamps
        // to 64; zero yields the empty string.
        assert_eq!(
            export_set(&[Datum::Int(5), d("Y"), d("N"), d(","), Datum::Int(100)]).unwrap(),
            d(&format!("Y,N,Y,N{}", ",N".repeat(60)))
        );
        assert_eq!(
            export_set(&[Datum::Int(5), d("Y"), d("N"), d(","), Datum::Int(0)]).unwrap(),
            d("")
        );
    }

    #[test]
    fn export_set_null_arguments_propagate_like_go() {
        for args in [
            vec![Datum::Null, d("Y"), d("N")],
            vec![Datum::Int(5), Datum::Null, d("N")],
            vec![Datum::Int(5), d("Y"), Datum::Null],
            vec![Datum::Int(5), d("Y"), d("N"), Datum::Null],
            vec![Datum::Int(5), d("Y"), d("N"), d(","), Datum::Null],
        ] {
            assert_eq!(
                export_set(&args).unwrap(),
                Datum::Null,
                "any NULL argument yields NULL"
            );
        }
    }

    #[test]
    fn export_set_is_reachable_through_the_values_dispatch() {
        let result = crate::tests::eval_builtin_func(
            "EXPORT_SET",
            &[Datum::Int(5), d("Y"), d("N"), d(","), Datum::Int(4)],
            &tidb_exprctx::context::NoColumns,
        );
        assert!(result.is_some(), "the dispatch must own EXPORT_SET");
        assert_eq!(result.unwrap().unwrap(), d("Y,N,Y,N"));
    }
}
