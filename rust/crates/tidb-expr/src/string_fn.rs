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
//! `LEFT`/`RIGHT`, `SUBSTRING`), dispatched from `crate::func::eval_func`,
//! plus `position`/`trim_value` below — the SAME kind of pure string
//! operation, but dispatched directly from `crate::eval_in`'s own
//! `Expr::Position`/`Expr::Trim` arms instead, since those are dedicated
//! AST variants (`POSITION(a IN b)`/`TRIM(...)`'s own grammar), not
//! ordinary `Expr::Func` calls.

use crate::coerce::{coerce_str, coerce_str_bytes};
use crate::ops::to_f64_with_mysql_string;
use crate::{Datum, EvalError};

/// CONCAT: `NULL` if any argument is `NULL`, else the concatenation.
pub(crate) fn concat(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.is_empty() {
        return Err(EvalError::Unsupported(
            "CONCAT requires at least one argument",
        ));
    }
    let mut out = Vec::new();
    for v in vals {
        match coerce_str_bytes(v)? {
            Some(s) => out.extend_from_slice(&s),
            None => return Ok(Datum::Null),
        }
    }
    Ok(Datum::new_string(out))
}

/// Applies a single-argument string function, propagating `NULL`.
pub(crate) fn str_unary(vals: &[Datum], f: impl Fn(&str) -> Datum) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    match coerce_str(&vals[0])? {
        Some(s) => Ok(f(&s)),
        None => Ok(Datum::Null),
    }
}

/// `LOWER`/`UPPER`: text signatures apply Unicode case mapping while binary
/// signatures return the original bytes unchanged (the source selects those
/// signatures from the argument FieldType before evaluation).  Numeric values
/// still pass through the ordinary ETString conversion.
pub(crate) fn case_convert(vals: &[Datum], upper: bool) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad case-conversion arity"));
    }
    match &vals[0] {
        Datum::Null => Ok(Datum::Null),
        Datum::Bytes(bytes) => Ok(Datum::new_bytes(bytes.clone())),
        value => match coerce_str(value)? {
            Some(text) => Ok(Datum::new_string(if upper {
                text.to_uppercase()
            } else {
                text.to_lowercase()
            })),
            None => Ok(Datum::Null),
        },
    }
}

/// `ASCII(s)`: return the first byte of the evaluated string, not the first
/// Unicode scalar value. Go's `EvalString` preserves binary arguments, so
/// this uses byte-preserving coercion instead of `str_unary`'s UTF-8 check.
pub(crate) fn ascii(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(bytes) = coerce_str_bytes(&vals[0])? else {
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
    let Some(bytes) = coerce_str_bytes(&vals[0])? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Int((bytes.len() as i64) * 8))
}

/// LEFT/RIGHT: the first or last `n` characters (`NULL` if any arg is `NULL`).
pub(crate) fn str_take(vals: &[Datum], from_left: bool) -> Result<Datum, EvalError> {
    if vals.len() != 2 {
        return Err(EvalError::Unsupported("bad LEFT/RIGHT arguments"));
    }
    let n = match &vals[1] {
        Datum::Null => return Ok(Datum::Null),
        value => crate::cast::to_i64_signed(value).max(0) as usize,
    };
    match &vals[0] {
        Datum::Null => Ok(Datum::Null),
        // Go selects the binary signature for binary strings, where LEFT and
        // RIGHT count raw bytes and preserve invalid UTF-8.  Keep that
        // boundary distinct from the UTF-8 character signature below.
        Datum::Bytes(bytes) => {
            let start = if from_left {
                0
            } else {
                bytes.len().saturating_sub(n)
            };
            let end = if from_left {
                bytes.len().min(n)
            } else {
                bytes.len()
            };
            Ok(Datum::new_bytes(bytes[start..end].to_vec()))
        }
        value => {
            if value.collation() == Some(tidb_datatype::Collation::Binary) {
                let bytes = coerce_str_bytes(value)?.expect("non-NULL value has bytes");
                let start = if from_left {
                    0
                } else {
                    bytes.len().saturating_sub(n)
                };
                let end = if from_left {
                    bytes.len().min(n)
                } else {
                    bytes.len()
                };
                return Ok(Datum::new_bytes(bytes[start..end].to_vec()));
            }
            let Some(text) = coerce_str(value)? else {
                return Ok(Datum::Null);
            };
            let chars: Vec<char> = text.chars().collect();
            let start = if from_left {
                0
            } else {
                chars.len().saturating_sub(n)
            };
            let end = if from_left {
                chars.len().min(n)
            } else {
                chars.len()
            };
            Ok(Datum::new_string(
                chars[start..end].iter().collect::<String>(),
            ))
        }
    }
}

/// `SUBSTRING(s, pos[, len])`: 1-indexed and character-based for this
/// UTF-8-only value domain.  This ports `builtinSubstring2ArgsUTF8Sig` and
/// `builtinSubstring3ArgsUTF8Sig` in `pkg/expression/builtin_string.go`:
/// negative positions count back from the end, while position zero and every
/// out-of-range position produce the empty string.
pub(crate) fn substring(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.contains(&Datum::Null) {
        return Ok(Datum::Null);
    }
    let (s, pos, length) = match vals {
        [str, Datum::Int(pos)] => (coerce_str(str)?, *pos, None),
        [str, Datum::Int(pos), Datum::Int(length)] => (coerce_str(str)?, *pos, Some(*length)),
        _ => return Err(EvalError::Unsupported("bad SUBSTRING arguments")),
    };
    let Some(s) = s else {
        return Ok(Datum::Null);
    };
    let chars: Vec<char> = s.chars().collect();
    let string_len = chars.len() as i64;
    let pos = if pos < 0 { pos + string_len } else { pos - 1 };
    let start = if !(0..=string_len).contains(&pos) {
        chars.len()
    } else {
        pos as usize
    };
    let end = match length {
        None => chars.len(),
        Some(length) if length <= 0 => start,
        Some(length) => {
            // Go's source computes `end := pos + length` in int64.  A
            // positive length can therefore wrap when `pos > 0`, and the
            // following `end < pos` branch returns the empty string.  Do
            // not use saturating_add here: it would silently turn that
            // source-visible overflow into an unexpectedly long tail.
            let Some(end) = (start as i64).checked_add(length) else {
                return Ok(Datum::new_string(String::new()));
            };
            (end as usize).min(chars.len())
        }
    };
    Ok(Datum::new_string(
        chars[start..end].iter().collect::<String>(),
    ))
}

/// `POSITION(substr IN str)`: the 1-indexed, character-based position of
/// `substr`'s first occurrence in `str`; `0` if not found; an empty
/// `substr` always matches at position `1` (confirmed via `gorun`).
/// `NULL` if either operand is `NULL`.
pub(crate) fn position(substr: Option<String>, str: Option<String>) -> Datum {
    let (Some(substr), Some(str)) = (substr, str) else {
        return Datum::Null;
    };
    let substr: Vec<char> = substr.chars().collect();
    let str: Vec<char> = str.chars().collect();
    if substr.is_empty() {
        return Datum::Int(1);
    }
    if substr.len() > str.len() {
        return Datum::Int(0);
    }
    for start in 0..=(str.len() - substr.len()) {
        if str[start..start + substr.len()] == substr[..] {
            return Datum::Int(start as i64 + 1);
        }
    }
    Datum::Int(0)
}

/// `REPEAT(str, count)`: `str` concatenated `count` times (empty for
/// `count <= 0`); `NULL` if either argument is `NULL`.
///
/// TiDB selects an `ETString, ETInt` signature, so the count follows the
/// same signed-integer coercion boundary as `EvalInt` (including string
/// numeric prefixes, decimal rounding, and float ties-to-even).  Go strings
/// are byte sequences rather than guaranteed UTF-8; retaining the evaluated
/// string bytes keeps binary input lossless here.  The context-free Rust
/// evaluator uses TiDB's default 64 MiB `max_allowed_packet`; the warning
/// channel used by `builtinRepeatSig.evalString` is outside this value-only
/// API, so an oversized result is represented as `NULL` without a warning.
pub(crate) fn repeat(vals: &[Datum]) -> Result<Datum, EvalError> {
    let [value, count] = vals else {
        return Err(EvalError::Unsupported("bad REPEAT arity"));
    };
    let Some(value) = coerce_str_bytes(value)? else {
        return Ok(Datum::Null);
    };
    if *count == Datum::Null {
        return Ok(Datum::Null);
    }
    let count = crate::cast::to_i64_signed(count);
    if count <= 0 || value.is_empty() {
        return Ok(Datum::new_string(Vec::<u8>::new()));
    }
    let count = count.min(i64::from(i32::MAX)) as usize;
    const DEFAULT_MAX_ALLOWED_PACKET: usize = 64 << 20;
    let Some(output_len) = value.len().checked_mul(count) else {
        return Ok(Datum::Null);
    };
    if output_len > DEFAULT_MAX_ALLOWED_PACKET {
        return Ok(Datum::Null);
    }
    let mut output = Vec::with_capacity(output_len);
    for _ in 0..count {
        output.extend_from_slice(&value);
    }
    Ok(Datum::new_string(output))
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
        coerce_str_bytes(value)?,
        coerce_str_bytes(from)?,
        coerce_str_bytes(to)?,
    ) else {
        return Ok(Datum::Null);
    };
    if from.is_empty() {
        return Ok(string_result(value, s));
    }
    Ok(string_result(value, replace_bytes(&s, &from, &to)))
}

/// Replaces non-overlapping byte occurrences, matching Go's
/// `strings.ReplaceAll` over an arbitrary Go string.  Keeping this byte based
/// means binary arguments never pass through a lossy UTF-8 decode.
fn replace_bytes(value: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len());
    let mut cursor = 0;
    while cursor <= value.len() {
        let Some(relative) = value[cursor..]
            .windows(from.len())
            .position(|window| window == from)
        else {
            out.extend_from_slice(&value[cursor..]);
            break;
        };
        let start = cursor + relative;
        out.extend_from_slice(&value[cursor..start]);
        out.extend_from_slice(to);
        cursor = start + from.len();
    }
    out
}

/// Preserve binary result semantics when the source string is binary; normal
/// SQL strings retain the ordinary String datum/collation boundary.
fn string_result(source: &Datum, bytes: Vec<u8>) -> Datum {
    if matches!(source, Datum::Bytes(_)) {
        Datum::new_bytes(bytes)
    } else {
        Datum::new_string(bytes)
    }
}

/// `SPACE(n)`: a string of `n` spaces (empty for `n <= 0`); `NULL` if the
/// argument is `NULL`.  This is the `ETInt` signature from
/// `builtinSpaceSig.evalString` in `pkg/expression/builtin_string.go`, not
/// an integer-literal-only convenience: decimal arguments round away from
/// zero while float arguments round ties to even through the shared `EvalInt`
/// conversion.  TiDB returns `NULL` rather than allocating above
/// `mysql.MaxBlobWidth`.
pub(crate) fn space(vals: &[Datum]) -> Result<Datum, EvalError> {
    // The value-only evaluator has no session context, so keep its existing
    // default packet limit while exposing the source signature's actual
    // boundary as a small parameterized helper for context-aware callers and
    // direct source-vector tests.
    space_with_max_allowed_packet(vals, 64 << 20)
}

/// Evaluates `SPACE(n)` with the caller's `max_allowed_packet` limit.
///
/// `builtinSpaceSig.evalString` checks the session packet limit before the
/// `mysql.MaxBlobWidth` result limit.  Both limits map to `NULL` in this
/// value-only API because statement warnings are intentionally outside its
/// result surface; preserving the order here keeps a future session-aware
/// caller on the same source path rather than duplicating the edge checks.
pub(crate) fn space_with_max_allowed_packet(
    vals: &[Datum],
    max_allowed_packet: u64,
) -> Result<Datum, EvalError> {
    let [value] = vals else {
        return Err(EvalError::Unsupported("bad SPACE arity"));
    };
    if *value == Datum::Null {
        return Ok(Datum::Null);
    }
    let width = crate::cast::to_i64_signed(value);
    let width = width.max(0);
    if (width as u64) > max_allowed_packet {
        return Ok(Datum::Null);
    }
    const MAX_BLOB_WIDTH: i64 = 16_777_216; // pkg/parser/mysql.MaxBlobWidth
    if width > MAX_BLOB_WIDTH {
        return Ok(Datum::Null);
    }
    Ok(Datum::new_string(" ".repeat(width as usize)))
}

/// `STRCMP(a, b)`: `-1`/`0`/`1` under TiDB's default `utf8mb4_bin` PAD SPACE
/// collation; a binary operand switches the source signature to the raw
/// binary collation. `NULL` propagates from either operand.
pub(crate) fn strcmp(vals: &[Datum]) -> Result<Datum, EvalError> {
    let [left, right] = vals else {
        return Err(EvalError::Unsupported("bad STRCMP arity"));
    };
    let (Some(a), Some(b)) = (coerce_str_bytes(left)?, coerce_str_bytes(right)?) else {
        return Ok(Datum::Null);
    };
    let collation = if matches!(left, Datum::Bytes(_)) || matches!(right, Datum::Bytes(_)) {
        tidb_datatype::Collation::Binary
    } else {
        left.collation()
            .or_else(|| right.collation())
            .unwrap_or(tidb_datatype::Collation::DEFAULT)
    };
    Ok(Datum::Int(match collation.compare(&a, &b) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }))
}

/// `LPAD(str, len, pad)` / `RPAD(str, len, pad)`: pad (or truncate) `str` to
/// `len` characters using `pad` on the left/right. Ported from
/// `builtinLpadUTF8Sig`/`builtinRpadUTF8Sig` in `pkg/expression/
/// builtin_string.go` (rune-based, the default for non-binary strings): a
/// NEGATIVE `len` yields `NULL` (not the empty string); `len == 0` yields
/// the empty string; truncation keeps the first `len` chars; an empty `pad`
/// that can't reach `len` yields the empty string. `NULL` if any argument is
/// `NULL` or `len` exceeds TiDB's `mysql.MaxBlobWidth`.
pub(crate) fn pad(vals: &[Datum], left: bool) -> Result<Datum, EvalError> {
    const MAX_BLOB_WIDTH: i64 = 16_777_216;
    if vals.len() != 3 {
        return Err(EvalError::Unsupported("bad LPAD/RPAD arguments"));
    }
    let len = match &vals[1] {
        Datum::Null => return Ok(Datum::Null),
        value => crate::cast::to_i64_signed(value),
    };
    if !(0..=MAX_BLOB_WIDTH).contains(&len) {
        return Ok(Datum::Null);
    }
    let binary = matches!(vals[0], Datum::Bytes(_))
        || matches!(vals[2], Datum::Bytes(_))
        || vals[0].collation() == Some(tidb_datatype::Collation::Binary)
        || vals[2].collation() == Some(tidb_datatype::Collation::Binary);
    if binary {
        let (Some(s), Some(padstr)) = (coerce_str_bytes(&vals[0])?, coerce_str_bytes(&vals[2])?)
        else {
            return Ok(Datum::Null);
        };
        return Ok(pad_bytes(&s, len as usize, &padstr, left));
    }
    let (s, padstr) = match (coerce_str(&vals[0])?, coerce_str(&vals[2])?) {
        (Some(s), Some(p)) => (s, p),
        (None, _) | (_, None) => return Ok(Datum::Null),
    };
    let chars: Vec<char> = s.chars().collect();
    let len = len as usize;
    if chars.len() >= len {
        return Ok(Datum::new_string(
            chars.into_iter().take(len).collect::<String>(),
        ));
    }
    let pad_chars: Vec<char> = padstr.chars().collect();
    if pad_chars.is_empty() {
        return Ok(Datum::new_string(String::new()));
    }
    let need = len - chars.len();
    let fill: String = pad_chars.iter().cycle().take(need).collect();
    Ok(Datum::new_string(if left {
        format!("{fill}{s}")
    } else {
        format!("{s}{fill}")
    }))
}

/// Binary LPAD/RPAD keeps the source's byte signature instead of decoding it
/// as UTF-8. The Go `builtinLpadSig`/`builtinRpadSig` paths count bytes, return
/// the first `len` bytes on truncation, and preserve arbitrary pad octets.
fn pad_bytes(source: &[u8], len: usize, pad: &[u8], left: bool) -> Datum {
    if source.len() >= len {
        return Datum::new_bytes(source[..len].to_vec());
    }
    if pad.is_empty() {
        return Datum::new_bytes(Vec::new());
    }
    let need = len - source.len();
    let mut fill = Vec::with_capacity(need);
    fill.extend(pad.iter().copied().cycle().take(need));
    if left {
        fill.extend_from_slice(source);
        Datum::new_bytes(fill)
    } else {
        let mut result = source.to_vec();
        result.extend_from_slice(&fill);
        Datum::new_bytes(result)
    }
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
pub(crate) fn hex(vals: &[Datum]) -> Result<Datum, EvalError> {
    match &vals[0] {
        Datum::Null => Ok(Datum::Null),
        Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_) | Datum::Real(_) => {
            let bits = radix_integer_bits(&vals[0])?.expect("non-NULL numeric HEX input");
            Ok(Datum::new_string(format!("{bits:X}")))
        }
        Datum::String(value) => Ok(Datum::new_string(
            value
                .bytes()
                .iter()
                .map(|byte| format!("{byte:02X}"))
                .collect::<String>(),
        )),
        Datum::Bytes(value) => Ok(Datum::new_string(
            value
                .iter()
                .map(|byte| format!("{byte:02X}"))
                .collect::<String>(),
        )),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel HEX argument"))
        }
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
    let Some(s) = coerce_str_bytes(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let mut digits = Vec::with_capacity(s.len() + s.len() % 2);
    if s.len() % 2 != 0 {
        digits.push(b'0');
    }
    digits.extend_from_slice(&s);
    let mut bytes = Vec::with_capacity(digits.len() / 2);
    for pair in digits.chunks_exact(2) {
        let hi = hex_nibble(pair[0]);
        let lo = hex_nibble(pair[1]);
        match (hi, lo) {
            (Some(h), Some(l)) => bytes.push((h << 4) | l),
            _ => return Ok(Datum::Null),
        }
    }
    Ok(Datum::new_bytes(bytes))
}

/// `BIN(n)`: the base-2 form of an implicitly-cast integer as an unsigned
/// 64-bit value.  TiDB's `binFunctionClass` requests `ETInt`, so strings use
/// their leading integer run, decimals round half-up, and reals round
/// ties-to-even before their resulting two's-complement bits are formatted.
pub(crate) fn bin(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad BIN arity"));
    }
    Ok(radix_integer_bits(&vals[0])?
        .map_or(Datum::Null, |bits| Datum::new_string(format!("{bits:b}"))))
}

/// `OCT(n)`: the base-8 form of an integer as an unsigned 64-bit value.
/// Go has two signatures here: the integer path renders the raw `u64` bits,
/// while every non-integer source first goes through `builtinOctStringSig`.
/// That second path is deliberately *not* the `BIN` integer cast: an empty
/// original string is `NULL`, a whitespace-only/invalid nonempty string is
/// zero, and a decimal-prefix overflow stays at `u64::MAX` even if it had a
/// leading minus sign.  Keeping that conversion local makes those cases the
/// normal signature split rather than test-specific exceptions.
pub(crate) fn oct(vals: &[Datum]) -> Result<Datum, EvalError> {
    match &vals[0] {
        Datum::Null => Ok(Datum::Null),
        Datum::Int(value) => Ok(Datum::new_string(format!("{:o}", *value as u64))),
        Datum::UInt(value) => Ok(Datum::new_string(format!("{value:o}"))),
        value => Ok(oct_string_bits(value)?
            .map_or(Datum::Null, |bits| Datum::new_string(format!("{bits:o}")))),
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

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
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
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel integer argument"))
        }
    }
}

/// `builtinCastStringAsIntSig` selects its signed path only when the trimmed
/// input has a `-` followed by at least one byte.  The other path accepts an
/// optional `+`, parses the leading ASCII digits, and preserves all `u64`
/// bits.  Invalid/trailing text only emits a warning in TiDB's test context,
/// so it leaves the valid prefix (or zero) intact.
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

/// `FIELD(needle, a, b, c, ...)`: the 1-based index of the first argument
/// equal to `needle`, or `0` if none match. A `NULL` `needle` never matches
/// (returns `0`). The Go function class chooses ONE signature for the whole
/// argument list before evaluation: all-string arguments use the collator,
/// all-integer arguments use integer equality, and every mixed/decimal/real
/// list uses `EvalReal` for every argument. Selecting that mode once is
/// important: pairwise equality would compare a string/string pair as text
/// even when a numeric argument later forces the source's REAL signature.
pub(crate) fn field(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals[0] == Datum::Null {
        return Ok(Datum::Int(0));
    }
    let mode = if vals
        .iter()
        .all(|value| matches!(value, Datum::Null | Datum::String(_) | Datum::Bytes(_)))
    {
        FieldComparisonMode::String
    } else if vals
        .iter()
        .all(|value| matches!(value, Datum::Null | Datum::Int(_) | Datum::UInt(_)))
    {
        FieldComparisonMode::Integer
    } else {
        FieldComparisonMode::Real
    };
    for (i, v) in vals[1..].iter().enumerate() {
        if *v == Datum::Null {
            continue;
        }
        let equal = match mode {
            FieldComparisonMode::String | FieldComparisonMode::Integer => {
                crate::eval_binary(tidb_ast::BinaryOp::Eq, vals[0].clone(), v.clone())?
                    == Datum::Int(1)
            }
            FieldComparisonMode::Real => {
                let needle = Datum::Real(to_f64_with_mysql_string(&vals[0]));
                let candidate = Datum::Real(to_f64_with_mysql_string(v));
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
/// `NULL` if `n` is out of range or `NULL`. TiDB evaluates `n` through its
/// ETInt conversion, then evaluates the selected argument through ETString.
pub(crate) fn elt(vals: &[Datum]) -> Result<Datum, EvalError> {
    let index = match &vals[0] {
        Datum::Null => return Ok(Datum::Null),
        Datum::Int(n) => *n,
        Datum::UInt(n) => match i64::try_from(*n) {
            Ok(n) => n,
            Err(_) => return Ok(Datum::Null),
        },
        Datum::Decimal(n) => n.round_to_i64_saturating(),
        Datum::Real(n) => round_float_to_i64_saturating(*n),
        Datum::String(n) => n.as_utf8().map(parse_string_i64_saturating).unwrap_or(0),
        Datum::Bytes(n) => std::str::from_utf8(n)
            .map(parse_string_i64_saturating)
            .unwrap_or(0),
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel ELT index"));
        }
    };
    if index < 1 || index as usize >= vals.len() {
        return Ok(Datum::Null);
    }
    Ok(coerce_str(&vals[index as usize])?.map_or(Datum::Null, Datum::new_string))
}

/// `CONCAT_WS(sep, a, b, ...)`: joins the non-`NULL` arguments with `sep`.
/// Unlike `CONCAT`, a `NULL` argument is SKIPPED (not propagated); only a
/// `NULL` separator yields `NULL` (confirmed via `gorun`).
pub(crate) fn concat_ws(vals: &[Datum]) -> Result<Datum, EvalError> {
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
    for value in &vals[1..] {
        let Some(value) = coerce_str_bytes(value)? else {
            continue;
        };
        if have_part {
            out.extend_from_slice(&sep);
        }
        out.extend_from_slice(&value);
        have_part = true;
    }
    Ok(Datum::new_string(out))
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
    let (Some(s), Some(delim)) = (coerce_str_bytes(value)?, coerce_str_bytes(delim_value)?) else {
        return Ok(Datum::Null);
    };
    if count_value == &Datum::Null {
        return Ok(Datum::Null);
    }
    if delim.is_empty() {
        return Ok(string_result(value, Vec::new()));
    }
    // A UInt64 above MaxInt64 is the source's unsigned ETInt overflow case;
    // builtinSubstringIndexSig returns the complete string before applying
    // the negative-count branch.  The ordinary signed path uses TiDB's
    // shared EvalInt coercion for strings, decimals, and reals.
    if matches!(count_value, Datum::UInt(n) if *n > i64::MAX as u64) {
        return Ok(string_result(value, s));
    }
    let count = crate::cast::to_i64_signed(count_value);
    if count == 0 {
        return Ok(string_result(value, Vec::new()));
    }
    let parts = split_bytes(&s, &delim);
    let (start, end) = if count > 0 {
        (0, (count as usize).min(parts.len()))
    } else if count == i64::MIN {
        (0, parts.len())
    } else {
        let n = (-count) as usize;
        (parts.len().saturating_sub(n), parts.len())
    };
    let mut out = Vec::new();
    for (index, part) in parts[start..end].iter().enumerate() {
        if index != 0 {
            out.extend_from_slice(&delim);
        }
        out.extend_from_slice(part);
    }
    Ok(string_result(value, out))
}

fn split_bytes<'a>(value: &'a [u8], delim: &[u8]) -> Vec<&'a [u8]> {
    debug_assert!(!delim.is_empty());
    let mut parts = Vec::new();
    let mut start = 0;
    let mut cursor = 0;
    while cursor + delim.len() <= value.len() {
        if &value[cursor..cursor + delim.len()] == delim {
            parts.push(&value[start..cursor]);
            cursor += delim.len();
            start = cursor;
        } else {
            cursor += 1;
        }
    }
    parts.push(&value[start..]);
    parts
}

/// `INSERT(str, pos, len, newstr)`: replaces the `len` characters of `str`
/// starting at 1-based character `pos` with `newstr`. A `pos` outside
/// `1..=len(str)` leaves `str` unchanged (matching MySQL). `NULL` if any
/// argument is `NULL`.
pub(crate) fn str_insert(vals: &[Datum]) -> Result<Datum, EvalError> {
    let (Some(s), Datum::Int(pos), Datum::Int(len), Some(new)) = (
        coerce_str(&vals[0])?,
        &vals[1],
        &vals[2],
        coerce_str(&vals[3])?,
    ) else {
        return Ok(Datum::Null);
    };
    let chars: Vec<char> = s.chars().collect();
    let n = chars.len();
    if *pos < 1 || *pos as usize > n {
        return Ok(Datum::new_string(s));
    }
    let start = (*pos - 1) as usize;
    let take = (*len).max(0) as usize;
    let end = (start + take).min(n);
    let mut out: String = chars[..start].iter().collect();
    out.push_str(&new);
    out.extend(chars[end..].iter());
    Ok(Datum::new_string(out))
}

/// `MAKE_SET(bits, a, b, c, ...)`: a comma-joined set of the arguments whose
/// 1-based position has the corresponding bit set in `bits` (arg `i` is
/// included when bit `i-1` of `bits` is `1`). `NULL` arguments are excluded
/// even when their bit is set (matching MySQL). `NULL` if `bits` is `NULL`.
pub(crate) fn make_set(vals: &[Datum]) -> Result<Datum, EvalError> {
    let Datum::Int(bits) = &vals[0] else {
        return Ok(Datum::Null);
    };
    let bits = *bits as u64;
    let mut parts = Vec::new();
    for (i, v) in vals[1..].iter().enumerate() {
        if bits & (1 << i) != 0 {
            if let Some(s) = coerce_str(v)? {
                parts.push(s);
            }
        }
    }
    Ok(Datum::new_string(parts.join(",")))
}

/// `TO_BASE64(str)`: standard base-64 encoding (with `=` padding) of the
/// argument's bytes.  `builtinToBase64Sig.evalString` in
/// `pkg/expression/builtin_string.go` inserts a newline after every 76
/// encoded characters; `NULL` propagates.  `max_allowed_packet` overflow is
/// session state and therefore intentionally outside this value-only layer.
pub(crate) fn to_base64(vals: &[Datum]) -> Result<Datum, EvalError> {
    let Some(bytes) = coerce_str_bytes(&vals[0])? else {
        return Ok(Datum::Null);
    };
    const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        let n = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);
        out.push(A[(n >> 18 & 63) as usize] as char);
        out.push(A[(n >> 12 & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            A[(n >> 6 & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            A[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    if out.len() > 76 {
        let mut wrapped = String::with_capacity(out.len() + out.len() / 76);
        for (i, chunk) in out.as_bytes().chunks(76).enumerate() {
            if i > 0 {
                wrapped.push('\n');
            }
            wrapped.push_str(std::str::from_utf8(chunk).unwrap());
        }
        out = wrapped;
    }
    Ok(Datum::new_string(out))
}

/// `FROM_BASE64(str)`: inverse of [`to_base64`], ported from
/// `builtinFromBase64Sig.evalString` in `pkg/expression/builtin_string.go`.
/// TiDB removes spaces/tabs before calling Go's `StdEncoding.DecodeString`,
/// whose decoder also ignores CR/LF.  The result is binary string data, so
/// invalid UTF-8 is preserved rather than replaced or turned into NULL.
pub(crate) fn from_base64(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("FROM_BASE64 arity"));
    }
    let Some(input) = coerce_str_bytes(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let cleaned: Vec<u8> = input
        .into_iter()
        .filter(|byte| !matches!(byte, b' ' | b'\t' | b'\r' | b'\n'))
        .collect();
    if cleaned.is_empty() {
        return Ok(Datum::new_bytes(Vec::new()));
    }
    if !cleaned.len().is_multiple_of(4) {
        return Ok(Datum::Null);
    }
    let val = |c: u8| -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    };
    let mut bytes = Vec::new();
    for (group_index, chunk) in cleaned.chunks_exact(4).enumerate() {
        let last = group_index + 1 == cleaned.len() / 4;
        let Some(a) = val(chunk[0]) else {
            return Ok(Datum::Null);
        };
        let Some(b) = val(chunk[1]) else {
            return Ok(Datum::Null);
        };
        bytes.push((a << 2) | (b >> 4));
        if chunk[2] == b'=' {
            if !last || chunk[3] != b'=' || b & 0x0f != 0 {
                return Ok(Datum::Null);
            }
            continue;
        }
        let Some(c) = val(chunk[2]) else {
            return Ok(Datum::Null);
        };
        bytes.push((b << 4) | (c >> 2));
        if chunk[3] == b'=' {
            if !last || c & 0x03 != 0 {
                return Ok(Datum::Null);
            }
            continue;
        }
        let Some(d) = val(chunk[3]) else {
            return Ok(Datum::Null);
        };
        bytes.push((c << 6) | d);
    }
    Ok(Datum::new_bytes(bytes))
}

#[cfg(test)]
mod from_base64_tests {
    use super::from_base64;
    use crate::Datum;

    fn s(value: &str) -> Datum {
        Datum::new_string(value.to_string())
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
                from_base64(&[s(input)]).unwrap(),
                Datum::new_bytes(expected.to_vec())
            );
        }
        assert_eq!(
            from_base64(&[Datum::new_bytes(b"/wA=".to_vec())]).unwrap(),
            Datum::new_bytes(vec![0xff, 0x00])
        );
        assert_eq!(from_base64(&[Datum::Null]).unwrap(), Datum::Null);
    }

    #[test]
    fn malformed_padding_and_arity_return_null_or_error() {
        for input in ["asc", "YWJj=", "YWI", "YQ===", "YQ=A", "Y!Jj", "YR=="] {
            assert_eq!(from_base64(&[s(input)]).unwrap(), Datum::Null, "{input:?}");
        }
        assert_eq!(
            from_base64(&[s("YQ==")]).unwrap(),
            Datum::new_bytes(b"a".to_vec())
        );
        assert_eq!(
            from_base64(&[s("YWI=")]).unwrap(),
            Datum::new_bytes(b"ab".to_vec())
        );
        assert!(from_base64(&[]).is_err());
        assert!(from_base64(&[s("YWJj"), s("extra")]).is_err());
    }
}

/// `ORD(s)`: the character-code of the leftmost character — for a
/// single-byte (ASCII) char its byte value, for a multibyte char its UTF-8
/// bytes folded as a base-256 number (`ORD('A')` = 65, `ORD('é')` = 50089).
/// `0` for the empty string; `NULL` propagates.
pub(crate) fn ord(vals: &[Datum]) -> Result<Datum, EvalError> {
    let [value] = vals else {
        return Err(EvalError::Unsupported("bad ORD arity"));
    };
    let Some(bytes) = coerce_str_bytes(value)? else {
        return Ok(Datum::Null);
    };
    let Some(&first_byte) = bytes.first() else {
        return Ok(Datum::Int(0));
    };
    // Binary signatures use the first raw byte. Text signatures fold the
    // complete first UTF-8 character, exactly as Go's charset transform does;
    // malformed text falls back to that first byte rather than being decoded
    // with replacement.
    let first = if matches!(value, Datum::Bytes(_))
        || value.collation() == Some(tidb_datatype::Collation::Binary)
    {
        vec![first_byte]
    } else if let Ok(text) = std::str::from_utf8(&bytes) {
        let mut buf = [0_u8; 4];
        text.chars()
            .next()
            .expect("non-empty UTF-8 text has a first character")
            .encode_utf8(&mut buf)
            .as_bytes()
            .to_vec()
    } else {
        vec![first_byte]
    };
    let n = first
        .iter()
        .fold(0_i64, |acc, &byte| acc * 256 + i64::from(byte));
    Ok(Datum::Int(n))
}

/// `QUOTE(s)`: `s` wrapped in single quotes with `'`, `\`, NUL and Ctrl-Z
/// backslash-escaped — a value safe to paste into SQL. A `NULL` argument
/// yields the four-character string `NULL` (NOT SQL `NULL`), matching MySQL.
pub(crate) fn quote(vals: &[Datum]) -> Result<Datum, EvalError> {
    let Some(s) = coerce_str(&vals[0])? else {
        return Ok(Datum::new_string("NULL".to_string()));
    };
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\'' => out.push_str("\\'"),
            '\\' => out.push_str("\\\\"),
            '\0' => out.push_str("\\0"),
            '\x1a' => out.push_str("\\Z"),
            _ => out.push(c),
        }
    }
    out.push('\'');
    Ok(Datum::new_string(out))
}

/// `BIT_COUNT(n)`: the number of set bits in `n` (as an unsigned 64-bit
/// value); `NULL` propagates. This follows the function's ETInt input
/// signature: decimal/real/string values first take their MySQL integer
/// coercion, whose statement warnings are outside this value-only domain.
pub(crate) fn bit_count(vals: &[Datum]) -> Result<Datum, EvalError> {
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
pub(crate) fn format_num(vals: &[Datum]) -> Result<Datum, EvalError> {
    format_num_locale(vals, "en_US")
}

/// Shared `FORMAT(x, d[, locale])` result formatter, ported from
/// `evalNumDecArgsForFormat`, `roundFormatArgs`, and `FormatByLocale` in
/// `pkg/expression/builtin_string.go` and `pkg/parser/mysql/locale_format.go`.
/// The locale warning side channel is intentionally absent: this evaluator has
/// no session statement context, but unknown/`NULL` locales use TiDB's
/// observable `en_US` fallback exactly.
pub(crate) fn format_num_locale(vals: &[Datum], locale: &str) -> Result<Datum, EvalError> {
    let [number, precision, ..] = vals else {
        return Err(EvalError::Unsupported("bad FORMAT arguments"));
    };
    let Some(number) = format_number_text(number)? else {
        return Ok(Datum::Null);
    };
    let Some(precision) = format_precision(precision)? else {
        return Ok(Datum::Null);
    };
    let precision = precision.clamp(0, 30) as usize;
    let rounded = round_format_args(&number, precision);
    let (negative, number) = rounded
        .strip_prefix('-')
        .map_or((false, rounded.as_str()), |n| (true, n));
    let (integer, fraction) = number.split_once('.').unwrap_or((number, ""));
    let (thousands, decimal, indian) = format_style(locale);
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    out.push_str(&group_integer(integer, thousands, indian));
    if precision > 0 {
        out.push_str(decimal);
        out.push_str(&fraction[..fraction.len().min(precision)]);
        out.push_str(&"0".repeat(precision.saturating_sub(fraction.len())));
    }
    Ok(Datum::new_string(out))
}

fn format_number_text(value: &Datum) -> Result<Option<String>, EvalError> {
    Ok(match value {
        Datum::Null => None,
        Datum::Int(n) => Some(n.to_string()),
        Datum::UInt(n) => Some(n.to_string()),
        Datum::Decimal(n) => Some(n.to_string()),
        Datum::Real(n) => Some(n.to_string()),
        // `FORMAT` requests ETReal for string inputs.  `to_f64_with_mysql_string`
        // is this crate's port of that numeric-prefix conversion; its warning is
        // outside this value-only domain.
        Datum::String(_) | Datum::Bytes(_) => {
            Some(crate::ops::to_f64_with_mysql_string(value).to_string())
        }
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel FORMAT argument"));
        }
    })
}

fn format_precision(value: &Datum) -> Result<Option<i64>, EvalError> {
    Ok(match value {
        Datum::Null => None,
        Datum::Int(n) => Some(*n),
        Datum::UInt(n) => Some(*n as i64),
        Datum::Decimal(n) => Some(n.round_to_i64_saturating()),
        Datum::Real(n) => Some(round_float_to_i64_saturating(*n)),
        Datum::String(s) => Some(s.as_utf8().map(parse_string_i64_saturating).unwrap_or(0)),
        Datum::Bytes(s) => Some(
            std::str::from_utf8(s)
                .map(parse_string_i64_saturating)
                .unwrap_or(0),
        ),
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel FORMAT precision"));
        }
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

fn parse_string_i64_saturating(value: &str) -> i64 {
    let value = value.trim_start();
    let (negative, digits) = match value.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => match value.strip_prefix('+') {
            Some(rest) => (false, rest),
            None => (false, value),
        },
    };
    let digits = &digits[..digits.bytes().take_while(u8::is_ascii_digit).count()];
    if digits.is_empty() {
        return 0;
    }
    let Ok(magnitude) = digits.parse::<u64>() else {
        return if negative { i64::MIN } else { i64::MAX };
    };
    if negative {
        if magnitude > i64::MAX as u64 {
            i64::MIN
        } else {
            -(magnitude as i64)
        }
    } else if magnitude > i64::MAX as u64 {
        i64::MAX
    } else {
        magnitude as i64
    }
}

fn round_format_args(number: &str, precision: usize) -> String {
    let (negative, number) = number
        .strip_prefix('-')
        .map_or((false, number), |n| (true, n));
    let (mut integer, fraction) = number.split_once('.').unwrap_or((number, ""));
    if !integer.bytes().all(|digit| digit.is_ascii_digit())
        || !fraction.bytes().all(|digit| digit.is_ascii_digit())
    {
        integer = "0";
    }
    let mut fraction: Vec<u8> = fraction.bytes().take(precision).collect();
    while fraction.len() < precision {
        fraction.push(b'0');
    }
    let round_up = number
        .split_once('.')
        .and_then(|(_, f)| f.as_bytes().get(precision))
        .is_some_and(|d| *d >= b'5');
    if round_up {
        let mut carry = true;
        for digit in fraction.iter_mut().rev() {
            if *digit == b'9' {
                *digit = b'0';
            } else {
                *digit += 1;
                carry = false;
                break;
            }
        }
        if carry {
            let mut digits = integer.as_bytes().to_vec();
            for digit in digits.iter_mut().rev() {
                if *digit == b'9' {
                    *digit = b'0';
                } else {
                    *digit += 1;
                    carry = false;
                    break;
                }
            }
            if carry {
                return format_number_parts(
                    negative,
                    format!("1{}", "0".repeat(integer.len())),
                    fraction,
                );
            }
            return format_number_parts(negative, String::from_utf8(digits).unwrap(), fraction);
        }
    }
    format_number_parts(negative, integer.to_string(), fraction)
}

fn format_number_parts(negative: bool, integer: String, fraction: Vec<u8>) -> String {
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    out.push_str(&integer);
    if !fraction.is_empty() {
        out.push('.');
        out.push_str(std::str::from_utf8(&fraction).unwrap());
    }
    out
}

fn group_integer(integer: &str, separator: &str, indian: bool) -> String {
    if separator.is_empty() || integer.len() <= 3 {
        return integer.to_string();
    }
    let mut widths = vec![3usize];
    if indian {
        widths.extend(std::iter::repeat_n(2, integer.len()));
    } else {
        widths.extend(std::iter::repeat_n(3, integer.len()));
    }
    let mut groups = Vec::new();
    let mut end = integer.len();
    for width in widths {
        if end == 0 {
            break;
        }
        let start = end.saturating_sub(width);
        groups.push(&integer[start..end]);
        end = start;
    }
    groups.reverse();
    groups.join(separator)
}

fn format_style(locale: &str) -> (&'static str, &'static str, bool) {
    let locale = locale.to_ascii_lowercase();
    let dot_comma = "be_by da_dk de_be de_de de_lu es_ar es_bo es_cl es_co es_ec es_es es_py es_uy es_ve fo_fo hu_hu id_id is_is lt_lt mn_mn ro_ro ru_ua sq_al tr_tr vi_vn nb_no uk_ua no_no";
    let space_comma = "cs_cz es_cr et_ee fi_fi lv_lv mk_mk ru_ru sk_sk sv_fi sv_se";
    let none_comma = "el_gr gl_es pt_pt sl_si ca_es de_at eu_es fr_be hr_hr it_it nl_be nl_nl pt_br fr_ca fr_fr fr_lu pl_pl fr_ch bg_bg";
    if dot_comma.split_whitespace().any(|name| name == locale) {
        (".", ",", false)
    } else if space_comma.split_whitespace().any(|name| name == locale) {
        (" ", ",", false)
    } else if none_comma.split_whitespace().any(|name| name == locale) {
        ("", ",", false)
    } else if locale == "de_ch" {
        ("'", ".", false)
    } else if locale == "it_ch" {
        ("'", ",", false)
    } else if matches!(locale.as_str(), "ar_sa" | "sr_rs") {
        ("", ".", false)
    } else if matches!(locale.as_str(), "en_in" | "ta_in" | "te_in") {
        (",", ".", true)
    } else {
        // `GetLocaleFormatStyle` returns `styleCommaDot` for both the large
        // explicit CommaDot set and unknown locales (with a warning in Go).
        (",", ".", false)
    }
}

/// `CHAR(n1, n2, ...)` (parser-renamed `CHAR_FUNC`) ported from
/// `builtinCharSig.convertToBytes` in `pkg/expression/builtin_string.go`.
/// No-`USING` CHAR returns `Datum::Bytes` exactly as TiDB's binary signature
/// does, including invalid UTF-8 and embedded NUL. A charset
/// conversion (`CHAR(... USING charset)`) needs TiDB's charset/session error
/// policy and is deliberately unsupported rather than lossy-decoded.
pub(crate) fn char_func(vals: &[Datum]) -> Result<Datum, EvalError> {
    // The last argument is the charset sentinel appended by the parser.
    let Some((charset, nums)) = vals.split_last() else {
        return Err(EvalError::Unsupported("CHAR requires arguments"));
    };
    if *charset != Datum::Null {
        return Err(EvalError::Unsupported("CHAR ... USING charset"));
    }
    let mut bytes = Vec::new();
    for v in nums {
        match v {
            Datum::Null => {} // skipped, matching TiDB's EvalInt NULL path
            _ => append_char_integer(&mut bytes, crate::cast::to_i64_signed(v)),
        }
    }
    Ok(Datum::new_bytes(bytes))
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
    if remstr.is_empty() {
        return if binary {
            Datum::new_bytes(str)
        } else {
            Datum::new_string(str)
        };
    }
    use tidb_ast::TrimDirection::*;
    if matches!(direction, Leading | Both) {
        while str.starts_with(&remstr) {
            str.drain(..remstr.len());
        }
    }
    if matches!(direction, Trailing | Both) {
        while str.ends_with(&remstr) {
            let new_len = str.len() - remstr.len();
            str.truncate(new_len);
        }
    }
    if binary {
        Datum::new_bytes(str)
    } else {
        Datum::new_string(str)
    }
}

#[cfg(test)]
mod space_tests {
    use super::{space, space_with_max_allowed_packet};
    use crate::{Datum, Decimal, EvalError};

    /// Complete scalar table from `TestSpace` in
    /// `pkg/expression/builtin_string_test.go`.  The Go test's injected
    /// `errors.New` input is a test harness error path, not a SQL value.
    #[test]
    fn space_matches_go_source_scalar_vectors() {
        let cases = [
            (Datum::Int(0), Datum::new_string(String::new())),
            (Datum::Int(3), Datum::new_string("   ".to_string())),
            (Datum::Int(16_777_217), Datum::Null),
            (Datum::Int(-1), Datum::new_string(String::new())),
            (
                Datum::new_string("abc".to_string()),
                Datum::new_string(String::new()),
            ),
            (
                Datum::new_string("3".to_string()),
                Datum::new_string("   ".to_string()),
            ),
            (Datum::Real(1.2), Datum::new_string(" ".to_string())),
            (Datum::Real(1.9), Datum::new_string("  ".to_string())),
            (Datum::Null, Datum::Null),
        ];
        for (input, want) in cases {
            assert_eq!(space(&[input]), Ok(want));
        }

        // EvalInt's decimal and FLOAT tie rules are intentionally different
        // in TiDB; retain that distinction at this function boundary.
        assert_eq!(
            space(&[Datum::Decimal(Decimal::from_literal("2.5"))]),
            Ok(Datum::new_string("   ".to_string()))
        );
        assert_eq!(
            space(&[Datum::Real(2.5)]),
            Ok(Datum::new_string("  ".to_string()))
        );
        assert_eq!(space(&[]), Err(EvalError::Unsupported("bad SPACE arity")));

        // `TestSpaceSig` constructs the signature with a 1000-byte packet
        // limit: six spaces succeed while 1001 spaces become NULL (the Go
        // side also emits a warning, which this value-only API deliberately
        // does not model).
        assert_eq!(
            space_with_max_allowed_packet(&[Datum::Int(6)], 1_000),
            Ok(Datum::new_string("      ".to_string()))
        );
        assert_eq!(
            space_with_max_allowed_packet(&[Datum::Int(1_001)], 1_000),
            Ok(Datum::Null)
        );
    }
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
            (Datum::new_string("9223372036854775808"), 1),
            (Datum::new_string("18446744073709551616"), 64),
            (Datum::new_string("xxx".to_string()), 0),
            (Datum::Decimal(Decimal::from_literal("3.1")), 2),
        ];
        for (input, want) in cases {
            assert_eq!(bit_count(&[input]), Ok(Datum::Int(want)));
        }
        assert_eq!(bit_count(&[Datum::Null]), Ok(Datum::Null));
        // Go's string conversion is byte-prefix based, so malformed UTF-8
        // after an ASCII number must not erase the numeric prefix.
        assert_eq!(
            bit_count(&[Datum::new_bytes(vec![b'1', 0xff])]),
            Ok(Datum::Int(1))
        );
    }
}

#[cfg(test)]
mod concat_source_tests {
    use super::{concat, concat_ws};
    use crate::{Datum, Decimal, EvalError};

    fn string(value: &str) -> Datum {
        Datum::new_string(value.to_string())
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
            concat(&[Datum::new_bytes(vec![0xff]), string("a")]),
            Ok(Datum::new_string(vec![0xff, b'a']))
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
                Datum::new_bytes(vec![b'|']),
                Datum::new_bytes(vec![0xff]),
                Datum::Null,
                string("b")
            ]),
            Ok(Datum::new_string(vec![0xff, b'|', b'b']))
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
mod to_base64_tests {
    use super::to_base64;
    use crate::Datum;

    /// Length and newline count of `TO_BASE64` over `byte_count` `'a'` bytes.
    fn shape(byte_count: usize) -> (usize, usize) {
        match to_base64(&[Datum::new_bytes(vec![b'a'; byte_count])]).unwrap() {
            Datum::String(text) => {
                let bytes = text.bytes();
                (bytes.len(), bytes.iter().filter(|&&b| b == b'\n').count())
            }
            other => panic!("expected a string, got {other:?}"),
        }
    }

    /// Go `builtinToBase64Sig` joins the encoded output into 76-char lines with
    /// `\n` (`splitToSubN` + `strings.Join`) ONLY when the length EXCEEDS 76, so
    /// exactly 76 gets no newline and there is never a trailing newline.
    /// goeval-verified `LENGTH(TO_BASE64(REPEAT('a', n)))`: 57 -> 76, 58 -> 81,
    /// 114 -> 153.
    #[test]
    fn to_base64_wraps_at_76_chars_like_go() {
        // 57 bytes -> exactly 76 base64 chars: no wrap, no newline.
        assert_eq!(shape(57), (76, 0));
        // 58 bytes -> 80 base64 chars -> "76\n4": one newline.
        assert_eq!(shape(58), (81, 1));
        // 114 bytes -> 152 base64 chars -> "76\n76": one newline, no trailing.
        assert_eq!(shape(114), (153, 1));
    }

    #[test]
    fn to_base64_null_is_null() {
        assert_eq!(to_base64(&[Datum::Null]).unwrap(), Datum::Null);
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
        match format_num(&[n, Datum::Int(precision)]).unwrap() {
            Datum::String(s) => String::from_utf8(s.bytes().to_vec()).unwrap(),
            other => panic!("expected a string, got {other:?}"),
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
