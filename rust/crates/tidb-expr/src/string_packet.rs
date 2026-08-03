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

//! The string builtins whose RESULT SIZE is checked against the session's
//! `max_allowed_packet` before the result is built.
//!
//! In Go these are exactly the signatures that capture
//! `ctx.GetEvalCtx().GetMaxAllowedPacket()` into a struct field while BUILDING
//! (`builtinSpaceSig`, `builtinRepeatSig`, `builtinLpadSig`/`builtinRpadSig`
//! and their UTF-8 twins, `builtinToBase64Sig`, `builtinWeightStringSig`),
//! and that answer NULL with warning 1301 rather than allocating. What they
//! share is that limit rather than any string semantics, which is why they sit
//! together here and not in `crate::string_fn`.
//!
//! [`crate::Columns::max_allowed_packet`] and
//! [`crate::Columns::handle_allowed_packet_overflowed`] are the one seam all
//! of them read.

use crate::coerce::{coerce_str, coerce_str_bytes};
use crate::{Datum, EvalError};

/// `REPEAT(str, count)`: `str` concatenated `count` times (empty for
/// `count <= 0`); `NULL` if either argument is `NULL`.
///
/// TiDB selects an `ETString, ETInt` signature, so the count follows the
/// same signed-integer coercion boundary as `EvalInt` (including string
/// numeric prefixes, decimal rounding, and float ties-to-even).  Go strings
/// are byte sequences rather than guaranteed UTF-8; retaining the evaluated
/// string bytes keeps binary input lossless here.  `builtinRepeatSig` checks
/// `byteLength*num` against the session `max_allowed_packet` and answers NULL
/// with warning 1301; the multiplication overflow arm takes the same exit,
/// since a product too large for `usize` is by construction over any limit.
pub(crate) fn repeat(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
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
    let Some(output_len) = value.len().checked_mul(count) else {
        ctx.handle_allowed_packet_overflowed("repeat");
        return Ok(Datum::Null);
    };
    if output_len as u64 > ctx.max_allowed_packet() {
        ctx.handle_allowed_packet_overflowed("repeat");
        return Ok(Datum::Null);
    }
    let mut output = Vec::with_capacity(output_len);
    for _ in 0..count {
        output.extend_from_slice(&value);
    }
    Ok(Datum::new_string(output))
}
/// `SPACE(n)`: a string of `n` spaces (empty for `n <= 0`); `NULL` if the
/// argument is `NULL`.  This is the `ETInt` signature from
/// `builtinSpaceSig.evalString` in `pkg/expression/builtin_string.go`, not
/// an integer-literal-only convenience: decimal arguments round away from
/// zero while float arguments round ties to even through the shared `EvalInt`
/// conversion.  TiDB returns `NULL` rather than allocating above
/// `mysql.MaxBlobWidth`, and warns 1301 above `max_allowed_packet`.
pub(crate) fn space(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    let [value] = vals else {
        return Err(EvalError::Unsupported("bad SPACE arity"));
    };
    if *value == Datum::Null {
        return Ok(Datum::Null);
    }
    let width = crate::cast::to_i64_signed(value);
    let width = width.max(0);
    // `builtinSpaceSig.evalString` checks the session packet limit BEFORE the
    // `mysql.MaxBlobWidth` result limit, and only the first of the two warns.
    if (width as u64) > ctx.max_allowed_packet() {
        ctx.handle_allowed_packet_overflowed("space");
        return Ok(Datum::Null);
    }
    const MAX_BLOB_WIDTH: i64 = 16_777_216; // pkg/parser/mysql.MaxBlobWidth
    if width > MAX_BLOB_WIDTH {
        return Ok(Datum::Null);
    }
    Ok(Datum::new_string(" ".repeat(width as usize)))
}
/// `LPAD(str, len, pad)` / `RPAD(str, len, pad)`: pad (or truncate) `str` to
/// `len` characters using `pad` on the left/right. Ported from
/// `builtinLpadUTF8Sig`/`builtinRpadUTF8Sig` in `pkg/expression/
/// builtin_string.go` (rune-based, the default for non-binary strings): a
/// NEGATIVE `len` yields `NULL` (not the empty string); `len == 0` yields
/// the empty string; truncation keeps the first `len` chars; an empty `pad`
/// that can't reach `len` yields the empty string. `NULL` if any argument is
/// `NULL` or `len` exceeds TiDB's `mysql.MaxBlobWidth`. A `len` whose result
/// could not fit `max_allowed_packet` is NULL with warning 1301.
pub(crate) fn pad(
    vals: &[Datum],
    left: bool,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    const MAX_BLOB_WIDTH: i64 = 16_777_216;
    if vals.len() != 3 {
        return Err(EvalError::Unsupported("bad LPAD/RPAD arguments"));
    }
    let len = match &vals[1] {
        Datum::Null => return Ok(Datum::Null),
        value => crate::cast::to_i64_signed(value),
    };
    // `lpadFunctionClass.getFunction` tests BOTH string arguments, so a binary
    // pad string makes the whole call byte-based.
    let binary = crate::string_signature::is_binary_str(&vals[0])
        || crate::string_signature::is_binary_str(&vals[2]);
    // The packet check comes FIRST in both signatures, before the negative /
    // MaxBlobWidth rejections, and is the only one of the three that warns.
    // The byte signature compares the target length itself; the rune one
    // compares it times `mysql.MaxBytesOfCharacter`, because that many bytes
    // is the widest the result can become.
    let requested = if binary {
        u64::try_from(len).unwrap_or(u64::MAX)
    } else {
        u64::try_from(len)
            .unwrap_or(u64::MAX)
            .saturating_mul(MAX_BYTES_OF_CHARACTER)
    };
    if requested > ctx.max_allowed_packet() {
        ctx.handle_allowed_packet_overflowed(if left { "lpad" } else { "rpad" });
        return Ok(Datum::Null);
    }
    if !(0..=MAX_BLOB_WIDTH).contains(&len) {
        return Ok(Datum::Null);
    }
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
/// `WEIGHT_STRING(str [AS {CHAR|BINARY}(n)])`, ported from
/// `builtinWeightStringSig.evalString` in `pkg/expression/builtin_string.go`:
/// the collation SORT KEY of `str`, which is what `ORDER BY` actually
/// compares, surfaced to SQL.
///
/// `padding` is the `AS` clause: `Some((binary, n))`. The two paddings are
/// genuinely different operations, not one with a flag:
///
/// - `AS CHAR(n)` counts RUNES, pads with SPACES, and keys under the
///   ARGUMENT's own collation (`b.args[0].GetType(ctx).GetCollate()`, not the
///   function's -- the function's is forced to `binary`).
/// - `AS BINARY(n)` counts BYTES, pads with NUL, keys under `binary`, and
///   WARNS 1292 when it truncates.
///
/// CAPTURED from TiDB (`HEX` of each):
///
/// ```text
/// weight_string('a')                                -> 61
/// weight_string('A' collate utf8mb4_general_ci)     -> 0041
/// weight_string('ab' as char(1))                    -> 61
/// weight_string('ab' as char(4))                    -> 6162
/// weight_string('ab' as binary(4))                  -> 61620000
/// weight_string('ab' as binary(1))                  -> 61  + warning 1292
/// weight_string('中')                                -> E4B8AD
/// ```
///
/// `weight_string('ab' AS CHAR(4))` is `6162`, NOT `61622020`: the padding
/// spaces go in before the key, and `utf8mb4_bin` is PAD SPACE, so
/// `Collator::key` trims them right back off. That is the whole reason the
/// padding cannot be applied after keying.
///
/// A NUMERIC argument is `builtinWeightStringNullSig` -- always NULL -- and
/// is decided from the argument's FieldType by the CALLER, since Go decides it
/// while BUILDING the function.
pub(crate) fn weight_string(
    value: &Datum,
    padding: Option<(bool, i64)>,
    collation: tidb_datatype::Collation,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    let Some(bytes) = coerce_str_bytes(value)? else {
        return Ok(Datum::Null);
    };
    let (bytes, collation) = match padding {
        None => (bytes, collation),
        Some((false, length)) => {
            let length = usize::try_from(length).unwrap_or(0);
            let runes: Vec<char> = String::from_utf8_lossy(&bytes).chars().collect();
            if length < runes.len() {
                (
                    runes[..length].iter().collect::<String>().into_bytes(),
                    collation,
                )
            } else {
                if (length - runes.len()) as u64 > ctx.max_allowed_packet() {
                    ctx.handle_allowed_packet_overflowed("weight_string");
                    return Ok(Datum::Null);
                }
                let mut padded = bytes;
                padded.extend(std::iter::repeat_n(b' ', length - runes.len()));
                (padded, collation)
            }
        }
        Some((true, length)) => {
            let length = usize::try_from(length).unwrap_or(0);
            if length < bytes.len() {
                ctx.append_warning(
                    1292,
                    &format!(
                        "Truncated incorrect BINARY({length}) value: '{}'",
                        String::from_utf8_lossy(&bytes)
                    ),
                );
                (bytes[..length].to_vec(), tidb_datatype::Collation::Binary)
            } else {
                if (length - bytes.len()) as u64 > ctx.max_allowed_packet() {
                    // Go names this one `cast_as_binary`, not `weight_string`.
                    ctx.handle_allowed_packet_overflowed("cast_as_binary");
                    return Ok(Datum::Null);
                }
                let mut padded = bytes;
                padded.resize(length, 0);
                (padded, tidb_datatype::Collation::Binary)
            }
        }
    };
    Ok(Datum::new_bytes(
        tidb_datatype::get_collator(collation.name()).key(&bytes),
    ))
}
/// Go `mysql.MaxBytesOfCharacter`: the widest a single character can encode
/// to, which `builtinLpadUTF8Sig`/`builtinRpadUTF8Sig` multiply the requested
/// character count by before testing `max_allowed_packet`.
const MAX_BYTES_OF_CHARACTER: u64 = 4;
/// Go `base64NeededEncodedLength`: the encoded width of `n` input bytes,
/// including the newline every 76 output characters. `None` is Go's `-1`,
/// the input width past which the answer would overflow a signed `int` --
/// which `builtinToBase64Sig` answers NULL for WITHOUT a packet warning,
/// since it never got as far as comparing a length.
fn base64_needed_encoded_length(n: usize) -> Option<u64> {
    // Go's 64-bit arm; the 32-bit constant is for a platform this crate does
    // not build for, and `usize` here is the same width Go's `int` is there.
    if n > 6_827_690_988_321_067_803 {
        return None;
    }
    let length = (n as u64).div_ceil(3) * 4;
    // Go computes `(length-1)/76` in signed `int`, where the empty input's
    // `-1/76` truncates toward zero rather than wrapping.
    Some(length + length.saturating_sub(1) / 76)
}
/// `TO_BASE64(str)`: standard base-64 encoding (with `=` padding) of the
/// argument's bytes.  `builtinToBase64Sig.evalString` in
/// `pkg/expression/builtin_string.go` inserts a newline after every 76
/// encoded characters; `NULL` propagates.  A result over `max_allowed_packet`
/// is NULL with warning 1301.
pub(crate) fn to_base64(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    let Some(bytes) = coerce_str_bytes(&vals[0])? else {
        return Ok(Datum::Null);
    };
    // Go `base64NeededEncodedLength` then the packet check, both BEFORE any
    // encoding happens -- the point is not to allocate the oversized result.
    let Some(needed) = base64_needed_encoded_length(bytes.len()) else {
        return Ok(Datum::Null);
    };
    if needed > ctx.max_allowed_packet() {
        ctx.handle_allowed_packet_overflowed("to_base64");
        return Ok(Datum::Null);
    }
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
#[cfg(test)]
mod space_tests {
    use super::{pad, repeat, space, to_base64};
    use crate::{Columns, Datum, Decimal, EvalError, NoColumns};

    /// A session whose `max_allowed_packet` is `limit` and that records the
    /// warnings the builtins raise. The warning is half of what is ported
    /// here: the oversized answer was ALREADY NULL, so a version that only
    /// returned NULL would pass every value assertion and still leave the
    /// client unable to tell a truncated result from a genuine one.
    #[derive(Default)]
    struct Packet {
        limit: u64,
        warnings: std::cell::RefCell<Vec<String>>,
    }

    impl Packet {
        fn new(limit: u64) -> Self {
            Self {
                limit,
                warnings: std::cell::RefCell::default(),
            }
        }
        fn warnings(&self) -> Vec<String> {
            self.warnings.borrow().clone()
        }
    }

    impl Columns for Packet {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn max_allowed_packet(&self) -> u64 {
            self.limit
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push(format!("{code} {message}"));
        }
    }

    /// The whole `max_allowed_packet` family, each against the packet limit
    /// its Go source test uses.
    ///
    /// `TestSpaceSig` and `TestRepeatSig` both build their signature with a
    /// 1000-byte limit; `LPAD`/`RPAD`/`TO_BASE64` are the three that had NO
    /// packet check at all here. The message is Go's
    /// `ErrWarnAllowedPacketOverflowed` verbatim -- CAPTURED from TiDB at the
    /// default limit:
    ///
    /// ```text
    /// select space(70000000);  show warnings;
    ///   Warning 1301 Result of space() was larger than
    ///                max_allowed_packet (67108864) - truncated
    /// ```
    #[test]
    fn max_allowed_packet_overflow_is_null_with_warning_1301() {
        let over = |name: &str, result: Datum, ctx: &Packet| {
            assert_eq!(result, Datum::Null, "{name} over the packet limit is NULL");
            assert_eq!(
                ctx.warnings(),
                vec![format!(
                    "1301 Result of {name}() was larger than max_allowed_packet ({}) - truncated",
                    ctx.limit
                )],
                "{name} must warn exactly once"
            );
        };

        let ctx = Packet::new(1_000);
        assert_eq!(
            space(&[Datum::Int(6)], &ctx),
            Ok(Datum::new_string("      ".to_string()))
        );
        assert!(
            ctx.warnings().is_empty(),
            "an in-budget result must not warn"
        );
        let ctx = Packet::new(1_000);
        over("space", space(&[Datum::Int(1_001)], &ctx).unwrap(), &ctx);

        let ctx = Packet::new(1_000);
        let repeated = repeat(
            &[Datum::new_string("a".to_string()), Datum::Int(1_001)],
            &ctx,
        );
        over("repeat", repeated.unwrap(), &ctx);

        // The rune signature multiplies the requested CHARACTER count by
        // `mysql.MaxBytesOfCharacter` (4) before the comparison, so 251
        // characters already exceed a 1000-byte packet while 250 do not.
        let lpad_args = |len: i64| {
            [
                Datum::new_string("a".to_string()),
                Datum::Int(len),
                Datum::new_string("x".to_string()),
            ]
        };
        let ctx = Packet::new(1_000);
        assert!(pad(&lpad_args(250), true, &ctx).unwrap() != Datum::Null);
        assert!(ctx.warnings().is_empty());
        let ctx = Packet::new(1_000);
        over("lpad", pad(&lpad_args(251), true, &ctx).unwrap(), &ctx);
        let ctx = Packet::new(1_000);
        over("rpad", pad(&lpad_args(251), false, &ctx).unwrap(), &ctx);

        // `base64NeededEncodedLength` is the 4/3 expansion plus one newline
        // per 76 output characters: 741 input bytes need exactly 1000 and fit,
        // 742 need 1005 and do not.
        let ctx = Packet::new(1_000);
        assert!(to_base64(&[Datum::new_string("a".repeat(741))], &ctx).unwrap() != Datum::Null);
        assert!(ctx.warnings().is_empty());
        let ctx = Packet::new(1_000);
        over(
            "to_base64",
            to_base64(&[Datum::new_string("a".repeat(742))], &ctx).unwrap(),
            &ctx,
        );
    }

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
            assert_eq!(space(&[input], &NoColumns), Ok(want));
        }

        // EvalInt's decimal and FLOAT tie rules are intentionally different
        // in TiDB; retain that distinction at this function boundary.
        assert_eq!(
            space(&[Datum::Decimal(Decimal::from_literal("2.5"))], &NoColumns),
            Ok(Datum::new_string("   ".to_string()))
        );
        assert_eq!(
            space(&[Datum::Real(2.5)], &NoColumns),
            Ok(Datum::new_string("  ".to_string()))
        );
        assert_eq!(
            space(&[], &NoColumns),
            Err(EvalError::Unsupported("bad SPACE arity"))
        );
    }
}
#[cfg(test)]
mod to_base64_tests {
    use super::to_base64;
    use crate::{Columns, Datum, NoColumns};

    /// Length and newline count of `TO_BASE64` over `byte_count` `'a'` bytes.
    fn shape(byte_count: usize) -> (usize, usize) {
        match to_base64(&[Datum::new_bytes(vec![b'a'; byte_count])], &NoColumns).unwrap() {
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
        assert_eq!(to_base64(&[Datum::Null], &NoColumns).unwrap(), Datum::Null);
    }

    /// The `maxAllowPacket` rows of `TestToBase64Sig`
    /// (`pkg/expression/builtin_string_test.go:2649`) -- the only half of that
    /// test not already covered: its four value rows are asserted byte for byte
    /// by `builtin_ext::string2::tests::to_base64_matches_go_source_vectors`,
    /// including the 76-column wrap of the 64-char alphabet and its triple.
    ///
    /// Here: when the encoded result would exceed `max_allowed_packet`, Go
    /// returns NULL and warns `errWarnAllowedPacketOverflowed`. This layer
    /// used to have no `max_allowed_packet` seam at all and encoded anyway,
    /// so the whole row set was `#[ignore]`d.
    #[test]
    fn to_base64_source_max_allowed_packet_rows() {
        const ALPHABET: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        struct Limit(u64);
        impl Columns for Limit {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn max_allowed_packet(&self) -> u64 {
                self.0
            }
        }
        for (input, max_allowed_packet) in [
            ("abc".to_owned(), 3u64),
            (ALPHABET.to_owned(), 88),
            (ALPHABET.repeat(3), 258),
        ] {
            assert_eq!(
                to_base64(
                    &[Datum::new_string(input.clone())],
                    &Limit(max_allowed_packet)
                )
                .unwrap(),
                Datum::Null,
                "{input:?} over its max_allowed_packet must be NULL"
            );
        }
    }
}
