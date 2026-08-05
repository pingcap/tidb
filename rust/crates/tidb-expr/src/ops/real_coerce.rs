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

//! The `ETReal` half of operator evaluation, split out of `ops.rs`.
//!
//! Everything an operand does once MySQL's promotion hierarchy has decided the
//! pair is a FLOAT pair: the `f64` arithmetic and comparison
//! ([`float_binary`], [`real_compare`]), the coercions that get a `Datum`
//! there ([`to_f64`], [`to_f64_with_mysql_string`], `bytes_to_f64` -- Go's
//! `types.StrToFloat` numeric-prefix scan and the `1292 Truncated incorrect
//! DOUBLE value` it raises), and the bounded `f64` -> integer conversions the
//! bitwise operators need.
//!
//! It is one module because these are the pieces a reader has to hold together
//! to answer "what does this pair of operands become", and keeping them beside
//! the promotion hierarchy in `ops.rs` was what pushed that file past the
//! source-size ratchet.

use super::*;

/// Float (`FLOAT`/`DOUBLE`) arithmetic and comparison: an `Int` or
/// `Decimal` operand promotes to `f64` (MySQL's implicit rule), using
/// NATIVE `f64` arithmetic throughout — unlike `Decimal`, `Float` needs no
/// custom digit-string math, since Rust's `f64` already implements the
/// same IEEE-754 semantics Go's does (confirmed via direct comparison of
/// `strconv.FormatFloat(f,'f',-1,64)` against Rust's own `f64` Display
/// across a wide value range, including subnormals and `f64::MAX` —
/// byte-identical in every case tried). A result that overflows to
/// `+/-infinity` is a genuine MySQL evaluation ERROR (confirmed via
/// `goeval`, not silently allowed as IEEE-754 would); `NullEq` has its
/// own NULL rule; every other operator here is `NULL` if either operand
/// is `NULL` — including `DIV`/`MOD` by zero, matching the `Int`/
/// `Decimal` case. `DIV`'s quotient truncates toward zero, same as `Int`;
/// bitwise/shift operators round to the nearest `i64` first, but TIES TO
/// EVEN — the OPPOSITE tie-breaking rule from `Decimal`'s own bitwise
/// conversion (ties away from zero), confirmed via `goeval`, not assumed.
pub(super) fn float_binary(
    op: BinaryOp,
    l: Datum,
    r: Datum,
    unsigned_pair: bool,
    ctx: &dyn crate::context::Columns,
) -> Result<Datum, EvalError> {
    use BinaryOp::*;
    if op == NullEq {
        return Ok(match (&l, &r) {
            (Datum::Null, Datum::Null) => Datum::Int(1),
            (Datum::Null, _) | (_, Datum::Null) => Datum::Int(0),
            _ => bool_int(to_f64(l) == to_f64(r)),
        });
    }
    if l == Datum::Null || r == Datum::Null {
        return Ok(Datum::Null);
    }
    // `intDivideFunctionClass.getFunction` stamps the result with
    // `UnsignedFlag` when EITHER operand carries it, and
    // `builtinArithmeticIntDivideDecimalSig` then reads the quotient back
    // through `ConvertDecimalToUint`, which REJECTS a negative quotient rather
    // than wrapping it. So `1u DIV -1` is an out-of-range error while
    // `1u DIV -2` is an unsigned 0.
    //
    // `unsigned_pair` is the caller's answer because the `Datum` alone cannot
    // give it: `DOUBLE UNSIGNED` and `DOUBLE` both read back as `Datum::Real`,
    // so deriving it from the operand KIND here missed every unsigned
    // floating-point column.
    let unsigned_div = unsigned_pair;
    let a = to_f64(l);
    let b = to_f64(r);
    Ok(match op {
        Plus => finite_float(a + b)?,
        Minus => finite_float(a - b)?,
        Mul => finite_float(a * b)?,
        Div => {
            if b == 0.0 {
                ctx.handle_division_by_zero()?;
                Datum::Null
            } else {
                finite_float(a / b)?
            }
        }
        IntDiv => {
            if b == 0.0 {
                ctx.handle_division_by_zero()?;
                Datum::Null
            } else {
                let quotient = (a / b).trunc();
                if unsigned_div {
                    if quotient < 0.0 {
                        return Err(EvalError::IntOverflow);
                    }
                    Datum::UInt(f64_to_u64(quotient).ok_or(EvalError::IntOverflow)?)
                } else {
                    Datum::Int(f64_to_i64(quotient).ok_or(EvalError::IntOverflow)?)
                }
            }
        }
        Mod => {
            if b == 0.0 {
                ctx.handle_division_by_zero()?;
                Datum::Null
            } else {
                finite_float(a % b)?
            }
        }
        Eq => bool_int(a == b),
        Ge => bool_int(a >= b),
        Gt => bool_int(a > b),
        Le => bool_int(a <= b),
        Lt => bool_int(a < b),
        Ne => bool_int(a != b),
        BitAnd | BitOr | BitXor | LeftShift | RightShift => {
            let (ai, bi) = match (
                f64_to_i64(a.round_ties_even()),
                f64_to_i64(b.round_ties_even()),
            ) {
                (Some(x), Some(y)) => (x, y),
                _ => return Err(EvalError::IntOverflow),
            };
            match op {
                BitAnd => Datum::UInt((ai as u64) & (bi as u64)),
                BitOr => Datum::UInt((ai as u64) | (bi as u64)),
                BitXor => Datum::UInt((ai as u64) ^ (bi as u64)),
                LeftShift => Datum::UInt(shift_left(ai as u64, bi as u64)),
                RightShift => Datum::UInt(shift_right(ai as u64, bi as u64)),
                _ => unreachable!("guarded by outer match"),
            }
        }
        LogicAnd | LogicOr | LogicXor | NullEq => unreachable!("handled by caller"),
    })
}

/// Coerces a non-`NULL` value to `f64` (MySQL's implicit promotion:
/// `Int`/`Decimal` both convert, lossily for `Decimal` past `f64`'s own
/// precision); `Str` is unreachable here — `eval_binary` guards it out
/// before dispatching to float handling. Also reused by `func::extremum`
/// to promote `LEAST`/`GREATEST`'s result when any argument is `Float`.
pub(crate) fn to_f64(v: Datum) -> f64 {
    match v {
        Datum::Real(f) => f,
        Datum::Decimal(d) => d.to_f64(),
        Datum::Int(i) => integer_to_f64(Integer::Signed(i)),
        Datum::UInt(i) => integer_to_f64(Integer::Unsigned(i)),
        Datum::String(_) | Datum::Bytes(_) | Datum::Null | Datum::MinNotNull | Datum::MaxValue => {
            unreachable!("guarded by caller")
        }
        other => {
            other
                .to_f64()
                .expect("numeric caller must supply a real-convertible datum")
                .value
        }
    }
}

/// Coerces a scalar to the `ETReal` comparison/function domain used by TiDB
/// when a string and a number meet.  The string case ports the numeric-prefix
/// consumption performed by `types.StrToFloat`: leading ASCII whitespace and
/// sign, digits, an optional fractional component, and a complete optional
/// exponent are accepted; everything after that prefix is ignored.  No prefix
/// is zero.  TiDB records truncation/overflow warnings in statement context;
/// this value-only layer has no warning domain, but preserves the resulting
/// numeric comparison/function value.
///
/// AUDIT of the `0.0` fallbacks below, against `Datum.ToFloat64` in
/// `pkg/types/datum.go`.  Callers reached today: `float_binary` (via
/// `eval_binary`), `math_fn::{sign, numeric_arg, ceil_floor}`,
/// `string_fn::{field, format_number_text}`, `builtin_ext::info::real_arg`
/// and `builtin_ext::compare2::interval_real` (which carries its own copy of
/// the same prefix rule, with the same fallbacks).  Every verdict below is a
/// captured TiDB answer, not a reading of the Go source alone.
///
/// | operand kind | Go | here | verdict |
/// | --- | --- | --- | --- |
/// | `Int`/`UInt`/`Real`/`Float32`/`Decimal` | value | value | MATCHES |
/// | `String`/`Bytes`, valid UTF-8 | numeric prefix, warn 1292 | prefix | MATCHES (warning missing, see below) |
/// | `String`/`Bytes`, INVALID UTF-8 | numeric prefix of the BYTES: `ABS(0x3132FF)` is 12 | `0.0` | DIVERGES |
/// | `Enum` | ORDINAL: `ABS(e)` is 2 for `e='8'` of `enum('9','8','7')` | ordinal | MATCHES |
/// | `Set` | bitmask: `ABS(s)` is 3 for `'8,9'` of `set('9','8')` | bitmask | MATCHES |
/// | `Bit`/`BinaryLiteral` | unsigned integer value (`b'11'` is 3) | same | MATCHES |
/// | `Time`/`Duration` | numeric form (`FORMAT_BYTES(DATE'2021-01-01')` reads 20210101) | same | MATCHES |
/// | `Json` | `ConvertJSONToFloat` (a JSON string takes the prefix rule) | same | MATCHES |
/// | `Raw`/`VectorFloat32` | ERROR (`SQRT(vec)`, `FIELD(1,vec)`, `INTERVAL(1,vec)` all fail) | ERROR | MATCHES |
/// | `Null`/`MinNotNull`/`MaxValue` | ERROR | ERROR | MATCHES |
///
/// The last two rows are why this returns a `Result`. There is no kind whose
/// conversion may be assumed to succeed, so there is no fallback to pick a
/// value for and no `unreachable!` left to be wrong about: whatever
/// `Datum::to_f64` declines, TiDB declines too, and the caller propagates it.
///
/// Both gaps this audit found are now CLOSED.
///
///  * `math_fn::{abs, sign, round_or_truncate}` matched a closed list of
///    kinds and refused the rest, so `ABS('12abc')` was an error where TiDB
///    answers 12. Each now ends in the `ETReal` signature Go's own
///    per-eval-type dispatch selects, reached through this coercion.
///  * TiDB raises `1292 Truncated incorrect DOUBLE value: '<text>'` whenever
///    the numeric prefix is shorter than the operand, and this function is
///    where Go raises it too (`getValidFloatPrefix` calls
///    `ctx.HandleTruncate` before returning). `ctx` is now threaded here so
///    the warning is raised ONCE, at the coercion, rather than once per
///    calling builtin -- see [`raise_truncated_double`].
pub(crate) fn to_f64_with_mysql_string(
    v: &Datum,
    ctx: &dyn crate::context::Columns,
) -> Result<f64, EvalError> {
    match v {
        Datum::String(s) => Ok(bytes_to_f64(s.bytes(), ctx)),
        Datum::Bytes(s) => Ok(bytes_to_f64(s, ctx)),
        Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_) | Datum::Real(_) | Datum::Float32(_) => {
            Ok(to_f64(v.clone()))
        }
        other => other
            .to_f64()
            .map(|converted| converted.value)
            .map_err(|_| EvalError::Unsupported("numeric argument conversion")),
    }
}

/// `types.StrToFloat` over the RAW BYTES of a SQL string.
///
/// Go strings are byte slices, so `StrToFloat` scans a `latin1`/`binary`
/// payload exactly as it scans a UTF-8 one: `ABS(0x3132FF)` is 12, not 0.
/// Decoding lossily is equivalent AND total here, because every byte an
/// invalid sequence can hold is a byte the numeric-prefix scan stops on
/// anyway -- so U+FFFD ends the prefix precisely where the raw byte would.
/// That leaves one implementation of the prefix rule instead of a decode
/// that can fail and a fallback that has to guess.
///
/// `is_function_cast` is TRUE because that is the flag the expression engine
/// reaches this conversion with: a string operand of a real-typed builtin or
/// comparison is wrapped in `WrapWithCastAsReal`, and
/// `builtinCastStringAsRealSig.evalReal` calls `types.StrToFloat(ctx, val,
/// true)`. The flag changes nothing about the VALUE, only whether the EMPTY
/// string counts as truncated -- and captured, an empty string raises no
/// warning at all where `'abc' + 1` raises 1292.
pub(super) fn bytes_to_f64(bytes: &[u8], ctx: &dyn crate::context::Columns) -> f64 {
    let text = String::from_utf8_lossy(bytes);
    let converted = tidb_datatype::str_to_float(&text, true);
    if converted.event.is_some() {
        raise_truncated_double(ctx, text.trim());
    }
    converted.value
}

/// Go `ErrTruncatedWrongVal.GenWithStackByArgs("DOUBLE", s)` at warning
/// level: `1292 Truncated incorrect DOUBLE value: '<s>'`.
///
/// `s` is the string AFTER `strings.TrimSpace`, because `StrToFloat` trims
/// before handing the value to `getValidFloatPrefix`, and both of that
/// function's raise sites name the trimmed form (captured: a padded
/// `' 12abc '` warns about `'12abc'`, and a padded `' 12 '` does not warn).
///
/// Go raises this ONCE PER EVALUATION, so a query with three rows and two
/// coercing sites records six warnings, not one (captured:
/// `SELECT ABS(a), a+0 FROM t` over three rows lists all six). Nothing here
/// deduplicates, for the same reason.
pub(crate) fn raise_truncated_double(ctx: &dyn crate::context::Columns, text: &str) {
    ctx.append_warning(1292, &format!("Truncated incorrect DOUBLE value: '{text}'"));
}

pub(super) fn real_compare(op: BinaryOp, a: f64, b: f64) -> Result<Datum, EvalError> {
    use BinaryOp::*;
    Ok(match op {
        Eq | NullEq => bool_int(a == b),
        Ge => bool_int(a >= b),
        Gt => bool_int(a > b),
        Le => bool_int(a <= b),
        Lt => bool_int(a < b),
        Ne => bool_int(a != b),
        _ => unreachable!("caller restricts this helper to comparison operators"),
    })
}

/// Wraps a finite `f64` as [`Datum::Real`], or reports the overflow
/// (confirmed via `goeval`: MySQL raises a genuine evaluation error for a
/// result that would overflow to `+/-infinity`, e.g. `1e300 * 1e300`
/// — never silently produces IEEE-754 infinity). Also reused by
/// `math_fn` for `POW`/`EXP`, where a `NaN` result (e.g. `POW(-2, 0.5)`,
/// a complex result) hits this SAME check and is reported the same way —
/// confirmed via `goeval` that real MySQL treats both alike, not assumed.
pub(crate) fn finite_float(f: f64) -> Result<Datum, EvalError> {
    if f.is_finite() {
        Ok(Datum::Real(f))
    } else {
        Err(EvalError::FloatOverflow)
    }
}

/// Converts an already-integer-valued `f64` to `i64`, `None` if it doesn't
/// fit. Compares against `i64::MIN`/`MAX` as their own exact `f64`
/// values (`-2^63`/`2^63`, both exactly representable) rather than
/// `i64::MIN as f64`/`i64::MAX as f64` — casting `i64::MAX` to `f64`
/// itself rounds UP to `2^63` (since `i64::MAX` isn't exactly
/// representable), which would let an out-of-range value slip past the
/// check.
pub(super) fn f64_to_i64(f: f64) -> Option<i64> {
    const I64_MIN: f64 = -9223372036854775808.0;
    const I64_MAX_EXCLUSIVE: f64 = 9223372036854775808.0;
    if (I64_MIN..I64_MAX_EXCLUSIVE).contains(&f) {
        Some(f as i64)
    } else {
        None
    }
}

/// The unsigned counterpart of [`f64_to_i64`], with the same exact-boundary
/// reasoning: `2^64` is exactly representable, `u64::MAX` is not.
pub(super) fn f64_to_u64(f: f64) -> Option<u64> {
    const U64_MAX_EXCLUSIVE: f64 = 18446744073709551616.0;
    if (0.0..U64_MAX_EXCLUSIVE).contains(&f) {
        Some(f as u64)
    } else {
        None
    }
}
