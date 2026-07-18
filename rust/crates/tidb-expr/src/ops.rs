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

//! Unary and binary operator evaluation over [`Datum`] — arithmetic,
//! bitwise, comparison, and logical (three-valued) semantics, for both the
//! `Int` and `Decimal` domains.

use tidb_ast::{BinaryOp, UnaryOp};

use crate::coerce::{
    bool_int, integer_bits, integer_cmp, integer_of, integer_to_decimal, integer_to_f64, truthy_of,
    Integer,
};
use crate::{Datum, Decimal, EvalError};
use tidb_datatype::{div_int64, div_int_with_uint, div_uint_with_int};

pub(crate) fn eval_unary(op: UnaryOp, v: Datum) -> Result<Datum, EvalError> {
    use UnaryOp::*;
    // Every unary operator applied to NULL is NULL.
    if v == Datum::Null {
        return Ok(Datum::Null);
    }
    if v.is_range_sentinel() {
        return Err(EvalError::Unsupported("range sentinel expression operand"));
    }
    // Logical NOT is three-valued truthiness, shared by Int and Decimal.
    if let Not | NotKeyword = op {
        return match truthy_with_mysql_string(&v)? {
            Some(t) => Ok(bool_int(!t)),
            None => Ok(Datum::Null),
        };
    }
    match v {
        // Numeric coercion of strings is out of the current domain.
        Datum::String(_) | Datum::Bytes(_) => Err(EvalError::Unsupported("string operand")),
        Datum::Decimal(d) => match op {
            Plus => Ok(Datum::Decimal(d)),
            Minus => Ok(Datum::Decimal(d.negate())),
            // `~x` rounds to the nearest integer first (ties away from zero),
            // then flips the bits exactly like the `Int` case.
            BitNeg => d
                .round_to_i64()
                .map(|i| Datum::UInt(!(i as u64)))
                .ok_or(EvalError::IntOverflow),
            Not | NotKeyword => unreachable!("handled above"),
        },
        // Negating a finite f64 is always finite, so no overflow check is
        // needed there — only `~` can fail (out-of-`i64`-range).
        Datum::Real(f) => match op {
            Plus => Ok(Datum::Real(f)),
            Minus => Ok(Datum::Real(-f)),
            // `~x` rounds to the nearest integer first — but TIES TO
            // EVEN, the OPPOSITE tie-breaking rule from `Decimal`'s own
            // `~` (ties away from zero) — a real, easy-to-miss asymmetry
            // confirmed via `goeval`, not assumed: `~2.5` is `-3` (2.5
            // rounds to the even 2, `~2` is `-3`), not `-4` (which
            // away-from-zero rounding to 3 would give).
            BitNeg => f64_to_i64(f.round_ties_even())
                .map(|i| Datum::UInt(!(i as u64)))
                .ok_or(EvalError::IntOverflow),
            Not | NotKeyword => unreachable!("handled above"),
        },
        Datum::Int(i) => Ok(match op {
            Plus => Datum::Int(i),
            // Negating the one signed magnitude without an i64 counterpart
            // promotes to DECIMAL, rather than wrapping back to Int::MIN.
            // This is the second unary-minus step in `--9223372036854775808`.
            Minus if i == i64::MIN => Datum::Decimal(Decimal::from_uint(1_u64 << 63)),
            Minus => Datum::Int(-i),
            BitNeg => Datum::UInt(!(i as u64)),
            Not | NotKeyword => unreachable!("handled above"),
        }),
        Datum::UInt(i) => Ok(match op {
            Plus => Datum::UInt(i),
            // A unary minus keeps an ordinary unsigned magnitude in the
            // signed domain when it fits, preserves the one representable
            // `-2^63` boundary, and otherwise promotes to DECIMAL instead
            // of wrapping through an invented UInt result. This is the
            // parser/evaluator outcome TiDB exposes for `-u64::MAX`.
            Minus if i <= i64::MAX as u64 => Datum::Int(-(i as i64)),
            Minus if i == (1_u64 << 63) => Datum::Int(i64::MIN),
            Minus => Datum::Decimal(Decimal::from_uint(i).negate()),
            BitNeg => Datum::UInt(!i),
            Not | NotKeyword => unreachable!("handled above"),
        }),
        Datum::Null => unreachable!("handled above"),
        Datum::MinNotNull | Datum::MaxValue => unreachable!("rejected above"),
    }
}

/// Evaluates a binary operation with the session's explicit decimal-division
/// scale increment. Context-free callers must use [`eval_binary`], which
/// preserves TiDB's default of 4.
pub(crate) fn eval_binary_with_div_precision(
    op: BinaryOp,
    l: Datum,
    r: Datum,
    div_precision_increment: u32,
) -> Result<Datum, EvalError> {
    use BinaryOp::*;
    if l.is_range_sentinel() || r.is_range_sentinel() {
        return Err(EvalError::Unsupported("range sentinel expression operand"));
    }
    // `<=>` never propagates NULL.  Handle its NULL cases before selecting a
    // comparison type, matching `compareFunctionClass` in
    // `pkg/expression/builtin_compare.go`; this also lets `NULL <=> '1'`
    // return false instead of being rejected by the mixed-string guard below.
    if op == NullEq {
        match (&l, &r) {
            (Datum::Null, Datum::Null) => return Ok(Datum::Int(1)),
            (Datum::Null, _) | (_, Datum::Null) => return Ok(Datum::Int(0)),
            _ => {}
        }
    }
    // Go builds AND/OR/XOR with ETInt arguments, so each operand first takes
    // MySQL's numeric-prefix truthiness path. This must precede the ordinary
    // string comparison branch: string-vs-string is a binary collation
    // comparison for `=`, `<`, etc., but never for a logical operator.
    match op {
        LogicAnd => return logic_and(l, r),
        LogicOr => return logic_or(l, r),
        LogicXor => return logic_xor(l, r),
        _ => {}
    }
    // Two strings compare under the session's utf8mb4_bin collation (byte order,
    // PAD SPACE).
    if let (Some(a), Some(b)) = (l.as_raw_bytes(), r.as_raw_bytes()) {
        return string_compare(op, a, b);
    }
    // `getBaseCmpType` in `builtin_compare.go` selects ETReal whenever a
    // string is compared with a numeric value.  Thus both operands use the
    // same MySQL numeric-prefix coercion as `EvalReal`; this is comparison
    // semantics only, not a claim that arbitrary string arithmetic is in
    // scope for this compact value evaluator.
    if matches!(l, Datum::String(_) | Datum::Bytes(_))
        || matches!(r, Datum::String(_) | Datum::Bytes(_))
    {
        if matches!(op, Eq | Ge | Gt | Le | Lt | Ne | NullEq) {
            if l == Datum::Null || r == Datum::Null {
                return Ok(Datum::Null);
            }
            return real_compare(
                op,
                to_f64_with_mysql_string(&l),
                to_f64_with_mysql_string(&r),
            );
        }
        return Err(EvalError::Unsupported("string operand"));
    }
    // `Float` dominates `Decimal` in MySQL's promotion hierarchy — the
    // OPPOSITE of how `Decimal` dominates `Int` below — so this check
    // must run before the `Div`/`Decimal` dispatch, not after: an
    // Int/Float or Decimal/Float pair promotes BOTH operands to `f64`,
    // not to `Decimal` (confirmed via goeval: `1.5e2 + 3.14` is
    // `FLOAT:153.14`, not a `Decimal`).
    if matches!(l, Datum::Real(_)) || matches!(r, Datum::Real(_)) {
        return float_binary(op, l, r);
    }
    // `/` always promotes both operands to Decimal and produces a Decimal
    // result — even for two Int operands, MySQL's `/` never yields an Int
    // (confirmed via goeval: `1 / 2` is `DEC:0.5000`) — so it's intercepted
    // here, before the Int-only/decimal-only dispatch below would otherwise
    // only reach it when a Decimal operand was ALREADY present.
    if op == Div {
        if l == Datum::Null || r == Datum::Null {
            return Ok(Datum::Null);
        }
        let a = to_decimal(l);
        let b = to_decimal(r);
        let target_scale = a.scale() + effective_div_precision_increment(div_precision_increment);
        return Ok(match a.true_div(&b, target_scale) {
            Some(q) => Datum::Decimal(q),
            None => Datum::Null,
        });
    }
    // A Decimal operand (an Int operand promotes to a scale-0 decimal, MySQL's
    // implicit rule) arithmetics/compares exactly; handles its own NullEq.
    if matches!(l, Datum::Decimal(_)) || matches!(r, Datum::Decimal(_)) {
        return decimal_binary(op, l, r);
    }
    if op == NullEq {
        return Ok(null_safe_eq(l, r));
    }
    // By this point `l`/`r` can only be an integral value or `Null` — `Str` is
    // guarded out at the very top, `Float`/`Decimal`/`Div` are all
    // intercepted above — so an explicit NULL-propagation arm here,
    // rather than a silent wildcard, means a FUTURE `Datum` variant
    // added without updating those upstream guards PANICS instead of
    // silently being treated as NULL.
    if l == Datum::Null || r == Datum::Null {
        return Ok(Datum::Null);
    }
    let (a, b) = match (integer_of(&l)?, integer_of(&r)?) {
        (Some(a), Some(b)) => (a, b),
        _ => unreachable!("eval_binary's own upstream guards exclude this: {l:?} {r:?}"),
    };
    integer_binary(op, a, b)
}

fn integer_binary(op: BinaryOp, a: Integer, b: Integer) -> Result<Datum, EvalError> {
    use BinaryOp::*;
    let lhs_unsigned = matches!(a, Integer::Unsigned(_));
    let rhs_unsigned = matches!(b, Integer::Unsigned(_));
    let unsigned = lhs_unsigned || rhs_unsigned;
    let bits_a = integer_bits(a);
    let bits_b = integer_bits(b);
    Ok(match op {
        Plus => integer_result(unsigned, bits_a.wrapping_add(bits_b)),
        Minus => integer_result(unsigned, bits_a.wrapping_sub(bits_b)),
        Mul => integer_result(unsigned, bits_a.wrapping_mul(bits_b)),
        // `DIV`/`MOD` by zero yield NULL in MySQL. `DIV` truncates toward zero.
        IntDiv => {
            if bits_b == 0 {
                Datum::Null
            } else {
                // Go selects a different checked helper for every signedness
                // pair.  In particular, a mixed signed/unsigned quotient is
                // an unsigned result and rejects a negative quotient instead
                // of dividing the raw two's-complement bit patterns.
                let quotient = match (a, b) {
                    (Integer::Unsigned(lhs), Integer::Unsigned(rhs)) => lhs / rhs,
                    (Integer::Unsigned(lhs), Integer::Signed(rhs)) => {
                        div_uint_with_int(lhs, rhs).map_err(|_| EvalError::IntOverflow)?
                    }
                    (Integer::Signed(lhs), Integer::Unsigned(rhs)) => {
                        div_int_with_uint(lhs, rhs).map_err(|_| EvalError::IntOverflow)?
                    }
                    (Integer::Signed(lhs), Integer::Signed(rhs)) => {
                        return div_int64(lhs, rhs)
                            .map(Datum::Int)
                            .map_err(|_| EvalError::IntOverflow);
                    }
                };
                Datum::UInt(quotient)
            }
        }
        Mod => {
            if bits_b == 0 {
                Datum::Null
            } else {
                // MOD's result flag follows the left operand only.  Go's
                // mixed-sign implementations also preserve the dividend sign
                // instead of taking a remainder over raw unsigned bits.
                match (a, b) {
                    (Integer::Unsigned(lhs), Integer::Unsigned(rhs)) => Datum::UInt(lhs % rhs),
                    (Integer::Unsigned(lhs), Integer::Signed(rhs)) => {
                        Datum::UInt(lhs % rhs.unsigned_abs())
                    }
                    (Integer::Signed(lhs), Integer::Unsigned(rhs)) => {
                        let remainder = if lhs < 0 {
                            -((lhs.unsigned_abs() % rhs) as i64)
                        } else {
                            (lhs as u64 % rhs) as i64
                        };
                        Datum::Int(remainder)
                    }
                    (Integer::Signed(lhs), Integer::Signed(rhs)) => {
                        Datum::Int(lhs.wrapping_rem(rhs))
                    }
                }
            }
        }
        BitAnd => Datum::UInt(bits_a & bits_b),
        BitOr => Datum::UInt(bits_a | bits_b),
        BitXor => Datum::UInt(bits_a ^ bits_b),
        LeftShift => Datum::UInt(shift_left(bits_a, bits_b)),
        RightShift => Datum::UInt(shift_right(bits_a, bits_b)),
        Eq => bool_int(integer_cmp(a, b).is_eq()),
        Ge => bool_int(integer_cmp(a, b).is_ge()),
        Gt => bool_int(integer_cmp(a, b).is_gt()),
        Le => bool_int(integer_cmp(a, b).is_le()),
        Lt => bool_int(integer_cmp(a, b).is_lt()),
        Ne => bool_int(!integer_cmp(a, b).is_eq()),
        Div => unreachable!("handled above"),
        LogicAnd | LogicOr | LogicXor | NullEq => unreachable!("handled above"),
    })
}

fn integer_result(unsigned: bool, bits: u64) -> Datum {
    if unsigned {
        Datum::UInt(bits)
    } else {
        Datum::Int(bits as i64)
    }
}

/// Evaluates a context-free binary operation with TiDB's default
/// `div_precision_increment` of 4.
pub(crate) fn eval_binary(op: BinaryOp, l: Datum, r: Datum) -> Result<Datum, EvalError> {
    eval_binary_with_div_precision(op, l, r, 4)
}

/// TiDB preserves a declared decimal division result scale when the session
/// value is zero: `SET div_precision_increment = 0; SELECT 8 / 7` still
/// renders `1.1429`, while values 1 through 30 use their exact increment.
/// This comes from the source divide builtin's result-type construction plus
/// its runtime `DecimalDiv` call (`builtin_arithmetic.go:745,810`), not from
/// treating the stored TypeUnsigned value as anything other than zero.
pub(crate) const fn effective_div_precision_increment(raw: u32) -> u32 {
    if raw == 0 {
        4
    } else {
        raw
    }
}

/// Decimal arithmetic and comparison: an `Int` operand promotes to a scale-0
/// decimal (MySQL's implicit rule), and `+`/`-`/`*` and every comparison are
/// exact (see [`Decimal`]). `NullEq` has its own NULL rule; every other
/// operator here is `NULL` if either operand is `NULL` — including `DIV`/
/// `MOD` by zero, matching the `Int` case. `/` itself never reaches this
/// function — `eval_binary` intercepts it earlier, since it must promote
/// even a pure `Int`/`Int` pair to `Decimal`.
fn decimal_binary(op: BinaryOp, l: Datum, r: Datum) -> Result<Datum, EvalError> {
    use BinaryOp::*;
    if op == NullEq {
        return Ok(match (&l, &r) {
            (Datum::Null, Datum::Null) => Datum::Int(1),
            (Datum::Null, _) | (_, Datum::Null) => Datum::Int(0),
            _ => bool_int(to_decimal(l) == to_decimal(r)),
        });
    }
    if l == Datum::Null || r == Datum::Null {
        return Ok(Datum::Null);
    }
    let a = to_decimal(l);
    let b = to_decimal(r);
    Ok(match op {
        Plus => Datum::Decimal(a.add(&b)),
        Minus => Datum::Decimal(a.add(&b.negate())),
        Mul => Datum::Decimal(a.mul(&b)),
        Eq => bool_int(a == b),
        Ge => bool_int(a >= b),
        Gt => bool_int(a > b),
        Le => bool_int(a <= b),
        Lt => bool_int(a < b),
        Ne => bool_int(a != b),
        Div => unreachable!("handled above"),
        IntDiv => match a.div_rem(&b) {
            Some((q, _)) => Datum::Int(q),
            None => Datum::Null,
        },
        Mod => match a.div_rem(&b) {
            Some((_, r)) => Datum::Decimal(r),
            None => Datum::Null,
        },
        // Bitwise/shift operators work on integers in MySQL, so a decimal
        // operand rounds to the nearest `i64` first (ties away from zero),
        // same as unary `~` above.
        BitAnd | BitOr | BitXor | LeftShift | RightShift => {
            let (ai, bi) = match (a.round_to_i64(), b.round_to_i64()) {
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

/// Coerces a non-`NULL` value to [`Decimal`] (an `Int` promotes to scale 0);
/// `Str`/`Float` are unreachable here — `eval_binary` guards both out
/// before dispatching to decimal handling (`Float` takes priority over
/// `Decimal`, so a `Float` operand never reaches this function at all).
/// Also reused by `func::extremum` (only when no argument is `Float`, so
/// the same invariant holds there too).
pub(crate) fn to_decimal(v: Datum) -> Decimal {
    match v {
        Datum::Decimal(d) => d,
        Datum::Int(i) => integer_to_decimal(Integer::Signed(i)),
        Datum::UInt(i) => integer_to_decimal(Integer::Unsigned(i)),
        Datum::String(_)
        | Datum::Bytes(_)
        | Datum::Real(_)
        | Datum::Null
        | Datum::MinNotNull
        | Datum::MaxValue => {
            unreachable!("guarded by caller")
        }
    }
}

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
fn float_binary(op: BinaryOp, l: Datum, r: Datum) -> Result<Datum, EvalError> {
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
    let a = to_f64(l);
    let b = to_f64(r);
    Ok(match op {
        Plus => finite_float(a + b)?,
        Minus => finite_float(a - b)?,
        Mul => finite_float(a * b)?,
        Div => {
            if b == 0.0 {
                Datum::Null
            } else {
                finite_float(a / b)?
            }
        }
        IntDiv => {
            if b == 0.0 {
                Datum::Null
            } else {
                Datum::Int(f64_to_i64((a / b).trunc()).ok_or(EvalError::IntOverflow)?)
            }
        }
        Mod => {
            if b == 0.0 {
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
pub(crate) fn to_f64_with_mysql_string(v: &Datum) -> f64 {
    match v {
        Datum::String(s) => s.as_utf8().map(mysql_real_prefix).unwrap_or(0.0),
        Datum::Bytes(s) => std::str::from_utf8(s).map(mysql_real_prefix).unwrap_or(0.0),
        Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_) | Datum::Real(_) => to_f64(v.clone()),
        Datum::Null | Datum::MinNotNull | Datum::MaxValue => {
            unreachable!("non-scalar values are handled before numeric coercion")
        }
    }
}

fn mysql_real_prefix(s: &str) -> f64 {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    let mut end = 0;
    if matches!(bytes.first(), Some(b'+' | b'-')) {
        end = 1;
    }
    let integer_start = end;
    while bytes.get(end).is_some_and(u8::is_ascii_digit) {
        end += 1;
    }
    let has_integer = end != integer_start;
    let mut has_fraction = false;
    if bytes.get(end) == Some(&b'.') {
        end += 1;
        let fraction_start = end;
        while bytes.get(end).is_some_and(u8::is_ascii_digit) {
            end += 1;
        }
        has_fraction = end != fraction_start;
    }
    if !has_integer && !has_fraction {
        return 0.0;
    }
    if matches!(bytes.get(end), Some(b'e' | b'E')) {
        let exponent_start = end;
        let mut exponent_end = end + 1;
        if matches!(bytes.get(exponent_end), Some(b'+' | b'-')) {
            exponent_end += 1;
        }
        let exponent_digits = exponent_end;
        while bytes.get(exponent_end).is_some_and(u8::is_ascii_digit) {
            exponent_end += 1;
        }
        if exponent_end != exponent_digits {
            end = exponent_end;
        } else {
            end = exponent_start;
        }
    }
    match s[..end].parse::<f64>() {
        Ok(value) if value.is_finite() => value,
        // `types.StrToFloat` clamps an overflowing text magnitude to the
        // largest finite DOUBLE and records a warning.  Keeping the clamp
        // finite is important because this helper also feeds math signatures
        // such as SQRT/LOG, not just comparisons.
        Ok(_) | Err(_) if s.starts_with('-') => -f64::MAX,
        Ok(_) | Err(_) => f64::MAX,
    }
}

fn real_compare(op: BinaryOp, a: f64, b: f64) -> Result<Datum, EvalError> {
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
fn f64_to_i64(f: f64) -> Option<i64> {
    const I64_MIN: f64 = -9223372036854775808.0;
    const I64_MAX_EXCLUSIVE: f64 = 9223372036854775808.0;
    if (I64_MIN..I64_MAX_EXCLUSIVE).contains(&f) {
        Some(f as i64)
    } else {
        None
    }
}

/// Compares two strings under the `utf8mb4_bin` PAD SPACE collation (the
/// session default): case-sensitive byte order, trailing spaces ignored. Only
/// comparison operators are defined on strings here.
fn string_compare(op: BinaryOp, a: &[u8], b: &[u8]) -> Result<Datum, EvalError> {
    use BinaryOp::*;
    let ord = cmp_pad_space(a, b);
    Ok(match op {
        Eq | NullEq => bool_int(ord == std::cmp::Ordering::Equal),
        Ne => bool_int(ord != std::cmp::Ordering::Equal),
        Lt => bool_int(ord == std::cmp::Ordering::Less),
        Le => bool_int(ord != std::cmp::Ordering::Greater),
        Gt => bool_int(ord == std::cmp::Ordering::Greater),
        Ge => bool_int(ord != std::cmp::Ordering::Less),
        _ => return Err(EvalError::Unsupported("string arithmetic")),
    })
}

/// Byte comparison with the shorter operand padded by spaces (PAD SPACE), so
/// trailing spaces do not affect ordering.
fn cmp_pad_space(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    let n = a.len().max(b.len());
    for i in 0..n {
        let ca = a.get(i).copied().unwrap_or(b' ');
        let cb = b.get(i).copied().unwrap_or(b' ');
        match ca.cmp(&cb) {
            std::cmp::Ordering::Equal => continue,
            other => return other,
        }
    }
    std::cmp::Ordering::Equal
}

/// MySQL shifts operate on 64-bit unsigned values; a shift amount `>= 64`
/// yields 0.
fn shift_left(a: u64, b: u64) -> u64 {
    if b >= 64 {
        0
    } else {
        a << b
    }
}

fn shift_right(a: u64, b: u64) -> u64 {
    if b >= 64 {
        0
    } else {
        a >> b
    }
}

/// FALSE dominates; otherwise NULL propagates if either side is unknown.
/// Also called directly from `crate::eval_in`'s `BETWEEN` handling (`x >= lo
/// AND x <= hi`), not just from `eval_binary`'s `LogicAnd` arm.
pub(crate) fn logic_and(l: Datum, r: Datum) -> Result<Datum, EvalError> {
    Ok(match (logic_truthy(&l)?, logic_truthy(&r)?) {
        (Some(false), _) | (_, Some(false)) => Datum::Int(0),
        (Some(true), Some(true)) => Datum::Int(1),
        _ => Datum::Null,
    })
}

fn logic_or(l: Datum, r: Datum) -> Result<Datum, EvalError> {
    // TRUE dominates; otherwise NULL propagates if either side is unknown.
    Ok(match (logic_truthy(&l)?, logic_truthy(&r)?) {
        (Some(true), _) | (_, Some(true)) => Datum::Int(1),
        (Some(false), Some(false)) => Datum::Int(0),
        _ => Datum::Null,
    })
}

fn logic_xor(l: Datum, r: Datum) -> Result<Datum, EvalError> {
    Ok(match (logic_truthy(&l)?, logic_truthy(&r)?) {
        (Some(a), Some(b)) => bool_int(a ^ b),
        _ => Datum::Null,
    })
}

/// Truthiness for logical binary operators. TiDB's `LogicAnd`, `LogicOr`,
/// and `LogicXor` signatures evaluate both arguments as `ETInt`; only this
/// path must coerce a string by MySQL's numeric-prefix rule. All other scalar
/// variants retain the shared, type-native [`truthy_of`] behavior, while
/// `NULL` stays unknown for three-valued logic.
fn logic_truthy(value: &Datum) -> Result<Option<bool>, EvalError> {
    truthy_with_mysql_string(value)
}

/// Truthiness for predicate/unary contexts that use TiDB's implicit numeric
/// coercion.  Unlike [`truthy_of`], this includes strings and raw byte values:
/// Go's `EvalReal` consumes a numeric prefix (`'0.3'` is true, while `'aaa'`
/// is false) before `NOT`, `IS TRUE`, `IS FALSE`, and the corresponding
/// control-function wrappers inspect the result.  `NULL` remains unknown so
/// callers can preserve either three-valued logic or the always-definite `IS`
/// predicate result as appropriate.
pub(crate) fn truthy_with_mysql_string(value: &Datum) -> Result<Option<bool>, EvalError> {
    match value {
        Datum::String(_) | Datum::Bytes(_) => Ok(Some(to_f64_with_mysql_string(value) != 0.0)),
        _ => truthy_of(value),
    }
}

/// Only ever called from `eval_binary`'s own `NullEq` arm, AFTER its
/// `Str`/`Float`/`Decimal` guards have already run — so `l`/`r` here can
/// only be `Int` or `Null`, the SAME invariant `eval_binary`'s own final
/// match relies on (see that match's own comment). An explicit
/// `unreachable!()` for anything else, not a silent wildcard, for the
/// same reason.
fn null_safe_eq(l: Datum, r: Datum) -> Datum {
    match (l, r) {
        (Datum::Int(a), Datum::Int(b)) => bool_int(a == b),
        (Datum::Int(a), Datum::UInt(b)) => {
            bool_int(integer_cmp(Integer::Signed(a), Integer::Unsigned(b)).is_eq())
        }
        (Datum::UInt(a), Datum::Int(b)) => {
            bool_int(integer_cmp(Integer::Unsigned(a), Integer::Signed(b)).is_eq())
        }
        (Datum::UInt(a), Datum::UInt(b)) => bool_int(a == b),
        (Datum::Null, Datum::Null) => Datum::Int(1),
        (Datum::Null, _) | (_, Datum::Null) => Datum::Int(0),
        (l, r) => unreachable!("null_safe_eq's own caller excludes this: {l:?} {r:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{eval_binary, mysql_real_prefix};
    use crate::{Datum, EvalError};
    use tidb_ast::BinaryOp;

    #[test]
    fn mixed_string_number_comparisons_use_mysql_real_prefix() {
        for (text, expected) in [
            ("123", 123.0),
            ("  -2.5tail", -2.5),
            (".5tail", 0.5),
            ("1e2tail", 100.0),
            ("1e", 1.0),
            ("not a number", 0.0),
        ] {
            assert_eq!(mysql_real_prefix(text), expected, "{text}");
        }
        assert_eq!(mysql_real_prefix("1e999"), f64::MAX);
        assert_eq!(mysql_real_prefix("-1e999"), -f64::MAX);
        assert_eq!(
            eval_binary(
                BinaryOp::Eq,
                Datum::new_string("12x".to_owned()),
                Datum::Int(12),
            ),
            Ok(Datum::Int(1))
        );
        assert_eq!(
            eval_binary(
                BinaryOp::NullEq,
                Datum::Null,
                Datum::new_string("1".to_owned())
            ),
            Ok(Datum::Int(0))
        );
        for sentinel in [Datum::min_not_null(), Datum::max_value()] {
            assert_eq!(
                eval_binary(BinaryOp::NullEq, Datum::Null, sentinel.clone()),
                Err(EvalError::Unsupported("range sentinel expression operand"))
            );
            assert_eq!(
                eval_binary(BinaryOp::Eq, sentinel, Datum::new_string("1")),
                Err(EvalError::Unsupported("range sentinel expression operand"))
            );
        }
    }

    #[test]
    fn signed_int_division_rejects_the_only_overflow_case() {
        assert_eq!(
            eval_binary(BinaryOp::IntDiv, Datum::Int(i64::MIN), Datum::Int(-1)),
            Err(EvalError::IntOverflow)
        );
        assert_eq!(
            eval_binary(BinaryOp::Mod, Datum::Int(i64::MIN), Datum::Int(-1)),
            Ok(Datum::Int(0))
        );
    }

    /// Direct rows from `builtin_arithmetic_test.go::{TestArithmeticIntDivide,
    /// TestArithmeticMod}`.  The source has separate evaluator signatures for
    /// every signedness pair; these assertions keep that distinction visible
    /// in the compact Datum evaluator instead of silently dividing raw bits.
    #[test]
    fn integer_division_matches_go_signedness_helpers() {
        for (lhs, rhs, expected) in [
            (Datum::Int(13), Datum::Int(11), Ok(Datum::Int(1))),
            (Datum::Int(-13), Datum::Int(11), Ok(Datum::Int(-1))),
            (Datum::UInt(13), Datum::UInt(11), Ok(Datum::UInt(1))),
            (Datum::UInt(13), Datum::Int(11), Ok(Datum::UInt(1))),
            (Datum::UInt(1), Datum::Int(-2), Ok(Datum::UInt(0))),
            (Datum::Int(13), Datum::UInt(11), Ok(Datum::UInt(1))),
            (Datum::Int(-1), Datum::UInt(11), Ok(Datum::UInt(0))),
            (Datum::Int(13), Datum::Int(0), Ok(Datum::Null)),
        ] {
            assert_eq!(eval_binary(BinaryOp::IntDiv, lhs, rhs), expected);
        }
        assert_eq!(
            eval_binary(BinaryOp::IntDiv, Datum::UInt(1), Datum::Int(-1)),
            Err(EvalError::IntOverflow)
        );
        assert_eq!(
            eval_binary(BinaryOp::IntDiv, Datum::Int(-13), Datum::UInt(11)),
            Err(EvalError::IntOverflow)
        );
    }

    #[test]
    fn integer_mod_preserves_go_dividend_sign_and_result_flag() {
        for (lhs, rhs, expected) in [
            (Datum::Int(13), Datum::Int(11), Datum::Int(2)),
            (Datum::Int(-13), Datum::Int(11), Datum::Int(-2)),
            (Datum::Int(13), Datum::Int(-11), Datum::Int(2)),
            (Datum::UInt(13), Datum::UInt(11), Datum::UInt(2)),
            (Datum::UInt(13), Datum::Int(-11), Datum::UInt(2)),
            (Datum::Int(-22), Datum::UInt(10), Datum::Int(-2)),
            (Datum::Int(i64::MIN), Datum::UInt(3), Datum::Int(-2)),
            (Datum::Int(13), Datum::Int(0), Datum::Null),
        ] {
            assert_eq!(eval_binary(BinaryOp::Mod, lhs, rhs), Ok(expected));
        }
    }
}
