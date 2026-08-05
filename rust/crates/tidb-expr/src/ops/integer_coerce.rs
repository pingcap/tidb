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

//! The `ETInt` half of operator evaluation, split out of `ops.rs` -- the
//! sibling of [`super::real_coerce`].
//!
//! Everything an operand does once MySQL's promotion hierarchy has decided the
//! pair is an INTEGER pair: the signedness rule that decides which of Go's
//! per-signedness signatures applies ([`unsigned_operand`]), the arithmetic and
//! bitwise operators themselves ([`integer_binary`] and the overflow checks it
//! delegates to), the shift width rule, and unary minus -- the one operator
//! whose Int signature can be replaced at BUILD time by a decimal one
//! ([`unary_minus_integer`]).
//!
//! It is one module because these are the pieces a reader has to hold together
//! to answer "what does Go's `isLHSUnsigned` mean for this pair", and keeping
//! them beside the promotion hierarchy in `ops.rs` was what pushed that file
//! past the source-size ratchet.

use super::*;

/// Go's `uval := uint64(val)`: an operand whose FIELD TYPE carries
/// `UnsignedFlag` is read through that flag, whatever `Datum` kind its value
/// came back in. Only the integer kinds can be reinterpreted -- a `DOUBLE
/// UNSIGNED` stays a `Real`, and its signedness travels separately -- so this
/// touches `Datum::Int` alone.
pub(super) fn unsigned_operand(value: Datum, operand: Operand<'_>) -> Datum {
    match value {
        Datum::Int(bits) if operand.is_unsigned() => Datum::UInt(bits as u64),
        other => other,
    }
}

/// Go `builtinUnaryMinusIntSig.evalInt`'s two branches
/// (`pkg/expression/builtin_op.go:1106-1124`), plus the ONE promotion
/// `unaryMinusFunctionClass.typeInfer` performs ahead of them:
///
/// ```go
/// if arg, ok := argExpr.(*Constant); ok && tp == types.ETInt {
///     overflow = c.handleIntOverflow(ctx, arg)
///     if overflow { tp = types.ETDecimal }
/// }
/// ```
///
/// A CONSTANT whose negation leaves `BIGINT` is rebuilt on the DECIMAL
/// signature and keeps its magnitude; a COLUMN keeps the Int signature and
/// reports `ErrOverflow`. Captured over `b BIGINT` holding -9223372036854775808
/// and `u BIGINT UNSIGNED` holding 9223372036854775809: both `SELECT -b` and
/// `SELECT -u` are `[types:1690] BIGINT value is out of range`, where this
/// evaluator answered the CLAMPED 9223372036854775807 and -9223372036854775808.
pub(super) fn unary_minus_integer(
    bits: u64,
    unsigned: bool,
    arg: Operand<'_>,
) -> Result<Datum, EvalError> {
    if !unsigned {
        let value = bits as i64;
        if value != i64::MIN {
            return Ok(Datum::Int(-value));
        }
    } else {
        if bits <= i64::MAX as u64 {
            return Ok(Datum::Int(-(bits as i64)));
        }
        // `-2^63` is representable, so Go returns it rather than overflowing.
        if bits == 1_u64 << 63 {
            return Ok(Datum::Int(i64::MIN));
        }
    }
    if !arg.is_constant() {
        return Err(EvalError::IntOverflow);
    }
    Ok(Datum::Decimal(if unsigned {
        Decimal::from_uint(bits).negate()
    } else {
        Decimal::from_uint(1_u64 << 63)
    }))
}

pub(super) fn integer_binary(
    op: BinaryOp,
    a: Integer,
    b: Integer,
    ctx: &dyn crate::context::Columns,
) -> Result<Datum, EvalError> {
    use BinaryOp::*;
    let lhs_unsigned = matches!(a, Integer::Unsigned(_));
    let rhs_unsigned = matches!(b, Integer::Unsigned(_));
    let unsigned = lhs_unsigned || rhs_unsigned;
    let bits_a = integer_bits(a);
    let bits_b = integer_bits(b);
    Ok(match op {
        Plus => return integer_add(a, b),
        // `-` reports `ErrOverflow` exactly where Go `builtinArithmeticMinusIntSig`
        // does — via [`minus_overflows`], a line-for-line port of Go's
        // `overflowCheck` (verified against goeval across every branch). A
        // non-overflowing result keeps its wrapped two's-complement value, typed
        // by `unsigned`.
        Minus => {
            if minus_overflows(lhs_unsigned, rhs_unsigned, bits_a as i64, bits_b as i64) {
                return Err(EvalError::IntOverflow);
            }
            integer_result(unsigned, bits_a.wrapping_sub(bits_b))
        }
        // `*` matches Go's two sigs, selected by whether either operand is
        // unsigned (`getFunction`: `HasUnsignedFlag(lhs) || HasUnsignedFlag(rhs)`
        // == this `unsigned`). `builtinArithmeticMultiplyIntUnsignedSig` multiplies
        // the u64 bit patterns and errors when the product wraps
        // (`unsignedA != 0 && result/unsignedA != unsignedB`); `...MultiplyIntSig`
        // multiplies as i64 (`a != 0 && result/a != b`, plus the `MinInt64 * -1`
        // case). Both are exactly `checked_mul` on the respective type.
        Mul => {
            if unsigned {
                return bits_a
                    .checked_mul(bits_b)
                    .map(Datum::UInt)
                    .ok_or(EvalError::IntOverflow);
            }
            return (bits_a as i64)
                .checked_mul(bits_b as i64)
                .map(Datum::Int)
                .ok_or(EvalError::IntOverflow);
        }
        // `DIV`/`MOD` by zero yield NULL in MySQL. `DIV` truncates toward zero.
        IntDiv => {
            if bits_b == 0 {
                ctx.handle_division_by_zero()?;
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
                ctx.handle_division_by_zero()?;
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

pub(super) fn integer_result(unsigned: bool, bits: u64) -> Datum {
    if unsigned {
        Datum::UInt(bits)
    } else {
        Datum::Int(bits as i64)
    }
}

/// Integer `+` with TiDB's overflow rule (`builtinArithmeticPlusIntSig`): the
/// result is `UNSIGNED` when either operand is, and any result past that type's
/// range is `ErrOverflow`, never a silent wrap. Go errors in every signedness
/// case rather than adding the raw two's-complement bits, so each case maps to a
/// checked operation: a mixed sum underflows past `0` when a negative addend
/// exceeds the unsigned operand, or overflows past `u64::MAX`.
pub(super) fn integer_add(a: Integer, b: Integer) -> Result<Datum, EvalError> {
    match (a, b) {
        (Integer::Signed(x), Integer::Signed(y)) => x
            .checked_add(y)
            .map(Datum::Int)
            .ok_or(EvalError::IntOverflow),
        (Integer::Unsigned(x), Integer::Unsigned(y)) => x
            .checked_add(y)
            .map(Datum::UInt)
            .ok_or(EvalError::IntOverflow),
        (Integer::Unsigned(x), Integer::Signed(y)) | (Integer::Signed(y), Integer::Unsigned(x)) => {
            let sum = if y < 0 {
                x.checked_sub(y.unsigned_abs())
            } else {
                x.checked_add(y.unsigned_abs())
            };
            sum.map(Datum::UInt).ok_or(EvalError::IntOverflow)
        }
    }
}

/// A line-for-line port of Go `builtinArithmeticMinusIntSig.overflowCheck`:
/// `true` when `a - b` overflows the result type. `a`/`b` are the operands
/// reinterpreted as `i64` (Go passes the raw `int64` bits). `signed` is
/// `!lhs_unsigned && !rhs_unsigned` — Go's `forceToSigned` is the
/// `NO_UNSIGNED_SUBTRACTION` sql_mode, which this context-free layer does not
/// model, so this is the default (mode off). The branch structure and the final
/// condition mirror Go exactly; verified against goeval across every branch.
pub(super) fn minus_overflows(lhs_unsigned: bool, rhs_unsigned: bool, a: i64, b: i64) -> bool {
    let signed = !lhs_unsigned && !rhs_unsigned;
    let res = a.wrapping_sub(b);
    let (ua, ub) = (a as u64, b as u64);
    let mut res_unsigned = false;
    if lhs_unsigned {
        if rhs_unsigned {
            if ua < ub {
                if res >= 0 {
                    return true;
                }
            } else {
                res_unsigned = true;
            }
        } else if b >= 0 {
            if ua > ub {
                res_unsigned = true;
            }
        } else if ua > u64::MAX - b.unsigned_abs() {
            // Go `testIfSumOverflowsUll(ua, uint64(-b))`.
            return true;
        } else {
            res_unsigned = true;
        }
    } else if rhs_unsigned {
        // Go `uint64(a - math.MinInt64) < ub`.
        if (a.wrapping_sub(i64::MIN) as u64) < ub {
            return true;
        }
    } else if a > 0 && b < 0 {
        res_unsigned = true;
    } else if a < 0 && b > 0 && res >= 0 {
        return true;
    }
    (!signed && !res_unsigned && res < 0)
        || (signed && res_unsigned && (res as u64) > i64::MAX as u64)
}

/// MySQL shifts operate on 64-bit unsigned values; a shift amount `>= 64`
/// yields 0.
pub(super) fn shift_left(a: u64, b: u64) -> u64 {
    if b >= 64 {
        0
    } else {
        a << b
    }
}

pub(super) fn shift_right(a: u64, b: u64) -> u64 {
    if b >= 64 {
        0
    } else {
        a >> b
    }
}
