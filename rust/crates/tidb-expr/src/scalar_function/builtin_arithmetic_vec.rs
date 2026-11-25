// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `pkg/expression/builtin_arithmetic_vec.go`: evaluate operands as columns
//! before the arithmetic loop, retaining the operand buffer Go uses as result.

use super::{Kernel, ScalarFunction, vec_eval};
use crate::{Columns, EvalError};
use tidb_ast::BinaryOp;
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{Datum, EvalType};

/// Go `builtinArithmetic*Sig.vecEval*` and `builtinAbs*Sig.vecEval*`.
pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let (op, domain) = match function.pb_builtin.kernel {
        Kernel::Binary(op, domain) => (op, domain),
        Kernel::IntegerMultiply { .. } => (BinaryOp::Mul, EvalType::Int),
        Kernel::IntegerMod { .. } => (BinaryOp::Mod, EvalType::Int),
        _ => return abs(function, ctx, input, result),
    };
    // Go always evaluates the complete left operand before the right operand.
    // Plus/Minus/Mod/IntDivide over integers reuse the right operand; the
    // other arithmetic signatures reuse the left, except decimal IntDivide.
    let reuse_right = domain == EvalType::Int
        && matches!(
            op,
            BinaryOp::Plus | BinaryOp::Minus | BinaryOp::Mod | BinaryOp::IntDiv
        );
    let separate_result = domain == EvalType::Decimal && op == BinaryOp::IntDiv;
    let buffer;
    if reuse_right || separate_result {
        buffer = vec_eval::read_argument(&function.args[0], domain, ctx, input)?.into_contiguous();
        vec_eval::eval_into(&function.args[1], domain, ctx, input, result)?;
    } else {
        vec_eval::eval_into(&function.args[0], domain, ctx, input, result)?;
        buffer = vec_eval::read_argument(&function.args[1], domain, ctx, input)?.into_contiguous();
    }
    let buffer = buffer.as_ref();
    if separate_result {
        let right = std::mem::take(result);
        result.resize_int64(input.num_rows(), false);
        for i in 0..input.num_rows() {
            let left = vec_eval::value(buffer, i, domain, function.args[0].static_type());
            let right = vec_eval::value(&right, i, domain, function.args[1].static_type());
            // Go `builtinArithmeticIntDivideDecimalSig.vecEvalInt` names the
            // operands' values (`num[i].String()`) in its overflow error,
            // where the row form names the argument expressions.
            let operands = format!("({} DIV {})", decimal_string(&left), decimal_string(&right));
            let value = function
                .eval_binary_values(op, left, right, ctx)
                .map_err(|error| match error {
                    EvalError::DataOutOfRange { value, .. } => EvalError::DataOutOfRange {
                        value,
                        expression: operands,
                    },
                    error => error,
                })?;
            vec_eval::put(result, i, EvalType::Int, &value)?;
        }
        return Ok(());
    }
    result.merge_nulls(&[buffer]);
    match (function.pb_builtin.kernel, op, domain) {
        (Kernel::IntegerMultiply { unsigned }, _, _) => {
            let overflow = if unsigned {
                checked_int_rows(result, buffer, false, u64::checked_mul)
            } else {
                checked_int_rows(result, buffer, true, |a, b| {
                    (a as i64).checked_mul(b as i64).map(|value| value as u64)
                })
            };
            return overflow.map_err(|()| super::arithmetic_overflow_error(function, op, ctx));
        }
        (_, BinaryOp::Plus | BinaryOp::Minus, EvalType::Int) => {
            return add_or_subtract_int(function, op, ctx, result, buffer);
        }
        (_, BinaryOp::Plus | BinaryOp::Minus | BinaryOp::Mul, EvalType::Real) => {
            return real_arithmetic(function, op, ctx, result, buffer);
        }
        (
            _,
            BinaryOp::BitAnd
            | BinaryOp::BitOr
            | BinaryOp::BitXor
            | BinaryOp::LeftShift
            | BinaryOp::RightShift,
            EvalType::Int,
        ) => {
            super::builtin_op_vec::bitwise(op, result, buffer);
            return Ok(());
        }
        _ => {}
    }
    for i in 0..input.num_rows() {
        if result.is_null(i) {
            continue;
        }
        let (left, right) = if reuse_right {
            (buffer, &*result)
        } else {
            (&*result, buffer)
        };
        let left = vec_eval::value(left, i, domain, function.args[0].static_type());
        let right = vec_eval::value(right, i, domain, function.args[1].static_type());
        let value = match function.pb_builtin.kernel {
            Kernel::IntegerMod { unsigned } => {
                let operand = |v: Datum, unsigned: bool| -> Result<Datum, EvalError> {
                    let i = crate::arg_eval_type::eval_int(&v)?.unwrap();
                    Ok(if unsigned {
                        Datum::UInt(i as u64)
                    } else {
                        Datum::Int(i)
                    })
                };
                function.eval_binary_values(
                    op,
                    operand(left, unsigned[0])?,
                    operand(right, unsigned[1])?,
                    ctx,
                )?
            }
            _ => function.eval_binary_values(op, left, right, ctx)?,
        };
        vec_eval::put(result, i, domain, &value)?;
    }
    Ok(())
}

/// Go's integer loops compute packed values even for null rows. Only an
/// overflowing non-null row ends evaluation. Signed multiplication clears
/// that row's null bit before returning the error.
fn checked_int_rows(
    result: &mut Column,
    buffer: &Column,
    null_on_overflow: bool,
    calculate: impl Fn(u64, u64) -> Option<u64>,
) -> Result<(), ()> {
    let source = buffer.raw_cells();
    result.with_fixed_cells_mut(|cells, bitmap| {
        for (row, (cell, other)) in cells.iter_mut().zip(source.fixed_cells()).enumerate() {
            let (a, b) = (u64::from_ne_bytes(*cell), u64::from_ne_bytes(*other));
            if let Some(value) = calculate(a, b) {
                *cell = value.to_ne_bytes();
            } else {
                let bit = 1 << (row & 7);
                let mask = &mut bitmap[row >> 3];
                if *mask & bit == 0 {
                    continue;
                }
                if null_on_overflow {
                    *mask &= !bit;
                }
                return Err(());
            }
        }
        Ok(())
    })
}

fn add_or_subtract_int(
    function: &ScalarFunction,
    op: BinaryOp,
    ctx: &dyn Columns,
    result: &mut Column,
    left: &Column,
) -> Result<(), EvalError> {
    // The result buffer holds the right argument in both Go signatures.
    let [left_unsigned, right_unsigned] = [0, 1].map(|i| {
        function.args[i]
            .static_type()
            .is_some_and(|field| field.is_unsigned())
    });
    let force_signed = op == BinaryOp::Minus && ctx.no_unsigned_subtraction();
    let status = match (op, left_unsigned, right_unsigned) {
        (BinaryOp::Plus, false, false) => checked_int_rows(result, left, false, |b, a| {
            (a as i64).checked_add(b as i64).map(|value| value as u64)
        }),
        (BinaryOp::Plus, true, true) => checked_int_rows(result, left, false, u64::checked_add),
        (BinaryOp::Plus, true, false) => {
            checked_int_rows(result, left, false, |b, a| a.checked_add_signed(b as i64))
        }
        (BinaryOp::Plus, false, true) => {
            checked_int_rows(result, left, false, |b, a| b.checked_add_signed(a as i64))
        }
        (BinaryOp::Minus, _, _) => checked_int_rows(result, left, false, |b, a| {
            (!crate::ops::minus_overflows(
                left_unsigned,
                right_unsigned,
                force_signed,
                a as i64,
                b as i64,
            ))
            .then(|| a.wrapping_sub(b))
        }),
        _ => unreachable!(),
    };
    status.map_err(|()| {
        let error = super::arithmetic_overflow_error(function, op, ctx);
        if let EvalError::DataOutOfRange { expression, .. } = error {
            EvalError::DataOutOfRange {
                value: if (left_unsigned || right_unsigned) && !force_signed {
                    "BIGINT UNSIGNED"
                } else {
                    "BIGINT"
                },
                expression,
            }
        } else {
            error
        }
    })
}

fn real_arithmetic(
    function: &ScalarFunction,
    op: BinaryOp,
    ctx: &dyn Columns,
    result: &mut Column,
    right: &Column,
) -> Result<(), EvalError> {
    let source = right.raw_cells();
    let status = result.with_fixed_cells_mut(|cells, bitmap| {
        for (row, (cell, other)) in cells.iter_mut().zip(source.fixed_cells()).enumerate() {
            let (a, b) = (f64::from_ne_bytes(*cell), f64::from_ne_bytes(*other));
            let overflow = match op {
                BinaryOp::Mul => {
                    // Go writes the product before checking for infinity.
                    let value = a * b;
                    *cell = value.to_ne_bytes();
                    value.is_infinite()
                }
                BinaryOp::Plus | BinaryOp::Minus => {
                    let value = if op == BinaryOp::Plus { a + b } else { a - b };
                    if value.is_finite() {
                        *cell = value.to_ne_bytes();
                    }
                    !value.is_finite()
                }
                _ => unreachable!(),
            };
            if overflow && bitmap[row >> 3] & (1 << (row & 7)) != 0 {
                return Err(());
            }
        }
        Ok(())
    });
    status.map_err(|()| super::real_arithmetic_overflow_error(function, op, ctx))
}

/// Go `MyDecimal.String()` of a decimal operand.
fn decimal_string(value: &Datum) -> String {
    match value {
        Datum::Decimal(decimal) => {
            String::from_utf8_lossy(&decimal.to_chunk_my_decimal_lossy().to_string_bytes()).into_owned()
        }
        other => other.sql_string().unwrap_or_default(),
    }
}

/// Go `builtinAbsIntSig.vecEvalInt`, `builtinAbsUIntSig.vecEvalInt`,
/// `builtinAbsRealSig.vecEvalReal`, `builtinAbsDecimalSig.vecEvalDecimal`.
fn abs(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let domain = function.get_static_type().expect("ABS type").eval_type();
    vec_eval::eval_into(&function.args[0], domain, ctx, input, result)?;
    for i in 0..input.num_rows() {
        if result.is_null(i) {
            continue;
        }
        let value = vec_eval::value(result, i, domain, function.args[0].static_type());
        let value = crate::math_fn::abs(&[value], ctx)
            .map_err(|error| super::math_overflow_error(function, error, ctx))?;
        vec_eval::put(result, i, domain, &value)?;
    }
    Ok(())
}
