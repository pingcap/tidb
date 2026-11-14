// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `pkg/expression/builtin_arithmetic_vec.go`: evaluate operands as columns
//! before the arithmetic loop, retaining the operand buffer Go uses as result.

use super::{vec_eval, Kernel, ScalarFunction};
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
    let mut buffer;
    if reuse_right || separate_result {
        buffer = vec_eval::argument(&function.args[0], domain, ctx, input)?;
        vec_eval::eval_into(&function.args[1], domain, ctx, input, result)?;
    } else {
        vec_eval::eval_into(&function.args[0], domain, ctx, input, result)?;
        buffer = vec_eval::argument(&function.args[1], domain, ctx, input)?;
    }
    if separate_result {
        let right = std::mem::take(result);
        result.resize_int64(input.num_rows(), false);
        for i in 0..input.num_rows() {
            let left = vec_eval::value(&buffer, i, domain, function.args[0].static_type());
            let right = vec_eval::value(&right, i, domain, function.args[1].static_type());
            // Go `builtinArithmeticIntDivideDecimalSig.vecEvalInt` names the
            // operands' values (`num[i].String()`) in its overflow error,
            // where the row form names the argument expressions.
            let operands = format!("({} DIV {})", decimal_string(&left), decimal_string(&right));
            let value = function
                .eval_binary_values(op, left, right, ctx)
                .map_err(|error| match error {
                    EvalError::DataOutOfRange { value, .. } => {
                        EvalError::DataOutOfRange { value, expression: operands }
                    }
                    error => error,
                })?;
            vec_eval::put(result, i, EvalType::Int, &value)?;
        }
        return Ok(());
    }
    result.merge_nulls(&[&buffer]);
    for i in 0..input.num_rows() {
        if result.is_null(i) {
            continue;
        }
        let (left, right) = if reuse_right {
            (&buffer, &*result)
        } else {
            (&*result, &buffer)
        };
        let left = vec_eval::value(left, i, domain, function.args[0].static_type());
        let right = vec_eval::value(right, i, domain, function.args[1].static_type());
        let value = match function.pb_builtin.kernel {
            Kernel::IntegerMultiply { unsigned } => {
                let l = crate::arg_eval_type::eval_int(&left)?.unwrap();
                let r = crate::arg_eval_type::eval_int(&right)?.unwrap();
                (if unsigned {
                    (l as u64).checked_mul(r as u64).map(Datum::UInt)
                } else {
                    l.checked_mul(r).map(Datum::Int)
                })
                .ok_or_else(|| super::arithmetic_overflow_error(function, op, ctx))?
            }
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
    // Drop the allocator buffer after the result is fully written.
    buffer.reset();
    Ok(())
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
